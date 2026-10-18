"""Contract tests for the admin runtime-catalog Function API."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import function_app
import pytest
import retrieval.catalog as catalog_module
from retrieval.catalog import BASELINE_CONFIG, build_catalog_item

OPERATION_ID = "11111111-1111-4111-8111-111111111111"
FUNCTION_APP_SOURCE = Path(function_app.__file__).read_text(encoding="utf-8")
FUNCTION_REQUIREMENTS = (
    Path(function_app.__file__).with_name("requirements.txt").read_text(encoding="utf-8")
)


class MissingItemError(RuntimeError):
    status_code = 404


class FakeContainer:
    def __init__(self, item: dict[str, Any] | None = None) -> None:
        self.item = deepcopy(item)
        self.create_count = 0

    def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
        assert item == "runtime-catalog"
        assert partition_key == "runtime-catalog"
        if self.item is None:
            raise MissingItemError("missing")
        return deepcopy(self.item)

    def create_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        self.create_count += 1
        if self.item is not None:
            error = RuntimeError("exists")
            error.status_code = 409
            raise error
        self.item = {**deepcopy(body), "_etag": '"etag-1"'}
        return deepcopy(self.item)


class FakeRequest:
    def __init__(
        self,
        method: str,
        *,
        body: dict[str, Any] | bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.method = method
        self._body = (
            json.dumps(body, separators=(",", ":")).encode("utf-8")
            if isinstance(body, dict)
            else body
        )
        self.headers = headers or {}

    def get_body(self) -> bytes:
        return self._body or b""

    def get_json(self) -> dict[str, Any]:
        if self._body is None:
            raise ValueError("missing body")
        return json.loads(self._body)


def _request_body(**extra: Any) -> dict[str, Any]:
    return {
        "config": deepcopy(BASELINE_CONFIG),
        "operationId": OPERATION_ID,
        "reason": "Approve baseline retrieval policy",
        **extra,
    }


def _persisted_item(*, operation_id: str = OPERATION_ID) -> dict[str, Any]:
    item = build_catalog_item(
        {"config": deepcopy(BASELINE_CONFIG)},
        operation_id=operation_id,
        changed_at=datetime(2026, 10, 17, tzinfo=timezone.utc),
        reason="Approve baseline retrieval policy",
    )
    return {**item, "_etag": '"etag-1"'}


def test_raw_request_and_canonical_item_budgets_match() -> None:
    assert (
        function_app.MAX_RETRIEVAL_CONFIG_REQUEST_BYTES
        == catalog_module.MAX_CATALOG_ITEM_BYTES
        == 1_572_864
    )


def test_route_avoids_reserved_functions_admin_path() -> None:
    assert 'route="management/retrieval-config"' in FUNCTION_APP_SOURCE
    assert 'route="admin/retrieval-config"' not in FUNCTION_APP_SOURCE


def test_function_package_declares_catalog_schema_validator() -> None:
    assert "jsonschema==4.26.0" in FUNCTION_REQUIREMENTS.splitlines()


@pytest.fixture(autouse=True)
def authorize(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(function_app, "_require_ingestion_operator", lambda _req: None)


def _use_container(monkeypatch: pytest.MonkeyPatch, container: FakeContainer) -> None:
    monkeypatch.setattr(
        function_app,
        "_build_retrieval_config_container",
        lambda: container,
    )


def test_get_returns_current_singleton_and_etag(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer(_persisted_item())
    _use_container(monkeypatch, container)

    response = function_app.manage_retrieval_config(FakeRequest("GET"))

    assert response.status_code == 200
    assert response.headers["ETag"] == '"etag-1"'
    payload = json.loads(response.get_body())
    assert payload["status"] == "current"
    assert payload["catalog"]["id"] == "runtime-catalog"


def test_get_returns_not_found_for_empty_container(monkeypatch: pytest.MonkeyPatch) -> None:
    _use_container(monkeypatch, FakeContainer())

    response = function_app.manage_retrieval_config(FakeRequest("GET"))

    assert response.status_code == 404
    assert json.loads(response.get_body()) == {"error": "catalog_not_found"}


def test_put_bootstraps_only_with_if_none_match(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    _use_container(monkeypatch, container)

    response = function_app.manage_retrieval_config(
        FakeRequest("PUT", body=_request_body(), headers={"If-None-Match": "*"})
    )

    assert response.status_code == 201
    assert container.create_count == 1
    assert json.loads(response.get_body())["status"] == "created"


def test_put_replays_same_operation_without_second_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = FakeContainer(_persisted_item())
    _use_container(monkeypatch, container)

    response = function_app.manage_retrieval_config(
        FakeRequest("PUT", body=_request_body(), headers={"If-None-Match": "*"})
    )

    assert response.status_code == 200
    assert container.create_count == 0
    assert json.loads(response.get_body())["status"] == "replayed"


def test_put_rejects_operation_id_reuse_with_different_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = FakeContainer(_persisted_item())
    _use_container(monkeypatch, container)
    body = _request_body()
    body["config"]["retrieval"]["overFetchFactor"] = 4

    response = function_app.manage_retrieval_config(
        FakeRequest("PUT", body=body, headers={"If-Match": '"etag-1"'})
    )

    assert response.status_code == 409
    assert json.loads(response.get_body()) == {"error": "operation_id_reused"}


@pytest.mark.parametrize(
    ("body", "headers", "expected_error"),
    [
        (_request_body(deploymentInstanceId="deploy-a"), {"If-None-Match": "*"}, "invalid_catalog_request"),
        (_request_body(), {}, "catalog_precondition_required"),
        (_request_body(reason=""), {"If-None-Match": "*"}, "invalid_catalog"),
    ],
)
def test_put_rejects_invalid_contract(
    monkeypatch: pytest.MonkeyPatch,
    body: dict[str, Any],
    headers: dict[str, str],
    expected_error: str,
) -> None:
    _use_container(monkeypatch, FakeContainer())

    response = function_app.manage_retrieval_config(
        FakeRequest("PUT", body=body, headers=headers)
    )

    assert response.status_code in {400, 428}
    assert json.loads(response.get_body()) == {"error": expected_error}


def test_put_rejects_declared_oversize_before_reading_body_or_cosmos(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = FakeRequest(
        "PUT",
        body=b"{}",
        headers={
            "Content-Length": str(function_app.MAX_RETRIEVAL_CONFIG_REQUEST_BYTES + 1),
            "If-None-Match": "*",
        },
    )
    monkeypatch.setattr(
        request,
        "get_body",
        lambda: pytest.fail("oversize declared body must not be read"),
    )
    monkeypatch.setattr(
        function_app,
        "_build_retrieval_config_container",
        lambda: pytest.fail("oversize request must not access Cosmos"),
    )

    response = function_app.manage_retrieval_config(request)

    assert response.status_code == 413
    assert json.loads(response.get_body()) == {"error": "catalog_request_too_large"}


@pytest.mark.parametrize("declared_length", [None, "2"])
def test_put_rejects_actual_oversize_whitespace_before_cosmos(
    monkeypatch: pytest.MonkeyPatch,
    declared_length: str | None,
) -> None:
    request_bytes = json.dumps(_request_body()).encode("utf-8")
    request_bytes += b" " * (
        function_app.MAX_RETRIEVAL_CONFIG_REQUEST_BYTES - len(request_bytes) + 1
    )
    headers = {"If-None-Match": "*"}
    if declared_length is not None:
        headers["Content-Length"] = declared_length
    monkeypatch.setattr(
        function_app,
        "_build_retrieval_config_container",
        lambda: pytest.fail("oversize request must not access Cosmos"),
    )

    response = function_app.manage_retrieval_config(
        FakeRequest(
            "PUT",
            body=request_bytes,
            headers=headers,
        )
    )

    assert response.status_code == 413
    assert json.loads(response.get_body()) == {"error": "catalog_request_too_large"}


def test_put_accepts_raw_body_at_exact_size_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_bytes = json.dumps(_request_body()).encode("utf-8")
    request_bytes += b" " * (
        function_app.MAX_RETRIEVAL_CONFIG_REQUEST_BYTES - len(request_bytes)
    )
    container = FakeContainer()
    _use_container(monkeypatch, container)

    response = function_app.manage_retrieval_config(
        FakeRequest(
            "PUT",
            body=request_bytes,
            headers={
                "Content-Length": str(
                    function_app.MAX_RETRIEVAL_CONFIG_REQUEST_BYTES
                ),
                "If-None-Match": "*",
            },
        )
    )

    assert response.status_code == 201
    assert container.create_count == 1


def test_put_maps_typed_canonical_item_size_failure_to_413(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        catalog_module,
        "build_catalog_item",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            catalog_module.CatalogSizeError("catalog too large")
        ),
    )
    monkeypatch.setattr(
        function_app,
        "_build_retrieval_config_container",
        lambda: pytest.fail("invalid catalog must not access Cosmos"),
    )

    response = function_app.manage_retrieval_config(
        FakeRequest(
            "PUT",
            body=_request_body(),
            headers={"If-None-Match": "*"},
        )
    )

    assert response.status_code == 413
    assert json.loads(response.get_body()) == {"error": "catalog_request_too_large"}


def test_put_rejects_stale_etag_before_guarded_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = FakeContainer(_persisted_item(operation_id="22222222-2222-4222-8222-222222222222"))
    _use_container(monkeypatch, container)
    called = False

    def fail_if_called(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(catalog_module, "apply_catalog_operation", fail_if_called)

    response = function_app.manage_retrieval_config(
        FakeRequest("PUT", body=_request_body(), headers={"If-Match": '"stale"'})
    )

    assert response.status_code == 412
    assert called is False


def test_put_update_uses_guarded_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer(_persisted_item(operation_id="22222222-2222-4222-8222-222222222222"))
    _use_container(monkeypatch, container)
    captured: dict[str, Any] = {}

    def apply(
        actual_container: FakeContainer,
        candidate: dict[str, Any],
        manifest: dict[str, Any],
    ) -> dict[str, Any]:
        captured.update(manifest)
        actual_container.item = {**deepcopy(candidate), "_etag": '"etag-2"'}
        return {"status": "applied-but-unassured"}

    monkeypatch.setattr(catalog_module, "apply_catalog_operation", apply)

    response = function_app.manage_retrieval_config(
        FakeRequest("PUT", body=_request_body(), headers={"If-Match": '"etag-1"'})
    )

    assert response.status_code == 200
    assert response.headers["ETag"] == '"etag-2"'
    assert captured["expectedEtag"] == '"etag-1"'
    assert captured["operationId"] == OPERATION_ID
    assert json.loads(response.get_body())["status"] == "updated"


def test_put_maps_guarded_writer_failure_to_service_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = FakeContainer(
        _persisted_item(operation_id="22222222-2222-4222-8222-222222222222")
    )
    _use_container(monkeypatch, container)

    def fail(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise catalog_module.CatalogError("Cosmos history write failed")

    monkeypatch.setattr(catalog_module, "apply_catalog_operation", fail)

    response = function_app.manage_retrieval_config(
        FakeRequest("PUT", body=_request_body(), headers={"If-Match": '"etag-1"'})
    )

    assert response.status_code == 503
    assert json.loads(response.get_body()) == {"error": "retrieval_config_unavailable"}


def test_authorization_failure_short_circuits_container_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    denied = function_app._json_response({"error": "operator_role_required"}, 403)
    monkeypatch.setattr(function_app, "_require_ingestion_operator", lambda _req: denied)
    monkeypatch.setattr(
        function_app,
        "_build_retrieval_config_container",
        lambda: pytest.fail("container must not be accessed"),
    )

    response = function_app.manage_retrieval_config(FakeRequest("GET"))

    assert response.status_code == 403
