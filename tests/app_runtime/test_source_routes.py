"""Operator source API: POST/GET /api/ingestion/sources and PATCH /api/ingestion/sources/{sourceId}."""

from __future__ import annotations

import base64
import asyncio
import json
from types import SimpleNamespace
from typing import Any

import azure.functions as func
import httpx
import pytest

import config as config_module
import function_app
from ingestion.source_registry import SourceInventoryPage, SourcePlan, SourceRegistryConflictError
from source_registry import SharePointSource, SourceRegistryError

SITE_ID = "tenant.sharepoint.com,site,web"
VALID_BODY = {
    "sourceId": "poc-pg",
    "siteUrl": "https://tenant.sharepoint.com/sites/POC-PG/",
    "driveId": "drive-pg",
}


def _principal_header(role: str, oid: str | None = None) -> str:
    claims = [{"typ": "tid", "val": "tenant-a"}, {"typ": "roles", "val": role}]
    if oid is not None:
        claims.append({"typ": "http://schemas.microsoft.com/identity/claims/objectidentifier", "val": oid})
    payload = json.dumps({"claims": claims}).encode("utf-8")
    return base64.b64encode(payload).decode("ascii")


def _post(body: Any, role: str | None = "Ingestion.Operator") -> func.HttpResponse:
    headers = {} if role is None else {"X-MS-CLIENT-PRINCIPAL": _principal_header(role)}
    raw = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    request = func.HttpRequest(
        method="POST", url="http://localhost/api/ingestion/sources", headers=headers, body=raw,
    )
    user_code = function_app.register_source._function._func
    return getattr(user_code, "__wrapped__", user_code)(request)


def _get(params: dict[str, str], role: str | None = "Ingestion.Operator") -> func.HttpResponse:
    headers = {} if role is None else {"X-MS-CLIENT-PRINCIPAL": _principal_header(role)}
    request = func.HttpRequest(
        method="GET", url="http://localhost/api/ingestion/sources", headers=headers, params=params, body=b"",
    )
    user_code = function_app.list_sources._function._func
    return getattr(user_code, "__wrapped__", user_code)(request)


def _patch(
    body: Any,
    if_match: str | None = "etag-old",
    role: str | None = "Ingestion.Operator",
    source_id: str = "poc-pg",
    operator: str | None = None,
    oid: str | None = None,
) -> func.HttpResponse:
    headers = {} if role is None else {"X-MS-CLIENT-PRINCIPAL": _principal_header(role, oid)}
    if operator is not None:
        headers["X-MS-CLIENT-PRINCIPAL-NAME"] = operator
    if if_match is not None:
        headers["If-Match"] = if_match
    raw = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    request = func.HttpRequest(
        method="PATCH", url=f"http://localhost/api/ingestion/sources/{source_id}",
        headers=headers, route_params={"sourceId": source_id}, body=raw,
    )
    user_code = function_app.change_source_state._function._func
    return getattr(user_code, "__wrapped__", user_code)(request)


def _reprovision(
    client: Any,
    *,
    body: Any = {"confirm": "reprovision"},
    if_match: str | None = "etag-old",
    role: str | None = "Ingestion.Operator",
    operator: str | None = None,
    oid: str | None = None,
) -> func.HttpResponse:
    headers = {} if role is None else {"X-MS-CLIENT-PRINCIPAL": _principal_header(role, oid)}
    if if_match is not None:
        headers["If-Match"] = if_match
    if operator is not None:
        headers["X-MS-CLIENT-PRINCIPAL-NAME"] = operator
    request = func.HttpRequest(
        method="POST",
        url="http://localhost/api/ingestion/sources/poc-pg/reprovision",
        headers=headers,
        route_params={"sourceId": "poc-pg"},
        body=json.dumps(body).encode("utf-8"),
    )
    user_code = function_app.reprovision_source._function._func
    return asyncio.run(getattr(user_code, "__wrapped__", user_code)(request, client))


def _plan(source_id: str, desired_state: str = "active") -> SourcePlan:
    source = SharePointSource(
        source_id, f"site-{source_id}", f"https://tenant.sharepoint.com/sites/{source_id}",
        f"drive-{source_id}", desired_state,
    )
    return SourcePlan(source, f"etag-{source_id}", "a" * 64)


class _Registry:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[dict[str, str]] = []
        self.list_calls: list[dict[str, Any]] = []
        self.change_calls: list[dict[str, str]] = []
        self.reprovision_calls: list[dict[str, str]] = []

    def change_source_state(self, **kwargs: str) -> SourcePlan:
        self.change_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return _plan(kwargs["source_id"], kwargs["desired_state"])

    def list_source_inventory_page(self, **kwargs: Any) -> SourceInventoryPage:
        self.list_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SourceInventoryPage(
            (_plan("source-a"), _plan("source-b", "paused")), (), 0, (), "next-token",
        )

    def register_sharepoint_source(self, **kwargs: str) -> SourcePlan:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        source = SharePointSource(
            kwargs["source_id"], kwargs["site_id"], kwargs["site_url"].rstrip("/"),
            kwargs["drive_id"], "paused",
        )
        return SourcePlan(source, "etag-1", "a" * 64)

    def inspect_source_reprovision(self, **kwargs: str) -> SourcePlan:
        self.reprovision_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        source = SharePointSource(
            kwargs["source_id"],
            SITE_ID,
            "https://tenant.sharepoint.com/sites/POC-PG",
            "drive-pg",
            "retired",
        )
        return SourcePlan(source, kwargs["if_match"], "a" * 64)

    def get_drive_coordinator(self, _source_id: str) -> None:
        return None


class _DurableClient:
    def __init__(self) -> None:
        self.started: list[dict[str, Any]] = []

    async def start_new(
        self,
        name: str,
        *,
        instance_id: str,
        client_input: dict[str, Any],
    ) -> str:
        self.started.append({
            "name": name,
            "instanceId": instance_id,
            "input": client_input,
        })
        return instance_id

    def create_check_status_response(
        self,
        _request: func.HttpRequest,
        instance_id: str,
    ) -> func.HttpResponse:
        return func.HttpResponse(instance_id, status_code=202)


@pytest.fixture
def graph(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    calls: list[Any] = []

    def validate(_client: Any, drive_id: str, site_url: str, max_pages: int) -> str:
        calls.append((drive_id, site_url, max_pages))
        return SITE_ID

    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", validate)
    return calls


@pytest.fixture
def registry(monkeypatch: pytest.MonkeyPatch) -> _Registry:
    stub = _Registry()
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: stub)
    return stub


class _AuditContainer:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []
        self.error: Exception | None = None

    def create_item(self, body: dict[str, Any]) -> None:
        if self.error is not None:
            raise self.error
        self.items.append(body)


@pytest.fixture(autouse=True)
def audit(monkeypatch: pytest.MonkeyPatch) -> _AuditContainer:
    container = _AuditContainer()
    monkeypatch.setattr(function_app, "_build_audit_container", lambda _config: container)
    return container


@pytest.fixture(autouse=True)
def _environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config_module, "load_config", lambda: SimpleNamespace(tenant_id="tenant-a", acl_max_pages=7),
    )
    monkeypatch.setattr(function_app, "INGESTION_ADMIN_ROLE", "Ingestion.Operator")
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())


def test_register_creates_paused_source_with_graph_site_id(graph: list[Any], registry: _Registry) -> None:
    response = _post(VALID_BODY)

    assert response.status_code == 201
    assert json.loads(response.get_body()) == {
        "sourceId": "poc-pg",
        "siteId": SITE_ID,
        "siteUrl": "https://tenant.sharepoint.com/sites/POC-PG",
        "driveId": "drive-pg",
        "desiredState": "paused",
        "etag": "etag-1",
    }
    assert graph == [("drive-pg", "https://tenant.sharepoint.com/sites/POC-PG", 7)]
    assert registry.calls == [{
        "source_id": "poc-pg",
        "site_id": SITE_ID,
        "site_url": "https://tenant.sharepoint.com/sites/POC-PG",
        "drive_id": "drive-pg",
    }]


def test_reprovision_starts_bounded_orchestration_with_operator_identity(
    graph: list[Any],
    registry: _Registry,
) -> None:
    client = _DurableClient()
    oid = "11111111-1111-1111-1111-111111111111"

    response = _reprovision(
        client,
        operator="operator@example.com",
        oid=oid,
    )

    assert response.status_code == 202
    assert len(client.started) == 1
    started = client.started[0]
    assert started["name"] == "source_reprovision_orchestrator"
    assert started["instanceId"].startswith("source-reprovision-")
    assert started["input"]["sourcePlan"]["desiredState"] == "retired"
    assert started["input"]["operatorObjectId"] == oid
    assert started["input"]["operator"] == "operator@example.com"


def test_reprovision_execution_requires_immutable_operator_identity(
    graph: list[Any],
    registry: _Registry,
) -> None:
    response = _reprovision(_DurableClient())

    assert response.status_code == 403
    assert json.loads(response.get_body()) == {"error": "operator_object_id_required"}
    assert len(graph) == 1
    assert len(registry.reprovision_calls) == 1


@pytest.mark.parametrize(
    ("body", "if_match", "role", "status", "error"),
    [
        ({}, "etag-old", "Ingestion.Operator", 400, "source_reprovision_confirmation_required"),
        ({"confirm": "reprovision"}, None, "Ingestion.Operator", 428, "if_match_required"),
        ({"confirm": "reprovision"}, "etag-old", None, 403, "operator_role_required"),
    ],
)
def test_reprovision_rejects_unsafe_requests(
    body: Any,
    if_match: str | None,
    role: str | None,
    status: int,
    error: str,
    graph: list[Any],
    registry: _Registry,
) -> None:
    response = _reprovision(
        _DurableClient(),
        body=body,
        if_match=if_match,
        role=role,
    )

    assert response.status_code == status
    assert json.loads(response.get_body()) == {"error": error}
    assert graph == []
    assert registry.reprovision_calls == []


@pytest.mark.parametrize("role", [None, "Reader"])
def test_register_requires_operator_role(role: str | None, graph: list[Any], registry: _Registry) -> None:
    response = _post(VALID_BODY, role=role)

    assert response.status_code == 403
    assert graph == [] and registry.calls == []


@pytest.mark.parametrize("body", [
    b"not-json",
    [],
    {"sourceId": "poc-pg", "siteUrl": VALID_BODY["siteUrl"]},
    VALID_BODY | {"desiredState": "active"},
    VALID_BODY | {"driveId": ""},
    VALID_BODY | {"driveId": 5},
    VALID_BODY | {"sourceId": "a/b"},
    VALID_BODY | {"siteUrl": "http://tenant.sharepoint.com/sites/POC-PG"},
    VALID_BODY | {"siteUrl": "https://tenant.sharepoint.com:bad/sites/POC-PG"},
    VALID_BODY | {"siteUrl": "https://[tenant/sites/POC-PG"},
])
def test_register_rejects_invalid_input(body: Any, graph: list[Any], registry: _Registry) -> None:
    response = _post(body)

    assert response.status_code == 400
    assert graph == [] and registry.calls == []


def _graph_error(monkeypatch: pytest.MonkeyPatch, error: Exception) -> None:
    def validate(*_args: Any) -> str:
        raise error

    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", validate)


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://graph.microsoft.com/v1.0/sites/x")
    return httpx.HTTPStatusError(
        "graph error", request=request, response=httpx.Response(status_code, request=request),
    )


@pytest.mark.parametrize("error", [
    ValueError("Configured SharePoint site does not own the configured drive"),
    _status_error(403),
    _status_error(404),
])
def test_register_returns_422_when_graph_rejects_site_or_drive(
    monkeypatch: pytest.MonkeyPatch, error: Exception, registry: _Registry,
) -> None:
    _graph_error(monkeypatch, error)

    response = _post(VALID_BODY)

    assert response.status_code == 422
    assert json.loads(response.get_body()) == {"error": "source_validation_failed"}
    assert registry.calls == []


@pytest.mark.parametrize("error", [
    _status_error(429),
    _status_error(503),
    httpx.ConnectError("unreachable"),
])
def test_register_returns_503_when_graph_unavailable(
    monkeypatch: pytest.MonkeyPatch, error: Exception, registry: _Registry,
) -> None:
    _graph_error(monkeypatch, error)

    response = _post(VALID_BODY)

    assert response.status_code == 503
    assert json.loads(response.get_body()) == {"error": "graph_unavailable"}
    assert registry.calls == []


@pytest.mark.parametrize("code", ["source_id_conflict", "sharepoint_drive_reservation_conflict"])
def test_register_returns_409_on_duplicate_source_or_drive(
    code: str, graph: list[Any], registry: _Registry,
) -> None:
    registry.error = SourceRegistryConflictError(code)

    response = _post(VALID_BODY)

    assert response.status_code == 409
    assert json.loads(response.get_body()) == {"error": code}


def test_register_returns_503_when_cosmos_unavailable(graph: list[Any], registry: _Registry) -> None:
    registry.error = RuntimeError("cosmos unavailable")

    response = _post(VALID_BODY)

    assert response.status_code == 503
    assert json.loads(response.get_body()) == {"error": "source_registry_unavailable"}


def test_list_returns_bounded_page_of_sources(monkeypatch: pytest.MonkeyPatch, registry: _Registry) -> None:
    monkeypatch.setattr(function_app, "SOURCE_DISPATCH_PAGE_SIZE", 2)

    response = _get({})

    assert response.status_code == 200
    assert json.loads(response.get_body()) == {
        "sources": [
            {
                "sourceId": "source-a",
                "siteUrl": "https://tenant.sharepoint.com/sites/source-a",
                "driveId": "drive-source-a",
                "desiredState": "active",
                "etag": "etag-source-a",
            },
            {
                "sourceId": "source-b",
                "siteUrl": "https://tenant.sharepoint.com/sites/source-b",
                "driveId": "drive-source-b",
                "desiredState": "paused",
                "etag": "etag-source-b",
            },
        ],
        "continuationToken": "next-token",
    }
    assert registry.list_calls == [{"page_size": 2, "continuation_token": None}]


def test_list_forwards_continuation_token(registry: _Registry) -> None:
    response = _get({"continuationToken": "page-2"})

    assert response.status_code == 200
    assert registry.list_calls[0]["continuation_token"] == "page-2"


@pytest.mark.parametrize("role", [None, "Reader"])
def test_list_requires_operator_role(role: str | None, registry: _Registry) -> None:
    response = _get({}, role=role)

    assert response.status_code == 403
    assert registry.list_calls == []


def test_list_returns_503_when_cosmos_unavailable(registry: _Registry) -> None:
    registry.error = RuntimeError("cosmos unavailable")

    response = _get({})

    assert response.status_code == 503
    assert json.loads(response.get_body()) == {"error": "source_registry_unavailable"}


def test_patch_changes_state_with_if_match(registry: _Registry) -> None:
    response = _patch({"desiredState": "active"})

    assert response.status_code == 200
    assert json.loads(response.get_body()) == {
        "sourceId": "poc-pg",
        "siteUrl": "https://tenant.sharepoint.com/sites/poc-pg",
        "driveId": "drive-poc-pg",
        "desiredState": "active",
        "etag": "etag-poc-pg",
    }
    assert registry.change_calls == [
        {"source_id": "poc-pg", "desired_state": "active", "if_match": "etag-old"},
    ]


def test_patch_audits_operator_and_resulting_state(registry: _Registry, audit: _AuditContainer) -> None:
    oid = "11111111-2222-3333-4444-555555555555"
    response = _patch({"desiredState": "retired"}, operator="operator@contoso.com", oid=oid)

    assert response.status_code == 200
    assert len(audit.items) == 1
    item = audit.items[0]
    assert {
        key: item[key]
        for key in ("sourceId", "runId", "operation", "desiredState", "catalogEtag", "operatorObjectId", "operator")
    } == {
        "sourceId": "poc-pg",
        "runId": "source-state",
        "operation": "source_state_changed",
        "desiredState": "retired",
        "catalogEtag": "etag-poc-pg",
        "operatorObjectId": oid,
        "operator": "operator@contoso.com",
    }
    assert item["recordedAt"]


@pytest.mark.parametrize("oid", [None, "not-a-guid", "11111111-2222-3333-4444-55555555555A"])
def test_patch_audit_records_null_object_id_when_oid_is_missing_or_invalid(
    oid: str | None, registry: _Registry, audit: _AuditContainer,
) -> None:
    assert _patch({"desiredState": "paused"}, oid=oid).status_code == 200
    assert audit.items[0]["operatorObjectId"] is None
    assert audit.items[0]["operator"] == "unknown"


def test_patch_audit_failure_does_not_fail_committed_change(
    monkeypatch: pytest.MonkeyPatch, registry: _Registry, audit: _AuditContainer,
) -> None:
    audit.error = RuntimeError("audit unavailable")

    assert _patch({"desiredState": "paused"}).status_code == 200

    def unavailable(_config: Any) -> Any:
        raise RuntimeError("cosmos unavailable")

    monkeypatch.setattr(function_app, "_build_audit_container", unavailable)

    assert _patch({"desiredState": "paused"}).status_code == 200
    assert audit.items == []


def test_patch_requires_if_match(registry: _Registry) -> None:
    response = _patch({"desiredState": "active"}, if_match=None)

    assert response.status_code == 428
    assert json.loads(response.get_body()) == {"error": "if_match_required"}
    assert registry.change_calls == []


@pytest.mark.parametrize(
    "body",
    [
        b"not-json",
        [],
        {},
        {"desiredState": "deleted"},
        {"desiredState": "active", "driveId": "drive-other"},
        {"desiredState": 1},
        {"desiredState": ["active"]},
        {"desiredState": {"active": True}},
    ],
)
def test_patch_rejects_invalid_body(body: Any, registry: _Registry) -> None:
    response = _patch(body)

    assert response.status_code == 400
    assert json.loads(response.get_body()) == {"error": "source_state_invalid"}
    assert registry.change_calls == []


def test_patch_rejects_invalid_source_id(registry: _Registry) -> None:
    response = _patch({"desiredState": "active"}, source_id="x" * 1024)

    assert response.status_code == 400
    assert json.loads(response.get_body()) == {"error": "source_sourceId_invalid"}
    assert registry.change_calls == []


@pytest.mark.parametrize("role", [None, "Reader"])
def test_patch_requires_operator_role(role: str | None, registry: _Registry) -> None:
    response = _patch({"desiredState": "active"}, role=role)

    assert response.status_code == 403
    assert registry.change_calls == []


@pytest.mark.parametrize(
    ("error", "status", "code"),
    [
        (SourceRegistryError("source_registration_missing"), 404, "source_registration_missing"),
        (SourceRegistryConflictError("source_etag_mismatch"), 412, "source_etag_mismatch"),
        (
            SourceRegistryConflictError("retired_source_cannot_be_reactivated"),
            409,
            "retired_source_cannot_be_reactivated",
        ),
        (SourceRegistryError("source_lifecycle_pending"), 503, "source_lifecycle_pending"),
        (SourceRegistryError("source_binding_not_accepted"), 409, "source_binding_not_accepted"),
        (RuntimeError("cosmos unavailable"), 503, "source_registry_unavailable"),
    ],
)
def test_patch_maps_registry_errors(
    error: Exception, status: int, code: str, registry: _Registry, audit: _AuditContainer,
) -> None:
    registry.error = error

    response = _patch({"desiredState": "active"})

    assert response.status_code == status
    assert json.loads(response.get_body()) == {"error": code}
    assert audit.items == []
