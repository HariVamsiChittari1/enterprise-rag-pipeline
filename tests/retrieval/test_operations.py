from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosResourceNotFoundError

import retrieval.operations as operations
from retrieval.catalog import CatalogConflictError, CatalogError, build_catalog_item, load_catalog_item


class FakeContainer:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.create_calls = 0

    def create_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        self.create_calls += 1
        key = (body["deploymentInstanceId"], body["id"])
        if key in self.items:
            raise CosmosResourceExistsError(status_code=409, message="exists")
        stored = {**body, "_etag": f"etag-{len(self.items) + 1}"}
        self.items[key] = stored
        return dict(stored)

    def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
        try:
            return dict(self.items[(partition_key, item)])
        except KeyError:
            raise CosmosResourceNotFoundError(
                status_code=404, message="missing"
            ) from None


class FakeCosmos:
    def __init__(self, container: FakeContainer) -> None:
        self.container = container
        self.closed = False

    def get_database_client(self, name: str) -> "FakeCosmos":
        return self

    def get_container_client(self, name: str) -> FakeContainer:
        return self.container

    def close(self) -> None:
        self.closed = True


class FakeCredential:
    def __init__(self, *, client_id: str) -> None:
        self.client_id = client_id
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _source() -> dict[str, Any]:
    return {
        "config": {
            "retrieval": {
                "overFetchFactor": 3,
                "hybridWeights": {"vector": 2, "text": 1},
                "fullTextScoreScope": "Global",
            },
            "defaultProfile": "default",
            "profiles": [{
                "name": "default",
                "textWeights": {"content": 1},
                "functionAggregation": "sum",
                "functions": [],
            }],
            "synonymMaps": [],
        },
    }


def _configure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, container: FakeContainer,
) -> tuple[FakeCosmos, list[FakeCredential]]:
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(_source()), encoding="utf-8")
    digest = load_catalog_item(build_catalog_item(_source(), "instance-a")).digest
    values = {
        "COSMOS_ENDPOINT": "https://cosmos.example",
        "COSMOS_DATABASE": "rag-db",
        "RETRIEVAL_CONFIG_CONTAINER": "retrieval-config",
        "DEPLOYMENT_INSTANCE_ID": "instance-a",
        "EXPECTED_CATALOG_DIGEST": digest,
        "MANAGED_IDENTITY_CLIENT_ID": "identity-client",
        "CATALOG_PATH": str(path),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    cosmos = FakeCosmos(container)
    credentials: list[FakeCredential] = []
    monkeypatch.setattr(operations, "CosmosClient", lambda *_a, **_k: cosmos)

    def _credential(**kwargs: Any) -> FakeCredential:
        credential = FakeCredential(**kwargs)
        credentials.append(credential)
        return credential

    monkeypatch.setattr(operations, "ManagedIdentityCredential", _credential)
    return cosmos, credentials


def test_private_runner_publishes_and_replays_same_catalog(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    container = FakeContainer()
    cosmos, credentials = _configure(monkeypatch, tmp_path, container)

    first = operations.publish_bootstrap_catalog()
    second = operations.publish_bootstrap_catalog()

    assert first == second
    assert first["catalogDigest"].startswith("sha256:")
    assert first["catalogEtag"]
    assert set(container.items) == {("instance-a", "runtime-catalog")}
    assert cosmos.closed is True
    assert all(credential.closed for credential in credentials)


def test_private_runner_rejects_reviewed_digest_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    container = FakeContainer()
    _configure(monkeypatch, tmp_path, container)
    monkeypatch.setenv("EXPECTED_CATALOG_DIGEST", "sha256:" + "a" * 64)

    with pytest.raises(
        operations.OperationsError, match="reviewed artifact"
    ):
        operations.publish_bootstrap_catalog()

    assert container.items == {}


def test_private_runner_refuses_different_existing_catalog(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    container = FakeContainer()
    _configure(monkeypatch, tmp_path, container)
    changed = _source()
    changed["config"]["retrieval"]["overFetchFactor"] = 4
    existing = build_catalog_item(changed, "instance-a")
    existing["_etag"] = "etag-existing"
    container.items[("instance-a", "runtime-catalog")] = existing

    with pytest.raises(CatalogConflictError, match="explicit disposition"):
        operations.publish_bootstrap_catalog()

    assert container.items[("instance-a", "runtime-catalog")] == existing


def test_verify_preserves_client_edit_without_seed_or_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    container = FakeContainer()
    cosmos, credentials = _configure(monkeypatch, tmp_path, container)
    changed = _source()
    changed["config"]["retrieval"]["overFetchFactor"] = 4
    existing = {**build_catalog_item(changed, "instance-a"), "_etag": "client-edit"}
    container.items[("instance-a", "runtime-catalog")] = existing
    monkeypatch.delenv("EXPECTED_CATALOG_DIGEST")
    monkeypatch.setenv("CATALOG_PATH", str(tmp_path / "missing.json"))

    assert operations.main(["verify-catalog"]) == 0

    result = json.loads(capsys.readouterr().out)
    assert result == {
        "status": "succeeded", "operation": "verify-catalog",
        "catalogId": "runtime-catalog", "catalogEtag": "client-edit",
        "catalogDigest": load_catalog_item(existing).digest,
    }
    assert container.create_calls == 0
    assert container.items[("instance-a", "runtime-catalog")] == existing
    assert cosmos.closed and all(credential.closed for credential in credentials)


@pytest.mark.parametrize("operation", ["publish-catalog", "verify-catalog"])
@pytest.mark.parametrize("corruption", ["partition", "etag", "config"])
def test_invalid_readback_fails_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, operation: str, corruption: str,
) -> None:
    container = FakeContainer()
    cosmos, credentials = _configure(monkeypatch, tmp_path, container)
    existing = {**build_catalog_item(_source(), "instance-a"), "_etag": "existing"}
    if corruption == "partition":
        existing["deploymentInstanceId"] = "other-instance"
    elif corruption == "etag":
        del existing["_etag"]
    else:
        existing["config"]["retrieval"]["overFetchFactor"] = 0
    container.items[("instance-a", "runtime-catalog")] = existing

    assert operations.main([operation]) == 2

    assert container.items[("instance-a", "runtime-catalog")] == existing
    assert cosmos.closed and all(credential.closed for credential in credentials)


def test_missing_current_item_is_not_initialized(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    container = FakeContainer()
    cosmos, credentials = _configure(monkeypatch, tmp_path, container)

    with pytest.raises(CatalogError, match="missing"):
        operations.verify_current_catalog()

    assert container.create_calls == 0 and container.items == {}
    assert cosmos.closed and all(credential.closed for credential in credentials)


def test_client_construction_failure_closes_credential(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    container = FakeContainer()
    _, credentials = _configure(monkeypatch, tmp_path, container)

    def fail_client(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("sensitive dependency detail")

    monkeypatch.setattr(operations, "CosmosClient", fail_client)

    with pytest.raises(operations.OperationsError, match="private catalog operation failed"):
        operations.verify_current_catalog()

    assert len(credentials) == 1 and credentials[0].closed
    assert container.create_calls == 0


@pytest.mark.parametrize("operation", ["publish-catalog", "verify-catalog"])
def test_denied_read_is_sanitized_and_resources_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    capsys: pytest.CaptureFixture[str], operation: str,
) -> None:
    container = FakeContainer()
    cosmos, credentials = _configure(monkeypatch, tmp_path, container)

    def deny_read(**kwargs: Any) -> None:
        raise PermissionError("sensitive dependency detail")

    monkeypatch.setattr(container, "read_item", deny_read)

    assert operations.main([operation]) == 2

    assert "sensitive dependency detail" not in capsys.readouterr().out
    assert cosmos.closed and all(credential.closed for credential in credentials)


def test_interrupted_create_can_be_replayed_without_overwrite(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    container = FakeContainer()
    cosmos, credentials = _configure(monkeypatch, tmp_path, container)
    create = container.create_item

    def interrupted_create(**kwargs: Any) -> None:
        create(**kwargs)
        raise TimeoutError("unknown write outcome")

    monkeypatch.setattr(container, "create_item", interrupted_create)
    with pytest.raises(CatalogError, match="create failed"):
        operations.publish_bootstrap_catalog()
    existing = dict(container.items[("instance-a", "runtime-catalog")])
    monkeypatch.setattr(container, "create_item", create)

    result = operations.publish_bootstrap_catalog()

    assert result["catalogEtag"] == existing["_etag"]
    assert container.items[("instance-a", "runtime-catalog")] == existing
    assert cosmos.closed and all(credential.closed for credential in credentials)
