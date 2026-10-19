"""Regression test for function_app.inspect_data: a cross-partition scan of service-audit
(whose partition key is each record's own id, so there's no natural partition filter) must
order by recordedAt DESC to surface recent activity instead of an arbitrary sample. Other
containers are unaffected.

Monkeypatches the Cosmos client and shared credential factory.
Production code is never modified by this test.
"""

from __future__ import annotations

from typing import Any

import azure.cosmos
import config
import cosmos_connection
import function_app
import pytest


class FakeContainer:
    def __init__(self) -> None:
        self.container_names: list[str] = []
        self.queries: list[str] = []
        self.parameters: list[list[dict] | None] = []
        self.partition_keys: list[Any] = []
        self.cross_partition: list[bool | None] = []

    def query_items(self, query: str, parameters: list[dict] | None = None, **kwargs: Any) -> list[dict]:
        self.queries.append(query)
        self.parameters.append(parameters)
        self.partition_keys.append(kwargs.get("partition_key"))
        self.cross_partition.append(kwargs.get("enable_cross_partition_query"))
        return []


class FakeDatabase:
    def __init__(self, container: FakeContainer) -> None:
        self._container = container

    def get_container_client(self, name: str) -> FakeContainer:
        self._container.container_names.append(name)
        return self._container


def _fake_cosmos_client_factory(container: FakeContainer):
    class FakeCosmosClient:
        def __init__(self, endpoint: str, credential: Any) -> None:
            self._db = FakeDatabase(container)

        def get_database_client(self, name: str) -> FakeDatabase:
            return self._db

    return FakeCosmosClient


class FakeRequest:
    def __init__(self, params: dict[str, str]) -> None:
        self.params = params


@pytest.fixture(autouse=True)
def patch_azure_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cosmos_connection,
        "create_azure_credential",
        lambda *_args: object(),
    )
    monkeypatch.setattr(function_app, "_require_ingestion_operator", lambda _req: None)


def _register_source(
    monkeypatch: pytest.MonkeyPatch,
    *,
    source_id: str = "source-1",
    manifests_container: str = "source-1-documents",
    chunks_container: str = "source-1-chunks",
) -> None:
    class Registry:
        def read_source_plan(self, requested_source_id: str) -> Any:
            assert requested_source_id == source_id
            source = type("Source", (), {
                "manifests_container": manifests_container,
                "chunks_container": chunks_container,
            })()
            return type("Plan", (), {"source": source})()

    monkeypatch.setattr(config, "load_config", lambda: object())
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())


def test_inspect_data_orders_service_audit_scan_by_recorded_at_desc(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "service-audit", "limit": "50"})

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert len(container.queries) == 1
    assert "ORDER BY c.recordedAt DESC" in container.queries[0]


def test_inspect_data_does_not_order_other_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    _register_source(monkeypatch)
    request = FakeRequest({
        "container": "source-documents",
        "sourceId": "source-1",
        "limit": "50",
    })

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert container.container_names == ["source-1-documents"]
    assert len(container.queries) == 1
    assert "ORDER BY" not in container.queries[0]
    assert "c.sourceId = @sourceId" in container.queries[0]


def test_inspect_data_run_filter_uses_explicit_source_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    _register_source(monkeypatch)
    request = FakeRequest({
        "container": "source-documents",
        "sourceId": "source-1",
        "runId": "run-1",
        "limit": "50",
    })

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert container.container_names == ["source-1-documents"]
    assert container.partition_keys == ["source-1:run-1"]
    assert "ORDER BY" not in container.queries[0]
    assert container.parameters[0] == [
        {"name": "@limit", "value": 50},
        {"name": "@sourceId", "value": "source-1"},
    ]


def test_inspect_data_run_filter_requires_source_id(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "source-documents", "runId": "run-1"})

    response = function_app.inspect_data(request)

    assert response.status_code == 400
    assert container.queries == []


def test_inspect_data_filters_chunks_by_document_key(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    _register_source(monkeypatch)
    request = FakeRequest({
        "container": "search-chunks",
        "sourceId": "source-1",
        "documentKey": "source-1:run-1:doc-1",
    })

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert container.container_names == ["source-1-chunks"]
    assert container.partition_keys == ["source-1:run-1:doc-1"]
    assert "c.sourceId = @sourceId" in container.queries[0]


def test_inspect_data_rejects_document_key_for_other_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({
        "container": "source-documents",
        "documentKey": "source-1:run-1:doc-1",
    })

    response = function_app.inspect_data(request)

    assert response.status_code == 400
    assert container.queries == []


@pytest.mark.parametrize("container_name", ["source-documents", "search-chunks"])
def test_inspect_data_requires_source_for_registered_containers(
    monkeypatch: pytest.MonkeyPatch,
    container_name: str,
) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))

    response = function_app.inspect_data(FakeRequest({"container": container_name}))

    assert response.status_code == 400
    assert container.container_names == []
    assert container.queries == []


def test_inspect_data_preserves_exact_registered_source_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    _register_source(monkeypatch, source_id=" source-1 ")

    response = function_app.inspect_data(FakeRequest({
        "container": "source-documents",
        "sourceId": " source-1 ",
    }))

    assert response.status_code == 200
    assert container.parameters[0][-1] == {
        "name": "@sourceId",
        "value": " source-1 ",
    }


@pytest.mark.parametrize("limit", ["invalid", "0", "-1"])
def test_inspect_data_rejects_invalid_limit(
    monkeypatch: pytest.MonkeyPatch,
    limit: str,
) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))

    response = function_app.inspect_data(FakeRequest({
        "container": "service-audit",
        "limit": limit,
    }))

    assert response.status_code == 400
    assert container.container_names == []
    assert container.queries == []


def test_inspect_data_filters_unresolved_work_by_source_and_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({
        "container": "ingestion-runs",
        "sourceId": "source-1",
        "ownerInstanceId": "instance-1",
        "unresolvedOnly": "true",
        "limit": "50",
    })

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert container.partition_keys == ["source-1"]
    assert "c.recordType = @recordType" in container.queries[0]
    assert "c.ownerInstanceId = @owner" in container.queries[0]
    assert "ARRAY_CONTAINS(@states, c.state)" in container.queries[0]
    assert container.parameters[0] == [
        {"name": "@limit", "value": 50},
        {"name": "@recordType", "value": "document-operation"},
        {"name": "@owner", "value": "instance-1"},
        {"name": "@states", "value": ["claimed", "running", "unresolved"]},
    ]


def test_inspect_data_rejects_partial_recovery_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "ingestion-runs", "sourceId": "source-1"})

    response = function_app.inspect_data(request)

    assert response.status_code == 400
    assert container.queries == []


def test_inspect_data_catalog_uses_environment_local_cross_partition_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "retrieval-config"})

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert container.partition_keys == [None]
    assert container.cross_partition == [True]
    assert "ORDER BY" not in container.queries[0]


def test_inspect_data_catalog_rejects_caller_supplied_deployment_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "retrieval-config", "deploymentInstanceId": "deploy-1"})

    response = function_app.inspect_data(request)

    assert response.status_code == 400
    assert container.queries == []


def test_inspect_data_catalog_rejects_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "retrieval-config", "runId": "run-1"})

    response = function_app.inspect_data(request)

    assert response.status_code == 400
    assert container.queries == []
