"""Regression test for function_app.inspect_data: a cross-partition scan of service-audit
(whose partition key is each record's own id, so there's no natural partition filter) must
order by recordedAt DESC to surface recent activity instead of an arbitrary sample. Other
containers are unaffected.

Monkeypatches azure.cosmos.CosmosClient / azure.identity.DefaultAzureCredential (the SDK
classes inspect_data imports locally). Production code is never modified by this test.
"""

from __future__ import annotations

from typing import Any

import azure.cosmos
import azure.identity
import pytest

import function_app


class FakeContainer:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.parameters: list[list[dict] | None] = []
        self.partition_keys: list[Any] = []

    def query_items(self, query: str, parameters: list[dict] | None = None, **kwargs: Any) -> list[dict]:
        self.queries.append(query)
        self.parameters.append(parameters)
        self.partition_keys.append(kwargs.get("partition_key"))
        return []


class FakeDatabase:
    def __init__(self, container: FakeContainer) -> None:
        self._container = container

    def get_container_client(self, name: str) -> FakeContainer:
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
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", lambda **kwargs: object())
    monkeypatch.setattr(function_app, "_require_ingestion_operator", lambda _req: None)


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
    request = FakeRequest({"container": "source-documents", "limit": "50"})

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert len(container.queries) == 1
    assert "ORDER BY" not in container.queries[0]


def test_inspect_data_run_filter_uses_explicit_source_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({
        "container": "source-documents",
        "sourceId": "source-1",
        "runId": "run-1",
        "limit": "50",
    })

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert container.partition_keys == ["source-1:run-1"]
    assert "ORDER BY" not in container.queries[0]


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
    request = FakeRequest({
        "container": "search-chunks",
        "documentKey": "source-1:run-1:doc-1",
    })

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert container.partition_keys == ["source-1:run-1:doc-1"]


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


def test_inspect_data_catalog_uses_deployment_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "retrieval-config", "deploymentInstanceId": "deploy-1"})

    response = function_app.inspect_data(request)

    assert response.status_code == 200
    assert container.partition_keys == ["deploy-1"]
    assert "ORDER BY" not in container.queries[0]


def test_inspect_data_catalog_requires_deployment_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "retrieval-config"})

    response = function_app.inspect_data(request)

    assert response.status_code == 400
    assert container.queries == []


def test_inspect_data_catalog_rejects_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    container = FakeContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(container))
    request = FakeRequest({"container": "retrieval-config", "deploymentInstanceId": "deploy-1", "runId": "run-1"})

    response = function_app.inspect_data(request)

    assert response.status_code == 400
    assert container.queries == []

