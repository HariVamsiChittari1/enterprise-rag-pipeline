"""Regression test for function_app.purge_data: a failing audit write must never mask
an already-completed purge as a failure to the caller.

Monkeypatches the Cosmos client and shared credential factory, with a fake
service-audit container whose
create_item always raises, simulating a transient Cosmos write failure during audit
logging. Production code is never modified by this test.
"""

from __future__ import annotations

import json
import base64
from typing import Any
from types import SimpleNamespace

import azure.cosmos
import cosmos_connection
import pytest

import config as config_module
import function_app


class FakeSourceDocumentsContainer:
    def __init__(self) -> None:
        self.deleted: list[str] = []
        self.deleted_partition_keys: list[str] = []

    def query_items(self, query: str, parameters: list[dict] | None = None, enable_cross_partition_query: bool = False):
        item_id = next((p["value"] for p in (parameters or []) if p["name"] == "@id"), None)
        if item_id is None:
            return []
        return [{"id": item_id, "pk": "run-1", "sourceId": "source-a"}]

    def delete_item(self, item: str, partition_key: str) -> None:
        self.deleted.append(item)
        self.deleted_partition_keys.append(partition_key)


class FailingAuditContainer:
    def create_item(self, item: dict) -> None:
        raise RuntimeError("simulated transient Cosmos write failure")


class FakeDatabase:
    def __init__(self) -> None:
        self.source_documents = FakeSourceDocumentsContainer()
        self.audit = FailingAuditContainer()
        self.requested_containers: list[str] = []

    def get_container_client(self, name: str) -> Any:
        self.requested_containers.append(name)
        return self.audit if name == "service-audit" else self.source_documents


class FakeCosmosClient:
    def __init__(self, endpoint: str, credential: Any) -> None:
        self.db = FakeDatabase()

    def get_database_client(self, name: str) -> FakeDatabase:
        return self.db


def _fake_cosmos_client_factory(database: FakeDatabase):
    class SharedFakeCosmosClient:
        def __init__(self, endpoint: str, credential: Any) -> None:
            self._db = database

        def get_database_client(self, name: str) -> FakeDatabase:
            return self._db

    return SharedFakeCosmosClient


class FakeRequest:
    def __init__(self, body: dict) -> None:
        self._body = body
        self.headers: dict[str, str] = {}

    def get_json(self) -> dict:
        return self._body


@pytest.fixture(autouse=True)
def patch_cosmos_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(azure.cosmos, "CosmosClient", FakeCosmosClient)
    monkeypatch.setattr(
        cosmos_connection,
        "create_azure_credential",
        lambda *_args: object(),
    )


def _principal_header(role: str) -> str:
    payload = json.dumps({"claims": [
        {"typ": "tid", "val": "tenant-a"},
        {"typ": "roles", "val": role},
    ]}).encode("utf-8")
    return base64.b64encode(payload).decode("ascii")


def test_operator_role_guard_requires_tenant_scoped_app_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace(tenant_id="tenant-a"))
    monkeypatch.setattr(function_app, "INGESTION_ADMIN_ROLE", "Ingestion.Operator")

    assert function_app._require_ingestion_operator(FakeRequest({})).status_code == 403
    denied = FakeRequest({})
    denied.headers["X-MS-CLIENT-PRINCIPAL"] = _principal_header("Reader")
    assert function_app._require_ingestion_operator(denied).status_code == 403
    allowed = FakeRequest({})
    allowed.headers["X-MS-CLIENT-PRINCIPAL"] = _principal_header("Ingestion.Operator")
    assert function_app._require_ingestion_operator(allowed) is None


def test_purge_route_rejects_missing_operator_role_before_cosmos_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace(tenant_id="tenant-a"))
    monkeypatch.setattr(
        azure.cosmos,
        "CosmosClient",
        lambda *_args, **_kwargs: pytest.fail("unauthorized purge must not access Cosmos"),
    )
    request = FakeRequest({
        "container": "source-documents",
        "sourceId": "source-a",
        "ids": ["doc-1"],
        "confirm": "yes",
    })
    request.headers["X-MS-CLIENT-PRINCIPAL"] = _principal_header("Reader")

    response = function_app.purge_data(request)

    assert response.status_code == 403
    assert json.loads(response.get_body()) == {"error": "operator_role_required"}


def test_purge_data_succeeds_even_when_audit_write_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = _FakePurgeRegistry()
    config = SimpleNamespace(
        cosmos_search_chunks_container="default-search-chunks",
        cosmos_source_documents_container="default-source-documents",
    )
    monkeypatch.setattr(config_module, "load_config", lambda: config)
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: registry)
    monkeypatch.setattr(function_app, "_require_ingestion_operator", lambda _req: None)
    request = FakeRequest({
        "container": "source-documents", "sourceId": "source-a",
        "ids": ["doc-1"], "confirm": "yes",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 200
    body = json.loads(response.get_body())
    assert body["deleted"] == 1
    assert body["failed"] == 0
    assert "auditId" in body
    assert registry.events == ["begin", "settle"]


class _FakePurgeRegistry:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.plan = type("Plan", (), {
            "source": type("Source", (), {
                "source_id": "source-a",
                "site_id": "site-a",
                "site_url": "https://tenant.sharepoint.com/sites/a",
                "drive_id": "drive-a",
                "desired_state": "active",
                "business_name": "Business A",
                "chunks_container": "business-a-search-chunks",
                "manifests_container": "business-a-source-documents",
            })(),
        })()

    def read_source_plan(self, source_id: str):
        assert source_id == "source-a"
        return self.plan

    def begin_operator_maintenance(self, plan, *, owner_instance_id: str, operation: str):
        assert plan is self.plan
        assert owner_instance_id.startswith("operator-purge-")
        assert operation == "operator-purge"
        self.events.append("begin")

    def settle_drive_coordinator(self, source_id: str, owner_instance_id: str):
        assert source_id == "source-a"
        assert owner_instance_id.startswith("operator-purge-")
        self.events.append("settle")


def test_purge_data_catalog_deletes_with_item_id_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    database = FakeDatabase()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(database))
    monkeypatch.setattr(function_app, "_require_ingestion_operator", lambda _req: None)
    request = FakeRequest({
        "container": "retrieval-config",
        "ids": ["runtime-catalog"],
        "confirm": "yes",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 200
    body = json.loads(response.get_body())
    assert body["deleted"] == 1
    assert body["failed"] == 0
    assert database.source_documents.deleted == ["runtime-catalog"]
    assert database.source_documents.deleted_partition_keys == ["runtime-catalog"]


@pytest.mark.parametrize(
    ("requested_role", "expected_container"),
    [
        ("source-documents", "business-a-source-documents"),
        ("search-chunks", "business-a-search-chunks"),
    ],
)
def test_ingestion_purge_uses_registered_source_container_pair(
    monkeypatch: pytest.MonkeyPatch,
    requested_role: str,
    expected_container: str,
) -> None:
    database = FakeDatabase()
    database.audit = _RecordingAuditContainer()
    config = SimpleNamespace(
        cosmos_search_chunks_container="default-search-chunks",
        cosmos_source_documents_container="default-source-documents",
    )
    monkeypatch.setattr(
        azure.cosmos,
        "CosmosClient",
        _fake_cosmos_client_factory(database),
    )
    monkeypatch.setattr(config_module, "load_config", lambda: config)
    monkeypatch.setattr(
        function_app,
        "_build_source_registry_repository",
        lambda _config: _FakePurgeRegistry(),
    )
    monkeypatch.setattr(
        function_app,
        "_require_ingestion_operator",
        lambda _req: None,
    )

    response = function_app.purge_data(FakeRequest({
        "container": requested_role,
        "sourceId": "source-a",
        "ids": ["doc-1"],
        "confirm": "yes",
    }))

    assert response.status_code == 200
    assert expected_container in database.requested_containers
    assert requested_role not in database.requested_containers
    assert database.audit.items[0]["businessName"] == "Business A"


class _RecordingAuditContainer:
    def __init__(self) -> None:
        self.items: list[dict] = []

    def create_item(self, item: dict) -> None:
        self.items.append(item)


def test_purge_audit_records_operator_object_id_and_name(monkeypatch: pytest.MonkeyPatch) -> None:
    database = FakeDatabase()
    database.audit = _RecordingAuditContainer()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(database))
    monkeypatch.setattr(function_app, "_require_ingestion_operator", lambda _req: None)
    oid = "11111111-2222-3333-4444-555555555555"
    request = FakeRequest({
        "container": "retrieval-config",
        "ids": ["runtime-catalog"],
        "confirm": "yes",
    })
    principal = json.dumps({"claims": [
        {"typ": "http://schemas.microsoft.com/identity/claims/objectidentifier", "val": oid},
    ]}).encode("utf-8")
    request.headers["X-MS-CLIENT-PRINCIPAL"] = base64.b64encode(principal).decode("ascii")
    request.headers["X-MS-CLIENT-PRINCIPAL-NAME"] = "operator@contoso.com"

    assert function_app.purge_data(request).status_code == 200
    assert len(database.audit.items) == 1
    assert database.audit.items[0]["operation"] == "purge"
    assert database.audit.items[0]["operatorObjectId"] == oid
    assert database.audit.items[0]["operator"] == "operator@contoso.com"


def test_purge_data_catalog_rejects_caller_supplied_deployment_instance() -> None:
    request = FakeRequest({
        "container": "retrieval-config",
        "ids": ["runtime-catalog"],
        "confirm": "yes",
        "deploymentInstanceId": "deploy-1",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 400


def test_purge_data_catalog_rejects_purge_all() -> None:
    request = FakeRequest({
        "container": "retrieval-config",
        "purgeAll": True,
        "confirm": "yes",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 400


def test_purge_data_catalog_requires_confirm() -> None:
    request = FakeRequest({"container": "retrieval-config", "ids": ["runtime-catalog"]})

    response = function_app.purge_data(request)

    assert response.status_code == 400


def test_ingestion_purge_requires_source_scope() -> None:
    request = FakeRequest({
        "container": "source-documents",
        "ids": ["doc-1"],
        "confirm": "yes",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 400


def test_ingestion_runs_purge_cannot_delete_source_control() -> None:
    request = FakeRequest({
        "container": "ingestion-runs",
        "sourceId": "source-a",
        "ids": ["drive-coordinator"],
        "confirm": "yes",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 409
    assert json.loads(response.get_body()) == {
        "error": "source_control_purge_not_supported",
    }


def test_ingestion_purge_refuses_an_active_source_coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import config as config_module
    from ingestion.source_registry import SourceRegistryConflictError

    class BusyRegistry(_FakePurgeRegistry):
        def begin_operator_maintenance(self, *_args, **_kwargs):
            raise SourceRegistryConflictError("drive_coordinator_not_quiescent")

    database = FakeDatabase()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(database))
    monkeypatch.setattr(
        config_module,
        "load_config",
        lambda: SimpleNamespace(
            cosmos_search_chunks_container="default-search-chunks",
            cosmos_source_documents_container="default-source-documents",
        ),
    )
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: BusyRegistry())
    monkeypatch.setattr(function_app, "_require_ingestion_operator", lambda _req: None)
    request = FakeRequest({
        "container": "source-documents",
        "sourceId": "source-a",
        "ids": ["doc-1"],
        "confirm": "yes",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 409
    assert database.source_documents.deleted == []
