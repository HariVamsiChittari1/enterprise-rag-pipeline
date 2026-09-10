"""Regression test for function_app.purge_data: a failing audit write must never mask
an already-completed purge as a failure to the caller.

Monkeypatches azure.cosmos.CosmosClient / azure.identity.DefaultAzureCredential (the
SDK classes purge_data imports locally), with a fake service-audit container whose
create_item always raises, simulating a transient Cosmos write failure during audit
logging. Production code is never modified by this test.
"""

from __future__ import annotations

import json
from typing import Any

import azure.cosmos
import azure.identity
import pytest

import function_app


class FakeSourceDocumentsContainer:
    def __init__(self) -> None:
        self.deleted: list[str] = []
        self.deleted_partition_keys: list[str] = []

    def query_items(self, query: str, parameters: list[dict] | None = None, enable_cross_partition_query: bool = False):
        item_id = next((p["value"] for p in (parameters or []) if p["name"] == "@id"), None)
        if item_id is None:
            return []
        return [{"id": item_id, "pk": "run-1"}]

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

    def get_container_client(self, name: str) -> Any:
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
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", lambda **kwargs: object())


def test_purge_data_succeeds_even_when_audit_write_fails() -> None:
    request = FakeRequest({"container": "source-documents", "ids": ["doc-1"]})

    response = function_app.purge_data(request)

    assert response.status_code == 200
    body = json.loads(response.get_body())
    assert body["deleted"] == 1
    assert body["failed"] == 0
    assert "auditId" in body


def test_purge_data_catalog_deletes_within_deployment_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    database = FakeDatabase()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", _fake_cosmos_client_factory(database))
    request = FakeRequest({
        "container": "retrieval-config",
        "deploymentInstanceId": "deploy-1",
        "ids": ["runtime-catalog"],
        "confirm": "yes",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 200
    body = json.loads(response.get_body())
    assert body["deleted"] == 1
    assert body["failed"] == 0
    assert database.source_documents.deleted == ["runtime-catalog"]
    assert database.source_documents.deleted_partition_keys == ["deploy-1"]


def test_purge_data_catalog_requires_deployment_instance() -> None:
    request = FakeRequest({"container": "retrieval-config", "ids": ["runtime-catalog"], "confirm": "yes"})

    response = function_app.purge_data(request)

    assert response.status_code == 400


def test_purge_data_catalog_rejects_purge_all() -> None:
    request = FakeRequest({
        "container": "retrieval-config",
        "purgeAll": True,
        "confirm": "yes",
        "deploymentInstanceId": "deploy-1",
    })

    response = function_app.purge_data(request)

    assert response.status_code == 400


def test_purge_data_catalog_requires_confirm() -> None:
    request = FakeRequest({"container": "retrieval-config", "ids": ["runtime-catalog"], "deploymentInstanceId": "deploy-1"})

    response = function_app.purge_data(request)

    assert response.status_code == 400
