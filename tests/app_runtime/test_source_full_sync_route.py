"""Operator source-specific full sync: POST /api/ingestion/sources/{sourceId}/full-sync."""

from __future__ import annotations

import asyncio
import base64
import json
from types import SimpleNamespace
from typing import Any

import azure.functions as func
import pytest

import config as config_module
import function_app
from ingestion.source_registry import SourcePlan, SourceRegistryConflictError
from source_registry import SharePointSource, SourceRegistryError


def _plan(source_id: str, desired_state: str = "active") -> SourcePlan:
    source = SharePointSource(
        source_id, f"site-{source_id}", f"https://tenant.sharepoint.com/sites/{source_id}",
        f"drive-{source_id}", desired_state,
    )
    return SourcePlan(source, f"etag-{source_id}", "a" * 64)


class _Registry:
    def __init__(self, plan: SourcePlan | None = None, error: Exception | None = None,
                 coordinator: dict[str, Any] | None = None,
                 reprovision_full_sync_required: bool = False) -> None:
        self.plan = plan
        self.error = error
        self.coordinator = coordinator
        self.reprovision_full_sync_required = reprovision_full_sync_required
        self.reads: list[str] = []

    def read_source_plan(self, source_id: str) -> SourcePlan:
        self.reads.append(source_id)
        if self.error is not None:
            raise self.error
        assert self.plan is not None
        return self.plan

    def get_drive_coordinator(self, _source_id: str) -> dict[str, Any] | None:
        return self.coordinator

    def is_reprovision_full_sync_required(self, _source_id: str) -> bool:
        return self.reprovision_full_sync_required


class _Client:
    def __init__(self) -> None:
        self.started: list[tuple[str, Any]] = []

    async def start_new(self, name: str, client_input: Any = None) -> str:
        self.started.append((name, client_input))
        return "instance-1"

    def create_check_status_response(self, _req: Any, instance_id: str) -> func.HttpResponse:
        return func.HttpResponse(instance_id, status_code=202)


def _principal() -> str:
    payload = json.dumps({"claims": [
        {"typ": "tid", "val": "tenant-a"}, {"typ": "roles", "val": "Ingestion.Operator"},
    ]}).encode("utf-8")
    return base64.b64encode(payload).decode("ascii")


def _start(registry: _Registry, client: _Client, source_id: str = "poc-pg") -> func.HttpResponse:
    request = func.HttpRequest(
        method="POST", url=f"http://localhost/api/ingestion/sources/{source_id}/full-sync",
        headers={"X-MS-CLIENT-PRINCIPAL": _principal()}, route_params={"sourceId": source_id}, body=b"",
    )
    user_code = function_app.start_source_full_sync._function._func
    return asyncio.run(getattr(user_code, "__wrapped__", user_code)(request, client))


@pytest.fixture(autouse=True)
def _environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace(tenant_id="tenant-a"))
    monkeypatch.setattr(function_app, "INGESTION_ADMIN_ROLE", "Ingestion.Operator")


def _use(monkeypatch: pytest.MonkeyPatch, registry: _Registry) -> None:
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: registry)


def test_starts_full_sync_for_only_the_requested_source(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan("poc-pg")
    registry = _Registry(plan, coordinator={"state": "settled", "operation": "full-sync"})
    _use(monkeypatch, registry)
    client = _Client()

    response = _start(registry, client)

    assert response.status_code == 202
    assert registry.reads == ["poc-pg"]
    assert client.started == [("full_sync_orchestrator", plan.to_payload())]


def test_starts_when_source_has_no_coordinator_yet(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = _Registry(_plan("poc-pg"))
    _use(monkeypatch, registry)
    client = _Client()

    assert _start(registry, client).status_code == 202
    assert len(client.started) == 1


def test_starts_required_bootstrap_full_sync_while_source_is_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan("poc-pg", "paused")
    registry = _Registry(plan, reprovision_full_sync_required=True)
    _use(monkeypatch, registry)
    client = _Client()

    response = _start(registry, client)

    assert response.status_code == 202
    assert client.started == [(
        "full_sync_orchestrator",
        plan.to_payload() | {"reprovisionBootstrap": True},
    )]


def test_rejects_invalid_source_id(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = _Registry(_plan("poc-pg"))
    _use(monkeypatch, registry)
    client = _Client()

    response = _start(registry, client, source_id=" ")

    assert response.status_code == 400
    assert json.loads(response.get_body()) == {"error": "source_sourceId_invalid"}
    assert registry.reads == [] and client.started == []


@pytest.mark.parametrize("error, status, code", [
    (SourceRegistryError("source_registration_missing"), 404, "source_registration_missing"),
    (SourceRegistryError("source_binding_not_accepted"), 409, "source_binding_not_accepted"),
    (SourceRegistryConflictError("source_binding_is_immutable"), 409, "source_binding_is_immutable"),
    (SourceRegistryError("source_registration_invalid"), 503, "source_registration_invalid"),
    (RuntimeError("cosmos down"), 503, "source_registry_unavailable"),
])
def test_maps_registry_errors_without_starting(
    monkeypatch: pytest.MonkeyPatch, error: Exception, status: int, code: str,
) -> None:
    registry = _Registry(error=error)
    _use(monkeypatch, registry)
    client = _Client()

    response = _start(registry, client)

    assert response.status_code == status
    assert json.loads(response.get_body()) == {"error": code}
    assert client.started == []


@pytest.mark.parametrize("state", ["paused", "retired"])
def test_rejects_source_that_is_not_active(monkeypatch: pytest.MonkeyPatch, state: str) -> None:
    registry = _Registry(_plan("poc-pg", state))
    _use(monkeypatch, registry)
    client = _Client()

    response = _start(registry, client)

    assert response.status_code == 409
    assert json.loads(response.get_body()) == {"error": "source_not_active"}
    assert client.started == []


@pytest.mark.parametrize("state, operation", [
    ("active", "delta-sync"), ("draining", "full-sync"), ("maintenance", "source-retirement"),
])
def test_rejects_busy_source(monkeypatch: pytest.MonkeyPatch, state: str, operation: str) -> None:
    registry = _Registry(_plan("poc-pg"), coordinator={"state": state, "operation": operation})
    _use(monkeypatch, registry)
    client = _Client()

    response = _start(registry, client)

    assert response.status_code == 409
    assert json.loads(response.get_body()) == {"error": "source_busy", "operation": operation}
    assert client.started == []
