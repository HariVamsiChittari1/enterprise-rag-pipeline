"""Operator-role checks on the ingestion control routes (full-sync, status, retry-failed, inspect)."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
from types import SimpleNamespace
from typing import Any

import azure.functions as func
import pytest

import config as config_module
import function_app


def _principal_header(role: str) -> str:
    payload = json.dumps({"claims": [
        {"typ": "tid", "val": "tenant-a"},
        {"typ": "roles", "val": role},
    ]}).encode("utf-8")
    return base64.b64encode(payload).decode("ascii")


def _request(method: str, route: str, role: str | None, params: dict[str, str]) -> func.HttpRequest:
    headers = {} if role is None else {"X-MS-CLIENT-PRINCIPAL": _principal_header(role)}
    return func.HttpRequest(
        method=method, url=f"http://localhost/api/{route}", headers=headers, params=params, body=b"",
    )


class _UntouchedClient:
    def __getattr__(self, name: str) -> Any:
        pytest.fail(f"unauthorized request must not use the Durable client ({name})")


def _call(route_name: str, request: func.HttpRequest, client: Any) -> func.HttpResponse:
    user_code = getattr(function_app, route_name)._function._func
    user_code = getattr(user_code, "__wrapped__", user_code)
    if "client" in inspect.signature(user_code).parameters:
        return asyncio.run(user_code(request, client))
    return user_code(request)


ROUTES = [
    ("start_full_sync", "POST", "ingestion/full-sync", {}),
    ("start_source_full_sync", "POST", "ingestion/sources/full-sync", {}),
    ("preflight_source", "POST", "ingestion/sources/preflight", {}),
    ("get_status", "GET", "ingestion/status", {}),
    ("retry_failed", "POST", "ingestion/retry-failed", {}),
    ("inspect_data", "GET", "ingestion/inspect", {"container": "invalid"}),
]


@pytest.fixture(autouse=True)
def _tenant_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace(tenant_id="tenant-a"))
    monkeypatch.setattr(function_app, "INGESTION_ADMIN_ROLE", "Ingestion.Operator")


@pytest.mark.parametrize("route_name, method, route, params", ROUTES)
@pytest.mark.parametrize("role", [None, "Reader"])
def test_route_rejects_caller_without_operator_role(
    route_name: str, method: str, route: str, params: dict[str, str], role: str | None,
) -> None:
    response = _call(route_name, _request(method, route, role, params), _UntouchedClient())

    assert response.status_code == 403
    assert json.loads(response.get_body()) == {"error": "operator_role_required"}


def _encode(payload: Any) -> str:
    return base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")


@pytest.mark.parametrize("route_name, method, route, params", ROUTES + [
    ("register_source", "POST", "ingestion/sources", {}),
    ("list_sources", "GET", "ingestion/sources", {}),
    ("change_source_state", "PATCH", "ingestion/sources/source-a", {}),
])
@pytest.mark.parametrize(
    "header",
    [
        "not-base64!",
        base64.b64encode(b"not-json").decode("ascii"),
        _encode({"claims": "not-a-list"}),
        _encode({"claims": [{"typ": "tid", "val": "tenant-other"}, {"typ": "roles", "val": "Ingestion.Operator"}]}),
        _encode({"claims": [{"typ": "roles", "val": "Ingestion.Operator"}]}),
    ],
    ids=["not-base64", "not-json", "claims-not-list", "wrong-tenant", "no-tenant"],
)
def test_route_rejects_malformed_or_wrong_tenant_principal(
    route_name: str, method: str, route: str, params: dict[str, str], header: str,
) -> None:
    request = func.HttpRequest(
        method=method, url=f"http://localhost/api/{route}",
        headers={"X-MS-CLIENT-PRINCIPAL": header}, params=params, body=b"",
    )

    response = _call(route_name, request, _UntouchedClient())

    assert response.status_code == 403
    assert json.loads(response.get_body()) == {"error": "operator_role_required"}


@pytest.mark.parametrize(
    "route_name, method, route, params",
    [r for r in ROUTES if r[0] != "start_full_sync"],
)
def test_operator_reaches_existing_route_validation(
    route_name: str, method: str, route: str, params: dict[str, str],
) -> None:
    response = _call(
        route_name, _request(method, route, "Ingestion.Operator", params), _UntouchedClient(),
    )

    assert response.status_code == 400


def test_operator_starts_full_sync() -> None:
    started: list[str] = []

    class Client:
        async def start_new(self, name: str) -> str:
            started.append(name)
            return "instance-1"

        def create_check_status_response(self, _req: Any, instance_id: str) -> func.HttpResponse:
            return func.HttpResponse(instance_id, status_code=202)

    response = _call(
        "start_full_sync",
        _request("POST", "ingestion/full-sync", "Ingestion.Operator", {}),
        Client(),
    )

    assert response.status_code == 202
    assert started == ["multi_source_full_sync_orchestrator"]
