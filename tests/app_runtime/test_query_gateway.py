"""Tests that the Function app no longer owns the query gateway."""

from __future__ import annotations

from pathlib import Path

import function_app
from retrieval.main import app as retrieval_app


def test_function_app_has_no_query_gateway_route() -> None:
    source = Path(function_app.__file__).read_text(encoding="utf-8")
    assert '@app.route(route="query"' not in source
    assert 'route="query"' not in source


def test_direct_aca_query_route_is_the_only_public_query_handler() -> None:
    route_paths = {getattr(route, "path", None) for route in retrieval_app.routes}
    assert "/api/query" in route_paths


def test_legacy_gateway_contract_is_removed() -> None:
    project_root = Path(function_app.__file__).resolve().parents[1]
    paths = (
        project_root / "app" / "config.py",
        project_root / "app" / "retrieval" / "auth.py",
        project_root / "app" / "retrieval" / "config.py",
        project_root / "infra" / "modules" / "functions.bicep",
        project_root / "infra" / "modules" / "retrieval-config.bicep",
        project_root / "infra" / "modules" / "aca.bicep",
    )
    combined = "\n".join(path.read_text(encoding="utf-8") for path in paths)

    for legacy_contract in (
        "X-RAG-GATEWAY-CONTEXT",
        "X-RAG-REQUEST-ID",
        "RETRIEVAL_SERVICE_SCOPE",
        "RETRIEVAL_SERVICE_URL",
        "QUERY_PROXY_TIMEOUT_SECONDS",
        "RETRIEVAL_GATEWAY_CLIENT_ID",
        "RETRIEVAL_GATEWAY_PRINCIPAL_ID",
    ):
        assert legacy_contract not in combined
