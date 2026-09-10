"""Tests for retrieval's optional GenAI OpenTelemetry tracing.

_configure_tracing must never raise -- it is called once at FastAPI startup and a
misconfiguration must not prevent the retrieval service from starting.
"""

from __future__ import annotations

from dataclasses import replace
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

structlog = pytest.importorskip("structlog", reason="structlog not installed")

from retrieval.config import RetrievalConfig
from retrieval.main import _configure_tracing


def _config(**overrides: object) -> RetrievalConfig:
    values = dict(
        cosmos_endpoint="https://cosmos.example",
        cosmos_database="db",
        cosmos_chunks_container="search-chunks",
        cosmos_manifests_container="source-documents",
        cosmos_audit_container="service-audit",
        openai_endpoint="https://openai.example",
        embedding_deployment="embedding",
        chat_deployment="chat",
        tenant_id="tenant",
        managed_identity_client_id="mi",
        retrieval_audience="api://retrieval-api",
        gateway_client_id="33333333-3333-4333-8333-333333333333",
        gateway_principal_id="44444444-4444-4444-8444-444444444444",
        deployment_instance_id="instance-a",
        retrieval_timeout_seconds=5.0,
        generation_timeout_seconds=3.0,
        agent_timeout_seconds=8.0,
        agent_max_iterations=5,
        agent_api_version="2025-04-01-preview",
        max_evidence_chunks=5,
        max_planned_queries=3,
        graph_group_timeout_seconds=10.0,
        openai_api_version="2024-10-21",
        app_insights_connection_string=None,
        include_citations=True,
        acl_enabled=True,
    )
    values.update(overrides)
    return RetrievalConfig(**values)


def test_configure_tracing_is_a_noop_when_connection_string_unset() -> None:
    _configure_tracing(_config(app_insights_connection_string=None))


def test_configure_tracing_swallows_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate the optional dependency being unavailable/misconfigured -- must not raise.
    original_import = __import__

    def _blocking_import(name: str, *args: object, **kwargs: object):
        if name == "azure.monitor.opentelemetry":
            raise ImportError("azure-monitor-opentelemetry not installed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _blocking_import)

    _configure_tracing(_config(app_insights_connection_string="InstrumentationKey=fake"))


def test_catalog_logger_is_registered_for_azure_monitor(monkeypatch):
    configure = Mock()
    instrumentor = Mock()
    monkeypatch.setitem(sys.modules, "azure.monitor.opentelemetry", SimpleNamespace(configure_azure_monitor=configure))
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.openai_v2", SimpleNamespace(OpenAIInstrumentor=instrumentor))
    credential = Mock()
    _configure_tracing(_config(app_insights_connection_string="InstrumentationKey=fake"), credential=credential)
    configure.assert_called_once_with(
        connection_string="InstrumentationKey=fake", logger_name="retrieval.catalog_runtime", credential=credential,
    )
    instrumentor.return_value.instrument.assert_called_once_with()
