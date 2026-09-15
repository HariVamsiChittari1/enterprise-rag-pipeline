"""Tests for GenAI capture suppression and optional Azure Monitor setup."""

from __future__ import annotations

from copy import deepcopy
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

structlog = pytest.importorskip("structlog", reason="structlog not installed")

from retrieval.config import RetrievalConfig
from retrieval.main import _configure_tracing


@pytest.fixture(autouse=True)
def isolated_agent_observability(monkeypatch):
    from agent_framework import observability

    monkeypatch.setattr(observability, "OBSERVABILITY_SETTINGS", deepcopy(observability.OBSERVABILITY_SETTINGS))


@pytest.mark.parametrize("configured", [False, True], ids=["monitor-unset", "monitor-set"])
def test_given_sensitive_capture_when_configuring_then_agent_instrumentation_is_disabled(monkeypatch, configured):
    from agent_framework import observability

    observability.enable_instrumentation(enable_sensitive_data=True, force=True)
    monkeypatch.setitem(sys.modules, "azure.monitor.opentelemetry", SimpleNamespace(configure_azure_monitor=lambda **kwargs: None))

    _configure_tracing(_config(app_insights_connection_string="InstrumentationKey=fake" if configured else None))
    observability.enable_sensitive_telemetry()

    assert not observability.OBSERVABILITY_SETTINGS.enable_instrumentation
    assert not observability.OBSERVABILITY_SETTINGS.enable_sensitive_data


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


def test_configure_tracing_swallows_failures(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    # Simulate the optional dependency being unavailable/misconfigured -- must not raise.
    original_import = __import__

    def _blocking_import(name: str, *args: object, **kwargs: object):
        if name == "azure.monitor.opentelemetry":
            raise ImportError("SYNTHETIC_PRIVATE_TRACING_FAILURE")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _blocking_import)

    _configure_tracing(_config(app_insights_connection_string="InstrumentationKey=fake"))

    captured_logs = capsys.readouterr()
    assert "tracing_configuration_failed" in captured_logs.out + captured_logs.err
    assert "SYNTHETIC_PRIVATE_TRACING_FAILURE" not in captured_logs.out + captured_logs.err


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
    instrumentor.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "provider-error", "tool-error"])
async def test_given_disabled_genai_when_sdk_calls_run_then_only_application_span_is_exported(monkeypatch, outcome, capsys):
    import httpx
    from agent_framework import observability
    from agent_framework.exceptions import ChatClientException
    from agent_framework.openai import OpenAIChatClient
    from openai import AsyncOpenAI, BadRequestError, OpenAI
    from opentelemetry import _logs, trace
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from retrieval.agent import create_rag_agent

    marker = "SYNTHETIC_PRIVATE_MODEL_CONTENT"
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    log_exporter = InMemoryLogRecordExporter()
    log_provider = LoggerProvider()
    log_provider.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    monkeypatch.setattr(_logs, "get_logger_provider", lambda: log_provider)
    monkeypatch.setenv("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "true")
    observability.enable_instrumentation(enable_sensitive_data=True, force=True)
    monkeypatch.setitem(sys.modules, "azure.monitor.opentelemetry", SimpleNamespace(configure_azure_monitor=lambda **kwargs: None))
    requests = []
    tool_calls = []
    failure = outcome == "provider-error"

    def respond(request):
        requests.append(request.url.path)
        if failure:
            return httpx.Response(400, json={"error": {"code": "invalid_parameter", "message": marker}})
        if request.url.path.endswith("chat/completions"):
            return httpx.Response(200, json={
                "id": "synthetic-chat", "object": "chat.completion", "created": 0, "model": "synthetic",
                "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": marker}}],
            })
        output = [{"id": "synthetic-message", "type": "message",
            "role": "assistant", "status": "completed", "content": [
                {"type": "output_text", "text": marker, "annotations": []},
            ]}]
        if requests.count("/v1/responses") == 1:
            output = [{"id": "synthetic-tool", "type": "function_call", "call_id": "synthetic-call",
                "name": "search_knowledge_base", "arguments": json.dumps({"query": marker}), "status": "completed"}]
        return httpx.Response(200, json={
            "id": "synthetic-response", "object": "response", "created_at": 0, "model": "synthetic",
            "status": "completed", "output": output,
        })

    async def search_knowledge_base(query: str) -> str:
        tool_calls.append(query)
        if outcome == "tool-error":
            raise RuntimeError(marker)
        return marker

    try:
        _configure_tracing(_config(app_insights_connection_string="InstrumentationKey=fake"))
        with provider.get_tracer("application").start_as_current_span("synthetic-request") as span:
            span.set_attribute("request_id", "synthetic-request-id")
            with OpenAI(api_key="synthetic", max_retries=0, http_client=httpx.Client(transport=httpx.MockTransport(respond))) as sdk:
                if failure:
                    with pytest.raises(BadRequestError):
                        sdk.chat.completions.create(model="synthetic", messages=[{"role": "user", "content": marker}])
                else:
                    response = sdk.chat.completions.create(model="synthetic", messages=[{"role": "user", "content": marker}])
                    assert response.choices[0].message.content == marker
            async with AsyncOpenAI(api_key="synthetic", max_retries=0, http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond))) as sdk:
                agent = create_rag_agent(OpenAIChatClient(model="synthetic", async_client=sdk), search_knowledge_base)
                if failure:
                    with pytest.raises(ChatClientException):
                        await agent.run(marker)
                else:
                    assert str(await agent.run(marker)) == marker

        spans = exporter.get_finished_spans()
        assert requests == ["/v1/chat/completions"] + ["/v1/responses"] * (1 if failure else 2)
        assert tool_calls == ([] if failure else [marker])
        assert [span.name for span in spans] == ["synthetic-request"]
        assert spans[0].attributes["request_id"] == "synthetic-request-id"
        assert marker not in json.dumps([span.to_json() for span in spans])
        assert log_exporter.get_finished_logs() == ()
        captured_logs = capsys.readouterr()
        assert marker not in captured_logs.out + captured_logs.err
    finally:
        provider.shutdown()
        log_provider.shutdown()
