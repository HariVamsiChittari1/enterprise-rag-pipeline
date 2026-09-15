"""Integration tests for the FastAPI retrieval endpoints."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from openai import AsyncOpenAI, AzureOpenAI
from agent_framework.openai import OpenAIChatClient

structlog = pytest.importorskip("structlog", reason="structlog not installed")

from fastapi.testclient import TestClient
from fastapi import HTTPException, Request

from retrieval.main import app
import retrieval.main as retrieval_main
from retrieval.auth import GatewayContext, Principal
from retrieval.catalog import RequestPolicy, RuntimeCatalogSnapshot
from retrieval.cosmos import RetrievalLocatorKind, RetrievalMode, RetrievedChunk
from retrieval.pipeline import RetrievalDependencyError
from retrieval.scoring import ScoringProfile
from retrieval.service import (
    RagService, SearchResult, UnknownScoringProfileError,
    suppress_content_filter_retry, suppress_content_filter_retry_async,
)
from retrieval.synonyms import SynonymExpander, SynonymMap


TENANT_ID = "11111111-1111-4111-8111-111111111111"
USER_ID = "22222222-2222-4222-8222-abcdefabcdef"
GATEWAY_CLIENT_ID = "33333333-3333-4333-8333-333333333333"
GATEWAY_PRINCIPAL_ID = "44444444-4444-4444-8444-444444444444"


class _PolicyService:
    def capture_policy(self, requested=None, expand_synonyms=None):
        snapshot = RuntimeCatalogSnapshot(
            deployment_instance_id="test", catalog_id="runtime-catalog", etag="etag-a",
            digest="sha256:" + "a" * 64, operation_id="operation-a",
            changed_at="2026-09-08T00:00:00Z", accepted_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
            over_fetch_factor=1, hybrid_weights=(1, 1), full_text_score_scope="Global",
            default_profile=None, profiles={}, synonym_maps={}, synonym_expanders={},
        )
        return RequestPolicy.capture(snapshot, requested, expand_synonyms)


def _encoded_service_principal(*, idtyp: str = "app") -> str:
    claims = {
        "oid": GATEWAY_PRINCIPAL_ID,
        "tid": TENANT_ID,
        "aud": "api://retrieval-api",
        "idtyp": idtyp,
        "azp": GATEWAY_CLIENT_ID,
        "roles": "Retrieval.Gateway",
    }
    return base64.b64encode(json.dumps({
        "claims": [{"typ": key, "val": value} for key, value in claims.items()]
    }).encode()).decode()


def _gateway_request(*, idtyp: str = "app", duplicate_context: bool = False) -> Request:
    context = GatewayContext(USER_ID, TENANT_ID).encode().encode()
    headers = [
        (b"x-ms-client-principal", _encoded_service_principal(idtyp=idtyp).encode()),
        (b"x-rag-gateway-context", context),
    ]
    if duplicate_context:
        headers.append((b"x-rag-gateway-context", context))
    return Request({"type": "http", "headers": headers})


@pytest.fixture
def client():
    return TestClient(
        app,
        raise_server_exceptions=False,
        headers={"X-RAG-REQUEST-ID": "11111111-1111-4111-8111-111111111111"},
    )


def test_health_live(client):
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json()["status"] == "alive"


@pytest.mark.parametrize(
    ("kind", "label", "expected_url"),
    [
        ("page", "Page 3", "https://sp.com/report.pdf#page=3"),
        ("section", "Benefits", "https://sp.com/guide.docx"),
        ("slide", "Slide 4", "https://sp.com/deck.pptx"),
        ("worksheet", "Worksheet Forecast", "https://sp.com/model.xlsx"),
    ],
)
def test_citation_response_uses_typed_location(
    kind: str,
    label: str,
    expected_url: str,
) -> None:
    extension = {
        "page": "pdf",
        "section": "docx",
        "slide": "pptx",
        "worksheet": "xlsx",
    }[kind]
    citation = retrieval_main._citation_from_result(2, {
        "source_name": f"source.{extension}",
        "source_url": expected_url.split("#", maxsplit=1)[0],
        "locator_kind": kind,
        "locator_label": label,
        "locator_ordinal_start": 3 if kind == "page" else 1,
        "locator_ordinal_end": 3 if kind == "page" else 1,
    })

    assert citation.ref == "[S2]"
    assert citation.location == label
    assert citation.url == expected_url


def test_query_missing_auth_returns_401(client):
    response = client.post("/api/query", json={"question": "What is RAG?"})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_gateway_headers"


def test_resolve_principal_accepts_only_function_app_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = Mock()
    resolver.resolve_transitive_security_groups.return_value = {"group-1"}
    monkeypatch.setattr(retrieval_main._state, "config", SimpleNamespace(
        tenant_id=TENANT_ID,
        retrieval_audience="api://retrieval-api",
        gateway_client_id=GATEWAY_CLIENT_ID,
        gateway_principal_id=GATEWAY_PRINCIPAL_ID,
        acl_enabled=True,
    ), raising=False)
    monkeypatch.setattr(retrieval_main._state, "group_resolver", resolver, raising=False)

    principal = retrieval_main._resolve_principal(_gateway_request())

    assert principal == Principal(USER_ID, TENANT_ID, frozenset({"group-1"}))


def test_resolve_principal_rejects_duplicate_context_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(retrieval_main._state, "config", SimpleNamespace(), raising=False)

    with pytest.raises(HTTPException, match="invalid_gateway_headers"):
        retrieval_main._resolve_principal(_gateway_request(duplicate_context=True))


def test_resolve_principal_rejects_delegated_direct_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(retrieval_main._state, "config", SimpleNamespace(
        tenant_id=TENANT_ID,
        retrieval_audience="api://retrieval-api",
        gateway_client_id=GATEWAY_CLIENT_ID,
        gateway_principal_id=GATEWAY_PRINCIPAL_ID,
        acl_enabled=False,
    ), raising=False)
    monkeypatch.setattr(retrieval_main._state, "group_resolver", None, raising=False)

    with pytest.raises(HTTPException, match="service_token_required"):
        retrieval_main._resolve_principal(_gateway_request(idtyp="user"))


def test_query_empty_question_returns_422(client):
    response = client.post(
        "/api/query",
        json={"question": ""},
        headers={"X-MS-CLIENT-PRINCIPAL": "dummy"},
    )
    assert response.status_code == 422


def test_query_question_too_long_returns_422(client):
    response = client.post(
        "/api/query",
        json={"question": "x" * 4001},
        headers={"X-MS-CLIENT-PRINCIPAL": "dummy"},
    )
    assert response.status_code == 422


def test_query_invalid_mode_returns_422(client):
    response = client.post(
        "/api/query",
        json={"question": "test", "mode": "invalid"},
        headers={"X-MS-CLIENT-PRINCIPAL": "dummy"},
    )
    assert response.status_code == 422


def test_query_unknown_scoring_profile_returns_stable_400(
    client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = type(
        "FakeService",
        (_PolicyService,),
        {
            "plan_queries": lambda self, question, history: (["q"], []),
            "answer_with_queries": lambda self, *args, **kwargs: (_ for _ in ()).throw(
                UnknownScoringProfileError("unknown_scoring_profile:missing")
            ),
        },
    )()
    monkeypatch.setattr(retrieval_main, "_resolve_principal", lambda request: Principal("u", "t", frozenset({"g"})))
    monkeypatch.setattr(retrieval_main._state, "rag_service", service, raising=False)
    monkeypatch.setattr(retrieval_main._state, "agent_chat_client", None, raising=False)

    response = client.post(
        "/api/query",
        json={"question": "test", "scoring_profile": "missing"},
        headers={"X-MS-CLIENT-PRINCIPAL": "ignored"},
    )

    assert response.status_code == 400
    assert response.json()["error"] == {
        "code": "unknown_scoring_profile",
        "message": "The requested scoring profile is unavailable.",
    }


def test_query_retrieval_outage_returns_stable_503(
    client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = type(
        "FakeService",
        (_PolicyService,),
        {
            "plan_queries": lambda self, question, history: (["q"], []),
            "answer_with_queries": lambda self, *args, **kwargs: (_ for _ in ()).throw(
                RetrievalDependencyError("retrieval_dependency_unavailable")
            ),
        },
    )()
    monkeypatch.setattr(
        retrieval_main, "_resolve_principal",
        lambda request: Principal("dependency-test-user", "t", frozenset({"g"})),
    )
    monkeypatch.setattr(retrieval_main._state, "rag_service", service, raising=False)
    monkeypatch.setattr(retrieval_main._state, "agent_chat_client", None, raising=False)

    response = client.post(
        "/api/query",
        json={"question": "test"},
        headers={"X-MS-CLIENT-PRINCIPAL": "ignored"},
    )

    assert response.status_code == 503
    assert response.json()["error"] == {
        "code": "retrieval_dependency_unavailable",
        "message": "Retrieval is temporarily unavailable.",
    }


def test_query_wall_clock_deadline_returns_safe_504(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = SimpleNamespace(
        plan_queries=lambda *_args: (time.sleep(0.1), (["q"], []))[1],
        capture_policy=_PolicyService().capture_policy,
    )
    monkeypatch.setattr(
        retrieval_main,
        "_resolve_principal",
        lambda request: Principal("u", "t", frozenset({"g"})),
    )
    monkeypatch.setattr(retrieval_main._state, "rag_service", service, raising=False)
    monkeypatch.setattr(
        retrieval_main._state,
        "config",
        SimpleNamespace(operation_timeout_seconds=0.01),
        raising=False,
    )

    response = client.post("/api/query", json={"question": "slow"})

    assert response.status_code == 504
    assert response.json()["error"]["code"] == "operation_timeout"
    assert response.json()["request_id"] == "11111111-1111-4111-8111-111111111111"


@pytest.mark.parametrize("requested_mode", ["hybrid", "vector", "full_text"])
def test_agentic_path_receives_request_mode_and_top_k(
    client, monkeypatch: pytest.MonkeyPatch, requested_mode: str,
) -> None:
    captured: dict = {}

    async def _fake_agentic(*args, **kwargs):
        captured["mode"] = args[2]
        captured["top_k"] = args[-1]
        return {"answer": "ok", "citations": [], "usage": []}

    service = type(
        "FakeService",
        (_PolicyService,),
        {"plan_queries": lambda self, question, history: (["one", "two"], [])},
    )()
    monkeypatch.setattr(
        retrieval_main,
        "_resolve_principal",
        lambda request: Principal("agentic-top-k-user", "t", frozenset({"g"})),
    )
    monkeypatch.setattr(retrieval_main._state, "rag_service", service, raising=False)
    monkeypatch.setattr(retrieval_main._state, "agent_chat_client", object(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "audit_container", object(), raising=False)
    monkeypatch.setattr(
        retrieval_main._state,
        "config",
        type("Config", (), {"include_citations": True})(),
        raising=False,
    )
    monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", True)
    monkeypatch.setattr(retrieval_main, "_run_agentic_path", _fake_agentic)
    monkeypatch.setattr(retrieval_main, "write_audit_records", lambda *args, **kwargs: None)
    monkeypatch.setattr(retrieval_main, "_write_query_summary", lambda *args, **kwargs: None)

    response = client.post(
        "/api/query",
        json={"question": "compare two policies", "mode": requested_mode, "top_k": 1},
        headers={"X-MS-CLIENT-PRINCIPAL": "ignored"},
    )

    assert response.status_code == 200
    assert captured["mode"] is retrieval_main.RetrievalMode(requested_mode)
    assert captured["top_k"] == 1


@pytest.mark.parametrize("requested_mode", ["hybrid", "vector", "full_text"])
def test_agentic_fallback_preserves_request_mode(
    client, monkeypatch: pytest.MonkeyPatch, requested_mode: str,
) -> None:
    captured: dict = {}

    async def _fake_agentic(*args, **kwargs):
        captured["agentic_mode"] = args[2]
        return None

    class FakeService(_PolicyService):
        def plan_queries(self, question, history):
            return ["one", "two"], []

        def answer_with_queries(self, *args, **kwargs):
            captured["fallback_mode"] = args[3]
            return {"answer": "fallback", "citations": [], "usage": []}

    monkeypatch.setattr(
        retrieval_main,
        "_resolve_principal",
        lambda request: Principal("agentic-fallback-user", "t", frozenset({"g"})),
    )
    monkeypatch.setattr(retrieval_main._state, "rag_service", FakeService(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "agent_chat_client", object(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "audit_container", object(), raising=False)
    monkeypatch.setattr(
        retrieval_main._state,
        "config",
        type("Config", (), {"include_citations": True})(),
        raising=False,
    )
    monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", True)
    monkeypatch.setattr(retrieval_main, "_run_agentic_path", _fake_agentic)
    monkeypatch.setattr(retrieval_main, "write_audit_records", lambda *args, **kwargs: None)
    monkeypatch.setattr(retrieval_main, "_write_query_summary", lambda *args, **kwargs: None)

    response = client.post(
        "/api/query",
        json={"question": "compare two policies", "mode": requested_mode},
        headers={"X-MS-CLIENT-PRINCIPAL": "ignored"},
    )

    expected_mode = retrieval_main.RetrievalMode(requested_mode)
    assert response.status_code == 200
    assert response.json()["answer"] == "fallback"
    assert captured == {
        "agentic_mode": expected_mode,
        "fallback_mode": expected_mode,
    }


@pytest.mark.parametrize("search_calls", [0, 1])
def test_given_agent_without_evidence_when_query_completes_then_refuses(
    client, monkeypatch: pytest.MonkeyPatch, search_calls: int,
) -> None:
    captured_policies = []
    audit_records = []

    class EmptyEvidenceService(_PolicyService):
        def plan_queries(self, *_args):
            return ["one", "two"], [{"operation": "query_planning"}]

        def search(self, *_args, policy, **_kwargs):
            captured_policies.append(policy)
            return SearchResult((), ())

        def answer_with_queries(self, *_args, **_kwargs):
            pytest.fail("A nonblank no-evidence answer must not trigger fallback")

    class AgentResponse:
        usage = SimpleNamespace(prompt_tokens=12, completion_tokens=7)

        def __str__(self):
            return "Unsupported agent answer"

    def agent_factory(_client, search_tool, **_kwargs):
        async def run(_question):
            for _ in range(search_calls):
                await search_tool(query="one")
            return AgentResponse()

        return SimpleNamespace(run=run)

    monkeypatch.setattr(retrieval_main, "_resolve_principal", lambda _: Principal(
        f"no-evidence-{search_calls}", "t", frozenset({"g"}),
    ))
    monkeypatch.setattr(retrieval_main._state, "rag_service", EmptyEvidenceService(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "agent_chat_client", object(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "audit_container", SimpleNamespace(
        create_item=audit_records.append,
    ), raising=False)
    monkeypatch.setattr(retrieval_main._state, "config", SimpleNamespace(
        include_citations=True, agent_timeout_seconds=2, max_evidence_chunks=5,
        chat_deployment="test-chat",
    ), raising=False)
    monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", True)
    monkeypatch.setattr(retrieval_main, "create_rag_agent", agent_factory)

    response = client.post("/api/query", json={"question": "policy"})

    assert response.status_code == 200
    assert response.json()["answer"] == "I could not find authorized evidence for this question."
    assert response.json()["citations"] == []
    assert len(captured_policies) == search_calls
    assert [record["operation"] for record in audit_records] == (
        ["query_planning"] + ["tool_invocation"] * search_calls
        + ["agent_generation", "query_request"]
    )
    generation = audit_records[-2]
    assert (generation["prompt_tokens"], generation["completion_tokens"]) == (12, 7)
    summary = audit_records[-1]
    assert summary["path"] == "agentic"
    assert summary["catalog_etag"] == "etag-a"
    assert summary["citations_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["evidence", "blank", "timeout", "error", "dependency", "cancelled"])
async def test_given_agent_outcome_when_accepting_then_preserves_existing_behavior(
    monkeypatch: pytest.MonkeyPatch, outcome: str, capsys,
) -> None:
    chunk = RetrievedChunk(
        chunk_id="chunk", document_id="document", content="Synthetic evidence",
        source_name="policy.pdf", source_url="https://example.com/policy.pdf",
        locator_kind=RetrievalLocatorKind.PAGE, locator_label="Page 1",
        locator_ordinal_start=1, locator_ordinal_end=1,
    )
    policy = _PolicyService().capture_policy()
    principal = Principal("agent-outcome", "t", frozenset({"g"}))
    planning_usage = [{"operation": "query_planning"}]

    class EvidenceService:
        def search(self, _question, _queries, caller, **kwargs):
            assert caller is principal
            assert kwargs["policy"] is policy
            if outcome == "dependency":
                raise RetrievalDependencyError("retrieval_dependency_unavailable")
            return SearchResult((chunk,), ())

    def agent_factory(_client, search_tool, **_kwargs):
        async def run(_question):
            await search_tool(query="one")
            if outcome == "timeout":
                raise TimeoutError()
            if outcome == "error":
                raise RuntimeError("SYNTHETIC_PRIVATE_AGENT_FAILURE")
            if outcome == "cancelled":
                raise asyncio.CancelledError()
            return " \n " if outcome == "blank" else " Evidence answer [S1] "

        return SimpleNamespace(run=run)

    monkeypatch.setattr(retrieval_main._state, "rag_service", EvidenceService(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "agent_chat_client", object(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "config", SimpleNamespace(
        agent_timeout_seconds=2, max_evidence_chunks=5, chat_deployment="test-chat",
    ), raising=False)
    monkeypatch.setattr(retrieval_main, "create_rag_agent", agent_factory)

    operation = retrieval_main._run_agentic_path(
        "policy", principal, RetrievalMode.HYBRID, planning_usage,
        structlog.get_logger(), policy=policy,
    )
    if outcome in {"dependency", "cancelled"}:
        expected_error = RetrievalDependencyError if outcome == "dependency" else asyncio.CancelledError
        with pytest.raises(expected_error):
            await operation
    else:
        result = await operation
        if outcome == "evidence":
            assert result is not None
            assert result["answer"] == "Evidence answer [S1]"
            assert [citation["chunk_id"] for citation in result["citations"]] == ["chunk"]
            assert result["policy"] == policy.metadata()
            assert result["usage"][0] == planning_usage[0]
        else:
            assert result is None

    captured_logs = capsys.readouterr()
    assert "SYNTHETIC_PRIVATE_AGENT_FAILURE" not in captured_logs.out + captured_logs.err
    if outcome == "error":
        assert "agent_error" in captured_logs.out + captured_logs.err


@pytest.mark.parametrize("path", ["standard", "agent", "fallback"])
@pytest.mark.parametrize("include_citations", [True, False])
@pytest.mark.parametrize("audit_fails", [False, True], ids=["audit-stored", "audit-failed"])
@pytest.mark.parametrize("answer", [
    "[Summary] First [S1], second [S2].\nAgain [S1][S2].",
    "I could not find authorized evidence for this question.",
    "Unknown [S9].", "Missing references.", "Malformed [S01].",
])
def test_given_generated_citations_when_querying_then_validates_before_presentation(
    client, monkeypatch: pytest.MonkeyPatch, path: str, include_citations: bool, answer: str,
    audit_fails: bool, capsys,
) -> None:
    generation_calls = []
    audit_records = []
    question = "Synthetic private question for audit minimization"
    principal = Principal(f"citations-{path}-{include_citations}-{audit_fails}", "t", frozenset({"g"}))

    def persist(item):
        audit_records.append(item)
        if audit_fails:
            raise RuntimeError(f"synthetic audit outage: {question}")

    evidence = tuple(
        RetrievedChunk(
            chunk_id=str(index), document_id="document", content="Synthetic evidence",
            source_name="policy.pdf", source_url="https://example.com/policy.pdf",
            locator_kind=RetrievalLocatorKind.PAGE, locator_label=f"Page {index}",
            locator_ordinal_start=index, locator_ordinal_end=index,
        )
        for index in (1, 2)
    )

    def generate(**_kwargs):
        generation_calls.append("standard")
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=answer))])

    class GeneratedService(_PolicyService):
        _chat_deployment = "test-chat"
        _generation_timeout_seconds = 2
        _openai = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=generate)))
        answer_with_queries = RagService.answer_with_queries
        _create_chat_completion = RagService._create_chat_completion

        def plan_queries(self, *_args):
            return (["one"] if path == "standard" else ["one", "two"]), []

        def search(self, *_args, **_kwargs):
            return SearchResult(evidence, ())

    def agent_factory(_client, search_tool, **_kwargs):
        async def run(_question):
            generation_calls.append("agent")
            if path == "fallback":
                raise TimeoutError()
            await search_tool(query="one")
            return answer

        return SimpleNamespace(run=run)

    monkeypatch.setattr(retrieval_main, "_resolve_principal", lambda _: principal)
    monkeypatch.setattr(retrieval_main._state, "rag_service", GeneratedService(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "agent_chat_client", object(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "audit_container", SimpleNamespace(
        create_item=persist,
    ), raising=False)
    monkeypatch.setattr(retrieval_main._state, "config", SimpleNamespace(
        include_citations=include_citations, agent_timeout_seconds=2,
        max_evidence_chunks=5, chat_deployment="test-chat",
    ), raising=False)
    monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", True)
    monkeypatch.setattr(retrieval_main, "create_rag_agent", agent_factory)

    response = client.post("/api/query", json={"question": question})

    captured_logs = capsys.readouterr()
    assert question not in captured_logs.out + captured_logs.err
    if audit_fails and audit_records:
        assert "audit_write_failed" in captured_logs.out + captured_logs.err
    assert generation_calls == {
        "standard": ["standard"], "agent": ["agent"], "fallback": ["agent", "standard"],
    }[path]
    payload = response.json()
    assert payload["request_id"] == client.headers["X-RAG-REQUEST-ID"]
    if answer.startswith(("Unknown", "Missing", "Malformed")):
        assert response.status_code == 502
        assert payload["error"] == {
            "code": "answer_citation_invalid",
            "message": "The generated answer could not be validated.",
        }
        assert "answer" not in payload
        assert audit_records == []
    else:
        assert response.status_code == 200
        is_refusal = answer.startswith("I could not")
        expected_answer = (
            answer if include_citations or is_refusal
            else "[Summary] First, second.\nAgain."
        )
        assert payload["answer"] == expected_answer
        assert [citation["ref"] for citation in payload["citations"]] == (
            ["[S1]", "[S2]"] if include_citations and not is_refusal else []
        )
        assert audit_records[-1]["citations_count"] == (0 if is_refusal else 2)
        summary = audit_records[-1]
        assert summary["operation"] == "query_request"
        assert summary["requestId"] == payload["request_id"]
        assert summary["userId"] == principal.user_id
        assert summary["tenantId"] == principal.tenant_id
        assert summary["path"] == {
            "standard": "standard", "agent": "agentic", "fallback": "agentic_fallback",
        }[path]
        assert summary["catalog_etag"] == "etag-a"
        assert summary["catalog_operation_id"] == "operation-a"
        assert summary["catalog_version"] == "sha256:" + "a" * 64
        assert all(
            {"question", "question_truncated", "answer_preview", "answer_truncated"}.isdisjoint(record)
            for record in audit_records
        )
        serialized = json.dumps(audit_records)
        for content in (question, answer, "Synthetic evidence"):
            assert json.dumps(content)[1:-1] not in serialized


@pytest.mark.parametrize("path", ["planning", "standard", "agent", "fallback"])
@pytest.mark.parametrize("block", ["prompt", "empty", "partial"])
@pytest.mark.parametrize("include_citations", [True, False])
def test_given_safety_block_when_querying_then_returns_safe_error_without_continuation(
    client, monkeypatch: pytest.MonkeyPatch, path: str, block: str, include_citations: bool,
) -> None:
    calls = []
    searches = []
    chunk = RetrievedChunk(
        chunk_id="chunk", document_id="document", content="Synthetic evidence",
        source_name="policy.pdf", source_url="https://example.com/policy.pdf",
        locator_kind=RetrievalLocatorKind.PAGE, locator_label="Page 1",
        locator_ordinal_start=1, locator_ordinal_end=1,
    )

    def respond(request: httpx.Request) -> httpx.Response:
        agent_request = request.url.path.endswith("/responses")
        planning = "response_format" in json.loads(request.content)
        calls.append("agent" if agent_request else "planning" if planning else "standard")
        should_block = not planning or path == "planning"
        if should_block and block == "prompt":
            return httpx.Response(400, headers={"x-should-retry": "true", "retry-after-ms": "1"}, json={
                "error": {"code": "content_filter", "message": "Synthetic private provider detail"},
            })
        text = "" if block == "empty" else "Synthetic partial answer [S1]."
        if agent_request:
            return httpx.Response(200, json={
                "id": "synthetic", "object": "response", "created_at": 0, "model": "synthetic",
                "status": "incomplete", "incomplete_details": {"reason": "content_filter"},
                "output": [{"id": "message", "type": "message", "role": "assistant", "status": "completed",
                            "content": [{"type": "output_text", "text": text, "annotations": []}]}],
            })
        if planning and not should_block:
            text = json.dumps({"queries": ["one"] if path == "standard" else ["one", "two"]})
        return httpx.Response(200, json={
            "id": "synthetic", "object": "chat.completion", "created": 0, "model": "synthetic",
            "choices": [{"index": 0, "finish_reason": "content_filter" if should_block else "stop",
                         "message": {"role": "assistant", "content": text}}],
        })

    class SafetyService(_PolicyService):
        _chat_deployment = "synthetic"
        _generation_timeout_seconds = 2
        plan_queries = RagService.plan_queries
        _plan_queries = RagService._plan_queries
        _create_chat_completion = RagService._create_chat_completion
        answer_with_queries = RagService.answer_with_queries

        def search(self, *_args, **_kwargs):
            searches.append("search")
            return SearchResult((chunk,), ())

    async_sdk = AsyncOpenAI(
        api_key="synthetic", base_url="https://sdk-test.invalid/v1", max_retries=2,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                     event_hooks={"response": [suppress_content_filter_retry_async]}),
    )
    with AzureOpenAI(
        api_key="synthetic", azure_endpoint="https://sdk-test.invalid", api_version="2024-10-21", max_retries=2,
        http_client=httpx.Client(transport=httpx.MockTransport(respond),
                                event_hooks={"response": [suppress_content_filter_retry]}),
    ) as sync_sdk:
        service = SafetyService()
        service._openai = sync_sdk
        monkeypatch.setattr(retrieval_main._state, "rag_service", service, raising=False)
        monkeypatch.setattr(retrieval_main._state, "agent_chat_client", OpenAIChatClient(
            model="synthetic", async_client=async_sdk,
        ), raising=False)
        monkeypatch.setattr(retrieval_main, "_resolve_principal", lambda _: Principal(
            f"safety-{path}-{block}-{include_citations}", "tenant", frozenset({"group"}),
        ))
        monkeypatch.setattr(retrieval_main._state, "config", SimpleNamespace(
            include_citations=include_citations, agent_timeout_seconds=2, max_evidence_chunks=5,
            chat_deployment="synthetic",
        ), raising=False)
        monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", True)
        if path == "fallback":
            async def timeout(_question):
                calls.append("agent-timeout")
                raise TimeoutError()
            monkeypatch.setattr(retrieval_main, "create_rag_agent", lambda *_args, **_kwargs: SimpleNamespace(run=timeout))
        try:
            response = client.post("/api/query", json={"question": "Synthetic question"})
        finally:
            asyncio.run(async_sdk.close())

    assert response.status_code == 422
    assert response.json() == {
        "error": {"code": "content_filtered", "message": "The request could not be completed under the content safety policy."},
        "request_id": client.headers["X-RAG-REQUEST-ID"],
    }
    assert calls == {
        "planning": ["planning"], "standard": ["planning", "standard"],
        "agent": ["planning", "agent"], "fallback": ["planning", "agent-timeout", "standard"],
    }[path]
    assert len(searches) == (1 if path in {"standard", "fallback"} else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "startup", "shutdown", "agent-init"])
async def test_given_safety_clients_when_lifespan_exits_then_preserves_settings_and_closes_resources(
    monkeypatch: pytest.MonkeyPatch, failure: str | None, capsys,
) -> None:
    import agent_framework.openai as framework_openai

    config = SimpleNamespace(
        managed_identity_client_id="synthetic", cosmos_endpoint="https://cosmos.invalid",
        cosmos_database="synthetic", cosmos_chunks_container="chunks", cosmos_manifests_container="manifests",
        cosmos_audit_container="audit", acl_enabled=True, openai_endpoint="https://sdk-test.invalid",
        openai_api_version="2024-10-21", agent_api_version="synthetic-api-version", chat_deployment="synthetic",
        catalog_container="catalog", deployment_instance_id="synthetic", catalog_poll_seconds=60,
        embedding_deployment="synthetic-embedding", retrieval_timeout_seconds=2, generation_timeout_seconds=2,
        max_evidence_chunks=5, max_planned_queries=3, graph_group_timeout_seconds=2,
    )
    credential = Mock()
    credential.get_token.return_value = SimpleNamespace(token="synthetic-token")
    registry = Mock()
    registry.__len__ = Mock(return_value=1)
    cosmos = Mock()
    provider = SimpleNamespace(
        start=AsyncMock(side_effect=RuntimeError("synthetic startup") if failure == "startup" else None),
        close=AsyncMock(side_effect=RuntimeError("synthetic shutdown") if failure == "shutdown" else None),
        snapshot=SimpleNamespace(digest="synthetic", etag="synthetic"),
    )
    clients = {}
    sync_factory = retrieval_main.DefaultHttpxClient
    async_factory = retrieval_main.DefaultAsyncHttpxClient
    agent_factory = framework_openai.OpenAIChatClient

    def sync_http(**kwargs):
        clients["sync_http"] = sync_factory(**kwargs)
        return clients["sync_http"]

    def async_http(**kwargs):
        clients["async_http"] = async_factory(**kwargs)
        return clients["async_http"]

    def agent_client(**kwargs):
        clients["agent_options"] = kwargs
        if failure == "agent-init":
            raise RuntimeError("SYNTHETIC_PRIVATE_CLIENT_INIT_FAILURE")
        result = agent_factory(**kwargs)
        clients["original"] = result.client
        return result

    monkeypatch.setattr(retrieval_main, "_state", retrieval_main._AppState())
    monkeypatch.setattr(retrieval_main, "load_retrieval_config", lambda: config)
    monkeypatch.setattr(retrieval_main, "ManagedIdentityCredential", lambda **_kwargs: credential)
    monkeypatch.setattr(retrieval_main, "_configure_tracing", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(retrieval_main, "load_cosmos_instance_configs", lambda **_kwargs: [])
    monkeypatch.setattr(retrieval_main, "build_cosmos_registry", lambda *_args, **_kwargs: registry)
    monkeypatch.setattr(retrieval_main, "CosmosClient", lambda **_kwargs: cosmos)
    monkeypatch.setattr(retrieval_main, "RuntimeCatalogLoader", Mock())
    monkeypatch.setattr(retrieval_main, "RuntimeCatalogProvider", lambda *_args, **_kwargs: provider)
    monkeypatch.setattr(retrieval_main, "catalog_event_emitter", Mock())
    monkeypatch.setattr(retrieval_main, "RagService", Mock())
    monkeypatch.setattr(retrieval_main, "GraphGroupResolver", Mock())
    monkeypatch.setattr(retrieval_main, "DefaultHttpxClient", sync_http)
    monkeypatch.setattr(retrieval_main, "DefaultAsyncHttpxClient", async_http)
    monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", True)
    monkeypatch.setattr(framework_openai, "OpenAIChatClient", agent_client)

    async def run_lifespan():
        async with retrieval_main._lifespan(app):
            assert not clients["sync_http"].is_closed
            if failure != "agent-init":
                assert not clients["async_http"].is_closed

    if failure in {"startup", "shutdown"}:
        with pytest.raises(RuntimeError, match=f"synthetic {failure}"):
            await run_lifespan()
    else:
        await run_lifespan()

    if failure == "agent-init":
        assert retrieval_main._state.agent_chat_client is None
        assert "async_http" not in clients
        assert clients["sync_http"].is_closed
        assert retrieval_main._state.openai_client.is_closed()
        provider.close.assert_awaited_once()
        credential.close.assert_called_once()
        registry.close.assert_called_once()
        cosmos.close.assert_called_once()
        captured_logs = capsys.readouterr()
        assert "agent_chat_client_init_failed" in captured_logs.out + captured_logs.err
        assert "SYNTHETIC_PRIVATE_CLIENT_INIT_FAILURE" not in captured_logs.out + captured_logs.err
        return

    copied = retrieval_main._state.agent_chat_client.client
    original = clients["original"]
    assert copied is not original
    assert copied.base_url == original.base_url
    assert {key: value for key, value in copied.default_headers.items() if isinstance(value, str)} == {
        key: value for key, value in original.default_headers.items() if isinstance(value, str)
    }
    assert copied.default_query == original.default_query
    assert copied.timeout == original.timeout
    assert copied.max_retries == original.max_retries == 2
    assert copied.organization == original.organization
    assert copied.project == original.project
    assert clients["agent_options"]["api_version"] == config.agent_api_version
    assert await clients["agent_options"]["api_key"]() == "synthetic-token"
    credential.get_token.assert_called_with("https://cognitiveservices.azure.com/.default")
    assert clients["sync_http"].event_hooks["response"] == [suppress_content_filter_retry]
    assert clients["async_http"].event_hooks["response"] == [suppress_content_filter_retry_async]
    assert original.is_closed() and copied.is_closed()
    assert clients["sync_http"].is_closed and clients["async_http"].is_closed
    assert retrieval_main._state.openai_client.is_closed()
    provider.close.assert_awaited_once()
    credential.close.assert_called_once()
    registry.close.assert_called_once()
    cosmos.close.assert_called_once()


def test_query_summary_records_effective_relevance_and_degraded_state() -> None:
    from unittest.mock import MagicMock

    container = MagicMock()
    retrieval_main._write_query_summary(
        container,
        "request-1", "user-1", "tenant-1", 2,
        "standard", "hybrid", 2, 125,
        "sha256:" + "a" * 64, "hr-relevance", "hr-en", True,
    )

    item = container.create_item.call_args.args[0]
    assert item["catalog_version"] == "sha256:" + "a" * 64
    assert item["scoring_profile"] == "hr-relevance"
    assert item["synonym_map"] == "hr-en"
    assert item["retrieval_degraded"] is True
    assert {"question", "question_truncated", "answer_preview", "answer_truncated"}.isdisjoint(item)
    assert set(item) == {
        "id", "requestId", "userId", "tenantId", "mode", "recordedAt", "operation",
        "citations_count", "path", "planned_queries", "e2e_latency_ms", "catalog_version",
        "catalog_etag", "catalog_operation_id", "scoring_profile", "synonym_map", "retrieval_degraded",
    }
    assert item["requestId"] == "request-1"
    assert item["userId"] == "user-1"
    assert item["tenantId"] == "tenant-1"
    assert item["mode"] == "hybrid"
    assert item["citations_count"] == 2
    assert item["planned_queries"] == 2
    assert item["e2e_latency_ms"] == 125


@pytest.mark.parametrize("path", ["standard", "agent", "timeout", "error"])
def test_given_reload_during_planning_when_request_completes_then_policy_and_audit_stay_bound(
    client, monkeypatch: pytest.MonkeyPatch, path: str,
) -> None:
    synonym_map = SynonymMap.parse("original-map", ["policy, guidance"])
    original = replace(
        _PolicyService().capture_policy().snapshot,
        profiles={"original": ScoringProfile(name="original", synonym_map="original-map")},
        default_profile="original",
        synonym_maps={"original-map": synonym_map},
        synonym_expanders={"original-map": SynonymExpander(synonym_map)},
    )
    captured_policies = []

    class ReloadingService:
        snapshot = original
        captures = 0

        def capture_policy(self, requested=None, expand_synonyms=None):
            self.captures += 1
            return RequestPolicy.capture(self.snapshot, requested, expand_synonyms)

        def plan_queries(self, *_args):
            self.snapshot = replace(
                original, etag="etag-b", operation_id="operation-b",
                digest="sha256:" + "b" * 64, default_profile=None,
                profiles={}, synonym_maps={}, synonym_expanders={},
            )
            return (["one"] if path == "standard" else ["one", "two"]), []

        def search(self, *_args, policy, **_kwargs):
            captured_policies.append(policy)
            return SearchResult((), ())

        def answer_with_queries(self, *_args, policy, **_kwargs):
            captured_policies.append(policy)
            return {"answer": "fallback", "citations": [], "usage": [], "policy": policy.metadata()}

    def agent_factory(_client, search_tool, **_kwargs):
        async def run(_question):
            await search_tool(query="one")
            await search_tool(query="two")
            if path == "timeout":
                raise TimeoutError()
            if path == "error":
                raise RuntimeError("synthetic agent failure")
            return "agent answer"

        return SimpleNamespace(run=run)

    service = ReloadingService()
    audit = Mock()
    monkeypatch.setattr(retrieval_main, "_resolve_principal", lambda _: Principal("reload-" + path, "t", frozenset({"g"})))
    monkeypatch.setattr(retrieval_main._state, "rag_service", service, raising=False)
    monkeypatch.setattr(retrieval_main._state, "agent_chat_client", object(), raising=False)
    monkeypatch.setattr(retrieval_main._state, "audit_container", audit, raising=False)
    monkeypatch.setattr(retrieval_main._state, "config", SimpleNamespace(
        include_citations=True, agent_timeout_seconds=2, max_evidence_chunks=5,
        chat_deployment="test-chat", operation_timeout_seconds=5,
    ), raising=False)
    monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", True)
    monkeypatch.setattr(retrieval_main, "create_rag_agent", agent_factory)

    response = client.post("/api/query", json={"question": "policy"})

    assert response.status_code == 200
    assert set(response.json()) == {"answer", "citations", "request_id"}
    assert service.captures == 1
    assert len(captured_policies) == {"standard": 1, "agent": 2, "timeout": 3, "error": 3}[path]
    assert all(policy is captured_policies[0] for policy in captured_policies)
    assert captured_policies[0].snapshot is original
    item = next(call.args[0] for call in audit.create_item.call_args_list if call.args[0]["operation"] == "query_request")
    assert item["catalog_etag"] == "etag-a"
    assert item["catalog_operation_id"] == "operation-a"
    assert item["catalog_version"] == original.digest
    assert item["scoring_profile"] == "original"
    assert item["synonym_map"] == "original-map"
