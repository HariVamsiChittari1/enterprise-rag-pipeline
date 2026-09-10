"""Integration tests for the FastAPI retrieval endpoints."""

from __future__ import annotations

import base64
import json
import time
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

structlog = pytest.importorskip("structlog", reason="structlog not installed")

from fastapi.testclient import TestClient
from fastapi import HTTPException, Request

from retrieval.main import app
import retrieval.main as retrieval_main
from retrieval.auth import GatewayContext, Principal
from retrieval.catalog import RequestPolicy, RuntimeCatalogSnapshot
from retrieval.pipeline import RetrievalDependencyError
from retrieval.scoring import ScoringProfile
from retrieval.service import SearchResult, UnknownScoringProfileError
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


def test_query_summary_records_effective_relevance_and_degraded_state() -> None:
    from unittest.mock import MagicMock

    container = MagicMock()
    retrieval_main._write_query_summary(
        container,
        "request-1", "user-1", "tenant-1", "question", "answer", 2,
        "standard", "hybrid", 2, 125,
        "sha256:" + "a" * 64, "hr-relevance", "hr-en", True,
    )

    item = container.create_item.call_args.args[0]
    assert item["catalog_version"] == "sha256:" + "a" * 64
    assert item["scoring_profile"] == "hr-relevance"
    assert item["synonym_map"] == "hr-en"
    assert item["retrieval_degraded"] is True


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
