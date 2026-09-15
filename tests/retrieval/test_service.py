from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from openai import APIStatusError, AzureOpenAI, BadRequestError

from retrieval.auth import Principal
from retrieval.catalog import RuntimeCatalogSnapshot, load_catalog_item
from retrieval.cosmos import (
    RetrievalLocatorKind,
    RetrievalMode,
    RetrievedChunk as _RetrievedChunk,
    SecureCosmosRetriever,
)
from retrieval.cosmos_registry import CosmosRegistry
from retrieval.pipeline import RetrievalDependencyError
from retrieval.service import ContentFilteredError, RagService, suppress_content_filter_retry


def RetrievedChunk(
    chunk_id: str,
    document_id: str,
    content: str,
    source_name: str,
    source_url: str,
    page_number: int,
    source_modified_at: str | None = None,
) -> _RetrievedChunk:
    return _RetrievedChunk(
        chunk_id=chunk_id,
        document_id=document_id,
        content=content,
        source_name=source_name,
        source_url=source_url,
        locator_kind=RetrievalLocatorKind.PAGE,
        locator_label=f"Page {page_number}",
        locator_ordinal_start=page_number,
        locator_ordinal_end=page_number,
        source_modified_at=source_modified_at,
    )


def _registry(retriever) -> CosmosRegistry:
    return CosmosRegistry({"source": retriever})


def _provider(**values) -> SimpleNamespace:
    snapshot = RuntimeCatalogSnapshot(
        deployment_instance_id="test", catalog_id="runtime-catalog", etag="etag-a",
        digest="sha256:" + "a" * 64, operation_id="operation-a",
        changed_at="2026-09-08T00:00:00Z", accepted_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
        over_fetch_factor=1, hybrid_weights=(1, 1), full_text_score_scope="Global",
        default_profile=None, profiles={}, synonym_maps={}, synonym_expanders={},
    )
    return SimpleNamespace(snapshot=replace(snapshot, **values))


def completion(content: str) -> SimpleNamespace:
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))]
    )


@pytest.mark.parametrize(("status", "payload", "expected"), [
    (400, {"error": {"code": "content_filter"}}, 1),
    (400, {"error": {"code": "invalid_parameter"}}, 3),
    (400, {"error": "content_filter"}, 3),
    (400, [], 3),
    (400, None, 3),
    (429, {"error": {"code": "content_filter"}}, 3),
    (500, {"error": {"code": "server_error"}}, 3),
], ids=["filter", "ordinary", "malformed-error", "array", "invalid-json", "throttle", "server"])
def test_given_sdk_retry_override_when_prompt_is_filtered_then_only_block_retries_are_suppressed(
    status: int, payload: object, expected: int,
) -> None:
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        kwargs = {"content": b"invalid-json"} if payload is None else {"json": payload}
        return httpx.Response(status, headers={"x-should-retry": "true", "retry-after-ms": "1"}, **kwargs)

    with AzureOpenAI(
        api_key="synthetic-test-key", azure_endpoint="https://sdk-test.invalid", api_version="2024-10-21",
        max_retries=2, http_client=httpx.Client(
            transport=httpx.MockTransport(respond), event_hooks={"response": [suppress_content_filter_retry]},
        ),
    ) as sdk:
        with pytest.raises(APIStatusError):
            sdk.chat.completions.create(model="synthetic", messages=[{"role": "user", "content": "Synthetic"}])

    assert len(requests) == expected


@pytest.mark.parametrize("stage", ["planning", "generation"])
@pytest.mark.parametrize("outcome", ["prompt-filter", "ordinary-400", "filtered-empty", "filtered-partial", "normal", "length"])
def test_given_sdk_safety_signal_when_service_runs_then_characterizes_current_behavior(
    stage: str, outcome: str,
) -> None:
    requests = []
    content = '{"queries":["synthetic planned"]}' if stage == "planning" else "Synthetic answer [S1]."

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if outcome in {"prompt-filter", "ordinary-400"}:
            return httpx.Response(400, json={"error": {
                "code": "content_filter" if outcome == "prompt-filter" else "invalid_parameter",
                "message": "Synthetic rejection", "type": "invalid_request_error",
            }})
        reason = "content_filter" if outcome.startswith("filtered") else "length" if outcome == "length" else "stop"
        return httpx.Response(200, json={
            "id": "completion-synthetic", "object": "chat.completion", "created": 0,
            "model": "synthetic-model", "choices": [{
                "index": 0, "finish_reason": reason,
                "message": {"role": "assistant", "content": None if outcome == "filtered-empty" else content},
            }],
        })

    class SyntheticRetriever:
        def retrieve(self, *_args, **_kwargs):
            return [RetrievedChunk(
                "1", "document", "Synthetic evidence", "policy.pdf", "https://example.com/policy.pdf", 1,
            )]

    with AzureOpenAI(
        api_key="synthetic-test-key", azure_endpoint="https://sdk-test.invalid",
        api_version="2024-10-21", max_retries=2,
        http_client=httpx.Client(transport=httpx.MockTransport(respond)),
    ) as sdk:
        service = RagService(sdk, _registry(SyntheticRetriever()), "embedding", "chat", catalog_provider=_provider())
        try:
            if stage == "planning":
                usage = []
                if outcome in {"prompt-filter", "filtered-empty", "filtered-partial"}:
                    with pytest.raises(ContentFilteredError, match="^content_filtered$"):
                        service._plan_queries("Synthetic question", [], usage)
                    assert usage == []
                    assert len(requests) == 1
                    return
                queries = service._plan_queries("Synthetic question", [], usage)
                assert queries == (
                    ["Synthetic question"] if outcome in {"prompt-filter", "ordinary-400", "filtered-empty"}
                    else ["synthetic planned"]
                )
                assert len(usage) == (0 if outcome in {"prompt-filter", "ordinary-400"} else 1)
            else:
                def generate():
                    return service.answer_with_queries(
                        "Synthetic question", ["synthetic"], Principal("user", "tenant", frozenset({"group"})),
                        mode=RetrievalMode.FULL_TEXT,
                    )

                if outcome in {"prompt-filter", "filtered-empty", "filtered-partial"}:
                    with pytest.raises(ContentFilteredError, match="^content_filtered$"):
                        generate()
                elif outcome == "ordinary-400":
                    with pytest.raises(BadRequestError) as caught:
                        generate()
                    assert caught.value.code == ("content_filter" if outcome == "prompt-filter" else "invalid_parameter")
                else:
                    result = generate()
                    assert result["answer"] == content
                    assert len(result["citations"]) == 1
        finally:
            service.close()

    assert len(requests) == 1
    assert requests[0].url.path == "/openai/deployments/chat/chat/completions"


@pytest.mark.parametrize("answer", [
    "No reference.", "Unknown [S2].", "Zero [S0].", "Leading zero [S01].",
    "Negative [S-1].", "Space [S 1].", "Placeholder [Sx].", "Placeholder [S#].",
    "Unclosed [S1", "Valid [S1] and invalid [S2].", "Valid [S1] then [Sx].",
    "Valid [S1] then [S+1].", "Valid [S1] then [S1", "Nested [S1 [S1].",
])
def test_given_invalid_citations_when_generating_then_rejects(answer: str) -> None:
    from retrieval.service import AnswerCitationError

    client = Mock()
    client.chat.completions.create.return_value = completion(answer)
    retriever = Mock()
    retriever.retrieve.return_value = [
        RetrievedChunk("1", "doc", "Evidence", "policy.pdf", "https://example.com/policy.pdf", 1),
    ]
    service = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider())
    try:
        with pytest.raises(AnswerCitationError, match="^answer_citation_invalid$"):
            service.answer_with_queries(
                "question", ["question"], Principal("user", "tenant", frozenset({"group"})),
                mode=RetrievalMode.FULL_TEXT,
            )
        assert client.chat.completions.create.call_count == 1
    finally:
        service.close()


@pytest.mark.parametrize("answer", [
    "Grounded [S1].", "Repeated [S1] and [S1].", "[Summary] Grounded [S1].",
    "I could not find authorized evidence for this question.",
])
def test_given_valid_citations_or_refusal_when_generating_then_accepts(answer: str) -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion(answer)
    retriever = Mock()
    retriever.retrieve.return_value = [
        RetrievedChunk("1", "doc", "Evidence", "policy.pdf", "https://example.com/policy.pdf", 1),
    ]
    service = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider())
    try:
        result = service.answer_with_queries(
            "question", ["question"], Principal("user", "tenant", frozenset({"group"})),
            mode=RetrievalMode.FULL_TEXT,
        )
        assert result["answer"] == answer
        assert len(result["citations"]) == (1 if "[S1]" in answer else 0)
        assert result["policy"]["catalog_etag"] == "etag-a"
        assert result["usage"][-1]["operation"] == "answer_generation"
    finally:
        service.close()


def test_given_raw_integral_float_catalog_when_searching_then_integer_budget_is_used() -> None:
    item = {
        "id": "runtime-catalog", "type": "retrieval-runtime-catalog",
        "deploymentInstanceId": "test", "_etag": "etag-a",
        "change": {
            "operationId": "b14c1a67-9fc4-4e78-a818-e5951473d15b",
            "changedAt": "2026-09-08T00:00:00Z", "reason": "Numeric regression",
        },
        "config": {
            "retrieval": {
                "overFetchFactor": 3.0, "hybridWeights": {"vector": 1, "text": 1},
                "fullTextScoreScope": "Global",
            },
            "defaultProfile": "plain", "profiles": [{"name": "plain"}], "synonymMaps": [],
        },
    }
    snapshot = load_catalog_item(item)
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock())
    service = RagService(
        Mock(), _registry(retriever), "embedding", "chat",
        catalog_provider=SimpleNamespace(snapshot=snapshot),
    )
    try:
        result = service.search(
            "question", ["question"], Principal("user", "tenant", frozenset({"group"})),
            mode=RetrievalMode.FULL_TEXT,
        )
        assert result.chunks == ()
        assert type(snapshot.over_fetch_factor) is int
        assert type(chunks.query_items.call_args.kwargs["parameters"][0]["value"]) is int
        assert type(item["config"]["retrieval"]["overFetchFactor"]) is float
    finally:
        service.close()


def test_service_bounds_planned_queries_and_evidence() -> None:
    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["one","two","three","four"]}'),
        completion("Grounded [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.side_effect = [
        [RetrievedChunk("1", "doc", "a", "a.pdf", "https://sp.com/a.pdf", 1)],
        [RetrievedChunk("1", "doc", "a", "a.pdf", "https://sp.com/a.pdf", 1), RetrievedChunk("2", "doc", "b", "b.pdf", "https://sp.com/b.pdf", 2)],
        [],
    ]
    service = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider())

    result = service.answer(
        "follow up",
        Principal("user", "tenant", frozenset({"group"})),
        [{"role": "user", "content": "earlier"}],
    )

    assert retriever.retrieve.call_count == 3
    assert sorted(call.args[0] for call in retriever.retrieve.call_args_list) == ["one", "three", "two"]
    assert len(result["citations"]) == 2


def test_given_reload_during_planning_when_answered_then_original_policy_is_used() -> None:
    provider = _provider()
    original = provider.snapshot
    client = Mock()

    def plan(**kwargs):
        provider.snapshot = replace(
            original, etag="etag-b", operation_id="operation-b",
            hybrid_weights=(7, 3), full_text_score_scope="Local",
        )
        return completion('{"queries":["question"]}')

    client.chat.completions.create.side_effect = plan
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)],
    )
    retriever = Mock()
    retriever.retrieve.return_value = []
    service = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=provider)
    try:
        result = service.answer("question", Principal("user", "tenant", frozenset({"group"})))
        assert provider.snapshot.etag == "etag-b"
        assert retriever.retrieve.call_args.kwargs["rrf_weights"] == original.hybrid_weights
        assert retriever.retrieve.call_args.kwargs["full_text_score_scope"] == original.full_text_score_scope
        assert result["policy"]["catalog_etag"] == "etag-a"
        assert result["policy"]["catalog_operation_id"] == "operation-a"
    finally:
        service.close()


def test_service_uses_typed_office_locator_in_answer_evidence() -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion("Grounded [S1].")
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.return_value = [
        _RetrievedChunk(
            "1",
            "doc",
            "Quarterly revenue increased.",
            "results.pptx",
            "https://sp.com/results.pptx",
            RetrievalLocatorKind.SLIDE,
            "Slide 7",
            7,
            7,
        )
    ]

    RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider()).answer_with_queries(
        "What changed?",
        ["quarterly change"],
        Principal("user", "tenant", frozenset({"group"})),
    )

    evidence_prompt = client.chat.completions.create.call_args.kwargs["messages"][1]["content"]
    assert "[S1] results.pptx, Slide 7" in evidence_prompt
    assert "page 7" not in evidence_prompt


def test_service_does_not_call_answer_model_without_evidence() -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion('{"queries":["question"]}')
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.return_value = []

    result = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider()).answer(
        "question",
        Principal("user", "tenant", frozenset({"group"})),
    )

    assert result["citations"] == []
    assert client.chat.completions.create.call_count == 1


def test_service_raises_dependency_error_when_every_retrieval_task_fails(capsys) -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion('{"queries":["question"]}')
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.side_effect = RuntimeError("SYNTHETIC_PRIVATE_RETRIEVAL_FAILURE")

    with pytest.raises(RetrievalDependencyError, match="retrieval_dependency_unavailable"):
        RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider()).answer(
            "question", Principal("user", "tenant", frozenset({"group"})),
        )

    captured_logs = capsys.readouterr()
    assert "retrieval_task_failed" in captured_logs.out + captured_logs.err
    assert "SYNTHETIC_PRIVATE_RETRIEVAL_FAILURE" not in captured_logs.out + captured_logs.err


def test_full_text_mode_does_not_create_embedding() -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion('{"queries":["question"]}')
    retriever = Mock()
    retriever.retrieve.return_value = []

    RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider()).answer(
        "question",
        Principal("user", "tenant", frozenset({"group"})),
        mode=RetrievalMode.FULL_TEXT,
    )

    client.embeddings.create.assert_not_called()
    assert retriever.retrieve.call_args.args[1] == []


def test_usage_is_tracked_for_embedding_and_generation_calls() -> None:
    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["question"]}'),
        completion("Grounded [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)],
        usage=SimpleNamespace(prompt_tokens=12, completion_tokens=0),
    )
    retriever = Mock()
    retriever.retrieve.return_value = [RetrievedChunk("1", "doc", "a", "a.pdf", "https://sp.com/a.pdf", 1)]

    result = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider()).answer(
        "question",
        Principal("user", "tenant", frozenset({"group"})),
    )

    operations = {record["operation"] for record in result["usage"]}
    assert operations == {"query_planning", "embedding", "retrieval_batch", "answer_generation"}
    embedding_record = next(r for r in result["usage"] if r["operation"] == "embedding")
    retrieval_record = next(r for r in result["usage"] if r["operation"] == "retrieval_batch")
    assert embedding_record["prompt_tokens"] == 12
    assert embedding_record["model"] == "embedding"
    assert isinstance(embedding_record["latency_ms"], int)
    assert retrieval_record["retrieval_mode"] == "hybrid"


def test_usage_is_returned_even_without_evidence() -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion('{"queries":["question"]}')
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)],
        usage=SimpleNamespace(prompt_tokens=5, completion_tokens=0),
    )
    retriever = Mock()
    retriever.retrieve.return_value = []

    result = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider()).answer(
        "question",
        Principal("user", "tenant", frozenset({"group"})),
    )

    assert result["citations"] == []
    assert result["usage"][0]["operation"] == "query_planning"
    assert result["usage"][1]["operation"] == "embedding"
    assert client.chat.completions.create.call_count == 1


def test_slow_retrieval_query_is_dropped_after_timeout_budget() -> None:
    import time

    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["fast","slow"]}'),
        completion("Grounded [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()

    def _retrieve(query, *_args, **_kwargs):
        if query == "slow":
            time.sleep(0.3)
            return [RetrievedChunk("2", "doc", "b", "b.pdf", "https://sp.com/b.pdf", 2)]
        return [RetrievedChunk("1", "doc", "a", "a.pdf", "https://sp.com/a.pdf", 1)]

    retriever.retrieve.side_effect = _retrieve
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        retrieval_timeout_seconds=0.05,
        generation_timeout_seconds=1.0,
        catalog_provider=_provider(),
    )

    result = service.answer(
        "follow up",
        Principal("user", "tenant", frozenset({"group"})),
        [{"role": "user", "content": "earlier"}],
    )

    assert [c["chunk_id"] for c in result["citations"]] == ["1"]
    retrieval_record = next(r for r in result["usage"] if r["operation"] == "retrieval_batch")
    assert retrieval_record["degraded"] is True
    assert retrieval_record["timed_out"] == 1


def test_plan_queries_returns_single_for_simple_question() -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion('{"queries":["What is RAG?"]}')
    retriever = Mock()
    service = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider())

    queries, usage = service.plan_queries("What is RAG?")

    assert queries == ["What is RAG?"]
    assert usage[0]["operation"] == "query_planning"


def test_given_private_planner_error_when_planning_then_fallback_log_omits_content(capsys) -> None:
    client = Mock()
    client.chat.completions.create.side_effect = RuntimeError("SYNTHETIC_PRIVATE_PLANNER_FAILURE")
    service = RagService(client, _registry(Mock()), "embedding", "chat", catalog_provider=_provider())

    queries, usage = service.plan_queries("Synthetic original question")

    assert queries == ["Synthetic original question"]
    assert usage == []
    captured_logs = capsys.readouterr()
    assert "falling back to original question" in captured_logs.out + captured_logs.err
    assert "SYNTHETIC_PRIVATE_PLANNER_FAILURE" not in captured_logs.out + captured_logs.err


def test_plan_queries_decomposes_complex_question() -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion(
        '{"queries":["security policy details","data governance policy details"]}'
    )
    retriever = Mock()
    service = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider())

    queries, usage = service.plan_queries(
        "Compare our security policy with our data governance policy"
    )

    assert len(queries) == 2
    assert usage[0]["operation"] == "query_planning"


def test_plan_queries_uses_history_as_context() -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion('{"queries":["standalone question"]}')
    retriever = Mock()
    service = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider())

    queries, _ = service.plan_queries(
        "Tell me more about that",
        [{"role": "user", "content": "What is our leave policy?"}],
    )

    assert queries == ["standalone question"]
    call_content = client.chat.completions.create.call_args[1]["messages"][1]["content"]
    assert "history" in call_content


from retrieval.cosmos import MAX_CANDIDATE_POOL_TOTAL
from retrieval.scoring import (
    FreshnessParameters,
    ScoringFunction,
    ScoringProfile,
)
from retrieval.service import UnknownScoringProfileError


def _raw_candidate(chunk_id: str, *, source_modified_at: str | None = None) -> dict:
    return {
        "id": chunk_id,
        "documentId": f"doc-{chunk_id}",
        "sourceRunId": "run",
        "content": f"content {chunk_id}",
        "sourceName": f"{chunk_id}.pdf",
        "sourceUrl": f"https://sp.com/{chunk_id}.pdf",
        "pageStart": 1,
        "sourceModifiedAt": source_modified_at,
    }


def test_service_omitting_scoring_profile_preserves_current_top_k_order() -> None:
    """R14 regression guard: no scoring_profile → today's byte-compat path."""
    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["q"]}'),
        completion("Answer [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.return_value = [
        RetrievedChunk("a", "doc1", "a", "a.pdf", "https://sp.com/a.pdf", 1),
        RetrievedChunk("b", "doc2", "b", "b.pdf", "https://sp.com/b.pdf", 2),
    ]

    result = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider()).answer(
        "q", Principal("user", "tenant", frozenset({"group"}))
    )

    assert [c["chunk_id"] for c in result["citations"]] == ["a", "b"]
    call_kwargs = retriever.retrieve.call_args.kwargs
    assert call_kwargs["over_fetch_factor"] == 1
    assert call_kwargs["raw"] is False


def test_service_with_scoring_profile_uses_over_fetch_and_reranks_by_freshness() -> None:
    from datetime import datetime, timezone

    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["q"]}'),
        completion("Answer [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )

    older = _raw_candidate("old", source_modified_at="2020-01-01T00:00:00Z")
    newer = _raw_candidate("new", source_modified_at="2024-01-01T00:00:00Z")

    retriever = Mock()
    retriever.retrieve.return_value = [older, newer]
    retriever.to_chunks.return_value = [
        RetrievedChunk("new", "doc-new", "content new", "new.pdf", "https://sp.com/new.pdf", 1, "2024-01-01T00:00:00Z"),
        RetrievedChunk("old", "doc-old", "content old", "old.pdf", "https://sp.com/old.pdf", 1, "2020-01-01T00:00:00Z"),
    ]

    profile = ScoringProfile(
        name="fresh",
        functions=(
            ScoringFunction(
                type="freshness",
                field_name="source_modified_at",
                boost=10.0,
                interpolation="linear",
                freshness=FreshnessParameters(boosting_duration_seconds=365 * 86400),
            ),
        ),
    )
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"fresh": profile}, over_fetch_factor=5),
    )

    result = service.answer(
        "q", Principal("user", "tenant", frozenset({"group"})),
        scoring_profile="fresh",
    )

    assert retriever.retrieve.call_args.kwargs["raw"] is True
    assert retriever.retrieve.call_args.kwargs["over_fetch_factor"] == 1
    assert retriever.retrieve.call_args.kwargs["top_k"] == 25
    # First chunk after rerank must be the fresher one (whichever chunk_id the test
    # asserts is a proxy for order preservation).
    assert [c["chunk_id"] for c in result["citations"]] == ["new", "old"]


def test_service_rejects_unknown_scoring_profile_name() -> None:
    client = Mock()
    client.chat.completions.create.return_value = completion('{"queries":["q"]}')
    retriever = Mock()
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"fresh": ScoringProfile(name="fresh")}),
    )
    with pytest.raises(UnknownScoringProfileError, match="unknown_scoring_profile"):
        service.answer(
            "q", Principal("user", "tenant", frozenset({"group"})),
            scoring_profile="does-not-exist",
        )


def test_service_bounds_candidate_pool_across_sub_queries() -> None:
    """R7 M4: the pool never exceeds MAX_CANDIDATE_POOL_TOTAL across all sub-queries."""
    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["a","b","c"]}'),
        completion("Answer [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )

    def _return_many_raw(*_args, **_kwargs):
        return [_raw_candidate(f"c-{i:03d}", source_modified_at="2024-01-01T00:00:00Z") for i in range(30)]

    retriever = Mock()
    retriever.retrieve.side_effect = _return_many_raw
    retriever.to_chunks.side_effect = lambda pool: [
        RetrievedChunk(item["id"], item["documentId"], item["content"], item["sourceName"], item["sourceUrl"], 1)
        for item in pool
    ]

    profile = ScoringProfile(name="p")
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"p": profile}, over_fetch_factor=5),
    )

    result = service.answer(
        "q", Principal("user", "tenant", frozenset({"group"})),
        scoring_profile="p",
    )

    assert retriever.retrieve.call_count == 3
    assert [call.kwargs["top_k"] for call in retriever.retrieve.call_args_list] == [9, 8, 8]
    assert sum(call.kwargs["top_k"] for call in retriever.retrieve.call_args_list) == 25
    assert all(call.kwargs["over_fetch_factor"] == 1 for call in retriever.retrieve.call_args_list)
    assert len(result["citations"]) == 5


def test_service_keeps_same_chunk_ordinal_from_different_documents() -> None:
    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["q"]}'),
        completion("Answer [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    first = _raw_candidate("chunk:000000")
    first["documentId"] = "doc-a"
    second = _raw_candidate("chunk:000000")
    second["documentId"] = "doc-b"
    retriever = Mock()
    retriever.retrieve.return_value = [first, second]
    retriever.to_chunks.side_effect = lambda pool: [
        RetrievedChunk(
            item["id"], item["documentId"], item["content"],
            item["sourceName"], item["sourceUrl"], 1,
        )
        for item in pool
    ]

    result = RagService(
        client,
        _registry(retriever),
        "embedding",
        "chat",
        catalog_provider=_provider(profiles={"p": ScoringProfile(name="p")}),
    ).answer(
        "q", Principal("user", "tenant", frozenset({"group"})), scoring_profile="p",
    )

    assert {citation["document_id"] for citation in result["citations"]} == {"doc-a", "doc-b"}


def test_service_passes_rrf_weights_and_score_scope_from_config() -> None:
    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["q"]}'),
        completion("Answer [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.return_value = [
        RetrievedChunk("a", "doc", "a", "a.pdf", "https://sp.com/a.pdf", 1),
    ]

    RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(full_text_score_scope="Local", hybrid_weights=(3.0, 1.0)),
    ).answer("q", Principal("user", "tenant", frozenset({"group"})))

    kwargs = retriever.retrieve.call_args.kwargs
    assert kwargs["full_text_score_scope"] == "Local"
    assert kwargs["rrf_weights"] == (3.0, 1.0)


# --- Profile-referenced synonym selection -------------------------------------

from retrieval.synonyms import SynonymExpander, SynonymMap


def _fresh_expander() -> SynonymExpander:
    return SynonymExpander(SynonymMap.parse("geo", ["dog, puppy, canine"]))


def _profile_with_synonym_map(name: str, map_name: str | None) -> ScoringProfile:
    return ScoringProfile(name=name, synonym_map=map_name)


def _make_retriever_for_profile_path() -> Mock:
    """Retriever mock that returns raw-dict candidates AND supports to_chunks conversion."""
    candidate = {
        "id": "a", "documentId": "doc", "content": "a",
        "sourceName": "a.pdf", "sourceUrl": "https://sp.com/a.pdf",
        "pageStart": 1, "sourceModifiedAt": None,
    }
    retriever = Mock()
    retriever.retrieve.return_value = [candidate]
    retriever.to_chunks.side_effect = lambda pool: [
        RetrievedChunk(c["id"], c["documentId"], c["content"], c["sourceName"], c["sourceUrl"], 1)
        for c in pool
    ]
    return retriever


# --- Phase 2 (2026): private evaluation seams ------------------------------------


def test_retrieve_rankings_returns_reranked_chunks_without_answer_generation() -> None:
    from datetime import datetime, timezone

    older = _raw_candidate("old", source_modified_at="2020-01-01T00:00:00Z")
    newer = _raw_candidate("new", source_modified_at="2024-01-01T00:00:00Z")

    client = Mock()
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.return_value = [older, newer]
    retriever.to_chunks.return_value = [
        RetrievedChunk("new", "doc-new", "content new", "new.pdf", "https://sp.com/new.pdf", 1, "2024-01-01T00:00:00Z"),
        RetrievedChunk("old", "doc-old", "content old", "old.pdf", "https://sp.com/old.pdf", 1, "2020-01-01T00:00:00Z"),
    ]
    profile = ScoringProfile(
        name="fresh",
        functions=(
            ScoringFunction(
                type="freshness",
                field_name="source_modified_at",
                boost=10.0,
                interpolation="linear",
                freshness=FreshnessParameters(boosting_duration_seconds=365 * 86400),
            ),
        ),
    )
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"fresh": profile}),
    )

    chunks = service.retrieve_rankings(
        "q", ["q"], Principal("user", "tenant", frozenset({"group"})),
        mode=RetrievalMode.HYBRID, scoring_profile="fresh",
        evaluation_as_of=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    assert client.chat.completions.create.call_count == 0
    assert [chunk.chunk_id for chunk in chunks] == ["new", "old"]


def test_retrieve_rankings_rejects_naive_evaluation_as_of() -> None:
    from datetime import datetime

    client = Mock()
    retriever = Mock()
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"p": ScoringProfile(name="p")}),
    )
    with pytest.raises(ValueError, match="timezone_aware"):
        service.retrieve_rankings(
            "q", ["q"], Principal("user", "tenant", frozenset({"group"})),
            scoring_profile="p",
            evaluation_as_of=datetime(2025, 1, 1),
        )


def test_retrieve_evaluation_pool_reuses_pool_across_multiple_profiles() -> None:
    """REQ-12: one Cosmos fetch, multiple profiles + shared clock."""
    from datetime import datetime, timezone

    older = _raw_candidate("old", source_modified_at="2020-01-01T00:00:00Z")
    newer = _raw_candidate("new", source_modified_at="2024-01-01T00:00:00Z")

    client = Mock()
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.return_value = [older, newer]
    retriever.to_chunks.side_effect = lambda pool: [
        RetrievedChunk(
            item["id"], item["documentId"], item["content"],
            item["sourceName"], item["sourceUrl"], 1,
            item.get("sourceModifiedAt"),
        )
        for item in pool
    ]

    baseline = ScoringProfile(
        name="baseline",
        functions=(
            ScoringFunction(
                type="freshness", field_name="source_modified_at",
                boost=10.0, interpolation="linear",
                freshness=FreshnessParameters(boosting_duration_seconds=10 * 365 * 86400),
            ),
        ),
        function_aggregation="sum",
    )
    candidate_profile = ScoringProfile(
        name="candidate",
        functions=(
            ScoringFunction(
                type="freshness", field_name="source_modified_at",
                boost=10.0, interpolation="linear",
                freshness=FreshnessParameters(boosting_duration_seconds=10 * 365 * 86400),
            ),
        ),
        function_aggregation="maximum",
    )
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"baseline": baseline, "candidate": candidate_profile}),
    )

    pool = service.retrieve_evaluation_pool(
        "q", ["q"], Principal("user", "tenant", frozenset({"group"})),
        scoring_profile="baseline",
    )
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    baseline_chunks = pool.rerank(now)
    candidate_chunks = pool.rerank(now, override_profile=candidate_profile)

    # Cosmos was queried exactly once for the entire evaluation.
    assert retriever.retrieve.call_count == 1
    assert [chunk.chunk_id for chunk in baseline_chunks] == ["new", "old"]
    assert [chunk.chunk_id for chunk in candidate_chunks] == ["new", "old"]


def test_retrieve_evaluation_pool_requires_explicit_profile_name() -> None:
    client = Mock()
    retriever = Mock()
    service = RagService(client, _registry(retriever), "embedding", "chat", catalog_provider=_provider())
    with pytest.raises(UnknownScoringProfileError):
        service.retrieve_evaluation_pool(
            "q", ["q"], Principal("user", "tenant", frozenset({"group"})),
            scoring_profile="not-configured",
        )


def test_retrieve_evaluation_pool_fails_closed_when_all_retrievals_fail() -> None:
    """Evaluation must preserve production RetrievalDependencyError semantics."""
    from datetime import datetime, timezone

    client = Mock()
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.side_effect = RuntimeError("cosmos_unavailable")
    profile = ScoringProfile(name="p")
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"p": profile}),
    )
    with pytest.raises(RetrievalDependencyError):
        service.retrieve_evaluation_pool(
            "q", ["q"], Principal("user", "tenant", frozenset({"group"})),
            scoring_profile="p",
        )


def test_retrieve_evaluation_pool_passes_acl_ids_unchanged() -> None:
    """Evaluation must forward the caller's ACL identities to the retriever."""
    client = Mock()
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    retriever = Mock()
    retriever.retrieve.return_value = [_raw_candidate("only")]
    profile = ScoringProfile(name="p")
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"p": profile}),
    )
    principal = Principal("user", "tenant", frozenset({"g1", "g2", "g3"}))
    service.retrieve_evaluation_pool(
        "q", ["q"], principal, scoring_profile="p",
    )
    assert retriever.retrieve.call_count == 1
    acl_ids = retriever.retrieve.call_args.args[2]
    assert sorted(acl_ids) == ["g1", "g2", "g3"]


def test_evaluation_pool_snapshot_is_independent_of_source_mutations() -> None:
    """Deep-copied snapshot must isolate the pool from later Cosmos response edits."""
    from datetime import datetime, timezone

    client = Mock()
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    fresh = _raw_candidate("only", source_modified_at="2024-01-01T00:00:00Z")
    # Give the candidate a nested list so we can detect shared references.
    fresh["sectionPath"] = ["Header"]
    retriever = Mock()
    retriever.retrieve.return_value = [fresh]
    retriever.to_chunks.side_effect = lambda pool: [
        RetrievedChunk(
            item["id"], item["documentId"], item["content"],
            item["sourceName"], item["sourceUrl"], 1,
            item.get("sourceModifiedAt"),
        )
        for item in pool
    ]
    profile = ScoringProfile(name="p")
    service = RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles={"p": profile}),
    )
    pool = service.retrieve_evaluation_pool(
        "q", ["q"], Principal("u", "t", frozenset({"g"})),
        scoring_profile="p",
    )
    # Mutating the retriever's returned list post-fetch must not affect the pool.
    fresh["sectionPath"].append("Injected")
    fresh["content"] = "REPLACED"

    reranked = pool.rerank(datetime(2025, 1, 1, tzinfo=timezone.utc))
    assert [chunk.chunk_id for chunk in reranked] == ["only"]
    # The snapshot preserved the original nested list and content.
    assert pool.pool[0]["sectionPath"] == ["Header"]
    assert pool.pool[0]["content"] != "REPLACED"


def _service_with_expander(
    retriever,
    client,
    *,
    profile: ScoringProfile | None = None,
    map_registered: bool = True,
) -> RagService:
    profiles = {profile.name: profile} if profile else {}
    expanders = {"geo": _fresh_expander()} if map_registered else {}
    return RagService(
        client, _registry(retriever), "embedding", "chat",
        catalog_provider=_provider(profiles=profiles, synonym_expanders=expanders),
    )


def _mock_openai(retriever_returns) -> Mock:
    client = Mock()
    client.chat.completions.create.side_effect = [
        completion('{"queries":["dog policy"]}'),
        completion("Answer [S1]."),
    ]
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.0] * 3072)]
    )
    return client


def test_given_removed_map_reference_when_request_opts_in_then_expansion_stays_off() -> None:
    retriever = _make_retriever_for_profile_path()
    client = _mock_openai(retriever.retrieve.return_value)

    profile = _profile_with_synonym_map("p", None)
    _service_with_expander(
        retriever, client, profile=profile,
    ).answer(
        "dog policy",
        Principal("u", "t", frozenset({"g"})),
        scoring_profile="p",
        expand_synonyms=True,
    )

    assert retriever.retrieve.call_args.kwargs.get("search_terms") is None


def test_synonym_enabled_and_profile_map_but_no_request_flag_expands() -> None:
    retriever = _make_retriever_for_profile_path()
    client = _mock_openai(retriever.retrieve.return_value)

    profile = _profile_with_synonym_map("p", "geo")
    _service_with_expander(
        retriever, client, profile=profile,
    ).answer(
        "dog policy",
        Principal("u", "t", frozenset({"g"})),
        scoring_profile="p",
    )

    terms = retriever.retrieve.call_args.kwargs.get("search_terms")
    assert terms is not None
    assert "dog policy" in terms
    assert "puppy policy" in terms


def test_synonym_request_false_overrides_profile_default() -> None:
    retriever = _make_retriever_for_profile_path()
    client = _mock_openai(retriever.retrieve.return_value)

    profile = _profile_with_synonym_map("p", "geo")
    _service_with_expander(
        retriever, client, profile=profile,
    ).answer(
        "dog policy",
        Principal("u", "t", frozenset({"g"})),
        scoring_profile="p",
        expand_synonyms=False,
    )

    assert retriever.retrieve.call_args.kwargs.get("search_terms") is None


def test_synonym_request_true_without_profile_map_is_noop() -> None:
    retriever = _make_retriever_for_profile_path()
    client = _mock_openai(retriever.retrieve.return_value)

    profile = _profile_with_synonym_map("p", None)
    _service_with_expander(
        retriever, client, profile=profile,
    ).answer(
        "dog policy",
        Principal("u", "t", frozenset({"g"})),
        scoring_profile="p",
        expand_synonyms=True,
    )

    assert retriever.retrieve.call_args.kwargs.get("search_terms") is None


def test_synonym_omitted_scoring_profile_preserves_byte_compat_path() -> None:
    retriever = Mock()
    retriever.retrieve.return_value = [
        RetrievedChunk("a", "doc", "a", "a.pdf", "https://sp.com/a.pdf", 1),
    ]
    client = _mock_openai(retriever.retrieve.return_value)

    _service_with_expander(
        retriever, client, profile=None,
    ).answer(
        "dog policy",
        Principal("u", "t", frozenset({"g"})),
    )

    assert retriever.retrieve.call_args.kwargs.get("search_terms") is None