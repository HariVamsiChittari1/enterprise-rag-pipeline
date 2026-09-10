from __future__ import annotations

from unittest.mock import Mock

import pytest

from retrieval.cosmos import (
    MAX_CANDIDATE_POOL_TOTAL,
    RetrievalLocatorKind,
    RetrievalMode,
    SecureCosmosRetriever,
)
from retrieval.pipeline import citation_location, citation_url


def candidate() -> dict[str, object]:
    return {
        "id": "chunk",
        "documentId": "document",
        "sourceRunId": "sharepoint-drive:run1",
        "content": "authorized content",
        "sourceName": "document.pdf",
        "sourceUrl": "https://example.sharepoint.com/sites/docs/document.pdf",
        "locatorKind": "page",
        "locatorLabel": "Page 2",
        "locatorOrdinalStart": 2,
        "locatorOrdinalEnd": 2,
    }


def active_manifest(**overrides: object) -> dict[str, object]:
    manifest: dict[str, object] = {
        "schemaVersion": 1,
        "recordType": "source_document",
        "status": "ready",
    }
    manifest.update(overrides)
    return manifest


@pytest.mark.parametrize("mode", list(RetrievalMode))
def test_every_ranked_query_filters_acl_before_ranking(mode: RetrievalMode) -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate()]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest()
    retriever = SecureCosmosRetriever(chunks, manifests)

    results = retriever.retrieve(
        "policy",
        [0.1, 0.2],
        ["group-2", "group-1"],
        mode=mode,
    )

    assert [result.content for result in results] == ["authorized content"]
    query = chunks.query_items.call_args.kwargs["query"]
    assert "EXISTS" in query
    assert "ARRAY_CONTAINS(@principalIds, gid)" in query
    assert query.index("WHERE") < query.index("ORDER BY")
    parameters = {
        parameter["name"]: parameter["value"]
        for parameter in chunks.query_items.call_args.kwargs["parameters"]
    }
    assert parameters["@principalIds"] == ["group-1", "group-2"]
    manifests.read_item.assert_called_once_with(
        item="document", partition_key="sharepoint-drive:run1",
    )


def test_non_ready_document_is_not_returned() -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate()]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest(status="failed")

    assert SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"]
    ) == []


def test_citation_uses_active_manifest_source_name_with_legacy_fallback() -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate()]
    manifests = Mock()
    manifests.read_item.side_effect = [
        active_manifest(sourceName="renamed.pdf"),
        active_manifest(),
    ]
    retriever = SecureCosmosRetriever(chunks, manifests)

    renamed = retriever.retrieve("policy", [0.1], ["group"])
    legacy = retriever.retrieve("policy", [0.1], ["group"])

    assert renamed[0].source_name == "renamed.pdf"
    assert legacy[0].source_name == "document.pdf"


@pytest.mark.parametrize(
    "manifest",
    [
        active_manifest(recordType="visual_manifest_page"),
        active_manifest(schemaVersion=2),
    ],
)
def test_non_source_or_non_v1_manifest_is_not_returned(
    manifest: dict[str, object],
) -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate()]
    manifests = Mock()
    manifests.read_item.return_value = manifest

    assert SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"]
    ) == []


@pytest.mark.parametrize(
    ("kind", "label", "source_name", "source_url", "expected_url"),
    [
        (
            "page",
            "Page 2",
            "document.pdf",
            "https://example.sharepoint.com/sites/docs/document.pdf",
            "https://example.sharepoint.com/sites/docs/document.pdf#page=2",
        ),
        (
            "page",
            "Rendered page 2",
            "document.docx",
            "https://example.sharepoint.com/sites/docs/document.docx?action=default",
            "https://example.sharepoint.com/sites/docs/document.docx?action=default",
        ),
        (
            "section",
            "Section 2",
            "document.pdf",
            "https://example.sharepoint.com/sites/docs/document.pdf",
            "https://example.sharepoint.com/sites/docs/document.pdf",
        ),
        (
            "slide",
            "Slide 2",
            "document.pdf",
            "https://example.sharepoint.com/sites/docs/document.pdf",
            "https://example.sharepoint.com/sites/docs/document.pdf",
        ),
        (
            "worksheet",
            "Summary",
            "document.pdf",
            "https://example.sharepoint.com/sites/docs/document.pdf",
            "https://example.sharepoint.com/sites/docs/document.pdf",
        ),
    ],
)
def test_typed_locator_drives_format_appropriate_citation(
    kind: str,
    label: str,
    source_name: str,
    source_url: str,
    expected_url: str,
) -> None:
    chunks = Mock()
    item = candidate()
    item["locatorKind"] = kind
    item["locatorLabel"] = label
    item["sourceName"] = source_name
    item["sourceUrl"] = source_url
    chunks.query_items.return_value = [item]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest()

    result = SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"]
    )[0]

    assert result.locator_kind is RetrievalLocatorKind(kind)
    assert citation_location(result) == label
    assert citation_url(result) == expected_url


def test_empty_principals_fail_before_query() -> None:
    chunks = Mock()
    with pytest.raises(ValueError, match="principal_ids_required"):
        SecureCosmosRetriever(chunks, Mock()).retrieve("policy", [0.1], [])
    chunks.query_items.assert_not_called()


def test_acl_disabled_omits_filter_and_accepts_empty_principals() -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate()]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest()
    retriever = SecureCosmosRetriever(chunks, manifests, acl_enabled=False)

    results = retriever.retrieve("policy", [0.1, 0.2], [], mode=RetrievalMode.HYBRID)

    assert len(results) == 1
    query = chunks.query_items.call_args.kwargs["query"]
    assert "allowedGroupIds" not in query
    assert "@principalIds" not in query


def test_projection_includes_source_modified_at_and_returns_field() -> None:
    row = candidate()
    row["sourceModifiedAt"] = "2024-05-01T00:00:00Z"
    chunks = Mock()
    chunks.query_items.return_value = [row]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest()

    result = SecureCosmosRetriever(chunks, manifests, acl_enabled=False).retrieve(
        "policy", [0.1], []
    )[0]

    query = chunks.query_items.call_args.kwargs["query"]
    assert "c.sourceModifiedAt" in query
    assert result.source_modified_at == "2024-05-01T00:00:00Z"


def test_raw_projection_excludes_ingestion_only_token_count() -> None:
    row = candidate()
    row["tokenCount"] = 321
    chunks = Mock()
    chunks.query_items.return_value = [row]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest()
    retriever = SecureCosmosRetriever(chunks, manifests, acl_enabled=False)

    raw = retriever.retrieve("policy", [0.1], [], raw=True)

    assert "c.tokenCount" not in chunks.query_items.call_args.kwargs["query"]


def test_every_ranked_query_filters_retrievable_chunks_before_rank() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("policy", [0.1], [], mode=RetrievalMode.VECTOR)

    query = chunks.query_items.call_args.kwargs["query"]
    assert "WHERE c.isRetrievable = true" in query
    assert query.index("c.isRetrievable = true") < query.index("ORDER BY")


def test_hybrid_weighted_rrf_adds_rrf_weights_parameter_only_when_supplied() -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate()]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest()
    retriever = SecureCosmosRetriever(chunks, manifests, acl_enabled=False)

    retriever.retrieve("policy", [0.1], [], rrf_weights=(2.0, 1.0))
    weighted_query = chunks.query_items.call_args.kwargs["query"]
    weighted_params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert "@rrfWeights" in weighted_query
    assert weighted_params["@rrfWeights"] == [2.0, 1.0]

    retriever.retrieve("policy", [0.1], [])
    unweighted_query = chunks.query_items.call_args.kwargs["query"]
    unweighted_params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert "@rrfWeights" not in unweighted_query
    assert "@rrfWeights" not in unweighted_params


def test_over_fetch_multiplies_top_k_and_respects_global_cap() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("policy", [0.1], [], top_k=5, over_fetch_factor=5)
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert params["@topK"] == 25

    retriever.retrieve("policy", [0.1], [], top_k=20, over_fetch_factor=5)
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert params["@topK"] == MAX_CANDIDATE_POOL_TOTAL


def test_full_text_score_scope_is_passed_through_as_kwarg() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("policy", [0.1], [], full_text_score_scope="Global")
    assert chunks.query_items.call_args.kwargs["full_text_score_scope"] == "Global"

    retriever.retrieve("policy", [0.1], [])
    assert "full_text_score_scope" not in chunks.query_items.call_args.kwargs


def test_invalid_full_text_score_scope_is_rejected() -> None:
    retriever = SecureCosmosRetriever(Mock(), Mock(), acl_enabled=False)
    with pytest.raises(ValueError, match="full_text_score_scope_invalid"):
        retriever.retrieve("policy", [0.1], [], full_text_score_scope="Both")


def test_raw_mode_returns_candidate_dicts_ready_for_rerank() -> None:
    row = candidate()
    row["sourceModifiedAt"] = "2024-05-01T00:00:00Z"
    chunks = Mock()
    chunks.query_items.return_value = [row]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest(sourceName="renamed.pdf")
    retriever = SecureCosmosRetriever(chunks, manifests, acl_enabled=False)

    raw = retriever.retrieve("policy", [0.1], [], raw=True)

    assert isinstance(raw, list) and isinstance(raw[0], dict)
    assert raw[0]["sourceName"] == "renamed.pdf"
    assert raw[0]["sourceModifiedAt"] == "2024-05-01T00:00:00Z"


def test_synonym_term_would_appear_only_in_parameters_not_raw_sql() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("acronym-expanded query", [0.1], [])
    query = chunks.query_items.call_args.kwargs["query"]
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert "acronym-expanded query" not in query
    assert params["@searchText"] == "acronym-expanded query"


# --- Phase 2b: multi-term FullTextScore SQL generation ----------------------------


def test_hybrid_multi_term_uses_one_full_text_score_with_all_bound_terms() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("dog", [0.1], [], search_terms=["dog", "puppy", "canine"])
    query = chunks.query_items.call_args.kwargs["query"]
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert query.count("FullTextScore(") == 1
    assert "FullTextScore(c.searchableText, @t0, @t1, @t2)" in query
    assert params["@t0"] == "dog"
    assert params["@t1"] == "puppy"
    assert params["@t2"] == "canine"


def test_multi_term_hybrid_never_leaks_synonym_values_into_raw_sql() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("dog", [0.1], [], search_terms=["dog", "puppy", "canine"])
    query = chunks.query_items.call_args.kwargs["query"]
    for term in ("dog", "puppy", "canine"):
        # Term values MUST appear only inside `parameters`, never in the raw SQL text.
        assert term not in query


def test_multi_term_weighted_rrf_keeps_stable_vector_text_weight_pair() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve(
        "dog", [0.1], [],
        search_terms=["dog", "puppy", "canine"],
        rrf_weights=(2.0, 1.0),
    )
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert params["@rrfWeights"] == [2.0, 1.0]


def test_search_terms_are_capped_at_max_terms_per_query() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    many_terms = [f"t{i}" for i in range(20)]
    retriever.retrieve("dog", [0.1], [], search_terms=many_terms)
    query = chunks.query_items.call_args.kwargs["query"]
    assert query.count("FullTextScore(") == 1
    assert "@t7" in query
    assert "@t8" not in query


def test_full_text_mode_with_multi_terms_uses_direct_full_text_score() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve(
        "dog", [], [], mode=RetrievalMode.FULL_TEXT,
        search_terms=["dog", "canine"],
    )
    query = chunks.query_items.call_args.kwargs["query"]
    assert "RRF(" not in query
    assert "ORDER BY RANK FullTextScore(c.searchableText, @t0, @t1)" in query


def test_single_term_search_terms_uses_phase_2a_single_search_text_path() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("dog", [0.1], [], search_terms=["dog"])
    query = chunks.query_items.call_args.kwargs["query"]
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert "@searchText" in query
    assert "@t0" not in params
    assert params["@searchText"] == "dog"


def test_search_terms_none_preserves_phase_2a_byte_compat() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("dog", [0.1], [])  # no search_terms kwarg
    query = chunks.query_items.call_args.kwargs["query"]
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    assert "@searchText" in query
    assert "@t0" not in params