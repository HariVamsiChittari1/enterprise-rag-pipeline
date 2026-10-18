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
        "schemaVersion": 1,
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


def audio_candidate() -> dict[str, object]:
    return candidate() | {
        "locatorKind": "time", "locatorLabel": "00:00-00:02",
        "startMs": 0, "endMs": 2000, "lifecycleGeneration": 1,
        "documentKey": "source:run:document", "sourceName": "recording.wav",
        "sourceUrl": "https://example.invalid/recording.wav",
        "audio": {
            "durationMs": 3000, "channelCount": 1, "locale": "en-US", "mode": "fast",
            "apiVersion": "2025-10-15", "profileVersion": "1", "sourceVersion": "etag-1",
            "sourceContentHash": "a" * 64,
        },
    }


def audio_manifest() -> dict[str, object]:
    row = audio_candidate()
    return active_manifest(
        mimeType="audio/wav", audio=row["audio"], lifecycleGeneration=1,
        documentId=row["documentId"], sourceRunId=row["sourceRunId"],
        documentKey=row["documentKey"], eTag="etag-1", contentHash="a" * 64,
        allowedGroupIds=["group"],
    )


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("mode", list(RetrievalMode))
def test_audio_transcript_is_retrieved_and_bound_to_manifest(raw: bool, mode: RetrievalMode) -> None:
    chunks, manifests = Mock(), Mock()
    chunks.query_items.return_value = [audio_candidate()]
    manifests.read_item.return_value = audio_manifest()
    results = SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], mode=mode, raw=raw,
    )
    assert len(results) == 1
    if not raw:
        assert (results[0].start_ms, results[0].end_ms, results[0].evidence_version) == (0, 2000, "etag-1")
    query = chunks.query_items.call_args.kwargs["query"]
    assert "OR (c.schemaVersion = 1 AND c.locatorKind = 'time')" in query.split("ORDER BY")[0]


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("overrides", [
    {"lifecycleGeneration": 2}, {"lifecycleGeneration": True},
    {"eTag": "updated"}, {"contentHash": "b" * 64},
    {"status": "retired"}, {"documentKey": "wrong"}, {"audio": {}},
    {"schemaVersion": 2}, {"mimeType": "application/pdf"},
])
def test_audio_manifest_validation_fails_closed(raw: bool, overrides: dict[str, object]) -> None:
    chunks, manifests = Mock(), Mock()
    chunks.query_items.return_value = [audio_candidate()]
    manifests.read_item.return_value = audio_manifest() | overrides
    assert SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], raw=raw,
    ) == []


@pytest.mark.parametrize("overrides", [
    {"startMs": True}, {"startMs": -1}, {"endMs": 3001}, {"endMs": 0},
    {"endMs": 1.5}, {"locatorKind": "page"}, {"schemaVersion": 2}, {"schemaVersion": 3},
])
def test_audio_rejects_invalid_temporal_contract(overrides: dict[str, object]) -> None:
    retriever = SecureCosmosRetriever(Mock(), Mock())
    with pytest.raises(ValueError, match="invalid_retrieval_record"):
        retriever.to_chunks([audio_candidate() | overrides])


def test_audio_accepts_batch_transcription_profile() -> None:
    retriever = SecureCosmosRetriever(Mock(), Mock())
    candidate = audio_candidate()
    candidate["audio"] = dict(candidate["audio"]) | {"mode": "batch", "apiVersion": "2024-11-15"}
    result = retriever.to_chunks([candidate])
    assert len(result) == 1
    assert (result[0].start_ms, result[0].end_ms) == (0, 2000)


@pytest.mark.parametrize("overrides", [
    {"mode": "batch", "apiVersion": "2025-10-15"},  # mismatched pair
    {"mode": "fast", "apiVersion": "2024-11-15"},
    {"mode": "unknown", "apiVersion": "2024-11-15"},
])
def test_audio_rejects_mismatched_transcription_profile(overrides: dict[str, object]) -> None:
    retriever = SecureCosmosRetriever(Mock(), Mock())
    candidate = audio_candidate()
    candidate["audio"] = dict(candidate["audio"]) | overrides
    with pytest.raises(ValueError, match="invalid_retrieval_record"):
        retriever.to_chunks([candidate])


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("mode", list(RetrievalMode))
def test_shared_schema_retrieves_mixed_document_and_transcript_text(
    raw: bool, mode: RetrievalMode,
) -> None:
    chunks, manifests = Mock(), Mock()
    chunks.query_items.return_value = [candidate(), audio_candidate()]
    manifests.read_item.side_effect = [active_manifest(), audio_manifest()]
    results = SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], mode=mode, raw=raw,
    )
    assert [row["locatorKind"] if raw else row.locator_kind.value for row in results] == [
        "page", "time",
    ]


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("schema_version", [2, 99, True, None])
@pytest.mark.parametrize("is_audio", [False, True])
def test_matching_unsupported_manifest_and_chunk_schemas_are_rejected(
    raw: bool, schema_version: object, is_audio: bool,
) -> None:
    chunks, manifests = Mock(), Mock()
    row = audio_candidate() if is_audio else candidate()
    manifest = audio_manifest() if is_audio else active_manifest()
    chunks.query_items.return_value = [row | {"schemaVersion": schema_version}]
    manifests.read_item.return_value = manifest | {"schemaVersion": schema_version}
    assert SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], raw=raw,
    ) == []


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("mime_type", ["audio/wav", "audio/mpeg", "audio/flac"])
@pytest.mark.parametrize("metadata_state", ["present", "null", "missing"])
def test_document_locator_cannot_bypass_audio_manifest_guards(
    raw: bool, mime_type: str, metadata_state: str,
) -> None:
    chunks, manifests = Mock(), Mock()
    chunks.query_items.return_value = [candidate()]
    manifest = audio_manifest() | {"mimeType": mime_type}
    if metadata_state == "missing":
        manifest.pop("audio")
    elif metadata_state == "null":
        manifest["audio"] = None
    manifests.read_item.return_value = manifest
    assert SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], raw=raw,
    ) == []


def test_acl_disabled_retrieves_audio_without_principals_or_acl_freshness() -> None:
    chunks, manifests = Mock(), Mock()
    chunks.query_items.return_value = [audio_candidate()]
    manifests.read_item.return_value = audio_manifest() | {
        "allowedGroupIds": None,
        "aclEvaluatedAt": None,
    }
    retriever = SecureCosmosRetriever(
        chunks,
        manifests,
        acl_enabled=False,
    )

    results = retriever.retrieve("policy", [0.1], [])

    assert [result.locator_kind for result in results] == [RetrievalLocatorKind.TIME]
    query = chunks.query_items.call_args.kwargs["query"]
    assert "allowedGroupIds" not in query
    assert "@principalIds" not in query


def test_audio_transcript_does_not_expire_by_file_format() -> None:
    chunks, manifests = Mock(), Mock()
    chunks.query_items.return_value = [audio_candidate()]
    manifests.read_item.return_value = audio_manifest() | {
        "aclEvaluatedAt": "2020-01-01T00:00:00+00:00",
        "sourceVerifiedAt": "2020-01-01T00:00:00+00:00",
    }

    results = SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"],
    )

    assert len(results) == 1


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


def test_shared_link_feedback_never_authorizes_retrieval() -> None:
    class AclFilteringChunks:
        def __init__(self) -> None:
            self.row = candidate() | {
                "allowedGroupIds": ["allowed-group"],
                "aclFeedback": ["SharedLink"],
            }
            self.queries: list[str] = []

        def query_items(
            self,
            *,
            query: str,
            parameters: list[dict[str, object]],
            enable_cross_partition_query: bool,
        ) -> list[dict[str, object]]:
            assert enable_cross_partition_query is True
            self.queries.append(query)
            parameter_values = {
                parameter["name"]: parameter["value"]
                for parameter in parameters
            }
            principal_ids = set(parameter_values["@principalIds"])
            return (
                [self.row]
                if principal_ids.intersection(self.row["allowedGroupIds"])
                else []
            )

    chunks = AclFilteringChunks()
    manifests = Mock()
    manifests.read_item.return_value = active_manifest(
        allowedGroupIds=["allowed-group"],
        aclStatus="Verified",
        aclFeedback=["SharedLink"],
    )
    retriever = SecureCosmosRetriever(chunks, manifests)

    allowed = retriever.retrieve("policy", [0.1], ["allowed-group"])
    denied = retriever.retrieve("policy", [0.1], ["outside-group"])

    assert [result.content for result in allowed] == ["authorized content"]
    assert denied == []
    assert all("aclFeedback" not in query for query in chunks.queries)
    assert all(
        "ARRAY_CONTAINS(@principalIds, gid)" in query
        for query in chunks.queries
    )
    manifests.read_item.assert_called_once_with(
        item="document",
        partition_key="sharepoint-drive:run1",
    )


def test_non_ready_document_is_not_returned() -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate()]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest(status="failed")

    assert SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"]
    ) == []


@pytest.mark.parametrize("mode", list(RetrievalMode))
@pytest.mark.parametrize("raw", [False, True])
def test_supported_document_and_transcript_filters_precede_ranking(
    mode: RetrievalMode, raw: bool,
) -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate()]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest()

    assert len(SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], mode=mode, raw=raw,
    )) == 1
    query = chunks.query_items.call_args.kwargs["query"]
    assert "c.schemaVersion = 1" in query.split("ORDER BY")[0]
    assert "c.locatorKind IN ('page', 'section', 'slide', 'worksheet')" in query
    assert "c.locatorKind = 'time'" in query.split("ORDER BY")[0]


@pytest.mark.parametrize("mode", list(RetrievalMode))
def test_source_eligibility_filters_candidates_before_ranking(mode: RetrievalMode) -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate() | {"sourceId": "source-a"}]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest(sourceId="source-a")

    results = SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], mode=mode,
        eligible_source_ids=["source-a"],
    )

    assert len(results) == 1
    query = chunks.query_items.call_args.kwargs["query"]
    assert "ARRAY_CONTAINS(@eligibleSourceIds, c.sourceId)" in query
    assert query.index("c.sourceId") < query.index("ORDER BY")
    parameters = {
        parameter["name"]: parameter["value"]
        for parameter in chunks.query_items.call_args.kwargs["parameters"]
    }
    assert parameters["@eligibleSourceIds"] == ["source-a"]


def test_production_retriever_requires_source_eligibility_before_query() -> None:
    chunks = Mock()
    retriever = SecureCosmosRetriever(
        chunks, Mock(), require_source_eligibility=True,
    )

    with pytest.raises(ValueError, match="source_eligibility_required"):
        retriever.retrieve("policy", [0.1], ["group"])

    chunks.query_items.assert_not_called()


def test_candidate_source_id_must_match_ready_manifest() -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate() | {"sourceId": "source-a"}]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest(sourceId="source-b")

    results = SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], eligible_source_ids=["source-a"],
    )

    assert results == []


def test_candidate_from_source_outside_request_snapshot_is_rejected() -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate() | {"sourceId": "source-b"}]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest(sourceId="source-b")

    results = SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], eligible_source_ids=["source-a"],
    )

    assert results == []
    manifests.read_item.assert_not_called()


@pytest.mark.parametrize("mode", list(RetrievalMode))
@pytest.mark.parametrize("eligible_source", ["source-a", "source-b"])
def test_mixed_source_candidates_return_only_eligible_source_with_acl_filter(
    mode: RetrievalMode, eligible_source: str,
) -> None:
    sources = ("source-a", "source-b")
    chunks = Mock()
    chunks.query_items.return_value = [
        candidate() | {
            "id": f"chunk-{source}", "documentId": f"document-{source}",
            "sourceId": source, "sourceRunId": f"{source}:run",
            "sourceName": f"{source}.pdf",
            "sourceUrl": f"https://tenant.sharepoint.com/sites/{source}/document.pdf",
        }
        for source in sources
    ]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest(sourceId=eligible_source)
    retriever = SecureCosmosRetriever(chunks, manifests, require_source_eligibility=True)

    results = retriever.retrieve(
        "policy", [0.1], [f"group-{eligible_source}"], mode=mode,
        eligible_source_ids=[eligible_source],
    )

    assert len(results) == 1
    assert results[0].source_id == eligible_source
    assert results[0].source_name == f"{eligible_source}.pdf"
    assert results[0].source_url == f"https://tenant.sharepoint.com/sites/{eligible_source}/document.pdf"
    manifests.read_item.assert_called_once_with(
        item=f"document-{eligible_source}", partition_key=f"{eligible_source}:run",
    )
    query = chunks.query_items.call_args.kwargs["query"]
    where = query.split("ORDER BY")[0]
    assert "ARRAY_CONTAINS(@eligibleSourceIds, c.sourceId)" in where
    assert "ARRAY_CONTAINS(@principalIds, gid)" in where
    parameters = {
        parameter["name"]: parameter["value"]
        for parameter in chunks.query_items.call_args.kwargs["parameters"]
    }
    assert parameters["@eligibleSourceIds"] == [eligible_source]
    assert parameters["@principalIds"] == [f"group-{eligible_source}"]


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("overrides", [
    {"locatorKind": "time"},
    {"schemaVersion": 2},
    {"schemaVersion": 99},
    {"schemaVersion": True},
    {"locatorOrdinalStart": True},
    {"locatorKind": "unknown"},
    {"content": ""},
    {"startMs": 0}, {"endMs": 1000},
])
def test_invalid_candidates_cannot_bypass_raw_validation(
    raw: bool, overrides: dict[str, object],
) -> None:
    chunks = Mock()
    chunks.query_items.return_value = [candidate() | overrides]
    manifests = Mock()
    manifests.read_item.return_value = active_manifest()

    assert SecureCosmosRetriever(chunks, manifests).retrieve(
        "policy", [0.1], ["group"], raw=raw,
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


def test_request_acl_policy_overrides_retriever_default() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock())

    retriever.retrieve(
        "policy",
        [0.1],
        [],
        eligible_source_ids=["source-a"],
        acl_enabled=False,
    )

    query = chunks.query_items.call_args.kwargs["query"]
    parameters = {
        parameter["name"]: parameter["value"]
        for parameter in chunks.query_items.call_args.kwargs["parameters"]
    }
    assert "c.isRetrievable = true" in query
    assert "ARRAY_CONTAINS(@eligibleSourceIds, c.sourceId)" in query
    assert "allowedGroupIds" not in query
    assert parameters["@eligibleSourceIds"] == ["source-a"]
    assert "@principalIds" not in parameters


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

    retriever.retrieve("policy", [0.1], [], raw=True)

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
    # The multi-word query tokenizes into single keyword terms bound as parameters;
    # keyword values MUST NOT appear in the raw SQL text.
    for keyword in ("acronym", "expanded", "query"):
        assert keyword not in query
        assert keyword in params.values()
    assert "@t0" in query


def test_multi_word_query_tokenizes_into_single_keyword_terms() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve(
        "What authentication topics are covered by the Password Policy?",
        [], [], mode=RetrievalMode.FULL_TEXT,
    )
    query = chunks.query_items.call_args.kwargs["query"]
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    term_values = [v for k, v in params.items() if k.startswith("@t") and k != "@topK"]
    # Stopwords are dropped; each remaining keyword becomes its own FULLTEXTSCORE term.
    assert "ORDER BY RANK FullTextScore(c.searchableText, @t0" in query
    assert {"authentication", "password", "policy"} <= set(term_values)
    assert "what" not in term_values and "the" not in term_values and "by" not in term_values


def test_keyword_tokenization_is_case_insensitive_and_deduped() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    retriever.retrieve("Password password PASSWORD access", [], [], mode=RetrievalMode.FULL_TEXT)
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    term_values = [v for k, v in params.items() if k.startswith("@t") and k != "@topK"]
    assert term_values == ["password", "access"]


def test_expander_variants_are_tokenized_into_keywords() -> None:
    chunks = Mock()
    chunks.query_items.return_value = []
    retriever = SecureCosmosRetriever(chunks, Mock(), acl_enabled=False)

    # Whole-query synonym variants (as SynonymExpander emits) tokenize and dedupe to keywords.
    retriever.retrieve(
        "vacation policy", [0.1], [],
        search_terms=["vacation policy", "annual leave policy"],
    )
    params = dict(
        (p["name"], p["value"]) for p in chunks.query_items.call_args.kwargs["parameters"]
    )
    term_values = [v for k, v in params.items() if k.startswith("@t") and k != "@topK"]
    assert term_values == ["vacation", "policy", "annual", "leave"]


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