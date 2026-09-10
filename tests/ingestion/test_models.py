from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingestion.models import (
    ActivityOutcome,
    ActivityStatus,
    CanonicalExtractionResult,
    CanonicalSegment,
    ChunkingProfile,
    ContentModality,
    DocumentStage,
    DocumentStatus,
    EmbeddingProfile,
    EnrichmentStatuses,
    Entity,
    ExtractionProvenance,
    IngestionRunRecord,
    LocatorKind,
    ModuleStatus,
    ProfileSnapshot,
    RunCounters,
    RunStage,
    RunStatus,
    SearchChunkRecord,
    Settings,
    SourceLocator,
    SourceControlRecord,
    SourceDocumentRecord,
    VisualDisposition,
    VisualCoverage,
    VisualCoverageStatus,
    VisualManifestEntry,
    VisualRelevance,
    canonical_group_ids,
    content_sha256,
    create_chunk_id,
    create_document_id,
    create_document_key,
    create_run_id,
    create_source_run_id,
    create_visual_manifest_pages,
    run_record_id,
    safe_error_from_exception,
    serialized_size_bytes,
    visual_manifest_hash,
)


UTC = "2026-08-05T12:00:00Z"


def test_identifiers_are_deterministic_and_run_scoped() -> None:
    first_document_id = create_document_id("source", "drive", "item")
    second_document_id = create_document_id("source", "drive", "item")

    assert first_document_id == second_document_id
    assert len(first_document_id) == 64
    assert create_document_key("source", "run-a", first_document_id) != create_document_key(
        "source", "run-b", first_document_id
    )
    assert create_source_run_id("source", "run-a") == "source:run-a"
    assert create_chunk_id(7) == "chunk:000007"


def test_run_id_requires_utc_and_uses_bounded_entropy_hash() -> None:
    run_id = create_run_id(datetime(2026, 8, 5, 12, tzinfo=timezone.utc), "request-id")

    assert run_id.startswith("20260805T120000Z-")
    assert len(run_id) == 33
    with pytest.raises(ValueError, match="timezone-aware"):
        create_run_id(datetime(2026, 8, 5, 12), "request-id")


def test_acl_groups_are_nonempty_sorted_and_unique() -> None:
    assert canonical_group_ids(["group-b", "group-a", "group-a"]) == ("group-a", "group-b")
    with pytest.raises(ValueError, match="at least one"):
        canonical_group_ids([])


def test_source_control_serializes_to_cosmos_field_names() -> None:
    record = SourceControlRecord(
        source_id="source",
        current_run_id="run-a",
        current_orchestration_instance_id="full-sync-test-instance",
        activated_at=UTC,
        updated_at=UTC,
    )

    assert record.to_cosmos_item() == {
        "sourceId": "source",
        "currentRunId": "run-a",
        "currentOrchestrationInstanceId": "full-sync-test-instance",
        "activatedAt": UTC,
        "updatedAt": UTC,
        "lastCompletedRunId": None,
        "id": "source-control",
        "schemaVersion": 1,
    }


def test_settings_and_run_serialize_exact_configuration_snapshot() -> None:
    profiles = ProfileSnapshot()
    settings = Settings("source", "drive", "commit-abc", profiles=profiles)
    run_id = "20260805T120000Z-730e938abe361240"
    record = IngestionRunRecord(
        source_id=settings.source_id,
        run_id=run_id,
        drive_id=settings.drive_id,
        orchestration_instance_id="full-sync-test-instance",
        status=RunStatus.RUNNING,
        stage=RunStage.DISCOVERING,
        started_at=UTC,
        activated_at=UTC,
        updated_at=UTC,
        counters=RunCounters(discovered=2, processing=1),
        profiles=settings.profiles,
        ingestion_mode="full-sync",
        id=run_record_id(run_id),
    )

    item = record.to_cosmos_item()
    assert settings.document_wave_size == 4
    assert settings.activity_attempts == 5
    assert item["counters"]["discovered"] == 2
    assert item["profiles"]["embedding"]["dimensions"] == 3_072
    assert item["profiles"]["enrichment"]["enabledModules"] == [
        "key_phrases",
        "entities",
    ]


def test_document_record_enforces_deterministic_keys() -> None:
    record = build_document_record()

    item = record.to_cosmos_item()
    assert item["sourceRunId"] == "source:run-a"
    assert item["recordType"] == "source_document"
    assert item["allowedGroupIds"] == ["group-a"]
    assert item["status"] == "discovered"


def test_visual_manifest_pages_are_deterministic_and_size_bounded() -> None:
    document = build_document_record()
    entries = tuple(
        VisualManifestEntry(
            ordinal=index,
            visual_id=f"visual-{index:03d}",
            object_type="chart",
            source_locator=SourceLocator(LocatorKind.PAGE, f"Page {index + 1}", index + 1, index + 1),
            relevance=VisualRelevance.REQUIRED,
            disposition=VisualDisposition.DESCRIBED,
            description="x" * 8_000,
            provenance=(ExtractionProvenance.DIRECT,),
        )
        for index in range(60)
    )

    pages = create_visual_manifest_pages(document, entries)
    replay = create_visual_manifest_pages(document, entries)

    assert len(pages) > 1
    assert pages == replay
    assert tuple(page.page_index for page in pages) == tuple(range(len(pages)))
    assert all(page.page_count == len(pages) for page in pages)
    assert all(page.record_type == "visual_manifest_page" for page in pages)
    assert all(serialized_size_bytes(page.to_cosmos_item()) <= 128 * 1024 for page in pages)
    assert visual_manifest_hash(pages) == visual_manifest_hash(replay)


def test_document_manifest_binding_is_atomic() -> None:
    values = document_values()
    values["visual_manifest_page_count"] = 1

    with pytest.raises(ValueError, match="manifest page count and hash"):
        SourceDocumentRecord(**values)



@pytest.mark.parametrize(
    "mime_type",
    [
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "text/markdown",
        "text/plain",
    ],
)
def test_document_record_accepts_supported_source_mime_types(mime_type: str) -> None:
    values = document_values()
    values["mime_type"] = mime_type

    record = SourceDocumentRecord(**values)

    assert record.mime_type == mime_type


def test_document_record_rejects_unsupported_source_mime_type() -> None:
    values = document_values()
    values["mime_type"] = "application/octet-stream"

    with pytest.raises(ValueError, match="MIME type is not supported"):
        SourceDocumentRecord(**values)


def test_canonical_extraction_requires_complete_visual_accounting() -> None:
    segment = CanonicalSegment(
        ordinal=0,
        text="Text plus a factual chart description.",
        locator=SourceLocator(LocatorKind.SLIDE, "Slide 1", 1, 1),
        modalities=(ContentModality.TEXT, ContentModality.VISUAL_DESCRIPTION),
        provenance=(ExtractionProvenance.DIRECT, ExtractionProvenance.RENDERED),
    )
    coverage = VisualCoverage(
        status=VisualCoverageStatus.COMPLETE,
        inventory_count=1,
        required_count=1,
        described_count=1,
        excluded_count=0,
        unsupported_count=0,
        uncovered_count=0,
    )

    result = CanonicalExtractionResult((segment,), coverage)

    assert result.segments[0].locator.kind is LocatorKind.SLIDE


def test_visual_coverage_rejects_unexplained_required_visuals() -> None:
    with pytest.raises(ValueError, match="required visual accounting"):
        VisualCoverage(
            status=VisualCoverageStatus.COMPLETE,
            inventory_count=2,
            required_count=2,
            described_count=1,
            excluded_count=0,
            unsupported_count=0,
            uncovered_count=0,
        )


def test_canonical_extraction_rejects_visual_count_without_visual_modality() -> None:
    segment = CanonicalSegment(
        ordinal=0,
        text="Text-only source segment.",
        locator=SourceLocator(LocatorKind.SECTION, "Document", 1, 1),
        modalities=(ContentModality.TEXT,),
        provenance=(ExtractionProvenance.DIRECT,),
    )
    coverage = VisualCoverage(
        status=VisualCoverageStatus.COMPLETE,
        inventory_count=1,
        required_count=1,
        described_count=1,
        excluded_count=0,
        unsupported_count=0,
        uncovered_count=0,
    )

    with pytest.raises(ValueError, match="visual descriptions do not match coverage"):
        CanonicalExtractionResult((segment,), coverage)


def test_retired_document_requires_retirement_fields_and_prior_ready_state() -> None:
    values = document_values()
    values.update(
        status=DocumentStatus.RETIRED,
        stage=DocumentStage.TERMINAL,
        ready_at=UTC,
        retired_at=UTC,
        retired_reason="superseded",
    )
    record = SourceDocumentRecord(**values)
    assert record.to_cosmos_item()["retiredReason"] == "superseded"

    missing_fields = dict(values, retired_at=None, retired_reason=None)
    with pytest.raises(ValueError, match="retired documents require"):
        SourceDocumentRecord(**missing_fields)

    never_ready = dict(values, ready_at=None)
    with pytest.raises(ValueError, match="previously ready"):
        SourceDocumentRecord(**never_ready)

    invalid_reason = dict(values, retired_reason="because_i_said_so")
    with pytest.raises(ValueError, match="retired_reason must be"):
        SourceDocumentRecord(**invalid_reason)

    unretired_with_fields = dict(
        document_values(), status=DocumentStatus.READY, ready_at=UTC,
        retired_at=UTC, retired_reason="superseded",
    )
    with pytest.raises(ValueError, match="others must omit them"):
        SourceDocumentRecord(**unretired_with_fields)


def test_chunk_record_separates_original_and_embedding_text() -> None:
    record = build_chunk_record()

    item = record.to_cosmos_item()
    assert item["content"] == "Original  content."
    assert item["embeddingText"] == "Original content."
    assert item["enrichmentStatus"] == {
        "summary": "succeeded",
        "keyPhrases": "succeeded",
        "entities": "succeeded",
    }
    assert item["isRetrievable"] is False
    assert item["lifecycleGeneration"] == 0
    assert item["locatorKind"] == "page"
    assert item["locatorLabel"] == "Page 1"
    assert item["modalities"] == ["text", "visual_description"]
    assert item["provenance"] == ["direct"]
    assert item["visualCoverage"] == "complete"


def test_lifecycle_admission_fields_are_strict() -> None:
    document = document_values()
    document["lifecycle_generation"] = True
    with pytest.raises(ValueError, match="lifecycle_generation"):
        SourceDocumentRecord(**document)

    chunk = chunk_values()
    chunk["is_retrievable"] = "true"
    with pytest.raises(ValueError, match="is_retrievable"):
        SearchChunkRecord(**chunk)

    chunk = chunk_values()
    chunk["lifecycle_generation"] = -1
    with pytest.raises(ValueError, match="lifecycle_generation"):
        SearchChunkRecord(**chunk)



def test_chunk_record_rejects_vector_dimension_mismatch() -> None:
    values = chunk_values()
    values["embedding"] = (0.0,) * 3_071

    with pytest.raises(ValueError, match="embedding length"):
        SearchChunkRecord(**values)


def test_profile_guards_reject_invalid_cosmos_vector_contract() -> None:
    with pytest.raises(ValueError, match="overlap_tokens"):
        ChunkingProfile(max_tokens=100, overlap_tokens=100)
    with pytest.raises(ValueError, match="cosine"):
        EmbeddingProfile(distance_function="dotproduct")


def test_safe_error_mapping_never_serializes_exception_text() -> None:
    error = safe_error_from_exception(RuntimeError("secret service payload"), "embedding")

    assert error.code == "internal_error"
    assert "secret" not in str(error)


def test_failed_activity_requires_safe_error() -> None:
    with pytest.raises(ValueError, match="require a safe error"):
        ActivityOutcome("a" * 64, ActivityStatus.FAILED, 0, 2)


def build_document_record() -> SourceDocumentRecord:
    return SourceDocumentRecord(**document_values())


def document_values() -> dict[str, object]:
    document_id = create_document_id("source", "drive", "item")
    return {
        "source_id": "source",
        "run_id": "run-a",
        "drive_id": "drive",
        "item_id": "item",
        "parent_item_id": "parent",
        "source_name": "fabricated.pdf",
        "source_path": "/fabricated.pdf",
        "source_url": "https://example.invalid/fabricated.pdf",
        "e_tag": "etag",
        "mime_type": "application/pdf",
        "size_bytes": 100,
        "discovery_ordinal": 1,
        "allowed_group_ids": ("group-a",),
        "acl_hash": content_sha256("group-a"),
        "acl_evaluated_at": UTC,
        "status": DocumentStatus.DISCOVERED,
        "stage": DocumentStage.DISCOVERED,
        "attempt_count": 0,
        "discovered_at": UTC,
        "updated_at": UTC,
        "id": document_id,
        "document_id": document_id,
        "source_run_id": create_source_run_id("source", "run-a"),
        "document_key": create_document_key("source", "run-a", document_id),
    }


def build_chunk_record() -> SearchChunkRecord:
    return SearchChunkRecord(**chunk_values())


def chunk_values() -> dict[str, object]:
    document_id = create_document_id("source", "drive", "item")
    content = "Original  content."
    embedding_text = "Original content."
    return {
        "source_id": "source",
        "run_id": "run-a",
        "document_id": document_id,
        "document_key": create_document_key("source", "run-a", document_id),
        "allowed_group_ids": ("group-a",),
        "source_name": "fabricated.pdf",
        "source_url": "https://example.invalid/fabricated.pdf",
        "page_start": 1,
        "page_end": 1,
        "section_path": ("Heading",),
        "locator_kind": LocatorKind.PAGE,
        "locator_label": "Page 1",
        "locator_ordinal_start": 1,
        "locator_ordinal_end": 1,
        "modalities": (ContentModality.TEXT, ContentModality.VISUAL_DESCRIPTION),
        "provenance": (ExtractionProvenance.DIRECT,),
        "visual_coverage": VisualCoverageStatus.COMPLETE,
        "chunk_index": 0,
        "created_at": UTC,
        "content": content,
        "content_hash": content_sha256(content),
        "embedding_text": embedding_text,
        "searchable_text": embedding_text,
        "token_count": 3,
        "enrichment_status": EnrichmentStatuses(
            ModuleStatus.SUCCEEDED,
            ModuleStatus.SUCCEEDED,
            ModuleStatus.SUCCEEDED,
        ),
        "summary": "Fabricated summary.",
        "key_phrases": ("content",),
        "entities": (Entity("Original", "Concept", confidence=0.9),),
        "language_code": "en",
        "embedding": (0.0,) * 3_072,
        "embedded_at": UTC,
        "id": create_chunk_id(0),
        "source_run_id": create_source_run_id("source", "run-a"),
    }


def test_document_and_chunk_accept_source_modified_at_optional() -> None:
    doc_values = document_values()
    doc_values["source_modified_at"] = "2024-05-01T00:00:00Z"
    record = SourceDocumentRecord(**doc_values)
    assert record.to_cosmos_item()["sourceModifiedAt"] == "2024-05-01T00:00:00Z"

    chunk = chunk_values()
    chunk["source_modified_at"] = "2024-05-01T00:00:00Z"
    chunk_record = SearchChunkRecord(**chunk)
    assert chunk_record.to_cosmos_item()["sourceModifiedAt"] == "2024-05-01T00:00:00Z"


def test_document_source_modified_at_defaults_to_none_and_serializes_as_null() -> None:
    doc = SourceDocumentRecord(**document_values())
    assert doc.source_modified_at is None
    assert doc.to_cosmos_item()["sourceModifiedAt"] is None


def test_source_modified_at_must_be_utc_when_present() -> None:
    doc_values = document_values()
    doc_values["source_modified_at"] = "2024-05-01T00:00:00+05:00"
    with pytest.raises(ValueError, match="UTC"):
        SourceDocumentRecord(**doc_values)

    chunk = chunk_values()
    chunk["source_modified_at"] = "not-a-timestamp"
    with pytest.raises(ValueError, match="ISO-8601"):
        SearchChunkRecord(**chunk)