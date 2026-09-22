from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from ingestion.models import (
    AudioMetadata,
    AudioOperationRecord,
    AudioOperationState,
    AudioTranscriptPage,
    AudioTranscriptSegment,
    AudioTranscriptDescriptor,
    ACL_POLICY_VERSION,
    create_audio_transcript_descriptor,
    verify_audio_transcript_descriptor,
    verified_acl_hash,
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
    audio_transcript_hash,
    content_sha256,
    create_audio_control_partition_id,
    create_audio_operation_id,
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


def audio_metadata(**overrides: object) -> AudioMetadata:
    values = dict(
        duration_ms=3000, channel_count=1, locale="en-US", mode="fast",
        api_version="2025-10-15", profile_version="1", source_version="etag",
        source_content_hash="a" * 64,
    )
    return AudioMetadata(**(values | overrides))


@pytest.mark.parametrize("overrides", [
    {"duration_ms": 0}, {"duration_ms": 1_800_001}, {"duration_ms": True},
    {"channel_count": 3}, {"channel_count": True}, {"locale": "fr-FR"},
    {"mode": "batch"}, {"api_version": "unknown"}, {"source_content_hash": "invalid"},
    {"profile_version": ""}, {"source_version": ""},
])
def test_audio_metadata_rejects_invalid_contract(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        audio_metadata(**overrides)


def test_audio_operation_identity_has_frozen_expected_values() -> None:
    assert create_audio_control_partition_id("source:one") == (
        "audio-control-f7627392c99f835a7288ee876df4c03d429a1a94c58b57d6ba582522d642a5fc"
    )
    assert create_audio_operation_id(
        "source:one", "drive:two", "item:three",
        audio_metadata(source_version="etag:four", profile_version="profile:five"),
    ) == "audio-operation:2cf2ea795c7a4d08f715dc35d2e17da8f00d84f9517ed1c8a337bf8cc99f7e98"


def test_audio_operation_identity_is_run_independent() -> None:
    metadata = audio_metadata()
    operation_id = create_audio_operation_id("source", "drive", "item", metadata)
    assert operation_id == create_audio_operation_id("source", "drive", "item", audio_metadata())
    assert operation_id.startswith("audio-operation:")
    assert len(operation_id) == len("audio-operation:") + 64
    partition_id = create_audio_control_partition_id("source")
    assert partition_id == create_audio_control_partition_id("source")
    assert ":" not in partition_id
    assert partition_id != create_audio_control_partition_id("other-source")
    for run_id in ("run-a", "run-b", partition_id, "audio-control:reserved"):
        assert partition_id != create_source_run_id("source", run_id)


@pytest.mark.parametrize("field,value", [
    ("source_version", "other-etag"), ("source_content_hash", "b" * 64),
    ("mode", "enhanced"), ("locale", "en-GB"), ("profile_version", "2"),
])
def test_audio_operation_identity_binds_transcription_inputs(field: str, value: str) -> None:
    assert create_audio_operation_id("source", "drive", "item", audio_metadata()) != (
        create_audio_operation_id("source", "drive", "item", audio_metadata(**{field: value}))
    )


@pytest.mark.parametrize("identity", [
    ("other", "drive", "item"), ("source", "other", "item"), ("source", "drive", "other"),
])
def test_audio_operation_identity_binds_source(identity: tuple[str, str, str]) -> None:
    assert create_audio_operation_id("source", "drive", "item", audio_metadata()) != (
        create_audio_operation_id(*identity, audio_metadata())
    )


def test_audio_operation_identity_does_not_use_ambiguous_delimiters() -> None:
    assert create_audio_operation_id("source", "a:b", "c", audio_metadata()) != (
        create_audio_operation_id("source", "a", "b:c", audio_metadata())
    )
    assert create_audio_operation_id("source", "drive", "item", audio_metadata(
        source_version="a:b", profile_version="c",
    )) != create_audio_operation_id("source", "drive", "item", audio_metadata(
        source_version="a", profile_version="b:c",
    ))


@pytest.mark.parametrize("source_id", ["", " " * 2, "a" * 201, None, 1])
def test_audio_operation_identity_rejects_invalid_source(source_id: object) -> None:
    with pytest.raises(ValueError):
        create_audio_control_partition_id(source_id)
    with pytest.raises(ValueError):
        create_audio_operation_id(source_id, "drive", "item", audio_metadata())


def test_audio_operation_identity_rejects_unvalidated_metadata() -> None:
    with pytest.raises(ValueError, match="validated audio metadata"):
        create_audio_operation_id("source", "drive", "item", {})


@pytest.mark.parametrize("state", list(AudioOperationState))
def test_audio_operation_record_binds_permit(state: AudioOperationState) -> None:
    record = AudioOperationRecord("source", "drive", "item", audio_metadata(), "owner", state=state)
    item = record.to_cosmos_item()
    permit = record.to_permit_item()
    assert item["id"] == permit["operationId"] == record.id
    assert item["sourceRunId"] == permit["sourceRunId"] == record.source_run_id
    assert item["ownerId"] == permit["ownerId"] == "owner"
    assert item["ownershipEpoch"] == permit["ownershipEpoch"] == 1
    assert item["state"] == permit["state"] == state.value
    assert item["schemaVersion"] == permit["schemaVersion"] == 1
    assert item["recordType"] == "audio_operation"
    assert permit["recordType"] == "audio_permit"
    assert permit["id"] == "audio-permit"
    assert "runId" not in item
    assert "ttl" not in item and "ttl" not in permit


@pytest.mark.parametrize("overrides", [
    {"owner_id": ""}, {"owner_id": " "}, {"owner_id": "a" * 201},
    {"ownership_epoch": 0}, {"ownership_epoch": True}, {"ownership_epoch": 1.5},
    {"state": "submitting"}, {"audio": {}},
])
def test_audio_operation_record_rejects_invalid_ownership(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        AudioOperationRecord(**({
            "source_id": "source", "drive_id": "drive", "item_id": "item",
            "audio": audio_metadata(), "owner_id": "owner",
        } | overrides))


def build_audio_transcript_page(**overrides: object) -> AudioTranscriptPage:
    operation = AudioOperationRecord("source", "drive", "item", audio_metadata(), "owner")
    return AudioTranscriptPage(**({
        "operation": operation, "source_run_id": "source:run-a", "page_index": 0,
        "page_count": 1, "segments": (AudioTranscriptSegment(0, "hello", 0, 3000),),
    } | overrides))


def test_audio_transcript_page_has_frozen_expected_values() -> None:
    operation = AudioOperationRecord(
        "source:one", "drive:two", "item:three",
        audio_metadata(source_version="etag:four", profile_version="profile:five"), "owner",
    )
    page = build_audio_transcript_page(operation=operation, source_run_id="source:one:run-a")
    assert page.id == "audio-transcript:51c878144d5a05450cf30211c027f53a43a764e29d35be4d86feece161b809f7"
    assert page.page_hash == "7757d0c363afc1a91afaa69e462d75eb6e08c6e99320d2b069eab7d430e73d53"
    assert audio_transcript_hash(
        (page,), max_pages=1, max_segments=1, max_total_bytes=10000,
    ) == "5a2acca51957bd9f027a895448f2281b22fb0dc170a483188511e1ad6d07d783"


def test_audio_transcript_page_accepts_maximum_run_partition() -> None:
    operation = replace(build_audio_transcript_page().operation, source_id="s" * 200)
    partition = create_source_run_id(operation.source_id, "r" * 100)
    assert len(partition) == 301
    assert build_audio_transcript_page(
        operation=operation, source_run_id=partition,
    ).to_cosmos_item()["sourceRunId"] == partition


def test_audio_transcript_page_preserves_overlap_order_and_text() -> None:
    segments = (
        AudioTranscriptSegment(0, " first ", 100, 3000),
        AudioTranscriptSegment(1, "second", 0, 2000),
    )
    page = build_audio_transcript_page(segments=segments)
    item = page.to_cosmos_item()
    assert item == build_audio_transcript_page(segments=segments).to_cosmos_item()
    assert item["segments"] == [
        {"ordinal": 0, "text": " first ", "startMs": 100, "endMs": 3000},
        {"ordinal": 1, "text": "second", "startMs": 0, "endMs": 2000},
    ]
    assert item["schemaVersion"] == 1
    assert item["recordType"] == "audio_transcript_page"
    assert item["operationId"] == page.operation.id
    assert item["ownerId"] == "owner"
    assert item["ownershipEpoch"] == 1
    assert item["audio"]["locale"] == "en-US"
    assert "ttl" not in item and "state" not in item and "committed" not in item
    item["segments"][0]["text"] = "changed"
    assert page.to_cosmos_item()["segments"][0]["text"] == " first "


@pytest.mark.parametrize("overrides", [
    {"ordinal": True}, {"ordinal": -1}, {"ordinal": 0.5},
    {"text": ""}, {"text": " "}, {"text": None},
    {"start_ms": True}, {"start_ms": -1}, {"end_ms": 0},
    {"end_ms": 1.5}, {"end_ms": 1_800_001},
])
def test_audio_transcript_segment_rejects_invalid_values(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        AudioTranscriptSegment(**({"ordinal": 0, "text": "hello", "start_ms": 0, "end_ms": 100} | overrides))


@pytest.mark.parametrize("overrides", [
    {"operation": {}}, {"source_run_id": ""}, {"source_run_id": "p" * 302},
    {"page_index": True}, {"page_index": -1}, {"page_index": 1},
    {"page_count": False}, {"page_count": 0}, {"page_count": 1.5},
    {"segments": ()}, {"segments": []}, {"segments": ({},)},
    {"segments": (AudioTranscriptSegment(1, "gap", 0, 100),)},
    {"segments": (AudioTranscriptSegment(0, "long", 0, 3001),)},
    {"segments": (AudioTranscriptSegment(0, "first", 0, 100), AudioTranscriptSegment(2, "gap", 0, 100))},
    {"segments": (AudioTranscriptSegment(0, "first", 0, 100), AudioTranscriptSegment(0, "duplicate", 0, 100))},
])
def test_audio_transcript_page_rejects_invalid_contract(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        build_audio_transcript_page(**overrides)


@pytest.mark.parametrize("changes", [
    {"owner_id": "other"}, {"ownership_epoch": 2}, {"item_id": "other"},
    {"audio": audio_metadata(profile_version="2")},
])
def test_audio_transcript_page_identity_binds_owner_and_profile(changes: dict[str, object]) -> None:
    page = build_audio_transcript_page()
    changed = replace(page, operation=replace(page.operation, **changes))
    assert changed.id != page.id
    assert changed.page_hash != page.page_hash


def test_audio_transcript_page_hash_binds_address_position_and_content() -> None:
    page = build_audio_transcript_page()
    for changed in (
        replace(page, source_run_id="source:run-b"),
        replace(page, page_count=2),
        replace(page, segments=(AudioTranscriptSegment(0, "changed", 0, 3000),)),
        replace(page, segments=(AudioTranscriptSegment(0, "hello", 1, 3000),)),
        replace(page, operation=replace(page.operation, audio=audio_metadata(duration_ms=4000))),
    ):
        assert changed.id == page.id
        assert changed.page_hash != page.page_hash
    later = replace(page, page_index=1, page_count=2, segments=(AudioTranscriptSegment(2, "later", 0, 100),))
    assert later.id != page.id
    assert later.page_hash != page.page_hash
    assert replace(page, operation=replace(page.operation, state=AudioOperationState.SUBMITTING)).page_hash == page.page_hash


@pytest.mark.parametrize("prefix", ["a", "\u00e9\U0001f642", '\"\\\n'])
def test_audio_transcript_page_enforces_exact_serialized_byte_ceiling(prefix: str) -> None:
    page = build_audio_transcript_page(segments=(AudioTranscriptSegment(0, prefix, 0, 3000),))
    remaining = 128 * 1024 - serialized_size_bytes(page.to_cosmos_item())
    exact = replace(page, segments=(AudioTranscriptSegment(0, prefix + "a" * remaining, 0, 3000),))
    assert serialized_size_bytes(exact.to_cosmos_item()) == 128 * 1024
    with pytest.raises(ValueError, match="exceeds 128 KiB"):
        replace(exact, segments=(replace(exact.segments[0], text=exact.segments[0].text + "a"),))


def build_audio_transcript_set() -> tuple[AudioTranscriptPage, ...]:
    first = build_audio_transcript_page(page_count=2)
    second = replace(first, page_index=1, segments=(AudioTranscriptSegment(1, "\u00e9\n\"", 0, 100),))
    return first, second


def test_audio_transcript_set_accepts_exact_budgets_and_excludes_state() -> None:
    pages = build_audio_transcript_set()
    size = sum(serialized_size_bytes(page.to_cosmos_item()) for page in pages)
    digest = audio_transcript_hash(pages, max_pages=2, max_segments=2, max_total_bytes=size)
    assert len(digest) == 64
    assert digest == audio_transcript_hash(pages, max_pages=3, max_segments=3, max_total_bytes=size + 1)
    changed_state = (pages[0], replace(pages[1], operation=replace(
        pages[1].operation, state=AudioOperationState.UNKNOWN,
    )))
    assert digest == audio_transcript_hash(changed_state, max_pages=2, max_segments=2, max_total_bytes=size)
    for limits, message in (
        ({"max_pages": 1}, "page limit"),
        ({"max_segments": 1}, "segment limit"),
        ({"max_total_bytes": size - 1}, "byte limit"),
    ):
        with pytest.raises(ValueError, match=message):
            audio_transcript_hash(pages, **({"max_pages": 2, "max_segments": 2, "max_total_bytes": size} | limits))


@pytest.mark.parametrize("limit", ["max_pages", "max_segments", "max_total_bytes"])
@pytest.mark.parametrize("value", [True, 0, -1, 1.5, None, "2"])
def test_audio_transcript_set_rejects_invalid_budgets(limit: str, value: object) -> None:
    with pytest.raises(ValueError, match=limit):
        audio_transcript_hash(build_audio_transcript_set(), **({
            "max_pages": 2, "max_segments": 2, "max_total_bytes": 10000,
        } | {limit: value}))


@pytest.mark.parametrize("case", ["empty", "list", "generator", "wrong-type", "missing", "duplicate", "reversed", "count", "gap", "overlap"])
def test_audio_transcript_set_rejects_incomplete_or_invalid_sets(case: str) -> None:
    first, second = build_audio_transcript_set()
    cases = {
        "empty": (), "list": [first, second], "generator": iter((first, second)),
        "wrong-type": (first, {}), "missing": (first,), "duplicate": (first, first),
        "reversed": (second, first), "count": (first, replace(second, page_count=3)),
        "gap": (first, replace(second, segments=(AudioTranscriptSegment(2, "gap", 0, 100),))),
        "overlap": (first, replace(second, segments=(AudioTranscriptSegment(0, "duplicate", 0, 100),))),
    }
    with pytest.raises(ValueError):
        audio_transcript_hash(cases[case], max_pages=2, max_segments=2, max_total_bytes=10000)


@pytest.mark.parametrize("changes", [
    {"source_id": "other"}, {"drive_id": "other"}, {"item_id": "other"},
    {"owner_id": "other"}, {"ownership_epoch": 2},
    {"audio": audio_metadata(duration_ms=4000)}, {"audio": audio_metadata(channel_count=2)},
    {"audio": audio_metadata(source_version="other")}, {"audio": audio_metadata(source_content_hash="b" * 64)},
    {"audio": audio_metadata(profile_version="2")}, {"audio": audio_metadata(locale="en-GB")},
    {"audio": audio_metadata(mode="enhanced")},
])
def test_audio_transcript_set_rejects_mixed_binding(changes: dict[str, object]) -> None:
    first, second = build_audio_transcript_set()
    second = replace(second, operation=replace(second.operation, **changes))
    with pytest.raises(ValueError, match="inconsistent identity"):
        audio_transcript_hash((first, second), max_pages=2, max_segments=2, max_total_bytes=10000)


def test_audio_transcript_set_hash_binds_all_page_content_and_addresses() -> None:
    pages = build_audio_transcript_set()
    limits = {"max_pages": 2, "max_segments": 2, "max_total_bytes": 10000}
    digest = audio_transcript_hash(pages, **limits)
    with pytest.raises(ValueError, match="inconsistent identity"):
        audio_transcript_hash((pages[0], replace(pages[1], source_run_id="source:run-b")), **limits)
    for changed in (
        tuple(replace(page, source_run_id="source:run-b") for page in pages),
        tuple(replace(page, operation=replace(page.operation, owner_id="other")) for page in pages),
        tuple(replace(page, operation=replace(page.operation, ownership_epoch=2)) for page in pages),
        tuple(replace(page, operation=replace(page.operation, audio=audio_metadata(channel_count=2))) for page in pages),
        (pages[0], replace(pages[1], segments=(AudioTranscriptSegment(1, "changed", 0, 100),))),
        (pages[0], replace(pages[1], segments=(replace(pages[1].segments[0], end_ms=200),))),
    ):
        assert audio_transcript_hash(changed, **limits) != digest


def build_audio_checkpoint_source(**overrides: object) -> SourceDocumentRecord:
    return SourceDocumentRecord(**(document_values() | {
        "mime_type": "audio/wav", "audio": audio_metadata(), "content_hash": "a" * 64,
        "source_verified_at": UTC,
        "acl_hash": verified_acl_hash(("group-a",), ACL_POLICY_VERSION),
    } | overrides))


def build_audio_descriptor() -> AudioTranscriptDescriptor:
    pages = build_audio_transcript_set()
    return create_audio_transcript_descriptor(
        pages[0].operation, build_audio_checkpoint_source(), pages,
        acl_policy_version=ACL_POLICY_VERSION, max_pages=2, max_segments=2, max_total_bytes=10000,
    )


def test_audio_descriptor_preserves_provenance_without_commit_or_state() -> None:
    descriptor = build_audio_descriptor()
    pages = build_audio_transcript_set()
    expected = replace(pages[0].operation, state=AudioOperationState.UNKNOWN)
    verify_audio_transcript_descriptor(
        descriptor, expected, pages, max_pages=2, max_segments=2, max_total_bytes=10000,
    )
    item = descriptor.to_dict()
    assert item == build_audio_descriptor().to_dict()
    assert item["schemaVersion"] == 1
    assert item["recordType"] == "audio_transcript_descriptor"
    assert item["pagesSourceRunId"] == "source:run-a"
    assert item["pageCount"] == item["segmentCount"] == 2
    assert item["aclPolicyVersion"] == ACL_POLICY_VERSION
    assert item["aclEvaluatedAt"] == item["sourceVerifiedAt"] == UTC
    assert not {"text", "segments", "state", "ttl", "committed", "_etag"}.intersection(item)
    assert replace(descriptor, operation=expected).to_dict() == item


@pytest.mark.parametrize("changes", [
    {"owner_id": "other"}, {"ownership_epoch": 2}, {"item_id": "other"},
    {"audio": audio_metadata(duration_ms=4000)}, {"audio": audio_metadata(channel_count=2)},
])
def test_audio_descriptor_rejects_consistently_wrong_pages(changes: dict[str, object]) -> None:
    pages = build_audio_transcript_set()
    expected = pages[0].operation
    wrong = tuple(replace(page, operation=replace(page.operation, **changes)) for page in pages)
    with pytest.raises(ValueError, match="expected operation"):
        create_audio_transcript_descriptor(
            expected, build_audio_checkpoint_source(), wrong,
            acl_policy_version=ACL_POLICY_VERSION, max_pages=2, max_segments=2, max_total_bytes=10000,
        )
    with pytest.raises(ValueError, match="expected operation"):
        verify_audio_transcript_descriptor(
            build_audio_descriptor(), replace(expected, **changes), pages,
            max_pages=2, max_segments=2, max_total_bytes=10000,
        )


@pytest.mark.parametrize("changes", [
    {"e_tag": "other"}, {"audio": None}, {"source_verified_at": None},
    {"acl_hash": "0" * 64}, {"allowed_group_ids": ()},
    {"audio": audio_metadata(duration_ms=4000)},
])
def test_audio_descriptor_rejects_mismatched_source_or_placeholder_acl(changes: dict[str, object]) -> None:
    pages = build_audio_transcript_set()
    with pytest.raises(ValueError):
        create_audio_transcript_descriptor(
            pages[0].operation, build_audio_checkpoint_source(**changes), pages,
            acl_policy_version=ACL_POLICY_VERSION, max_pages=2, max_segments=2, max_total_bytes=10000,
        )


@pytest.mark.parametrize("changes", [
    {"pages_source_run_id": "source:run-b"}, {"page_count": 3}, {"segment_count": 3},
    {"total_bytes": 1}, {"transcript_hash": "0" * 64},
])
def test_audio_descriptor_rejects_tampered_references(changes: dict[str, object]) -> None:
    pages = build_audio_transcript_set()
    with pytest.raises(ValueError, match="complete page set"):
        verify_audio_transcript_descriptor(
            replace(build_audio_descriptor(), **changes), pages[0].operation, pages,
            max_pages=2, max_segments=2, max_total_bytes=10000,
        )


@pytest.mark.parametrize("changes", [
    {"page_count": True}, {"segment_count": 0}, {"total_bytes": 1.5},
    {"acl_policy_version": "unknown"}, {"acl_evaluated_at": "not-a-time"},
    {"source_verified_at": None}, {"source_verified_at": "2026-09-18T01:00:00+01:00"},
    {"allowed_group_ids": ["group-a"]}, {"allowed_group_ids": (None,)},
    {"allowed_group_ids": ("group-b", "group-a")}, {"allowed_group_ids": ("group-a", "group-a")},
    {"allowed_group_ids": ("group\x1fa",)},
])
def test_audio_descriptor_rejects_invalid_metadata(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        replace(build_audio_descriptor(), **changes)


@pytest.mark.parametrize("case", ["text", "timing", "missing", "reversed", "address", "budget"])
def test_audio_descriptor_rejects_changed_pages_or_budget(case: str) -> None:
    pages = build_audio_transcript_set()
    descriptor = build_audio_descriptor()
    expected = pages[0].operation
    budget = 10000
    if case in ("text", "timing"):
        segment = replace(pages[1].segments[0], **({"text": "changed"} if case == "text" else {"end_ms": 200}))
        pages = (pages[0], replace(pages[1], segments=(segment,)))
    elif case == "missing":
        pages = pages[:1]
    elif case == "reversed":
        pages = tuple(reversed(pages))
    elif case == "address":
        pages = tuple(replace(page, source_run_id="source:run-b") for page in pages)
    else:
        budget = descriptor.total_bytes - 1
    with pytest.raises(ValueError):
        verify_audio_transcript_descriptor(
            descriptor, expected, pages, max_pages=2, max_segments=2, max_total_bytes=budget,
        )


def test_audio_descriptor_factory_rejects_wrong_storage_address() -> None:
    pages = tuple(replace(page, source_run_id="source:run-b") for page in build_audio_transcript_set())
    with pytest.raises(ValueError, match="complete page set"):
        create_audio_transcript_descriptor(
            pages[0].operation, build_audio_checkpoint_source(), pages,
            acl_policy_version=ACL_POLICY_VERSION, max_pages=2, max_segments=2, max_total_bytes=10000,
        )


def test_audio_descriptor_enforces_serialized_size_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    import ingestion.models as models

    descriptor = build_audio_descriptor()
    size = serialized_size_bytes(descriptor.to_dict())
    monkeypatch.setattr(models, "MAX_DOCUMENT_ITEM_BYTES", size)
    assert replace(descriptor).to_dict() == descriptor.to_dict()
    monkeypatch.setattr(models, "MAX_DOCUMENT_ITEM_BYTES", size - 1)
    with pytest.raises(ValueError, match="descriptor exceeds"):
        replace(descriptor)


def test_audio_descriptor_rejects_oversize_acl_provenance() -> None:
    groups = tuple(f"group-{ordinal:04d}-" + "x" * 480 for ordinal in range(300))
    with pytest.raises(ValueError, match="descriptor exceeds 128 KiB"):
        replace(build_audio_descriptor(), allowed_group_ids=groups, acl_hash=verified_acl_hash(groups, ACL_POLICY_VERSION))


def test_audio_manifest_and_chunk_round_trip_to_compatible_reader() -> None:
    from unittest.mock import Mock
    from retrieval.cosmos import SecureCosmosRetriever

    metadata = audio_metadata()
    document = SourceDocumentRecord(**(document_values() | {
        "mime_type": "audio/wav", "audio": metadata,
        "content_hash": "a" * 64, "source_verified_at": UTC,
    }))
    chunk = SearchChunkRecord(**(chunk_values() | {
        "audio": metadata, "start_ms": 0, "end_ms": 3000,
        "page_start": None, "page_end": None, "locator_kind": LocatorKind.TIME,
        "locator_label": "00:00-00:03",
        "modalities": (ContentModality.TEXT, ContentModality.AUDIO_TRANSCRIPT),
        "provenance": (ExtractionProvenance.TRANSCRIBED,),
        "visual_coverage": VisualCoverageStatus.NOT_REQUIRED,
    }))
    assert document.to_cosmos_item()["schemaVersion"] == chunk.to_cosmos_item()["schemaVersion"] == 1
    assert document.to_cosmos_item()["audio"] == chunk.to_cosmos_item()["audio"]
    result = SecureCosmosRetriever(Mock(), Mock()).to_chunks([chunk.to_cosmos_item()])[0]
    assert (result.start_ms, result.end_ms, result.evidence_version) == (0, 3000, "etag")


def test_v1_serialization_does_not_gain_audio_fields() -> None:
    assert "audio" not in build_document_record().to_cosmos_item()
    assert "sourceVerifiedAt" not in build_document_record().to_cosmos_item()
    assert "transcriptionJobUrl" not in build_document_record().to_cosmos_item()
    assert "stagingBlobName" not in build_document_record().to_cosmos_item()
    assert not {"audio", "startMs", "endMs"}.intersection(build_chunk_record().to_cosmos_item())


def _transcribing_values() -> dict[str, object]:
    return document_values() | {
        "mime_type": "audio/wav",
        "status": DocumentStatus.PROCESSING,
        "stage": DocumentStage.TRANSCRIBING,
        "transcription_job_url": (
            "https://speech.cognitiveservices.azure.com/speechtotext/transcriptions/abc"
            "?api-version=2024-11-15"
        ),
        "staging_blob_name": "source/abc.wav",
    }


def test_transcribing_document_round_trips_batch_tracking_fields() -> None:
    from ingestion import repository as repository_module

    record = SourceDocumentRecord(**_transcribing_values())
    item = record.to_cosmos_item()
    assert item["stage"] == "transcribing"
    assert item["transcriptionJobUrl"] == record.transcription_job_url
    assert item["stagingBlobName"] == "source/abc.wav"

    restored = repository_module._document_from_item(item)
    assert restored.stage is DocumentStage.TRANSCRIBING
    assert restored.transcription_job_url == record.transcription_job_url
    assert restored.staging_blob_name == "source/abc.wav"


@pytest.mark.parametrize("drop", ["transcription_job_url", "staging_blob_name"])
def test_batch_tracking_fields_must_be_set_together(drop: str) -> None:
    values = _transcribing_values() | {drop: None}
    with pytest.raises(ValueError, match="must be set together"):
        SourceDocumentRecord(**values)


@pytest.mark.parametrize("field", ["transcription_job_url", "staging_blob_name"])
def test_non_audio_document_rejects_batch_tracking_fields(field: str) -> None:
    values = document_values() | {field: "x"}
    with pytest.raises(ValueError, match="does not accept audio fields"):
        SourceDocumentRecord(**values)


def test_ready_audio_requires_verified_source_and_metadata() -> None:
    values = document_values() | {"mime_type": "audio/wav"}
    assert SourceDocumentRecord(**values).audio is None
    with pytest.raises(ValueError, match="ready audio requires"):
        SourceDocumentRecord(**(values | {"status": DocumentStatus.READY}))


@pytest.mark.parametrize("start,end", [(True, 100), (-1, 100), (0, 0), (100, 50), (0, 1_800_001)])
def test_temporal_locator_rejects_invalid_range(start: int, end: int) -> None:
    with pytest.raises(ValueError, match="audio time range"):
        SourceLocator(LocatorKind.TIME, "Time", 1, 1, start, end)


def test_document_schema_cannot_carry_audio_metadata() -> None:
    with pytest.raises(ValueError, match="does not accept audio"):
        SourceDocumentRecord(**(document_values() | {"audio": audio_metadata()}))
    with pytest.raises(ValueError, match="does not accept audio"):
        SearchChunkRecord(**(chunk_values() | {"audio": audio_metadata()}))


@pytest.mark.parametrize("schema_version", [2, 99, True, None])
@pytest.mark.parametrize("is_audio", [False, True])
def test_media_records_reject_unsupported_schema_versions(schema_version: object, is_audio: bool) -> None:
    document = document_values() | {"schema_version": schema_version}
    chunk = chunk_values() | {"schema_version": schema_version}
    if is_audio:
        document["mime_type"] = "audio/wav"
        chunk["locator_kind"] = LocatorKind.TIME
    with pytest.raises(ValueError, match="schema version"):
        SourceDocumentRecord(**document)
    with pytest.raises(ValueError, match="schema version"):
        SearchChunkRecord(**chunk)


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