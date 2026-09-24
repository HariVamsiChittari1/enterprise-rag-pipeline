from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import inspect
from typing import Any

import pytest

import ingestion.repository as repository_module

from azure.core import MatchConditions

from ingestion.models import (
    AudioMetadata,
    AudioOperationRecord,
    AudioOperationState,
    AudioTranscriptPage,
    AudioTranscriptSegment,
    ContentModality,
    DocumentStage,
    DocumentStatus,
    IngestionRunRecord,
    EnrichmentStatuses,
    Entity,
    ExtractionProvenance,
    LocatorKind,
    ModuleStatus,
    ProfileSnapshot,
    RunCounters,
    RunStage,
    RunStatus,
    SafeError,
    SearchChunkRecord,
    SourceControlRecord,
    SourceDocumentRecord,
    SourceLocator,
    VisualDisposition,
    VisualCoverageStatus,
    VisualManifestEntry,
    VisualRelevance,
    content_sha256,
    create_audio_control_partition_id,
    create_chunk_id,
    create_document_id,
    create_document_key,
    create_source_run_id,
    create_visual_manifest_pages,
    run_record_id,
    serialized_size_bytes,
    visual_manifest_hash,
)
from ingestion.repository import (
    IngestionRepository,
    RepositoryConflictError,
    RepositoryDataError,
    RepositoryError,
)


UTC = "2026-08-05T12:00:00Z"
FAKE_COSMOS_BATCH_PAYLOAD_BYTES = 2_000_000


class FakeNotFoundError(Exception):
    status_code = 404


class PointReadContainer:
    def __init__(self, items: dict[tuple[str, str], dict[str, Any]] | None = None) -> None:
        self.items = items or {}
        self.reads: list[tuple[str, str]] = []

    def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
        self.reads.append((item, partition_key))
        try:
            return self.items[(partition_key, item)]
        except KeyError:
            raise FakeNotFoundError from None


class FakeCosmosError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__("sensitive sdk response")
        self.status_code = status_code


class FakeCosmosStatusError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__("sensitive sdk response")
        self.status = status


class StatefulContainer(PointReadContainer):
    def __init__(self, partition_field: str) -> None:
        super().__init__()
        self.partition_field = partition_field
        self.batch_calls: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.replace_calls: list[dict[str, Any]] = []
        self.query_calls: list[dict[str, Any]] = []
        self.delete_calls: list[tuple[str, str]] = []
        self.fail_batch_call_numbers: set[int] = set()
        self.before_batch: Any = None
        self.batch_error: Exception | None = None
        self.batch_status_override: int | None = None
        self.before_replace: Any = None
        self.before_query: Any = None
        self._etag_counter = 0

    def _store(self, body: dict[str, Any]) -> dict[str, Any]:
        self._etag_counter += 1
        stored = deepcopy(body)
        stored["_etag"] = f"etag-{self._etag_counter}"
        self.items[(stored[self.partition_field], stored["id"])] = stored
        return deepcopy(stored)

    def create_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        key = (body[self.partition_field], body["id"])
        if key in self.items:
            raise FakeCosmosError(409)
        return self._store(body)

    def replace_item(
        self,
        *,
        item: str,
        body: dict[str, Any],
        etag: str,
        match_condition: MatchConditions,
    ) -> dict[str, Any]:
        self.replace_calls.append(
            {"item": item, "body": deepcopy(body), "etag": etag, "match_condition": match_condition}
        )
        if self.before_replace is not None:
            callback = self.before_replace
            self.before_replace = None
            callback()
        key = (body[self.partition_field], item)
        if key not in self.items or self.items[key].get("_etag") != etag:
            raise FakeCosmosError(412)
        return self._store(body)

    def execute_item_batch(
        self,
        *,
        batch_operations: list[tuple[Any, ...]],
        partition_key: str,
    ) -> list[dict[str, int]]:
        self.batch_calls.append((partition_key, deepcopy(batch_operations)))
        payload_bytes = sum(
            serialized_size_bytes(operation[1][-1])
            + repository_module.BATCH_OPERATION_OVERHEAD_BYTES
            for operation in batch_operations
        )
        if len(batch_operations) > 100 or payload_bytes > FAKE_COSMOS_BATCH_PAYLOAD_BYTES:
            raise FakeCosmosError(413)
        if len(self.batch_calls) in self.fail_batch_call_numbers:
            self.fail_batch_call_numbers.remove(len(self.batch_calls))
            raise FakeCosmosError(500)
        if self.batch_error is not None:
            error = self.batch_error
            self.batch_error = None
            raise error
        if self.before_batch is not None:
            callback = self.before_batch
            self.before_batch = None
            callback()
        snapshot = deepcopy(self.items)
        etag_counter = self._etag_counter
        try:
            for operation in batch_operations:
                kind, arguments, *options = operation
                if kind == "create":
                    self.create_item(body=arguments[0])
                elif kind == "replace":
                    item_id, body = arguments
                    kwargs = options[0]
                    key = (partition_key, item_id)
                    if key not in self.items or self.items[key].get("_etag") != kwargs["if_match_etag"]:
                        raise FakeCosmosError(412)
                    self._store(body)
                else:
                    raise AssertionError(f"unsupported batch operation: {kind}")
        except Exception:
            self.items = snapshot
            self._etag_counter = etag_counter
            raise
        if self.batch_status_override is not None:
            self.items = snapshot
            self._etag_counter = etag_counter
            return [{"statusCode": self.batch_status_override} for _ in batch_operations]
        return [
            {"statusCode": 201 if operation[0] == "create" else 200}
            for operation in batch_operations
        ]

    def query_items(
        self,
        *,
        query: str,
        parameters: list[dict[str, Any]],
        partition_key: str,
        max_item_count: int,
    ) -> "FakeQueryIterator":
        if self.before_query is not None:
            self.before_query(len(self.query_calls) + 1)
        self.query_calls.append(
            {
                "query": query,
                "parameters": deepcopy(parameters),
                "partition_key": partition_key,
                "max_item_count": max_item_count,
            }
        )
        parameter_values = {parameter["name"]: parameter["value"] for parameter in parameters}
        rows = [
            deepcopy(item)
            for (item_partition, _), item in self.items.items()
            if item_partition == partition_key
        ]
        for field, parameter in (
            ("documentKey", "@documentKey"),
            ("sourceRunId", "@sourceRunId"),
            ("status", "@status"),
            ("recordType", "@recordType"),
        ):
            if parameter in parameter_values:
                rows = [row for row in rows if row.get(field) == parameter_values[parameter]]
        if 'STARTSWITH(c.id, "run:")' in query:
            rows = [row for row in rows if row.get("id", "").startswith("run:")]
        if "ORDER BY c.startedAt DESC" in query:
            rows.sort(key=lambda row: (row.get("startedAt", ""), row["id"]), reverse=True)
        else:
            rows.sort(key=lambda row: (row.get("discoveryOrdinal", 0), row["id"]))
        if "SELECT *" not in query.upper():
            projection = query.split("SELECT ", 1)[1].split(" FROM c", 1)[0]
            projected_rows: list[dict[str, Any]] = []
            for row in rows:
                projected: dict[str, Any] = {}
                for expression in projection.split(","):
                    source, _, alias = expression.strip().partition(" AS ")
                    source_parts = source.removeprefix("c.").split(".")
                    value: Any = row
                    for part in source_parts:
                        value = value.get(part) if isinstance(value, dict) else None
                    projected[alias or source_parts[-1]] = deepcopy(value)
                projected_rows.append(projected)
            rows = projected_rows
        return FakeQueryIterator(rows, max_item_count)

    def delete_item(self, *, item: str, partition_key: str) -> None:
        self.delete_calls.append((item, partition_key))
        try:
            del self.items[(partition_key, item)]
        except KeyError:
            raise FakeNotFoundError from None


class FakeQueryIterator:
    def __init__(self, rows: list[dict[str, Any]], page_size: int) -> None:
        self.rows = rows
        self.page_size = page_size

    def by_page(self, continuation_token: str | None) -> "FakePager":
        return FakePager(self.rows, self.page_size, int(continuation_token or 0))


class FakePager:
    def __init__(self, rows: list[dict[str, Any]], page_size: int, offset: int) -> None:
        self.rows = rows
        self.page_size = page_size
        self.offset = offset
        self.continuation_token: str | None = None
        self._read = False

    def __iter__(self) -> "FakePager":
        return self

    def __next__(self) -> list[dict[str, Any]]:
        if self._read:
            raise StopIteration
        self._read = True
        end = min(self.offset + self.page_size, len(self.rows))
        self.continuation_token = str(end) if end < len(self.rows) else None
        return self.rows[self.offset:end]


def test_point_read_uses_exact_item_and_partition_keys() -> None:
    control = SourceControlRecord(
        source_id="source",
        current_run_id="run-a",
        current_orchestration_instance_id="orchestration-a",
        activated_at=UTC,
        updated_at=UTC,
    ).to_cosmos_item()
    control["_etag"] = "etag-1"
    runs = PointReadContainer({("source", "source-control"): control})
    documents = PointReadContainer()
    chunks = PointReadContainer()
    repository = IngestionRepository(runs, documents, chunks)

    result = repository.get_source_control("source")

    assert result is not None
    assert result.record.current_run_id == "run-a"
    assert result.etag == "etag-1"
    assert runs.reads == [("source-control", "source")]
    assert repository.get_run("source", "run-a") is None
    assert runs.reads[-1] == ("run:run-a", "source")
    assert repository.get_document("source:run-a", "document-a") is None
    assert documents.reads == [("document-a", "source:run-a")]
    assert repository.get_chunk("document-key", "chunk:000000") is None
    assert chunks.reads == [("chunk:000000", "document-key")]


def test_mutable_point_read_rejects_missing_etag() -> None:
    control = SourceControlRecord(
        source_id="source",
        current_run_id="run-a",
        current_orchestration_instance_id="orchestration-a",
        activated_at=UTC,
        updated_at=UTC,
    ).to_cosmos_item()
    repository = IngestionRepository(PointReadContainer({("source", "source-control"): control}), PointReadContainer(), PointReadContainer())

    with pytest.raises(RepositoryDataError, match="missing a valid ETag"):
        repository.get_source_control("source")


def test_atomic_activation_creates_run_and_control_or_rolls_back() -> None:
    runs = StatefulContainer("sourceId")
    repository = IngestionRepository(runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey"))
    run = build_run("run-a", UTC)
    control = build_control("run-a", UTC)

    activated = repository.activate_run(run, control)

    assert activated.run.record == run
    assert activated.source_control.record == control
    assert len(runs.batch_calls) == 1
    assert [operation[0] for operation in runs.batch_calls[0][1]] == ["create", "create"]

    conflicting_runs = StatefulContainer("sourceId")
    conflicting_runs.create_item(body=run.to_cosmos_item())
    conflicting_repository = IngestionRepository(
        conflicting_runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey")
    )
    with pytest.raises(RepositoryConflictError, match="activation conflicts"):
        conflicting_repository.activate_run(run, control)
    assert conflicting_repository.get_source_control("source") is None


def test_activation_replay_accepts_same_active_run() -> None:
    runs = StatefulContainer("sourceId")
    repository = IngestionRepository(runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey"))
    run = build_run("run-a", UTC)
    control = build_control("run-a", UTC)
    repository.activate_run(run, control)

    replay = repository.activate_run(run, control)

    assert replay.run.record == run
    assert len(runs.batch_calls) == 1


def test_activation_preserves_last_completed_run_pointer() -> None:
    runs = StatefulContainer("sourceId")
    repository = IngestionRepository(
        runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey")
    )
    repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    stored = repository.get_source_control("source")
    assert stored is not None
    runs._store(replace(stored.record, last_completed_run_id="run-a").to_cosmos_item())
    later = "2026-08-05T12:01:00Z"

    activated = repository.activate_run(
        build_run("run-b", later),
        build_control("run-b", later),
    )

    assert activated.source_control.record.last_completed_run_id == "run-a"


def test_stale_activation_cannot_restore_old_current_run() -> None:
    runs = StatefulContainer("sourceId")
    repository = IngestionRepository(runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey"))
    repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    later = "2026-08-05T12:01:00Z"
    repository.activate_run(build_run("run-b", later), build_control("run-b", later))

    with pytest.raises(RepositoryConflictError, match="stale"):
        repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))

    current = repository.get_source_control("source")
    assert current is not None
    assert current.record.current_run_id == "run-b"


def test_discovered_document_identical_retry_succeeds_but_different_content_conflicts() -> None:
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, StatefulContainer("documentKey"))
    document = build_document()

    first = repository.create_discovered_document(document)
    replay = repository.create_discovered_document(document)

    assert replay.record == first.record
    with pytest.raises(RepositoryConflictError, match="different content"):
        repository.create_discovered_document(replace(document, size_bytes=101))


def test_document_processing_requires_current_etag() -> None:
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, StatefulContainer("documentKey"))
    discovered = repository.create_discovered_document(build_document())
    processing = replace(
        discovered.record,
        status=DocumentStatus.PROCESSING,
        stage=DocumentStage.ACL,
        attempt_count=1,
        processing_started_at=UTC,
    )

    updated = repository.mark_document_processing(processing, discovered.etag)

    assert updated.record.status is DocumentStatus.PROCESSING
    assert documents.replace_calls[-1]["match_condition"] is MatchConditions.IfNotModified
    assert "_etag" not in documents.replace_calls[-1]["body"]
    with pytest.raises(RepositoryConflictError, match="concurrently"):
        repository.mark_document_processing(processing, discovered.etag)


def test_first_terminal_document_transition_wins() -> None:
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, StatefulContainer("documentKey"))
    discovered = repository.create_discovered_document(build_document())
    processing = repository.mark_document_processing(
        replace(
            discovered.record,
            status=DocumentStatus.PROCESSING,
            stage=DocumentStage.ACL,
            attempt_count=1,
            processing_started_at=UTC,
        ),
        discovered.etag,
    )
    failed = replace(
        processing.record,
        status=DocumentStatus.FAILED,
        stage=DocumentStage.TERMINAL,
        failed_at=UTC,
        error=SafeError("processing_failed", "embedding", False),
    )

    terminal = repository.mark_document_failed(failed, processing.etag)

    assert terminal.record.status is DocumentStatus.FAILED
    with pytest.raises(RepositoryConflictError, match="no longer legal"):
        repository.mark_document_failed(failed, terminal.etag)


def test_chunk_writes_split_batches_at_one_hundred_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repository_module, "MAX_BATCH_PAYLOAD_BYTES", 100_000_000)
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), StatefulContainer("sourceRunId"), chunks)

    written = repository.write_chunks(build_chunks(201))

    assert written == 201
    assert [len(call[1]) for call in chunks.batch_calls] == [100, 100, 1]


def test_chunk_writes_split_batches_within_application_payload_budget() -> None:
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(
        StatefulContainer("sourceId"), StatefulContainer("sourceRunId"), chunks
    )
    records = tuple(
        replace(
            chunk,
            content=f"{chunk.chunk_index}:" + ("x" * 100_000),
            content_hash=content_sha256(f"{chunk.chunk_index}:" + ("x" * 100_000)),
        )
        for chunk in build_chunks(12)
    )

    assert repository.write_chunks(records) == len(records)

    assert len(chunks.batch_calls) > 1
    for _, operations in chunks.batch_calls:
        assert len(operations) <= repository_module.MAX_BATCH_OPERATIONS
        assert sum(
            serialized_size_bytes(operation[1][0])
            + repository_module.BATCH_OPERATION_OVERHEAD_BYTES
            for operation in operations
        ) <= repository_module.MAX_BATCH_PAYLOAD_BYTES


def test_chunk_write_rejects_single_operation_over_application_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(
        StatefulContainer("sourceId"), StatefulContainer("sourceRunId"), chunks
    )
    record = build_chunks(1)[0]
    operation_bytes = (
        serialized_size_bytes(record.to_cosmos_item())
        + repository_module.BATCH_OPERATION_OVERHEAD_BYTES
    )
    monkeypatch.setattr(repository_module, "MAX_BATCH_PAYLOAD_BYTES", operation_bytes - 1)

    with pytest.raises(ValueError, match="batch payload limit"):
        repository.write_chunks((record,))

    assert chunks.batch_calls == []


def test_chunk_writes_reject_empty_and_noncontiguous_input() -> None:
    repository = IngestionRepository(
        StatefulContainer("sourceId"),
        StatefulContainer("sourceRunId"),
        StatefulContainer("documentKey"),
    )

    with pytest.raises(ValueError, match="cannot be empty"):
        repository.write_chunks(())
    with pytest.raises(ValueError, match="contiguous"):
        repository.write_chunks(build_chunks(2)[1:])
    with pytest.raises(TypeError, match="SearchChunkRecord"):
        repository.write_chunks((object(),))  # type: ignore[arg-type]


def test_chunk_retry_converges_after_prior_batch_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repository_module, "MAX_BATCH_PAYLOAD_BYTES", 100_000_000)
    chunks = StatefulContainer("documentKey")
    chunks.fail_batch_call_numbers.add(2)
    repository = IngestionRepository(StatefulContainer("sourceId"), StatefulContainer("sourceRunId"), chunks)
    records = build_chunks(101)

    with pytest.raises(RepositoryError, match="batch create failed"):
        repository.write_chunks(records)
    assert len(chunks.items) == 100

    assert repository.write_chunks(records) == 101
    assert len(chunks.items) == 101
    assert all(len(call[1]) <= 100 for call in chunks.batch_calls)


def test_conflicting_chunk_content_is_rejected() -> None:
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), StatefulContainer("sourceRunId"), chunks)
    original = build_chunks(1)[0]
    repository.write_chunks((original,))
    conflicting = replace(
        original,
        content="Different content.",
        content_hash=content_sha256("Different content."),
    )

    with pytest.raises(RepositoryConflictError, match="different content"):
        repository.write_chunks((conflicting,))


def test_chunk_batch_handles_status_only_conflict_exception() -> None:
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(
        StatefulContainer("sourceId"), StatefulContainer("sourceRunId"), chunks
    )
    record = build_chunks(1)[0]
    chunks._store(record.to_cosmos_item())
    chunks.batch_error = FakeCosmosStatusError(409)

    assert repository.write_chunks((record,)) == 1


def test_chunk_batch_rejects_non_success_operation_result_without_committing() -> None:
    chunks = StatefulContainer("documentKey")
    chunks.batch_status_override = 413
    repository = IngestionRepository(
        StatefulContainer("sourceId"), StatefulContainer("sourceRunId"), chunks
    )

    with pytest.raises(RepositoryError, match="Cosmos chunk batch failed"):
        repository.write_chunks(build_chunks(1))

    assert chunks.items == {}


def test_ready_verification_rejects_missing_and_extra_chunks() -> None:
    documents = StatefulContainer("sourceRunId")
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, chunks)
    processing = create_processing_document(repository)
    repository.write_chunks(build_chunks(1, is_retrievable=True))
    admitting = begin_admission(repository, processing, 2)
    ready = build_ready_document(admitting.record, 2)

    with pytest.raises(RepositoryConflictError, match="missing chunks"):
        repository.verify_and_mark_document_ready(ready, admitting.etag)

    repository.write_chunks(build_chunks(3, is_retrievable=True))
    with pytest.raises(RepositoryConflictError, match="missing or extra"):
        repository.verify_and_mark_document_ready(ready, admitting.etag)


def test_manifest_write_is_idempotent_and_required_for_admission() -> None:
    documents = StatefulContainer("sourceRunId")
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, chunks)
    processing = create_processing_document(repository)
    pages = build_manifest_pages(processing.record)
    bound_document = replace(
        processing.record,
        visual_manifest_page_count=len(pages),
        visual_manifest_hash=visual_manifest_hash(pages),
    )
    bound = repository.update_processing_document(bound_document, processing.etag)

    assert repository.write_visual_manifest_pages(pages) == len(pages)
    assert repository.write_visual_manifest_pages(pages) == len(pages)

    repository.write_chunks(build_chunks(1, is_retrievable=True))
    admitting = begin_admission(repository, bound, 1)
    ready = repository.verify_and_mark_document_ready(
        build_ready_document(admitting.record, 1),
        admitting.etag,
    )

    assert ready.record.status is DocumentStatus.READY


def test_admission_rejects_missing_manifest_page() -> None:
    repository = IngestionRepository(
        StatefulContainer("sourceId"),
        StatefulContainer("sourceRunId"),
        StatefulContainer("documentKey"),
    )
    processing = create_processing_document(repository)
    pages = build_manifest_pages(processing.record)
    bound_document = replace(
        processing.record,
        visual_manifest_page_count=len(pages),
        visual_manifest_hash=visual_manifest_hash(pages),
    )
    bound = repository.update_processing_document(bound_document, processing.etag)

    with pytest.raises(RepositoryConflictError, match="missing manifest pages"):
        begin_admission(repository, bound, 0)


def _audio_processing_document(repository: IngestionRepository) -> Any:
    base = build_document(item_id="audio-1")
    audio_doc = replace(
        base, source_name="clip.wav", source_path="/clip.wav",
        source_url="https://example.invalid/clip.wav", mime_type="audio/wav",
    )
    return create_processing_document(repository, document=audio_doc)


def test_audio_document_admits_and_readies_without_visual_manifest() -> None:
    from ingestion.models import AudioMetadata

    documents = StatefulContainer("sourceRunId")
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, chunks)
    processing = _audio_processing_document(repository)

    content_hash = "a" * 64
    audio = AudioMetadata(
        3000, None, "en-US", "batch", "2024-11-15", "speech_batch",
        processing.record.e_tag, content_hash,
    )
    # The PERSISTING update sets the audio metadata (no visual manifest is created).
    bound = repository.update_processing_document(
        replace(
            processing.record, content_hash=content_hash, extraction_mode="speech_batch",
            expected_chunk_count=1, audio=audio,
        ),
        processing.etag,
    )
    repository.write_chunks(build_chunks(1, document=bound.record, is_retrievable=True))

    admitting = repository.begin_document_admission(
        replace(
            bound.record, status=DocumentStatus.ADMITTING, stage=DocumentStage.VERIFYING,
            written_chunk_count=1, source_verified_at=UTC,
        ),
        bound.etag,
    )
    assert admitting.record.visual_manifest_page_count is None  # audio carries no manifest

    ready = repository.verify_and_mark_document_ready(
        build_ready_document(admitting.record, 1), admitting.etag,
    )
    assert ready.record.status is DocumentStatus.READY
    assert ready.record.audio is not None


def test_non_audio_document_still_requires_manifest_for_admission() -> None:
    repository = IngestionRepository(
        StatefulContainer("sourceId"), StatefulContainer("sourceRunId"), StatefulContainer("documentKey"),
    )
    processing = create_processing_document(repository)  # PDF, audio=None, no manifest bound
    with pytest.raises(ValueError, match="admitting document integrity fields are incomplete"):
        repository.begin_document_admission(
            replace(
                processing.record, status=DocumentStatus.ADMITTING, stage=DocumentStage.VERIFYING,
                expected_chunk_count=1, written_chunk_count=1,
            ),
            processing.etag,
        )


def test_chunk_from_item_rehydrates_audio_metadata() -> None:
    from ingestion.models import (
        AudioMetadata, ContentModality, ExtractionProvenance, VisualCoverageStatus,
    )

    content_hash = "a" * 64
    audio = AudioMetadata(
        3000, None, "en-US", "batch", "2024-11-15", "speech_batch", "source-etag", content_hash,
    )
    audio_chunk = replace(
        build_chunks(1, is_retrievable=True)[0],
        audio=audio, start_ms=0, end_ms=3000, page_start=None, page_end=None,
        locator_kind=LocatorKind.TIME, locator_label="00:00-00:03",
        modalities=(ContentModality.TEXT, ContentModality.AUDIO_TRANSCRIPT),
        provenance=(ExtractionProvenance.TRANSCRIBED,),
        visual_coverage=VisualCoverageStatus.NOT_REQUIRED,
    )
    restored = repository_module._chunk_from_item(audio_chunk.to_cosmos_item())
    assert isinstance(restored.audio, AudioMetadata)
    assert (restored.start_ms, restored.end_ms) == (0, 3000)
    assert restored.audio.source_content_hash == content_hash


def test_ready_verification_consumes_every_projected_page_without_point_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repository_module, "INTERNAL_PAGE_SIZE", 50)
    documents = StatefulContainer("sourceRunId")
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, chunks)
    processing = create_processing_document(repository)
    records = build_chunks(205, is_retrievable=True)
    for record in records:
        chunks._store(record.to_cosmos_item())

    admitting = begin_admission(repository, processing, len(records))
    ready = repository.verify_and_mark_document_ready(
        build_ready_document(admitting.record, len(records)),
        admitting.etag,
    )

    assert ready.record.status is DocumentStatus.READY
    assert chunks.reads == []
    assert len(chunks.query_calls) == 5
    assert all("chunkIndex" in call["query"] for call in chunks.query_calls)
    assert all("SELECT *" not in call["query"].upper() for call in chunks.query_calls)


def test_ready_verification_rejects_invalid_chunk_id_index_mapping() -> None:
    documents = StatefulContainer("sourceRunId")
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, chunks)
    processing = create_processing_document(repository)
    records = build_chunks(2, is_retrievable=True)
    for record in records:
        chunks._store(record.to_cosmos_item())
    second_key = (processing.record.document_key, create_chunk_id(1))
    chunks.items[second_key]["chunkIndex"] = 0

    admitting = begin_admission(repository, processing, 2)
    with pytest.raises(RepositoryConflictError, match="duplicate chunks"):
        repository.verify_and_mark_document_ready(
            build_ready_document(admitting.record, 2),
            admitting.etag,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("isRetrievable", False, "ineligible chunks"),
        ("lifecycleGeneration", 1, "lifecycle generation mismatch"),
        ("allowedGroupIds", ["group-other"], "ACL mismatch"),
    ],
)
def test_ready_verification_rejects_lifecycle_mismatch(
    field: str,
    value: Any,
    message: str,
) -> None:
    documents = StatefulContainer("sourceRunId")
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, chunks)
    processing = create_processing_document(repository)
    records = build_chunks(1, is_retrievable=True)
    chunks._store(records[0].to_cosmos_item())
    chunks.items[(processing.record.document_key, create_chunk_id(0))][field] = value
    admitting = begin_admission(repository, processing, 1)

    with pytest.raises(RepositoryConflictError, match=message):
        repository.verify_and_mark_document_ready(
            build_ready_document(admitting.record, 1),
            admitting.etag,
        )


def test_concurrent_failed_transition_wins_over_ready() -> None:
    documents = StatefulContainer("sourceRunId")
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(StatefulContainer("sourceId"), documents, chunks)
    processing = create_processing_document(repository)
    repository.write_chunks(build_chunks(1, is_retrievable=True))
    admitting = begin_admission(repository, processing, 1)
    ready = build_ready_document(admitting.record, 1)
    failed = replace(
        processing.record,
        status=DocumentStatus.FAILED,
        stage=DocumentStage.TERMINAL,
        failed_at=UTC,
        error=SafeError("processing_failed", "embedding", False),
    )
    documents.before_replace = lambda: documents._store(failed.to_cosmos_item())

    with pytest.raises(RepositoryConflictError, match="concurrently"):
        repository.verify_and_mark_document_ready(ready, admitting.etag)

    stored = repository.get_document(processing.record.source_run_id, processing.record.id)
    assert stored is not None
    assert stored.record.status is DocumentStatus.FAILED


def test_exact_counters_scan_every_page_and_count_only_ready_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(repository_module, "INTERNAL_PAGE_SIZE", 2)
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(runs, documents, StatefulContainer("documentKey"))
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    statuses = (
        DocumentStatus.DISCOVERED,
        DocumentStatus.PROCESSING,
        DocumentStatus.READY,
        DocumentStatus.READY,
        DocumentStatus.FAILED,
    )
    for index, status in enumerate(statuses):
        document = build_document(item_id=f"item-{index}")
        values: dict[str, Any] = {"status": status, "stage": DocumentStage.DISCOVERED}
        if status is DocumentStatus.READY:
            values.update(
                stage=DocumentStage.TERMINAL,
                expected_chunk_count=index,
                written_chunk_count=index,
                ready_at=UTC,
            )
        elif status is DocumentStatus.PROCESSING:
            values.update(stage=DocumentStage.ACL, processing_started_at=UTC)
        elif status is DocumentStatus.FAILED:
            values.update(
                stage=DocumentStage.TERMINAL,
                written_chunk_count=99,
                failed_at=UTC,
                error=SafeError("processing_failed", "embedding", False),
            )
        documents._store(replace(document, **values).to_cosmos_item())
    counters = repository.compute_run_counters(
        activated.run.record.source_id,
        activated.run.record.run_id,
        retries=7,
        items_scanned=42,
    )

    assert counters == RunCounters(
        discovered=1,
        processing=1,
        ready=2,
        failed=1,
        chunks_written=5,
        retries=7,
        items_scanned=42,
    )
    assert len(documents.query_calls) == 3


def test_finalization_requires_all_documents_to_be_terminal() -> None:
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(runs, documents, StatefulContainer("documentKey"))
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    repository.create_discovered_document(build_document())
    terminal_run = build_terminal_run(activated.run.record, RunStatus.COMPLETED)

    with pytest.raises(RepositoryConflictError, match="documents are nonterminal"):
        repository.finalize_run(terminal_run, activated.run.etag, retries=0, items_scanned=1)


def test_transcribing_documents_are_counted_separately_and_do_not_block_finalization() -> None:
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(runs, documents, StatefulContainer("documentKey"))
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    # Audio parked for async batch transcription (status=processing, stage=transcribing) is
    # finalized by the poll timer outside the run and must not block run finalization.
    item = build_document(item_id="audio-1").to_cosmos_item()
    item["status"] = DocumentStatus.PROCESSING.value
    item["stage"] = DocumentStage.TRANSCRIBING.value
    documents._store(item)

    counters = repository.compute_run_counters("source", "run-a", retries=0, items_scanned=1)
    assert counters.transcribing == 1
    assert counters.processing == 0

    finalized = repository.finalize_run(
        build_terminal_run(activated.run.record, RunStatus.COMPLETED),
        activated.run.etag,
        retries=0,
        items_scanned=1,
    )
    assert finalized.record.status is RunStatus.COMPLETED
    assert finalized.record.counters.transcribing == 1


def test_finalization_detects_document_that_becomes_nonterminal_during_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(repository_module, "INTERNAL_PAGE_SIZE", 1)
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(runs, documents, StatefulContainer("documentKey"))
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    for index in range(2):
        document = build_document(item_id=f"item-{index}")
        documents._store(
            replace(
                document,
                discovery_ordinal=index + 1,
                status=DocumentStatus.FAILED,
                stage=DocumentStage.TERMINAL,
                failed_at=UTC,
                error=SafeError("processing_failed", "embedding", False),
            ).to_cosmos_item()
        )
    second_key = ("source:run-a", create_document_id("source", "drive", "item-1"))

    def make_second_document_nonterminal(query_number: int) -> None:
        if query_number == 2:
            stored = documents.items[second_key]
            stored["status"] = DocumentStatus.PROCESSING.value
            stored["stage"] = DocumentStage.EMBEDDING.value

    documents.before_query = make_second_document_nonterminal

    with pytest.raises(RepositoryConflictError, match="documents are nonterminal"):
        repository.finalize_run(
            build_terminal_run(activated.run.record, RunStatus.COMPLETED_WITH_ERRORS),
            activated.run.etag,
            retries=0,
            items_scanned=2,
        )


@pytest.mark.parametrize(
    "status",
    [RunStatus.COMPLETED, RunStatus.COMPLETED_WITH_ERRORS],
)
def test_successful_current_run_finalization_is_atomic_and_replayable(
    status: RunStatus,
) -> None:
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(
        runs, documents, StatefulContainer("documentKey")
    )
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    terminal_run = build_terminal_run(activated.run.record, status)

    finalized = repository.finalize_run(
        terminal_run, activated.run.etag, retries=2, items_scanned=3
    )
    query_count = len(documents.query_calls)
    replay = repository.finalize_run(
        terminal_run, activated.run.etag, retries=2, items_scanned=3
    )

    control = repository.get_source_control("source")
    assert replay.record == finalized.record
    assert control is not None
    assert control.record.last_completed_run_id == "run-a"
    assert control.record.updated_at == terminal_run.updated_at
    assert [operation[0] for operation in runs.batch_calls[-1][1]] == ["replace", "replace"]
    assert len(documents.query_calls) == query_count


def test_exact_concurrent_finalization_commit_is_reconciled() -> None:
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(runs, documents, StatefulContainer("documentKey"))
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    terminal_run = build_terminal_run(activated.run.record, RunStatus.COMPLETED)

    def commit_requested_outcome() -> None:
        runs._store(terminal_run.to_cosmos_item())
        control = repository.get_source_control("source")
        assert control is not None
        runs._store(
            replace(
                control.record,
                last_completed_run_id="run-a",
                updated_at=terminal_run.updated_at,
            ).to_cosmos_item()
        )

    runs.before_batch = commit_requested_outcome

    finalized = repository.finalize_run(
        terminal_run, activated.run.etag, retries=0, items_scanned=0
    )

    assert finalized.record == terminal_run


def test_competing_concurrent_terminal_outcome_wins() -> None:
    runs = StatefulContainer("sourceId")
    repository = IngestionRepository(
        runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey")
    )
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    requested = build_terminal_run(activated.run.record, RunStatus.COMPLETED)
    competing = replace(requested, status=RunStatus.FAILED)
    runs.before_batch = lambda: runs._store(competing.to_cosmos_item())

    with pytest.raises(RepositoryConflictError, match="changed concurrently"):
        repository.finalize_run(
            requested, activated.run.etag, retries=0, items_scanned=0
        )

    stored = repository.get_run("source", "run-a")
    assert stored is not None
    assert stored.record.status is RunStatus.FAILED


def test_terminal_run_outcome_cannot_be_changed() -> None:
    runs = StatefulContainer("sourceId")
    repository = IngestionRepository(
        runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey")
    )
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    completed = build_terminal_run(activated.run.record, RunStatus.COMPLETED)
    repository.finalize_run(completed, activated.run.etag, retries=0, items_scanned=0)

    with pytest.raises(RepositoryConflictError, match="different terminal outcome"):
        repository.finalize_run(
            replace(completed, status=RunStatus.FAILED),
            activated.run.etag,
            retries=0,
            items_scanned=0,
        )


@pytest.mark.parametrize("status", [RunStatus.FAILED, RunStatus.TERMINATED])
def test_unsuccessful_run_never_advances_last_completed_pointer(status: RunStatus) -> None:
    runs = StatefulContainer("sourceId")
    repository = IngestionRepository(
        runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey")
    )
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))

    repository.finalize_run(
        build_terminal_run(activated.run.record, status),
        activated.run.etag,
        retries=0,
        items_scanned=0,
    )

    control = repository.get_source_control("source")
    assert control is not None
    assert control.record.last_completed_run_id is None


def test_queries_are_projected_parameterized_partitioned_and_resumable() -> None:
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(runs, documents, StatefulContainer("documentKey"))
    repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    for index in range(3):
        repository.create_discovered_document(build_document(item_id=f"item-{index}"))
    manifest = build_manifest_pages(build_document(item_id="item-0"))[0]
    documents._store(manifest.to_cosmos_item())

    first = repository.list_document_page("source:run-a", page_size=2)
    second = repository.list_document_page(
        "source:run-a",
        page_size=2,
        continuation_token=first.continuation_token,
    )
    runs_page = repository.list_run_page("source", page_size=1)

    assert len(first.items) == 2
    assert len(second.items) == 1
    assert first.continuation_token is not None
    assert second.continuation_token is None
    assert len(runs_page.items) == 1
    for call in documents.query_calls + runs.query_calls:
        assert "SELECT *" not in call["query"].upper()
        assert call["parameters"]
        assert call["partition_key"] in ("source:run-a", "source")
        assert 0 < call["max_item_count"] <= 100
    assert documents.query_calls[0]["parameters"] == [
        {"name": "@sourceRunId", "value": "source:run-a"},
        {"name": "@recordType", "value": "source_document"},
    ]


def test_run_history_is_ordered_by_started_at_descending() -> None:
    runs = StatefulContainer("sourceId")
    repository = IngestionRepository(
        runs, StatefulContainer("sourceRunId"), StatefulContainer("documentKey")
    )
    repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    later = "2026-08-05T12:01:00Z"
    repository.activate_run(build_run("run-b", later), build_control("run-b", later))

    page = repository.list_run_page("source", page_size=2)

    assert [row["runId"] for row in page.items] == ["run-b", "run-a"]
    assert "ORDER BY c.startedAt DESC" in runs.query_calls[-1]["query"]


def build_audio_operation(**overrides: Any) -> AudioOperationRecord:
    return AudioOperationRecord(**({
        "source_id": "source", "drive_id": "drive", "item_id": "item", "owner_id": "owner",
        "audio": AudioMetadata(3000, 1, "en-US", "fast", "2025-10-15", "1", "etag", "a" * 64),
    } | overrides))


def build_audio_page() -> AudioTranscriptPage:
    return AudioTranscriptPage(
        operation=build_audio_operation(), source_run_id="source:original-run",
        page_index=0, page_count=1,
        segments=(AudioTranscriptSegment(0, 'hello\n"world"', 100, 1000),
                  AudioTranscriptSegment(1, "overlap", 200, 900)),
    )


def test_audio_page_read_round_trip_preserves_original_address_and_state_independence() -> None:
    page = build_audio_page()
    documents = PointReadContainer({
        (page.source_run_id, page.id): page.to_cosmos_item() | {"_etag": "unused", "_ts": 1},
    })
    repo = IngestionRepository(None, documents, None)
    expected = replace(page.operation, state=AudioOperationState.UNKNOWN)
    stored = repo.get_audio_transcript_page(page.source_run_id, page.id, expected_operation=expected)
    assert stored == replace(page, operation=expected)
    assert stored.to_cosmos_item() == page.to_cosmos_item()
    assert documents.reads == [(page.id, page.source_run_id)]


@pytest.mark.parametrize("field,value", [
    ("schemaVersion", True), ("schemaVersion", 1.0), ("schemaVersion", 2),
    ("ownershipEpoch", True), ("ownershipEpoch", 1.0), ("ownershipEpoch", 2),
    ("recordType", "source_document"), ("operationId", "wrong"), ("ownerId", "other"),
    ("id", "wrong"), ("sourceRunId", "source:wrong"), ("pageHash", "a" * 64),
    ("pageIndex", True), ("pageIndex", 0.0), ("pageCount", "1"), ("pageCount", 2),
    ("audio", None), ("audio", []), ("audio", {1: "invalid"}),
    ("segments", None), ("segments", {}), ("segments", []), ("segments", [None]),
    ("segments", [{}]), ("ttl", 60), ("committed", True),
])
def test_audio_page_read_rejects_malformed_fields(field: str, value: Any) -> None:
    page = build_audio_page()
    documents = PointReadContainer({
        (page.source_run_id, page.id): page.to_cosmos_item() | {field: value},
    })
    repo = IngestionRepository(None, documents, None)
    with pytest.raises(RepositoryDataError, match="^audio transcript page does not match schema version 1$"):
        repo.get_audio_transcript_page(page.source_run_id, page.id, expected_operation=page.operation)


@pytest.mark.parametrize("field,value", [
    ("ordinal", True), ("ordinal", 2), ("text", "changed"), ("text", None),
    ("startMs", 100.0), ("endMs", 3001), ("endMs", 999), ("unexpected", "value"),
])
def test_audio_page_read_rejects_changed_or_invalid_segment(field: str, value: Any) -> None:
    page = build_audio_page()
    item = page.to_cosmos_item()
    item["segments"][0][field] = value
    documents = PointReadContainer({(page.source_run_id, page.id): item})
    with pytest.raises(RepositoryDataError):
        IngestionRepository(None, documents, None).get_audio_transcript_page(
            page.source_run_id, page.id, expected_operation=page.operation,
        )


@pytest.mark.parametrize("missing", ["pageHash", "audio", "segments", "schemaVersion"])
def test_audio_page_read_rejects_missing_fields(missing: str) -> None:
    page = build_audio_page()
    item = page.to_cosmos_item()
    del item[missing]
    documents = PointReadContainer({(page.source_run_id, page.id): item})
    with pytest.raises(RepositoryDataError):
        IngestionRepository(None, documents, None).get_audio_transcript_page(
            page.source_run_id, page.id, expected_operation=page.operation,
        )


@pytest.mark.parametrize("item", [[], "invalid", {1: "invalid"}])
def test_audio_page_read_rejects_nonobject_payload(item: Any) -> None:
    page = build_audio_page()
    documents = PointReadContainer({(page.source_run_id, page.id): item})
    with pytest.raises(RepositoryDataError):
        IngestionRepository(None, documents, None).get_audio_transcript_page(
            page.source_run_id, page.id, expected_operation=page.operation,
        )


@pytest.mark.parametrize("field,value", [
    ("source_id", "other"), ("drive_id", "other"), ("item_id", "other"),
    ("owner_id", "other"), ("ownership_epoch", 2),
    ("profile_version", "other"), ("duration_ms", 3001), ("channel_count", 2),
])
def test_audio_page_read_rejects_wrong_independent_operation(field: str, value: Any) -> None:
    page = build_audio_page()
    expected = (
        replace(page.operation, audio=replace(page.operation.audio, **{field: value}))
        if field in ("profile_version", "duration_ms", "channel_count")
        else replace(page.operation, **{field: value})
    )
    documents = PointReadContainer({(page.source_run_id, page.id): page.to_cosmos_item()})
    with pytest.raises(RepositoryDataError):
        IngestionRepository(None, documents, None).get_audio_transcript_page(
            page.source_run_id, page.id, expected_operation=expected,
        )


@pytest.mark.parametrize("wrong_address", ["partition", "id"])
def test_audio_page_read_rejects_valid_rehashed_page_at_wrong_address(wrong_address: str) -> None:
    page = build_audio_page()
    partition = "source:other" if wrong_address == "partition" else page.source_run_id
    page_id = "audio-transcript:" + "0" * 64 if wrong_address == "id" else page.id
    documents = PointReadContainer({(partition, page_id): page.to_cosmos_item()})
    with pytest.raises(RepositoryDataError, match="address does not match point read"):
        IngestionRepository(None, documents, None).get_audio_transcript_page(
            partition, page_id, expected_operation=page.operation,
        )
    assert documents.reads == [(page_id, partition)]


@pytest.mark.parametrize("field,value", [
    ("source_run_id", None), ("source_run_id", " "), ("source_run_id", "x" * 302),
    ("page_id", None), ("page_id", "wrong"), ("page_id", "audio-transcript:" + "A" * 64),
    ("page_id", "audio-transcript:" + "a" * 64 + "\n"), ("expected_operation", {}),
])
def test_audio_page_read_rejects_invalid_inputs_before_io(field: str, value: Any) -> None:
    page = build_audio_page()
    documents = PointReadContainer()
    arguments = {"source_run_id": page.source_run_id, "page_id": page.id, "expected_operation": page.operation}
    arguments[field] = value
    with pytest.raises(ValueError):
        IngestionRepository(None, documents, None).get_audio_transcript_page(**arguments)
    assert not documents.reads


def test_audio_page_read_missing_returns_none() -> None:
    page = build_audio_page()
    documents = PointReadContainer()
    assert IngestionRepository(None, documents, None).get_audio_transcript_page(
        page.source_run_id, page.id, expected_operation=page.operation,
    ) is None
    assert documents.reads == [(page.id, page.source_run_id)]


@pytest.mark.parametrize("status", [403, 429, 500])
def test_audio_page_read_sanitizes_sdk_failure(status: int, monkeypatch: pytest.MonkeyPatch) -> None:
    page = build_audio_page()
    documents = PointReadContainer()

    def fail_read(*, item: str, partition_key: str) -> None:
        documents.reads.append((item, partition_key))
        raise FakeCosmosError(status)

    monkeypatch.setattr(documents, "read_item", fail_read)
    with pytest.raises(RepositoryError, match="^Cosmos point read failed$"):
        IngestionRepository(None, documents, None).get_audio_transcript_page(
            page.source_run_id, page.id, expected_operation=page.operation,
        )
    assert documents.reads == [(page.id, page.source_run_id)]


@pytest.mark.parametrize("prefix", ["a", "\u00e9\U0001f642", '\"\\\n'])
def test_audio_page_read_enforces_exact_byte_limit(prefix: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import ingestion.models as models

    page = replace(build_audio_page(), segments=(AudioTranscriptSegment(0, prefix, 0, 3000),))
    remaining = 128 * 1024 - serialized_size_bytes(page.to_cosmos_item())
    exact = replace(page, segments=(replace(page.segments[0], text=prefix + "a" * remaining),))
    assert serialized_size_bytes(exact.to_cosmos_item()) == 128 * 1024
    documents = PointReadContainer({(exact.source_run_id, exact.id): exact.to_cosmos_item()})
    repo = IngestionRepository(None, documents, None)
    assert repo.get_audio_transcript_page(
        exact.source_run_id, exact.id, expected_operation=exact.operation,
    ) == exact
    with monkeypatch.context() as patch:
        patch.setattr(models, "MAX_DOCUMENT_ITEM_BYTES", 128 * 1024 + 1)
        oversized = replace(exact, segments=(replace(exact.segments[0], text=exact.segments[0].text + "a"),))
        documents.items[(exact.source_run_id, exact.id)] = oversized.to_cosmos_item()
    assert serialized_size_bytes(documents.items[(exact.source_run_id, exact.id)]) == 128 * 1024 + 1
    with pytest.raises(RepositoryDataError):
        repo.get_audio_transcript_page(exact.source_run_id, exact.id, expected_operation=exact.operation)


def test_audio_page_read_accepts_max_partition_and_later_page_without_claiming_completeness() -> None:
    page = replace(
        build_audio_page(), source_run_id="x" * 301, page_index=1, page_count=2,
        segments=(AudioTranscriptSegment(4, "later", 0, 3000),),
    )
    documents = PointReadContainer({(page.source_run_id, page.id): page.to_cosmos_item()})
    assert IngestionRepository(None, documents, None).get_audio_transcript_page(
        page.source_run_id, page.id, expected_operation=page.operation,
    ) == page


@pytest.mark.parametrize("field,value", [
    ("durationMs", 3000.0), ("channelCount", True), ("locale", None), ("unexpected", 1),
])
def test_audio_page_read_rejects_invalid_nested_audio(field: str, value: Any) -> None:
    page = build_audio_page()
    item = page.to_cosmos_item()
    item["audio"][field] = value
    documents = PointReadContainer({(page.source_run_id, page.id): item})
    with pytest.raises(RepositoryDataError):
        IngestionRepository(None, documents, None).get_audio_transcript_page(
            page.source_run_id, page.id, expected_operation=page.operation,
        )


def test_audio_ownership_claim_intent_and_unknown_hold_source_permit() -> None:
    documents = StatefulContainer("sourceRunId")
    repo = IngestionRepository(None, documents, None, audio_pilot_source_id="source")
    operation = build_audio_operation()

    claimed = repo.claim_audio_operation(operation)
    submitted = repo.mark_audio_submission_intent(claimed)
    unknown = repo.mark_audio_operation_unknown(submitted)

    assert unknown.record.state is AudioOperationState.UNKNOWN
    assert documents.items[(operation.source_run_id, "audio-permit")]["state"] == "unknown"
    assert len(documents.items) == 2
    assert len({claimed.etag, submitted.etag, unknown.etag}) == 3
    assert all(partition == operation.source_run_id for partition, _ in documents.batch_calls)
    for candidate in (operation, replace(operation, owner_id="other"), replace(operation, item_id="other")):
        with pytest.raises(RepositoryConflictError):
            repo.claim_audio_operation(candidate)
    with pytest.raises(RepositoryConflictError):
        repo.mark_audio_submission_intent(unknown)
    assert len(documents.items) == 2
    assert not documents.delete_calls


@pytest.mark.parametrize("pilot", [None, "other-source"])
def test_audio_ownership_disabled_or_nonpilot_never_writes(pilot: str | None) -> None:
    documents = StatefulContainer("sourceRunId")
    repo = IngestionRepository(None, documents, None, audio_pilot_source_id=pilot)
    with pytest.raises(RepositoryConflictError, match="configured pilot"):
        repo.claim_audio_operation(build_audio_operation())
    assert not documents.batch_calls


def test_audio_ownership_replay_never_grants_second_submission() -> None:
    documents = StatefulContainer("sourceRunId")
    repo = IngestionRepository(None, documents, None, audio_pilot_source_id="source")
    operation = build_audio_operation()
    claimed = repo.claim_audio_operation(operation)
    with pytest.raises(RepositoryConflictError):
        repo.claim_audio_operation(operation)
    submitted = repo.mark_audio_submission_intent(claimed)
    for replay in (claimed, submitted, repo.get_audio_operation("source", operation.id)):
        with pytest.raises(RepositoryConflictError):
            repo.mark_audio_submission_intent(replay)


@pytest.mark.parametrize("raced_id", ["operation", "permit"])
def test_audio_ownership_lost_cas_cannot_partially_update(raced_id: str) -> None:
    documents = StatefulContainer("sourceRunId")
    repo = IngestionRepository(None, documents, None, audio_pilot_source_id="source")
    claimed = repo.claim_audio_operation(build_audio_operation())
    key = (claimed.record.source_run_id, claimed.record.id if raced_id == "operation" else "audio-permit")

    def concurrent_write() -> None:
        documents._store(documents.items[key] | {"ownerId": "winner"})

    documents.before_batch = concurrent_write
    with pytest.raises(RepositoryConflictError):
        repo.mark_audio_submission_intent(claimed)
    assert all(item["state"] == "validated" for item in documents.items.values())
    assert documents.items[key]["ownerId"] == "winner"


@pytest.mark.parametrize("status", [409, 412, 429, 500])
@pytest.mark.parametrize("raised", [False, True])
def test_audio_ownership_transaction_failure_never_retries(status: int, raised: bool) -> None:
    documents = StatefulContainer("sourceRunId")
    if raised:
        documents.batch_error = FakeCosmosError(status)
    else:
        documents.batch_status_override = status
    repo = IngestionRepository(None, documents, None, audio_pilot_source_id="source")
    error_type = RepositoryConflictError if status in (409, 412) else RepositoryError
    with pytest.raises(error_type) as failure:
        repo.claim_audio_operation(build_audio_operation())
    assert "sensitive" not in str(failure.value)
    assert len(documents.batch_calls) == 1
    assert not documents.items


@pytest.mark.parametrize("field,value", [
    ("schemaVersion", 2), ("recordType", "source_document"), ("state", "ready"),
    ("sourceRunId", "source:run-a"), ("id", "wrong"), ("ownershipEpoch", True),
    ("_etag", ""), ("audio", None), ("audio", []), ("audio", "invalid"),
])
def test_audio_ownership_rejects_malformed_persisted_controls(field: str, value: Any) -> None:
    operation = build_audio_operation()
    documents = StatefulContainer("sourceRunId")
    documents._store(operation.to_cosmos_item())
    documents.items[(operation.source_run_id, operation.id)][field] = value
    repo = IngestionRepository(None, documents, None)
    with pytest.raises(RepositoryDataError):
        repo.get_audio_operation(operation.source_id, operation.id)


@pytest.mark.parametrize("stage", ["claim", "intent", "unknown"])
@pytest.mark.parametrize("failure_mode", ["response", "read"])
def test_audio_ownership_committed_but_response_lost_blocks_replay(
    stage: str, failure_mode: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    documents = StatefulContainer("sourceRunId")
    repo = IngestionRepository(None, documents, None, audio_pilot_source_id="source")
    operation = build_audio_operation()
    current = None
    if stage == "claim":
        expected = AudioOperationState.VALIDATED
    else:
        current = repo.claim_audio_operation(operation)
        if stage == "intent":
            expected = AudioOperationState.SUBMITTING
        else:
            current = repo.mark_audio_submission_intent(current)
            expected = AudioOperationState.UNKNOWN

    def invoke() -> None:
        if stage == "claim":
            repo.claim_audio_operation(operation)
        else:
            assert current is not None
            if stage == "intent":
                repo.mark_audio_submission_intent(current)
            else:
                repo.mark_audio_operation_unknown(current)

    original_batch = documents.execute_item_batch
    original_read = documents.read_item

    def failed_read(**kwargs: Any) -> dict[str, Any]:
        monkeypatch.setattr(documents, "read_item", original_read)
        raise FakeCosmosError(500)

    def committed_batch(**kwargs: Any) -> list[dict[str, int]]:
        result = original_batch(**kwargs)
        monkeypatch.setattr(documents, "execute_item_batch", original_batch)
        if failure_mode == "response":
            raise FakeCosmosError(500)
        monkeypatch.setattr(documents, "read_item", failed_read)
        return result

    monkeypatch.setattr(documents, "execute_item_batch", committed_batch)
    before = len(documents.batch_calls)
    with pytest.raises(RepositoryError) as failure:
        invoke()
    assert "sensitive" not in str(failure.value)
    assert len(documents.batch_calls) == before + 1
    assert len(documents.items) == 2
    assert {item["state"] for item in documents.items.values()} == {expected.value}
    assert not documents.delete_calls
    with pytest.raises(RepositoryConflictError):
        invoke()
    with pytest.raises(RepositoryConflictError):
        repo.claim_audio_operation(replace(operation, owner_id="new-run"))
    assert {item["state"] for item in documents.items.values()} == {expected.value}


def test_bounded_cleanup_never_deletes_the_current_run() -> None:
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    chunks = StatefulContainer("documentKey")
    repository = IngestionRepository(runs, documents, chunks)
    repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    old_document = build_document(run_id="run-a")
    repository.create_discovered_document(old_document)
    repository.write_chunks(build_chunks(3, document=old_document))
    repository.write_visual_manifest_pages(build_manifest_pages(old_document))
    control_partition = create_audio_control_partition_id("source")
    audio_control = documents.create_item(body={
        "id": "audio-operation:sentinel",
        "sourceId": "source",
        "sourceRunId": control_partition,
        "recordType": "audio_operation",
        "status": "unknown",
    })
    later = "2026-08-05T12:01:00Z"
    repository.activate_run(build_run("run-b", later), build_control("run-b", later))

    first = repository.cleanup_run_page("source", "run-a", page_size=2)
    second = repository.cleanup_run_page("source", "run-a", page_size=2)

    assert first == repository_module.CleanupPage(0, 2, False)
    assert second == repository_module.CleanupPage(1, 1, True)
    assert documents.items == {(control_partition, audio_control["id"]): audio_control}
    assert all(partition != control_partition for partition, _ in documents.delete_calls)
    assert not chunks.items
    with pytest.raises(RepositoryConflictError, match="current run"):
        repository.cleanup_run_page("source", "run-b", page_size=2)


def test_stale_run_writes_never_change_source_control() -> None:
    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(runs, documents, StatefulContainer("documentKey"))
    old = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))
    later = "2026-08-05T12:01:00Z"
    repository.activate_run(build_run("run-b", later), build_control("run-b", later))
    terminal_old = replace(
        old.run.record,
        status=RunStatus.COMPLETED,
        stage=RunStage.TERMINAL,
        completed_at=later,
        updated_at=later,
    )

    repository.finalize_run(terminal_old, old.run.etag, retries=0, items_scanned=0)

    current = repository.get_source_control("source")
    assert current is not None
    assert current.record.current_run_id == "run-b"
    assert current.record.last_completed_run_id is None


def test_services_finalize_marks_completed_with_errors_when_documents_failed() -> None:
    from config import IngestionConfig
    from ingestion.services import finalize

    runs = StatefulContainer("sourceId")
    documents = StatefulContainer("sourceRunId")
    repository = IngestionRepository(runs, documents, StatefulContainer("documentKey"))
    activated = repository.activate_run(build_run("run-a", UTC), build_control("run-a", UTC))

    discovered = repository.create_discovered_document(build_document())
    processing = repository.mark_document_processing(
        replace(
            discovered.record,
            status=DocumentStatus.PROCESSING,
            stage=DocumentStage.ACL,
            attempt_count=1,
            processing_started_at=UTC,
        ),
        discovered.etag,
    )
    failed = replace(
        processing.record,
        status=DocumentStatus.FAILED,
        stage=DocumentStage.TERMINAL,
        failed_at=UTC,
        error=SafeError("processing_failed", "embedding", False),
    )
    repository.mark_document_failed(failed, processing.etag)

    config = IngestionConfig(
        extraction_enabled=True, enrichment_enabled=True, summary_enabled=False,
        key_phrases_enabled=True, entities_enabled=True, allowed_extensions=(".pdf",),
        source_id="source", drive_id="drive", tenant_id="tenant", app_client_id="app",
        certificate_secret_name="cert", key_vault_uri="https://kv.example",
        cosmos_endpoint="https://cosmos.example", cosmos_database="db",
        cosmos_ingestion_runs_container="ingestion-runs",
        cosmos_source_documents_container="source-documents",
        cosmos_search_chunks_container="search-chunks",
        document_intelligence_endpoint="https://di.example",
        content_understanding_endpoint="https://cu.example",
        content_understanding_analyzer_id="rag-document-search-v1",
        language_endpoint="https://lang.example", openai_endpoint="https://openai.example",
        vision_deployment="gpt-5.4",
        managed_identity_client_id="mi",
        chunk_max_tokens=800, chunk_overlap_tokens=100, acl_max_pages=10,
        download_timeout_seconds=120.0, delta_max_pages=200, embedding_batch_size=100,
        max_pdf_pages=500, vision_max_output_tokens=400,
        vision_max_image_bytes=2 * 1024 * 1024, vision_max_figures=60,
        query_proxy_timeout_seconds=30.0,
        sharepoint_site_url="",
    )

    finalized = finalize(config, activated.run.etag, repository, items_scanned=1)

    assert finalized.record.status is RunStatus.COMPLETED_WITH_ERRORS
    assert finalized.record.counters.failed == 1


def test_repository_exposes_no_forbidden_api_or_terminology() -> None:
    source = inspect.getsource(repository_module).lower()

    for forbidden in ("lease", "fence", "delta", "checkpoint", "patch_item", "upsert_item"):
        assert forbidden not in source


def build_control(run_id: str, activated_at: str) -> SourceControlRecord:
    return SourceControlRecord(
        source_id="source",
        current_run_id=run_id,
        current_orchestration_instance_id="orchestration-a",
        activated_at=activated_at,
        updated_at=activated_at,
    )


def build_run(run_id: str, activated_at: str) -> IngestionRunRecord:
    return IngestionRunRecord(
        source_id="source",
        run_id=run_id,
        drive_id="drive",
        orchestration_instance_id="orchestration-a",
        status=RunStatus.RUNNING,
        stage=RunStage.DISCOVERING,
        started_at=activated_at,
        activated_at=activated_at,
        updated_at=activated_at,
        counters=RunCounters(),
        profiles=ProfileSnapshot(),
        ingestion_mode="full-sync",
        id=run_record_id(run_id),
    )


def build_terminal_run(
    run: IngestionRunRecord,
    status: RunStatus,
) -> IngestionRunRecord:
    completed_at = "2026-08-05T12:05:00Z"
    return replace(
        run,
        status=status,
        stage=RunStage.TERMINAL,
        completed_at=completed_at,
        updated_at=completed_at,
    )


def build_document(*, item_id: str = "item", run_id: str = "run-a") -> SourceDocumentRecord:
    document_id = create_document_id("source", "drive", item_id)
    return SourceDocumentRecord(
        source_id="source",
        run_id=run_id,
        drive_id="drive",
        item_id=item_id,
        parent_item_id="parent",
        source_name=f"{item_id}.pdf",
        source_path=f"/{item_id}.pdf",
        source_url=f"https://example.invalid/{item_id}.pdf",
        e_tag="source-etag",
        mime_type="application/pdf",
        size_bytes=100,
        discovery_ordinal=1,
        allowed_group_ids=("group-a",),
        acl_hash=content_sha256("group-a"),
        acl_evaluated_at=UTC,
        status=DocumentStatus.DISCOVERED,
        stage=DocumentStage.DISCOVERED,
        attempt_count=0,
        discovered_at=UTC,
        updated_at=UTC,
        id=document_id,
        document_id=document_id,
        source_run_id=create_source_run_id("source", run_id),
        document_key=create_document_key("source", run_id, document_id),
    )


def create_processing_document(
    repository: IngestionRepository,
    *,
    document: SourceDocumentRecord | None = None,
) -> Any:
    discovered = repository.create_discovered_document(document or build_document())
    return repository.mark_document_processing(
        replace(
            discovered.record,
            status=DocumentStatus.PROCESSING,
            stage=DocumentStage.PERSISTING,
            attempt_count=1,
            processing_started_at=UTC,
        ),
        discovered.etag,
    )


def build_ready_document(document: SourceDocumentRecord, count: int) -> SourceDocumentRecord:
    return replace(
        document,
        status=DocumentStatus.READY,
        stage=DocumentStage.TERMINAL,
        expected_chunk_count=count,
        written_chunk_count=count,
        ready_at=UTC,
    )


def begin_admission(
    repository: IngestionRepository,
    processing: Any,
    count: int,
) -> Any:
    return repository.begin_document_admission(
        replace(
            processing.record,
            status=DocumentStatus.ADMITTING,
            stage=DocumentStage.VERIFYING,
            expected_chunk_count=count,
            written_chunk_count=count,
            visual_manifest_page_count=(
                processing.record.visual_manifest_page_count
                if processing.record.visual_manifest_page_count is not None
                else 0
            ),
            visual_manifest_hash=(
                processing.record.visual_manifest_hash
                if processing.record.visual_manifest_hash is not None
                else visual_manifest_hash(())
            ),
        ),
        processing.etag,
    )


def build_manifest_pages(document: SourceDocumentRecord) -> tuple[Any, ...]:
    entries = (
        VisualManifestEntry(
            ordinal=0,
            visual_id="figure-1",
            object_type="figure",
            source_locator=SourceLocator(LocatorKind.PAGE, "Page 1", 1, 1),
            relevance=VisualRelevance.REQUIRED,
            disposition=VisualDisposition.DESCRIBED,
            description="A chart with a rising series.",
            provenance=(ExtractionProvenance.DIRECT,),
        ),
    )
    return create_visual_manifest_pages(document, entries)


def build_chunks(
    count: int,
    *,
    document: SourceDocumentRecord | None = None,
    is_retrievable: bool = False,
) -> tuple[SearchChunkRecord, ...]:
    source_document = document or build_document()
    records: list[SearchChunkRecord] = []
    for index in range(count):
        content = f"Original content {index}."
        records.append(
            SearchChunkRecord(
                source_id=source_document.source_id,
                run_id=source_document.run_id,
                document_id=source_document.document_id,
                document_key=source_document.document_key,
                allowed_group_ids=source_document.allowed_group_ids,
                source_name=source_document.source_name,
                source_url=source_document.source_url,
                page_start=1,
                page_end=1,
                section_path=("Heading",),
                locator_kind=LocatorKind.PAGE,
                locator_label="Page 1",
                locator_ordinal_start=1,
                locator_ordinal_end=1,
                modalities=(ContentModality.TEXT,),
                provenance=(ExtractionProvenance.DIRECT,),
                visual_coverage=VisualCoverageStatus.NOT_REQUIRED,
                chunk_index=index,
                created_at=UTC,
                content=content,
                content_hash=content_sha256(content),
                embedding_text=content,
                searchable_text=content,
                token_count=3,
                enrichment_status=EnrichmentStatuses(
                    ModuleStatus.SUCCEEDED,
                    ModuleStatus.SUCCEEDED,
                    ModuleStatus.SUCCEEDED,
                ),
                summary="Summary.",
                key_phrases=("content",),
                entities=(Entity("Original", "Concept", confidence=0.9),),
                language_code="en",
                embedding=(0.0,) * 3_072,
                embedded_at=UTC,
                id=create_chunk_id(index),
                source_run_id=source_document.source_run_id,
                is_retrievable=is_retrievable,
            )
        )
    return tuple(records)


def _legacy_document_item() -> dict[str, Any]:
    """Simulate a Cosmos item written before source_modified_at was added."""
    document = build_document()
    item = document.to_cosmos_item()
    item.pop("sourceModifiedAt", None)
    return item


def _legacy_chunk_item() -> dict[str, Any]:
    document = build_document()
    chunk = build_chunks(count=1, document=document)[0]
    item = chunk.to_cosmos_item()
    item.pop("sourceModifiedAt", None)
    return item


def test_document_from_item_backward_compat_missing_source_modified_at() -> None:
    record = repository_module._document_from_item(_legacy_document_item())
    assert record.source_modified_at is None


def test_chunk_from_item_backward_compat_missing_source_modified_at() -> None:
    record = repository_module._chunk_from_item(_legacy_chunk_item())
    assert record.source_modified_at is None