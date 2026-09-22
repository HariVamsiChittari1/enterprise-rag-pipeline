"""Cosmos persistence owner for schema-v1 full-sync ingestion."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Generic, Mapping, Sequence, TypeVar

from azure.core import MatchConditions

logger = logging.getLogger(__name__)

from ingestion.models import (
    AudioMetadata,
    AudioOperationRecord,
    AudioOperationState,
    AudioTranscriptPage,
    AudioTranscriptSegment,
    ChunkingProfile,
    ContentModality,
    DocumentStage,
    DocumentStatus,
    EmbeddingProfile,
    EnrichmentProfile,
    EnrichmentStatuses,
    Entity,
    ExtractionProvenance,
    ExtractionProfile,
    IngestionRunRecord,
    LocatorKind,
    ModuleStatus,
    ProfileSnapshot,
    RunCounters,
    RunStage,
    RunStatus,
    SafeError,
    ScaleLimits,
    SearchChunkRecord,
    SOURCE_DOCUMENT_RECORD_TYPE,
    SourceControlRecord,
    SourceDocumentRecord,
    SourceLocator,
    VisualDisposition,
    VisualCoverageStatus,
    VisualManifestEntry,
    VisualManifestPage,
    VisualRelevance,
    VISUAL_MANIFEST_PAGE_RECORD_TYPE,
    create_audio_control_partition_id,
    create_chunk_id,
    create_source_run_id,
    run_record_id,
    serialized_size_bytes,
    visual_manifest_hash,
)


RecordT = TypeVar("RecordT")
MAX_BATCH_OPERATIONS = 100
MAX_BATCH_PAYLOAD_BYTES = 1_000_000
BATCH_OPERATION_OVERHEAD_BYTES = 1_024
MAX_CONFLICT_RETRIES = 3
MAX_THROTTLE_RETRIES = 5
THROTTLE_BASE_DELAY_SECONDS = 1.0
INTERNAL_PAGE_SIZE = 100


class RepositoryError(RuntimeError):
    """Base error with a message safe to expose to application logs."""


class RepositoryDataError(RepositoryError):
    """Raised when persisted schema-v1 data is malformed."""


class RepositoryConflictError(RepositoryError):
    """Raised when persisted state conflicts with the requested operation."""


@dataclass(frozen=True)
class VersionedRecord(Generic[RecordT]):
    """A domain record paired with its Cosmos concurrency token."""

    record: RecordT
    etag: str


@dataclass(frozen=True)
class ActivatedRun:
    """The run and source control committed by one activation transaction."""

    run: VersionedRecord[IngestionRunRecord]
    source_control: VersionedRecord[SourceControlRecord]


@dataclass(frozen=True)
class QueryPage:
    """One bounded SDK query page and its opaque resume token."""

    items: tuple[Mapping[str, Any], ...]
    continuation_token: str | None


@dataclass(frozen=True)
class CleanupPage:
    """Bounded cleanup progress for one noncurrent run."""

    documents_deleted: int
    chunks_deleted: int
    complete: bool


class IngestionRepository:
    """Own schema-v1 ingestion reads and writes across three Cosmos containers."""

    def __init__(
        self, ingestion_runs: Any, source_documents: Any, search_chunks: Any,
        *, audio_pilot_source_id: str | None = None,
    ) -> None:
        if audio_pilot_source_id is not None:
            create_audio_control_partition_id(audio_pilot_source_id)
        self._ingestion_runs = ingestion_runs
        self._source_documents = source_documents
        self._search_chunks = search_chunks
        self._audio_pilot_source_id = audio_pilot_source_id

    def get_audio_operation(
        self, source_id: str, operation_id: str,
    ) -> VersionedRecord[AudioOperationRecord] | None:
        """Read internal ownership metadata, independently of an ingestion run."""
        item = self._read_item(
            self._source_documents, operation_id, create_audio_control_partition_id(source_id),
        )
        stored = self._versioned(item, _audio_operation_from_item, "audio operation")
        if stored is not None and (
            stored.record.source_id != source_id or stored.record.id != operation_id
        ):
            raise RepositoryDataError("audio operation identity does not match point read")
        return stored

    def claim_audio_operation(self, operation: AudioOperationRecord) -> VersionedRecord[AudioOperationRecord]:
        """Create operation and source permit atomically; replay never grants ownership."""
        self._require_audio_pilot(operation)
        if operation.state is not AudioOperationState.VALIDATED or operation.ownership_epoch != 1:
            raise ValueError("new audio operation requires initial validated ownership")
        self._write_audio_transaction(operation, [
            ("create", (operation.to_cosmos_item(),)),
            ("create", (operation.to_permit_item(),)),
        ])
        return self._read_audio_write(operation)

    def mark_audio_submission_intent(
        self, current: VersionedRecord[AudioOperationRecord],
    ) -> VersionedRecord[AudioOperationRecord]:
        """Persist intent once under both ownership tokens, before any provider call."""
        if current.record.state is not AudioOperationState.VALIDATED:
            raise RepositoryConflictError("audio submission intent is already recorded")
        return self._transition_audio_operation(current, AudioOperationState.SUBMITTING)

    def mark_audio_operation_unknown(
        self, current: VersionedRecord[AudioOperationRecord],
    ) -> VersionedRecord[AudioOperationRecord]:
        """Quarantine an ambiguous submission without releasing its source permit."""
        if current.record.state is not AudioOperationState.SUBMITTING:
            raise RepositoryConflictError("only submitted audio intent can become unknown")
        return self._transition_audio_operation(current, AudioOperationState.UNKNOWN)

    def _require_audio_pilot(self, operation: AudioOperationRecord) -> None:
        if self._audio_pilot_source_id is None or operation.source_id != self._audio_pilot_source_id:
            raise RepositoryConflictError("audio operation source is not the configured pilot")

    def _transition_audio_operation(
        self, current: VersionedRecord[AudioOperationRecord], state: AudioOperationState,
    ) -> VersionedRecord[AudioOperationRecord]:
        operation = current.record
        self._require_audio_pilot(operation)
        if not isinstance(current.etag, str) or not current.etag.strip():
            raise ValueError("audio operation requires an ETag")
        permit = self._versioned(
            self._read_item(self._source_documents, "audio-permit", operation.source_run_id),
            _domain_item, "audio permit",
        )
        if permit is None or permit.record != operation.to_permit_item():
            raise RepositoryConflictError("audio operation no longer owns the source permit")
        updated = replace(operation, state=state)
        self._write_audio_transaction(updated, [
            ("replace", (operation.id, updated.to_cosmos_item()), {"if_match_etag": current.etag}),
            ("replace", ("audio-permit", updated.to_permit_item()), {"if_match_etag": permit.etag}),
        ])
        return self._read_audio_write(updated)

    def _write_audio_transaction(
        self, operation: AudioOperationRecord, operations: list[tuple[Any, ...]],
    ) -> None:
        try:
            results = self._source_documents.execute_item_batch(
                batch_operations=operations, partition_key=operation.source_run_id,
            )
        except Exception as error:
            if _error_status(error) in (409, 412):
                raise RepositoryConflictError("audio operation ownership conflicts") from None
            raise RepositoryError("Cosmos audio operation transaction failed") from None
        failure = _batch_failure_status(results)
        if failure in (409, 412):
            raise RepositoryConflictError("audio operation ownership conflicts")
        if failure is not None or not isinstance(results, Sequence) or len(results) != 2:
            raise RepositoryError("Cosmos audio operation transaction failed")

    def _read_audio_write(self, operation: AudioOperationRecord) -> VersionedRecord[AudioOperationRecord]:
        stored = self.get_audio_operation(operation.source_id, operation.id)
        if stored is None or stored.record != operation:
            raise RepositoryConflictError("audio operation changed after transaction")
        return stored

    def get_source_control(self, source_id: str) -> VersionedRecord[SourceControlRecord] | None:
        item = self._read_item(self._ingestion_runs, "source-control", source_id)
        return self._versioned(item, _source_control_from_item, "source control")

    def get_run(self, source_id: str, run_id: str) -> VersionedRecord[IngestionRunRecord] | None:
        item = self._read_item(self._ingestion_runs, run_record_id(run_id), source_id)
        return self._versioned(item, _run_from_item, "ingestion run")

    def get_document(
        self, source_run_id: str, document_id: str
    ) -> VersionedRecord[SourceDocumentRecord] | None:
        item = self._read_item(self._source_documents, document_id, source_run_id)
        return self._versioned(item, _document_from_item, "source document")

    def get_chunk(self, document_key: str, chunk_id: str) -> SearchChunkRecord | None:
        item = self._read_item(self._search_chunks, chunk_id, document_key)
        if item is None:
            return None
        return _hydrate(item, _chunk_from_item, "search chunk")

    def get_audio_transcript_page(
        self, source_run_id: str, page_id: str, *, expected_operation: AudioOperationRecord,
    ) -> AudioTranscriptPage | None:
        """Read page integrity against independent ownership, not commitment or authorization."""
        if not isinstance(expected_operation, AudioOperationRecord):
            raise ValueError("audio page requires expected operation metadata")
        if not isinstance(source_run_id, str) or not source_run_id.strip() or len(source_run_id) > 301:
            raise ValueError("audio page partition is invalid")
        if not isinstance(page_id, str) or re.fullmatch(r"audio-transcript:[0-9a-f]{64}", page_id) is None:
            raise ValueError("audio page ID is invalid")
        item = self._read_item(self._source_documents, page_id, source_run_id)
        if item is None:
            return None
        page = _hydrate(
            item, lambda stored: _audio_transcript_page_from_item(stored, expected_operation),
            "audio transcript page",
        )
        if page.id != page_id or page.source_run_id != source_run_id:
            raise RepositoryDataError("audio transcript page address does not match point read")
        return page

    def get_visual_manifest_page(
        self,
        source_run_id: str,
        page_id: str,
    ) -> VisualManifestPage | None:
        item = self._read_item(self._source_documents, page_id, source_run_id)
        if item is None:
            return None
        return _hydrate(item, _visual_manifest_page_from_item, "visual manifest page")

    def activate_run(
        self,
        run: IngestionRunRecord,
        source_control: SourceControlRecord,
    ) -> ActivatedRun:
        current = self.get_source_control(run.source_id)
        if current is not None:
            source_control = replace(
                source_control,
                last_completed_run_id=current.record.last_completed_run_id,
            )
        self._validate_activation(run, source_control)
        if current is not None and current.record.current_run_id == run.run_id:
            return self._reconcile_activation(run, source_control)
        if current is not None and _parse_utc(source_control.activated_at) <= _parse_utc(
            current.record.activated_at
        ):
            raise RepositoryConflictError("run activation is stale")

        operations: list[tuple[Any, ...]] = [("create", (run.to_cosmos_item(),))]
        if current is None:
            operations.append(("create", (source_control.to_cosmos_item(),)))
        else:
            operations.append(
                (
                    "replace",
                    (source_control.id, source_control.to_cosmos_item()),
                    {"if_match_etag": current.etag},
                )
            )
        try:
            results = self._ingestion_runs.execute_item_batch(
                batch_operations=operations,
                partition_key=run.source_id,
            )
            failure_status = _batch_failure_status(results)
            if failure_status is not None:
                if failure_status in (409, 412):
                    return self._reconcile_activation(run, source_control)
                raise RepositoryError("Cosmos run activation failed")
        except Exception as error:
            if _error_status(error) in (409, 412):
                return self._reconcile_activation(run, source_control)
            if isinstance(error, RepositoryError):
                raise
            raise RepositoryError("Cosmos run activation failed") from None
        return self._read_activated_run(run.source_id, run.run_id)

    def create_discovered_document(
        self, document: SourceDocumentRecord
    ) -> VersionedRecord[SourceDocumentRecord]:
        if document.status is not DocumentStatus.DISCOVERED or document.stage is not DocumentStage.DISCOVERED:
            raise ValueError("new source documents must be discovered")
        try:
            self._source_documents.create_item(body=document.to_cosmos_item())
        except Exception as error:
            if getattr(error, "status_code", None) != 409:
                raise RepositoryError("Cosmos source-document create failed") from None
            stored = self.get_document(document.source_run_id, document.id)
            if stored is None or not _same_domain(stored.record, document):
                raise RepositoryConflictError("source-document id has different content") from None
            return stored
        stored = self.get_document(document.source_run_id, document.id)
        if stored is None:
            raise RepositoryDataError("created source document could not be read")
        return stored

    def mark_document_processing(
        self,
        document: SourceDocumentRecord,
        etag: str,
    ) -> VersionedRecord[SourceDocumentRecord]:
        return self._replace_document(
            document,
            etag,
            expected_status=DocumentStatus.DISCOVERED,
            allowed_status=DocumentStatus.PROCESSING,
            allowed_changes={
                "status",
                "stage",
                "attemptCount",
                "processingStartedAt",
                "updatedAt",
            },
        )

    def update_processing_document(
        self,
        document: SourceDocumentRecord,
        etag: str,
    ) -> VersionedRecord[SourceDocumentRecord]:
        return self._replace_document(
            document,
            etag,
            expected_status=DocumentStatus.PROCESSING,
            allowed_status=DocumentStatus.PROCESSING,
            allowed_changes={
                "stage",
                "attemptCount",
                "updatedAt",
                "pageCount",
                "expectedChunkCount",
                "writtenChunkCount",
                "visualManifestPageCount",
                "visualManifestHash",
                "contentHash",
                "extractionMode",
                "audio",
                "transcriptionJobUrl",
                "stagingBlobName",
            },
        )

    def mark_document_failed(
        self,
        document: SourceDocumentRecord,
        etag: str,
    ) -> VersionedRecord[SourceDocumentRecord]:
        if document.stage is not DocumentStage.TERMINAL or document.failed_at is None or document.error is None:
            raise ValueError("failed documents require terminal stage, failed_at, and safe error")
        if document.ready_at is not None:
            raise ValueError("failed documents cannot contain ready_at")
        return self._replace_document(
            document,
            etag,
            expected_status=DocumentStatus.PROCESSING,
            allowed_status=DocumentStatus.FAILED,
            allowed_changes={"status", "stage", "updatedAt", "failedAt", "error"},
        )

    def begin_document_admission(
        self,
        document: SourceDocumentRecord,
        etag: str,
    ) -> VersionedRecord[SourceDocumentRecord]:
        # Audio documents carry transcript chunks but no visual manifest.
        is_audio = document.audio is not None
        if (
            document.status is not DocumentStatus.ADMITTING
            or document.stage is not DocumentStage.VERIFYING
            or document.expected_chunk_count is None
            or document.written_chunk_count != document.expected_chunk_count
            or (not is_audio and (document.visual_manifest_page_count is None or document.visual_manifest_hash is None))
            or not document.allowed_group_ids
            or not document.acl_hash
            or document.acl_evaluated_at is None
        ):
            raise ValueError("admitting document integrity fields are incomplete")
        if not is_audio:
            self._verify_visual_manifest(document)
        return self._replace_document(
            document,
            etag,
            expected_status=DocumentStatus.PROCESSING,
            allowed_status=DocumentStatus.ADMITTING,
            allowed_changes={
                "status",
                "stage",
                "updatedAt",
                "allowedGroupIds",
                "aclHash",
                "aclEvaluatedAt",
                "expectedChunkCount",
                "writtenChunkCount",
                "visualManifestPageCount",
                "visualManifestHash",
                "sourceVerifiedAt",
            },
        )

    def fail_nonterminal_documents(self, source_id: str, run_id: str, error_message: str) -> int:
        """Mark all discovered/processing docs as failed for orchestration termination."""
        source_run_id = create_source_run_id(source_id, run_id)
        query = (
            "SELECT * FROM c "
            "WHERE c.sourceRunId = @sourceRunId "
            "AND c.recordType = @recordType "
            "AND c.status IN ('discovered', 'processing')"
        )
        parameters = [
            {"name": "@sourceRunId", "value": source_run_id},
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
        ]
        failed_count = 0
        continuation: str | None = None
        now = datetime.now(tz=__import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        while True:
            page, continuation = self._query_page(
                self._source_documents, query, parameters,
                source_run_id, INTERNAL_PAGE_SIZE, continuation,
            )
            for row in page:
                doc_id = row["id"]
                etag = row.get("_etag")
                original_status = row.get("status", "unknown")
                row["status"] = DocumentStatus.FAILED.value
                row["stage"] = DocumentStage.TERMINAL.value
                row["failedAt"] = now
                row["updatedAt"] = now
                row["error"] = {"message": error_message, "stage": original_status, "retryable": False}
                try:
                    self._source_documents.replace_item(
                        item=doc_id,
                        body=row,
                        etag=etag,
                        match_condition=MatchConditions.IfNotModified,
                    )
                    failed_count += 1
                except Exception:
                    logger.warning("Could not fail doc %s during termination", doc_id, exc_info=True)
            if continuation is None:
                break
        return failed_count

    def get_failed_documents(self, source_id: str, run_id: str) -> list[dict[str, Any]]:
        """Return all failed documents for a given run."""
        source_run_id = create_source_run_id(source_id, run_id)
        query = (
            "SELECT * FROM c WHERE c.sourceRunId = @sourceRunId "
            "AND c.recordType = @recordType AND c.status = 'failed'"
        )
        parameters = [
            {"name": "@sourceRunId", "value": source_run_id},
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
        ]
        results: list[dict[str, Any]] = []
        continuation: str | None = None
        while True:
            page, continuation = self._query_page(
                self._source_documents, query, parameters,
                source_run_id, INTERNAL_PAGE_SIZE, continuation,
            )
            results.extend(page)
            if continuation is None:
                break
        return results

    def reset_failed_to_discovered(self, doc: dict[str, Any]) -> dict[str, Any] | None:
        """Reset a failed document back to discovered so it can be reprocessed."""
        doc_id = doc["id"]
        etag = doc.get("_etag")
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        doc["status"] = DocumentStatus.DISCOVERED.value
        doc["stage"] = DocumentStage.DISCOVERED.value
        doc["attemptCount"] = 0
        doc["updatedAt"] = now
        doc["retriedAt"] = now
        doc["processingStartedAt"] = None
        doc["failedAt"] = None
        doc["error"] = None
        doc["pageCount"] = None
        doc["expectedChunkCount"] = None
        doc["writtenChunkCount"] = None
        doc["contentHash"] = None
        doc["extractionMode"] = None
        doc["readyAt"] = None
        try:
            self._source_documents.replace_item(
                item=doc_id,
                body=doc,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
            return doc
        except Exception:
            logger.warning("Could not reset doc %s for retry", doc_id, exc_info=True)
            return None

    def write_chunks(self, chunks: Sequence[SearchChunkRecord]) -> int:
        validated = self._validate_chunks(chunks)
        batch: list[SearchChunkRecord] = []
        payload_bytes = 0
        for chunk in validated:
            operation_bytes = (
                serialized_size_bytes(chunk.to_cosmos_item()) + BATCH_OPERATION_OVERHEAD_BYTES
            )
            if operation_bytes > MAX_BATCH_PAYLOAD_BYTES:
                raise ValueError("chunk operation exceeds the application batch payload limit")
            if batch and (
                len(batch) == MAX_BATCH_OPERATIONS
                or payload_bytes + operation_bytes > MAX_BATCH_PAYLOAD_BYTES
            ):
                self._create_chunk_batch(tuple(batch))
                batch = []
                payload_bytes = 0
            batch.append(chunk)
            payload_bytes += operation_bytes
        if batch:
            self._create_chunk_batch(tuple(batch))
        return len(validated)

    def write_visual_manifest_pages(
        self,
        pages: Sequence[VisualManifestPage],
    ) -> int:
        validated = self._validate_visual_manifest_pages(pages)
        for page in validated:
            try:
                self._source_documents.create_item(body=page.to_cosmos_item())
            except Exception as error:
                if _error_status(error) != 409:
                    raise RepositoryError("Cosmos visual-manifest create failed") from None
                stored = self.get_visual_manifest_page(page.source_run_id, page.id)
                if stored is None or not _same_domain(stored, page):
                    raise RepositoryConflictError(
                        "visual-manifest page id has different content"
                    ) from None
        return len(validated)

    def verify_and_mark_document_ready(
        self,
        document: SourceDocumentRecord,
        etag: str,
    ) -> VersionedRecord[SourceDocumentRecord]:
        if (
            document.status is not DocumentStatus.READY
            or document.stage is not DocumentStage.TERMINAL
            or document.expected_chunk_count is None
            or document.written_chunk_count != document.expected_chunk_count
            or document.ready_at is None
            or document.failed_at is not None
            or document.error is not None
        ):
            raise ValueError("ready document integrity fields are incomplete")
        # Audio documents have transcript chunks but no visual manifest to verify.
        if document.audio is None:
            self._verify_visual_manifest(document)
        self._verify_exact_chunks(document)
        return self._replace_document(
            document,
            etag,
            expected_status=DocumentStatus.ADMITTING,
            allowed_status=DocumentStatus.READY,
            allowed_changes={
                "status",
                "stage",
                "updatedAt",
                "allowedGroupIds",
                "aclHash",
                "aclEvaluatedAt",
                "expectedChunkCount",
                "writtenChunkCount",
                "readyAt",
            },
        )

    def compute_run_counters(
        self,
        source_id: str,
        run_id: str,
        *,
        retries: int,
        items_scanned: int,
    ) -> RunCounters:
        if retries < 0 or items_scanned < 0:
            raise ValueError("run counters cannot be negative")
        source_run_id = create_source_run_id(source_id, run_id)
        query = (
            "SELECT c.status, c.writtenChunkCount FROM c "
            "WHERE c.sourceRunId = @sourceRunId AND c.recordType = @recordType"
        )
        parameters = [
            {"name": "@sourceRunId", "value": source_run_id},
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
        ]
        counts = {status.value: 0 for status in DocumentStatus}
        chunks_written = 0
        continuation: str | None = None
        while True:
            page, continuation = self._query_page(
                self._source_documents,
                query,
                parameters,
                source_run_id,
                INTERNAL_PAGE_SIZE,
                continuation,
            )
            for row in page:
                status = row.get("status")
                if status not in counts:
                    raise RepositoryDataError("source document has an invalid persisted status")
                counts[status] += 1
                if status == DocumentStatus.READY.value:
                    written = row.get("writtenChunkCount")
                    if not isinstance(written, int) or written < 0:
                        raise RepositoryDataError("ready source document has an invalid chunk count")
                    chunks_written += written
            if continuation is None:
                break
        return RunCounters(
            discovered=counts[DocumentStatus.DISCOVERED.value],
            processing=counts[DocumentStatus.PROCESSING.value],
            ready=counts[DocumentStatus.READY.value],
            failed=counts[DocumentStatus.FAILED.value],
            chunks_written=chunks_written,
            retries=retries,
            items_scanned=items_scanned,
        )

    def update_run(
        self,
        run: IngestionRunRecord,
        etag: str,
    ) -> VersionedRecord[IngestionRunRecord]:
        if run.status in {
            RunStatus.COMPLETED,
            RunStatus.COMPLETED_WITH_ERRORS,
            RunStatus.FAILED,
            RunStatus.TERMINATED,
        } or run.stage is RunStage.TERMINAL:
            raise ValueError("terminal run changes must use finalize_run")
        return self._replace_run(
            run,
            etag,
            allowed_changes={
                "status",
                "stage",
                "updatedAt",
                "error",
            },
        )

    def finalize_run(
        self,
        run: IngestionRunRecord,
        etag: str,
        *,
        retries: int,
        items_scanned: int,
    ) -> VersionedRecord[IngestionRunRecord]:
        terminal_statuses = {
            RunStatus.COMPLETED,
            RunStatus.COMPLETED_WITH_ERRORS,
            RunStatus.FAILED,
            RunStatus.TERMINATED,
        }
        if run.status not in terminal_statuses or run.stage is not RunStage.TERMINAL or run.completed_at is None:
            raise ValueError("finalized runs require terminal status, stage, and completed_at")
        current = self.get_run(run.source_id, run.run_id)
        if current is None:
            raise RepositoryConflictError("ingestion run no longer exists")
        if current.record.stage is RunStage.TERMINAL:
            replay_counters = replace(
                current.record.counters,
                retries=retries,
                items_scanned=items_scanned,
            )
            if _same_domain(current.record, replace(run, counters=replay_counters)):
                return current
            raise RepositoryConflictError("ingestion run already has a different terminal outcome")
        if current.etag != etag:
            raise RepositoryConflictError("ingestion run changed concurrently")
        exact_counters = self.compute_run_counters(
            run.source_id,
            run.run_id,
            retries=retries,
            items_scanned=items_scanned,
        )
        if exact_counters.discovered or exact_counters.processing:
            raise RepositoryConflictError("run cannot finalize while documents are nonterminal")
        finalized = replace(run, counters=exact_counters)
        return self._commit_finalized_run(finalized, etag)

    def _commit_finalized_run(
        self,
        finalized: IngestionRunRecord,
        etag: str,
    ) -> VersionedRecord[IngestionRunRecord]:
        current_run = self.get_run(finalized.source_id, finalized.run_id)
        if current_run is None:
            raise RepositoryConflictError("ingestion run no longer exists")
        if current_run.record.stage is RunStage.TERMINAL:
            if _same_domain(current_run.record, finalized):
                return current_run
            raise RepositoryConflictError("ingestion run already has a different terminal outcome")
        if current_run.etag != etag:
            raise RepositoryConflictError("ingestion run changed concurrently")
        _require_only_record_changes(
            current_run.record,
            finalized,
            {
                "status",
                "stage",
                "updatedAt",
                "completedAt",
                "error",
                "counters",
            },
            "run finalization",
        )

        control = self.get_source_control(finalized.source_id)
        successful = finalized.status in {
            RunStatus.COMPLETED,
            RunStatus.COMPLETED_WITH_ERRORS,
        }
        if successful and control is not None and control.record.current_run_id == finalized.run_id:
            completed_control = replace(
                control.record,
                last_completed_run_id=finalized.run_id,
                updated_at=finalized.updated_at,
            )
            operations = [
                (
                    "replace",
                    (finalized.id, finalized.to_cosmos_item()),
                    {"if_match_etag": current_run.etag},
                ),
                (
                    "replace",
                    (completed_control.id, completed_control.to_cosmos_item()),
                    {"if_match_etag": control.etag},
                ),
            ]
            try:
                results = self._ingestion_runs.execute_item_batch(
                    batch_operations=operations,
                    partition_key=finalized.source_id,
                )
                failure_status = _batch_failure_status(results)
                if failure_status is not None:
                    if failure_status in (409, 412):
                        return self._reconcile_finalization(finalized, update_control=True)
                    raise RepositoryError("Cosmos run finalization failed")
            except Exception as error:
                if _error_status(error) in (409, 412):
                    return self._reconcile_finalization(finalized, update_control=True)
                if isinstance(error, RepositoryError):
                    raise
                raise RepositoryError("Cosmos run finalization failed") from None
            return self._reconcile_finalization(finalized, update_control=True)

        try:
            self._ingestion_runs.replace_item(
                item=finalized.id,
                body=finalized.to_cosmos_item(),
                etag=current_run.etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) == 412:
                return self._reconcile_finalization(finalized, update_control=False)
            raise RepositoryError("Cosmos run finalization failed") from None
        return self._reconcile_finalization(finalized, update_control=False)

    def _reconcile_finalization(
        self,
        finalized: IngestionRunRecord,
        *,
        update_control: bool,
    ) -> VersionedRecord[IngestionRunRecord]:
        stored = self.get_run(finalized.source_id, finalized.run_id)
        if stored is None or not _same_domain(stored.record, finalized):
            raise RepositoryConflictError("ingestion run finalization changed concurrently")
        if update_control:
            control = self.get_source_control(finalized.source_id)
            if (
                control is None
                or control.record.current_run_id != finalized.run_id
                or control.record.last_completed_run_id != finalized.run_id
                or control.record.updated_at != finalized.updated_at
            ):
                raise RepositoryConflictError("source control finalization changed concurrently")
        return stored

    def list_document_page(
        self,
        source_run_id: str,
        *,
        page_size: int,
        continuation_token: str | None = None,
        status: DocumentStatus | None = None,
    ) -> QueryPage:
        _validate_page_size(page_size)
        query = (
            "SELECT c.id, c.sourceId, c.runId, c.sourceRunId, c.documentKey, c.sourceName, "
            "c.sourcePath, c.status, c.stage, c.discoveryOrdinal, c.attemptCount, "
            "c.expectedChunkCount, c.writtenChunkCount, c.updatedAt, c.error.code AS errorCode "
            "FROM c WHERE c.sourceRunId = @sourceRunId AND c.recordType = @recordType"
        )
        parameters: list[dict[str, Any]] = [
            {"name": "@sourceRunId", "value": source_run_id},
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
        ]
        if status is not None:
            query += " AND c.status = @status"
            parameters.append({"name": "@status", "value": status.value})
        query += " ORDER BY c.discoveryOrdinal ASC"
        rows, token = self._query_page(
            self._source_documents,
            query,
            parameters,
            source_run_id,
            page_size,
            continuation_token,
        )
        return QueryPage(tuple(rows), token)

    def list_run_page(
        self,
        source_id: str,
        *,
        page_size: int,
        continuation_token: str | None = None,
    ) -> QueryPage:
        _validate_page_size(page_size)
        query = (
            "SELECT c.id, c.runId, c.sourceId, c.status, c.stage, c.startedAt, c.activatedAt, "
            "c.updatedAt, c.completedAt, c.counters, c.error.code AS errorCode "
            "FROM c WHERE c.sourceId = @sourceId AND STARTSWITH(c.id, \"run:\") "
            "ORDER BY c.startedAt DESC"
        )
        parameters = [
            {"name": "@sourceId", "value": source_id},
        ]
        rows, token = self._query_page(
            self._ingestion_runs,
            query,
            parameters,
            source_id,
            page_size,
            continuation_token,
        )
        return QueryPage(tuple(rows), token)

    def cleanup_run_page(
        self,
        source_id: str,
        run_id: str,
        *,
        page_size: int,
    ) -> CleanupPage:
        _validate_page_size(page_size)
        self._ensure_not_current(source_id, run_id)
        source_run_id = create_source_run_id(source_id, run_id)
        documents = self.list_document_page(source_run_id, page_size=page_size)
        documents_deleted = 0
        chunks_deleted = 0
        incomplete_chunks = False
        for row in documents.items:
            document_id = row.get("id")
            document_key = row.get("documentKey")
            if not isinstance(document_id, str) or not isinstance(document_key, str):
                raise RepositoryDataError("cleanup query returned invalid document identity")
            chunk_query = (
                "SELECT c.id, c.documentKey FROM c WHERE c.documentKey = @documentKey"
            )
            chunks, chunk_token = self._query_page(
                self._search_chunks,
                chunk_query,
                [{"name": "@documentKey", "value": document_key}],
                document_key,
                page_size,
                None,
            )
            manifest_query = (
                "SELECT c.id, c.documentId FROM c "
                "WHERE c.sourceRunId = @sourceRunId "
                "AND c.documentId = @documentId AND c.recordType = @recordType"
            )
            manifest_pages, manifest_token = self._query_page(
                self._source_documents,
                manifest_query,
                [
                    {"name": "@sourceRunId", "value": source_run_id},
                    {"name": "@documentId", "value": document_id},
                    {
                        "name": "@recordType",
                        "value": VISUAL_MANIFEST_PAGE_RECORD_TYPE,
                    },
                ],
                source_run_id,
                page_size,
                None,
            )
            self._ensure_not_current(source_id, run_id)
            for chunk in chunks:
                chunk_id = chunk.get("id")
                if not isinstance(chunk_id, str) or chunk.get("documentKey") != document_key:
                    raise RepositoryDataError("cleanup query returned invalid chunk identity")
                if self._delete_item(self._search_chunks, chunk_id, document_key):
                    chunks_deleted += 1
            for manifest_page in manifest_pages:
                manifest_page_id = manifest_page.get("id")
                if (
                    not isinstance(manifest_page_id, str)
                    or manifest_page.get("documentId") != document_id
                ):
                    raise RepositoryDataError(
                        "cleanup query returned invalid visual-manifest identity"
                    )
                self._delete_item(
                    self._source_documents,
                    manifest_page_id,
                    source_run_id,
                )
            if chunk_token is None and manifest_token is None:
                self._ensure_not_current(source_id, run_id)
                if self._delete_item(self._source_documents, document_id, source_run_id):
                    documents_deleted += 1
            else:
                incomplete_chunks = True
        complete = documents.continuation_token is None and not incomplete_chunks
        return CleanupPage(documents_deleted, chunks_deleted, complete)

    def _replace_run(
        self,
        run: IngestionRunRecord,
        etag: str,
        *,
        allowed_changes: set[str],
    ) -> VersionedRecord[IngestionRunRecord]:
        current = self.get_run(run.source_id, run.run_id)
        if current is None:
            raise RepositoryConflictError("ingestion run no longer exists")
        if current.etag != etag:
            raise RepositoryConflictError("ingestion run changed concurrently")
        _require_only_record_changes(current.record, run, allowed_changes, "run update")
        try:
            self._ingestion_runs.replace_item(
                item=run.id,
                body=run.to_cosmos_item(),
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if getattr(error, "status_code", None) == 412:
                raise RepositoryConflictError("ingestion run changed concurrently") from None
            raise RepositoryError("Cosmos ingestion-run replace failed") from None
        stored = self.get_run(run.source_id, run.run_id)
        if stored is None:
            raise RepositoryDataError("updated ingestion run could not be read")
        return stored

    def _ensure_not_current(self, source_id: str, run_id: str) -> None:
        control = self.get_source_control(source_id)
        if control is None:
            raise RepositoryConflictError("source control is unavailable for cleanup")
        if control.record.current_run_id == run_id:
            raise RepositoryConflictError("current run cannot be cleaned up")

    @staticmethod
    def _delete_item(container: Any, item_id: str, partition_key: str) -> bool:
        try:
            container.delete_item(item=item_id, partition_key=partition_key)
            return True
        except Exception as error:
            if getattr(error, "status_code", None) == 404:
                return False
            raise RepositoryError("Cosmos cleanup delete failed") from None

    @staticmethod
    def _validate_chunks(chunks: Sequence[SearchChunkRecord]) -> tuple[SearchChunkRecord, ...]:
        if not chunks:
            raise ValueError("chunk input cannot be empty")
        validated = tuple(chunks)
        if any(not isinstance(chunk, SearchChunkRecord) for chunk in validated):
            raise TypeError("chunks must be SearchChunkRecord values")
        if len(validated) > ScaleLimits().max_chunks_per_pdf:
            raise ValueError("chunk input exceeds the schema-v1 document limit")
        first = validated[0]
        identity = (
            first.document_key,
            first.document_id,
            first.source_id,
            first.run_id,
            first.source_run_id,
        )
        for index, chunk in enumerate(validated):
            if (
                chunk.document_key,
                chunk.document_id,
                chunk.source_id,
                chunk.run_id,
                chunk.source_run_id,
            ) != identity:
                raise ValueError("all chunks must belong to one document and run")
            if chunk.chunk_index != index or chunk.id != create_chunk_id(index):
                raise ValueError("chunk ids must be sorted, unique, and contiguous from zero")
        return validated

    def _create_chunk_batch(self, chunks: tuple[SearchChunkRecord, ...]) -> None:
        remaining = chunks
        for _ in range(MAX_CONFLICT_RETRIES):
            self._execute_batch_with_throttle_retry(remaining)
            missing: list[SearchChunkRecord] = []
            for chunk in remaining:
                stored = self.get_chunk(chunk.document_key, chunk.id)
                if stored is None:
                    missing.append(chunk)
                elif not _same_domain(stored, chunk):
                    raise RepositoryConflictError("chunk id has different content")
            if not missing:
                return
            remaining = tuple(missing)
        raise RepositoryConflictError("chunk batch conflicts did not converge")

    @staticmethod
    def _validate_visual_manifest_pages(
        pages: Sequence[VisualManifestPage],
    ) -> tuple[VisualManifestPage, ...]:
        validated = tuple(pages)
        if any(not isinstance(page, VisualManifestPage) for page in validated):
            raise TypeError("manifest pages must be VisualManifestPage values")
        if not validated:
            return validated
        if tuple(page.page_index for page in validated) != tuple(range(len(validated))):
            raise ValueError("manifest page indices must be contiguous from zero")
        if any(page.page_count != len(validated) for page in validated):
            raise ValueError("manifest page count is inconsistent")
        first = validated[0]
        identity = (first.source_run_id, first.document_id, first.document_key)
        if any(
            (page.source_run_id, page.document_id, page.document_key) != identity
            for page in validated
        ):
            raise ValueError("manifest pages must belong to one document and run")
        visual_manifest_hash(validated)
        return validated

    def _execute_batch_with_throttle_retry(
        self, chunks: tuple[SearchChunkRecord, ...]
    ) -> None:
        """Execute batch with retry on 429 throttling."""
        operations = [("create", (chunk.to_cosmos_item(),)) for chunk in chunks]
        for attempt in range(MAX_THROTTLE_RETRIES):
            try:
                results = self._search_chunks.execute_item_batch(
                    batch_operations=operations,
                    partition_key=chunks[0].document_key,
                )
                failure_status = _batch_failure_status(results)
                if failure_status is None:
                    return
                if failure_status == 429:
                    delay = THROTTLE_BASE_DELAY_SECONDS * (2 ** attempt)
                    logger.warning("Cosmos batch throttled (429), retry %d after %.1fs", attempt + 1, delay)
                    time.sleep(delay)
                    continue
                if failure_status == 409:
                    return  # handled by caller's conflict resolution
                raise RepositoryError(f"Cosmos chunk batch failed with status {failure_status}")
            except Exception as error:
                status = _error_status(error)
                if status == 429:
                    delay = THROTTLE_BASE_DELAY_SECONDS * (2 ** attempt)
                    logger.warning("Cosmos batch throttled (429 exception), retry %d after %.1fs", attempt + 1, delay)
                    time.sleep(delay)
                    continue
                if status == 409:
                    return  # handled by caller's conflict resolution
                if isinstance(error, RepositoryError):
                    raise
                raise RepositoryError(f"Cosmos chunk batch create failed: {error}") from error
        raise RepositoryError("Cosmos batch throttled after max retries")

    def _verify_exact_chunks(self, document: SourceDocumentRecord) -> None:
        expected_count = document.expected_chunk_count or 0
        query = (
            "SELECT c.id, c.chunkIndex, c.documentKey, c.documentId, c.sourceId, c.runId, "
            "c.sourceRunId, c.allowedGroupIds, c.isRetrievable, c.lifecycleGeneration "
            "FROM c WHERE c.documentKey = @documentKey"
        )
        parameters = [{"name": "@documentKey", "value": document.document_key}]
        seen_ids: set[str] = set()
        seen_indices: set[int] = set()
        row_count = 0
        continuation: str | None = None
        while True:
            page, continuation = self._query_page(
                self._search_chunks,
                query,
                parameters,
                document.document_key,
                INTERNAL_PAGE_SIZE,
                continuation,
            )
            for row in page:
                row_count += 1
                chunk_id = row.get("id")
                chunk_index = row.get("chunkIndex")
                if not isinstance(chunk_id, str) or (
                    not isinstance(chunk_index, int) or isinstance(chunk_index, bool)
                ):
                    raise RepositoryConflictError("ready verification found invalid chunk identity")
                if chunk_id in seen_ids or chunk_index in seen_indices:
                    raise RepositoryConflictError("ready verification found duplicate chunks")
                if chunk_index < 0 or chunk_id != create_chunk_id(chunk_index):
                    raise RepositoryConflictError("ready verification found invalid chunk identity")
                if chunk_index >= expected_count:
                    raise RepositoryConflictError("ready verification found missing or extra chunks")
                _require_chunk_row_identity(row, document)
                if row.get("isRetrievable") is not True:
                    raise RepositoryConflictError(
                        "ready verification found ineligible chunks"
                    )
                if row.get("lifecycleGeneration") != document.lifecycle_generation:
                    raise RepositoryConflictError(
                        "ready verification found a lifecycle generation mismatch"
                    )
                if row.get("allowedGroupIds") != list(document.allowed_group_ids):
                    raise RepositoryConflictError(
                        "ready verification found an ACL mismatch"
                    )
                seen_ids.add(chunk_id)
                seen_indices.add(chunk_index)
            if continuation is None:
                break
        if row_count < expected_count:
            raise RepositoryConflictError("ready verification found missing chunks")
        if row_count > expected_count:
            raise RepositoryConflictError("ready verification found missing or extra chunks")
        if seen_indices != set(range(expected_count)):
            raise RepositoryConflictError("ready verification found missing or extra chunks")

    def _verify_visual_manifest(self, document: SourceDocumentRecord) -> None:
        expected_count = document.visual_manifest_page_count
        expected_hash = document.visual_manifest_hash
        if expected_count is None or expected_hash is None:
            raise RepositoryConflictError("source document is missing its manifest binding")
        query = (
            "SELECT * FROM c WHERE c.sourceRunId = @sourceRunId "
            "AND c.recordType = @recordType AND c.documentId = @documentId"
        )
        parameters = [
            {"name": "@sourceRunId", "value": document.source_run_id},
            {"name": "@recordType", "value": "visual_manifest_page"},
            {"name": "@documentId", "value": document.document_id},
        ]
        pages: list[VisualManifestPage] = []
        continuation: str | None = None
        while True:
            rows, continuation = self._query_page(
                self._source_documents,
                query,
                parameters,
                document.source_run_id,
                INTERNAL_PAGE_SIZE,
                continuation,
            )
            for row in rows:
                try:
                    pages.append(_hydrate(row, _visual_manifest_page_from_item, "visual manifest page"))
                except RepositoryDataError:
                    raise RepositoryConflictError(
                        "manifest verification found invalid page content"
                    ) from None
            if continuation is None:
                break
        if len(pages) < expected_count:
            raise RepositoryConflictError("manifest verification found missing manifest pages")
        if len(pages) > expected_count:
            raise RepositoryConflictError("manifest verification found missing or extra manifest pages")
        pages.sort(key=lambda page: page.page_index)
        try:
            actual_hash = visual_manifest_hash(tuple(pages))
        except ValueError:
            raise RepositoryConflictError("manifest verification found invalid page ordering") from None
        if actual_hash != expected_hash:
            raise RepositoryConflictError("manifest verification found a manifest hash mismatch")

    @staticmethod
    def _query_page(
        container: Any,
        query: str,
        parameters: list[dict[str, Any]],
        partition_key: str,
        page_size: int,
        continuation_token: str | None,
    ) -> tuple[list[Mapping[str, Any]], str | None]:
        try:
            iterator = container.query_items(
                query=query,
                parameters=parameters,
                partition_key=partition_key,
                max_item_count=page_size,
            )
            pager = iterator.by_page(continuation_token)
            page = list(next(pager, []))
            return page, pager.continuation_token
        except Exception:
            raise RepositoryError("Cosmos partition query failed") from None

    def _replace_document(
        self,
        document: SourceDocumentRecord,
        etag: str,
        *,
        expected_status: DocumentStatus,
        allowed_status: DocumentStatus,
        allowed_changes: set[str],
    ) -> VersionedRecord[SourceDocumentRecord]:
        if document.status is not allowed_status:
            raise ValueError("document has an invalid target status")
        current = self.get_document(document.source_run_id, document.id)
        if current is None:
            raise RepositoryConflictError("source document no longer exists")
        if current.etag != etag:
            raise RepositoryConflictError("source document changed concurrently")
        if current.record.status is not expected_status:
            raise RepositoryConflictError("source document transition is no longer legal")
        _require_only_changes(current.record, document, allowed_changes)
        try:
            self._source_documents.replace_item(
                item=document.id,
                body=document.to_cosmos_item(),
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            status = getattr(error, "status_code", None)
            if status == 412:
                raise RepositoryConflictError("source document changed concurrently") from None
            if status == 429:
                time.sleep(THROTTLE_BASE_DELAY_SECONDS)
                try:
                    self._source_documents.replace_item(
                        item=document.id,
                        body=document.to_cosmos_item(),
                        etag=etag,
                        match_condition=MatchConditions.IfNotModified,
                    )
                except Exception:
                    raise RepositoryError("Cosmos source-document replace failed after throttle retry") from error
            else:
                raise RepositoryError(f"Cosmos source-document replace failed: {error}") from error
        stored = self.get_document(document.source_run_id, document.id)
        if stored is None:
            raise RepositoryDataError("updated source document could not be read")
        return stored

    @staticmethod
    def _validate_activation(run: IngestionRunRecord, control: SourceControlRecord) -> None:
        if run.source_id != control.source_id or run.run_id != control.current_run_id:
            raise ValueError("run activation identifiers do not match")
        if run.orchestration_instance_id != control.current_orchestration_instance_id:
            raise ValueError("run activation orchestration identifiers do not match")
        if run.activated_at != control.activated_at:
            raise ValueError("run activation timestamps do not match")

    def _reconcile_activation(
        self,
        run: IngestionRunRecord,
        control: SourceControlRecord,
    ) -> ActivatedRun:
        current = self.get_source_control(run.source_id)
        stored_run = self.get_run(run.source_id, run.run_id)
        if (
            current is None
            or stored_run is None
            or current.record.current_run_id != run.run_id
            or not _same_domain(current.record, control)
            or not _same_domain(stored_run.record, run)
        ):
            raise RepositoryConflictError("run activation conflicts with current source state")
        return ActivatedRun(stored_run, current)

    def _read_activated_run(self, source_id: str, run_id: str) -> ActivatedRun:
        current = self.get_source_control(source_id)
        run = self.get_run(source_id, run_id)
        if current is None or run is None or current.record.current_run_id != run_id:
            raise RepositoryDataError("activated run could not be read")
        return ActivatedRun(run, current)

    @staticmethod
    def _read_item(container: Any, item_id: str, partition_key: str) -> Mapping[str, Any] | None:
        try:
            return container.read_item(item=item_id, partition_key=partition_key)
        except Exception as error:
            if getattr(error, "status_code", None) == 404:
                return None
            raise RepositoryError("Cosmos point read failed") from None

    @staticmethod
    def _versioned(
        item: Mapping[str, Any] | None,
        factory: Any,
        record_name: str,
    ) -> VersionedRecord[Any] | None:
        if item is None:
            return None
        etag = item.get("_etag")
        if not isinstance(etag, str) or not etag.strip():
            raise RepositoryDataError(f"{record_name} is missing a valid ETag")
        return VersionedRecord(_hydrate(item, factory, record_name), etag)


def _hydrate(item: Mapping[str, Any], factory: Any, record_name: str) -> Any:
    try:
        return factory(item)
    except (KeyError, TypeError, ValueError):
        raise RepositoryDataError(f"{record_name} does not match schema version 1") from None


def _audio_operation_from_item(item: Mapping[str, Any]) -> AudioOperationRecord:
    if not isinstance(item["audio"], Mapping):
        raise ValueError("audio operation metadata must be an object")
    record = AudioOperationRecord(
        source_id=item["sourceId"], drive_id=item["driveId"], item_id=item["itemId"],
        audio=AudioMetadata(**_snake_keys(item["audio"])), owner_id=item["ownerId"],
        ownership_epoch=item["ownershipEpoch"], state=AudioOperationState(item["state"]),
    )
    if record.to_cosmos_item() != _domain_item(item):
        raise ValueError("audio operation fields do not match validated identity")
    return record


def _audio_transcript_page_from_item(
    item: Mapping[str, Any], expected_operation: AudioOperationRecord,
) -> AudioTranscriptPage:
    if not isinstance(item, Mapping) or any(not isinstance(key, str) for key in item):
        raise ValueError("audio transcript page must be an object")
    if type(item["schemaVersion"]) is not int or type(item["ownershipEpoch"]) is not int:
        raise ValueError("audio transcript page versions must be integers")
    audio = item["audio"]
    if not isinstance(audio, Mapping) or any(not isinstance(key, str) for key in audio):
        raise ValueError("audio transcript metadata must be an object")
    if AudioMetadata(**_snake_keys(audio)) != expected_operation.audio:
        raise ValueError("audio transcript metadata does not match expected operation")
    segments = item["segments"]
    if not isinstance(segments, list) or any(not isinstance(segment, Mapping) for segment in segments):
        raise ValueError("audio transcript segments must be an object array")
    page = AudioTranscriptPage(
        operation=expected_operation, source_run_id=item["sourceRunId"],
        page_index=item["pageIndex"], page_count=item["pageCount"],
        segments=tuple(AudioTranscriptSegment(
            ordinal=segment["ordinal"], text=segment["text"],
            start_ms=segment["startMs"], end_ms=segment["endMs"],
        ) for segment in segments),
    )
    if page.to_cosmos_item() != _domain_item(item):
        raise ValueError("audio transcript fields do not match validated page")
    return page


def _source_control_from_item(item: Mapping[str, Any]) -> SourceControlRecord:
    return SourceControlRecord(
        source_id=item["sourceId"],
        current_run_id=item["currentRunId"],
        current_orchestration_instance_id=item["currentOrchestrationInstanceId"],
        activated_at=item["activatedAt"],
        updated_at=item["updatedAt"],
        last_completed_run_id=item.get("lastCompletedRunId"),
        id=item["id"],
        schema_version=item["schemaVersion"],
    )


def _run_from_item(item: Mapping[str, Any]) -> IngestionRunRecord:
    error = item.get("error")
    profiles_data = item.get("profiles", {})
    enrichment_data = profiles_data.get("enrichment", {})
    enrichment_data.pop("enabledModules", None)
    return IngestionRunRecord(
        source_id=item["sourceId"],
        run_id=item["runId"],
        drive_id=item["driveId"],
        orchestration_instance_id=item["orchestrationInstanceId"],
        status=RunStatus(item["status"]),
        stage=RunStage(item["stage"]),
        started_at=item["startedAt"],
        activated_at=item["activatedAt"],
        updated_at=item["updatedAt"],
        counters=RunCounters(**_snake_keys(item["counters"])),
        profiles=ProfileSnapshot(
            extraction=ExtractionProfile(**_snake_keys(profiles_data.get("extraction", {}))),
            chunking=ChunkingProfile(**_snake_keys(profiles_data.get("chunking", {}))),
            enrichment=EnrichmentProfile(**_snake_keys(enrichment_data)),
            embedding=EmbeddingProfile(**_snake_keys(profiles_data.get("embedding", {}))),
        ) if profiles_data else ProfileSnapshot(),
        ingestion_mode=item.get("ingestionMode", ""),
        completed_at=item.get("completedAt"),
        error=SafeError(**_snake_keys(error)) if error is not None else None,
        id=item["id"],
        schema_version=item["schemaVersion"],
    )


def _document_from_item(item: Mapping[str, Any]) -> SourceDocumentRecord:
    values = _snake_keys(_domain_item(item))
    if "lifecycle_generation" not in values:
        raise RepositoryDataError("source document is missing lifecycleGeneration")
    values["status"] = DocumentStatus(values["status"])
    values["stage"] = DocumentStage(values["stage"])
    values["allowed_group_ids"] = tuple(values["allowed_group_ids"])
    values.setdefault("source_modified_at", None)
    for _removed in ("quality_flags", "profiles", "acl_policy_version", "verified_at"):
        values.pop(_removed, None)
    if values.get("error") is not None:
        values["error"] = SafeError(**_snake_keys(values["error"]))
    if values.get("audio") is not None:
        audio_values = _snake_keys(values["audio"])
        audio_values.setdefault("channel_count", None)  # omitted from storage when absent
        values["audio"] = AudioMetadata(**audio_values)
    return SourceDocumentRecord(**values)


def _visual_manifest_page_from_item(item: Mapping[str, Any]) -> VisualManifestPage:
    values = _snake_keys(_domain_item(item))
    entries: list[VisualManifestEntry] = []
    for entry_data in values["entries"]:
        entry = _snake_keys(entry_data)
        locator = _snake_keys(entry["source_locator"])
        entry["source_locator"] = SourceLocator(
            kind=LocatorKind(locator["kind"]),
            label=locator["label"],
            ordinal_start=locator["ordinal_start"],
            ordinal_end=locator["ordinal_end"],
        )
        derivative = entry.get("derivative_locator")
        if derivative is not None:
            derivative_values = _snake_keys(derivative)
            entry["derivative_locator"] = SourceLocator(
                kind=LocatorKind(derivative_values["kind"]),
                label=derivative_values["label"],
                ordinal_start=derivative_values["ordinal_start"],
                ordinal_end=derivative_values["ordinal_end"],
            )
        entry["relevance"] = VisualRelevance(entry["relevance"])
        entry["disposition"] = VisualDisposition(entry["disposition"])
        entry["provenance"] = tuple(
            ExtractionProvenance(value) for value in entry["provenance"]
        )
        entries.append(VisualManifestEntry(**entry))
    values["entries"] = tuple(entries)
    return VisualManifestPage(**values)


def _chunk_from_item(item: Mapping[str, Any]) -> SearchChunkRecord:
    values = _snake_keys(_domain_item(item))
    if "is_retrievable" not in values or "lifecycle_generation" not in values:
        raise RepositoryDataError(
            "search chunk is missing lifecycle admission fields"
        )
    values["allowed_group_ids"] = tuple(values["allowed_group_ids"])
    values["section_path"] = tuple(values["section_path"])
    values["locator_kind"] = LocatorKind(values["locator_kind"])
    values["modalities"] = tuple(
        ContentModality(modality) for modality in values["modalities"]
    )
    values["provenance"] = tuple(
        ExtractionProvenance(provenance) for provenance in values["provenance"]
    )
    values["visual_coverage"] = VisualCoverageStatus(values["visual_coverage"])
    values["key_phrases"] = tuple(values["key_phrases"])
    values["entities"] = tuple(Entity(**_snake_keys(entity)) for entity in values["entities"])
    values["embedding"] = tuple(values["embedding"])
    for _removed in (
        "source_path", "drive_id", "item_id", "embedding_input_hash",
        "chunking_strategy", "chunking_profile_version", "tokenizer",
        "max_tokens", "overlap_tokens", "enrichment_profile_version",
        "embedding_model", "embedding_deployment", "embedding_dimensions",
        "embedding_profile_version", "quality_flags", "extraction_confidence",
        "processing_warnings", "record_type",
    ):
        values.pop(_removed, None)
    statuses = values["enrichment_status"]
    values["enrichment_status"] = EnrichmentStatuses(
        summary=ModuleStatus(statuses["summary"]),
        key_phrases=ModuleStatus(statuses["keyPhrases"]),
        entities=ModuleStatus(statuses["entities"]),
    )
    if values.get("audio") is not None:
        audio_values = _snake_keys(values["audio"])
        audio_values.setdefault("channel_count", None)  # omitted from storage when absent
        values["audio"] = AudioMetadata(**audio_values)
    return SearchChunkRecord(**values)


def _domain_item(item: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if not key.startswith("_")}


def _same_domain(stored: Any, requested: Any) -> bool:
    return stored.to_cosmos_item() == requested.to_cosmos_item()


def _error_status(error: Exception) -> int | None:
    for attribute in ("status_code", "status"):
        status = getattr(error, attribute, None)
        if isinstance(status, int):
            return status
    return None


def _batch_failure_status(results: Any) -> int | None:
    if not isinstance(results, Sequence):
        return None
    for result in results:
        if isinstance(result, Mapping):
            status = result.get("statusCode", result.get("status_code", result.get("status")))
        else:
            status = getattr(result, "status_code", getattr(result, "status", None))
        if isinstance(status, int) and not 200 <= status < 300:
            return status
    return None


def _require_only_changes(
    current: SourceDocumentRecord,
    requested: SourceDocumentRecord,
    allowed_changes: set[str],
) -> None:
    current_item = current.to_cosmos_item()
    requested_item = requested.to_cosmos_item()
    changed = {
        key
        for key in current_item.keys() | requested_item.keys()
        if current_item.get(key) != requested_item.get(key)
    }
    if not changed <= allowed_changes:
        raise ValueError("document transition changes immutable fields")


def _require_only_record_changes(
    current: Any,
    requested: Any,
    allowed_changes: set[str],
    operation_name: str,
) -> None:
    current_item = current.to_cosmos_item()
    requested_item = requested.to_cosmos_item()
    changed = {
        key
        for key in current_item.keys() | requested_item.keys()
        if current_item.get(key) != requested_item.get(key)
    }
    if not changed <= allowed_changes:
        raise ValueError(f"{operation_name} changes immutable fields")


def _require_chunk_row_identity(
    chunk: Mapping[str, Any],
    document: SourceDocumentRecord,
) -> None:
    if (
        chunk.get("documentKey") != document.document_key
        or chunk.get("documentId") != document.document_id
        or chunk.get("sourceId") != document.source_id
        or chunk.get("runId") != document.run_id
        or chunk.get("sourceRunId") != document.source_run_id
    ):
        raise RepositoryConflictError("ready verification found mismatched chunk identity")


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _validate_page_size(page_size: int) -> None:
    if not isinstance(page_size, int) or isinstance(page_size, bool) or not 1 <= page_size <= INTERNAL_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {INTERNAL_PAGE_SIZE}")


def _snake_keys(item: Mapping[str, Any]) -> dict[str, Any]:
    return {_camel_to_snake(key): value for key, value in item.items()}


def _camel_to_snake(value: str) -> str:
    characters: list[str] = []
    for character in value:
        if character.isupper():
            characters.extend(("_", character.lower()))
        else:
            characters.append(character)
    return "".join(characters)