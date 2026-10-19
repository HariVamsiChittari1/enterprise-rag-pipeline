"""Post-ingestion document lifecycle: ACL resync, retirement, and delta-sync's
delete/supersede paths (Goals 6b and 8).

Kept separate from ingestion.repository.IngestionRepository by design: that module is
contractually schema-v1 full-sync only (see
tests/ingestion/test_repository.py::test_repository_exposes_no_forbidden_api_or_terminology,
which forbids "patch_item", "delta", and related terms from its source). This module owns
every Cosmos Patch API call and the delta-sync cursor.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from azure.core import MatchConditions

from ingestion.models import (
    RETIRED_REASONS,
    SOURCE_DOCUMENT_RECORD_TYPE,
    VISUAL_MANIFEST_PAGE_RECORD_TYPE,
    AclFeedback,
    AclStatus,
    DocumentStage,
    DocumentStatus,
    canonical_acl_feedback,
)

logger = logging.getLogger(__name__)

DELTA_CONTROL_ID = "delta-control"
WEBHOOK_CONTROL_ID = "webhook-subscription"
DELTA_SYNC_TRIGGER_ID = "delta-sync-trigger"
ACL_RESYNC_TRIGGER_ID = "acl-resync-trigger"
LIFECYCLE_RECONCILE_TRIGGER_ID = "lifecycle-reconcile-trigger"
AUDIO_POLL_TRIGGER_ID = "audio-poll-trigger"
MAX_PATCH_BATCH_OPERATIONS = 100
MAX_PATCH_BATCH_ATTEMPTS = 3
PATCH_RETRY_BASE_SECONDS = 0.25
TRANSIENT_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
INTERNAL_PAGE_SIZE = 100


class LifecycleRepositoryError(RuntimeError):
    """Base error with a message safe to expose in logs."""


class LifecycleConflictError(LifecycleRepositoryError):
    """Raised when persisted state conflicts with the requested operation (e.g. already
    retired, or changed concurrently). Callers may treat this as an idempotent no-op."""


@dataclass(frozen=True)
class ReadyDocumentRef:
    """A minimal projection of a ready source-document row, for lifecycle scans."""

    document_id: str
    source_run_id: str
    document_key: str
    item_id: str
    allowed_group_ids: tuple[str, ...]
    acl_hash: str
    etag: str
    acl_status: AclStatus = AclStatus.VERIFIED
    acl_feedback: tuple[AclFeedback, ...] = ()
    source_etag: str | None = None
    lifecycle_generation: int = 0
    status: DocumentStatus = DocumentStatus.READY
    source_id: str | None = None
    drive_id: str | None = None
    processing_profile_digest: str | None = None


@dataclass(frozen=True)
class ReadyDocumentPage:
    items: tuple[ReadyDocumentRef, ...]
    continuation_token: str | None


@dataclass(frozen=True)
class LifecycleDocumentRef:
    document_id: str
    source_run_id: str
    document_key: str
    status: DocumentStatus
    lifecycle_generation: int
    etag: str
    allowed_group_ids: tuple[str, ...]
    acl_hash: str
    expected_chunk_count: int | None = None
    pending_allowed_group_ids: tuple[str, ...] | None = None
    pending_acl_hash: str | None = None
    pending_acl_status: AclStatus | None = None
    pending_acl_feedback: tuple[AclFeedback, ...] | None = None
    pending_retired_reason: str | None = None


@dataclass(frozen=True)
class LifecycleDocumentPage:
    items: tuple[LifecycleDocumentRef, ...]
    continuation_token: str | None


@dataclass(frozen=True)
class DuplicateDocumentPage:
    document_ids: tuple[str, ...]
    continuation_token: str | None


@dataclass(frozen=True)
class AudioTranscribingRef:
    """Minimal projection of a pending or terminal audio-work document."""

    document_id: str
    source_run_id: str
    status: DocumentStatus = DocumentStatus.PROCESSING
    stage: DocumentStage = DocumentStage.TRANSCRIBING
    transcription_work_id: str | None = None
    transcription_attempt_id: str | None = None

    def __post_init__(self) -> None:
        if (self.transcription_work_id is None) != (self.transcription_attempt_id is None):
            raise ValueError("transcription work and attempt IDs must be set together")


@dataclass(frozen=True)
class AudioTranscribingPage:
    items: tuple[AudioTranscribingRef, ...]
    continuation_token: str | None


@dataclass(frozen=True)
class ChunkManifestRef:
    chunk_id: str
    document_key: str
    source_run_id: str
    document_id: str
    source_id: str | None = None


@dataclass(frozen=True)
class ChunkManifestPage:
    items: tuple[ChunkManifestRef, ...]
    continuation_token: str | None


class DocumentLifecycleRepository:
    """Owns the ACL-resync scan, retire/ACL-patch/hard-delete writes, and the delta-sync
    cursor. Uses the same three Cosmos containers as IngestionRepository."""

    def __init__(self, ingestion_runs: Any, source_documents: Any, search_chunks: Any) -> None:
        self._ingestion_runs = ingestion_runs
        self._source_documents = source_documents
        self._search_chunks = search_chunks

    def list_ready_documents_page(
        self,
        *,
        source_id: str,
        page_size: int,
        continuation_token: str | None = None,
    ) -> ReadyDocumentPage:
        _validate_page_size(page_size)
        query = (
            "SELECT c.sourceId, c.driveId, c.documentId, c.sourceRunId, c.documentKey, c.itemId, "
            "c.allowedGroupIds, c.aclHash, c.aclStatus, c.aclFeedback, c.eTag, c.status, "
            "c.lifecycleGeneration, c.processingProfileDigest, c._etag FROM c "
            "WHERE c.sourceId = @sourceId AND c.recordType = @recordType AND ("
            "ARRAY_CONTAINS(@statuses, c.status) OR "
            "(c.status = @retiredStatus AND c.retiredReason = @retiredReason))"
        )
        parameters = [
            {"name": "@sourceId", "value": source_id},
            {
                "name": "@statuses",
                "value": [DocumentStatus.READY.value, DocumentStatus.ACL_REFRESHING.value],
            },
            {"name": "@retiredStatus", "value": DocumentStatus.RETIRED.value},
            {"name": "@retiredReason", "value": "acl_revoked"},
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
        ]
        try:
            iterator = self._source_documents.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            page, token = _read_resumable_query_page(iterator, continuation_token)
        except Exception:
            raise LifecycleRepositoryError("Cosmos ready-document scan failed") from None
        return ReadyDocumentPage(tuple(_ready_ref_from_row(row) for row in page), token)

    def find_ready_document_by_document_id(
        self, source_id: str, document_id: str,
    ) -> ReadyDocumentRef | None:
        query = (
            "SELECT c.sourceId, c.driveId, c.documentId, c.sourceRunId, c.documentKey, c.itemId, "
            "c.allowedGroupIds, c.aclHash, c.aclStatus, c.aclFeedback, c.eTag, c.status, "
            "c.lifecycleGeneration, c.processingProfileDigest, c._etag FROM c "
            "WHERE c.sourceId = @sourceId AND c.recordType = @recordType "
            "AND ARRAY_CONTAINS(@statuses, c.status) AND c.documentId = @documentId"
        )
        parameters = [
            {"name": "@sourceId", "value": source_id},
            {
                "name": "@statuses",
                "value": [DocumentStatus.READY.value, DocumentStatus.ACL_REFRESHING.value],
            },
            {"name": "@documentId", "value": document_id},
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
        ]
        try:
            rows = list(
                self._source_documents.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True,
                    max_item_count=1,
                )
            )
        except Exception:
            raise LifecycleRepositoryError("Cosmos ready-document lookup failed") from None
        if not rows:
            return None
        return _ready_ref_from_row(rows[0])

    def find_acl_revoked_document_by_document_id(
        self,
        source_id: str,
        document_id: str,
    ) -> ReadyDocumentRef | None:
        query = (
            "SELECT c.sourceId, c.driveId, c.documentId, c.sourceRunId, c.documentKey, c.itemId, "
            "c.allowedGroupIds, c.aclHash, c.aclStatus, c.aclFeedback, c.eTag, c.status, "
            "c.lifecycleGeneration, c._etag, c._ts FROM c "
            "WHERE c.sourceId = @sourceId AND c.recordType = @recordType AND c.status = @retiredStatus "
            "AND c.retiredReason = @retiredReason "
            "AND c.documentId = @documentId"
        )
        parameters = [
            {"name": "@sourceId", "value": source_id},
            {"name": "@retiredStatus", "value": DocumentStatus.RETIRED.value},
            {"name": "@retiredReason", "value": "acl_revoked"},
            {"name": "@documentId", "value": document_id},
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
        ]
        try:
            rows = list(self._source_documents.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
                max_item_count=INTERNAL_PAGE_SIZE,
            ))
        except Exception:
            raise LifecycleRepositoryError(
                "Cosmos ACL-revoked document lookup failed"
            ) from None
        if not rows:
            return None
        latest = max(rows, key=_document_version_order)
        return _ready_ref_from_row(latest)

    def is_authoritative_document_version(
        self,
        *,
        source_id: str,
        document_id: str,
        source_run_id: str,
        source_etag: str,
    ) -> bool:
        query = (
            "SELECT c.sourceRunId, c.eTag, c.status, c._ts FROM c "
            "WHERE c.sourceId = @sourceId AND c.recordType = @recordType "
            "AND c.documentId = @documentId"
        )
        try:
            rows = list(self._source_documents.query_items(
                query=query,
                parameters=[
                    {"name": "@sourceId", "value": source_id},
                    {"name": "@documentId", "value": document_id},
                    {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
                ],
                enable_cross_partition_query=True,
                max_item_count=INTERNAL_PAGE_SIZE,
            ))
        except Exception:
            raise LifecycleRepositoryError(
                "Cosmos document-version authority lookup failed"
            ) from None
        if any(
            row.get("status") in {
                DocumentStatus.READY.value,
                DocumentStatus.ACL_REFRESHING.value,
            }
            for row in rows
        ):
            return False
        matching = [row for row in rows if row.get("eTag") == source_etag]
        if not matching:
            return False
        latest = max(matching, key=_document_version_order)
        return latest.get("sourceRunId") == source_run_id

    def list_lifecycle_transitions_page(
        self,
        *,
        source_id: str,
        page_size: int,
        continuation_token: str | None = None,
    ) -> LifecycleDocumentPage:
        _validate_page_size(page_size)
        query = (
            "SELECT c.sourceId, c.driveId, c.documentId, c.sourceRunId, c.documentKey, c.status, "
            "c.lifecycleGeneration, c.allowedGroupIds, c.aclHash, "
            "c.expectedChunkCount, c.pendingAllowedGroupIds, c.pendingAclHash, "
            "c.pendingAclStatus, c.pendingAclFeedback, "
            "c.pendingRetiredReason, c._etag FROM c "
            "WHERE c.sourceId = @sourceId AND c.recordType = @recordType "
            "AND ARRAY_CONTAINS(@statuses, c.status)"
        )
        parameters = [
            {"name": "@sourceId", "value": source_id},
            {
                "name": "@statuses",
                "value": [
                    DocumentStatus.ADMITTING.value,
                    DocumentStatus.ACL_REFRESHING.value,
                    DocumentStatus.RETIRING.value,
                    DocumentStatus.DELETING.value,
                ],
            },
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
        ]
        try:
            iterator = self._source_documents.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            page, token = _read_resumable_query_page(iterator, continuation_token)
        except Exception:
            raise LifecycleRepositoryError(
                "Cosmos lifecycle-transition scan failed"
            ) from None
        return LifecycleDocumentPage(
            tuple(_lifecycle_ref_from_row(row) for row in page),
            token,
        )

    def list_transcribing_documents_page(
        self,
        *,
        source_id: str,
        page_size: int,
        continuation_token: str | None = None,
        settlement_only: bool = False,
    ) -> AudioTranscribingPage:
        """Page audio documents parked at PROCESSING/TRANSCRIBING for the batch poller."""
        _validate_page_size(page_size)
        query = (
            "SELECT c.documentId, c.sourceRunId, c.status, c.stage, "
            "c.transcriptionWorkId, c.transcriptionAttemptId FROM c "
            "WHERE c.sourceId = @sourceId AND c.recordType = @recordType "
            "AND ((c.status = @processing AND c.stage = @transcribing AND "
            "(@settlementOnly = false OR "
            "(IS_DEFINED(c.transcriptionWorkId) AND IS_DEFINED(c.transcriptionAttemptId)))) OR "
            "(IS_DEFINED(c.transcriptionWorkId) AND IS_DEFINED(c.transcriptionAttemptId) AND "
            "((c.status = @ready AND c.stage = @terminal) OR "
            "(c.status = @failed AND c.stage = @terminal))))"
        )
        parameters = [
            {"name": "@sourceId", "value": source_id},
            {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
            {"name": "@processing", "value": DocumentStatus.PROCESSING.value},
            {"name": "@transcribing", "value": DocumentStage.TRANSCRIBING.value},
            {"name": "@ready", "value": DocumentStatus.READY.value},
            {"name": "@failed", "value": DocumentStatus.FAILED.value},
            {"name": "@terminal", "value": DocumentStage.TERMINAL.value},
            {"name": "@settlementOnly", "value": settlement_only},
        ]
        try:
            iterator = self._source_documents.query_items(
                query=query,
                parameters=parameters,
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            page, token = _read_resumable_query_page(iterator, continuation_token)
        except Exception:
            raise LifecycleRepositoryError("Cosmos transcribing scan failed") from None
        return AudioTranscribingPage(
            tuple(
                AudioTranscribingRef(
                    row["documentId"], row["sourceRunId"],
                    DocumentStatus(row["status"]), DocumentStage(row["stage"]),
                    row.get("transcriptionWorkId"), row.get("transcriptionAttemptId"),
                )
                for row in page
            ),
            token,
        )

    def find_transcribing_document_by_document_id(
        self,
        source_id: str,
        document_id: str,
    ) -> Mapping[str, Any] | None:
        query = (
            "SELECT c.sourceId, c.driveId, c.sourceRunId, c.documentId, c.itemId, c.eTag "
            "FROM c WHERE c.sourceId = @sourceId AND c.recordType = @recordType "
            "AND c.documentId = @documentId AND c.status = @status AND c.stage = @stage"
        )
        try:
            rows = list(self._source_documents.query_items(
                query=query,
                parameters=[
                    {"name": "@sourceId", "value": source_id},
                    {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
                    {"name": "@documentId", "value": document_id},
                    {"name": "@status", "value": DocumentStatus.PROCESSING.value},
                    {"name": "@stage", "value": DocumentStage.TRANSCRIBING.value},
                ],
                enable_cross_partition_query=True,
                max_item_count=2,
            ))
        except Exception:
            raise LifecycleRepositoryError("Cosmos pending-audio lookup failed") from None
        if len(rows) > 1:
            raise LifecycleRepositoryError("multiple pending audio versions for source document")
        return rows[0] if rows else None

    def list_duplicate_ready_document_ids_page(
        self,
        *,
        source_id: str,
        page_size: int,
        continuation_token: str | None = None,
    ) -> DuplicateDocumentPage:
        _validate_page_size(page_size)
        query = (
            "SELECT c.documentId FROM c "
            "WHERE c.sourceId = @sourceId AND c.recordType = @recordType AND c.status = 'ready'"
        )
        try:
            iterator = self._source_documents.query_items(
                query=query,
                parameters=[
                    {"name": "@sourceId", "value": source_id},
                    {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE}
                ],
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            page, token = _read_resumable_query_page(iterator, continuation_token)
        except Exception:
            raise LifecycleRepositoryError(
                "Cosmos duplicate-ready scan failed"
            ) from None
        document_ids: list[str] = []
        seen_document_ids: set[str] = set()
        for row in page:
            document_id = row.get("documentId")
            if not isinstance(document_id, str) or not document_id:
                raise LifecycleRepositoryError(
                    "Cosmos duplicate-ready row is malformed"
                )
            if document_id not in seen_document_ids:
                seen_document_ids.add(document_id)
                document_ids.append(document_id)
        return DuplicateDocumentPage(tuple(document_ids), token)

    def list_ready_document_versions(
        self,
        source_id: str,
        document_id: str,
    ) -> tuple[ReadyDocumentRef, ...]:
        """Compatibility wrapper returning every source-scoped physical document version."""
        refs: list[ReadyDocumentRef] = []
        continuation_token: str | None = None
        while True:
            page = self.list_ready_document_versions_page(
                source_id=source_id,
                document_id=document_id,
                page_size=INTERNAL_PAGE_SIZE,
                continuation_token=continuation_token,
            )
            refs.extend(page.items)
            continuation_token = page.continuation_token
            if continuation_token is None:
                return tuple(refs)

    def list_ready_document_versions_page(
        self,
        *,
        source_id: str,
        document_id: str,
        page_size: int,
        continuation_token: str | None = None,
    ) -> ReadyDocumentPage:
        """Page every source-scoped physical version, including non-ready/failed records."""
        _validate_page_size(page_size)
        query = (
            "SELECT c.sourceId, c.driveId, c.documentId, c.sourceRunId, c.documentKey, c.itemId, "
            "c.allowedGroupIds, c.aclHash, c.aclStatus, c.aclFeedback, c.eTag, c.status, "
            "c.lifecycleGeneration, c.processingProfileDigest, c._etag FROM c "
            "WHERE c.sourceId = @sourceId AND c.recordType = @recordType "
            "AND c.documentId = @documentId"
        )
        try:
            iterator = self._source_documents.query_items(
                query=query,
                parameters=[
                    {"name": "@sourceId", "value": source_id},
                    {"name": "@documentId", "value": document_id},
                    {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
                ],
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            rows, token = _read_resumable_query_page(iterator, continuation_token)
        except Exception:
            raise LifecycleRepositoryError(
                "Cosmos ready-version lookup failed"
            ) from None
        return ReadyDocumentPage(tuple(_ready_ref_from_row(row) for row in rows), token)

    def list_chunk_manifest_refs_page(
        self,
        *,
        source_id: str,
        page_size: int,
        continuation_token: str | None = None,
    ) -> ChunkManifestPage:
        _validate_page_size(page_size)
        query = (
            "SELECT c.id, c.sourceId, c.documentKey, c.sourceRunId, c.documentId FROM c "
            "WHERE c.sourceId = @sourceId"
        )
        try:
            iterator = self._search_chunks.query_items(
                query=query,
                parameters=[{"name": "@sourceId", "value": source_id}],
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            page, token = _read_resumable_query_page(iterator, continuation_token)
        except Exception:
            raise LifecycleRepositoryError("Cosmos orphan-chunk scan failed") from None
        refs: list[ChunkManifestRef] = []
        for row in page:
            try:
                refs.append(ChunkManifestRef(
                    chunk_id=row["id"],
                    document_key=row["documentKey"],
                    source_run_id=row["sourceRunId"],
                    document_id=row["documentId"],
                    source_id=row.get("sourceId"),
                ))
            except KeyError as error:
                raise LifecycleRepositoryError(
                    "Cosmos orphan-chunk row is malformed"
                ) from error
        return ChunkManifestPage(tuple(refs), token)

    def delete_orphan_chunk(self, *, chunk_id: str, document_key: str) -> None:
        self._delete_item(self._search_chunks, chunk_id, document_key)

    def retire_document(
        self,
        *,
        source_run_id: str,
        document_id: str,
        document_key: str,
        etag: str,
        reason: str,
    ) -> None:
        if reason not in RETIRED_REASONS:
            raise ValueError("retired_reason must be one of the supported values")
        now = _now_iso()
        current = self._read_document(source_run_id, document_id)
        current_status = current.get("status")
        generation = (
            _require_generation(current)
            if current_status == DocumentStatus.RETIRING.value
            else _require_generation(current) + 1
        )
        begin_operations = [
            {"op": "set", "path": "/status", "value": DocumentStatus.RETIRING.value},
            {"op": "set", "path": "/lifecycleGeneration", "value": generation},
            {"op": "set", "path": "/pendingRetiredReason", "value": reason},
            {"op": "set", "path": "/updatedAt", "value": now},
        ]
        try:
            retiring = self._source_documents.patch_item(
                item=document_id,
                partition_key=source_run_id,
                patch_operations=begin_operations,
                filter_predicate=(
                    "from c where c.status = 'ready' OR "
                    "c.status = 'acl_refreshing' OR c.status = 'retiring'"
                ),
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) in (404, 412):
                raise LifecycleConflictError("document already retired or changed") from None
            raise LifecycleRepositoryError("Cosmos document retirement start failed") from None

        self.set_document_chunks_retrievable(
            document_key=document_key,
            lifecycle_generation=generation,
            is_retrievable=False,
        )
        complete_operations = [
            {"op": "set", "path": "/status", "value": DocumentStatus.RETIRED.value},
            {"op": "set", "path": "/retiredAt", "value": now},
            {"op": "set", "path": "/retiredReason", "value": reason},
            {"op": "set", "path": "/updatedAt", "value": now},
            {"op": "remove", "path": "/pendingRetiredReason"},
        ]
        for pending_acl_field in (
            "pendingAllowedGroupIds",
            "pendingAclHash",
            "pendingAclStatus",
            "pendingAclFeedback",
        ):
            if pending_acl_field in current:
                complete_operations.append(
                    {"op": "remove", "path": f"/{pending_acl_field}"}
                )
        try:
            self._source_documents.patch_item(
                item=document_id,
                partition_key=source_run_id,
                patch_operations=complete_operations,
                filter_predicate="from c where c.status = 'retiring'",
                etag=_require_etag(retiring, "document retirement"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) in (404, 412):
                raise LifecycleConflictError("document already retired or changed") from None
            raise LifecycleRepositoryError(
                "Cosmos document retirement completion failed"
            ) from None

    def list_source_documents_for_deletion_page(
        self, *, source_id: str, page_size: int,
    ) -> LifecycleDocumentPage:
        """Read a bounded page of all source-document states for source retirement."""
        _validate_page_size(page_size)
        try:
            iterator = self._source_documents.query_items(
                query=(
                    "SELECT TOP @pageSize c.documentId, c.sourceRunId, c.documentKey, "
                    "c.status, c.lifecycleGeneration, c.allowedGroupIds, c.aclHash, "
                    "c.pendingAclStatus, c.pendingAclFeedback, c._etag FROM c "
                    "WHERE c.sourceId = @sourceId AND c.recordType = @recordType"
                ),
                parameters=[
                    {"name": "@pageSize", "value": page_size},
                    {"name": "@sourceId", "value": source_id},
                    {"name": "@recordType", "value": SOURCE_DOCUMENT_RECORD_TYPE},
                ],
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            rows = list(iterator)
        except Exception as error:
            raise LifecycleRepositoryError("Cosmos source-retirement scan failed") from error
        return LifecycleDocumentPage(tuple(_lifecycle_ref_from_row(row) for row in rows), None)

    def delete_source_chunks_page(self, *, source_id: str, page_size: int) -> int:
        """Delete remaining source chunks, including orphan chunks."""
        _validate_page_size(page_size)
        try:
            iterator = self._search_chunks.query_items(
                query=(
                    "SELECT TOP @pageSize c.id, c.documentKey FROM c "
                    "WHERE c.sourceId = @sourceId"
                ),
                parameters=[
                    {"name": "@pageSize", "value": page_size},
                    {"name": "@sourceId", "value": source_id},
                ],
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            rows = list(iterator)
        except Exception as error:
            raise LifecycleRepositoryError("Cosmos source chunk retirement scan failed") from error
        for row in rows:
            self._delete_item(self._search_chunks, row["id"], row["documentKey"])
        return len(rows)

    def refresh_document_acl(
        self,
        *,
        source_run_id: str,
        document_id: str,
        document_key: str,
        etag: str,
        allowed_group_ids: tuple[str, ...],
        acl_hash: str,
        acl_status: AclStatus = AclStatus.VERIFIED,
        acl_feedback: tuple[AclFeedback, ...] = (),
    ) -> None:
        if acl_status is not AclStatus.VERIFIED:
            raise ValueError("ACL refresh requires verified status")
        if canonical_acl_feedback(acl_feedback) != acl_feedback:
            raise ValueError("acl_feedback must be sorted and unique")
        now = _now_iso()
        current = self._read_document(source_run_id, document_id)
        generation = (
            _require_generation(current)
            if current.get("status") == DocumentStatus.ACL_REFRESHING.value
            else _require_generation(current) + 1
        )
        begin_patch = [
            {"op": "set", "path": "/status", "value": DocumentStatus.ACL_REFRESHING.value},
            {"op": "set", "path": "/lifecycleGeneration", "value": generation},
            {
                "op": "set",
                "path": "/pendingAllowedGroupIds",
                "value": list(allowed_group_ids),
            },
            {"op": "set", "path": "/pendingAclHash", "value": acl_hash},
            {"op": "set", "path": "/pendingAclStatus", "value": acl_status.value},
            {
                "op": "set",
                "path": "/pendingAclFeedback",
                "value": [value.value for value in acl_feedback],
            },
            {"op": "set", "path": "/updatedAt", "value": now},
        ]
        if (
            current.get("status") == DocumentStatus.RETIRED.value
            and current.get("retiredReason") == "acl_revoked"
        ):
            begin_patch.extend([
                {"op": "remove", "path": "/retiredAt"},
                {"op": "remove", "path": "/retiredReason"},
            ])
        try:
            refreshing = self._source_documents.patch_item(
                item=document_id,
                partition_key=source_run_id,
                patch_operations=begin_patch,
                filter_predicate=(
                    "from c where c.status = 'ready' OR c.status = 'acl_refreshing' OR "
                    "(c.status = 'retired' AND c.retiredReason = 'acl_revoked')"
                ),
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) in (404, 412):
                raise LifecycleConflictError("document already changed or missing") from None
            raise LifecycleRepositoryError("Cosmos document ACL refresh start failed") from None

        refreshing_etag = refreshing.get("_etag") if isinstance(refreshing, Mapping) else None
        if not isinstance(refreshing_etag, str) or not refreshing_etag:
            raise LifecycleRepositoryError("Cosmos document ACL refresh returned no ETag")

        self.set_document_chunks_retrievable(
            document_key=document_key,
            lifecycle_generation=generation,
            is_retrievable=False,
            allowed_group_ids=allowed_group_ids,
        )
        self.set_document_chunks_retrievable(
            document_key=document_key,
            lifecycle_generation=generation,
            is_retrievable=True,
            allowed_group_ids=allowed_group_ids,
        )

        complete_patch = [
            {"op": "set", "path": "/allowedGroupIds", "value": list(allowed_group_ids)},
            {"op": "set", "path": "/aclHash", "value": acl_hash},
            {"op": "set", "path": "/aclStatus", "value": acl_status.value},
            {
                "op": "set",
                "path": "/aclFeedback",
                "value": [value.value for value in acl_feedback],
            },
            {"op": "set", "path": "/aclEvaluatedAt", "value": now},
            {"op": "set", "path": "/status", "value": DocumentStatus.READY.value},
            {"op": "set", "path": "/lifecycleGeneration", "value": generation},
            {"op": "set", "path": "/updatedAt", "value": now},
            {"op": "remove", "path": "/pendingAllowedGroupIds"},
            {"op": "remove", "path": "/pendingAclHash"},
            {"op": "remove", "path": "/pendingAclStatus"},
            {"op": "remove", "path": "/pendingAclFeedback"},
        ]
        try:
            self._source_documents.patch_item(
                item=document_id,
                partition_key=source_run_id,
                patch_operations=complete_patch,
                filter_predicate="from c where c.status = 'acl_refreshing'",
                etag=refreshing_etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) in (404, 412):
                raise LifecycleConflictError("document changed during ACL refresh") from None
            raise LifecycleRepositoryError("Cosmos document ACL refresh completion failed") from None

    def update_document_acl_feedback(
        self,
        *,
        source_run_id: str,
        document_id: str,
        etag: str,
        acl_status: AclStatus,
        acl_feedback: tuple[AclFeedback, ...],
    ) -> None:
        """Persist non-authorizing ACL observations without touching chunks or generation."""
        if acl_status is not AclStatus.VERIFIED:
            raise ValueError("ACL feedback update requires verified status")
        if canonical_acl_feedback(acl_feedback) != acl_feedback:
            raise ValueError("acl_feedback must be sorted and unique")
        now = _now_iso()
        try:
            self._source_documents.patch_item(
                item=document_id,
                partition_key=source_run_id,
                patch_operations=[
                    {"op": "set", "path": "/aclStatus", "value": acl_status.value},
                    {
                        "op": "set",
                        "path": "/aclFeedback",
                        "value": [value.value for value in acl_feedback],
                    },
                    {"op": "set", "path": "/aclEvaluatedAt", "value": now},
                    {"op": "set", "path": "/updatedAt", "value": now},
                ],
                filter_predicate="from c where c.status = 'ready'",
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) in (404, 412):
                raise LifecycleConflictError("document already changed or missing") from None
            raise LifecycleRepositoryError("Cosmos document ACL feedback update failed") from None

    def set_document_chunks_retrievable(
        self,
        *,
        document_key: str,
        lifecycle_generation: int,
        is_retrievable: bool,
        allowed_group_ids: tuple[str, ...] | None = None,
        expected_count: int | None = None,
    ) -> int:
        if (
            isinstance(lifecycle_generation, bool)
            or not isinstance(lifecycle_generation, int)
            or lifecycle_generation < 0
        ):
            raise ValueError("lifecycle_generation must be a non-negative integer")
        if not isinstance(is_retrievable, bool):
            raise ValueError("is_retrievable must be boolean")
        rows = self._list_chunk_rows(document_key)
        if expected_count is not None and len(rows) != expected_count:
            raise LifecycleRepositoryError("Cosmos chunk admission count mismatch")
        batch: list[str] = []
        for row in rows:
            current_generation = row.get("lifecycleGeneration")
            if (
                isinstance(current_generation, bool)
                or not isinstance(current_generation, int)
                or current_generation < 0
            ):
                raise LifecycleRepositoryError(
                    "Cosmos chunk lifecycle generation is malformed"
                )
            if current_generation > lifecycle_generation:
                raise LifecycleConflictError(
                    "newer chunk lifecycle generation already exists"
                )
            batch.append(row["id"])
            if len(batch) == MAX_PATCH_BATCH_OPERATIONS:
                self._patch_chunk_batch(
                    document_key,
                    batch,
                    lifecycle_generation,
                    is_retrievable,
                    allowed_group_ids,
                )
                batch = []
        if batch:
            self._patch_chunk_batch(
                document_key,
                batch,
                lifecycle_generation,
                is_retrievable,
                allowed_group_ids,
            )

        verified = self._list_chunk_rows(document_key)
        if expected_count is not None and len(verified) != expected_count:
            raise LifecycleRepositoryError("Cosmos chunk admission count mismatch")
        expected_groups = list(allowed_group_ids) if allowed_group_ids is not None else None
        for row in verified:
            if (
                row.get("lifecycleGeneration") != lifecycle_generation
                or row.get("isRetrievable") is not is_retrievable
                or (
                    expected_groups is not None
                    and row.get("allowedGroupIds") != expected_groups
                )
            ):
                raise LifecycleRepositoryError(
                    "Cosmos chunk admission verification failed"
                )
        return len(verified)

    def _patch_chunk_batch(
        self,
        document_key: str,
        chunk_ids: list[str],
        lifecycle_generation: int,
        is_retrievable: bool,
        allowed_group_ids: tuple[str, ...] | None,
    ) -> None:
        patch_operations = [
            {
                "op": "set",
                "path": "/lifecycleGeneration",
                "value": lifecycle_generation,
            },
            {"op": "set", "path": "/isRetrievable", "value": is_retrievable},
        ]
        if allowed_group_ids is not None:
            patch_operations.append({
                "op": "set",
                "path": "/allowedGroupIds",
                "value": list(allowed_group_ids),
            })
        predicate = f"from c where c.lifecycleGeneration <= {lifecycle_generation}"
        operations = [
            ("patch", (chunk_id, patch_operations), {"filter_predicate": predicate})
            for chunk_id in chunk_ids
        ]
        for attempt in range(MAX_PATCH_BATCH_ATTEMPTS):
            try:
                results = self._search_chunks.execute_item_batch(
                    batch_operations=operations,
                    partition_key=document_key,
                    no_response=True,
                )
                failure = _batch_failure_status(results)
            except Exception as error:
                failure = _error_status(error)
                if failure not in TRANSIENT_STATUS_CODES:
                    raise LifecycleRepositoryError(
                        "Cosmos chunk lifecycle batch patch failed"
                    ) from None
            if failure is None:
                return
            if failure == 412:
                raise LifecycleConflictError("chunk lifecycle generation changed")
            if failure not in TRANSIENT_STATUS_CODES:
                raise LifecycleRepositoryError(
                    "Cosmos chunk lifecycle batch patch failed"
                )
            if attempt + 1 < MAX_PATCH_BATCH_ATTEMPTS:
                time.sleep(PATCH_RETRY_BASE_SECONDS * (2 ** attempt))
        raise LifecycleRepositoryError(
            "Cosmos chunk lifecycle batch patch failed after retries"
        )

    def delete_document_and_chunks(
        self,
        *,
        source_run_id: str,
        document_id: str,
        document_key: str,
        etag: str,
        allow_unready: bool = False,
    ) -> None:
        try:
            current = self._read_document(source_run_id, document_id)
        except LifecycleConflictError:
            return
        generation = (
            _require_generation(current)
            if current.get("status") == DocumentStatus.DELETING.value
            else _require_generation(current) + 1
        )
        try:
            deleting_document = self._source_documents.patch_item(
                item=document_id,
                partition_key=source_run_id,
                patch_operations=[
                    {"op": "set", "path": "/status", "value": DocumentStatus.DELETING.value},
                    {"op": "set", "path": "/lifecycleGeneration", "value": generation},
                    {"op": "set", "path": "/updatedAt", "value": _now_iso()},
                ],
                filter_predicate=(
                    "from c where IS_DEFINED(c.status)"
                    if allow_unready
                    else "from c where c.status = 'ready' OR c.status = 'deleting' "
                    "OR c.status = 'retired' OR c.status = 'failed'"
                ),
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) == 404:
                return
            if _error_status(error) == 412:
                raise LifecycleConflictError(
                    "document already changed or removed"
                ) from None
            raise LifecycleRepositoryError("Cosmos document delete start failed") from None

        self.set_document_chunks_retrievable(
            document_key=document_key,
            lifecycle_generation=generation,
            is_retrievable=False,
        )
        for chunk_id in self._list_chunk_ids(document_key):
            self._delete_item(self._search_chunks, chunk_id, document_key)
        for manifest_page_id in self._list_visual_manifest_page_ids(
            source_run_id,
            document_id,
        ):
            self._delete_item(
                self._source_documents,
                manifest_page_id,
                source_run_id,
            )
        try:
            self._source_documents.delete_item(
                item=document_id,
                partition_key=source_run_id,
                etag=_require_etag(deleting_document, "document deletion"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) == 404:
                return
            if _error_status(error) == 412:
                raise LifecycleConflictError("document already changed or removed") from None
            raise LifecycleRepositoryError("Cosmos document delete failed") from None

    def _list_chunk_ids(self, document_key: str) -> list[str]:
        return [row["id"] for row in self._list_chunk_rows(document_key)]

    def _list_chunk_rows(self, document_key: str) -> list[dict[str, Any]]:
        query = (
            "SELECT c.id, c.documentKey, c.allowedGroupIds, "
            "c.isRetrievable, c.lifecycleGeneration FROM c "
            "WHERE c.documentKey = @documentKey"
        )
        parameters = [{"name": "@documentKey", "value": document_key}]
        rows: list[dict[str, Any]] = []
        continuation: str | None = None
        while True:
            try:
                iterator = self._search_chunks.query_items(
                    query=query,
                    parameters=parameters,
                    partition_key=document_key,
                    max_item_count=INTERNAL_PAGE_SIZE,
                )
                pager = iterator.by_page(continuation)
                page = list(next(pager, []))
                continuation = pager.continuation_token
            except Exception:
                raise LifecycleRepositoryError("Cosmos chunk id scan failed") from None
            rows.extend(page)
            if continuation is None:
                break
        return rows

    def _list_visual_manifest_page_ids(
        self,
        source_run_id: str,
        document_id: str,
    ) -> list[str]:
        query = (
            "SELECT c.id FROM c WHERE c.sourceRunId = @sourceRunId "
            "AND c.documentId = @documentId AND c.recordType = @recordType"
        )
        parameters = [
            {"name": "@sourceRunId", "value": source_run_id},
            {"name": "@documentId", "value": document_id},
            {"name": "@recordType", "value": VISUAL_MANIFEST_PAGE_RECORD_TYPE},
        ]
        try:
            rows = self._source_documents.query_items(
                query=query,
                parameters=parameters,
                partition_key=source_run_id,
                max_item_count=INTERNAL_PAGE_SIZE,
            )
            return [row["id"] for row in rows]
        except (KeyError, TypeError):
            raise LifecycleRepositoryError(
                "Cosmos visual-manifest row is malformed"
            ) from None
        except Exception:
            raise LifecycleRepositoryError(
                "Cosmos visual-manifest scan failed"
            ) from None

    def _read_document(self, source_run_id: str, document_id: str) -> dict[str, Any]:
        try:
            return self._source_documents.read_item(
                item=document_id,
                partition_key=source_run_id,
            )
        except Exception as error:
            if _error_status(error) == 404:
                raise LifecycleConflictError("document is missing") from None
            raise LifecycleRepositoryError("Cosmos document read failed") from None

    @staticmethod
    def _delete_item(container: Any, item_id: str, partition_key: str) -> None:
        try:
            container.delete_item(item=item_id, partition_key=partition_key)
        except Exception as error:
            if _error_status(error) == 404:
                return
            raise LifecycleRepositoryError("Cosmos delete failed") from None

    def get_delta_cursor(self, source_id: str) -> str | None:
        try:
            item = self._ingestion_runs.read_item(item=DELTA_CONTROL_ID, partition_key=source_id)
        except Exception as error:
            if _error_status(error) == 404:
                return None
            raise LifecycleRepositoryError("Cosmos delta cursor read failed") from None
        cursor = item.get("deltaLink")
        if cursor is not None and not isinstance(cursor, str):
            raise LifecycleRepositoryError("Cosmos delta cursor is malformed")
        return cursor

    def save_delta_cursor(self, source_id: str, delta_link: str) -> None:
        if not delta_link:
            raise ValueError("delta_link is required")
        item = {
            "id": DELTA_CONTROL_ID,
            "sourceId": source_id,
            "deltaLink": delta_link,
            "updatedAt": _now_iso(),
        }
        try:
            self._ingestion_runs.upsert_item(body=item)
        except Exception:
            raise LifecycleRepositoryError("Cosmos delta cursor save failed") from None

    def get_trigger_instance_id(self, source_id: str, control_id: str) -> str | None:
        """Last Durable instance ID dispatched for a periodic/webhook-triggered
        orchestration (control_id is DELTA_SYNC_TRIGGER_ID or ACL_RESYNC_TRIGGER_ID).

        Durable instance-ID reuse is best-effort/racy at the storage layer
        (Azure/azure-functions-durable-python#410), so callers must mint a fresh,
        never-reused ID per tick via save_trigger_instance_id rather than polling a
        fixed ID. This record only remembers which ID to poll for "still running".
        """
        try:
            item = self._ingestion_runs.read_item(item=control_id, partition_key=source_id)
        except Exception as error:
            if _error_status(error) == 404:
                return None
            raise LifecycleRepositoryError("Cosmos trigger-instance read failed") from None
        instance_id = item.get("currentInstanceId")
        if instance_id is not None and not isinstance(instance_id, str):
            raise LifecycleRepositoryError("Cosmos trigger-instance record is malformed")
        return instance_id

    def save_trigger_instance_id(self, source_id: str, control_id: str, instance_id: str) -> None:
        if not instance_id:
            raise ValueError("instance_id is required")
        item = {
            "id": control_id,
            "sourceId": source_id,
            "currentInstanceId": instance_id,
            "updatedAt": _now_iso(),
        }
        try:
            self._ingestion_runs.upsert_item(body=item)
        except Exception:
            raise LifecycleRepositoryError("Cosmos trigger-instance save failed") from None

    def try_claim_trigger_instance_id(
        self,
        source_id: str,
        control_id: str,
        expected_instance_id: str | None,
        instance_id: str,
    ) -> bool:
        """Atomically replace the last-dispatched trigger ID when state is unchanged."""
        if not instance_id:
            raise ValueError("instance_id is required")
        item = {
            "id": control_id,
            "sourceId": source_id,
            "currentInstanceId": instance_id,
            "updatedAt": _now_iso(),
        }
        try:
            current = self._ingestion_runs.read_item(
                item=control_id,
                partition_key=source_id,
            )
        except Exception as error:
            if _error_status(error) != 404:
                raise LifecycleRepositoryError(
                    "Cosmos trigger-instance claim read failed"
                ) from None
            if expected_instance_id is not None:
                return False
            try:
                self._ingestion_runs.create_item(body=item)
            except Exception as create_error:
                if _error_status(create_error) == 409:
                    return False
                raise LifecycleRepositoryError(
                    "Cosmos trigger-instance claim create failed"
                ) from None
            return True

        current_instance_id = current.get("currentInstanceId")
        if current_instance_id is not None and not isinstance(current_instance_id, str):
            raise LifecycleRepositoryError("Cosmos trigger-instance record is malformed")
        if current_instance_id != expected_instance_id:
            return False
        try:
            self._ingestion_runs.replace_item(
                item=control_id,
                body=item,
                etag=_require_etag(current, "trigger-instance claim"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) in {409, 412}:
                return False
            raise LifecycleRepositoryError(
                "Cosmos trigger-instance claim replace failed"
            ) from None
        return True

    def get_webhook_subscription_id(self, source_id: str) -> str | None:
        try:
            item = self._ingestion_runs.read_item(item=WEBHOOK_CONTROL_ID, partition_key=source_id)
        except Exception as error:
            if _error_status(error) == 404:
                return None
            raise LifecycleRepositoryError("Cosmos webhook subscription read failed") from None
        sub_id = item.get("subscriptionId")
        if sub_id is not None and not isinstance(sub_id, str):
            raise LifecycleRepositoryError("Cosmos webhook subscription ID is malformed")
        return sub_id

    def save_webhook_subscription_id(self, source_id: str, subscription_id: str) -> None:
        if not subscription_id:
            raise ValueError("subscription_id is required")
        item = {
            "id": WEBHOOK_CONTROL_ID,
            "sourceId": source_id,
            "subscriptionId": subscription_id,
            "updatedAt": _now_iso(),
        }
        try:
            self._ingestion_runs.upsert_item(body=item)
        except Exception:
            raise LifecycleRepositoryError("Cosmos webhook subscription save failed") from None

    def clear_webhook_subscription_id(self, source_id: str) -> None:
        try:
            current = self._ingestion_runs.read_item(
                item=WEBHOOK_CONTROL_ID,
                partition_key=source_id,
            )
        except Exception as error:
            if _error_status(error) == 404:
                return
            raise LifecycleRepositoryError("Cosmos webhook subscription read failed") from None
        try:
            self._ingestion_runs.delete_item(
                item=WEBHOOK_CONTROL_ID,
                partition_key=source_id,
                etag=current.get("_etag"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            if _error_status(error) == 404:
                return
            if _error_status(error) == 412:
                raise LifecycleConflictError("webhook subscription changed during deletion") from None
            raise LifecycleRepositoryError("Cosmos webhook subscription delete failed") from None


def _ready_ref_from_row(row: Mapping[str, Any]) -> ReadyDocumentRef:
    try:
        return ReadyDocumentRef(
            document_id=row["documentId"],
            source_run_id=row["sourceRunId"],
            document_key=row["documentKey"],
            item_id=row["itemId"],
            allowed_group_ids=tuple(row["allowedGroupIds"]),
            acl_hash=row["aclHash"],
            etag=row["_etag"],
            acl_status=AclStatus(row.get("aclStatus", AclStatus.PENDING.value)),
            acl_feedback=_acl_feedback(row.get("aclFeedback", [])),
            source_etag=row.get("eTag") if isinstance(row.get("eTag"), str) else None,
            lifecycle_generation=_require_generation(row),
            status=DocumentStatus(row.get("status", DocumentStatus.READY.value)),
            source_id=row.get("sourceId") if isinstance(row.get("sourceId"), str) else None,
            drive_id=row.get("driveId") if isinstance(row.get("driveId"), str) else None,
            processing_profile_digest=(
                row.get("processingProfileDigest")
                if isinstance(row.get("processingProfileDigest"), str)
                else None
            ),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise LifecycleRepositoryError("ready-document row is missing an expected field") from error


def _document_version_order(row: Mapping[str, Any]) -> tuple[int, str]:
    timestamp = row.get("_ts")
    source_run_id = row.get("sourceRunId")
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, int)
        or not isinstance(source_run_id, str)
        or not source_run_id
    ):
        raise LifecycleRepositoryError("document-version row is malformed")
    return timestamp, source_run_id


def _lifecycle_ref_from_row(row: Mapping[str, Any]) -> LifecycleDocumentRef:
    try:
        groups = row["allowedGroupIds"]
        pending_groups = row.get("pendingAllowedGroupIds")
        if not isinstance(groups, list) or not all(
            isinstance(value, str) for value in groups
        ):
            raise ValueError
        if pending_groups is not None and (
            not isinstance(pending_groups, list)
            or not all(isinstance(value, str) for value in pending_groups)
        ):
            raise ValueError
        expected_count = row.get("expectedChunkCount")
        if expected_count is not None and (
            isinstance(expected_count, bool)
            or not isinstance(expected_count, int)
            or expected_count < 0
        ):
            raise ValueError
        return LifecycleDocumentRef(
            document_id=row["documentId"],
            source_run_id=row["sourceRunId"],
            document_key=row["documentKey"],
            status=DocumentStatus(row["status"]),
            lifecycle_generation=_require_generation(row),
            etag=row["_etag"],
            allowed_group_ids=tuple(groups),
            acl_hash=row["aclHash"],
            expected_chunk_count=expected_count,
            pending_allowed_group_ids=(
                tuple(pending_groups) if pending_groups is not None else None
            ),
            pending_acl_hash=(
                row.get("pendingAclHash")
                if isinstance(row.get("pendingAclHash"), str)
                else None
            ),
            pending_acl_status=(
                AclStatus(row["pendingAclStatus"])
                if row.get("pendingAclStatus") is not None else None
            ),
            pending_acl_feedback=(
                _acl_feedback(row["pendingAclFeedback"])
                if row.get("pendingAclFeedback") is not None else None
            ),
            pending_retired_reason=(
                row.get("pendingRetiredReason")
                if isinstance(row.get("pendingRetiredReason"), str)
                else None
            ),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise LifecycleRepositoryError(
            "lifecycle-transition row is malformed"
        ) from error


def _acl_feedback(value: Any) -> tuple[AclFeedback, ...]:
    if not isinstance(value, list):
        raise ValueError("persisted ACL feedback must be a list")
    feedback = tuple(AclFeedback(item) for item in value)
    if canonical_acl_feedback(feedback) != feedback:
        raise ValueError("persisted ACL feedback must be sorted and unique")
    return feedback


def _error_status(error: Exception) -> int | None:
    for attribute in ("status_code", "status"):
        status = getattr(error, attribute, None)
        if isinstance(status, int):
            return status
    return None


def _require_generation(item: Mapping[str, Any]) -> int:
    generation = item.get("lifecycleGeneration")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
    ):
        raise LifecycleRepositoryError(
            "lifecycle generation is missing or malformed"
        )
    return generation


def _require_etag(item: Mapping[str, Any], operation: str) -> str:
    etag = item.get("_etag")
    if not isinstance(etag, str) or not etag:
        raise LifecycleRepositoryError(f"Cosmos {operation} returned no ETag")
    return etag


def _batch_failure_status(results: Any) -> int | None:
    if not isinstance(results, list):
        return None
    for result in results:
        if isinstance(result, Mapping):
            status = result.get("statusCode", result.get("status_code"))
        else:
            status = getattr(result, "status_code", None)
        if isinstance(status, int) and not 200 <= status < 300:
            return status
    return None


def _validate_page_size(page_size: int) -> None:
    if (
        not isinstance(page_size, int)
        or isinstance(page_size, bool)
        or not 1 <= page_size <= INTERNAL_PAGE_SIZE
    ):
        raise ValueError(f"page_size must be between 1 and {INTERNAL_PAGE_SIZE}")


def _read_resumable_query_page(
    iterator: Any,
    continuation_token: str | None,
) -> tuple[list[dict[str, Any]], str | None]:
    pager = iterator.by_page(continuation_token)
    items: list[dict[str, Any]] = []
    token = None
    while True:
        sdk_page = list(next(pager, []))
        if not sdk_page:
            break
        items.extend(sdk_page)
        token = pager.continuation_token
        if token is not None:
            break
    return items, token


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _require_utc_scalar(name: str, value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} is required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from error
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{name} must be a UTC timestamp")
