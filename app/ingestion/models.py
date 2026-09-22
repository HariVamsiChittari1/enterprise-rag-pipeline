"""Versioned contracts for full-sync ingestion and Cosmos persistence."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, fields, is_dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping


SCHEMA_VERSION = 1
SUPPORTED_AUDIO_MIME_TYPES = frozenset({"audio/wav", "audio/mpeg", "audio/flac"})
SOURCE_CONTROL_ID = "source-control"
SOURCE_DOCUMENT_RECORD_TYPE = "source_document"
VISUAL_MANIFEST_PAGE_RECORD_TYPE = "visual_manifest_page"
MAX_DOCUMENT_ITEM_BYTES = 128 * 1024
MAX_CHUNK_ITEM_BYTES = 256 * 1024
ENRICHMENT_MODULES = ("summary", "key_phrases", "entities")
SUPPORTED_SOURCE_MIME_TYPES = frozenset(
    {
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "text/markdown",
        "text/plain",
    }
)


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"
    TERMINATED = "terminated"


class RunStage(str, Enum):
    ACTIVATING = "activating"
    CLEANING = "cleaning"
    DISCOVERING = "discovering"
    PROCESSING = "processing"
    FINALIZING = "finalizing"
    TERMINAL = "terminal"


class DocumentStatus(str, Enum):
    DISCOVERED = "discovered"
    PROCESSING = "processing"
    ADMITTING = "admitting"
    ACL_REFRESHING = "acl_refreshing"
    RETIRING = "retiring"
    DELETING = "deleting"
    READY = "ready"
    FAILED = "failed"
    RETIRED = "retired"


# Reasons a previously-ready document can leave retrieval: permission loss detected by
# ACL resync, a newer version superseding it, or the source item being deleted.
RETIRED_REASONS = frozenset({"acl_revoked", "superseded", "deleted"})


class DocumentStage(str, Enum):
    DISCOVERED = "discovered"
    ACL = "acl"
    DOWNLOAD = "download"
    EXTRACTION = "extraction"
    TRANSCRIBING = "transcribing"
    CHUNKING = "chunking"
    ENRICHMENT = "enrichment"
    EMBEDDING = "embedding"
    PERSISTING = "persisting"
    VERIFYING = "verifying"
    TERMINAL = "terminal"


class ModuleStatus(str, Enum):
    NOT_REQUESTED = "not_requested"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ActivityStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class LocatorKind(str, Enum):
    PAGE = "page"
    SECTION = "section"
    SLIDE = "slide"
    WORKSHEET = "worksheet"
    TIME = "time"


class ContentModality(str, Enum):
    TEXT = "text"
    VISUAL_DESCRIPTION = "visual_description"
    AUDIO_TRANSCRIPT = "audio_transcript"


class ExtractionProvenance(str, Enum):
    DIRECT = "direct"
    RENDERED = "rendered"
    TRANSCRIBED = "transcribed"


class VisualCoverageStatus(str, Enum):
    NOT_REQUIRED = "not_required"
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"


class VisualRelevance(str, Enum):
    REQUIRED = "required"
    DECORATIVE = "decorative"
    UNRESOLVED = "unresolved"


class VisualDisposition(str, Enum):
    DESCRIBED = "described"
    EXCLUDED = "excluded"
    UNSUPPORTED = "unsupported"
    AMBIGUOUS = "ambiguous"
    FAILED = "failed"


@dataclass(frozen=True)
class SourceLocator:
    kind: LocatorKind
    label: str
    ordinal_start: int
    ordinal_end: int
    start_ms: int | None = field(default=None, metadata={"omit_none": True})
    end_ms: int | None = field(default=None, metadata={"omit_none": True})

    def __post_init__(self) -> None:
        if not isinstance(self.kind, LocatorKind):
            raise ValueError("locator kind is invalid")
        _require_text("locator label", self.label, 200)
        if self.ordinal_start < 1 or self.ordinal_end < self.ordinal_start:
            raise ValueError("locator ordinal range is invalid")
        if self.kind is LocatorKind.TIME:
            _validate_time_range(self.start_ms, self.end_ms, 1_800_000)
        elif self.start_ms is not None or self.end_ms is not None:
            raise ValueError("only time locators accept timestamps")


@dataclass(frozen=True)
class VisualCoverage:
    status: VisualCoverageStatus
    inventory_count: int
    required_count: int
    described_count: int
    excluded_count: int
    unsupported_count: int
    uncovered_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.status, VisualCoverageStatus):
            raise ValueError("visual coverage status is invalid")
        counts = (
            self.inventory_count,
            self.required_count,
            self.described_count,
            self.excluded_count,
            self.unsupported_count,
            self.uncovered_count,
        )
        if any(isinstance(count, bool) or not isinstance(count, int) or count < 0 for count in counts):
            raise ValueError("visual coverage counts must be non-negative integers")
        if self.inventory_count != self.required_count + self.excluded_count + self.unsupported_count:
            raise ValueError("visual inventory accounting is inconsistent")
        if self.required_count != self.described_count + self.uncovered_count:
            raise ValueError("required visual accounting is inconsistent")
        if self.status is VisualCoverageStatus.NOT_REQUIRED and self.required_count != 0:
            raise ValueError("not-required visual coverage cannot contain required visuals")
        if self.status is VisualCoverageStatus.COMPLETE and self.uncovered_count != 0:
            raise ValueError("complete visual coverage cannot contain uncovered visuals")
        if self.status is VisualCoverageStatus.INCOMPLETE and self.uncovered_count == 0:
            raise ValueError("incomplete visual coverage requires uncovered visuals")


@dataclass(frozen=True)
class CanonicalSegment:
    ordinal: int
    text: str
    locator: SourceLocator
    modalities: tuple[ContentModality, ...]
    provenance: tuple[ExtractionProvenance, ...]

    def __post_init__(self) -> None:
        if self.ordinal < 0:
            raise ValueError("segment ordinal cannot be negative")
        _require_text("segment text", self.text, 8_000_000)
        _validate_enum_tuple(
            "segment modalities",
            self.modalities,
            ContentModality,
            required=ContentModality.TEXT,
        )
        _validate_enum_tuple(
            "segment provenance",
            self.provenance,
            ExtractionProvenance,
        )


@dataclass(frozen=True)
class CanonicalExtractionResult:
    segments: tuple[CanonicalSegment, ...]
    visual_coverage: VisualCoverage
    visual_manifest_entries: tuple[VisualManifestEntry, ...] = ()

    def __post_init__(self) -> None:
        if tuple(segment.ordinal for segment in self.segments) != tuple(range(len(self.segments))):
            raise ValueError("canonical segment ordinals must be contiguous")
        has_visual_descriptions = any(
            ContentModality.VISUAL_DESCRIPTION in segment.modalities
            for segment in self.segments
        )
        if has_visual_descriptions != (self.visual_coverage.described_count > 0):
            raise ValueError("canonical visual descriptions do not match coverage")
        if self.visual_manifest_entries:
            ordinals = tuple(entry.ordinal for entry in self.visual_manifest_entries)
            if ordinals != tuple(range(len(self.visual_manifest_entries))):
                raise ValueError("canonical visual manifest ordinals must be contiguous")
            if len(self.visual_manifest_entries) != self.visual_coverage.inventory_count:
                raise ValueError("canonical visual manifest does not match inventory coverage")
            required_count = sum(
                entry.relevance is VisualRelevance.REQUIRED
                for entry in self.visual_manifest_entries
            )
            described_count = sum(
                entry.disposition is VisualDisposition.DESCRIBED
                for entry in self.visual_manifest_entries
            )
            excluded_count = sum(
                entry.disposition is VisualDisposition.EXCLUDED
                for entry in self.visual_manifest_entries
            )
            unsupported_count = sum(
                entry.disposition is VisualDisposition.UNSUPPORTED
                for entry in self.visual_manifest_entries
            )
            if (
                required_count != self.visual_coverage.required_count
                or described_count != self.visual_coverage.described_count
                or excluded_count != self.visual_coverage.excluded_count
                or unsupported_count != self.visual_coverage.unsupported_count
            ):
                raise ValueError("canonical visual manifest dispositions do not match coverage")


@dataclass(frozen=True)
class VisualManifestEntry:
    ordinal: int
    visual_id: str
    object_type: str
    source_locator: SourceLocator
    relevance: VisualRelevance
    disposition: VisualDisposition
    description: str | None = None
    reason: str | None = None
    provenance: tuple[ExtractionProvenance, ...] = ()
    derivative_locator: SourceLocator | None = None
    source_reference: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.ordinal, bool) or not isinstance(self.ordinal, int) or self.ordinal < 0:
            raise ValueError("visual manifest ordinal must be a non-negative integer")
        _require_text("visual id", self.visual_id, 500)
        _require_text("visual object type", self.object_type, 100)
        if not isinstance(self.relevance, VisualRelevance):
            raise ValueError("visual relevance is invalid")
        if not isinstance(self.disposition, VisualDisposition):
            raise ValueError("visual disposition is invalid")
        if len(self.provenance) != len(set(self.provenance)) or any(
            not isinstance(value, ExtractionProvenance) for value in self.provenance
        ):
            raise ValueError("visual provenance contains an invalid or duplicate value")
        if self.disposition is VisualDisposition.DESCRIBED:
            if self.description is None or not self.provenance:
                raise ValueError("described visuals require a description and provenance")
            _require_text("visual description", self.description, 8_000)
            if self.reason is not None:
                raise ValueError("described visuals cannot contain a disposition reason")
        else:
            if self.description is not None:
                raise ValueError("non-described visuals cannot contain a description")
            if self.reason is None:
                raise ValueError("non-described visuals require a disposition reason")
            _require_text("visual disposition reason", self.reason, 500)
        if self.relevance is VisualRelevance.REQUIRED and self.disposition is VisualDisposition.EXCLUDED:
            raise ValueError("required visuals cannot be excluded")
        if self.derivative_locator is not None and ExtractionProvenance.RENDERED not in self.provenance:
            raise ValueError("derivative locators require rendered provenance")
        if self.source_reference is not None:
            _require_text("visual source reference", self.source_reference, 1_000)


@dataclass(frozen=True)
class SafeError:
    code: str
    stage: str
    retryable: bool

    def __post_init__(self) -> None:
        _require_text("error code", self.code, 100)
        _require_text("error stage", self.stage, 100)


@dataclass(frozen=True)
class ScaleLimits:
    max_eligible_pdfs: int = 10_000
    max_drive_items: int = 50_000
    max_folders: int = 10_000
    max_folder_depth: int = 32
    max_graph_pages: int = 20_000
    max_source_bytes: int = 100 * 1024 * 1024
    max_rendered_pdf_bytes: int = 200 * 1024 * 1024
    max_document_units: int = 300
    max_office_characters: int = 8_000_000
    max_visual_descriptions: int = 60
    max_chunks_per_pdf: int = 2_000

    def __post_init__(self) -> None:
        for field in fields(self):
            if getattr(self, field.name) <= 0:
                raise ValueError(f"{field.name} must be positive")

    @property
    def max_pdf_bytes(self) -> int:
        return self.max_source_bytes

    @property
    def max_pdf_pages(self) -> int:
        return self.max_document_units


@dataclass(frozen=True)
class ExtractionProfile:
    version: str = "document-intelligence-layout-v1"
    model: str = "prebuilt-layout"
    output_format: str = "markdown"

    def __post_init__(self) -> None:
        _validate_profile_texts(self)


@dataclass(frozen=True)
class ChunkingProfile:
    version: str = "layout-page-token-v1"
    strategy: str = "layout-page-token"
    tokenizer: str = "cl100k_base"
    max_tokens: int = 800
    overlap_tokens: int = 100

    def __post_init__(self) -> None:
        _validate_profile_texts(self)
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        if self.overlap_tokens < 0 or self.overlap_tokens >= self.max_tokens:
            raise ValueError("overlap_tokens must be between zero and max_tokens")


@dataclass(frozen=True)
class EnrichmentProfile:
    version: str = "language-all-v1"
    summary_enabled: bool = False
    key_phrases_enabled: bool = True
    entities_enabled: bool = True
    max_key_phrases: int = 25
    max_entities: int = 50
    enabled_modules: tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        _require_text("enrichment profile version", self.version, 100)
        if self.max_key_phrases <= 0 or self.max_entities <= 0:
            raise ValueError("enrichment output limits must be positive")
        enabled: list[str] = []
        if self.summary_enabled:
            enabled.append("summary")
        if self.key_phrases_enabled:
            enabled.append("key_phrases")
        if self.entities_enabled:
            enabled.append("entities")
        object.__setattr__(self, "enabled_modules", tuple(enabled))


@dataclass(frozen=True)
class EmbeddingProfile:
    version: str = "text-embedding-3-large-3072-v1"
    model: str = "text-embedding-3-large"
    deployment: str = "text-embedding-3-large"
    dimensions: int = 3_072
    distance_function: str = "cosine"

    def __post_init__(self) -> None:
        _validate_profile_texts(self)
        if self.dimensions != 3_072:
            raise ValueError("schema version 1 requires 3072 embedding dimensions")
        if self.distance_function != "cosine":
            raise ValueError("only cosine distance is supported by schema version 1")


@dataclass(frozen=True)
class ProfileSnapshot:
    extraction: ExtractionProfile = field(default_factory=ExtractionProfile)
    chunking: ChunkingProfile = field(default_factory=ChunkingProfile)
    enrichment: EnrichmentProfile = field(default_factory=EnrichmentProfile)
    embedding: EmbeddingProfile = field(default_factory=EmbeddingProfile)


@dataclass(frozen=True)
class Settings:
    source_id: str
    drive_id: str
    code_version: str
    limits: ScaleLimits = field(default_factory=ScaleLimits)
    profiles: ProfileSnapshot = field(default_factory=ProfileSnapshot)
    activity_timeout_seconds: float = 300.0
    document_wave_size: int = 4
    activity_attempts: int = 5

    def __post_init__(self) -> None:
        _require_text("source_id", self.source_id, 200)
        _require_text("drive_id", self.drive_id, 500)
        _require_text("code_version", self.code_version, 100)
        if self.activity_timeout_seconds <= 0:
            raise ValueError("activity_timeout_seconds must be positive")



@dataclass(frozen=True)
class RunCounters:
    discovered: int = 0
    processing: int = 0
    ready: int = 0
    failed: int = 0
    chunks_written: int = 0
    retries: int = 0
    items_scanned: int = 0

    def __post_init__(self) -> None:
        for field in fields(self):
            if getattr(self, field.name) < 0:
                raise ValueError(f"{field.name} cannot be negative")


@dataclass(frozen=True)
class SourceControlRecord:
    source_id: str
    current_run_id: str
    current_orchestration_instance_id: str
    activated_at: str
    updated_at: str
    last_completed_run_id: str | None = None
    id: str = SOURCE_CONTROL_ID
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.id != SOURCE_CONTROL_ID:
            raise ValueError("invalid source-control discriminator")
        _require_schema_version(self.schema_version)
        _require_text("source_id", self.source_id, 200)
        _require_text("current_run_id", self.current_run_id, 100)
        _require_text("current_orchestration_instance_id", self.current_orchestration_instance_id, 100)
        _require_utc("activated_at", self.activated_at)
        _require_utc("updated_at", self.updated_at)

    def to_cosmos_item(self) -> dict[str, Any]:
        return _serialize_dataclass(self)


@dataclass(frozen=True)
class IngestionRunRecord:
    source_id: str
    run_id: str
    drive_id: str
    orchestration_instance_id: str
    status: RunStatus
    stage: RunStage
    started_at: str
    activated_at: str
    updated_at: str
    counters: RunCounters
    profiles: ProfileSnapshot
    ingestion_mode: str
    completed_at: str | None = None
    error: SafeError | None = None
    id: str = ""
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        expected_id = run_record_id(self.run_id)
        if self.id != expected_id:
            raise ValueError(f"run id must be {expected_id}")
        _require_schema_version(self.schema_version)
        for name, value, maximum in (
            ("source_id", self.source_id, 200),
            ("run_id", self.run_id, 100),
            ("drive_id", self.drive_id, 500),
            ("orchestration_instance_id", self.orchestration_instance_id, 100),
            ("ingestion_mode", self.ingestion_mode, 100),
        ):
            _require_text(name, value, maximum)
        for name, value in (
            ("started_at", self.started_at),
            ("activated_at", self.activated_at),
            ("updated_at", self.updated_at),
        ):
            _require_utc(name, value)
        if self.completed_at is not None:
            _require_utc("completed_at", self.completed_at)

    def to_cosmos_item(self) -> dict[str, Any]:
        return _serialize_dataclass(self)


@dataclass(frozen=True)
class AudioMetadata:
    """Validated media and transcription identity shared by audio manifests and chunks."""

    duration_ms: int
    channel_count: int | None = field(metadata={"omit_none": True})
    locale: str
    mode: str
    api_version: str
    profile_version: str
    source_version: str
    source_content_hash: str

    def __post_init__(self) -> None:
        if type(self.duration_ms) is not int or not 0 < self.duration_ms <= 1_800_000:
            raise ValueError("audio duration is out of range")
        if self.channel_count is not None and (
            type(self.channel_count) is not int or self.channel_count not in (1, 2)
        ):
            raise ValueError("audio channel count is invalid")
        if not isinstance(self.locale, str) or re.fullmatch(r"en-[A-Z]{2}", self.locale) is None:
            raise ValueError("audio requires an English locale")
        if not (
            (self.mode in ("fast", "enhanced") and self.api_version == "2025-10-15")
            or (self.mode == "batch" and self.api_version == "2024-11-15")
        ):
            raise ValueError("audio transcription profile is unsupported")
        _require_text("audio profile version", self.profile_version, 200)
        _require_text("audio source version", self.source_version, 500)
        _require_text("audio source content hash", self.source_content_hash, 64)
        _require_sha256("audio source content hash", self.source_content_hash)


class AudioOperationState(str, Enum):
    VALIDATED = "validated"
    SUBMITTING = "submitting"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class AudioOperationRecord:
    """Run-independent ownership metadata; no transcript or source content."""

    source_id: str
    drive_id: str
    item_id: str
    audio: AudioMetadata
    owner_id: str
    ownership_epoch: int = 1
    state: AudioOperationState = AudioOperationState.VALIDATED

    def __post_init__(self) -> None:
        create_audio_operation_id(self.source_id, self.drive_id, self.item_id, self.audio)
        _require_text("audio owner id", self.owner_id, 200)
        if type(self.ownership_epoch) is not int or self.ownership_epoch < 1:
            raise ValueError("audio ownership epoch must be positive")
        if not isinstance(self.state, AudioOperationState):
            raise ValueError("audio operation state is invalid")

    @property
    def id(self) -> str:
        return create_audio_operation_id(self.source_id, self.drive_id, self.item_id, self.audio)

    @property
    def source_run_id(self) -> str:
        return create_audio_control_partition_id(self.source_id)

    def to_cosmos_item(self) -> dict[str, Any]:
        """Serialize an internal schema-1 operation in its reserved partition."""
        return _serialize_dataclass(self) | {
            "id": self.id, "sourceRunId": self.source_run_id,
            "recordType": "audio_operation", "schemaVersion": 1,
        }

    def to_permit_item(self) -> dict[str, Any]:
        """Bind the one-source admission permit to this exact owner and state."""
        return {
            "id": "audio-permit", "sourceRunId": self.source_run_id,
            "sourceId": self.source_id, "recordType": "audio_permit", "schemaVersion": 1,
            "operationId": self.id, "ownerId": self.owner_id,
            "ownershipEpoch": self.ownership_epoch, "state": self.state.value,
        }


@dataclass(frozen=True)
class AudioTranscriptSegment:
    """One ordered phrase with provider timings, without inferred precision."""

    ordinal: int
    text: str
    start_ms: int
    end_ms: int

    def __post_init__(self) -> None:
        if type(self.ordinal) is not int or self.ordinal < 0:
            raise ValueError("audio segment ordinal must be a nonnegative integer")
        _require_text("audio segment text", self.text, MAX_DOCUMENT_ITEM_BYTES)
        _validate_time_range(self.start_ms, self.end_ms, 1_800_000)


@dataclass(frozen=True)
class AudioTranscriptPage:
    """Bounded internal page; validity does not imply a committed checkpoint."""

    operation: AudioOperationRecord
    source_run_id: str
    page_index: int
    page_count: int
    segments: tuple[AudioTranscriptSegment, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.operation, AudioOperationRecord):
            raise ValueError("audio page requires validated operation metadata")
        _require_text("audio page partition", self.source_run_id, 301)
        if (
            type(self.page_index) is not int or type(self.page_count) is not int
            or not 0 <= self.page_index < self.page_count
        ):
            raise ValueError("audio page position is invalid")
        if not isinstance(self.segments, tuple) or not self.segments:
            raise ValueError("audio page requires an immutable nonempty segment tuple")
        if any(not isinstance(segment, AudioTranscriptSegment) for segment in self.segments):
            raise ValueError("audio page contains an invalid segment")
        first_ordinal = self.segments[0].ordinal
        if self.page_index == 0 and first_ordinal != 0:
            raise ValueError("first audio page must start at ordinal zero")
        for offset, segment in enumerate(self.segments):
            if segment.ordinal != first_ordinal + offset:
                raise ValueError("audio page segment ordinals must be contiguous")
            _validate_time_range(segment.start_ms, segment.end_ms, self.operation.audio.duration_ms)
        if serialized_size_bytes(self.to_cosmos_item()) > MAX_DOCUMENT_ITEM_BYTES:
            raise ValueError("audio transcript page exceeds 128 KiB")

    @property
    def id(self) -> str:
        binding = json.dumps(
            [self.operation.id, self.operation.owner_id, self.operation.ownership_epoch, self.page_index],
            ensure_ascii=False, separators=(",", ":"),
        )
        return f"audio-transcript:{content_sha256(binding)}"

    @property
    def page_hash(self) -> str:
        return content_sha256(json.dumps(self._payload(), ensure_ascii=False, separators=(",", ":")))

    def _payload(self) -> dict[str, Any]:
        return {
            "id": self.id, "sourceRunId": self.source_run_id,
            "recordType": "audio_transcript_page", "schemaVersion": SCHEMA_VERSION,
            "operationId": self.operation.id, "ownerId": self.operation.owner_id,
            "ownershipEpoch": self.operation.ownership_epoch,
            "audio": _serialize_dataclass(self.operation.audio),
            "pageIndex": self.page_index, "pageCount": self.page_count,
            "segments": _serialize_dataclass(self.segments),
        }

    def to_cosmos_item(self) -> dict[str, Any]:
        """Serialize the full binding and content with its deterministic digest."""
        return self._payload() | {"pageHash": self.page_hash}


@dataclass(frozen=True)
class AudioTranscriptDescriptor:
    """Verified checkpoint metadata, not a persisted commit or authorization grant."""

    operation: AudioOperationRecord
    pages_source_run_id: str
    page_count: int
    segment_count: int
    total_bytes: int
    transcript_hash: str
    allowed_group_ids: tuple[str, ...]
    acl_hash: str
    acl_policy_version: str
    acl_evaluated_at: str
    source_verified_at: str

    def __post_init__(self) -> None:
        if not isinstance(self.operation, AudioOperationRecord):
            raise ValueError("audio descriptor requires validated operation metadata")
        _require_text("audio page partition", self.pages_source_run_id, 301)
        for name in ("page_count", "segment_count", "total_bytes"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"audio descriptor {name} must be positive")
        _require_text("transcript hash", self.transcript_hash, 64)
        _require_sha256("transcript hash", self.transcript_hash)
        if self.acl_hash != verified_acl_hash(self.allowed_group_ids, self.acl_policy_version):
            raise ValueError("audio descriptor ACL hash does not match provenance")
        for name in ("acl_evaluated_at", "source_verified_at"):
            value = getattr(self, name)
            _require_text(name, value, 100)
            _require_utc(name, value)
        if serialized_size_bytes(self.to_dict()) > MAX_DOCUMENT_ITEM_BYTES:
            raise ValueError("audio transcript descriptor exceeds 128 KiB")

    def to_dict(self) -> dict[str, Any]:
        """Serialize checkpoint metadata without text, mutable state, or Cosmos ETags."""
        operation = self.operation
        return {
            "schemaVersion": SCHEMA_VERSION, "recordType": "audio_transcript_descriptor",
            "sourceId": operation.source_id, "driveId": operation.drive_id,
            "itemId": operation.item_id, "operationId": operation.id,
            "audio": _serialize_dataclass(operation.audio),
            "ownerId": operation.owner_id, "ownershipEpoch": operation.ownership_epoch,
            "pagesSourceRunId": self.pages_source_run_id,
            "pageCount": self.page_count, "segmentCount": self.segment_count,
            "totalBytes": self.total_bytes, "transcriptHash": self.transcript_hash,
            "allowedGroupIds": list(self.allowed_group_ids), "aclHash": self.acl_hash,
            "aclPolicyVersion": self.acl_policy_version, "aclEvaluatedAt": self.acl_evaluated_at,
            "sourceVerifiedAt": self.source_verified_at,
        }


@dataclass(frozen=True)
class SourceDocumentRecord:
    source_id: str
    run_id: str
    drive_id: str
    item_id: str
    parent_item_id: str
    source_name: str
    source_path: str
    source_url: str
    e_tag: str
    mime_type: str
    size_bytes: int
    discovery_ordinal: int
    allowed_group_ids: tuple[str, ...]
    acl_hash: str
    acl_evaluated_at: str
    status: DocumentStatus
    stage: DocumentStage
    attempt_count: int
    discovered_at: str
    updated_at: str
    page_count: int | None = None
    expected_chunk_count: int | None = None
    written_chunk_count: int | None = None
    visual_manifest_page_count: int | None = None
    visual_manifest_hash: str | None = None
    content_hash: str | None = None
    extraction_mode: str | None = None
    processing_started_at: str | None = None
    ready_at: str | None = None
    failed_at: str | None = None
    error: SafeError | None = None
    retired_at: str | None = None
    retired_reason: str | None = None
    retried_at: str | None = None
    source_modified_at: str | None = None
    lifecycle_generation: int = 0
    ingestion_mode: str = "full-sync"
    id: str = ""
    document_id: str = ""
    source_run_id: str = ""
    document_key: str = ""
    record_type: str = SOURCE_DOCUMENT_RECORD_TYPE
    schema_version: int = SCHEMA_VERSION
    audio: AudioMetadata | None = field(default=None, metadata={"omit_none": True})
    source_verified_at: str | None = field(default=None, metadata={"omit_none": True})
    # Transient batch-transcription tracking, set together while stage == TRANSCRIBING.
    transcription_job_url: str | None = field(default=None, metadata={"omit_none": True})
    staging_blob_name: str | None = field(default=None, metadata={"omit_none": True})

    def __post_init__(self) -> None:
        expected_document_id = create_document_id(self.source_id, self.drive_id, self.item_id)
        expected_source_run_id = create_source_run_id(self.source_id, self.run_id)
        expected_document_key = create_document_key(self.source_id, self.run_id, expected_document_id)
        if self.id != expected_document_id or self.document_id != expected_document_id:
            raise ValueError("document identifiers do not match their deterministic value")
        if self.source_run_id != expected_source_run_id or self.document_key != expected_document_key:
            raise ValueError("document run keys do not match their deterministic value")
        if self.record_type != SOURCE_DOCUMENT_RECORD_TYPE:
            raise ValueError("invalid source-document record type")
        is_audio = self.mime_type in SUPPORTED_AUDIO_MIME_TYPES
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ValueError("unsupported source-document schema version")
        if is_audio:
            if type(self.size_bytes) is not int or not 0 < self.size_bytes <= 100 * 1024 * 1024:
                raise ValueError("audio size is out of range")
            if self.audio is not None:
                if not isinstance(self.audio, AudioMetadata):
                    raise ValueError("audio metadata is invalid")
                if self.audio.source_version != self.e_tag or self.audio.source_content_hash != self.content_hash:
                    raise ValueError("audio metadata does not match source version")
            if self.status in (DocumentStatus.ADMITTING, DocumentStatus.READY) and (
                self.audio is None or self.source_verified_at is None
            ):
                raise ValueError("ready audio requires metadata and source verification")
            if self.source_verified_at is not None:
                _require_utc("source_verified_at", self.source_verified_at)
            if (self.transcription_job_url is None) != (self.staging_blob_name is None):
                raise ValueError("transcription job url and staging blob name must be set together")
            for _name, _value in (
                ("transcription_job_url", self.transcription_job_url),
                ("staging_blob_name", self.staging_blob_name),
            ):
                if _value is not None and (not isinstance(_value, str) or not _value.strip()):
                    raise ValueError(f"{_name} must be a non-empty string when set")
        elif (
            self.audio is not None
            or self.source_verified_at is not None
            or self.transcription_job_url is not None
            or self.staging_blob_name is not None
        ):
            raise ValueError("document schema does not accept audio fields")
        _validate_document_fields(self)
        _validate_sorted_unique("allowed_group_ids", self.allowed_group_ids, require_nonempty=True)
        _require_sha256("acl_hash", self.acl_hash)
        if self.content_hash is not None:
            _require_sha256("content_hash", self.content_hash)
        has_manifest_count = self.visual_manifest_page_count is not None
        has_manifest_hash = self.visual_manifest_hash is not None
        if has_manifest_count != has_manifest_hash:
            raise ValueError("manifest page count and hash must be set together")
        if self.visual_manifest_page_count is not None:
            if self.visual_manifest_page_count < 0:
                raise ValueError("visual_manifest_page_count cannot be negative")
            _require_sha256("visual_manifest_hash", self.visual_manifest_hash or "")
        _validate_optional_utc_fields(self)
        _validate_retirement(self)
        if serialized_size_bytes(self.to_cosmos_item()) > MAX_DOCUMENT_ITEM_BYTES:
            raise ValueError("document item exceeds 128 KiB")

    def to_cosmos_item(self) -> dict[str, Any]:
        return _serialize_dataclass(self)


@dataclass(frozen=True)
class VisualManifestPage:
    source_id: str
    run_id: str
    source_run_id: str
    document_id: str
    document_key: str
    source_version: str
    page_index: int
    page_count: int
    entries: tuple[VisualManifestEntry, ...]
    page_hash: str
    id: str
    record_type: str = VISUAL_MANIFEST_PAGE_RECORD_TYPE
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        expected_source_run_id = create_source_run_id(self.source_id, self.run_id)
        expected_document_key = create_document_key(self.source_id, self.run_id, self.document_id)
        expected_id = create_visual_manifest_page_id(self.document_id, self.page_index)
        if self.source_run_id != expected_source_run_id or self.document_key != expected_document_key:
            raise ValueError("visual manifest run keys do not match their deterministic value")
        if self.id != expected_id:
            raise ValueError("visual manifest page id does not match its deterministic value")
        if self.record_type != VISUAL_MANIFEST_PAGE_RECORD_TYPE:
            raise ValueError("invalid visual-manifest record type")
        _require_schema_version(self.schema_version)
        _require_text("manifest source version", self.source_version, 500)
        if self.page_index < 0 or self.page_count < 1 or self.page_index >= self.page_count:
            raise ValueError("visual manifest page position is invalid")
        if not self.entries:
            raise ValueError("visual manifest pages cannot be empty")
        ordinals = tuple(entry.ordinal for entry in self.entries)
        if ordinals != tuple(sorted(set(ordinals))):
            raise ValueError("visual manifest entry ordinals must be sorted and unique")
        if self.page_hash != _visual_manifest_page_hash(self.entries):
            raise ValueError("visual manifest page hash does not match its entries")
        if serialized_size_bytes(self.to_cosmos_item()) > MAX_DOCUMENT_ITEM_BYTES:
            raise ValueError("visual manifest page exceeds 128 KiB")

    def to_cosmos_item(self) -> dict[str, Any]:
        return _serialize_dataclass(self)


@dataclass(frozen=True)
class EnrichmentStatuses:
    summary: ModuleStatus
    key_phrases: ModuleStatus
    entities: ModuleStatus


@dataclass(frozen=True)
class Entity:
    text: str
    category: str
    subcategory: str | None = None
    confidence: float | None = None

    def __post_init__(self) -> None:
        _require_text("entity text", self.text, 500)
        _require_text("entity category", self.category, 100)
        if self.subcategory is not None:
            _require_text("entity subcategory", self.subcategory, 100)
        if self.confidence is not None and not 0 <= self.confidence <= 1:
            raise ValueError("entity confidence must be between zero and one")


@dataclass(frozen=True)
class SearchChunkRecord:
    source_id: str
    run_id: str
    document_id: str
    document_key: str
    allowed_group_ids: tuple[str, ...]
    source_name: str
    source_url: str
    page_start: int | None
    page_end: int | None
    section_path: tuple[str, ...]
    locator_kind: LocatorKind
    locator_label: str
    locator_ordinal_start: int
    locator_ordinal_end: int
    modalities: tuple[ContentModality, ...]
    provenance: tuple[ExtractionProvenance, ...]
    visual_coverage: VisualCoverageStatus
    chunk_index: int
    created_at: str
    content: str
    content_hash: str
    embedding_text: str
    searchable_text: str
    token_count: int
    enrichment_status: EnrichmentStatuses
    summary: str | None
    key_phrases: tuple[str, ...]
    entities: tuple[Entity, ...]
    language_code: str
    embedding: tuple[float, ...]
    embedded_at: str
    source_modified_at: str | None = None
    is_retrievable: bool = False
    lifecycle_generation: int = 0
    id: str = ""
    source_run_id: str = ""
    schema_version: int = SCHEMA_VERSION
    audio: AudioMetadata | None = field(default=None, metadata={"omit_none": True})
    start_ms: int | None = field(default=None, metadata={"omit_none": True})
    end_ms: int | None = field(default=None, metadata={"omit_none": True})

    def __post_init__(self) -> None:
        expected_id = create_chunk_id(self.chunk_index)
        expected_source_run_id = create_source_run_id(self.source_id, self.run_id)
        expected_document_key = create_document_key(self.source_id, self.run_id, self.document_id)
        if self.id != expected_id or self.source_run_id != expected_source_run_id:
            raise ValueError("chunk identifiers do not match their deterministic value")
        if self.document_key != expected_document_key:
            raise ValueError("chunk document key does not match its deterministic value")
        is_audio = self.locator_kind is LocatorKind.TIME
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ValueError("unsupported chunk schema version")
        if is_audio:
            if not isinstance(self.audio, AudioMetadata):
                raise ValueError("audio chunk requires metadata")
            _validate_time_range(self.start_ms, self.end_ms, self.audio.duration_ms)
            if (
                self.page_start is not None or self.page_end is not None
                or self.modalities != (ContentModality.TEXT, ContentModality.AUDIO_TRANSCRIPT)
                or self.provenance != (ExtractionProvenance.TRANSCRIBED,)
                or self.visual_coverage is not VisualCoverageStatus.NOT_REQUIRED
            ):
                raise ValueError("audio chunk cannot contain document-only fields")
            if type(self.token_count) is not int or not 0 < self.token_count <= 800:
                raise ValueError("audio chunk token count is out of range")
            if type(self.chunk_index) is not int or not 0 <= self.chunk_index < 2000:
                raise ValueError("audio chunk index is out of range")
        elif (
            self.audio is not None or self.start_ms is not None or self.end_ms is not None
            or ContentModality.AUDIO_TRANSCRIPT in self.modalities
            or ExtractionProvenance.TRANSCRIBED in self.provenance
        ):
            raise ValueError("document chunk does not accept audio fields")
        _validate_chunk_fields(self)
        if serialized_size_bytes(self.to_cosmos_item()) > MAX_CHUNK_ITEM_BYTES:
            raise ValueError("chunk item exceeds 256 KiB")

    def to_cosmos_item(self) -> dict[str, Any]:
        return _serialize_dataclass(self)


@dataclass(frozen=True)
class ActivityOutcome:
    document_id: str
    status: ActivityStatus
    chunks_written: int
    retry_count: int
    error: SafeError | None = None

    def __post_init__(self) -> None:
        _require_text("document_id", self.document_id, 64)
        if self.chunks_written < 0 or self.retry_count < 0:
            raise ValueError("activity counters cannot be negative")
        if self.status is ActivityStatus.FAILED and self.error is None:
            raise ValueError("failed activities require a safe error")
        if self.status is not ActivityStatus.FAILED and self.error is not None:
            raise ValueError("only failed activities can contain an error")

    def to_dict(self) -> dict[str, Any]:
        return _serialize_dataclass(self)


def create_run_id(timestamp: datetime, entropy: str) -> str:
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("run timestamp must be timezone-aware")
    _require_text("run entropy", entropy, 500)
    timestamp_part = timestamp.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    entropy_part = hashlib.sha256(entropy.encode("utf-8")).hexdigest()[:16]
    return f"{timestamp_part}-{entropy_part}"


def run_record_id(run_id: str) -> str:
    _require_text("run_id", run_id, 100)
    return f"run:{run_id}"


def create_source_run_id(source_id: str, run_id: str) -> str:
    _require_text("source_id", source_id, 200)
    _require_text("run_id", run_id, 100)
    return f"{source_id}:{run_id}"


def create_document_id(source_id: str, drive_id: str, item_id: str) -> str:
    for name, value, maximum in (
        ("source_id", source_id, 200),
        ("drive_id", drive_id, 500),
        ("item_id", item_id, 500),
    ):
        _require_text(name, value, maximum)
    identity = json.dumps([source_id, drive_id, item_id], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def create_document_key(source_id: str, run_id: str, document_id: str) -> str:
    _require_sha256("document_id", document_id)
    return f"{create_source_run_id(source_id, run_id)}:{document_id}"


def create_audio_control_partition_id(source_id: str) -> str:
    """Return a source-scoped control partition outside the source:run namespace."""
    _require_text("source_id", source_id, 200)
    return f"audio-control-{content_sha256(source_id)}"


def create_audio_operation_id(
    source_id: str,
    drive_id: str,
    item_id: str,
    audio: AudioMetadata,
) -> str:
    """Identify a source/profile transcription operation independently of ingestion runs."""
    document_id = create_document_id(source_id, drive_id, item_id)
    if not isinstance(audio, AudioMetadata):
        raise ValueError("audio operation requires validated audio metadata")
    identity = json.dumps(
        [document_id, audio.source_version, audio.source_content_hash,
         audio.mode, audio.locale, audio.api_version, audio.profile_version],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"audio-operation:{content_sha256(identity)}"


def create_chunk_id(chunk_index: int) -> str:
    if chunk_index < 0 or chunk_index > 999_999:
        raise ValueError("chunk_index must be between 0 and 999999")
    return f"chunk:{chunk_index:06d}"


def create_visual_manifest_page_id(document_id: str, page_index: int) -> str:
    _require_sha256("document_id", document_id)
    if page_index < 0 or page_index > 999_999:
        raise ValueError("page_index must be between 0 and 999999")
    return f"visual-manifest:{document_id}:{page_index:06d}"


def create_visual_manifest_pages(
    document: SourceDocumentRecord,
    entries: tuple[VisualManifestEntry, ...],
) -> tuple[VisualManifestPage, ...]:
    if tuple(entry.ordinal for entry in entries) != tuple(range(len(entries))):
        raise ValueError("visual manifest entry ordinals must be contiguous from zero")
    if not entries:
        return ()

    grouped_entries: list[tuple[VisualManifestEntry, ...]] = []
    current_entries: tuple[VisualManifestEntry, ...] = ()
    for entry in entries:
        candidate = current_entries + (entry,)
        try:
            _create_visual_manifest_page(
                document,
                candidate,
                page_index=len(grouped_entries),
                page_count=999_999,
            )
            current_entries = candidate
            continue
        except ValueError as error:
            if str(error) != "visual manifest page exceeds 128 KiB":
                raise
        if not current_entries:
            raise ValueError("visual manifest entry exceeds the page size limit")
        grouped_entries.append(current_entries)
        current_entries = (entry,)
        _create_visual_manifest_page(
            document,
            current_entries,
            page_index=len(grouped_entries),
            page_count=999_999,
        )
    grouped_entries.append(current_entries)

    page_count = len(grouped_entries)
    return tuple(
        _create_visual_manifest_page(
            document,
            page_entries,
            page_index=page_index,
            page_count=page_count,
        )
        for page_index, page_entries in enumerate(grouped_entries)
    )


ACL_POLICY_VERSION = "verified-entra-security-groups-v1"


def verified_acl_hash(group_ids: tuple[str, ...], policy_version: str) -> str:
    """Hash canonical ACL provenance without asserting current authorization."""
    if policy_version != ACL_POLICY_VERSION:
        raise ValueError("unsupported ACL policy version")
    if not isinstance(group_ids, tuple):
        raise ValueError("ACL groups must be an immutable tuple")
    if any(not isinstance(group_id, str) for group_id in group_ids):
        raise ValueError("ACL groups must contain strings")
    _validate_sorted_unique("ACL groups", group_ids, require_nonempty=True)
    if any("\x1f" in group_id for group_id in group_ids):
        raise ValueError("ACL groups cannot contain the hash delimiter")
    return content_sha256("\x1f".join((*group_ids, policy_version)))


def audio_transcript_hash(
    pages: tuple[AudioTranscriptPage, ...],
    *,
    max_pages: int,
    max_segments: int,
    max_total_bytes: int,
) -> str:
    """Validate declared completeness within caller budgets, not commitment or authorization."""
    for name, limit in (
        ("max_pages", max_pages), ("max_segments", max_segments),
        ("max_total_bytes", max_total_bytes),
    ):
        if type(limit) is not int or limit < 1:
            raise ValueError(f"{name} must be a positive integer")
    if not isinstance(pages, tuple) or not pages:
        raise ValueError("audio transcript requires an immutable nonempty page tuple")
    if len(pages) > max_pages:
        raise ValueError("audio transcript exceeds page limit")
    expected_binding = None
    segment_count = 0
    total_bytes = 0
    page_bindings: list[dict[str, str]] = []
    for page_index, page in enumerate(pages):
        if not isinstance(page, AudioTranscriptPage):
            raise ValueError("audio transcript contains an invalid page")
        if page.page_index != page_index or page.page_count != len(pages):
            raise ValueError("audio transcript page positions or counts are inconsistent")
        operation = page.operation
        binding = (
            operation.source_id, operation.drive_id, operation.item_id, operation.id,
            operation.audio, operation.owner_id, operation.ownership_epoch, page.source_run_id,
        )
        if expected_binding is None:
            expected_binding = binding
        elif binding != expected_binding:
            raise ValueError("audio transcript pages have inconsistent identity or ownership")
        if segment_count + len(page.segments) > max_segments:
            raise ValueError("audio transcript exceeds segment limit")
        for segment in page.segments:
            if segment.ordinal != segment_count:
                raise ValueError("audio transcript segment ordinals must be contiguous from zero")
            segment_count += 1
        item = page.to_cosmos_item()
        total_bytes += serialized_size_bytes(item)
        if total_bytes > max_total_bytes:
            raise ValueError("audio transcript exceeds total byte limit")
        page_bindings.append({"id": item["id"], "pageHash": item["pageHash"]})
    return content_sha256(json.dumps(page_bindings, ensure_ascii=False, separators=(",", ":")))


def create_audio_transcript_descriptor(
    expected_operation: AudioOperationRecord,
    source: SourceDocumentRecord,
    pages: tuple[AudioTranscriptPage, ...],
    *,
    acl_policy_version: str,
    max_pages: int,
    max_segments: int,
    max_total_bytes: int,
) -> AudioTranscriptDescriptor:
    """Bind complete pages and recorded provenance to an independently expected operation."""
    if not isinstance(expected_operation, AudioOperationRecord) or not isinstance(source, SourceDocumentRecord):
        raise ValueError("audio descriptor requires validated operation and source records")
    if (
        (source.source_id, source.drive_id, source.item_id)
        != (expected_operation.source_id, expected_operation.drive_id, expected_operation.item_id)
        or source.audio != expected_operation.audio
        or source.e_tag != expected_operation.audio.source_version
        or source.content_hash != expected_operation.audio.source_content_hash
    ):
        raise ValueError("audio descriptor source does not match expected operation")
    digest = audio_transcript_hash(
        pages, max_pages=max_pages, max_segments=max_segments, max_total_bytes=max_total_bytes,
    )
    descriptor = AudioTranscriptDescriptor(
        operation=expected_operation, pages_source_run_id=source.source_run_id,
        page_count=len(pages), segment_count=sum(len(page.segments) for page in pages),
        total_bytes=sum(serialized_size_bytes(page.to_cosmos_item()) for page in pages),
        transcript_hash=digest, allowed_group_ids=source.allowed_group_ids,
        acl_hash=source.acl_hash, acl_policy_version=acl_policy_version,
        acl_evaluated_at=source.acl_evaluated_at, source_verified_at=source.source_verified_at,
    )
    verify_audio_transcript_descriptor(
        descriptor, expected_operation, pages,
        max_pages=max_pages, max_segments=max_segments, max_total_bytes=max_total_bytes,
    )
    return descriptor


def verify_audio_transcript_descriptor(
    descriptor: AudioTranscriptDescriptor,
    expected_operation: AudioOperationRecord,
    pages: tuple[AudioTranscriptPage, ...],
    *,
    max_pages: int,
    max_segments: int,
    max_total_bytes: int,
) -> None:
    """Check integrity against independent ownership, without proving commitment or freshness."""
    if not isinstance(descriptor, AudioTranscriptDescriptor) or not isinstance(expected_operation, AudioOperationRecord):
        raise ValueError("audio checkpoint requires validated descriptor and expected operation")
    digest = audio_transcript_hash(
        pages, max_pages=max_pages, max_segments=max_segments, max_total_bytes=max_total_bytes,
    )
    for operation in (descriptor.operation, pages[0].operation):
        if replace(operation, state=AudioOperationState.VALIDATED) != replace(
            expected_operation, state=AudioOperationState.VALIDATED,
        ):
            raise ValueError("audio checkpoint does not match expected operation")
    if (
        descriptor.pages_source_run_id != pages[0].source_run_id
        or descriptor.page_count != len(pages)
        or descriptor.segment_count != sum(len(page.segments) for page in pages)
        or descriptor.total_bytes != sum(serialized_size_bytes(page.to_cosmos_item()) for page in pages)
        or descriptor.transcript_hash != digest
    ):
        raise ValueError("audio checkpoint does not match its complete page set")


def visual_manifest_hash(pages: tuple[VisualManifestPage, ...]) -> str:
    if pages:
        identity = (pages[0].source_run_id, pages[0].document_id, pages[0].document_key)
        if tuple(page.page_index for page in pages) != tuple(range(len(pages))):
            raise ValueError("visual manifest page indices must be contiguous from zero")
        if any(page.page_count != len(pages) for page in pages):
            raise ValueError("visual manifest page count is inconsistent")
        if any(
            (page.source_run_id, page.document_id, page.document_key) != identity
            for page in pages
        ):
            raise ValueError("visual manifest pages must belong to one document and run")
    binding = json.dumps(
        [{"id": page.id, "pageHash": page.page_hash} for page in pages],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return content_sha256(binding)


def _create_visual_manifest_page(
    document: SourceDocumentRecord,
    entries: tuple[VisualManifestEntry, ...],
    *,
    page_index: int,
    page_count: int,
) -> VisualManifestPage:
    return VisualManifestPage(
        source_id=document.source_id,
        run_id=document.run_id,
        source_run_id=document.source_run_id,
        document_id=document.document_id,
        document_key=document.document_key,
        source_version=document.e_tag,
        page_index=page_index,
        page_count=page_count,
        entries=entries,
        page_hash=_visual_manifest_page_hash(entries),
        id=create_visual_manifest_page_id(document.document_id, page_index),
    )


def _visual_manifest_page_hash(entries: tuple[VisualManifestEntry, ...]) -> str:
    payload = json.dumps(
        _serialize_dataclass(entries),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return content_sha256(payload)


def canonical_group_ids(group_ids: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    canonical = tuple(sorted({_require_text("group id", group_id, 100) for group_id in group_ids}))
    if not canonical:
        raise ValueError("ACL must contain at least one verified group")
    return canonical


def content_sha256(content: str) -> str:
    if not isinstance(content, str):
        raise TypeError("content must be a string")
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def content_sha256_bytes(content: bytes) -> str:
    if not isinstance(content, bytes):
        raise TypeError("content must be bytes")
    return hashlib.sha256(content).hexdigest()


def safe_error_from_exception(error: BaseException, stage: str) -> SafeError:
    if isinstance(error, TimeoutError):
        return SafeError("dependency_timeout", stage, True)
    if isinstance(error, PermissionError):
        return SafeError("authorization_failed", stage, False)
    if isinstance(error, ValueError):
        return SafeError("invalid_data", stage, False)
    return SafeError("internal_error", stage, False)


def serialized_size_bytes(item: Mapping[str, Any]) -> int:
    return len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _serialize_dataclass(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {
            _snake_to_camel(field.name): _serialize_dataclass(getattr(value, field.name))
            for field in fields(value)
            if not (field.metadata.get("omit_none") and getattr(value, field.name) is None)
        }
    if isinstance(value, Mapping):
        return {str(key): _serialize_dataclass(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_serialize_dataclass(item) for item in value]
    return value


def _snake_to_camel(name: str) -> str:
    first, *rest = name.split("_")
    return first + "".join(part.capitalize() for part in rest)


def _require_text(name: str, value: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    if len(value) > maximum:
        raise ValueError(f"{name} exceeds {maximum} characters")
    return value.strip()


def _require_sha256(name: str, value: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _require_utc(name: str, value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError(f"{name} must be a UTC timestamp")


def _require_schema_version(value: int) -> None:
    if value != SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION}")


def _validate_time_range(start_ms: int | None, end_ms: int | None, duration_ms: int) -> None:
    if (
        type(start_ms) is not int or type(end_ms) is not int
        or not 0 <= start_ms < end_ms <= duration_ms
    ):
        raise ValueError("audio time range is invalid")


def _validate_profile_texts(profile: Any) -> None:
    for field in fields(profile):
        value = getattr(profile, field.name)
        if isinstance(value, str):
            _require_text(field.name, value, 200)


def _validate_sorted_unique(name: str, values: tuple[str, ...], require_nonempty: bool = False) -> None:
    if require_nonempty and not values:
        raise ValueError(f"{name} cannot be empty")
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")
    for value in values:
        _require_text(name, value, 500)


def _validate_enum_tuple(
    name: str,
    values: tuple[Any, ...],
    enum_type: type[Enum],
    *,
    required: Enum | None = None,
) -> None:
    if not values:
        raise ValueError(f"{name} cannot be empty")
    if any(not isinstance(value, enum_type) for value in values):
        raise ValueError(f"{name} contains an invalid value")
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")
    if required is not None and required not in values:
        raise ValueError(f"{name} must include {required.value}")


def _validate_document_fields(record: SourceDocumentRecord) -> None:
    for name, value, maximum in (
        ("source_id", record.source_id, 200),
        ("run_id", record.run_id, 100),
        ("drive_id", record.drive_id, 500),
        ("item_id", record.item_id, 500),
        ("source_name", record.source_name, 500),
        ("mime_type", record.mime_type, 200),
    ):
        _require_text(name, value, maximum)
    if record.mime_type not in SUPPORTED_SOURCE_MIME_TYPES | SUPPORTED_AUDIO_MIME_TYPES:
        raise ValueError("document MIME type is not supported")
    if record.size_bytes < 0 or record.discovery_ordinal < 0 or record.attempt_count < 0:
        raise ValueError("document counters cannot be negative")
    if isinstance(record.lifecycle_generation, bool) or not isinstance(
        record.lifecycle_generation, int
    ) or record.lifecycle_generation < 0:
        raise ValueError("lifecycle_generation must be a non-negative integer")
    for count_name in ("page_count", "expected_chunk_count", "written_chunk_count"):
        count = getattr(record, count_name)
        if count is not None and count < 0:
            raise ValueError(f"{count_name} cannot be negative")


def _validate_optional_utc_fields(record: SourceDocumentRecord) -> None:
    _require_utc("acl_evaluated_at", record.acl_evaluated_at)
    _require_utc("discovered_at", record.discovered_at)
    _require_utc("updated_at", record.updated_at)
    for field_name in ("processing_started_at", "ready_at", "failed_at", "retired_at", "source_modified_at"):
        value = getattr(record, field_name)
        if value is not None:
            _require_utc(field_name, value)


def _validate_retirement(record: SourceDocumentRecord) -> None:
    is_retired = record.status is DocumentStatus.RETIRED
    has_retirement_fields = record.retired_at is not None and record.retired_reason is not None
    if is_retired != has_retirement_fields:
        raise ValueError("retired documents require retired_at and retired_reason; others must omit them")
    if is_retired and record.ready_at is None:
        raise ValueError("only a previously ready document can be retired")
    if is_retired and (record.failed_at is not None or record.error is not None):
        raise ValueError("retired documents cannot carry a failed_at or error")
    if record.retired_reason is not None and record.retired_reason not in RETIRED_REASONS:
        raise ValueError("retired_reason must be one of the supported values")


def _validate_chunk_fields(record: SearchChunkRecord) -> None:
    _validate_sorted_unique("allowed_group_ids", record.allowed_group_ids, require_nonempty=True)
    _validate_sorted_unique("key_phrases", record.key_phrases)
    if not isinstance(record.is_retrievable, bool):
        raise ValueError("is_retrievable must be boolean")
    if isinstance(record.lifecycle_generation, bool) or not isinstance(
        record.lifecycle_generation, int
    ) or record.lifecycle_generation < 0:
        raise ValueError("lifecycle_generation must be a non-negative integer")
    if record.locator_kind is not LocatorKind.TIME and (
        type(record.page_start) is not int or type(record.page_end) is not int
        or record.page_start < 1 or record.page_end < record.page_start
    ):
        raise ValueError("chunk page range is invalid")
    if not isinstance(record.locator_kind, LocatorKind):
        raise ValueError("chunk locator kind is invalid")
    _require_text("chunk locator label", record.locator_label, 200)
    if record.locator_ordinal_start < 1 or record.locator_ordinal_end < record.locator_ordinal_start:
        raise ValueError("chunk locator ordinal range is invalid")
    _validate_enum_tuple(
        "chunk modalities",
        record.modalities,
        ContentModality,
        required=ContentModality.TEXT,
    )
    _validate_enum_tuple(
        "chunk provenance",
        record.provenance,
        ExtractionProvenance,
    )
    if not isinstance(record.visual_coverage, VisualCoverageStatus):
        raise ValueError("chunk visual coverage is invalid")
    if record.visual_coverage is VisualCoverageStatus.INCOMPLETE:
        raise ValueError("incomplete visual coverage cannot be persisted")
    if record.token_count <= 0:
        raise ValueError("chunk token count must be positive")
    if len(record.embedding) != 3072:
        raise ValueError("embedding length does not match expected 3072 dimensions")
    if record.language_code != "en":
        raise ValueError("schema version 1 supports only English")
    if len(record.key_phrases) > 25 or len(record.entities) > 50:
        raise ValueError("enrichment output exceeds schema bounds")
    for name, value, maximum in (
        ("source_id", record.source_id, 200),
        ("run_id", record.run_id, 100),
        ("document_id", record.document_id, 64),
        ("source_name", record.source_name, 500),
        ("content", record.content, 128_000),
        ("embedding_text", record.embedding_text, 128_000),
    ):
        _require_text(name, value, maximum)
    _require_sha256("content_hash", record.content_hash)
    _require_utc("created_at", record.created_at)
    _require_utc("embedded_at", record.embedded_at)
    if record.source_modified_at is not None:
        _require_utc("source_modified_at", record.source_modified_at)


@dataclass(frozen=True)
class Page:
    number: int
    text: str
    locator: SourceLocator | None = None
    modalities: tuple[ContentModality, ...] = (ContentModality.TEXT,)
    provenance: tuple[ExtractionProvenance, ...] = (ExtractionProvenance.DIRECT,)
    visual_coverage: VisualCoverageStatus = VisualCoverageStatus.NOT_REQUIRED

    def __post_init__(self) -> None:
        if self.number < 1:
            raise ValueError("page number must be positive")
        _require_text("page text", self.text, 8_000_000)
        _validate_enum_tuple(
            "page modalities",
            self.modalities,
            ContentModality,
            required=ContentModality.TEXT,
        )
        _validate_enum_tuple("page provenance", self.provenance, ExtractionProvenance)
        if not isinstance(self.visual_coverage, VisualCoverageStatus):
            raise ValueError("page visual coverage is invalid")


@dataclass(frozen=True)
class Chunk:
    ordinal: int
    page_number: int
    content: str
    locator: SourceLocator
    modalities: tuple[ContentModality, ...]
    provenance: tuple[ExtractionProvenance, ...]
    visual_coverage: VisualCoverageStatus
