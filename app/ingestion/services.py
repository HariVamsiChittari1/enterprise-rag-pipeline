"""Ingestion business logic: activate, discover, process, finalize."""

from __future__ import annotations

import logging
import hashlib
import json
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from itertools import chain
from typing import Any, Callable, Iterator

from config import ExtractionProvider, IngestionConfig
from ingestion.audio_staging import delete_audio, upload_audio
from ingestion.audio_transcription import build_transcript, build_transcript_from_batch
from ingestion.chunking import chunk_audio_segments, chunk_pages, token_count
from ingestion.embedding import embed_texts
from ingestion.enrichment import enrich_chunks
from ingestion.errors import TerminalDocumentError
from ingestion.speech_batch import (
    delete_transcription,
    download_transcription_result,
    get_transcription,
    list_transcription_files,
    submit_transcription,
)
from ingestion.speech_fast import transcribe_audio
from ingestion.extraction import (
    MIN_TEXT_CHARACTERS,
    VisionExtractionConfig,
    extract_content_understanding,
    extract_markdown,
    extract_office_document_intelligence,
    extract_pdf,
    extract_rendered_pdf_visuals,
)
from ingestion.graph import (
    DeltaResetRequired,
    DiscoveryState,
    ResolvedMarkdownImage,
    VerifiedAcl,
    canonical_source_mime,
    discovered_pdf_from_item,
    resolve_source_format,
    validate_source_signature,
)
from ingestion.lifecycle_repository import (
    DocumentLifecycleRepository,
    LifecycleConflictError,
    LifecycleDocumentRef,
    ReadyDocumentRef,
)
from ingestion.models import (
    ActivityOutcome,
    ActivityStatus,
    AudioMetadata,
    CanonicalExtractionResult,
    Chunk,
    DocumentStage,
    DocumentStatus,
    EnrichmentProfile,
    IngestionRunRecord,
    Page,
    ProfileSnapshot,
    RETIRED_REASONS,
    RunCounters,
    RunStage,
    RunStatus,
    SafeError,
    ScaleLimits,
    SearchChunkRecord,
    Settings,
    SUPPORTED_AUDIO_MIME_TYPES,
    SourceControlRecord,
    SourceDocumentRecord,
    content_sha256,
    content_sha256_bytes,
    create_chunk_id,
    create_document_id,
    create_document_key,
    create_run_id,
    create_source_run_id,
    create_visual_manifest_pages,
    run_record_id,
    safe_error_from_exception,
    visual_manifest_hash,
)
from ingestion.office_visuals import (
    bind_office_content_understanding_visuals,
    inventory_office_visuals,
    merge_office_visuals,
)
from ingestion.repository import (
    ActivatedRun,
    IngestionRepository,
    RepositoryConflictError,
    VersionedRecord,
)
from ingestion.source_connector import SourceConnector
from ingestion.telemetry import write_audit_record

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SourceSnapshot:
    source_format: str
    mime_type: str


@dataclass(frozen=True)
class _AuthorizedImage:
    image: ResolvedMarkdownImage
    acl: VerifiedAcl


def activate(config: IngestionConfig, repository: IngestionRepository, orchestration_instance_id: str) -> ActivatedRun:
    """Create a new run and atomically update currentRunId."""
    current_control = repository.get_source_control(config.source_id)
    if (
        current_control is not None
        and current_control.record.current_orchestration_instance_id == orchestration_instance_id
    ):
        current_run = repository.get_run(config.source_id, current_control.record.current_run_id)
        if current_run is not None:
            return ActivatedRun(current_run, current_control)
    now = _utc_now()
    run_id = create_run_id(now, config.source_id)
    instance_id = orchestration_instance_id
    run = IngestionRunRecord(
        source_id=config.source_id,
        run_id=run_id,
        drive_id=config.drive_id,
        orchestration_instance_id=instance_id,
        status=RunStatus.RUNNING,
        stage=RunStage.ACTIVATING,
        started_at=_fmt(now),
        activated_at=_fmt(now),
        updated_at=_fmt(now),
        counters=RunCounters(),
        profiles=ProfileSnapshot(
            enrichment=EnrichmentProfile(
                summary_enabled=config.summary_enabled,
                key_phrases_enabled=config.key_phrases_enabled,
                entities_enabled=config.entities_enabled,
            ),
        ),
        ingestion_mode="full-sync",
        id=run_record_id(run_id),
    )
    control = SourceControlRecord(
        source_id=config.source_id,
        current_run_id=run_id,
        current_orchestration_instance_id=instance_id,
        activated_at=_fmt(now),
        updated_at=_fmt(now),
    )
    return repository.activate_run(run, control)


def discover_all(
    config: IngestionConfig,
    run_id: str,
    repository: IngestionRepository,
    connector: SourceConnector,
    *,
    work_registry: Any | None = None,
    source_plan: Any | None = None,
    owner_instance_id: str | None = None,
) -> tuple[list[SourceDocumentRecord], int]:
    """Discover all eligible files from the source and persist as documents."""
    state = DiscoveryState.initial()
    documents: list[SourceDocumentRecord] = []
    skipped = 0
    rejected = 0
    while not state.complete:
        state, page_documents, page_skipped, page_rejected = discover_next_page(
            config,
            run_id,
            repository,
            connector,
            state,
            work_registry=work_registry,
            source_plan=source_plan,
            owner_instance_id=owner_instance_id,
        )
        documents.extend(page_documents)
        skipped += page_skipped
        rejected += page_rejected
    logger.info(
        "Discovery complete: %d to process, %d skipped, %d rejected, %d items scanned",
        len(documents), skipped, rejected, state.items_scanned,
    )
    return documents, state.items_scanned


def discover_next_page(
    config: IngestionConfig,
    run_id: str,
    repository: IngestionRepository,
    connector: SourceConnector,
    state: DiscoveryState,
    *,
    work_registry: Any | None = None,
    source_plan: Any | None = None,
    owner_instance_id: str | None = None,
) -> tuple[DiscoveryState, list[SourceDocumentRecord], int, int]:
    """Capture and persist one bounded Graph children page."""
    if work_registry is not None and (
        source_plan is None or not isinstance(owner_instance_id, str) or not owner_instance_id
    ):
        raise ValueError("discovery_work_context_required")
    settings = Settings(source_id=config.source_id, drive_id=config.drive_id, code_version="durable-sync")
    documents: list[SourceDocumentRecord] = []
    skipped = 0

    control = repository.get_source_control(config.source_id)
    prev_run_id = control.record.last_completed_run_id if control else None
    prev_source_run_id = create_source_run_id(config.source_id, prev_run_id) if prev_run_id else None
    step = connector.discover_next_page(
        state, settings.limits, allowed_extensions=config.allowed_extensions,
    )
    for pdf in step.pdfs:
        doc_id = create_document_id(config.source_id, config.drive_id, pdf.item_id)
        winner = (
            work_registry.get_document_winner(config.source_id, doc_id)
            if work_registry is not None else None
        )
        winner_source_run_id = (
            winner.get("sourceRunId")
            if isinstance(winner, dict)
            and winner.get("sourceId") == config.source_id
            and winner.get("documentId") == doc_id
            else None
        )
        baseline_source_run_id = (
            winner_source_run_id
            if isinstance(winner_source_run_id, str) and winner_source_run_id
            else prev_source_run_id
        )
        if baseline_source_run_id:
            try:
                prev = repository.get_document(baseline_source_run_id, doc_id)
            except Exception:
                logger.warning("Skip-if-ready lookup failed for %s", doc_id, exc_info=True)
                prev = None
            if prev and prev.record.status is DocumentStatus.READY and prev.record.e_tag == pdf.e_tag:
                discovered_modified = getattr(pdf, "last_modified_date_time", None)
                if discovered_modified is None or prev.record.source_modified_at == discovered_modified:
                    if work_registry is None or _winner_matches_document_record(
                        winner,
                        prev.record,
                        source_id=config.source_id,
                        drive_id=config.drive_id,
                        source_etag=pdf.e_tag,
                        profile_digest=_processing_profile_digest(config),
                    ):
                        if work_registry is None:
                            skipped += 1
                            continue
                        try:
                            current_item = connector.read_item(pdf.item_id)
                            current_acl = connector.read_verified_acl(
                                pdf.item_id,
                                config.acl_max_pages,
                            )
                            reusable = (
                                current_item is not None
                                and current_item.get("eTag") == pdf.e_tag
                                and current_acl.acl_hash == prev.record.acl_hash
                            )
                        except Exception:
                            logger.warning(
                                "Winner reuse verification failed for %s; reprocessing",
                                doc_id,
                                exc_info=True,
                            )
                            reusable = False
                        if reusable:
                            skipped += 1
                            continue
        doc = _source_to_document(pdf, config, run_id)
        if work_registry is not None:
            claim = work_registry.begin_document_operation(
                source_plan,
                owner_instance_id=owner_instance_id,
                physical_run_id=doc.source_run_id,
                item_id=doc.document_id,
                operation_kind="full-sync",
            )
            if claim.get("status") not in {"acquired", "completed"}:
                raise RepositoryConflictError("discovery_document_work_not_acquired")
            if claim.get("status") == "completed":
                stored = repository.get_document(doc.source_run_id, doc.document_id)
                if stored is None:
                    raise RepositoryConflictError("completed_discovery_document_missing")
            else:
                # Leave the operation claimed; process_document_activity owns the start.
                stored = repository.create_discovered_document(doc)
        else:
            stored = repository.create_discovered_document(doc)
        documents.append(stored.record)
    return step.state, documents, skipped, step.rejected_count


def _winner_matches_document_record(
    winner: Any,
    record: SourceDocumentRecord,
    *,
    source_id: str,
    drive_id: str,
    source_etag: str,
    profile_digest: str,
) -> bool:
    return (
        isinstance(winner, dict)
        and record.status is DocumentStatus.READY
        and record.stage is DocumentStage.TERMINAL
        and all((
            winner.get("recordType") == "document-winner",
            winner.get("schemaVersion") == 1,
            winner.get("sourceId") == source_id == record.source_id,
            winner.get("documentId") == record.document_id,
            winner.get("driveId") == drive_id == record.drive_id,
            winner.get("itemId") == record.item_id,
            winner.get("sourceRunId") == record.source_run_id,
            winner.get("documentKey") == record.document_key,
            winner.get("sourceEtag") == source_etag == record.e_tag,
            winner.get("lifecycleGeneration") == record.lifecycle_generation,
            winner.get("processingProfileDigest") == profile_digest,
            record.processing_profile_digest == profile_digest,
        ))
    )


def process_document(
    config: IngestionConfig,
    document: SourceDocumentRecord,
    document_etag: str,
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    di_client: Any | None,
    language_client: Any | None,
    openai_client: Any,
    audit_container: Any | None = None,
    *,
    cu_client: Any | None = None,
    speech_token_provider: Callable[[], str] | None = None,
    blob_service_client: Any | None = None,
    document_work_id: str | None = None,
    document_work_attempt_id: str | None = None,
) -> ActivityOutcome:
    """Process a single document through the full pipeline."""
    if (document_work_id is None) != (document_work_attempt_id is None):
        raise ValueError("document work and attempt IDs must be set together")
    try:
        processing = repository.mark_document_processing(
            replace(
                document,
                status=DocumentStatus.PROCESSING,
                stage=DocumentStage.ACL,
                attempt_count=document.attempt_count + 1,
                processing_started_at=_fmt(_utc_now()),
                updated_at=_fmt(_utc_now()),
            ),
            document_etag,
        )
    except RepositoryConflictError:
        return ActivityOutcome(document_id=document.document_id, status=ActivityStatus.SKIPPED, chunks_written=0, retry_count=0)

    current_doc = processing.record
    current_etag = processing.etag
    try:
        limits = ScaleLimits()
        source_snapshot = _read_and_validate_source_snapshot(connector, current_doc)
        source_format = source_snapshot.source_format
        acl = connector.read_verified_acl(current_doc.item_id, config.acl_max_pages)
        authorized_images: list[_AuthorizedImage] = []

        content = connector.download_content_sync(
            current_doc.item_id,
            limits.max_source_bytes,
            config.download_timeout_seconds,
        )
        validate_source_signature(source_format, content)

        if source_snapshot.mime_type in SUPPORTED_AUDIO_MIME_TYPES:
            if not config.audio_writer_enabled:
                raise TerminalDocumentError("audio_writer_disabled")
            if speech_token_provider is None:
                raise TerminalDocumentError("audio_transcription_token_provider_missing")
            source_content_hash = content_sha256_bytes(content)
            if config.audio_transcription_provider == "speech_batch":
                if blob_service_client is None:
                    raise TerminalDocumentError("audio_staging_client_missing")
                blob_name = _staging_blob_name(current_doc)
                content_url = upload_audio(
                    blob_service_client=blob_service_client,
                    container=config.audio_staging_container,
                    blob_name=blob_name,
                    data=content,
                )
                job_url = submit_transcription(
                    endpoint=config.speech_endpoint,
                    content_urls=[content_url],
                    locale=config.audio_locale,
                    display_name=current_doc.document_id,
                    time_to_live_hours=config.audio_batch_ttl_hours,
                    token_provider=speech_token_provider,
                    timeout_seconds=config.speech_request_timeout_seconds,
                )
                submitted = replace(
                    current_doc, stage=DocumentStage.TRANSCRIBING,
                    content_hash=source_content_hash,
                    extraction_mode=config.audio_transcription_provider,
                    transcription_job_url=job_url, staging_blob_name=blob_name,
                    transcription_work_id=document_work_id,
                    transcription_attempt_id=document_work_attempt_id,
                    updated_at=_fmt(_utc_now()),
                )
                repository.update_processing_document(submitted, current_etag)
                # Submit succeeded; the poller finalizes this doc once the batch job completes.
                return ActivityOutcome(
                    document_id=document.document_id, status=ActivityStatus.PENDING,
                    chunks_written=0, retry_count=document.attempt_count,
                )
            response = transcribe_audio(
                endpoint=config.speech_endpoint,
                audio=content,
                filename=current_doc.source_name,
                content_type=source_snapshot.mime_type,
                locale=config.audio_locale,
                token_provider=speech_token_provider,
                max_response_bytes=config.audio_max_response_bytes,
                timeout_seconds=config.speech_request_timeout_seconds,
            )
            audio, segments = build_transcript(
                response,
                locale=config.audio_locale,
                profile_version=config.audio_transcription_provider,
                source_version=current_doc.e_tag,
                source_content_hash=source_content_hash,
                max_response_bytes=config.audio_max_response_bytes,
                max_phrases=config.audio_max_phrases,
                max_words=config.audio_max_words,
            )
            written = _finalize_audio_document(
                config, current_doc, current_etag, audio, segments, acl, source_snapshot,
                repository, lifecycle_repository, connector, language_client, openai_client,
                audit_container,
            )
            return ActivityOutcome(
                document_id=document.document_id, status=ActivityStatus.SUCCEEDED,
                chunks_written=written, retry_count=document.attempt_count,
            )

        if config.extraction_enabled:
            extraction_start = time.perf_counter()
            derivative_used = False
            extraction_provider = "direct"
            excluded_reasons: list[str] = []
            unsupported_reasons: list[str] = []
            vision_config = VisionExtractionConfig(
                deployment=config.vision_deployment,
                max_output_tokens=config.vision_max_output_tokens,
                max_image_bytes=config.vision_max_image_bytes,
                max_figures=min(
                    config.vision_max_figures,
                    limits.max_visual_descriptions,
                ),
            )
            if source_format == ".md":
                def load_markdown_image(path: str) -> bytes:
                    image = connector.resolve_relative_markdown_image_sync(
                        current_doc.parent_item_id,
                        path,
                        config.vision_max_image_bytes,
                    )
                    image_acl = connector.read_verified_acl(
                        image.item_id,
                        config.acl_max_pages,
                    )
                    if not set(acl.allowed_group_ids).issubset(
                        image_acl.allowed_group_ids
                    ):
                        raise TerminalDocumentError(
                            "markdown_image_acl_not_authorized"
                        )
                    image_content = connector.download_content_sync(
                        image.item_id,
                        config.vision_max_image_bytes,
                        config.download_timeout_seconds,
                    )
                    authorized_images.append(_AuthorizedImage(image, image_acl))
                    return image_content

                extraction = extract_markdown(
                    content,
                    image_loader=load_markdown_image,
                    openai_client=openai_client,
                    vision_config=vision_config,
                )
                extraction_mode = "direct-markdown"
            elif source_format == ".pdf":
                if config.extraction_provider is ExtractionProvider.CONTENT_UNDERSTANDING:
                    if cu_client is None:
                        raise TerminalDocumentError(
                            "content_understanding_configuration_missing"
                        )
                    extraction = extract_content_understanding(
                        cu_client,
                        content,
                        source_snapshot.mime_type,
                        max_pages=min(config.max_pdf_pages, limits.max_document_units),
                        analyzer_id=config.content_understanding_analyzer_id,
                    )
                    extraction_provider = ExtractionProvider.CONTENT_UNDERSTANDING.value
                    extraction_mode = config.content_understanding_analyzer_id
                else:
                    if di_client is None:
                        raise TerminalDocumentError(
                            "document_intelligence_configuration_missing"
                        )
                    extraction = extract_pdf(
                        di_client,
                        content,
                        max_pdf_pages=min(
                            config.max_pdf_pages,
                            limits.max_document_units,
                        ),
                        openai_client=openai_client,
                        vision_config=vision_config,
                    )
                    extraction_provider = ExtractionProvider.DOCUMENT_INTELLIGENCE.value
                    extraction_mode = "prebuilt-layout"
            else:
                inventory = inventory_office_visuals(
                    content,
                    source_snapshot.mime_type,
                    max_content_units=limits.max_document_units,
                )
                if len(inventory.required) > limits.max_visual_descriptions:
                    raise TerminalDocumentError(
                        "visual_description_limit_exceeded"
                    )
                if config.extraction_provider is ExtractionProvider.CONTENT_UNDERSTANDING:
                    if cu_client is None:
                        raise TerminalDocumentError(
                            "content_understanding_configuration_missing"
                        )
                    analysis_content = connector.download_content_as_pdf_sync(
                        current_doc.item_id,
                        limits.max_rendered_pdf_bytes,
                        config.download_timeout_seconds,
                    )
                    analysis_content_type = "application/pdf"
                    derivative_used = True
                    extraction = extract_content_understanding(
                        cu_client,
                        analysis_content,
                        analysis_content_type,
                        max_pages=min(config.max_pdf_pages, limits.max_document_units),
                        analyzer_id=config.content_understanding_analyzer_id,
                    )
                    if (
                        inventory.required
                        and extraction.visual_coverage.required_count
                        != len(inventory.required)
                    ):
                        raise TerminalDocumentError(
                            "content_understanding_office_visual_coverage_mismatch"
                        )
                    extraction = bind_office_content_understanding_visuals(
                        extraction,
                        inventory,
                        source_snapshot.mime_type,
                    )
                    native_segment_count = len(extraction.segments)
                    extraction_provider = ExtractionProvider.CONTENT_UNDERSTANDING.value
                    extraction_mode = config.content_understanding_analyzer_id
                else:
                    if di_client is None:
                        raise TerminalDocumentError(
                            "document_intelligence_configuration_missing"
                        )
                    native_extraction = extract_office_document_intelligence(
                        di_client,
                        content,
                        source_snapshot.mime_type,
                        max_pages=min(config.max_pdf_pages, limits.max_document_units),
                    )
                    native_segment_count = len(native_extraction.segments)
                    rendered_visuals = ()
                    if inventory.required:
                        rendered_pdf = connector.download_content_as_pdf_sync(
                            current_doc.item_id,
                            limits.max_rendered_pdf_bytes,
                            config.download_timeout_seconds,
                        )
                        rendered_visuals = extract_rendered_pdf_visuals(
                            di_client,
                            rendered_pdf,
                            max_pdf_pages=min(
                                config.max_pdf_pages,
                                limits.max_document_units,
                            ),
                            openai_client=openai_client,
                            vision_config=vision_config,
                        )
                        derivative_used = True
                    extraction = merge_office_visuals(
                        native_extraction,
                        inventory,
                        rendered_visuals,
                        source_snapshot.mime_type,
                    )
                    extraction_provider = ExtractionProvider.DOCUMENT_INTELLIGENCE.value
                    extraction_mode = "document-intelligence-layout+rendered-visuals"
                excluded_reasons = sorted(
                    {omission.reason for omission in inventory.excluded}
                )
                unsupported_reasons = sorted(
                    {omission.reason for omission in inventory.unsupported}
                )
            if source_format in {".md", ".pdf"}:
                native_segment_count = len(extraction.segments)
            if (
                extraction.visual_coverage.described_count
                > limits.max_visual_descriptions
            ):
                raise TerminalDocumentError("visual_description_limit_exceeded")
            if source_format not in {".md", ".pdf"} and sum(
                len(segment.text) for segment in extraction.segments
            ) > limits.max_office_characters:
                raise TerminalDocumentError("office_character_limit_exceeded")
            pages = _pages_from_extraction(extraction)
            if sum(len(page.text) for page in pages) < MIN_TEXT_CHARACTERS:
                raise TerminalDocumentError("extraction_insufficient_text")
            if audit_container is not None:
                total_chars = sum(len(p.text) for p in pages)
                provenance = sorted(
                    {
                        value.value
                        for segment in extraction.segments
                        for value in segment.provenance
                    }
                )
                write_audit_record(audit_container, config.source_id, current_doc.source_run_id, {
                    "operation": "document_extraction", "model": extraction_mode,
                    "extractionProvider": extraction_provider,
                    "format": source_format,
                    "pages": len(pages), "characters": total_chars,
                    "nativeSegmentCount": native_segment_count,
                    "derivativeUsed": derivative_used,
                    "provenance": provenance,
                    "visualCoverage": extraction.visual_coverage.status.value,
                    "visualInventoryCount": extraction.visual_coverage.inventory_count,
                    "visualRequiredCount": extraction.visual_coverage.required_count,
                    "visualDescribedCount": extraction.visual_coverage.described_count,
                    "visualExcludedCount": extraction.visual_coverage.excluded_count,
                    "visualUnsupportedCount": extraction.visual_coverage.unsupported_count,
                    "visualUncoveredCount": extraction.visual_coverage.uncovered_count,
                    "excludedReasons": excluded_reasons,
                    "unsupportedReasons": unsupported_reasons,
                    "latency_ms": int((time.perf_counter() - extraction_start) * 1000),
                    "documentId": current_doc.document_id, "sourceName": current_doc.source_name,
                })
        else:
            raise TerminalDocumentError("extraction_disabled_no_alternative")

        chunks = chunk_pages(pages)

        if config.enrichment_enabled and language_client is not None:
            enrichment_start = time.perf_counter()
            enrichments = enrich_chunks(
                language_client, [chunk.content for chunk in chunks],
                summary_enabled=config.summary_enabled,
                key_phrases_enabled=config.key_phrases_enabled,
                entities_enabled=config.entities_enabled,
            )
            if audit_container is not None:
                statuses = [e["status"] for e in enrichments]
                write_audit_record(audit_container, config.source_id, current_doc.source_run_id, {
                    "operation": "enrichment", "chunks": len(chunks),
                    "key_phrases": "succeeded" if any(s.key_phrases.value == "succeeded" for s in statuses) else "failed",
                    "entities": "succeeded" if any(s.entities.value == "succeeded" for s in statuses) else "failed",
                    "summary": "succeeded" if config.summary_enabled and any(s.summary.value == "succeeded" for s in statuses) else "not_requested",
                    "latency_ms": int((time.perf_counter() - enrichment_start) * 1000),
                })
        else:
            enrichments = enrich_chunks(None, [chunk.content for chunk in chunks])

        searchable_texts = [
            _build_searchable_text(chunk.content, enrichments[i]["key_phrases"], enrichments[i]["summary"])
            for i, chunk in enumerate(chunks)
        ]

        embeddings = embed_texts(
            openai_client, searchable_texts,
            audit_container=audit_container,
            source_id=config.source_id,
            run_id=current_doc.source_run_id,
            batch_size=config.embedding_batch_size,
        )

        now = _fmt(_utc_now())
        chunk_records = _build_chunk_records(current_doc, acl, chunks, searchable_texts, enrichments, embeddings, now)

        current_doc = replace(current_doc, stage=DocumentStage.PERSISTING, page_count=len(pages), expected_chunk_count=len(chunk_records), content_hash=content_sha256("\n".join(p.text for p in pages)), extraction_mode=extraction_mode, updated_at=_fmt(_utc_now()))
        updated = repository.update_processing_document(current_doc, current_etag)
        current_doc, current_etag = updated.record, updated.etag

        written = repository.write_chunks(chunk_records)
        manifest_pages = create_visual_manifest_pages(
            current_doc,
            extraction.visual_manifest_entries,
        )
        repository.write_visual_manifest_pages(manifest_pages)
        current_doc = replace(
            current_doc,
            visual_manifest_page_count=len(manifest_pages),
            visual_manifest_hash=visual_manifest_hash(manifest_pages),
            updated_at=_fmt(_utc_now()),
        )
        updated = repository.update_processing_document(current_doc, current_etag)
        current_doc, current_etag = updated.record, updated.etag

        _read_and_validate_source_snapshot(
            connector,
            current_doc,
            expected_snapshot=source_snapshot,
        )
        final_acl = connector.read_verified_acl(
            current_doc.item_id,
            config.acl_max_pages,
        )
        if final_acl != acl:
            raise TerminalDocumentError("source_acl_changed_during_processing")
        for authorized_image in authorized_images:
            _validate_resolved_image_snapshot(
                connector.read_item(authorized_image.image.item_id),
                authorized_image.image,
            )
            final_image_acl = connector.read_verified_acl(
                authorized_image.image.item_id,
                config.acl_max_pages,
            )
            if final_image_acl != authorized_image.acl:
                raise TerminalDocumentError(
                    "markdown_image_acl_changed_during_processing"
                )
            if not set(final_acl.allowed_group_ids).issubset(
                final_image_acl.allowed_group_ids
            ):
                raise TerminalDocumentError("markdown_image_acl_not_authorized")

        admitting_doc = replace(
            current_doc,
            status=DocumentStatus.ADMITTING,
            stage=DocumentStage.VERIFYING,
            allowed_group_ids=acl.allowed_group_ids,
            acl_hash=acl.acl_hash,
            acl_evaluated_at=_fmt(_utc_now()),
            expected_chunk_count=len(chunk_records),
            written_chunk_count=written,
            updated_at=_fmt(_utc_now()),
        )
        admitting = repository.begin_document_admission(admitting_doc, current_etag)
        current_doc, current_etag = admitting.record, admitting.etag

        lifecycle_repository.set_document_chunks_retrievable(
            document_key=current_doc.document_key,
            lifecycle_generation=current_doc.lifecycle_generation,
            is_retrievable=True,
            allowed_group_ids=acl.allowed_group_ids,
            expected_count=written,
        )

        ready_doc = replace(
            current_doc,
            status=DocumentStatus.READY, stage=DocumentStage.TERMINAL,
            ready_at=_fmt(_utc_now()), updated_at=_fmt(_utc_now()),
        )
        repository.verify_and_mark_document_ready(ready_doc, current_etag)

        return ActivityOutcome(document_id=document.document_id, status=ActivityStatus.SUCCEEDED, chunks_written=written, retry_count=document.attempt_count)

    except TerminalDocumentError as error:
        _fail_document(current_doc, current_etag, error, repository)
        return ActivityOutcome(document_id=document.document_id, status=ActivityStatus.FAILED, chunks_written=0, retry_count=document.attempt_count, error=SafeError(str(error), current_doc.stage.value, False))
    except RepositoryConflictError:
        return ActivityOutcome(document_id=document.document_id, status=ActivityStatus.SKIPPED, chunks_written=0, retry_count=document.attempt_count)
    except Exception as error:
        logger.error("Document %s failed at stage=%s: %s", document.document_id, current_doc.stage.value, error, exc_info=True)
        safe = safe_error_from_exception(error, current_doc.stage.value)
        _fail_document(current_doc, current_etag, error, repository)
        return ActivityOutcome(document_id=document.document_id, status=ActivityStatus.FAILED, chunks_written=0, retry_count=document.attempt_count, error=safe)


def _staging_blob_name(document: SourceDocumentRecord) -> str:
    """Deterministic, collision-free staging blob name for one document version."""
    dot = document.source_name.rfind(".")
    extension = document.source_name[dot:].lower() if dot != -1 else ".wav"
    return f"{document.document_key}{extension}"


def _finalize_audio_document(
    config: IngestionConfig,
    current_doc: SourceDocumentRecord,
    current_etag: str,
    audio: Any,
    segments: Any,
    acl: Any,
    source_snapshot: Any,
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    language_client: Any | None,
    openai_client: Any,
    audit_container: Any | None,
) -> int:
    """Chunk, enrich, embed, persist, admit, and mark ready a transcribed audio document."""
    chunks = chunk_audio_segments(segments, audio)
    if config.enrichment_enabled and language_client is not None:
        enrichments = enrich_chunks(
            language_client, [chunk.content for chunk in chunks],
            summary_enabled=config.summary_enabled,
            key_phrases_enabled=config.key_phrases_enabled,
            entities_enabled=config.entities_enabled,
        )
    else:
        enrichments = enrich_chunks(None, [chunk.content for chunk in chunks])
    searchable_texts = [
        _build_searchable_text(chunk.content, enrichments[i]["key_phrases"], enrichments[i]["summary"])
        for i, chunk in enumerate(chunks)
    ]
    embeddings = embed_texts(
        openai_client, searchable_texts,
        audit_container=audit_container,
        source_id=config.source_id,
        run_id=current_doc.source_run_id,
        batch_size=config.embedding_batch_size,
    )
    now = _fmt(_utc_now())
    chunk_records = _build_chunk_records(
        current_doc, acl, chunks, searchable_texts, enrichments, embeddings, now, audio=audio,
    )
    # Clearing the tracking fields is a no-op for the inline path and cleans up the batch path.
    persisting = replace(
        current_doc, stage=DocumentStage.PERSISTING, page_count=None,
        expected_chunk_count=len(chunk_records), content_hash=audio.source_content_hash,
        extraction_mode=config.audio_transcription_provider, audio=audio,
        transcription_job_url=None, staging_blob_name=None, updated_at=_fmt(_utc_now()),
    )
    updated = repository.update_processing_document(persisting, current_etag)
    current_doc, current_etag = updated.record, updated.etag
    written = repository.write_chunks(chunk_records)
    _read_and_validate_source_snapshot(connector, current_doc, expected_snapshot=source_snapshot)
    final_acl = connector.read_verified_acl(current_doc.item_id, config.acl_max_pages)
    if final_acl != acl:
        raise TerminalDocumentError("source_acl_changed_during_processing")
    admitting_doc = replace(
        current_doc, status=DocumentStatus.ADMITTING, stage=DocumentStage.VERIFYING,
        allowed_group_ids=acl.allowed_group_ids, acl_hash=acl.acl_hash,
        acl_evaluated_at=_fmt(_utc_now()), expected_chunk_count=len(chunk_records),
        written_chunk_count=written, source_verified_at=_fmt(_utc_now()),
        updated_at=_fmt(_utc_now()),
    )
    admitting = repository.begin_document_admission(admitting_doc, current_etag)
    current_doc, current_etag = admitting.record, admitting.etag
    lifecycle_repository.set_document_chunks_retrievable(
        document_key=current_doc.document_key,
        lifecycle_generation=current_doc.lifecycle_generation,
        is_retrievable=True,
        allowed_group_ids=acl.allowed_group_ids,
        expected_count=written,
    )
    ready_doc = replace(
        current_doc, status=DocumentStatus.READY, stage=DocumentStage.TERMINAL,
        ready_at=_fmt(_utc_now()), updated_at=_fmt(_utc_now()),
    )
    repository.verify_and_mark_document_ready(ready_doc, current_etag)
    return written


def finalize_audio_transcription(
    config: IngestionConfig,
    source_run_id: str,
    document_id: str,
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    language_client: Any | None,
    openai_client: Any,
    *,
    speech_token_provider: Callable[[], str],
    blob_service_client: Any,
    audit_container: Any | None = None,
) -> str:
    """Advance one submitted audio document by polling its batch job.

    Returns 'succeeded', 'failed', 'pending' (still transcribing), or 'skipped' (not awaiting).
    """
    stored = repository.get_document(source_run_id, document_id)
    if stored is None:
        return "skipped"
    current_doc, current_etag = stored.record, stored.etag
    if current_doc.stage is not DocumentStage.TRANSCRIBING or not current_doc.transcription_job_url:
        return "skipped"
    job_url = current_doc.transcription_job_url
    try:
        job = get_transcription(
            transcription_url=job_url, token_provider=speech_token_provider,
            timeout_seconds=config.speech_request_timeout_seconds,
        )
    except TimeoutError:
        return "pending"
    if job.status in ("NotStarted", "Running"):
        return "pending"
    try:
        if job.status == "Succeeded":
            body = _download_batch_transcript(config, job, speech_token_provider)
            audio, segments = build_transcript_from_batch(
                body, locale=config.audio_locale,
                profile_version=config.audio_transcription_provider,
                source_version=current_doc.e_tag,
                source_content_hash=current_doc.content_hash or "",
                max_response_bytes=config.audio_max_response_bytes,
                max_phrases=config.audio_max_phrases, max_words=config.audio_max_words,
            )
            acl = connector.read_verified_acl(current_doc.item_id, config.acl_max_pages)
            source_snapshot = _read_and_validate_source_snapshot(connector, current_doc)
            _finalize_audio_document(
                config, current_doc, current_etag, audio, segments, acl, source_snapshot,
                repository, lifecycle_repository, connector, language_client, openai_client,
                audit_container,
            )
            _cleanup_batch_job(config, job_url, current_doc.staging_blob_name, speech_token_provider, blob_service_client)
            return "succeeded"
        _fail_document(current_doc, current_etag, TerminalDocumentError(f"audio_batch_status:{job.status}"), repository)
        _cleanup_batch_job(config, job_url, current_doc.staging_blob_name, speech_token_provider, blob_service_client)
        return "failed"
    except TimeoutError:
        return "pending"
    except TerminalDocumentError as error:
        _fail_reloaded_document(repository, source_run_id, document_id, error)
        _cleanup_batch_job(config, job_url, current_doc.staging_blob_name, speech_token_provider, blob_service_client)
        return "failed"
    except Exception as error:  # defensive: never leave a poller pass unhandled
        logger.error("audio finalize failed for %s: %s", document_id, error, exc_info=True)
        _fail_reloaded_document(repository, source_run_id, document_id, error)
        return "failed"


def _fail_reloaded_document(
    repository: IngestionRepository, source_run_id: str, document_id: str, error: BaseException,
) -> None:
    """Mark a document failed using its current etag (finalize may have advanced it)."""
    reloaded = repository.get_document(source_run_id, document_id)
    if reloaded is not None:
        _fail_document(reloaded.record, reloaded.etag, error, repository)


def _download_batch_transcript(config: IngestionConfig, job: Any, token_provider: Callable[[], str]) -> bytes:
    if not job.files_url:
        raise TerminalDocumentError("audio_batch_files_link_missing")
    files = list_transcription_files(
        files_url=job.files_url, token_provider=token_provider,
        timeout_seconds=config.speech_request_timeout_seconds,
    )
    transcript = next((entry for entry in files if entry.kind == "Transcription"), None)
    if transcript is None:
        raise TerminalDocumentError("audio_batch_transcript_file_missing")
    return download_transcription_result(
        content_url=transcript.content_url,
        max_response_bytes=config.audio_max_response_bytes,
        timeout_seconds=config.speech_request_timeout_seconds,
    )


def _cleanup_batch_job(
    config: IngestionConfig, job_url: str, blob_name: str | None,
    token_provider: Callable[[], str], blob_service_client: Any,
) -> None:
    """Best-effort deletion of the finished job and its staging blob (TTL is the safety net)."""
    try:
        delete_transcription(
            transcription_url=job_url, token_provider=token_provider,
            timeout_seconds=config.speech_request_timeout_seconds,
        )
    except Exception:
        logger.warning("failed to delete batch transcription job", exc_info=True)
    if blob_service_client is not None and blob_name:
        try:
            delete_audio(
                blob_service_client=blob_service_client,
                container=config.audio_staging_container, blob_name=blob_name,
            )
        except Exception:
            logger.warning("failed to delete staging blob", exc_info=True)


def finalize(
    config: IngestionConfig,
    run_etag: str,
    repository: IngestionRepository,
    items_scanned: int,
) -> VersionedRecord[IngestionRunRecord]:
    """Compute exact counters and mark run terminal."""
    control = repository.get_source_control(config.source_id)
    if control is None:
        raise RepositoryConflictError("source control not found")
    current = repository.get_run(config.source_id, control.record.current_run_id)
    if current is None:
        raise RepositoryConflictError("run no longer exists")
    counters = repository.compute_run_counters(
        config.source_id, current.record.run_id, retries=0, items_scanned=items_scanned,
    )
    status = RunStatus.COMPLETED_WITH_ERRORS if counters.failed else RunStatus.COMPLETED
    terminal_run = replace(
        current.record,
        status=status,
        stage=RunStage.TERMINAL,
        completed_at=_fmt(_utc_now()),
        updated_at=_fmt(_utc_now()),
    )
    return repository.finalize_run(terminal_run, run_etag, retries=0, items_scanned=items_scanned)


def terminate_run(
    config: IngestionConfig,
    repository: IngestionRepository,
) -> dict[str, Any]:
    """Fail all non-terminal docs and finalize the run as TERMINATED."""
    control = repository.get_source_control(config.source_id)
    if control is None:
        return {"status": "no_active_run"}
    current = repository.get_run(config.source_id, control.record.current_run_id)
    if current is None:
        return {"status": "no_active_run"}
    if current.record.stage is RunStage.TERMINAL:
        return {"status": "already_terminal", "runStatus": current.record.status.value}

    failed_count = repository.fail_nonterminal_documents(
        config.source_id, current.record.run_id, "orchestration_terminated",
    )
    terminal_run = replace(
        current.record,
        status=RunStatus.TERMINATED,
        stage=RunStage.TERMINAL,
        completed_at=_fmt(_utc_now()),
        updated_at=_fmt(_utc_now()),
    )
    finalized = repository.finalize_run(terminal_run, current.etag, retries=0, items_scanned=0)
    return {
        "status": "terminated",
        "runId": current.record.run_id,
        "docsForceFailed": failed_count,
        "counters": finalized.record.counters.__dict__ if hasattr(finalized.record.counters, '__dict__') else {},
    }


def get_retry_candidates(
    config: IngestionConfig,
    repository: IngestionRepository,
) -> list[dict[str, Any]]:
    """Return failed documents from the current run that can be retried."""
    control = repository.get_source_control(config.source_id)
    if control is None:
        return []
    run_id = control.record.current_run_id
    if not run_id:
        return []
    return repository.get_failed_documents(config.source_id, run_id)


@dataclass(frozen=True)
class DeltaSyncOutcome:
    bootstrapped: bool = False
    created_or_updated: int = 0
    deleted: int = 0
    acl_resynced: int = 0
    failed: int = 0
    pending: int = 0
    items_seen: int = 0
    continuation_token: str | None = None
    continuation_phase: str | None = None


def run_delta_sync(
    config: IngestionConfig,
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    di_client: Any | None,
    language_client: Any | None,
    openai_client: Any,
    audit_container: Any | None = None,
    *,
    cu_client: Any | None = None,
    speech_token_provider: Callable[[], str] | None = None,
    blob_service_client: Any | None = None,
    work_registry: Any | None = None,
    source_plan: Any | None = None,
    owner_instance_id: str | None = None,
    delta_execution_id: str | None = None,
    reduced_items_continuation_token: str | None = None,
    reduced_items_page_size: int = 50,
    processing_phase: str = "items",
    phase_continuation_token: str | None = None,
    defer_cursor_commit: bool = False,
    force_full_baseline: bool = False,
    enumerate_only: bool = False,
    round_start_link: str | None = None,
) -> DeltaSyncOutcome:
    """One delta-sync tick: process adds/updates/deletes for source_id since the last
    cursor. Uses its own run_id per tick purely as a schema-compliant namespacing device
    (sourceRunId/documentKey); it does not touch full-sync's source-control singleton."""
    if work_registry is not None and (
        source_plan is None or not owner_instance_id or not delta_execution_id
    ):
        raise ValueError("delta_document_work_context_required")
    committed_cursor = lifecycle_repository.get_delta_cursor(config.source_id)
    round_anchor = None if force_full_baseline else (
        round_start_link if round_start_link is not None else committed_cursor
    )
    request_cursor = None if force_full_baseline else (
        round_start_link if round_start_link is not None else committed_cursor
    )
    if work_registry is not None:
        round_state = work_registry.begin_delta_round(
            source_plan, owner_instance_id, delta_execution_id, round_anchor,
        )
        if round_state.get("state") in {"cursor_ready", "committed"}:
            if enumerate_only:
                return DeltaSyncOutcome(
                    bootstrapped=True,
                    items_seen=work_registry.count_delta_round_entries(
                        config.source_id, delta_execution_id,
                    ),
                    continuation_token=round_state.get("candidateDeltaLink"),
                    continuation_phase="baseline_complete",
                )
            if defer_cursor_commit and round_state.get("state") == "cursor_ready":
                candidate = round_state.get("candidateDeltaLink")
                if not isinstance(candidate, str) or not candidate:
                    raise RepositoryConflictError("delta_round_candidate_link_missing")
                return DeltaSyncOutcome(
                    **(round_state.get("outcome") or {}),
                    continuation_token=candidate,
                    continuation_phase="commit",
                )
            if round_state.get("state") == "cursor_ready":
                candidate = round_state.get("candidateDeltaLink")
                if not isinstance(candidate, str) or not candidate:
                    raise RepositoryConflictError("delta_round_candidate_link_missing")
                lifecycle_repository.save_delta_cursor(config.source_id, candidate)
                round_state = work_registry.complete_delta_round(
                    config.source_id, delta_execution_id, owner_instance_id,
                )
            return DeltaSyncOutcome(**(round_state.get("outcome") or {}))
    page_capture = (
        work_registry.delta_page_capture(source_plan, owner_instance_id, delta_execution_id)
        if work_registry is not None else None
    )
    try:
        if page_capture is None:
            delta = connector.read_drive_delta(config.delta_max_pages, delta_link=request_cursor)
        else:
            delta = connector.read_drive_delta(
                config.delta_max_pages, delta_link=request_cursor, page_capture=page_capture,
            )
    except DeltaResetRequired as reset:
        logger.warning("delta_cursor_invalidated, following explicit reset location")
        if page_capture is None:
            delta = connector.read_drive_delta(
                config.delta_max_pages,
                delta_link=reset.location,
            )
        else:
            delta = connector.read_drive_delta(
                config.delta_max_pages,
                delta_link=reset.location,
                page_capture=page_capture,
            )
        delta = replace(delta, is_full_baseline=True)
    if page_capture is not None and processing_phase == "items" and not enumerate_only:
        reduced_page = page_capture.list_reduced_items_page(
            page_size=reduced_items_page_size,
            continuation_token=reduced_items_continuation_token,
        )
        delta = replace(delta, items=list(reduced_page.items))
        reduced_items_continuation_token = reduced_page.continuation_token
    elif page_capture is not None:
        delta = replace(delta, items=[])
    else:
        reduced_items_continuation_token = None
    if processing_phase not in {"items", "baseline", "acl"}:
        raise ValueError("delta_processing_phase_invalid")
    if enumerate_only:
        if work_registry is None or page_capture is None or not delta.delta_link:
            raise RepositoryConflictError("full_sync_baseline_capture_required")
        observed_entries = page_capture.count_observed_entries()
        outcome = {
            "bootstrapped": True,
            "created_or_updated": 0,
            "deleted": 0,
            "acl_resynced": 0,
            "failed": 0,
            "pending": 0,
            "items_seen": observed_entries,
        }
        work_registry.mark_delta_round_cursor_ready(
            config.source_id,
            delta_execution_id,
            owner_instance_id,
            delta.delta_link,
            outcome,
        )
        return DeltaSyncOutcome(
            bootstrapped=True,
            items_seen=observed_entries,
            continuation_token=delta.delta_link,
            continuation_phase="baseline_complete",
        )
    run_id = (
        f"delta-{delta_execution_id}"
        if delta_execution_id is not None
        else create_run_id(_utc_now(), f"{config.source_id}:delta")
    )

    created_or_updated = 0
    deleted = 0
    acl_resynced = 0
    failed = 0
    pending = 0
    delta_source_run_id = create_source_run_id(config.source_id, run_id)

    def claim_delta_item(document_id: str, operation_kind: str) -> dict[str, Any]:
        if work_registry is None:
            return {"status": "untracked"}
        claim = work_registry.begin_document_operation(
            source_plan,
            owner_instance_id=owner_instance_id,
            physical_run_id=delta_source_run_id,
            item_id=document_id,
            operation_kind=operation_kind,
        )
        claim["operationKind"] = operation_kind
        if claim.get("status") != "acquired":
            return claim
        started = work_registry.start_document_operation(
            config.source_id,
            owner_instance_id,
            claim["operationId"],
            delta_source_run_id,
            document_id,
            operation_kind,
        )
        return claim if started.get("status") == "started" else started

    def settle_delta_item(
        claim: dict[str, Any], document_id: str, status: str, outcome: dict[str, Any],
    ) -> None:
        if claim.get("status") == "acquired":
            work_registry.settle_document_operation(
                config.source_id,
                owner_instance_id,
                claim["operationId"],
                physical_run_id=delta_source_run_id,
                item_id=document_id,
                operation_kind=claim["operationKind"],
                status="succeeded" if status == "completed" else status,
                outcome=outcome,
            )

    for ordinal, item in enumerate(delta.items):
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id:
            failed += 1
            continue
        document_id = create_document_id(config.source_id, config.drive_id, item_id)

        if item.get("deleted") is not None:
            claim = claim_delta_item(document_id, "delta-delete")
            if claim.get("status") == "completed":
                prior = claim.get("outcome") or {}
                deleted += int(prior.get("status") == "succeeded" and prior.get("deleted") is True)
                failed += int(prior.get("status") not in {"succeeded", "skipped"})
                continue
            if claim.get("status") not in {"acquired", "untracked"}:
                pending += 1
                continue
            try:
                versions = _iter_ready_document_versions(
                    lifecycle_repository, config.source_id, document_id,
                )
                removed = False
                cleanup_ok = True
                for ref in versions:
                    if (
                        ref.source_id != config.source_id
                        or ref.drive_id != config.drive_id
                        or ref.item_id != item_id
                        or _document_version_has_unsettled_mutation(ref)
                    ):
                        failed += 1
                        cleanup_ok = False
                        continue
                    try:
                        lifecycle_repository.delete_document_and_chunks(
                            source_run_id=ref.source_run_id,
                            document_id=document_id,
                            document_key=ref.document_key,
                            etag=ref.etag,
                        )
                    except LifecycleConflictError:
                        failed += 1
                        cleanup_ok = False
                    else:
                        removed = True
                        _clear_winner_if_points_to(
                            work_registry,
                            config.source_id,
                            owner_instance_id,
                            document_id,
                            ref.source_run_id,
                            ref.document_key,
                        )
                if removed:
                    deleted += 1
                    if audit_container is not None:
                        write_audit_record(audit_container, config.source_id, run_id, {
                            "operation": "document_deleted", "documentId": document_id,
                            "reason": "deleted", "method": "delta_sync",
                        })
                if cleanup_ok:
                    settle_delta_item(
                        claim, document_id, "completed",
                        {"documentId": document_id, "status": "succeeded", "deleted": removed},
                    )
            except Exception:
                logger.error("delta_sync delete failed for item %s", item_id, exc_info=True)
                failed += 1
            continue

        # Per-item permission change: Graph only surfaces this for file-level ACL edits;
        # library-level permission changes are caught by the zero-delta ACL resync path.
        if item.get("@microsoft.graph.sharedChanged") is True:
            ref = lifecycle_repository.find_ready_document_by_document_id(
                config.source_id, document_id,
            )
            if ref is None:
                ref = lifecycle_repository.find_acl_revoked_document_by_document_id(
                    config.source_id, document_id,
                )
            if ref is None:
                continue
            acl_claim = claim_delta_item(document_id, "delta-acl")
            if acl_claim.get("status") == "completed":
                prior = acl_claim.get("outcome") or {}
                result = prior.get("aclResult")
                if result not in {"updated", "retired", "unchanged"}:
                    failed += 1
                    continue
                acl_resynced += 1
            elif acl_claim.get("status") not in {"acquired", "untracked"}:
                pending += 1
                continue
            else:
                result = None
            try:
                old_groups = list(ref.allowed_group_ids)
                if result is None:
                    result = resync_document_acl(config, ref, lifecycle_repository, connector)
                    if result == "retired":
                        _clear_winner_if_points_to(
                            work_registry,
                            config.source_id,
                            owner_instance_id,
                            document_id,
                            ref.source_run_id,
                            ref.document_key,
                        )
                    settle_delta_item(
                        acl_claim, document_id, "completed",
                        {"documentId": document_id, "status": "succeeded", "aclResult": result},
                    )
                acl_resynced += 1
                if audit_container is not None:
                    write_audit_record(audit_container, config.source_id, run_id, {
                        "operation": "acl_resynced", "documentId": document_id,
                        "result": result, "method": "delta_sync",
                        "previousGroupIds": old_groups,
                    })
            except Exception:
                logger.error("delta_sync acl_resync failed for item %s", item_id, exc_info=True)
                failed += 1
                continue
            if result == "retired" or not _source_etag_changed(item, ref.source_etag):
                continue

        pending_document = lifecycle_repository.find_transcribing_document_by_document_id(
            config.source_id, document_id,
        )
        if pending_document is not None:
            pending += 1
            continue

        try:
            document = _delta_item_to_document(item, config, run_id, ordinal)
            if document is None:
                previous_versions = _iter_ready_document_versions(
                    lifecycle_repository, config.source_id, document_id,
                )
                first_previous = next(previous_versions, None)
                claim = claim_delta_item(document_id, "delta-cleanup") if first_previous is not None else {
                    "status": "untracked",
                }
                if claim.get("status") == "completed":
                    prior = claim.get("outcome") or {}
                    deleted += int(prior.get("status") == "succeeded")
                    failed += int(prior.get("status") != "succeeded")
                    continue
                if first_previous is not None and claim.get("status") not in {"acquired", "untracked"}:
                    pending += 1
                    continue
                cleanup_ok = True
                for previous in chain((first_previous,), previous_versions) if first_previous is not None else ():
                    if (
                        previous.source_id != config.source_id
                        or previous.drive_id != config.drive_id
                        or previous.item_id != item_id
                        or _document_version_has_unsettled_mutation(previous)
                    ):
                        failed += 1
                        cleanup_ok = False
                        continue
                    try:
                        lifecycle_repository.delete_document_and_chunks(
                            source_run_id=previous.source_run_id,
                            document_id=document_id,
                            document_key=previous.document_key,
                            etag=previous.etag,
                        )
                    except Exception:
                        failed += 1
                        cleanup_ok = False
                    else:
                        deleted += 1
                        _clear_winner_if_points_to(
                            work_registry,
                            config.source_id,
                            owner_instance_id,
                            document_id,
                            previous.source_run_id,
                            previous.document_key,
                        )
                if cleanup_ok:
                    settle_delta_item(
                        claim, document_id, "completed",
                        {"documentId": document_id, "status": "succeeded"},
                    )
                continue
            prev_ref = lifecycle_repository.find_ready_document_by_document_id(
                config.source_id, document_id,
            )
            if prev_ref is not None and prev_ref.source_etag == document.e_tag:
                if work_registry is None:
                    continue
                get_winner = getattr(work_registry, "get_document_winner", None)
                winner = get_winner(config.source_id, document_id) if callable(get_winner) else None
                if _winner_matches_ready_ref(
                    winner,
                    prev_ref,
                    source_id=config.source_id,
                    drive_id=config.drive_id,
                    source_etag=document.e_tag,
                    profile_digest=_processing_profile_digest(config),
                ):
                    acl_claim = claim_delta_item(document_id, "delta-acl")
                    if acl_claim.get("status") == "acquired":
                        current_item = connector.read_item(item_id)
                        if current_item is None or current_item.get("eTag") != document.e_tag:
                            raise RepositoryConflictError("winner_source_version_changed")
                        acl_result = resync_document_acl(
                            config, prev_ref, lifecycle_repository, connector,
                            audit_container=audit_container,
                        )
                        if acl_result == "retired":
                            _clear_winner_if_points_to(
                                work_registry,
                                config.source_id,
                                owner_instance_id,
                                document_id,
                                prev_ref.source_run_id,
                                prev_ref.document_key,
                            )
                            settle_delta_item(
                                acl_claim,
                                document_id,
                                "completed",
                                {"documentId": document_id, "status": "succeeded", "aclResult": "retired"},
                            )
                        else:
                            verified_ref = next((
                                ref for ref in _iter_ready_document_versions(
                                    lifecycle_repository, config.source_id, document_id,
                                )
                                if ref.source_run_id == winner.get("sourceRunId")
                                and ref.document_key == winner.get("documentKey")
                            ), None)
                            current_item = connector.read_item(item_id)
                            if (
                                verified_ref is None
                                or verified_ref.status is not DocumentStatus.READY
                                or verified_ref.source_id != config.source_id
                                or verified_ref.drive_id != config.drive_id
                                or verified_ref.item_id != item_id
                                or verified_ref.source_etag != document.e_tag
                                or verified_ref.processing_profile_digest != _processing_profile_digest(config)
                                or current_item is None
                                or current_item.get("eTag") != document.e_tag
                            ):
                                raise RepositoryConflictError("winner_reuse_verification_failed")
                            work_registry.record_document_winner(
                                config.source_id,
                                owner_instance_id,
                                document_id=verified_ref.document_id,
                                drive_id=verified_ref.drive_id,
                                item_id=verified_ref.item_id,
                                source_run_id=verified_ref.source_run_id,
                                document_key=verified_ref.document_key,
                                source_etag=verified_ref.source_etag,
                                lifecycle_generation=verified_ref.lifecycle_generation,
                                processing_profile_digest=verified_ref.processing_profile_digest,
                            )
                            cleanup_ok = True
                            for previous in _iter_ready_document_versions(
                                lifecycle_repository, config.source_id, document_id,
                            ):
                                if (
                                    previous.source_run_id == verified_ref.source_run_id
                                    and previous.document_key == verified_ref.document_key
                                ):
                                    continue
                                if (
                                    previous.source_id != config.source_id
                                    or previous.drive_id != config.drive_id
                                    or previous.item_id != item_id
                                    or _document_version_has_unsettled_mutation(previous)
                                ):
                                    cleanup_ok = False
                                    failed += 1
                                    continue
                                lifecycle_repository.delete_document_and_chunks(
                                    source_run_id=previous.source_run_id,
                                    document_id=document_id,
                                    document_key=previous.document_key,
                                    etag=previous.etag,
                                )
                            if cleanup_ok:
                                settle_delta_item(
                                    acl_claim,
                                    document_id,
                                    "completed",
                                    {"documentId": document_id, "status": "succeeded", "aclResult": acl_result},
                                )
                                acl_resynced += int(acl_result in {"updated", "retired"})
                                continue
                            pending += 1
                            continue
                    elif acl_claim.get("status") not in {"completed", "untracked"}:
                        pending += 1
                        continue
            operation_id = None
            if work_registry is not None:
                claim = work_registry.begin_document_operation(
                    source_plan,
                    owner_instance_id=owner_instance_id,
                    physical_run_id=document.source_run_id,
                    item_id=document.document_id,
                    operation_kind="delta-sync",
                )
                if claim.get("status") == "completed":
                    prior = claim.get("outcome") or {}
                    if prior.get("status") == "succeeded":
                        created_or_updated += 1
                    elif prior.get("status") == "pending":
                        pending += 1
                    else:
                        failed += 1
                    continue
                if claim.get("status") != "acquired":
                    pending += 1
                    continue
                operation_id = claim["operationId"]
                started = work_registry.start_document_operation(
                    config.source_id,
                    owner_instance_id,
                    operation_id,
                    document.source_run_id,
                    document.document_id,
                    "delta-sync",
                )
                if started.get("status") == "completed":
                    prior = started.get("outcome") or {}
                    if prior.get("status") == "succeeded":
                        created_or_updated += 1
                    elif prior.get("status") == "pending":
                        pending += 1
                    else:
                        failed += 1
                    continue
                if started.get("status") != "started":
                    pending += 1
                    continue
            stored = repository.create_discovered_document(document)
            outcome = process_document(
                config, stored.record, stored.etag, repository, lifecycle_repository,
                connector, di_client, language_client, openai_client,
                audit_container=audit_container,
                cu_client=cu_client,
                speech_token_provider=speech_token_provider,
                blob_service_client=blob_service_client,
                document_work_id=operation_id,
                document_work_attempt_id=operation_id,
            )
            if work_registry is not None and outcome.status is ActivityStatus.PENDING:
                work_registry.settle_document_operation(
                    config.source_id, owner_instance_id, operation_id,
                    physical_run_id=document.source_run_id,
                    item_id=document.document_id,
                    operation_kind="delta-sync",
                    status="unresolved",
                    outcome={"documentId": document.document_id, "status": "pending"},
                )
                pending += 1
                continue
            if work_registry is not None and outcome.status is not ActivityStatus.SUCCEEDED:
                work_registry.settle_document_operation(
                    config.source_id, owner_instance_id, operation_id,
                    physical_run_id=document.source_run_id,
                    item_id=document.document_id,
                    operation_kind="delta-sync",
                    status="failed",
                    outcome={"documentId": document.document_id, "status": "failed"},
                )
                failed += 1
                continue
        except Exception:
            logger.error("delta_sync item %s failed", item_id, exc_info=True)
            failed += 1
            continue

        if outcome.status is ActivityStatus.PENDING:
            pending += 1
            continue
        if outcome.status is not ActivityStatus.SUCCEEDED:
            failed += 1
            continue
        created_or_updated += 1
        if work_registry is not None:
            ready = repository.get_document(document.source_run_id, document.document_id)
            if (
                ready is None
                or ready.record.status is not DocumentStatus.READY
                or ready.record.stage is not DocumentStage.TERMINAL
            ):
                failed += 1
                continue
            work_registry.record_document_winner(
                config.source_id,
                owner_instance_id,
                document_id=ready.record.document_id,
                drive_id=ready.record.drive_id,
                item_id=ready.record.item_id,
                source_run_id=ready.record.source_run_id,
                document_key=ready.record.document_key,
                source_etag=ready.record.e_tag,
                lifecycle_generation=ready.record.lifecycle_generation,
                processing_profile_digest=ready.record.processing_profile_digest,
            )
        if audit_container is not None:
            write_audit_record(audit_container, config.source_id, run_id, {
                "operation": "document_ingested", "documentId": document_id,
                "sourceName": document.source_name, "sourceUrl": document.source_url,
                "method": "delta_sync", "action": "updated" if prev_ref else "created",
                "chunks": outcome.chunks_written,
            })
        try:
            cleanup_failed = False
            versions = _iter_ready_document_versions(
                lifecycle_repository, config.source_id, document_id,
            )
            for previous in versions:
                if previous.document_key == document.document_key:
                    continue
                if _document_version_has_unsettled_mutation(previous):
                    failed += 1
                    cleanup_failed = True
                    continue
                lifecycle_repository.delete_document_and_chunks(
                    source_run_id=previous.source_run_id,
                    document_id=document_id,
                    document_key=previous.document_key,
                    etag=previous.etag,
                )
                if audit_container is not None:
                    write_audit_record(audit_container, config.source_id, run_id, {
                        "operation": "document_deleted", "documentId": document_id,
                        "reason": "superseded", "method": "delta_sync",
                        "replacedDocumentKey": previous.document_key,
                        "newDocumentKey": document.document_key,
                        "sourceName": document.source_name,
                    })
        except LifecycleConflictError:
            failed += 1
            cleanup_failed = True
        except Exception:
            logger.exception("delta_sync version cleanup failed for item %s", item_id)
            failed += 1
            cleanup_failed = True
        if work_registry is not None and not cleanup_failed:
            work_registry.settle_document_operation(
                config.source_id, owner_instance_id, operation_id,
                physical_run_id=document.source_run_id,
                item_id=document.document_id,
                operation_kind="delta-sync",
                status="succeeded",
                outcome={"documentId": document.document_id, "status": "succeeded"},
            )

    observed_entries = (
        page_capture.count_observed_entries()
        if page_capture is not None
        else len(delta.items)
    )
    if reduced_items_continuation_token is not None and failed == 0 and pending == 0:
        return DeltaSyncOutcome(
            bootstrapped=request_cursor is None or delta.is_full_baseline,
            created_or_updated=created_or_updated,
            deleted=deleted,
            acl_resynced=acl_resynced,
            failed=failed,
            pending=pending,
            items_seen=observed_entries,
            continuation_token=reduced_items_continuation_token,
            continuation_phase="items",
        )

    if work_registry is None and delta.is_full_baseline and failed == 0 and pending == 0:
        continuation_token: str | None = None
        reconciled_documents: set[str] = set()
        try:
            while True:
                page = lifecycle_repository.list_ready_documents_page(
                    source_id=config.source_id,
                    page_size=50,
                    continuation_token=continuation_token,
                )
                for ref in page.items:
                    item_is_present = (
                        page_capture.has_reduced_item(ref.item_id)
                        if page_capture is not None
                        else any(item.get("id") == ref.item_id for item in delta.items)
                    )
                    if item_is_present or ref.document_id in reconciled_documents:
                        continue
                    reconciled_documents.add(ref.document_id)
                    if connector.read_item(ref.item_id) is not None:
                        continue
                    claim = claim_delta_item(ref.document_id, "delta-delete")
                    if claim.get("status") == "completed":
                        prior = claim.get("outcome") or {}
                        if prior.get("status") != "succeeded":
                            failed += 1
                            continue
                        deleted += int(prior.get("deleted", False))
                        continue
                    if claim.get("status") not in {"acquired", "untracked"}:
                        pending += 1
                        continue
                    versions = _iter_ready_document_versions(
                        lifecycle_repository, config.source_id, ref.document_id,
                    )
                    cleanup_ok = True
                    versions_found = False
                    for version in versions:
                        versions_found = True
                        if _document_version_has_unsettled_mutation(version):
                            failed += 1
                            cleanup_ok = False
                            break
                        try:
                            lifecycle_repository.delete_document_and_chunks(
                                source_run_id=version.source_run_id,
                                document_id=version.document_id,
                                document_key=version.document_key,
                                etag=version.etag,
                            )
                            _clear_winner_if_points_to(
                                work_registry,
                                config.source_id,
                                owner_instance_id,
                                version.document_id,
                                version.source_run_id,
                                version.document_key,
                            )
                        except Exception:
                            failed += 1
                            cleanup_ok = False
                            break
                    if cleanup_ok:
                        deleted += 1
                        settle_delta_item(
                            claim, ref.document_id, "completed",
                            {
                                "documentId": ref.document_id,
                                "status": "succeeded",
                                "deleted": versions_found,
                            },
                        )
                continuation_token = page.continuation_token
                if continuation_token is None:
                    break
        except Exception:
            logger.exception("delta_sync baseline absence reconciliation failed")
            failed += 1

    if work_registry is None and failed == 0 and pending == 0:
        continuation_token: str | None = None
        try:
            while True:
                acl_page, continuation_token = run_acl_resync_page(
                    config,
                    lifecycle_repository,
                    connector,
                    page_size=50,
                    continuation_token=continuation_token,
                    audit_container=audit_container,
                    work_registry=work_registry,
                    source_plan=source_plan,
                    owner_instance_id=owner_instance_id,
                    work_scope_id=delta_execution_id,
                )
                acl_resynced += acl_page.updated + acl_page.retired
                if continuation_token is None:
                    break
        except Exception:
            logger.exception("delta_sync full ACL reconciliation failed")
            failed += 1
    if work_registry is not None and failed == 0 and pending == 0:
        if processing_phase == "items":
            processing_phase = "baseline" if delta.is_full_baseline else "acl"
            phase_continuation_token = None
        if processing_phase == "baseline":
            try:
                page = lifecycle_repository.list_ready_documents_page(
                    source_id=config.source_id,
                    page_size=50,
                    continuation_token=phase_continuation_token,
                )
                for ref in page.items:
                    if (
                        page_capture is not None and page_capture.has_reduced_item(ref.item_id)
                    ) or ref.source_id != config.source_id or ref.drive_id != config.drive_id:
                        if ref.source_id != config.source_id or ref.drive_id != config.drive_id:
                            failed += 1
                        continue
                    if connector.read_item(ref.item_id) is not None:
                        continue
                    claim = claim_delta_item(ref.document_id, "delta-delete")
                    if claim.get("status") == "completed":
                        prior = claim.get("outcome") or {}
                        if prior.get("status") != "succeeded":
                            failed += 1
                        else:
                            deleted += int(prior.get("deleted", False))
                        continue
                    if claim.get("status") not in {"acquired", "untracked"}:
                        pending += 1
                        continue
                    cleanup_ok = True
                    versions_found = False
                    for version in _iter_ready_document_versions(
                        lifecycle_repository, config.source_id, ref.document_id,
                    ):
                        versions_found = True
                        if (
                            version.source_id != config.source_id
                            or version.drive_id != config.drive_id
                            or _document_version_has_unsettled_mutation(version)
                        ):
                            failed += 1
                            cleanup_ok = False
                            break
                        try:
                            lifecycle_repository.delete_document_and_chunks(
                                source_run_id=version.source_run_id,
                                document_id=version.document_id,
                                document_key=version.document_key,
                                etag=version.etag,
                            )
                        except Exception:
                            failed += 1
                            cleanup_ok = False
                            break
                    if cleanup_ok:
                        deleted += int(versions_found)
                        settle_delta_item(
                            claim,
                            ref.document_id,
                            "completed",
                            {
                                "documentId": ref.document_id,
                                "status": "succeeded",
                                "deleted": versions_found,
                            },
                        )
                phase_continuation_token = page.continuation_token
            except Exception:
                logger.exception("delta_sync baseline absence page failed")
                failed += 1
            if failed == 0 and pending == 0 and phase_continuation_token is not None:
                return DeltaSyncOutcome(
                    bootstrapped=True,
                    created_or_updated=created_or_updated,
                    deleted=deleted,
                    acl_resynced=acl_resynced,
                    failed=failed,
                    pending=pending,
                    items_seen=observed_entries,
                    continuation_token=phase_continuation_token,
                    continuation_phase="baseline",
                )
            if failed == 0 and pending == 0:
                processing_phase = "acl"
                phase_continuation_token = None
        if processing_phase == "acl" and failed == 0 and pending == 0:
            try:
                acl_page, phase_continuation_token = run_acl_resync_page(
                    config,
                    lifecycle_repository,
                    connector,
                    page_size=50,
                    continuation_token=phase_continuation_token,
                    audit_container=audit_container,
                    work_registry=work_registry,
                    source_plan=source_plan,
                    owner_instance_id=owner_instance_id,
                    work_scope_id=delta_execution_id,
                )
                acl_resynced += acl_page.updated + acl_page.retired
            except Exception:
                logger.exception("delta_sync ACL page failed")
                failed += 1
            if failed == 0 and pending == 0 and phase_continuation_token is not None:
                return DeltaSyncOutcome(
                    bootstrapped=request_cursor is None or delta.is_full_baseline,
                    created_or_updated=created_or_updated,
                    deleted=deleted,
                    acl_resynced=acl_resynced,
                    failed=failed,
                    pending=pending,
                    items_seen=observed_entries,
                    continuation_token=phase_continuation_token,
                    continuation_phase="acl",
                )
    if (
        failed == 0
        and pending == 0
        and work_registry is not None
        and work_registry.get_unresolved_document_operation_count(config.source_id) > 0
    ):
        pending += 1
    outcome = DeltaSyncOutcome(
        bootstrapped=request_cursor is None or delta.is_full_baseline,
        created_or_updated=created_or_updated,
        deleted=deleted,
        acl_resynced=acl_resynced,
        failed=failed,
        pending=pending,
        items_seen=observed_entries,
    )
    if failed == 0 and pending == 0:
        if work_registry is not None:
            work_registry.mark_delta_round_cursor_ready(
                config.source_id,
                delta_execution_id,
                owner_instance_id,
                delta.delta_link,
                {
                    "bootstrapped": outcome.bootstrapped,
                    "created_or_updated": outcome.created_or_updated,
                    "deleted": outcome.deleted,
                    "acl_resynced": outcome.acl_resynced,
                    "failed": outcome.failed,
                    "pending": outcome.pending,
                    "items_seen": outcome.items_seen,
                },
            )
        if defer_cursor_commit:
            return replace(
                outcome,
                continuation_token=delta.delta_link,
                continuation_phase="commit",
            )
        lifecycle_repository.save_delta_cursor(config.source_id, delta.delta_link)
        if work_registry is not None:
            work_registry.complete_delta_round(
                config.source_id, delta_execution_id, owner_instance_id,
            )
    return outcome


def _delta_item_to_document(
    item: dict[str, Any], config: IngestionConfig, run_id: str, ordinal: int
) -> SourceDocumentRecord | None:
    """Adapt one Graph delta driveItem into a DISCOVERED document shell, or None if the
    item is a folder/package or doesn't match the configured file extensions."""
    if item.get("folder") is not None or item.get("package") is not None:
        return None
    if item.get("file") is None:
        return None
    name = item.get("name")
    if not isinstance(name, str) or not any(
        name.lower().endswith(ext) for ext in config.allowed_extensions
    ):
        return None
    pdf = discovered_pdf_from_item(item, ordinal)
    return _source_to_document(pdf, config, run_id, ingestion_mode="delta-sync")


def _winner_matches_ready_ref(
    winner: Any,
    ref: ReadyDocumentRef,
    *,
    source_id: str,
    drive_id: str,
    source_etag: str,
    profile_digest: str,
) -> bool:
    return (
        isinstance(winner, dict)
        and ref.status is DocumentStatus.READY
        and ref.source_id == source_id
        and ref.drive_id == drive_id
        and ref.source_etag == source_etag
        and all((
            winner.get("recordType") == "document-winner",
            winner.get("sourceId") == source_id,
            winner.get("documentId") == ref.document_id,
            winner.get("driveId") == drive_id,
            winner.get("itemId") == ref.item_id,
            winner.get("sourceRunId") == ref.source_run_id,
            winner.get("documentKey") == ref.document_key,
            winner.get("sourceEtag") == source_etag,
            winner.get("lifecycleGeneration") == ref.lifecycle_generation,
            winner.get("processingProfileDigest") == profile_digest,
            ref.processing_profile_digest == profile_digest,
        ))
    )


def _clear_winner_if_points_to(
    work_registry: Any | None,
    source_id: str,
    owner_instance_id: str | None,
    document_id: str,
    source_run_id: str,
    document_key: str,
) -> bool:
    if work_registry is None or not owner_instance_id:
        return False
    get_winner = getattr(work_registry, "get_document_winner", None)
    clear_winner = getattr(work_registry, "clear_document_winner", None)
    if not callable(get_winner) or not callable(clear_winner):
        return False
    winner = get_winner(source_id, document_id)
    if (
        winner is None
        or winner.get("sourceRunId") != source_run_id
        or winner.get("documentKey") != document_key
    ):
        return False
    return clear_winner(
        source_id,
        owner_instance_id,
        document_id=document_id,
        expected_source_run_id=source_run_id,
        expected_document_key=document_key,
    )


def _document_version_has_unsettled_mutation(ref: ReadyDocumentRef) -> bool:
    return ref.status in {
        DocumentStatus.DISCOVERED,
        DocumentStatus.PROCESSING,
        DocumentStatus.ADMITTING,
        DocumentStatus.ACL_REFRESHING,
        DocumentStatus.RETIRING,
        DocumentStatus.DELETING,
    }


def _iter_ready_document_versions(
    lifecycle_repository: Any,
    source_id: str,
    document_id: str,
) -> Iterator[ReadyDocumentRef]:
    page_method = getattr(lifecycle_repository, "list_ready_document_versions_page", None)
    if not callable(page_method):
        yield from lifecycle_repository.list_ready_document_versions(source_id, document_id)
        return
    continuation_token: str | None = None
    while True:
        page = page_method(
            source_id=source_id,
            document_id=document_id,
            page_size=50,
            continuation_token=continuation_token,
        )
        yield from page.items
        continuation_token = page.continuation_token
        if continuation_token is None:
            return


def _source_etag_changed(item: dict[str, Any], persisted_etag: str | None) -> bool:
    source_etag = item.get("eTag")
    return isinstance(source_etag, str) and bool(source_etag) and source_etag != persisted_etag


@dataclass(frozen=True)
class AclResyncOutcome:
    checked: int = 0
    unchanged: int = 0
    updated: int = 0
    retired: int = 0


@dataclass(frozen=True)
class LifecycleReconciliationOutcome:
    checked: int = 0
    repaired: int = 0
    failed: int = 0


@dataclass(frozen=True)
class AudioWorkSettlement:
    source_run_id: str
    document_id: str
    work_id: str
    attempt_id: str
    status: str


@dataclass(frozen=True)
class AudioPollOutcome:
    checked: int = 0
    succeeded: int = 0
    failed: int = 0
    pending: int = 0
    skipped: int = 0
    terminal_work: tuple[AudioWorkSettlement, ...] = ()


def _claim_registered_item_work(
    work_registry: Any | None,
    source_plan: Any | None,
    owner_instance_id: str | None,
    work_scope_id: str | None,
    document_id: str,
    operation_kind: str,
) -> dict[str, Any]:
    if work_registry is None:
        return {"status": "untracked"}
    if source_plan is None or not owner_instance_id or not work_scope_id:
        raise ValueError("registered_item_work_context_required")
    source_run_id = create_source_run_id(source_plan.source_id, work_scope_id)
    claim = work_registry.begin_document_operation(
        source_plan,
        owner_instance_id=owner_instance_id,
        physical_run_id=source_run_id,
        item_id=document_id,
        operation_kind=operation_kind,
    )
    claim["operationKind"] = operation_kind
    if claim.get("status") != "acquired":
        return {
            **claim, "sourceId": source_plan.source_id,
            "workSourceRunId": source_run_id,
        }
    started = work_registry.start_document_operation(
        source_plan.source_id,
        owner_instance_id,
        claim["operationId"],
        source_run_id,
        document_id,
        operation_kind,
    )
    if started.get("status") == "started":
        claim_status = "acquired"
    else:
        claim_status = started.get("status")
    return {
        **claim,
        "sourceId": source_plan.source_id,
        "status": claim_status,
        "workSourceRunId": source_run_id,
        "operationId": claim.get("operationId"),
        "outcome": started.get("outcome", claim.get("outcome")),
    }


def _settle_registered_item_work(
    work_registry: Any | None,
    owner_instance_id: str | None,
    claim: dict[str, Any],
    document_id: str,
    status: str,
    outcome: dict[str, Any],
) -> None:
    if claim.get("status") == "acquired":
        work_registry.settle_document_operation(
            claim["sourceId"],
            owner_instance_id,
            claim["operationId"],
            physical_run_id=claim["workSourceRunId"],
            item_id=document_id,
            operation_kind=claim["operationKind"],
            status="succeeded" if status == "completed" else status,
            outcome=outcome,
        )


def run_audio_transcription_poll_page(
    config: IngestionConfig,
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    language_client: Any | None,
    openai_client: Any,
    *,
    speech_token_provider: Callable[[], str],
    blob_service_client: Any,
    page_size: int,
    continuation_token: str | None = None,
    audit_container: Any | None = None,
    settlement_only: bool = False,
    work_registry: Any | None = None,
    owner_instance_id: str | None = None,
) -> tuple[AudioPollOutcome, str | None]:
    """Poll one bounded page of documents awaiting batch transcription and advance each."""
    page = lifecycle_repository.list_transcribing_documents_page(
        source_id=config.source_id,
        page_size=page_size,
        continuation_token=continuation_token,
        settlement_only=settlement_only,
    )
    tally = {"succeeded": 0, "failed": 0, "pending": 0, "skipped": 0}
    terminal_work: list[AudioWorkSettlement] = []
    for ref in page.items:
        if ref.status in {DocumentStatus.READY, DocumentStatus.FAILED}:
            result = "succeeded" if ref.status is DocumentStatus.READY else "failed"
        elif ref.status is DocumentStatus.PROCESSING and ref.stage is DocumentStage.TRANSCRIBING:
            if work_registry is not None:
                if (
                    not ref.transcription_work_id
                    or not ref.transcription_attempt_id
                    or not owner_instance_id
                ):
                    tally["pending"] += 1
                    continue
                operation_kind = work_registry.get_document_operation_kind(
                    config.source_id,
                    ref.transcription_work_id,
                    ref.source_run_id,
                    ref.document_id,
                )
                work_registry.assert_document_operation_unresolved(
                    config.source_id,
                    owner_instance_id,
                    ref.transcription_work_id,
                    ref.source_run_id,
                    ref.document_id,
                    operation_kind,
                    allow_pending=True,
                )
            result = finalize_audio_transcription(
                config, ref.source_run_id, ref.document_id,
                repository, lifecycle_repository, connector, language_client, openai_client,
                speech_token_provider=speech_token_provider,
                blob_service_client=blob_service_client,
                audit_container=audit_container,
            )
        else:
            result = "pending"

        if result in {"succeeded", "failed"} and ref.transcription_work_id:
            terminal = repository.get_document(ref.source_run_id, ref.document_id)
            expected_status = (
                DocumentStatus.READY if result == "succeeded" else DocumentStatus.FAILED
            )
            if (
                terminal is None
                or terminal.record.status is not expected_status
                or terminal.record.stage is not DocumentStage.TERMINAL
                or terminal.record.transcription_work_id != ref.transcription_work_id
                or terminal.record.transcription_attempt_id != ref.transcription_attempt_id
            ):
                result = "pending"
        tally[result] = tally.get(result, 0) + 1
        if (
            result in {"succeeded", "failed"}
            and ref.transcription_work_id
            and ref.transcription_attempt_id
        ):
            terminal_work.append(AudioWorkSettlement(
                ref.source_run_id,
                ref.document_id,
                ref.transcription_work_id,
                ref.transcription_attempt_id,
                result,
            ))
    outcome = AudioPollOutcome(
        checked=len(page.items), succeeded=tally["succeeded"], failed=tally["failed"],
        pending=tally["pending"], skipped=tally["skipped"],
        terminal_work=tuple(terminal_work),
    )
    return outcome, page.continuation_token


def run_source_retirement_page(
    lifecycle_repository: DocumentLifecycleRepository,
    audit_container: Any,
    *,
    source_id: str,
    page_size: int,
    work_registry: Any,
    owner_instance_id: str,
) -> int:
    """Delete a bounded source page using the existing document-deletion and audit path."""
    page = lifecycle_repository.list_source_documents_for_deletion_page(
        source_id=source_id, page_size=page_size,
    )
    for ref in page.items:
        _clear_winner_if_points_to(
            work_registry, source_id, owner_instance_id,
            ref.document_id, ref.source_run_id, ref.document_key,
        )
        lifecycle_repository.delete_document_and_chunks(
            source_run_id=ref.source_run_id,
            document_id=ref.document_id,
            document_key=ref.document_key,
            etag=ref.etag,
            allow_unready=True,
        )
        write_audit_record(audit_container, source_id, owner_instance_id, {
            "operation": "document_deleted",
            "documentId": ref.document_id,
            "sourceRunId": ref.source_run_id,
            "documentKey": ref.document_key,
            "reason": "source_retired",
            "method": "source_retirement",
        })
    return len(page.items)


def run_lifecycle_reconciliation_page(
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    *,
    source_id: str,
    page_size: int,
    continuation_token: str | None = None,
    work_registry: Any | None = None,
    source_plan: Any | None = None,
    owner_instance_id: str | None = None,
    work_scope_id: str | None = None,
) -> tuple[LifecycleReconciliationOutcome, str | None]:
    page = lifecycle_repository.list_lifecycle_transitions_page(
        source_id=source_id,
        page_size=page_size,
        continuation_token=continuation_token,
    )
    repaired = 0
    failed = 0
    for ref in page.items:
        claim = _claim_registered_item_work(
            work_registry, source_plan, owner_instance_id, work_scope_id,
            ref.document_id, "lifecycle-reconcile",
        )
        if claim.get("status") == "completed":
            prior = claim.get("outcome") or {}
            if prior.get("status") == "succeeded":
                repaired += 1
            else:
                failed += 1
            continue
        if claim.get("status") not in {"acquired", "untracked"}:
            failed += 1
            continue
        try:
            _reconcile_lifecycle_document(repository, lifecycle_repository, ref)
            if ref.status in {DocumentStatus.RETIRING, DocumentStatus.DELETING}:
                _clear_winner_if_points_to(
                    work_registry,
                    source_id,
                    owner_instance_id,
                    ref.document_id,
                    ref.source_run_id,
                    ref.document_key,
                )
            repaired += 1
            _settle_registered_item_work(
                work_registry, owner_instance_id, claim, ref.document_id, "completed",
                {"documentId": ref.document_id, "status": "succeeded"},
            )
        except Exception:
            failed += 1
            logger.warning(
                "Lifecycle reconciliation failed for document %s",
                ref.document_id,
                exc_info=True,
            )
    return (
        LifecycleReconciliationOutcome(
            checked=len(page.items),
            repaired=repaired,
            failed=failed,
        ),
        page.continuation_token,
    )


def run_full_sync_absence_reconciliation_page(
    config: IngestionConfig,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    *,
    current_source_run_id: str,
    page_size: int,
    continuation_token: str | None,
    work_registry: Any,
    source_plan: Any,
    owner_instance_id: str,
) -> tuple[LifecycleReconciliationOutcome, str | None]:
    page = lifecycle_repository.list_ready_documents_page(
        source_id=config.source_id,
        page_size=page_size,
        continuation_token=continuation_token,
    )
    repaired = failed = 0
    seen_document_ids: set[str] = set()
    for ref in page.items:
        if ref.document_id in seen_document_ids or ref.source_run_id == current_source_run_id:
            continue
        seen_document_ids.add(ref.document_id)
        if ref.source_id != config.source_id or ref.drive_id != config.drive_id:
            failed += 1
            continue
        current_item = connector.read_item(ref.item_id)
        if current_item is not None and _is_current_source_item_supported(current_item, config.allowed_extensions):
            continue
        claim = work_registry.begin_document_operation(
            source_plan,
            owner_instance_id=owner_instance_id,
            physical_run_id=current_source_run_id,
            item_id=ref.document_id,
            operation_kind="delta-delete",
        )
        if claim.get("status") == "completed":
            prior = claim.get("outcome") or {}
            if prior.get("status") == "succeeded":
                repaired += int(prior.get("deleted", False))
            else:
                failed += 1
            continue
        if claim.get("status") != "acquired":
            failed += 1
            continue
        started = work_registry.start_document_operation(
            config.source_id,
            owner_instance_id,
            claim["operationId"],
            current_source_run_id,
            ref.document_id,
            "delta-delete",
        )
        if started.get("status") != "started":
            failed += 1
            continue
        cleanup_ok = True
        versions_found = False
        try:
            for version in _iter_ready_document_versions(
                lifecycle_repository, config.source_id, ref.document_id,
            ):
                versions_found = True
                if (
                    version.source_id != config.source_id
                    or version.drive_id != config.drive_id
                    or version.item_id != ref.item_id
                    or _document_version_has_unsettled_mutation(version)
                ):
                    raise RepositoryConflictError("full_sync_absence_version_identity_mismatch")
                lifecycle_repository.delete_document_and_chunks(
                    source_run_id=version.source_run_id,
                    document_id=version.document_id,
                    document_key=version.document_key,
                    etag=version.etag,
                )
                _clear_winner_if_points_to(
                    work_registry,
                    config.source_id,
                    owner_instance_id,
                    version.document_id,
                    version.source_run_id,
                    version.document_key,
                )
        except Exception:
            cleanup_ok = False
            failed += 1
            logger.warning(
                "Full-sync absence cleanup failed for document %s",
                ref.document_id,
                exc_info=True,
            )
        if cleanup_ok:
            work_registry.settle_document_operation(
                config.source_id,
                owner_instance_id,
                claim["operationId"],
                physical_run_id=current_source_run_id,
                item_id=ref.document_id,
                operation_kind="delta-delete",
                status="succeeded",
                outcome={
                    "documentId": ref.document_id,
                    "status": "succeeded",
                    "deleted": versions_found,
                },
            )
            repaired += int(versions_found)
    return (
        LifecycleReconciliationOutcome(checked=len(page.items), repaired=repaired, failed=failed),
        page.continuation_token,
    )


def _is_current_source_item_supported(
    item: dict[str, Any],
    allowed_extensions: tuple[str, ...],
) -> bool:
    if item.get("folder") is not None or item.get("package") is not None:
        return False
    file_facet = item.get("file")
    if file_facet is None:
        return False
    if not isinstance(file_facet, dict):
        raise RepositoryConflictError("full_sync_source_item_malformed")
    name = item.get("name")
    if not isinstance(name, str) or not any(name.lower().endswith(ext) for ext in allowed_extensions):
        return False
    try:
        resolve_source_format(name, file_facet.get("mimeType"))
    except TerminalDocumentError:
        return False
    return True


def run_duplicate_version_reconciliation_page(
    config: IngestionConfig,
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    *,
    page_size: int,
    continuation_token: str | None = None,
    work_registry: Any | None = None,
    source_plan: Any | None = None,
    owner_instance_id: str | None = None,
    work_scope_id: str | None = None,
) -> tuple[LifecycleReconciliationOutcome, str | None]:
    page = lifecycle_repository.list_duplicate_ready_document_ids_page(
        source_id=config.source_id,
        page_size=page_size,
        continuation_token=continuation_token,
    )
    control = repository.get_source_control(config.source_id)
    current_source_run_id = (
        create_source_run_id(config.source_id, control.record.current_run_id)
        if control is not None
        else None
    )
    repaired = 0
    failed = 0
    for document_id in page.document_ids:
        claim = _claim_registered_item_work(
            work_registry, source_plan, owner_instance_id, work_scope_id,
            document_id, "duplicate-reconcile",
        )
        if claim.get("status") == "completed":
            prior = claim.get("outcome") or {}
            if prior.get("status") in {"succeeded", "skipped"}:
                repaired += int(prior.get("deletedVersions", 0))
            else:
                failed += 1
            continue
        if claim.get("status") not in {"acquired", "untracked"}:
            failed += 1
            continue
        try:
            versions = _iter_ready_document_versions(
                lifecycle_repository, config.source_id, document_id,
            )
            if current_source_run_id is None or not any(
                ref.source_run_id == current_source_run_id for ref in versions
            ):
                _settle_registered_item_work(
                    work_registry, owner_instance_id, claim, document_id, "completed",
                    {"documentId": document_id, "status": "skipped", "deletedVersions": 0},
                )
                continue
            deleted_versions = 0
            for ref in _iter_ready_document_versions(
                lifecycle_repository, config.source_id, document_id,
            ):
                if ref.source_run_id == current_source_run_id:
                    continue
                lifecycle_repository.delete_document_and_chunks(
                    source_run_id=ref.source_run_id,
                    document_id=ref.document_id,
                    document_key=ref.document_key,
                    etag=ref.etag,
                )
                repaired += 1
                deleted_versions += 1
            _settle_registered_item_work(
                work_registry, owner_instance_id, claim, document_id, "completed",
                {
                    "documentId": document_id,
                    "status": "succeeded",
                    "deletedVersions": deleted_versions,
                },
            )
        except Exception:
            failed += 1
            logger.warning(
                "Duplicate-version reconciliation failed for document %s",
                document_id,
                exc_info=True,
            )
    return (
        LifecycleReconciliationOutcome(
            checked=len(page.document_ids),
            repaired=repaired,
            failed=failed,
        ),
        page.continuation_token,
    )


def run_orphan_chunk_reconciliation_page(
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    *,
    source_id: str,
    page_size: int,
    continuation_token: str | None = None,
    work_registry: Any | None = None,
    source_plan: Any | None = None,
    owner_instance_id: str | None = None,
    work_scope_id: str | None = None,
) -> tuple[LifecycleReconciliationOutcome, str | None]:
    page = lifecycle_repository.list_chunk_manifest_refs_page(
        source_id=source_id,
        page_size=page_size,
        continuation_token=continuation_token,
    )
    repaired = 0
    failed = 0
    for ref in page.items:
        try:
            manifest = repository.get_document(ref.source_run_id, ref.document_id)
            if manifest is None:
                work_document_id = f"orphan:{ref.document_key}:{ref.chunk_id}"
                claim = _claim_registered_item_work(
                    work_registry, source_plan, owner_instance_id, work_scope_id,
                    work_document_id, "orphan-reconcile",
                )
                if claim.get("status") == "completed":
                    prior = claim.get("outcome") or {}
                    if prior.get("status") == "succeeded":
                        repaired += 1
                    else:
                        failed += 1
                    continue
                if claim.get("status") not in {"acquired", "untracked"}:
                    failed += 1
                    continue
                lifecycle_repository.delete_orphan_chunk(
                    chunk_id=ref.chunk_id,
                    document_key=ref.document_key,
                )
                repaired += 1
                _settle_registered_item_work(
                    work_registry, owner_instance_id, claim, work_document_id, "completed",
                    {"documentId": ref.document_id, "chunkId": ref.chunk_id, "status": "succeeded"},
                )
        except Exception:
            failed += 1
            logger.warning(
                "Orphan-chunk reconciliation failed for chunk %s",
                ref.chunk_id,
                exc_info=True,
            )
    return (
        LifecycleReconciliationOutcome(
            checked=len(page.items),
            repaired=repaired,
            failed=failed,
        ),
        page.continuation_token,
    )


def _reconcile_lifecycle_document(
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    ref: LifecycleDocumentRef,
) -> None:
    if ref.status is DocumentStatus.ADMITTING:
        stored = repository.get_document(ref.source_run_id, ref.document_id)
        if stored is None or stored.record.status is not DocumentStatus.ADMITTING:
            raise RepositoryConflictError("admitting document changed during reconciliation")
        expected_count = stored.record.expected_chunk_count
        if expected_count is None:
            raise RepositoryConflictError("admitting document has no expected chunk count")
        lifecycle_repository.set_document_chunks_retrievable(
            document_key=stored.record.document_key,
            lifecycle_generation=stored.record.lifecycle_generation,
            is_retrievable=True,
            allowed_group_ids=stored.record.allowed_group_ids,
            expected_count=expected_count,
        )
        repository.verify_and_mark_document_ready(
            replace(
                stored.record,
                status=DocumentStatus.READY,
                stage=DocumentStage.TERMINAL,
                ready_at=_fmt(_utc_now()),
                updated_at=_fmt(_utc_now()),
            ),
            stored.etag,
        )
        return
    if ref.status is DocumentStatus.ACL_REFRESHING:
        if ref.pending_allowed_group_ids is None or not ref.pending_acl_hash:
            raise RepositoryConflictError("ACL refresh intent is incomplete")
        lifecycle_repository.refresh_document_acl(
            source_run_id=ref.source_run_id,
            document_id=ref.document_id,
            document_key=ref.document_key,
            etag=ref.etag,
            allowed_group_ids=ref.pending_allowed_group_ids,
            acl_hash=ref.pending_acl_hash,
        )
        return
    if ref.status is DocumentStatus.RETIRING:
        if ref.pending_retired_reason not in RETIRED_REASONS:
            raise RepositoryConflictError("retirement intent is incomplete")
        lifecycle_repository.retire_document(
            source_run_id=ref.source_run_id,
            document_id=ref.document_id,
            document_key=ref.document_key,
            etag=ref.etag,
            reason=ref.pending_retired_reason,
        )
        return
    if ref.status is DocumentStatus.DELETING:
        lifecycle_repository.delete_document_and_chunks(
            source_run_id=ref.source_run_id,
            document_id=ref.document_id,
            document_key=ref.document_key,
            etag=ref.etag,
        )
        return
    raise RepositoryConflictError("unsupported lifecycle transition")


def resync_document_acl(
    config: IngestionConfig,
    ref: ReadyDocumentRef,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    audit_container: Any | None = None,
) -> str:
    """Re-verify one ACL-eligible document. Returns unchanged, updated, or retired."""
    try:
        acl = connector.read_verified_acl(ref.item_id, config.acl_max_pages)
    except TerminalDocumentError:
        if ref.status is DocumentStatus.RETIRED:
            return "unchanged"
        try:
            lifecycle_repository.retire_document(
                source_run_id=ref.source_run_id,
                document_id=ref.document_id,
                document_key=ref.document_key,
                etag=ref.etag,
                reason="acl_revoked",
            )
        except LifecycleConflictError:
            raise
        else:
            if audit_container is not None:
                write_audit_record(audit_container, config.source_id, ref.source_run_id, {
                    "operation": "document_retired", "documentId": ref.document_id,
                    "retiredReason": "acl_revoked",
                    "sourceName": getattr(ref, "source_name", ""),
                    "sourceUrl": getattr(ref, "source_url", ""),
                })
        return "retired"

    if acl.acl_hash == ref.acl_hash and ref.status is DocumentStatus.READY:
        return "unchanged"
    if ref.status is DocumentStatus.RETIRED:
        source_item = connector.read_item(ref.item_id)
        source_etag = source_item.get("eTag") if source_item is not None else None
        if not isinstance(source_etag, str) or source_etag != ref.source_etag:
            return "unchanged"
        if not lifecycle_repository.is_authoritative_document_version(
            source_id=config.source_id,
            document_id=ref.document_id,
            source_run_id=ref.source_run_id,
            source_etag=source_etag,
        ):
            return "unchanged"
    try:
        lifecycle_repository.refresh_document_acl(
            source_run_id=ref.source_run_id,
            document_id=ref.document_id,
            document_key=ref.document_key,
            etag=ref.etag,
            allowed_group_ids=acl.allowed_group_ids,
            acl_hash=acl.acl_hash,
        )
    except LifecycleConflictError:
        raise
    return "updated"


def run_acl_resync_page(
    config: IngestionConfig,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    *,
    page_size: int,
    continuation_token: str | None,
    audit_container: Any | None = None,
    work_registry: Any | None = None,
    source_plan: Any | None = None,
    owner_instance_id: str | None = None,
    work_scope_id: str | None = None,
) -> tuple[AclResyncOutcome, str | None]:
    """Re-verify ACLs for one bounded page of ready documents (Durable-activity-sized)."""
    page = lifecycle_repository.list_ready_documents_page(
        source_id=config.source_id,
        page_size=page_size,
        continuation_token=continuation_token,
    )
    unchanged = updated = retired = 0
    for ref in page.items:
        claim = _claim_registered_item_work(
            work_registry, source_plan, owner_instance_id, work_scope_id,
            ref.document_id, "acl-resync",
        )
        if claim.get("status") == "completed":
            prior = claim.get("outcome") or {}
            result = prior.get("aclResult")
            if result == "unchanged":
                unchanged += 1
            elif result == "updated":
                updated += 1
            elif result == "retired":
                retired += 1
            else:
                raise RepositoryConflictError("ACL resync work replay outcome is invalid")
            continue
        if claim.get("status") not in {"acquired", "untracked"}:
            raise RepositoryConflictError("ACL resync work remains unresolved")
        result = resync_document_acl(
            config, ref, lifecycle_repository, connector, audit_container=audit_container,
        )
        if result == "retired":
            _clear_winner_if_points_to(
                work_registry,
                config.source_id,
                owner_instance_id,
                ref.document_id,
                ref.source_run_id,
                ref.document_key,
            )
        _settle_registered_item_work(
            work_registry, owner_instance_id, claim, ref.document_id, "completed",
            {"documentId": ref.document_id, "status": "succeeded", "aclResult": result},
        )
        if result == "unchanged":
            unchanged += 1
        elif result == "updated":
            updated += 1
        else:
            retired += 1
    outcome = AclResyncOutcome(
        checked=len(page.items), unchanged=unchanged, updated=updated, retired=retired
    )
    return outcome, page.continuation_token


def _fail_document(doc: SourceDocumentRecord, etag: str, error: BaseException, repository: IngestionRepository) -> None:
    try:
        if isinstance(error, TerminalDocumentError):
            safe = SafeError(str(error), doc.stage.value, False)
        else:
            safe = safe_error_from_exception(error, doc.stage.value)
        failed = replace(doc, status=DocumentStatus.FAILED, stage=DocumentStage.TERMINAL, failed_at=_fmt(_utc_now()), updated_at=_fmt(_utc_now()), error=safe)
        repository.mark_document_failed(failed, etag)
    except Exception:
        logger.warning("Failed to mark document as failed", exc_info=True)


def _build_searchable_text(content: str, key_phrases: tuple[str, ...], summary: str | None) -> str:
    """Concatenate content with enrichment data for embedding and full-text search."""
    parts = [content]
    if key_phrases:
        parts.append("Key terms: " + ", ".join(key_phrases))
    if summary:
        parts.append("Summary: " + summary)
    return "\n\n".join(parts)


def _read_and_validate_source_snapshot(
    connector: SourceConnector,
    document: SourceDocumentRecord,
    *,
    expected_snapshot: _SourceSnapshot | None = None,
) -> _SourceSnapshot:
    item = connector.read_item(document.item_id)
    if item is None:
        raise TerminalDocumentError("source_item_not_found")
    name = item.get("name")
    e_tag = item.get("eTag")
    size = item.get("size")
    file_metadata = item.get("file")
    graph_mime = (
        file_metadata.get("mimeType") if isinstance(file_metadata, dict) else None
    )
    if name != document.source_name:
        raise TerminalDocumentError("source_name_changed_during_processing")
    if e_tag != document.e_tag:
        raise TerminalDocumentError("source_etag_changed_during_processing")
    if size != document.size_bytes:
        raise TerminalDocumentError("source_size_changed_during_processing")
    source_format = resolve_source_format(name, graph_mime)
    normalized_mime = canonical_source_mime(name, graph_mime)
    snapshot = _SourceSnapshot(source_format, normalized_mime)
    if expected_snapshot is not None and source_format != expected_snapshot.source_format:
        raise TerminalDocumentError("source_format_changed_during_processing")
    if expected_snapshot is not None and normalized_mime != expected_snapshot.mime_type:
        raise TerminalDocumentError("source_mime_changed_during_processing")
    return snapshot


def _validate_resolved_image_snapshot(
    item: dict[str, Any] | None,
    expected: ResolvedMarkdownImage,
) -> None:
    if item is None:
        raise TerminalDocumentError("markdown_image_removed_during_processing")
    file_metadata = item.get("file")
    mime_type = (
        file_metadata.get("mimeType") if isinstance(file_metadata, dict) else None
    )
    if item.get("name") != expected.name:
        raise TerminalDocumentError("markdown_image_name_changed_during_processing")
    if item.get("eTag") != expected.e_tag:
        raise TerminalDocumentError("markdown_image_etag_changed_during_processing")
    if item.get("size") != expected.size_bytes:
        raise TerminalDocumentError("markdown_image_size_changed_during_processing")
    if not isinstance(mime_type, str) or mime_type.lower() != expected.mime_type:
        raise TerminalDocumentError("markdown_image_mime_changed_during_processing")


def _source_to_document(source: Any, config: IngestionConfig, run_id: str, ingestion_mode: str = "full-sync") -> SourceDocumentRecord:
    document_id = create_document_id(config.source_id, config.drive_id, source.item_id)
    now = _fmt(_utc_now())
    return SourceDocumentRecord(
        source_id=config.source_id, run_id=run_id, drive_id=config.drive_id,
        item_id=source.item_id, parent_item_id=source.parent_item_id,
        source_name=source.name, source_path=source.source_path,
        source_url=source.source_url, e_tag=source.e_tag,
        mime_type=source.mime_type, size_bytes=source.size_bytes,
        discovery_ordinal=source.discovery_ordinal,
        allowed_group_ids=("pending",), acl_hash=content_sha256("pending"),
        acl_evaluated_at=now,
        status=DocumentStatus.DISCOVERED, stage=DocumentStage.DISCOVERED,
        attempt_count=0, discovered_at=now, updated_at=now,
        source_modified_at=getattr(source, "last_modified_date_time", None),
        ingestion_mode=ingestion_mode,
        processing_profile_digest=_processing_profile_digest(config),
        id=document_id, document_id=document_id,
        source_run_id=create_source_run_id(config.source_id, run_id),
        document_key=create_document_key(config.source_id, run_id, document_id),
    )


def _processing_profile_digest(config: IngestionConfig) -> str:
    payload = {
        "profileVersion": 1,
        "settings": vars(config),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _build_chunk_records(
    document: SourceDocumentRecord, acl: Any, chunks: list[Chunk],
    searchable_texts: list[str], enrichments: list[dict], embeddings: list[tuple[float, ...]],
    now: str, *, audio: AudioMetadata | None = None,
) -> tuple[SearchChunkRecord, ...]:
    is_audio = audio is not None
    records: list[SearchChunkRecord] = []
    for i, chunk in enumerate(chunks):
        enrichment = enrichments[i]
        searchable = _build_searchable_text(chunk.content, enrichment["key_phrases"], enrichment["summary"])
        records.append(SearchChunkRecord(
            source_id=document.source_id, run_id=document.run_id,
            document_id=document.document_id, document_key=document.document_key,
            allowed_group_ids=acl.allowed_group_ids,
            source_name=document.source_name,
            source_url=document.source_url,
            page_start=None if is_audio else chunk.page_number,
            page_end=None if is_audio else chunk.page_number,
            section_path=_section_path(chunk.content),
            locator_kind=chunk.locator.kind,
            locator_label=chunk.locator.label,
            locator_ordinal_start=chunk.locator.ordinal_start,
            locator_ordinal_end=chunk.locator.ordinal_end,
            modalities=chunk.modalities,
            provenance=chunk.provenance,
            visual_coverage=chunk.visual_coverage,
            chunk_index=chunk.ordinal,
            created_at=now, content=chunk.content,
            content_hash=content_sha256(chunk.content),
            embedding_text=searchable,
            searchable_text=searchable,
            token_count=token_count(chunk.content),
            enrichment_status=enrichment["status"],
            summary=enrichment["summary"],
            key_phrases=enrichment["key_phrases"],
            entities=enrichment["entities"],
            language_code="en",
            embedding=embeddings[i],
            embedded_at=now,
            source_modified_at=document.source_modified_at,
            is_retrievable=False,
            lifecycle_generation=document.lifecycle_generation,
            id=create_chunk_id(chunk.ordinal),
            source_run_id=create_source_run_id(document.source_id, document.run_id),
            audio=audio,
            start_ms=chunk.locator.start_ms if is_audio else None,
            end_ms=chunk.locator.end_ms if is_audio else None,
        ))
    return tuple(records)


def _pages_from_extraction(extraction: CanonicalExtractionResult) -> list[Page]:
    return [
        Page(
            number=segment.locator.ordinal_start,
            text=segment.text,
            locator=segment.locator,
            modalities=segment.modalities,
            provenance=segment.provenance,
            visual_coverage=extraction.visual_coverage.status,
        )
        for segment in extraction.segments
    ]


def _section_path(content: str) -> tuple[str, ...]:
    headings: list[str] = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            heading = stripped.lstrip("#").strip()
            if heading:
                headings.append(heading[:200])
    return tuple(headings[:5])


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
