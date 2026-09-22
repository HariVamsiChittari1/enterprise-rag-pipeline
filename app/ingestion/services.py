"""Ingestion business logic: activate, discover, process, finalize."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable

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
) -> tuple[list[SourceDocumentRecord], int]:
    """Discover all eligible files from the source and persist as documents."""
    settings = Settings(source_id=config.source_id, drive_id=config.drive_id, code_version="durable-sync")
    state = DiscoveryState.initial()
    documents: list[SourceDocumentRecord] = []
    skipped = 0

    # Check previous run for skip-if-ready optimization
    control = repository.get_source_control(config.source_id)
    prev_run_id = control.record.last_completed_run_id if control else None
    prev_source_run_id = create_source_run_id(config.source_id, prev_run_id) if prev_run_id else None

    while not state.complete:
        step = connector.discover_next_page(
            state, settings.limits, allowed_extensions=config.allowed_extensions,
        )
        state = step.state
        for pdf in step.pdfs:
            doc_id = create_document_id(config.source_id, config.drive_id, pdf.item_id)
            if prev_source_run_id:
                try:
                    prev = repository.get_document(prev_source_run_id, doc_id)
                except Exception:
                    logger.warning("Skip-if-ready lookup failed for %s", doc_id, exc_info=True)
                    prev = None
                if prev and prev.record.status is DocumentStatus.READY and prev.record.e_tag == pdf.e_tag:
                    discovered_modified = getattr(pdf, "last_modified_date_time", None)
                    if discovered_modified is None or prev.record.source_modified_at == discovered_modified:
                        skipped += 1
                        continue
            doc = _source_to_document(pdf, config, run_id)
            stored = repository.create_discovered_document(doc)
            documents.append(stored.record)

    logger.info("Discovery complete: %d to process, %d skipped (unchanged), %d items scanned", len(documents), skipped, state.items_scanned)
    return documents, state.items_scanned


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
) -> ActivityOutcome:
    """Process a single document through the full pipeline."""
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
                    updated_at=_fmt(_utc_now()),
                )
                repository.update_processing_document(submitted, current_etag)
                # Submit succeeded; the poller finalizes this doc once the batch job completes.
                return ActivityOutcome(
                    document_id=document.document_id, status=ActivityStatus.SUCCEEDED,
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
    items_seen: int = 0


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
) -> DeltaSyncOutcome:
    """One delta-sync tick: process adds/updates/deletes for source_id since the last
    cursor. Uses its own run_id per tick purely as a schema-compliant namespacing device
    (sourceRunId/documentKey); it does not touch full-sync's source-control singleton."""
    cursor = lifecycle_repository.get_delta_cursor(config.source_id)
    if cursor is None:
        bootstrap_link = connector.bootstrap_delta_cursor()
        lifecycle_repository.save_delta_cursor(config.source_id, bootstrap_link)
        return DeltaSyncOutcome(bootstrapped=True)

    try:
        delta = connector.read_drive_delta(config.delta_max_pages, delta_link=cursor)
    except DeltaResetRequired:
        # Cursor permanently invalidated; re-bootstrap per MS Graph guidance
        logger.warning("delta_cursor_invalidated, re-bootstrapping")
        bootstrap_link = connector.bootstrap_delta_cursor()
        lifecycle_repository.save_delta_cursor(config.source_id, bootstrap_link)
        return DeltaSyncOutcome(bootstrapped=True)
    run_id = create_run_id(_utc_now(), f"{config.source_id}:delta")

    created_or_updated = 0
    deleted = 0
    acl_resynced = 0
    failed = 0
    for ordinal, item in enumerate(delta.items):
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id:
            failed += 1
            continue
        document_id = create_document_id(config.source_id, config.drive_id, item_id)

        if item.get("deleted") is not None:
            try:
                ref = lifecycle_repository.find_ready_document_by_document_id(document_id)
                if ref is not None:
                    try:
                        lifecycle_repository.delete_document_and_chunks(
                            source_run_id=ref.source_run_id,
                            document_id=document_id,
                            document_key=ref.document_key,
                            etag=ref.etag,
                        )
                    except LifecycleConflictError:
                        pass  # already deleted/changed concurrently -- acceptable no-op
                    else:
                        deleted += 1
                        if audit_container is not None:
                            write_audit_record(audit_container, config.source_id, run_id, {
                                "operation": "document_deleted", "documentId": document_id,
                                "reason": "deleted",
                                "sourceName": getattr(ref, "source_name", ""),
                                "sourceUrl": getattr(ref, "source_url", ""),
                                "method": "delta_sync",
                            })
            except Exception:
                logger.error("delta_sync delete failed for item %s", item_id, exc_info=True)
                failed += 1
            continue

        # Per-item permission change: Graph only surfaces this for file-level ACL edits;
        # library-level permission changes are caught by the zero-delta ACL resync path.
        if item.get("@microsoft.graph.sharedChanged") is True:
            ref = lifecycle_repository.find_ready_document_by_document_id(document_id)
            if ref is None:
                ref = lifecycle_repository.find_acl_revoked_document_by_document_id(
                    document_id
                )
            if ref is None:
                continue
            try:
                old_groups = list(ref.allowed_group_ids)
                result = resync_document_acl(config, ref, lifecycle_repository, connector)
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

        try:
            document = _delta_item_to_document(item, config, run_id, ordinal)
            if document is None:
                continue
            prev_ref = lifecycle_repository.find_ready_document_by_document_id(document_id)
            stored = repository.create_discovered_document(document)
            outcome = process_document(
                config, stored.record, stored.etag, repository, lifecycle_repository,
                connector, di_client, language_client, openai_client,
                audit_container=audit_container,
                cu_client=cu_client,
                speech_token_provider=speech_token_provider,
                blob_service_client=blob_service_client,
            )
        except Exception:
            logger.error("delta_sync item %s failed", item_id, exc_info=True)
            failed += 1
            continue

        if outcome.status is not ActivityStatus.SUCCEEDED:
            failed += 1
            continue
        created_or_updated += 1
        if audit_container is not None:
            write_audit_record(audit_container, config.source_id, run_id, {
                "operation": "document_ingested", "documentId": document_id,
                "sourceName": document.source_name, "sourceUrl": document.source_url,
                "method": "delta_sync", "action": "updated" if prev_ref else "created",
                "chunks": outcome.chunks_written,
            })
        if prev_ref is not None and prev_ref.document_key != document.document_key:
            try:
                lifecycle_repository.delete_document_and_chunks(
                    source_run_id=prev_ref.source_run_id,
                    document_id=document_id,
                    document_key=prev_ref.document_key,
                    etag=prev_ref.etag,
                )
            except LifecycleConflictError:
                pass  # already deleted/changed concurrently (e.g. ACL resync) -- acceptable no-op
            else:
                if audit_container is not None:
                    write_audit_record(audit_container, config.source_id, run_id, {
                        "operation": "document_deleted", "documentId": document_id,
                        "reason": "superseded", "method": "delta_sync",
                        "replacedDocumentKey": prev_ref.document_key,
                        "newDocumentKey": document.document_key,
                        "sourceName": document.source_name,
                    })

    if failed == 0:
        lifecycle_repository.save_delta_cursor(config.source_id, delta.delta_link)
    return DeltaSyncOutcome(
        created_or_updated=created_or_updated,
        deleted=deleted,
        acl_resynced=acl_resynced,
        failed=failed,
        items_seen=len(delta.items),
    )


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
class AudioPollOutcome:
    checked: int = 0
    succeeded: int = 0
    failed: int = 0
    pending: int = 0
    skipped: int = 0


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
) -> tuple[AudioPollOutcome, str | None]:
    """Poll one bounded page of documents awaiting batch transcription and advance each."""
    page = lifecycle_repository.list_transcribing_documents_page(
        page_size=page_size, continuation_token=continuation_token,
    )
    tally = {"succeeded": 0, "failed": 0, "pending": 0, "skipped": 0}
    for ref in page.items:
        result = finalize_audio_transcription(
            config, ref.source_run_id, ref.document_id,
            repository, lifecycle_repository, connector, language_client, openai_client,
            speech_token_provider=speech_token_provider,
            blob_service_client=blob_service_client,
            audit_container=audit_container,
        )
        tally[result] = tally.get(result, 0) + 1
    outcome = AudioPollOutcome(
        checked=len(page.items), succeeded=tally["succeeded"], failed=tally["failed"],
        pending=tally["pending"], skipped=tally["skipped"],
    )
    return outcome, page.continuation_token


def run_lifecycle_reconciliation_page(
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    *,
    page_size: int,
    continuation_token: str | None = None,
) -> tuple[LifecycleReconciliationOutcome, str | None]:
    page = lifecycle_repository.list_lifecycle_transitions_page(
        page_size=page_size,
        continuation_token=continuation_token,
    )
    repaired = 0
    failed = 0
    for ref in page.items:
        try:
            _reconcile_lifecycle_document(repository, lifecycle_repository, ref)
            repaired += 1
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


def run_duplicate_version_reconciliation_page(
    config: IngestionConfig,
    repository: IngestionRepository,
    lifecycle_repository: DocumentLifecycleRepository,
    *,
    page_size: int,
    continuation_token: str | None = None,
) -> tuple[LifecycleReconciliationOutcome, str | None]:
    page = lifecycle_repository.list_duplicate_ready_document_ids_page(
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
        try:
            versions = lifecycle_repository.list_ready_document_versions(document_id)
            if current_source_run_id is None or not any(
                ref.source_run_id == current_source_run_id for ref in versions
            ):
                continue
            for ref in versions:
                if ref.source_run_id == current_source_run_id:
                    continue
                lifecycle_repository.delete_document_and_chunks(
                    source_run_id=ref.source_run_id,
                    document_id=ref.document_id,
                    document_key=ref.document_key,
                    etag=ref.etag,
                )
                repaired += 1
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
    page_size: int,
    continuation_token: str | None = None,
) -> tuple[LifecycleReconciliationOutcome, str | None]:
    page = lifecycle_repository.list_chunk_manifest_refs_page(
        page_size=page_size,
        continuation_token=continuation_token,
    )
    repaired = 0
    failed = 0
    for ref in page.items:
        try:
            manifest = repository.get_document(ref.source_run_id, ref.document_id)
            if manifest is None:
                lifecycle_repository.delete_orphan_chunk(
                    chunk_id=ref.chunk_id,
                    document_key=ref.document_key,
                )
                repaired += 1
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
            pass
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
        return "unchanged"
    return "updated"


def run_acl_resync_page(
    config: IngestionConfig,
    lifecycle_repository: DocumentLifecycleRepository,
    connector: SourceConnector,
    *,
    page_size: int,
    continuation_token: str | None,
    audit_container: Any | None = None,
) -> tuple[AclResyncOutcome, str | None]:
    """Re-verify ACLs for one bounded page of ready documents (Durable-activity-sized)."""
    page = lifecycle_repository.list_ready_documents_page(
        page_size=page_size, continuation_token=continuation_token
    )
    unchanged = updated = retired = 0
    for ref in page.items:
        result = resync_document_acl(config, ref, lifecycle_repository, connector, audit_container=audit_container)
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
        id=document_id, document_id=document_id,
        source_run_id=create_source_run_id(config.source_id, run_id),
        document_key=create_document_key(config.source_id, run_id, document_id),
    )


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
