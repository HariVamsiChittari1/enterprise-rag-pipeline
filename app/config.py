"""Centralized configuration loaded from environment variables."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from enum import Enum
from math import isfinite
from urllib.parse import urlsplit

from ingestion.models import SourceAdmissionConfig


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise EnvironmentError(f"Required environment variable {name} is not set")
    return value


def _bool(name: str, default: bool = True) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    return value in ("true", "1", "yes")


def _int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer") from None


def _float(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        parsed = float(value)
    except ValueError:
        raise ValueError(f"{name} must be a number") from None
    if not isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


class ExtractionProvider(str, Enum):
    DOCUMENT_INTELLIGENCE = "document_intelligence"
    CONTENT_UNDERSTANDING = "content_understanding"


def _select_extraction_provider(
    extraction_enabled: bool,
    document_intelligence_enabled: bool,
    content_understanding_enabled: bool,
) -> ExtractionProvider | None:
    if not extraction_enabled:
        return None
    if content_understanding_enabled:
        return ExtractionProvider.CONTENT_UNDERSTANDING
    if document_intelligence_enabled:
        return ExtractionProvider.DOCUMENT_INTELLIGENCE
    raise EnvironmentError(
        "EXTRACTION_ENABLED requires at least one extraction provider"
    )


@dataclass(frozen=True)
class ExecutionBatchConfig:
    acl_max_pages: int = 10
    download_timeout_seconds: float = 120.0
    delta_max_pages: int = 200
    embedding_batch_size: int = 100
    speech_request_timeout_seconds: float = 120.0
    audio_batch_ttl_hours: int = 48
    wave_size: int = 4
    wave_timeout_minutes: int = 20
    process_document_max_attempts: int = 5
    process_document_retry_delay_seconds: int = 60
    acl_resync_page_size: int = 50
    lifecycle_reconcile_page_size: int = 50
    audio_poll_page_size: int = 20
    source_dispatch_page_size: int = 10

    def __post_init__(self) -> None:
        for name, value in (
            ("acl_max_pages", self.acl_max_pages),
            ("delta_max_pages", self.delta_max_pages),
            ("embedding_batch_size", self.embedding_batch_size),
            ("wave_size", self.wave_size),
            ("wave_timeout_minutes", self.wave_timeout_minutes),
            ("process_document_max_attempts", self.process_document_max_attempts),
            (
                "process_document_retry_delay_seconds",
                self.process_document_retry_delay_seconds,
            ),
            ("acl_resync_page_size", self.acl_resync_page_size),
            (
                "lifecycle_reconcile_page_size",
                self.lifecycle_reconcile_page_size,
            ),
            ("audio_poll_page_size", self.audio_poll_page_size),
            ("source_dispatch_page_size", self.source_dispatch_page_size),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name, value in (
            ("download_timeout_seconds", self.download_timeout_seconds),
            ("speech_request_timeout_seconds", self.speech_request_timeout_seconds),
        ):
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a finite positive number")
        if type(self.audio_batch_ttl_hours) is not int or not (
            6 <= self.audio_batch_ttl_hours <= 744
        ):
            raise ValueError("AUDIO_BATCH_TTL_HOURS must be between 6 and 744")


@dataclass(frozen=True)
class IngestionConfig:
    extraction_enabled: bool
    enrichment_enabled: bool
    summary_enabled: bool
    key_phrases_enabled: bool
    entities_enabled: bool
    allowed_extensions: tuple[str, ...]

    # SharePoint / Graph
    source_id: str
    drive_id: str
    tenant_id: str
    app_client_id: str
    certificate_secret_name: str
    key_vault_uri: str

    # Azure services
    cosmos_endpoint: str
    cosmos_database: str
    cosmos_ingestion_runs_container: str
    cosmos_source_documents_container: str
    cosmos_search_chunks_container: str
    document_intelligence_endpoint: str
    content_understanding_endpoint: str
    content_understanding_analyzer_id: str
    language_endpoint: str
    openai_endpoint: str
    embedding_deployment: str
    vision_deployment: str
    managed_identity_client_id: str

    admission: SourceAdmissionConfig
    execution: ExecutionBatchConfig

    # Specialized algorithm tuning
    chunk_max_tokens: int
    chunk_overlap_tokens: int
    vision_max_output_tokens: int
    sharepoint_site_url: str
    cosmos_ingestion_sources_container: str = "ingestion-sources"
    sharepoint_site_id: str | None = None
    business_name: str | None = None
    document_intelligence_enabled: bool = True
    content_understanding_enabled: bool = False
    audio_writer_enabled: bool = False
    audio_transcription_provider: str = "speech_fast"
    speech_endpoint: str = ""
    speech_region: str = ""
    audio_deployment_region: str = ""
    audio_locale: str = ""
    audio_max_response_bytes: int = 16 * 1024 * 1024
    audio_staging_blob_endpoint: str = ""
    audio_staging_container: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.admission, SourceAdmissionConfig):
            raise TypeError("admission must be SourceAdmissionConfig")
        if not isinstance(self.execution, ExecutionBatchConfig):
            raise TypeError("execution must be ExecutionBatchConfig")
        if (
            not isinstance(self.embedding_deployment, str)
            or not self.embedding_deployment.strip()
        ):
            raise ValueError("OPENAI_EMBEDDING_DEPLOYMENT_NAME must be a non-empty string")
        if type(self.audio_writer_enabled) is not bool:
            raise ValueError("AUDIO_WRITER_ENABLED must be true or false")
        if self.audio_transcription_provider not in ("speech_fast", "speech_batch"):
            raise ValueError("AUDIO_TRANSCRIPTION_PROVIDER must be speech_fast or speech_batch")
        if self.audio_writer_enabled and not self.extraction_enabled:
            raise ValueError("AUDIO_WRITER_ENABLED requires EXTRACTION_ENABLED")
        for name, value in (
            ("SPEECH_ENDPOINT", self.speech_endpoint),
            ("SPEECH_REGION", self.speech_region),
            ("AUDIO_DEPLOYMENT_REGION", self.audio_deployment_region),
            ("AUDIO_LOCALE", self.audio_locale),
        ):
            if not isinstance(value, str):
                raise ValueError(f"{name} must be a string")
            if self.audio_writer_enabled and not value:
                raise ValueError(f"AUDIO_WRITER_ENABLED requires {name}")
        if self.speech_endpoint:
            try:
                endpoint = urlsplit(self.speech_endpoint)
            except ValueError:
                raise ValueError("SPEECH_ENDPOINT must be an HTTPS custom-domain base URL") from None
            if (
                any(character.isspace() or ord(character) < 32 for character in self.speech_endpoint)
                or endpoint.scheme != "https"
                or endpoint.netloc != endpoint.hostname
                or not re.fullmatch(
                    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.cognitiveservices\.azure\.com",
                    endpoint.hostname or "",
                )
                or endpoint.path not in ("", "/")
                or "?" in self.speech_endpoint
                or "#" in self.speech_endpoint
            ):
                raise ValueError("SPEECH_ENDPOINT must be an HTTPS custom-domain base URL")
        for name, region in (
            ("SPEECH_REGION", self.speech_region),
            ("AUDIO_DEPLOYMENT_REGION", self.audio_deployment_region),
        ):
            if region and region not in ("eastus2", "centralindia", "southeastasia"):
                raise ValueError(f"{name} is outside the initial audio region allowlist")
        if self.speech_region and self.audio_deployment_region and self.speech_region != self.audio_deployment_region:
            raise ValueError("SPEECH_REGION must match AUDIO_DEPLOYMENT_REGION")
        if self.audio_locale and self.audio_locale not in ("en-US", "en-GB", "en-IN"):
            raise ValueError("AUDIO_LOCALE must be en-US, en-GB, or en-IN")
        if type(self.audio_max_response_bytes) is not int or self.audio_max_response_bytes <= 0:
            raise ValueError("AUDIO_MAX_RESPONSE_BYTES must be a positive integer")
        # Batch transcription stages source audio in a dedicated blob container Speech can read.
        if self.audio_writer_enabled and self.audio_transcription_provider == "speech_batch":
            for name, value in (
                ("AUDIO_STAGING_BLOB_ENDPOINT", self.audio_staging_blob_endpoint),
                ("AUDIO_STAGING_CONTAINER", self.audio_staging_container),
            ):
                if not value:
                    raise ValueError(f"AUDIO_TRANSCRIPTION_PROVIDER=speech_batch requires {name}")
            staging = urlsplit(self.audio_staging_blob_endpoint)
            if (
                any(character.isspace() or ord(character) < 32 for character in self.audio_staging_blob_endpoint)
                or staging.scheme != "https"
                or staging.netloc != staging.hostname
                or not (staging.hostname or "").endswith(".blob.core.windows.net")
            ):
                raise ValueError("AUDIO_STAGING_BLOB_ENDPOINT must be an HTTPS blob endpoint URL")

    @property
    def extraction_provider(self) -> ExtractionProvider | None:
        return _select_extraction_provider(
            self.extraction_enabled,
            self.document_intelligence_enabled,
            self.content_understanding_enabled,
        )


def load_execution_batch_config() -> ExecutionBatchConfig:
    defaults = ExecutionBatchConfig()
    return ExecutionBatchConfig(
        acl_max_pages=_int("ACL_MAX_PAGES", defaults.acl_max_pages),
        download_timeout_seconds=_float(
            "DOWNLOAD_TIMEOUT_SECONDS",
            defaults.download_timeout_seconds,
        ),
        delta_max_pages=_int("DELTA_MAX_PAGES", defaults.delta_max_pages),
        embedding_batch_size=_int(
            "EMBEDDING_BATCH_SIZE",
            defaults.embedding_batch_size,
        ),
        speech_request_timeout_seconds=_float(
            "SPEECH_REQUEST_TIMEOUT_SECONDS",
            defaults.speech_request_timeout_seconds,
        ),
        audio_batch_ttl_hours=_int(
            "AUDIO_BATCH_TTL_HOURS",
            defaults.audio_batch_ttl_hours,
        ),
        wave_size=_int("WAVE_SIZE", defaults.wave_size),
        wave_timeout_minutes=_int(
            "WAVE_TIMEOUT_MINUTES",
            defaults.wave_timeout_minutes,
        ),
        process_document_max_attempts=_int(
            "PROCESS_DOCUMENT_MAX_ATTEMPTS",
            defaults.process_document_max_attempts,
        ),
        process_document_retry_delay_seconds=_int(
            "PROCESS_DOCUMENT_RETRY_DELAY_SECONDS",
            defaults.process_document_retry_delay_seconds,
        ),
        acl_resync_page_size=_int(
            "ACL_RESYNC_PAGE_SIZE",
            defaults.acl_resync_page_size,
        ),
        lifecycle_reconcile_page_size=_int(
            "LIFECYCLE_RECONCILE_PAGE_SIZE",
            defaults.lifecycle_reconcile_page_size,
        ),
        audio_poll_page_size=_int(
            "AUDIO_POLL_PAGE_SIZE",
            defaults.audio_poll_page_size,
        ),
        source_dispatch_page_size=_int(
            "SOURCE_DISPATCH_PAGE_SIZE",
            defaults.source_dispatch_page_size,
        ),
    )


def load_config() -> IngestionConfig:
    admission_defaults = SourceAdmissionConfig()
    audio_enabled_raw = os.getenv("AUDIO_WRITER_ENABLED", "false").strip().lower()
    if audio_enabled_raw not in ("true", "false"):
        raise ValueError("AUDIO_WRITER_ENABLED must be true or false")
    extensions_raw = os.getenv("ALLOWED_FILE_EXTENSIONS", ".pdf")
    extensions = tuple(ext.strip().lower() for ext in extensions_raw.split(",") if ext.strip())

    extraction = _bool("EXTRACTION_ENABLED", True)
    document_intelligence_enabled = _bool("DOCUMENT_INTELLIGENCE_ENABLED", True)
    content_understanding_enabled = _bool("CONTENT_UNDERSTANDING_ENABLED", False)
    summary = _bool("SUMMARY_ENABLED", False)
    key_phrases = _bool("KEY_PHRASES_ENABLED", True)
    entities = _bool("ENTITIES_ENABLED", True)
    any_enrichment = summary or key_phrases or entities
    selected_provider = _select_extraction_provider(
        extraction,
        document_intelligence_enabled,
        content_understanding_enabled,
    )

    return IngestionConfig(
        extraction_enabled=extraction,
        enrichment_enabled=any_enrichment,
        summary_enabled=summary and any_enrichment,
        key_phrases_enabled=key_phrases and any_enrichment,
        entities_enabled=entities and any_enrichment,
        allowed_extensions=extensions,
        source_id=os.getenv("INGESTION_SOURCE_ID", "").strip(),
        drive_id=os.getenv("SHAREPOINT_ASSIGNED_DRIVE_ID", "").strip(),
        tenant_id=_required("SHAREPOINT_TENANT_ID"),
        app_client_id=_required("SHAREPOINT_APP_CLIENT_ID"),
        certificate_secret_name=os.getenv("SHAREPOINT_CERTIFICATE_SECRET_NAME", "sharepoint-app-cert"),
        key_vault_uri=_required("KEY_VAULT_URI"),
        cosmos_endpoint=_required("COSMOS_ENDPOINT"),
        cosmos_database=_required("COSMOS_DATABASE_NAME"),
        cosmos_ingestion_runs_container=os.getenv("COSMOS_INGESTION_RUNS_CONTAINER_NAME", "ingestion-runs"),
        cosmos_source_documents_container="",
        cosmos_search_chunks_container="",
        document_intelligence_endpoint=(
            _required("DOCUMENT_INTELLIGENCE_ENDPOINT")
            if selected_provider is ExtractionProvider.DOCUMENT_INTELLIGENCE
            else ""
        ),
        content_understanding_endpoint=(
            _required("CONTENT_UNDERSTANDING_ENDPOINT")
            if selected_provider is ExtractionProvider.CONTENT_UNDERSTANDING
            else ""
        ),
        content_understanding_analyzer_id=(
            _required("CONTENT_UNDERSTANDING_ANALYZER_ID")
            if selected_provider is ExtractionProvider.CONTENT_UNDERSTANDING
            else ""
        ),
        language_endpoint=_required("AZURE_LANGUAGE_ENDPOINT") if any_enrichment else "",
        openai_endpoint=_required("OPENAI_ENDPOINT"),
        embedding_deployment=_required("OPENAI_EMBEDDING_DEPLOYMENT_NAME"),
        vision_deployment=_required("OPENAI_CHAT_DEPLOYMENT_NAME") if extraction else "",
        managed_identity_client_id=os.getenv("AZURE_CLIENT_ID", ""),
        admission=SourceAdmissionConfig(
            max_eligible_sources=_int(
                "MAX_ELIGIBLE_SOURCES", admission_defaults.max_eligible_sources
            ),
            max_drive_items=_int(
                "MAX_DRIVE_ITEMS", admission_defaults.max_drive_items
            ),
            max_folders=_int("MAX_FOLDERS", admission_defaults.max_folders),
            max_folder_depth=_int(
                "MAX_FOLDER_DEPTH", admission_defaults.max_folder_depth
            ),
            max_graph_pages=_int(
                "MAX_GRAPH_PAGES", admission_defaults.max_graph_pages
            ),
            max_source_bytes=_int(
                "MAX_SOURCE_BYTES", admission_defaults.max_source_bytes
            ),
            max_rendered_derivative_bytes=_int(
                "MAX_RENDERED_DERIVATIVE_BYTES",
                admission_defaults.max_rendered_derivative_bytes,
            ),
            max_pages_per_source=_int(
                "MAX_PAGES_PER_SOURCE", admission_defaults.max_pages_per_source
            ),
            max_content_units_per_source=_int(
                "MAX_CONTENT_UNITS_PER_SOURCE",
                admission_defaults.max_content_units_per_source,
            ),
            max_office_characters=_int(
                "MAX_OFFICE_CHARACTERS", admission_defaults.max_office_characters
            ),
            max_visual_asset_bytes=_int(
                "MAX_VISUAL_ASSET_BYTES",
                admission_defaults.max_visual_asset_bytes,
            ),
            max_visual_assets_per_source=_int(
                "MAX_VISUAL_ASSETS_PER_SOURCE",
                admission_defaults.max_visual_assets_per_source,
            ),
            max_audio_phrases_per_source=_int(
                "MAX_AUDIO_PHRASES_PER_SOURCE",
                admission_defaults.max_audio_phrases_per_source,
            ),
            max_audio_words_per_source=_int(
                "MAX_AUDIO_WORDS_PER_SOURCE",
                admission_defaults.max_audio_words_per_source,
            ),
            max_chunks_per_source=_int(
                "MAX_CHUNKS_PER_SOURCE", admission_defaults.max_chunks_per_source
            ),
        ),
        execution=load_execution_batch_config(),
        chunk_max_tokens=_int("CHUNK_MAX_TOKENS", 800),
        chunk_overlap_tokens=_int("CHUNK_OVERLAP_TOKENS", 100),
        vision_max_output_tokens=_int("VISION_MAX_OUTPUT_TOKENS", 400),
        sharepoint_site_url=os.getenv("SHAREPOINT_SITE_URL", "").strip(),
        cosmos_ingestion_sources_container=os.getenv(
            "COSMOS_INGESTION_SOURCES_CONTAINER_NAME", "ingestion-sources",
        ).strip() or "ingestion-sources",
        document_intelligence_enabled=document_intelligence_enabled,
        content_understanding_enabled=content_understanding_enabled,
        audio_writer_enabled=audio_enabled_raw == "true",
        audio_transcription_provider=os.getenv("AUDIO_TRANSCRIPTION_PROVIDER", "speech_fast").strip().lower(),
        speech_endpoint=os.getenv("SPEECH_ENDPOINT", "").strip(),
        speech_region=os.getenv("SPEECH_REGION", "").strip().lower(),
        audio_deployment_region=os.getenv("AUDIO_DEPLOYMENT_REGION", "").strip().lower(),
        audio_locale=os.getenv("AUDIO_LOCALE", "").strip(),
        audio_max_response_bytes=_int("AUDIO_MAX_RESPONSE_BYTES", 16 * 1024 * 1024),
        audio_staging_blob_endpoint=os.getenv("AUDIO_STAGING_BLOB_ENDPOINT", "").strip(),
        audio_staging_container=os.getenv("AUDIO_STAGING_CONTAINER", "").strip(),
    )
