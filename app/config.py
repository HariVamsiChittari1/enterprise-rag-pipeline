"""Centralized configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


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
    return int(value) if value else default


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
    vision_deployment: str
    managed_identity_client_id: str

    # Tuning (operator-adjustable per environment)
    chunk_max_tokens: int
    chunk_overlap_tokens: int
    acl_max_pages: int
    download_timeout_seconds: float
    delta_max_pages: int
    embedding_batch_size: int
    max_pdf_pages: int
    vision_max_output_tokens: int
    vision_max_image_bytes: int
    vision_max_figures: int
    query_proxy_timeout_seconds: float
    sharepoint_site_url: str
    document_intelligence_enabled: bool = True
    content_understanding_enabled: bool = False

    @property
    def extraction_provider(self) -> ExtractionProvider | None:
        return _select_extraction_provider(
            self.extraction_enabled,
            self.document_intelligence_enabled,
            self.content_understanding_enabled,
        )


def load_config() -> IngestionConfig:
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
        source_id=_required("INGESTION_SOURCE_ID"),
        drive_id=_required("SHAREPOINT_ASSIGNED_DRIVE_ID"),
        tenant_id=_required("SHAREPOINT_TENANT_ID"),
        app_client_id=_required("SHAREPOINT_APP_CLIENT_ID"),
        certificate_secret_name=os.getenv("SHAREPOINT_CERTIFICATE_SECRET_NAME", "sharepoint-app-cert"),
        key_vault_uri=_required("KEY_VAULT_URI"),
        cosmos_endpoint=_required("COSMOS_ENDPOINT"),
        cosmos_database=_required("COSMOS_DATABASE_NAME"),
        cosmos_ingestion_runs_container=os.getenv("COSMOS_INGESTION_RUNS_CONTAINER_NAME", "ingestion-runs"),
        cosmos_source_documents_container=os.getenv("COSMOS_SOURCE_DOCUMENTS_CONTAINER_NAME", "source-documents"),
        cosmos_search_chunks_container=os.getenv("COSMOS_SEARCH_CHUNKS_CONTAINER_NAME", "search-chunks"),
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
        vision_deployment=_required("OPENAI_CHAT_DEPLOYMENT_NAME") if extraction else "",
        managed_identity_client_id=os.getenv("AZURE_CLIENT_ID", ""),
        chunk_max_tokens=_int("CHUNK_MAX_TOKENS", 800),
        chunk_overlap_tokens=_int("CHUNK_OVERLAP_TOKENS", 100),
        acl_max_pages=_int("ACL_MAX_PAGES", 10),
        download_timeout_seconds=float(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "120.0")),
        delta_max_pages=_int("DELTA_MAX_PAGES", 200),
        embedding_batch_size=_int("EMBEDDING_BATCH_SIZE", 100),
        max_pdf_pages=_int("MAX_PDF_PAGES", 500),
        vision_max_output_tokens=_int("VISION_MAX_OUTPUT_TOKENS", 400),
        vision_max_image_bytes=_int("VISION_MAX_IMAGE_BYTES", 2 * 1024 * 1024),
        vision_max_figures=_int("VISION_MAX_FIGURES", 60),
        query_proxy_timeout_seconds=float(os.getenv("QUERY_PROXY_TIMEOUT_SECONDS", "30.0")),
        sharepoint_site_url=_required("SHAREPOINT_SITE_URL"),
        document_intelligence_enabled=document_intelligence_enabled,
        content_understanding_enabled=content_understanding_enabled,
    )
