"""Retrieval service configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise EnvironmentError(f"Required environment variable {name} is not set")
    return value


def parse_catalog_poll_seconds(raw: str | None) -> int:
    if raw is None:
        return 7200
    if not raw or not raw.isascii() or not raw.isdecimal():
        raise ValueError("RETRIEVAL_CATALOG_POLL_SECONDS must be an integer from 60 through 86400")
    seconds = int(raw)
    if not 60 <= seconds <= 86400:
        raise ValueError("RETRIEVAL_CATALOG_POLL_SECONDS must be an integer from 60 through 86400")
    return seconds


def _optional_positive_seconds(name: str) -> int | None:
    value = os.getenv(name)
    if value is None:
        return None
    if not value.isascii() or not value.isdecimal() or int(value) <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return int(value)


@dataclass(frozen=True)
class RetrievalConfig:
    cosmos_endpoint: str
    cosmos_database: str
    cosmos_chunks_container: str
    cosmos_manifests_container: str
    cosmos_audit_container: str
    openai_endpoint: str
    embedding_deployment: str
    chat_deployment: str
    tenant_id: str
    managed_identity_client_id: str
    retrieval_audience: str
    gateway_client_id: str
    gateway_principal_id: str
    deployment_instance_id: str
    retrieval_timeout_seconds: float
    generation_timeout_seconds: float
    agent_timeout_seconds: float
    agent_max_iterations: int
    agent_api_version: str
    max_evidence_chunks: int
    max_planned_queries: int
    graph_group_timeout_seconds: float
    openai_api_version: str
    app_insights_connection_string: str | None
    include_citations: bool
    acl_enabled: bool
    catalog_poll_seconds: int = 7200
    catalog_container: str = "retrieval-config"
    operation_timeout_seconds: float = 27.0
    audio_retrieval_enabled: bool = False
    audio_max_acl_age_seconds: int | None = None
    audio_max_source_age_seconds: int | None = None

    def __post_init__(self) -> None:
        if type(self.audio_retrieval_enabled) is not bool:
            raise ValueError("AUDIO_RETRIEVAL_ENABLED must be true or false")
        if self.audio_retrieval_enabled and (
            not self.acl_enabled
            or any(type(value) is not int or value <= 0 for value in (
                self.audio_max_acl_age_seconds, self.audio_max_source_age_seconds,
            ))
        ):
            raise ValueError("audio_requires_acl_and_freshness_limits")


def load_retrieval_config() -> RetrievalConfig:
    audio_enabled = os.getenv("AUDIO_RETRIEVAL_ENABLED", "false").strip().lower()
    if audio_enabled not in ("true", "false"):
        raise ValueError("AUDIO_RETRIEVAL_ENABLED must be true or false")
    return RetrievalConfig(
        cosmos_endpoint=_required("COSMOS_ENDPOINT"),
        cosmos_database=_required("COSMOS_DATABASE"),
        cosmos_chunks_container=os.getenv("COSMOS_CHUNKS_CONTAINER", "search-chunks"),
        cosmos_manifests_container=os.getenv("COSMOS_MANIFESTS_CONTAINER", "source-documents"),
        cosmos_audit_container=os.getenv("COSMOS_AUDIT_CONTAINER", "service-audit"),
        openai_endpoint=_required("AZURE_OPENAI_ENDPOINT"),
        embedding_deployment=os.getenv("EMBEDDING_DEPLOYMENT", "text-embedding-3-large"),
        chat_deployment=_required("CHAT_DEPLOYMENT"),
        tenant_id=_required("TENANT_ID"),
        managed_identity_client_id=_required("MANAGED_IDENTITY_CLIENT_ID"),
        retrieval_audience=_required("RETRIEVAL_API_AUDIENCE"),
        gateway_client_id=_required("RETRIEVAL_GATEWAY_CLIENT_ID"),
        gateway_principal_id=_required("RETRIEVAL_GATEWAY_PRINCIPAL_ID"),
        deployment_instance_id=_required("DEPLOYMENT_INSTANCE_ID"),
        retrieval_timeout_seconds=float(os.getenv("RETRIEVAL_TIMEOUT_SECONDS", "5.0")),
        generation_timeout_seconds=float(os.getenv("GENERATION_TIMEOUT_SECONDS", "15.0")),
        agent_timeout_seconds=float(os.getenv("AGENT_TIMEOUT_SECONDS", "8.0")),
        agent_max_iterations=int(os.getenv("AGENT_MAX_ITERATIONS", "5")),
        # Azure OpenAI v1 Responses API only supports "preview" today; "latest" (GA) isn't
        # released yet. Override via AGENT_OPENAI_API_VERSION once Microsoft ships "latest".
        agent_api_version=os.getenv("AGENT_OPENAI_API_VERSION", "preview"),
        max_evidence_chunks=int(os.getenv("MAX_EVIDENCE_CHUNKS", "5")),
        max_planned_queries=int(os.getenv("MAX_PLANNED_QUERIES", "3")),
        graph_group_timeout_seconds=float(os.getenv("GRAPH_GROUP_TIMEOUT_SECONDS", "10.0")),
        openai_api_version=os.getenv("OPENAI_API_VERSION", "2024-10-21"),
        app_insights_connection_string=os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "").strip() or None,
        include_citations=os.getenv("INCLUDE_CITATIONS", "true").strip().lower() != "false",
        acl_enabled=os.getenv("ACL_ENABLED", "true").strip().lower() not in ("false", "0", "no"),
        catalog_poll_seconds=parse_catalog_poll_seconds(os.getenv("RETRIEVAL_CATALOG_POLL_SECONDS")),
        catalog_container=os.getenv("RETRIEVAL_CONFIG_CONTAINER", "retrieval-config").strip() or "retrieval-config",
        operation_timeout_seconds=float(os.getenv("RETRIEVAL_OPERATION_TIMEOUT_SECONDS", "27.0")),
        audio_retrieval_enabled=audio_enabled == "true",
        audio_max_acl_age_seconds=_optional_positive_seconds("AUDIO_MAX_ACL_AGE_SECONDS"),
        audio_max_source_age_seconds=_optional_positive_seconds("AUDIO_MAX_SOURCE_AGE_SECONDS"),
    )
