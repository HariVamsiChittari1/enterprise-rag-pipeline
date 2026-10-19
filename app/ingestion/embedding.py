"""OpenAI embedding generation for document chunks."""

from __future__ import annotations

import logging
import time
from typing import Any

from ingestion.errors import TerminalDocumentError
from ingestion.telemetry import usage_record, write_audit_record

logger = logging.getLogger(__name__)

EMBEDDING_DIMENSIONS = 3_072


def embed_texts(
    client: Any,
    texts: list[str],
    deployment: str,
    audit_container: Any | None = None,
    source_id: str = "",
    run_id: str = "",
    business_name: str | None = None,
    batch_size: int = 100,
) -> list[tuple[float, ...]]:
    """Generate embeddings for a list of texts in batches."""
    embeddings: list[tuple[float, ...]] = []
    for offset in range(0, len(texts), batch_size):
        batch = texts[offset : offset + batch_size]
        try:
            start = time.perf_counter()
            response = client.embeddings.create(
                model=deployment,
                input=batch,
                dimensions=EMBEDDING_DIMENSIONS,
                encoding_format="float",
            )
        except Exception as error:
            # Classify OpenAI errors: 429/timeout → retryable, others → terminal
            error_type = type(error).__name__
            if "RateLimit" in error_type or "Timeout" in error_type or "APITimeout" in error_type:
                raise TimeoutError(f"openai_throttled:{error_type}") from error
            if "APIConnectionError" in error_type or "InternalServerError" in error_type:
                raise TimeoutError(f"openai_transient:{error_type}") from error
            raise TerminalDocumentError(f"openai_failed:{error_type}") from error
        if audit_container is not None:
            write_audit_record(
                audit_container, source_id, run_id,
                usage_record("ingestion_embedding", deployment, response, start),
                business_name=business_name,
            )
        ordered = sorted(response.data, key=lambda item: item.index)
        if len(ordered) != len(batch):
            raise TerminalDocumentError("embedding_count_mismatch")
        for item in ordered:
            vector = tuple(item.embedding)
            if len(vector) != EMBEDDING_DIMENSIONS:
                raise TerminalDocumentError("embedding_dimension_mismatch")
            embeddings.append(vector)

    logger.info("Generated %d embeddings (%d dimensions each)", len(embeddings), EMBEDDING_DIMENSIONS)
    return embeddings
