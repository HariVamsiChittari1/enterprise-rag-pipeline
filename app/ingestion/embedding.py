"""OpenAI embedding generation for document chunks."""

from __future__ import annotations

import logging
import math
import time
from email.utils import parsedate_to_datetime
from typing import Any

from openai import RateLimitError

from ingestion.errors import TerminalDocumentError
from ingestion.telemetry import usage_record, write_audit_record

logger = logging.getLogger(__name__)

EMBEDDING_DIMENSIONS = 3_072
MAX_BATCH_ATTEMPTS = 5
MAX_BATCH_RETRY_SECONDS = 180


def _retry_delay(error: RateLimitError, attempt: int) -> float:
    headers = error.response.headers
    milliseconds = headers.get("retry-after-ms")
    retry_after = headers.get("retry-after")
    try:
        delay = (
            float(milliseconds) / 1000
            if milliseconds is not None
            else float(retry_after)
        )
    except (TypeError, ValueError):
        try:
            delay = parsedate_to_datetime(retry_after).timestamp() - time.time()
        except (TypeError, ValueError, OverflowError):
            delay = float(2**attempt)
    if not math.isfinite(delay) or delay < 0:
        delay = float(2**attempt)
    return delay


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
            deadline = time.monotonic() + MAX_BATCH_RETRY_SECONDS
            for attempt in range(MAX_BATCH_ATTEMPTS):
                try:
                    response = client.embeddings.create(
                        model=deployment,
                        input=batch,
                        dimensions=EMBEDDING_DIMENSIONS,
                        encoding_format="float",
                    )
                    break
                except RateLimitError as error:
                    delay = _retry_delay(error, attempt)
                    if (
                        attempt == MAX_BATCH_ATTEMPTS - 1
                        or delay > deadline - time.monotonic()
                    ):
                        raise
                    logger.warning(
                        "ingestion_embedding_throttled sourceId=%s runId=%s batchOffset=%d attempt=%d retryAfterSeconds=%s",
                        source_id,
                        run_id,
                        offset,
                        attempt + 1,
                        delay,
                    )
                    time.sleep(delay)
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
