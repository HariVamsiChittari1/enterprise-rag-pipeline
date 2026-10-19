"""Best-effort audit persistence for ingestion service calls."""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

COSMOS_AUDIT_CONTAINER_NAME = "service-audit"


def usage_record(operation: str, model: str, response: Any, start: float) -> dict[str, Any]:
    usage = getattr(response, "usage", None)
    return {
        "operation": operation,
        "model": model,
        "prompt_tokens": getattr(usage, "prompt_tokens", 0) if usage else 0,
        "completion_tokens": getattr(usage, "completion_tokens", 0) if usage else 0,
        "latency_ms": int((time.perf_counter() - start) * 1000),
    }


def write_audit_record(
    container: Any,
    source_id: str,
    run_id: str,
    record: dict[str, Any],
    *,
    business_name: str | None = None,
) -> None:
    """Persist one audit item. Never raises; failures are logged only."""
    item = {
        "id": str(uuid.uuid4()),
        "sourceId": source_id,
        "runId": run_id,
        "recordedAt": datetime.now(timezone.utc).isoformat(),
        **record,
    }
    if business_name is not None:
        item["businessName"] = business_name
    try:
        container.create_item(item)
    except Exception:
        logger.warning(
            "audit_write_failed source_id=%s operation=%s",
            source_id,
            record.get("operation"),
            exc_info=True,
        )
