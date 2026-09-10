"""Best-effort audit persistence for per-request LLM call usage."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger()
CATALOG_LOGGER_NAME = "retrieval.catalog_runtime"


def catalog_event_emitter(revision: str, replica: str, *, application: str = "", deployment_instance_hash: str = ""):
    event_logger = logging.getLogger(CATALOG_LOGGER_NAME)
    event_logger.setLevel(logging.INFO)
    process_incarnation = str(uuid.uuid4())

    def emit(event: str, fields: dict[str, Any]) -> None:
        event_logger.info(event, extra={
            **fields,
            "revision": revision,
            "replica": replica,
            "process_incarnation": process_incarnation,
            "application": application,
            "deployment_instance_hash": deployment_instance_hash,
        })

    return emit


def write_audit_records(
    container: Any,
    request_id: str,
    user_id: str,
    tenant_id: str,
    mode: str,
    usage: list[dict[str, Any]],
) -> None:
    """Persist one Cosmos item per LLM call. Never raises; failures are logged only."""
    recorded_at = datetime.now(timezone.utc).isoformat()
    for record in usage:
        item = {
            "id": str(uuid.uuid4()),
            "requestId": request_id,
            "userId": user_id,
            "tenantId": tenant_id,
            "mode": mode,
            "recordedAt": recorded_at,
            **record,
        }
        try:
            container.create_item(item)
        except Exception:
            logger.warning(
                "audit_write_failed",
                request_id=request_id,
                operation=record.get("operation"),
                exc_info=True,
            )
