"""Fail-closed source lifecycle eligibility for retrieval requests."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from source_registry import SOURCE_RECORD_TYPE, SharePointSource

logger = logging.getLogger(__name__)

_SOURCE_LIST_QUERY = (
    "SELECT c.id, c.sourceId, c.recordType, c.schemaVersion, c.sourceKind, "
    "c.desiredState, c.config, c._etag FROM c WHERE c.recordType = @recordType"
)


class SourcePolicyReadError(RuntimeError):
    """The complete source registry could not be enumerated."""


@dataclass(frozen=True)
class SourceEligibility:
    source_id: str
    catalog_etag: str


@dataclass(frozen=True)
class SourcePolicySnapshot:
    sources: tuple[SourceEligibility, ...]

    @property
    def eligible_source_ids(self) -> list[str]:
        return [source.source_id for source in self.sources]

    def get(self, source_id: str) -> SourceEligibility | None:
        return next((source for source in self.sources if source.source_id == source_id), None)


class SourcePolicyReader:
    """Read schema-v1 source lifecycle policy and validate cited sources."""

    def __init__(
        self,
        sources: Any,
        _ingestion_runs: Any = None,
        _tenant_id: str | None = None,
    ) -> None:
        self._sources = sources

    def capture(self) -> SourcePolicySnapshot:
        try:
            rows = list(
                self._sources.query_items(
                    query=_SOURCE_LIST_QUERY,
                    parameters=[{"name": "@recordType", "value": SOURCE_RECORD_TYPE}],
                    enable_cross_partition_query=True,
                    max_item_count=100,
                )
            )
        except Exception as error:
            raise SourcePolicyReadError("source_registry_enumeration_failed") from error

        eligible: list[SourceEligibility] = []
        for item in rows:
            try:
                current = _eligibility_from_item(item)
            except Exception as error:
                logger.warning(
                    "source_policy_denied",
                    extra={
                        "source_id": item.get("sourceId") if isinstance(item, dict) else None,
                        "reason": type(error).__name__,
                    },
                )
                continue
            if current is not None:
                eligible.append(current)
        return SourcePolicySnapshot(tuple(sorted(eligible, key=lambda source: source.source_id)))

    def validate_sources(
        self,
        snapshot: SourcePolicySnapshot,
        source_ids: set[str] | tuple[str, ...] | list[str],
    ) -> bool:
        for source_id in set(source_ids):
            expected = snapshot.get(source_id)
            if expected is None:
                return False
            try:
                current = self._read_eligibility(source_id)
            except Exception:
                return False
            if current != expected:
                return False
        return True

    def _read_eligibility(self, source_id: str) -> SourceEligibility | None:
        source_item = self._sources.read_item(item=source_id, partition_key=source_id)
        current = _eligibility_from_item(source_item)
        if current is None or current.source_id != source_id:
            return None
        return current


def _eligibility_from_item(item: object) -> SourceEligibility | None:
    source = SharePointSource.from_item(item)
    if source.desired_state != "active":
        return None
    catalog_etag = item.get("_etag") if isinstance(item, dict) else None
    if not isinstance(catalog_etag, str) or not catalog_etag:
        return None
    return SourceEligibility(source.source_id, catalog_etag)