"""Fail-closed source lifecycle eligibility for retrieval requests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from source_registry import SharePointSource, validate_source_id


class SourcePolicyReadError(RuntimeError):
    """The requested source policy could not be read safely."""


@dataclass(frozen=True)
class SourceEligibility:
    source_id: str
    catalog_etag: str
    desired_state: str
    content_scope: str
    chunks_container: str
    manifests_container: str
    retrieval_acl_enabled: bool
    business_name: str

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

    def capture(self, source_id: str) -> SourcePolicySnapshot:
        try:
            normalized_source_id = validate_source_id(source_id)
            current = self._read_eligibility(normalized_source_id)
        except Exception as error:
            raise SourcePolicyReadError("source_registry_point_read_failed") from error
        return SourcePolicySnapshot((current,) if current is not None else ())

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
            if current is None or not _same_retrieval_semantics(expected, current):
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
    return SourceEligibility(
        source_id=source.source_id,
        catalog_etag=catalog_etag,
        desired_state=source.desired_state,
        content_scope=source.content_scope,
        chunks_container=source.chunks_container,
        manifests_container=source.manifests_container,
        retrieval_acl_enabled=source.retrieval_acl_enabled,
        business_name=source.business_name,
    )


def _same_retrieval_semantics(
    expected: SourceEligibility,
    current: SourceEligibility,
) -> bool:
    return (
        expected.source_id == current.source_id
        and expected.desired_state == current.desired_state
        and expected.content_scope == current.content_scope
        and expected.chunks_container == current.chunks_container
        and expected.manifests_container == current.manifests_container
        and expected.retrieval_acl_enabled == current.retrieval_acl_enabled
    )