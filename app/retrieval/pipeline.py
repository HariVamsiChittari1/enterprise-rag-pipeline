"""Shared retrieval fan-out policy for standard and agentic paths."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Hashable, Iterable, Mapping, TypeVar


T = TypeVar("T")


class RetrievalDependencyError(RuntimeError):
    """All configured retrieval dependencies failed or timed out."""


@dataclass(frozen=True)
class RetrievalBatchStatus:
    submitted: int
    succeeded: int
    failed: int
    timed_out: int = 0

    @property
    def degraded(self) -> bool:
        return self.succeeded > 0 and (self.failed > 0 or self.timed_out > 0)

    def ensure_available(self) -> None:
        if self.submitted == 0 or self.succeeded == 0:
            raise RetrievalDependencyError("retrieval_dependency_unavailable")

    def to_usage(self, candidate_budget: int) -> dict[str, Any]:
        return {
            "operation": "retrieval_batch",
            "submitted": self.submitted,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "timed_out": self.timed_out,
            "degraded": self.degraded,
            "candidate_budget": candidate_budget,
        }


def allocate_candidate_budget(total: int, task_count: int) -> tuple[int, ...]:
    """Distribute an exact nonnegative budget deterministically across tasks."""
    if total < 0 or task_count < 0:
        raise ValueError("candidate budget and task count must be nonnegative")
    if task_count == 0:
        return ()
    share, remainder = divmod(total, task_count)
    return tuple(share + (1 if index < remainder else 0) for index in range(task_count))


def merge_ranked_results(
    ranked_lists: Iterable[list[T]],
    *,
    limit: int,
    identity: Callable[[T], Hashable | None],
) -> list[T]:
    """Round-robin ranked lists so one source or query cannot fill the pool first."""
    lists = list(ranked_lists)
    merged: list[T] = []
    seen: set[Hashable] = set()
    rank = 0
    while len(merged) < limit:
        advanced = False
        for ranked in lists:
            if rank >= len(ranked):
                continue
            advanced = True
            item = ranked[rank]
            key = identity(item)
            if key is None or key in seen:
                continue
            seen.add(key)
            merged.append(item)
            if len(merged) == limit:
                break
        if not advanced:
            break
        rank += 1
    return merged


def evidence_identity(item: Any) -> tuple[str, str] | None:
    if isinstance(item, dict):
        document_id = item.get("documentId")
        chunk_id = item.get("id")
    else:
        document_id = getattr(item, "document_id", None)
        chunk_id = getattr(item, "chunk_id", None)
    if not isinstance(document_id, str) or not isinstance(chunk_id, str):
        return None
    return document_id, chunk_id


def citation_label(index: int) -> str:
    return f"[S{index}]"


def _citation_value(item: Any, field: str) -> Any:
    return item[field] if isinstance(item, Mapping) else getattr(item, field)


def citation_location(item: Any) -> str:
    return str(_citation_value(item, "locator_label"))


def citation_source(item: Any) -> str:
    return f"{_citation_value(item, 'source_name')}, {citation_location(item)}"


def citation_url(item: Any) -> str:
    source = _citation_value(item, "source_url") or _citation_value(item, "source_name")
    source_text = str(source).lower()
    source_name = str(_citation_value(item, "source_name")).lower()
    if (
        str(_citation_value(item, "locator_kind")) == "page"
        and (source_text.endswith(".pdf") or ".pdf#" in source_text or source_name.endswith(".pdf"))
    ):
        return f"{source}#page={_citation_value(item, 'locator_ordinal_start')}"
    return source