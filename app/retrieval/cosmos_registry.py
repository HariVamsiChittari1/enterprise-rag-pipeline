"""Lazy Cosmos retriever resolution by validated content-container pair."""

from __future__ import annotations

from threading import Lock
from typing import Any, Callable

from retrieval.cosmos import SecureCosmosRetriever
from retrieval.source_policy import SourceEligibility
from source_registry import validate_container_name


RetrieverFactory = Callable[[SourceEligibility], SecureCosmosRetriever]


class CosmosRegistry:
    """Cache one retriever per validated chunks/manifests container pair."""

    def __init__(
        self,
        database: Any,
        *,
        retriever_factory: RetrieverFactory | None = None,
    ) -> None:
        legacy_retrievers = database if isinstance(database, dict) else None
        if legacy_retrievers is not None and len(legacy_retrievers) != 1:
            raise ValueError(
                "legacy registry must contain exactly one Cosmos retriever",
            )
        self._database = None if legacy_retrievers is not None else database
        self._legacy_items = (
            list(legacy_retrievers.items())
            if legacy_retrievers is not None
            else []
        )
        self._retriever_factory = retriever_factory
        self._retrievers: dict[
            tuple[str, str],
            SecureCosmosRetriever,
        ] = {}
        self._lock = Lock()

    def resolve(self, source: SourceEligibility) -> SecureCosmosRetriever:
        if self._legacy_items:
            return self._legacy_items[0][1]
        chunks_container = validate_container_name(
            source.chunks_container,
            "chunksContainer",
        )
        manifests_container = validate_container_name(
            source.manifests_container,
            "manifestsContainer",
        )
        pair = (chunks_container, manifests_container)
        with self._lock:
            existing = self._retrievers.get(pair)
            if existing is not None:
                return existing
            retriever = (
                self._retriever_factory(source)
                if self._retriever_factory is not None
                else SecureCosmosRetriever(
                    self._database.get_container_client(chunks_container),
                    self._database.get_container_client(manifests_container),
                    require_source_eligibility=True,
                )
            )
            self._retrievers[pair] = retriever
            return retriever

    def __len__(self) -> int:
        return len(self._legacy_items) + len(self._retrievers)

    def items(self) -> list[tuple[str, SecureCosmosRetriever]]:
        return list(self._legacy_items)

    def close(self) -> None:
        with self._lock:
            retrievers = tuple(self._retrievers.values()) + tuple(
                retriever for _, retriever in self._legacy_items
            )
            self._retrievers.clear()
            self._legacy_items.clear()
        closed_ids: set[int] = set()
        for retriever in retrievers:
            if id(retriever) in closed_ids:
                continue
            closed_ids.add(id(retriever))
            close = getattr(retriever, "close", None)
            if callable(close):
                close()


def build_cosmos_registry(database: Any) -> CosmosRegistry:
    return CosmosRegistry(database)
