"""Tests for lazy, pair-keyed Cosmos retriever resolution."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from retrieval.cosmos_registry import CosmosRegistry, build_cosmos_registry
from retrieval.source_policy import SourceEligibility
from source_registry import SourceRegistryError


def _source(
    source_id: str = "source-a",
    *,
    chunks: str = "business-a-search-chunks",
    manifests: str = "business-a-source-documents",
) -> SourceEligibility:
    return SourceEligibility(
        source_id,
        f"etag-{source_id}",
        "active",
        "business",
        chunks,
        manifests,
        True,
        f"Business {source_id}",
    )


class _Database:
    def __init__(self) -> None:
        self.containers: list[str] = []

    def get_container_client(self, name: str) -> str:
        self.containers.append(name)
        return name


def test_build_is_lazy_until_source_pair_is_resolved() -> None:
    database = _Database()
    registry = build_cosmos_registry(database)

    assert len(registry) == 0
    assert database.containers == []

    registry.resolve(_source())

    assert len(registry) == 1
    assert database.containers == [
        "business-a-search-chunks",
        "business-a-source-documents",
    ]


def test_sources_sharing_pair_reuse_retriever() -> None:
    created: list[str] = []

    def factory(source: SourceEligibility) -> Any:
        created.append(source.source_id)
        return object()

    registry = CosmosRegistry(None, retriever_factory=factory)

    first = registry.resolve(_source("source-a"))
    second = registry.resolve(_source("source-b"))

    assert first is second
    assert created == ["source-a"]


def test_different_pairs_never_share_retriever() -> None:
    registry = CosmosRegistry(
        None,
        retriever_factory=lambda source: SimpleNamespace(
            source_id=source.source_id,
        ),
    )

    first = registry.resolve(_source("source-a"))
    second = registry.resolve(_source(
        "source-b",
        chunks="business-b-search-chunks",
        manifests="business-b-source-documents",
    ))

    assert first is not second
    assert first.source_id == "source-a"
    assert second.source_id == "source-b"


def test_invalid_pair_fails_before_client_creation() -> None:
    database = _Database()
    registry = CosmosRegistry(database)

    with pytest.raises(SourceRegistryError, match="chunksContainer"):
        registry.resolve(_source(chunks="invalid/name"))

    assert database.containers == []


def test_close_closes_and_evicts_cached_retrievers() -> None:
    closed: list[str] = []

    class Retriever:
        def __init__(self, source_id: str) -> None:
            self.source_id = source_id

        def close(self) -> None:
            closed.append(self.source_id)

    registry = CosmosRegistry(
        None,
        retriever_factory=lambda source: Retriever(source.source_id),
    )
    first = registry.resolve(_source())

    registry.close()
    second = registry.resolve(_source())

    assert closed == ["source-a"]
    assert second is not first


def test_legacy_compatibility_accepts_only_one_retriever() -> None:
    retriever = object()
    registry = CosmosRegistry({"source-a": retriever})

    assert registry.items() == [("source-a", retriever)]

    with pytest.raises(ValueError, match="exactly one"):
        CosmosRegistry({"source-a": object(), "source-b": object()})
