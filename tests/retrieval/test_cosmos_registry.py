"""Tests for CosmosRegistry: config parsing and default single-instance behavior."""

from __future__ import annotations

import pytest

from retrieval import cosmos_registry
from retrieval.cosmos_registry import (
    CosmosInstanceConfig,
    CosmosRegistry,
    build_cosmos_registry,
    load_cosmos_instance_configs,
)


def test_build_uses_shared_cosmos_client_with_supplied_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class _Client:
        def get_database_client(self, _name: str):
            return self

        def get_container_client(self, name: str) -> str:
            return name

    def _factory(endpoint: str, *, credential: object) -> _Client:
        calls.append((endpoint, credential))
        return _Client()

    monkeypatch.setattr(cosmos_registry, "create_cosmos_client", _factory)
    credential = object()

    registry = build_cosmos_registry(
        (
            CosmosInstanceConfig(
                "source-a",
                "https://cosmos.example/",
                "db",
                "chunks",
                "manifests",
            ),
        ),
        credential,
    )

    assert calls == [("https://cosmos.example/", credential)]
    assert [source_id for source_id, _ in registry.items()] == ["source-a"]


def test_defaults_to_single_instance_from_current_settings_when_registry_json_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COSMOS_REGISTRY_JSON", raising=False)

    configs = load_cosmos_instance_configs(
        default_source_id="source-a",
        default_endpoint="https://cosmos-a.example",
        default_database="db",
        default_chunks_container="search-chunks",
        default_manifests_container="source-documents",
    )

    assert len(configs) == 1
    assert configs[0].source_id == "source-a"
    assert configs[0].endpoint == "https://cosmos-a.example"


def test_parses_multiple_instances_from_registry_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "COSMOS_REGISTRY_JSON",
        '[{"sourceId": "source-a", "endpoint": "https://a.example", "database": "db-a"},'
        ' {"sourceId": "source-b", "endpoint": "https://b.example", "database": "db-b", "chunksContainer": "chunks-b"}]',
    )

    configs = load_cosmos_instance_configs(
        default_source_id="ignored",
        default_endpoint="ignored",
        default_database="ignored",
        default_chunks_container="search-chunks",
        default_manifests_container="source-documents",
    )

    assert [c.source_id for c in configs] == ["source-a", "source-b"]
    assert configs[0].chunks_container == "search-chunks"  # falls back to default
    assert configs[1].chunks_container == "chunks-b"


def test_rejects_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COSMOS_REGISTRY_JSON", "not json")

    with pytest.raises(EnvironmentError, match="not valid JSON"):
        load_cosmos_instance_configs(
            default_source_id="s", default_endpoint="e", default_database="d",
            default_chunks_container="c", default_manifests_container="m",
        )


def test_rejects_empty_array(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COSMOS_REGISTRY_JSON", "[]")

    with pytest.raises(EnvironmentError, match="non-empty"):
        load_cosmos_instance_configs(
            default_source_id="s", default_endpoint="e", default_database="d",
            default_chunks_container="c", default_manifests_container="m",
        )


def test_rejects_entry_missing_required_field(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COSMOS_REGISTRY_JSON", '[{"sourceId": "source-a"}]')

    with pytest.raises(EnvironmentError, match="endpoint"):
        load_cosmos_instance_configs(
            default_source_id="s", default_endpoint="e", default_database="d",
            default_chunks_container="c", default_manifests_container="m",
        )


def test_registry_requires_at_least_one_entry() -> None:
    with pytest.raises(ValueError, match="at least one"):
        CosmosRegistry({})


def test_registry_items_and_len() -> None:
    registry = CosmosRegistry({"source-a": object(), "source-b": object()})

    assert len(registry) == 2
    assert {source_id for source_id, _ in registry.items()} == {"source-a", "source-b"}
