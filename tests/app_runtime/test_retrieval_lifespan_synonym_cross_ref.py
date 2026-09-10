"""Lifespan tests for the required singleton Cosmos retrieval catalog."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from retrieval import main as retrieval_main
from retrieval.catalog import CatalogError, RuntimeCatalogSnapshot
from retrieval.config import RetrievalConfig
from retrieval.scoring import ScoringProfile
from retrieval.synonyms import SynonymExpander, SynonymMap


DIGEST = "sha256:" + "a" * 64


def _base_config() -> RetrievalConfig:
    return RetrievalConfig(
        cosmos_endpoint="https://cosmos.example/",
        cosmos_database="rag",
        cosmos_chunks_container="search-chunks",
        cosmos_manifests_container="source-documents",
        cosmos_audit_container="service-audit",
        openai_endpoint="https://openai.example/",
        embedding_deployment="text-embedding-3-large",
        chat_deployment="gpt-4o",
        tenant_id="11111111-1111-4111-8111-111111111111",
        managed_identity_client_id="55555555-5555-4555-8555-555555555555",
        retrieval_audience="api://retrieval-api",
        gateway_client_id="33333333-3333-4333-8333-333333333333",
        gateway_principal_id="44444444-4444-4444-8444-444444444444",
        deployment_instance_id="instance-a",
        retrieval_timeout_seconds=5.0,
        generation_timeout_seconds=15.0,
        agent_timeout_seconds=8.0,
        agent_max_iterations=5,
        agent_api_version="preview",
        max_evidence_chunks=5,
        max_planned_queries=3,
        graph_group_timeout_seconds=10.0,
        openai_api_version="2024-10-21",
        app_insights_connection_string=None,
        include_citations=True,
        acl_enabled=True,
    )


def _catalog() -> RuntimeCatalogSnapshot:
    synonym_map = SynonymMap.parse("geo-map", ["dog, puppy"])
    return RuntimeCatalogSnapshot(
        deployment_instance_id="instance-a",
        catalog_id="runtime-catalog",
        etag="etag-a",
        digest=DIGEST,
        operation_id="operation-a",
        changed_at="2026-08-26T00:00:00Z",
        accepted_at=datetime(2026, 8, 26, tzinfo=timezone.utc),
        over_fetch_factor=3,
        hybrid_weights=(2.0, 1.0),
        full_text_score_scope="Local",
        default_profile="geo",
        profiles={"geo": ScoringProfile(name="geo", synonym_map="geo-map")},
        synonym_maps={"geo-map": synonym_map},
        synonym_expanders={"geo-map": SynonymExpander(synonym_map)},
    )


def _patch_azure_sdks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retrieval_main, "ManagedIdentityCredential", lambda **_: MagicMock())
    monkeypatch.setattr(retrieval_main, "load_cosmos_instance_configs", lambda **_: [])
    monkeypatch.setattr(retrieval_main, "build_cosmos_registry", lambda *_a, **_k: MagicMock())
    monkeypatch.setattr(retrieval_main, "CosmosClient", lambda **_: MagicMock())
    monkeypatch.setattr(retrieval_main, "AzureOpenAI", lambda **_: MagicMock())
    monkeypatch.setattr(retrieval_main, "_configure_tracing", lambda _cfg, **_kwargs: None)
    monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", False)
    monkeypatch.setattr(retrieval_main, "load_retrieval_config", _base_config)


async def _enter_lifespan() -> None:
    async with retrieval_main._lifespan(MagicMock()):
        pass


def test_lifespan_loads_singleton_and_applies_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_azure_sdks(monkeypatch)
    captured: list[str] = []

    def _loader(_container: object, instance_id: str) -> object:
        captured.append(instance_id)
        return SimpleNamespace(load=_catalog)

    monkeypatch.setattr(retrieval_main, "RuntimeCatalogLoader", _loader)

    asyncio.run(_enter_lifespan())

    assert captured == ["instance-a"]
    policy = retrieval_main._state.rag_service.capture_policy()
    assert policy.snapshot.digest == DIGEST
    assert policy.snapshot.over_fetch_factor == 3
    assert policy.snapshot.hybrid_weights == (2.0, 1.0)
    assert policy.profile.name == "geo"
    assert policy.expander.map_name == "geo-map"
    assert retrieval_main._state.catalog_provider._closed is True


def test_lifespan_catalog_failure_prevents_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_azure_sdks(monkeypatch)
    monkeypatch.setattr(
        retrieval_main,
        "RuntimeCatalogLoader",
        lambda *_args: SimpleNamespace(
            load=lambda: (_ for _ in ()).throw(CatalogError("catalog corrupt"))
        ),
    )

    with pytest.raises(CatalogError):
        asyncio.run(_enter_lifespan())


def test_lifespan_catalog_failure_closes_acquired_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential = MagicMock()
    registry = MagicMock()
    cosmos = MagicMock()
    openai_client = MagicMock()
    monkeypatch.setattr(retrieval_main, "ManagedIdentityCredential", lambda **_: credential)
    monkeypatch.setattr(retrieval_main, "load_cosmos_instance_configs", lambda **_: [])
    monkeypatch.setattr(retrieval_main, "build_cosmos_registry", lambda *_a, **_k: registry)
    monkeypatch.setattr(retrieval_main, "CosmosClient", lambda **_: cosmos)
    monkeypatch.setattr(retrieval_main, "AzureOpenAI", lambda **_: openai_client)
    monkeypatch.setattr(retrieval_main, "_configure_tracing", lambda _cfg, **_kwargs: None)
    monkeypatch.setattr(retrieval_main, "_AGENT_AVAILABLE", False)
    monkeypatch.setattr(retrieval_main, "load_retrieval_config", _base_config)
    monkeypatch.setattr(
        retrieval_main,
        "RuntimeCatalogLoader",
        lambda *_args: SimpleNamespace(
            load=lambda: (_ for _ in ()).throw(CatalogError("catalog corrupt"))
        ),
    )

    with pytest.raises(CatalogError):
        asyncio.run(_enter_lifespan())

    credential.close.assert_called_once()
    registry.close.assert_called_once()
    cosmos.close.assert_called_once()
    openai_client.close.assert_called_once()
