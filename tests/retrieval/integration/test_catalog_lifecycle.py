"""Env-gated live-Cosmos validation for direct editing and protected-copy recovery.

Requires a disposable `retrieval-config` container and a principal with container-scoped
Data Contributor. The test uses a unique partition and cleans up every created item.

  $env:RAG_INTEGRATION_COSMOS_ENDPOINT="https://<account>.documents.azure.com:443/"
  $env:RAG_INTEGRATION_COSMOS_DATABASE="rag-db"
  $env:RAG_INTEGRATION_COSMOS_CONFIG_CONTAINER="retrieval-config"
  python -m pytest tests/retrieval/integration/test_catalog_lifecycle.py -q
"""

from __future__ import annotations

import os
import uuid
import asyncio
from copy import deepcopy
from contextlib import ExitStack

import pytest
from azure.core import MatchConditions
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from retrieval.catalog import (
    CatalogConflictError,
    RuntimeCatalogLoader,
    build_catalog_item,
    publish_catalog,
)
from retrieval.runtime_catalog import RuntimeCatalogProvider

_REQUIRED_ENV = (
    "RAG_INTEGRATION_COSMOS_ENDPOINT",
    "RAG_INTEGRATION_COSMOS_DATABASE",
    "RAG_INTEGRATION_COSMOS_CONFIG_CONTAINER",
)

pytestmark = pytest.mark.skipif(
    any(not os.getenv(name) for name in _REQUIRED_ENV),
    reason="Live catalog integration test; set RAG_INTEGRATION_COSMOS_* env vars to run",
)


def _source(over_fetch_factor: int) -> dict:
    return {
        "config": {
            "retrieval": {
                "overFetchFactor": over_fetch_factor,
                "hybridWeights": {"vector": 2.0, "text": 1.0},
                "fullTextScoreScope": "Global",
            },
            "profiles": [],
            "synonymMaps": [],
        },
    }


@pytest.fixture
def live_catalog():
    from azure.cosmos import CosmosClient
    from azure.identity import DefaultAzureCredential

    with ExitStack() as resources:
        credential = resources.enter_context(DefaultAzureCredential())
        client = resources.enter_context(CosmosClient(
            os.environ["RAG_INTEGRATION_COSMOS_ENDPOINT"], credential=credential,
        ))
        container = (
            client.get_database_client(os.environ["RAG_INTEGRATION_COSMOS_DATABASE"])
            .get_container_client(os.environ["RAG_INTEGRATION_COSMOS_CONFIG_CONTAINER"])
        )
        environment = f"integration-{uuid.uuid4()}"
        created_ids: list[str] = []
        try:
            yield container, environment, created_ids
        finally:
            failures = 0
            for item_id in reversed(created_ids):
                try:
                    container.delete_item(item=item_id, partition_key=environment)
                except CosmosResourceNotFoundError:
                    continue
                except Exception:
                    failures += 1
            if failures:
                pytest.fail(f"Catalog integration cleanup failed for {failures} registered items")


def test_publish_direct_edit_conflict_invalid_edit_and_recovery(live_catalog) -> None:
    container, environment, created_ids = live_catalog
    first_item = build_catalog_item(_source(3), environment)
    second_item = build_catalog_item(_source(4), environment)
    created_ids.append(first_item["id"])
    publish_catalog(container, first_item)
    publish_catalog(container, first_item)
    with pytest.raises(CatalogConflictError):
        publish_catalog(container, second_item)

    async def exercise() -> None:
        provider = RuntimeCatalogProvider(RuntimeCatalogLoader(container, environment), 60)
        try:
            assert await provider.refresh()
            original = provider.snapshot
            with pytest.raises(Exception) as conflict:
                container.replace_item(
                    item="runtime-catalog", body=second_item, etag='"stale"',
                    match_condition=MatchConditions.IfNotModified, retry_write=False,
                )
            assert getattr(conflict.value, "status_code", None) == 412
            changed = container.replace_item(
                item="runtime-catalog", body=second_item, etag=original.etag,
                match_condition=MatchConditions.IfNotModified, retry_write=False,
            )
            assert await provider.refresh()
            accepted = provider.snapshot
            assert accepted.over_fetch_factor == 4
            assert accepted.etag != original.etag
            invalid = deepcopy(second_item)
            invalid["config"]["retrieval"]["overFetchFactor"] = 0
            rejected = container.replace_item(
                item="runtime-catalog", body=invalid, etag=changed["_etag"],
                match_condition=MatchConditions.IfNotModified, retry_write=False,
            )
            assert not await provider.refresh()
            assert provider.snapshot is accepted
            assert provider.health.degraded
            container.replace_item(
                item="runtime-catalog", body=first_item, etag=rejected["_etag"],
                match_condition=MatchConditions.IfNotModified, retry_write=False,
            )
            assert await provider.refresh()
            assert provider.snapshot.digest == original.digest
            assert provider.snapshot.etag != original.etag
            assert not provider.health.degraded
        finally:
            await provider.close()
    asyncio.run(exercise())
