"""Tests for read-only source registration preflight validation."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from ingestion.source_preflight import (
    ProposedSourceRegistration,
    SourcePreflightError,
    inspect_content_pair,
)
from source_registry import SourceRegistryError


def _proposal() -> ProposedSourceRegistration:
    return ProposedSourceRegistration.from_body({
        "sourceId": "source-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/source-a",
        "driveId": "drive-a",
        "businessName": "Business A",
        "contentScope": "business",
        "chunksContainer": "business-a-search-chunks",
        "manifestsContainer": "business-a-source-documents",
        "retrievalAclEnabled": True,
    })


def _chunks_policy() -> dict[str, Any]:
    return {
        "id": "business-a-search-chunks",
        "partitionKey": {
            "paths": ["/documentKey"],
            "kind": "Hash",
            "version": 2,
        },
        "indexingPolicy": {
            "indexingMode": "consistent",
            "automatic": True,
            "includedPaths": [
                {"path": "/sourceId/?"},
                {"path": "/documentKey/?"},
                {"path": "/allowedGroupIds/[]/?"},
                {"path": "/isRetrievable/?"},
                {"path": "/lifecycleGeneration/?"},
            ],
            "vectorIndexes": [{
                "path": "/embedding",
                "type": "diskANN",
                "quantizerType": "product",
            }],
            "fullTextIndexes": [
                {"path": "/content"},
                {"path": "/searchableText"},
            ],
        },
        "vectorEmbeddingPolicy": {
            "vectorEmbeddings": [{
                "path": "/embedding",
                "dataType": "float32",
                "dimensions": 3072,
                "distanceFunction": "cosine",
            }],
        },
        "fullTextPolicy": {
            "defaultLanguage": "en-US",
            "fullTextPaths": [
                {"path": "/content", "language": "en-US"},
                {"path": "/searchableText", "language": "en-US"},
            ],
        },
    }


def _manifests_policy() -> dict[str, Any]:
    return {
        "id": "business-a-source-documents",
        "partitionKey": {
            "paths": ["/sourceRunId"],
            "kind": "Hash",
            "version": 2,
        },
        "indexingPolicy": {
            "indexingMode": "consistent",
            "automatic": True,
            "includedPaths": [
                {"path": "/sourceId/?"},
                {"path": "/driveId/?"},
                {"path": "/status/?"},
                {"path": "/documentId/?"},
                {"path": "/lifecycleGeneration/?"},
            ],
            "compositeIndexes": [[
                {"path": "/status", "order": "ascending"},
                {"path": "/discoveryOrdinal", "order": "ascending"},
            ]],
        },
    }


class _Container:
    def __init__(
        self,
        properties: dict[str, Any],
        *,
        error: Exception | None = None,
    ) -> None:
        self.properties = properties
        self.error = error
        self.read_calls = 0
        self.queries: list[dict[str, Any]] = []

    def read(self) -> dict[str, Any]:
        self.read_calls += 1
        if self.error is not None:
            raise self.error
        return deepcopy(self.properties)

    def query_items(self, **kwargs: Any) -> list[str]:
        self.queries.append(kwargs)
        if self.error is not None:
            raise self.error
        return []


class _Database:
    def __init__(self, containers: dict[str, _Container]) -> None:
        self.containers = containers

    def get_container_client(self, name: str) -> _Container:
        return self.containers[name]


class _CosmosError(RuntimeError):
    def __init__(self, status_code: int) -> None:
        super().__init__("cosmos failure")
        self.status_code = status_code


def test_inspect_content_pair_is_read_only_and_accepts_live_policy_shape() -> None:
    chunks = _Container(_chunks_policy())
    manifests = _Container(_manifests_policy())

    result = inspect_content_pair(_Database({
        "business-a-search-chunks": chunks,
        "business-a-source-documents": manifests,
    }), _proposal())

    assert result == {
        "chunksContainer": "business-a-search-chunks",
        "manifestsContainer": "business-a-source-documents",
        "chunksPartitionKey": "/documentKey",
        "manifestsPartitionKey": "/sourceRunId",
        "metadataRead": "passed",
        "query": "passed",
        "functionWritePermission": "requires-deployment-role-evidence",
        "retrievalReadPermission": "requires-deployment-role-evidence",
    }
    assert chunks.read_calls == manifests.read_calls == 1
    assert chunks.queries == manifests.queries == [{
        "query": "SELECT TOP 1 VALUE c.id FROM c",
        "enable_cross_partition_query": True,
        "max_item_count": 1,
    }]


@pytest.mark.parametrize(
    ("container", "mutate"),
    [
        ("chunks", lambda value: value["partitionKey"].update(
            {"paths": ["/sourceRunId"]}
        )),
        ("chunks", lambda value: value["indexingPolicy"].update(
            {"vectorIndexes": []}
        )),
        ("chunks", lambda value: value["vectorEmbeddingPolicy"].update(
            {"vectorEmbeddings": []}
        )),
        ("chunks", lambda value: value["fullTextPolicy"].update(
            {"fullTextPaths": []}
        )),
        ("chunks", lambda value: value["fullTextPolicy"]["fullTextPaths"][0].update(
            {"language": "fr-FR"}
        )),
        ("manifests", lambda value: value["partitionKey"].update(
            {"paths": ["/documentKey"]}
        )),
        ("manifests", lambda value: value["indexingPolicy"].update(
            {"compositeIndexes": []}
        )),
    ],
)
def test_inspect_content_pair_rejects_incompatible_policy(
    container: str,
    mutate: Any,
) -> None:
    chunks_policy = _chunks_policy()
    manifests_policy = _manifests_policy()
    selected = chunks_policy if container == "chunks" else manifests_policy
    mutate(selected)
    database = _Database({
        "business-a-search-chunks": _Container(chunks_policy),
        "business-a-source-documents": _Container(manifests_policy),
    })

    with pytest.raises(
        SourcePreflightError,
        match="source_preflight_container_incompatible",
    ):
        inspect_content_pair(database, _proposal())


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (403, "source_preflight_function_access_denied"),
        (404, "source_preflight_container_missing"),
        (500, "source_preflight_container_unavailable"),
    ],
)
def test_inspect_content_pair_maps_dependency_failures(
    status_code: int,
    expected: str,
) -> None:
    database = _Database({
        "business-a-search-chunks": _Container(
            _chunks_policy(),
            error=_CosmosError(status_code),
        ),
        "business-a-source-documents": _Container(_manifests_policy()),
    })

    with pytest.raises(SourcePreflightError, match=expected) as caught:
        inspect_content_pair(database, _proposal())

    assert caught.value.dependency == "business-a-search-chunks"


def test_common_proposal_defaults_acl_policy_to_false() -> None:
    body = {
        "sourceId": "common",
        "siteUrl": "https://tenant.sharepoint.com/sites/common",
        "driveId": "drive-common",
        "businessName": "Common",
        "contentScope": "common",
        "chunksContainer": "common-search-chunks",
        "manifestsContainer": "common-source-documents",
    }

    proposal = ProposedSourceRegistration.from_body(body)

    assert proposal.retrieval_acl_enabled is False


@pytest.mark.parametrize("content_scope", [1, [], {}, "shared"])
def test_proposal_rejects_invalid_content_scope(content_scope: Any) -> None:
    body = {
        "sourceId": "source-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/source-a",
        "driveId": "drive-a",
        "businessName": "Business A",
        "contentScope": content_scope,
        "chunksContainer": "business-a-search-chunks",
        "manifestsContainer": "business-a-source-documents",
        "retrievalAclEnabled": True,
    }

    with pytest.raises(SourceRegistryError, match="source_contentScope_invalid"):
        ProposedSourceRegistration.from_body(body)
