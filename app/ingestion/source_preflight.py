"""Read-only validation for proposed SharePoint source registrations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from source_registry import (
    SourceRegistryError,
    validate_business_name,
    validate_container_name,
    validate_site_url,
    validate_source_id,
)

_CHUNK_INCLUDED_PATHS = frozenset({
    "/sourceId/?",
    "/documentKey/?",
    "/allowedGroupIds/[]/?",
    "/isRetrievable/?",
    "/lifecycleGeneration/?",
})
_MANIFEST_INCLUDED_PATHS = frozenset({
    "/sourceId/?",
    "/driveId/?",
    "/status/?",
    "/documentId/?",
    "/lifecycleGeneration/?",
})
_FULL_TEXT_PATHS = frozenset({"/content", "/searchableText"})


class SourcePreflightError(RuntimeError):
    """A proposed source dependency is unavailable or incompatible."""

    def __init__(self, code: str, dependency: str) -> None:
        super().__init__(code)
        self.code = code
        self.dependency = dependency


@dataclass(frozen=True)
class ProposedSourceRegistration:
    """A validated strict registration request that has not been persisted."""

    source_id: str
    site_url: str
    drive_id: str
    business_name: str
    content_scope: str
    chunks_container: str
    manifests_container: str
    retrieval_acl_enabled: bool

    @classmethod
    def from_body(cls, body: object) -> ProposedSourceRegistration:
        """Parse the exact request shape shared by preflight and registration."""
        if isinstance(body, dict) and "businessName" not in body:
            raise SourceRegistryError("source_businessName_invalid")
        required_fields = {
            "sourceId",
            "siteUrl",
            "driveId",
            "businessName",
            "contentScope",
            "chunksContainer",
            "manifestsContainer",
        }
        if (
            not isinstance(body, dict)
            or not required_fields <= set(body)
            or not set(body) <= required_fields | {"retrievalAclEnabled"}
        ):
            raise SourceRegistryError("source_registration_invalid")
        if not all(
            isinstance(body[field], str) and body[field].strip()
            for field in ("sourceId", "siteUrl", "driveId")
        ):
            raise SourceRegistryError("source_registration_invalid")
        content_scope = body["contentScope"]
        if (
            not isinstance(content_scope, str)
            or content_scope not in {"business", "common"}
        ):
            raise SourceRegistryError("source_contentScope_invalid")
        if content_scope == "business" and "retrievalAclEnabled" not in body:
            raise SourceRegistryError("source_retrievalAclEnabled_invalid")
        retrieval_acl_enabled = body.get("retrievalAclEnabled", False)
        if type(retrieval_acl_enabled) is not bool or (
            content_scope == "common" and retrieval_acl_enabled
        ):
            raise SourceRegistryError("source_retrievalAclEnabled_invalid")
        return cls(
            source_id=validate_source_id(body["sourceId"]),
            site_url=validate_site_url(body["siteUrl"].strip()),
            drive_id=body["driveId"].strip(),
            business_name=validate_business_name(body["businessName"]),
            content_scope=content_scope,
            chunks_container=validate_container_name(
                body["chunksContainer"],
                "chunksContainer",
            ),
            manifests_container=validate_container_name(
                body["manifestsContainer"],
                "manifestsContainer",
            ),
            retrieval_acl_enabled=retrieval_acl_enabled,
        )


def inspect_content_pair(
    database: Any,
    proposal: ProposedSourceRegistration,
    *,
    embedding_dimensions: int = 3072,
) -> dict[str, Any]:
    """Validate target policies and prove metadata/query access without writes."""
    if (
        isinstance(embedding_dimensions, bool)
        or not isinstance(embedding_dimensions, int)
        or embedding_dimensions < 1
    ):
        raise ValueError("source_preflight_embedding_dimensions_invalid")

    chunks = _read_and_query_container(database, proposal.chunks_container)
    manifests = _read_and_query_container(database, proposal.manifests_container)
    _validate_chunks_policy(
        chunks,
        proposal.chunks_container,
        embedding_dimensions,
    )
    _validate_manifests_policy(manifests, proposal.manifests_container)
    return {
        "chunksContainer": proposal.chunks_container,
        "manifestsContainer": proposal.manifests_container,
        "chunksPartitionKey": "/documentKey",
        "manifestsPartitionKey": "/sourceRunId",
        "metadataRead": "passed",
        "query": "passed",
        "functionWritePermission": "requires-deployment-role-evidence",
        "retrievalReadPermission": "requires-deployment-role-evidence",
    }


def _read_and_query_container(database: Any, container_name: str) -> dict[str, Any]:
    container = database.get_container_client(container_name)
    try:
        properties = container.read()
        list(container.query_items(
            query="SELECT TOP 1 VALUE c.id FROM c",
            enable_cross_partition_query=True,
            max_item_count=1,
        ))
    except Exception as error:
        status_code = getattr(error, "status_code", None)
        if status_code == 404:
            code = "source_preflight_container_missing"
        elif status_code == 403:
            code = "source_preflight_function_access_denied"
        else:
            code = "source_preflight_container_unavailable"
        raise SourcePreflightError(code, container_name) from error
    if not isinstance(properties, dict):
        raise SourcePreflightError(
            "source_preflight_container_incompatible",
            container_name,
        )
    return properties


def _validate_chunks_policy(
    properties: dict[str, Any],
    container_name: str,
    embedding_dimensions: int,
) -> None:
    indexing = properties.get("indexingPolicy")
    vector_policy = properties.get("vectorEmbeddingPolicy")
    full_text_policy = properties.get("fullTextPolicy")
    if (
        not _partition_matches(properties, "/documentKey")
        or not _indexing_baseline_matches(indexing, _CHUNK_INCLUDED_PATHS)
        or not isinstance(indexing, dict)
        or not _has_index(indexing.get("vectorIndexes"), "/embedding", "diskANN")
        or _paths(indexing.get("fullTextIndexes")) != _FULL_TEXT_PATHS
        or not isinstance(vector_policy, dict)
        or vector_policy.get("vectorEmbeddings") != [{
            "path": "/embedding",
            "dataType": "float32",
            "dimensions": embedding_dimensions,
            "distanceFunction": "cosine",
        }]
        or not isinstance(full_text_policy, dict)
        or full_text_policy.get("defaultLanguage") != "en-US"
        or not _full_text_paths_match(full_text_policy.get("fullTextPaths"))
    ):
        raise SourcePreflightError(
            "source_preflight_container_incompatible",
            container_name,
        )


def _validate_manifests_policy(
    properties: dict[str, Any],
    container_name: str,
) -> None:
    indexing = properties.get("indexingPolicy")
    if (
        not _partition_matches(properties, "/sourceRunId")
        or not _indexing_baseline_matches(indexing, _MANIFEST_INCLUDED_PATHS)
        or not isinstance(indexing, dict)
        or [
            {"path": "/status", "order": "ascending"},
            {"path": "/discoveryOrdinal", "order": "ascending"},
        ] not in indexing.get("compositeIndexes", [])
    ):
        raise SourcePreflightError(
            "source_preflight_container_incompatible",
            container_name,
        )


def _partition_matches(properties: dict[str, Any], path: str) -> bool:
    partition_key = properties.get("partitionKey")
    return (
        isinstance(partition_key, dict)
        and partition_key.get("paths") == [path]
        and partition_key.get("kind") == "Hash"
        and partition_key.get("version") == 2
    )


def _indexing_baseline_matches(
    indexing: object,
    required_paths: frozenset[str],
) -> bool:
    return (
        isinstance(indexing, dict)
        and indexing.get("indexingMode") == "consistent"
        and indexing.get("automatic") is True
        and required_paths <= _paths(indexing.get("includedPaths"))
    )


def _has_index(indexes: object, path: str, index_type: str) -> bool:
    return isinstance(indexes, list) and any(
        isinstance(index, dict)
        and index.get("path") == path
        and index.get("type") == index_type
        for index in indexes
    )


def _full_text_paths_match(values: object) -> bool:
    return (
        isinstance(values, list)
        and _paths(values) == _FULL_TEXT_PATHS
        and all(
            isinstance(value, dict) and value.get("language") == "en-US"
            for value in values
        )
    )


def _paths(values: object) -> frozenset[str]:
    if not isinstance(values, list):
        return frozenset()
    return frozenset(
        value["path"]
        for value in values
        if isinstance(value, dict) and isinstance(value.get("path"), str)
    )
