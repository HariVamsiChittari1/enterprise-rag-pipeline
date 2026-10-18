from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from ingestion.source_registry import SourcePlan
from source_registry import (
    SharePointSource,
    SourceRegistryError,
    source_binding_digest,
)


def _source(**config_overrides: Any) -> dict[str, Any]:
    config = {
        "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/source-a",
        "driveId": "drive-a",
        "businessName": "Business A",
        "contentScope": "business",
        "chunksContainer": "business-a-search-chunks",
        "manifestsContainer": "business-a-source-documents",
        "retrievalAclEnabled": True,
    }
    config.update(config_overrides)
    return {
        "id": "source-a",
        "sourceId": "source-a",
        "schemaVersion": 1,
        "sourceKind": "sharepoint-drive",
        "desiredState": "active",
        "config": config,
        "_etag": "etag-a",
    }


def test_source_contract_parses_business_policy_and_routing() -> None:
    source = SharePointSource.from_item(_source())

    assert source.business_name == "Business A"
    assert source.content_scope == "business"
    assert source.chunks_container == "business-a-search-chunks"
    assert source.manifests_container == "business-a-source-documents"
    assert source.retrieval_acl_enabled is True


@pytest.mark.parametrize("retrieval_acl_enabled", [True, False])
def test_business_source_accepts_explicit_acl_policy(
    retrieval_acl_enabled: bool,
) -> None:
    source = SharePointSource.from_item(
        _source(retrievalAclEnabled=retrieval_acl_enabled)
    )

    assert source.retrieval_acl_enabled is retrieval_acl_enabled


def test_common_source_requires_acl_disabled() -> None:
    source = SharePointSource.from_item(
        _source(contentScope="common", retrievalAclEnabled=False)
    )

    assert source.content_scope == "common"
    assert source.retrieval_acl_enabled is False


def test_common_source_rejects_acl_enabled() -> None:
    with pytest.raises(
        SourceRegistryError,
        match="source_retrievalAclEnabled_invalid",
    ):
        SharePointSource.from_item(
            _source(contentScope="common", retrievalAclEnabled=True)
        )


@pytest.mark.parametrize(
    "content_scope",
    ["shared", "COMMON", "", None, 1, [], {}],
)
def test_source_rejects_unknown_content_scope(content_scope: object) -> None:
    with pytest.raises(SourceRegistryError, match="source_contentScope_invalid"):
        SharePointSource.from_item(_source(contentScope=content_scope))


@pytest.mark.parametrize("retrieval_acl_enabled", [None, 0, 1, "false"])
def test_source_rejects_non_boolean_acl_policy(
    retrieval_acl_enabled: object,
) -> None:
    with pytest.raises(
        SourceRegistryError,
        match="source_retrievalAclEnabled_invalid",
    ):
        SharePointSource.from_item(
            _source(retrievalAclEnabled=retrieval_acl_enabled)
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("chunksContainer", ""),
        ("chunksContainer", "x" * 256),
        ("chunksContainer", "bad/name"),
        ("chunksContainer", "bad\\name"),
        ("chunksContainer", "bad?name"),
        ("chunksContainer", "bad#name"),
        ("chunksContainer", "bad-name "),
        ("manifestsContainer", ""),
        ("manifestsContainer", "x" * 256),
        ("manifestsContainer", "bad/name"),
        ("manifestsContainer", "bad\\name"),
        ("manifestsContainer", "bad?name"),
        ("manifestsContainer", "bad#name"),
        ("manifestsContainer", "bad-name "),
    ],
)
def test_source_rejects_invalid_container_names(field: str, value: str) -> None:
    with pytest.raises(SourceRegistryError, match=f"source_{field}_invalid"):
        SharePointSource.from_item(_source(**{field: value}))


def test_source_rejects_legacy_record_type() -> None:
    item = _source()
    item["recordType"] = "ingestion-source"

    with pytest.raises(SourceRegistryError, match="source_record_invalid"):
        SharePointSource.from_item(item)


@pytest.mark.parametrize(
    "field",
    [
        "contentScope",
        "businessName",
        "chunksContainer",
        "manifestsContainer",
        "retrievalAclEnabled",
    ],
)
def test_source_rejects_missing_routing_policy_field(field: str) -> None:
    item = _source()
    del item["config"][field]

    with pytest.raises(SourceRegistryError, match="source_config_invalid"):
        SharePointSource.from_item(item)


def test_source_rejects_unknown_config_field() -> None:
    item = _source()
    item["config"]["unknown"] = "value"

    with pytest.raises(SourceRegistryError, match="source_config_invalid"):
        SharePointSource.from_item(item)


def test_binding_digest_includes_scope_and_container_pair_but_not_acl() -> None:
    business = _source()
    business_acl_disabled = deepcopy(business)
    business_acl_disabled["config"]["retrievalAclEnabled"] = False
    renamed_business = deepcopy(business)
    renamed_business["config"]["businessName"] = "Business A Renamed"
    common = _source(contentScope="common", retrievalAclEnabled=False)
    different_chunks = _source(chunksContainer="business-b-search-chunks")
    different_manifests = _source(
        manifestsContainer="business-b-source-documents"
    )

    digest = source_binding_digest(
        SharePointSource.from_item(business),
        "tenant-a",
    )

    assert source_binding_digest(
        SharePointSource.from_item(business_acl_disabled),
        "tenant-a",
    ) == digest
    assert source_binding_digest(
        SharePointSource.from_item(renamed_business),
        "tenant-a",
    ) == digest
    assert source_binding_digest(
        SharePointSource.from_item(common),
        "tenant-a",
    ) != digest
    assert source_binding_digest(
        SharePointSource.from_item(different_chunks),
        "tenant-a",
    ) != digest
    assert source_binding_digest(
        SharePointSource.from_item(different_manifests),
        "tenant-a",
    ) != digest


@pytest.mark.parametrize(
    "business_name",
    [None, "", " \t ", 1, [], "x" * 201],
)
def test_source_rejects_invalid_business_name(business_name: object) -> None:
    with pytest.raises(SourceRegistryError, match="source_businessName_invalid"):
        SharePointSource.from_item(_source(businessName=business_name))


def test_source_normalizes_business_name_for_both_content_scopes() -> None:
    business = SharePointSource.from_item(_source(businessName="  Business A  "))
    common = SharePointSource.from_item(
        _source(
            businessName="  Enterprise Policies  ",
            contentScope="common",
            retrievalAclEnabled=False,
        )
    )

    assert business.business_name == "Business A"
    assert common.business_name == "Enterprise Policies"


def test_source_plan_round_trip_carries_business_name() -> None:
    source = SharePointSource.from_item(_source(businessName="Retail Lending"))
    plan = SourcePlan(source, "etag-a", "a" * 64)

    assert plan.to_payload()["businessName"] == "Retail Lending"
    assert SourcePlan.from_payload(plan.to_payload()) == plan
