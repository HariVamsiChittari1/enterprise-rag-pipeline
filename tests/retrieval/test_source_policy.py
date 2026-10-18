from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from retrieval.source_policy import SourcePolicyReader, SourcePolicyReadError


def _source(source_id: str = "source-a", **overrides: Any) -> dict[str, Any]:
    config_overrides = overrides.pop("config", {})
    item: dict[str, Any] = {
        "id": source_id,
        "sourceId": source_id,
        "schemaVersion": 1,
        "sourceKind": "sharepoint-drive",
        "desiredState": "active",
        "config": {
            "siteId": "site-a",
            "siteUrl": "https://tenant.sharepoint.com/sites/a",
            "driveId": "drive-a",
            "businessName": "Business A",
            "contentScope": "business",
            "chunksContainer": "business-a-search-chunks",
            "manifestsContainer": "business-a-source-documents",
            "retrievalAclEnabled": True,
        },
        "_etag": "catalog-etag-a",
    }
    item.update(overrides)
    item["config"].update(config_overrides)
    return item


class FakeContainer:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.items = {row["sourceId"]: row for row in rows}
        self.reads: list[tuple[str, str]] = []
        self.query_calls = 0
        self.query_error: Exception | None = None
        self.read_error: Exception | None = None

    def query_items(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.query_calls += 1
        assert kwargs["parameters"] == []
        assert "SELECT c.id" in kwargs["query"]
        assert "recordType" not in kwargs["query"]
        if self.query_error:
            raise self.query_error
        return self.rows

    def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
        self.reads.append((item, partition_key))
        if self.read_error:
            raise self.read_error
        try:
            return deepcopy(self.items[partition_key])
        except KeyError:
            raise LookupError("not_found") from None


def _reader(*source_items: dict[str, Any]) -> tuple[SourcePolicyReader, FakeContainer]:
    sources = FakeContainer(list(source_items))
    return SourcePolicyReader(sources), sources


def test_capture_admits_active_source() -> None:
    source = _source()
    reader, sources = _reader(source)

    snapshot = reader.capture("source-a")

    assert snapshot.eligible_source_ids == ["source-a"]
    assert sources.query_calls == 0
    assert sources.reads == [("source-a", "source-a")]


def test_capture_point_reads_one_source_policy() -> None:
    source = _source()
    reader, sources = _reader(source)

    snapshot = reader.capture("source-a")

    assert snapshot.eligible_source_ids == ["source-a"]
    assert snapshot.sources[0].catalog_etag == "catalog-etag-a"
    assert snapshot.sources[0].business_name == "Business A"
    assert snapshot.sources[0].content_scope == "business"
    assert snapshot.sources[0].chunks_container == "business-a-search-chunks"
    assert snapshot.sources[0].manifests_container == "business-a-source-documents"
    assert snapshot.sources[0].retrieval_acl_enabled is True
    assert sources.query_calls == 0
    assert sources.reads == [("source-a", "source-a")]


@pytest.mark.parametrize("desired_state", ["paused", "retired"])
def test_capture_withholds_inactive_source(desired_state: str) -> None:
    reader, _ = _reader(_source(desiredState=desired_state))

    assert reader.capture("source-a").eligible_source_ids == []


def test_capture_rejects_malformed_source_policy() -> None:
    reader, _ = _reader(_source(schemaVersion=True))

    with pytest.raises(
        SourcePolicyReadError,
        match="source_registry_point_read_failed",
    ):
        reader.capture("source-a")


def test_capture_withholds_source_policy_without_etag() -> None:
    reader, _ = _reader(_source(_etag=""))

    assert reader.capture("source-a").eligible_source_ids == []


def test_point_read_does_not_enumerate_registry() -> None:
    source = _source()
    reader, sources = _reader(source)
    sources.query_error = RuntimeError("cosmos unavailable")

    assert reader.capture("source-a").eligible_source_ids == ["source-a"]
    assert sources.query_calls == 0


@pytest.mark.parametrize("desired_state", ["paused", "retired"])
def test_final_check_detects_withdrawal_after_capture(desired_state: str) -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture("source-a")
    sources.items["source-a"]["desiredState"] = desired_state

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_final_check_allows_label_and_etag_edit_after_capture() -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture("source-a")
    sources.items["source-a"]["_etag"] = "new-etag"
    sources.items["source-a"]["config"]["businessName"] = "Renamed Business"

    assert reader.validate_sources(snapshot, {"source-a"})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("contentScope", "common"),
        ("chunksContainer", "business-b-search-chunks"),
        ("manifestsContainer", "business-b-source-documents"),
        ("retrievalAclEnabled", False),
    ],
)
def test_final_check_detects_retrieval_semantic_change_after_capture(
    field: str,
    value: object,
) -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture("source-a")
    sources.items["source-a"]["config"][field] = value
    sources.items["source-a"]["_etag"] = "new-etag"

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_final_check_withholds_missing_policy() -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture("source-a")
    del sources.items["source-a"]

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_final_check_withholds_unreadable_policy() -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture("source-a")
    sources.read_error = RuntimeError("cosmos unavailable")

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_final_check_withholds_malformed_policy() -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture("source-a")
    sources.items["source-a"]["schemaVersion"] = True

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_unknown_source_is_not_validated_as_eligible() -> None:
    reader, _ = _reader(_source())

    assert not reader.validate_sources(
        reader.capture("source-a"),
        {"not-registered"},
    )


@pytest.mark.parametrize("other_state", ["active", "paused", "retired"])
@pytest.mark.parametrize("active_source", ["source-a", "source-b"])
def test_mixed_source_lifecycle_keeps_unrelated_active_source_eligible(
    other_state: str, active_source: str,
) -> None:
    other_source = "source-b" if active_source == "source-a" else "source-a"
    active = _source(
        active_source,
        config={
            "siteId": f"site-{active_source}",
            "siteUrl": f"https://tenant.sharepoint.com/sites/{active_source}",
            "driveId": f"drive-{active_source}",
        },
    )
    other = _source(
        other_source, desiredState=other_state,
        config={
            "siteId": f"site-{other_source}",
            "siteUrl": f"https://tenant.sharepoint.com/sites/{other_source}",
            "driveId": f"drive-{other_source}",
        },
    )
    reader, sources = _reader(active, other)
    snapshot = reader.capture(active_source)

    assert snapshot.eligible_source_ids == [active_source]
    assert sources.query_calls == 0
    assert sources.reads == [(active_source, active_source)]
    assert reader.validate_sources(snapshot, {active_source})
    assert not reader.validate_sources(snapshot, {other_source})

    sources.items[other_source]["desiredState"] = "retired"
    sources.items[other_source]["_etag"] = "changed-etag"
    assert reader.validate_sources(snapshot, {active_source})
    assert not reader.validate_sources(snapshot, {other_source})
    assert not reader.validate_sources(snapshot, {active_source, other_source})