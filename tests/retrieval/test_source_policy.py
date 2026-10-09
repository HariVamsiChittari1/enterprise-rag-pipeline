from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from retrieval.source_policy import SourcePolicyReadError, SourcePolicyReader
def _source(source_id: str = "source-a", **overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": source_id,
        "sourceId": source_id,
        "recordType": "ingestion-source",
        "schemaVersion": 1,
        "sourceKind": "sharepoint-drive",
        "desiredState": "active",
        "config": {
            "siteId": "site-a",
            "siteUrl": "https://tenant.sharepoint.com/sites/a",
            "driveId": "drive-a",
        },
        "_etag": "catalog-etag-a",
    }
    item.update(overrides)
    return item


class FakeContainer:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.items = {row["sourceId"]: row for row in rows}
        self.reads: list[tuple[str, str]] = []
        self.query_error: Exception | None = None
        self.read_error: Exception | None = None

    def query_items(self, **kwargs: Any) -> list[dict[str, Any]]:
        assert "@recordType" in {p["name"] for p in kwargs["parameters"]}
        assert "SELECT c.id" in kwargs["query"]
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

    snapshot = reader.capture()

    assert snapshot.eligible_source_ids == ["source-a"]
    assert sources.reads == []


@pytest.mark.parametrize("desired_state", ["paused", "retired"])
def test_capture_withholds_inactive_source(desired_state: str) -> None:
    reader, _ = _reader(_source(desiredState=desired_state))

    assert reader.capture().eligible_source_ids == []


@pytest.mark.parametrize("overrides", [{"schemaVersion": True}, {"_etag": ""}])
def test_capture_withholds_unreadable_source_policy(overrides: dict[str, Any]) -> None:
    reader, _ = _reader(_source(**overrides))

    assert reader.capture().eligible_source_ids == []


def test_registry_enumeration_failure_fails_closed() -> None:
    source = _source()
    reader, sources = _reader(source)
    sources.query_error = RuntimeError("cosmos unavailable")

    with pytest.raises(SourcePolicyReadError, match="source_registry_enumeration_failed"):
        reader.capture()


@pytest.mark.parametrize("desired_state", ["paused", "retired"])
def test_final_check_detects_withdrawal_after_capture(desired_state: str) -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture()
    sources.items["source-a"]["desiredState"] = desired_state

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_final_check_detects_policy_edit_after_capture() -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture()
    sources.items["source-a"]["_etag"] = "new-etag"

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_final_check_withholds_missing_policy() -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture()
    del sources.items["source-a"]

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_final_check_withholds_unreadable_policy() -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture()
    sources.read_error = RuntimeError("cosmos unavailable")

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_final_check_withholds_malformed_policy() -> None:
    source = _source()
    reader, sources = _reader(source)
    snapshot = reader.capture()
    sources.items["source-a"]["schemaVersion"] = True

    assert not reader.validate_sources(snapshot, {"source-a"})


def test_unknown_source_is_not_validated_as_eligible() -> None:
    reader, _ = _reader(_source())

    assert not reader.validate_sources(reader.capture(), {"not-registered"})


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
    snapshot = reader.capture()

    expected = {active_source, other_source} if other_state == "active" else {active_source}
    assert set(snapshot.eligible_source_ids) == expected
    assert reader.validate_sources(snapshot, {active_source})
    assert reader.validate_sources(snapshot, {other_source}) == (other_state == "active")

    sources.items[other_source]["desiredState"] = "retired"
    sources.items[other_source]["_etag"] = "changed-etag"
    assert reader.validate_sources(snapshot, {active_source})
    assert not reader.validate_sources(snapshot, {other_source})
    assert not reader.validate_sources(snapshot, {active_source, other_source})