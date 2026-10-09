from __future__ import annotations

import asyncio
import hashlib
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from threading import Event
from unittest.mock import Mock

import pytest
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from retrieval.catalog import (
    BASELINE_ETAG,
    CatalogError,
    RuntimeCatalogLoader,
    RuntimeCatalogSnapshot,
    build_catalog_item,
)
from retrieval.config import load_retrieval_config, parse_catalog_poll_seconds
from retrieval.runtime_catalog import RuntimeCatalogProvider
from retrieval.telemetry import CATALOG_LOGGER_NAME, catalog_event_emitter


def test_observations_emit_exportable_generation_and_process_fields(caplog) -> None:
    async def exercise() -> None:
        loader = Mock()
        loader.load.return_value = _snapshot()
        provider = RuntimeCatalogProvider(
            loader, 60, emit=catalog_event_emitter("revision-a", "replica-a"),
        )
        try:
            assert await provider.refresh()
            assert await provider.refresh()
            records = [record for record in caplog.records if record.name == CATALOG_LOGGER_NAME]
            assert len(records) == 2
            assert records[0].process_incarnation == records[1].process_incarnation
            for record in records:
                assert record.getMessage() == "catalog_observed"
                assert record.etag_hash == hashlib.sha256(b"etag-a").hexdigest()
                assert record.digest == provider.snapshot.digest
                assert record.revision == "revision-a"
                assert record.replica == "replica-a"
                assert datetime.fromisoformat(record.observed_at).tzinfo is not None
                assert datetime.fromisoformat(record.accepted_at).tzinfo is not None
                assert "etag" not in record.__dict__
                assert "operation_id" not in record.__dict__
            catalog_event_emitter("revision-a", "replica-a")("catalog_observed", {})
            assert caplog.records[-1].process_incarnation != records[0].process_incarnation
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_export_failure_is_visible_without_blocking_refresh(caplog) -> None:
    async def exercise() -> None:
        loader = Mock()
        loader.load.return_value = _snapshot()
        provider = RuntimeCatalogProvider(loader, 60, emit=Mock(side_effect=RuntimeError("private")))
        try:
            assert await provider.refresh()
            assert not provider.health.degraded
            assert "catalog_event_export_failed" in caplog.text
            assert "private" not in caplog.text
        finally:
            await provider.close()
    asyncio.run(exercise())


def _snapshot(etag: str = "etag-a", operation: str = "operation-a") -> RuntimeCatalogSnapshot:
    return RuntimeCatalogSnapshot(
        deployment_instance_id="test", catalog_id="runtime-catalog", etag=etag,
        digest="sha256:" + "a" * 64, operation_id=operation,
        changed_at="2026-09-08T00:00:00Z", accepted_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
        over_fetch_factor=3, hybrid_weights=(2, 1), full_text_score_scope="Global",
        default_profile=None, profiles={}, synonym_maps={}, synonym_expanders={},
    )


def _real_item(etag: str = "etag-real") -> dict:
    item = build_catalog_item(
        {"config": {
            "retrieval": {
                "overFetchFactor": 3, "hybridWeights": {"vector": 2, "text": 1},
                "fullTextScoreScope": "Global",
            },
            "profiles": [], "synonymMaps": [],
        }},
        "test",
    )
    item["_etag"] = etag
    return item


def test_start_succeeds_when_catalog_absent() -> None:
    async def exercise() -> None:
        container = Mock()
        container.read_item.side_effect = CosmosResourceNotFoundError(status_code=404, message="missing")
        provider = RuntimeCatalogProvider(RuntimeCatalogLoader(container, "test"), 60)
        try:
            await provider.start()
            assert provider.snapshot.etag == BASELINE_ETAG
            assert dict(provider.snapshot.profiles) == {}
            assert provider.snapshot.default_profile is None
            assert not provider.health.degraded
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_runtime_deletion_transitions_to_baseline() -> None:
    async def exercise() -> None:
        container = Mock()
        container.read_item.side_effect = [
            _real_item(),
            CosmosResourceNotFoundError(status_code=404, message="missing"),
        ]
        provider = RuntimeCatalogProvider(RuntimeCatalogLoader(container, "test"), 60)
        try:
            assert await provider.refresh()
            assert provider.snapshot.etag == "etag-real"
            assert await provider.refresh()
            assert provider.snapshot.etag == BASELINE_ETAG
            assert not provider.health.degraded
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_baseline_is_stable_across_repeated_polls() -> None:
    async def exercise() -> None:
        container = Mock()
        container.read_item.side_effect = CosmosResourceNotFoundError(status_code=404, message="missing")
        provider = RuntimeCatalogProvider(RuntimeCatalogLoader(container, "test"), 60)
        try:
            assert await provider.refresh()
            first = provider.snapshot
            assert await provider.refresh()
            assert provider.snapshot.etag == BASELINE_ETAG
            assert provider.snapshot.digest == first.digest
            assert not provider.health.degraded
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_baseline_transitions_to_real_when_item_appears() -> None:
    async def exercise() -> None:
        container = Mock()
        container.read_item.side_effect = [
            CosmosResourceNotFoundError(status_code=404, message="missing"),
            _real_item(),
        ]
        provider = RuntimeCatalogProvider(RuntimeCatalogLoader(container, "test"), 60)
        try:
            assert await provider.refresh()
            assert provider.snapshot.etag == BASELINE_ETAG
            assert await provider.refresh()
            assert provider.snapshot.etag == "etag-real"
            assert not provider.health.degraded
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_recurring_failure_after_recovery_invalidates_cohort_evidence() -> None:
    from tools.publish_retrieval_catalog import _cohort_converged

    async def exercise() -> None:
        base = datetime(2026, 9, 8, tzinfo=timezone.utc)
        now = base
        records = []
        member = {"revision": "revision-a", "replica": "replica-a", "container": "container-a",
                  "restart_count": 0, "process_incarnation": "process-a"}
        loader = Mock()
        loader.load.side_effect = [_snapshot(), CatalogError("unavailable"), _snapshot(), CatalogError("unavailable")]
        provider = RuntimeCatalogProvider(
            loader, 60, utcnow=lambda: now,
            emit=lambda event, fields: records.append({"event": event, **fields, **member}),
        )
        try:
            for seconds in (-60, 0, 60, 120):
                now = base + timedelta(seconds=seconds)
                await provider.refresh()
            assert provider.health.degraded
            assert sum(record["event"] == "catalog_rejected" for record in records) == 1
            assert sum(record["event"] == "catalog_degraded" for record in records) == 2
            assert not _cohort_converged(
                {"observedAt": (base + timedelta(seconds=30)).isoformat(), "members": [member]},
                {"observedAt": (base + timedelta(seconds=150)).isoformat(), "members": [member]},
                records, "etag-a", provider.snapshot.digest,
            )
        finally:
            await provider.close()
    asyncio.run(exercise())


@pytest.mark.parametrize("with_writer_metadata", [False, True])
def test_given_direct_config_edit_when_refreshed_then_adopted_without_new_operation(
    with_writer_metadata: bool,
) -> None:
    async def exercise() -> None:
        now = datetime(2026, 9, 8, tzinfo=timezone.utc)
        item = build_catalog_item(
            {"config": {
                "retrieval": {
                    "overFetchFactor": 3, "hybridWeights": {"vector": 1, "text": 1},
                    "fullTextScoreScope": "Global",
                },
                "profiles": [], "synonymMaps": [],
            }},
            "test", operation_id="b14c1a67-9fc4-4e78-a818-e5951473d15b",
            changed_at=now, reason="Synthetic direct edit",
        )
        if not with_writer_metadata:
            del item["change"]
        item["_etag"] = "etag-a"
        container = Mock()
        container.read_item.return_value = item
        provider = RuntimeCatalogProvider(RuntimeCatalogLoader(container, "test"), 60)
        try:
            assert await provider.refresh()
            original = provider.snapshot
            edited = deepcopy(item)
            edited["_etag"] = "etag-b"
            edited["config"]["retrieval"]["overFetchFactor"] = 5
            container.read_item.return_value = edited
            assert await provider.refresh()
            accepted = provider.snapshot
            assert accepted.etag == "etag-b"
            assert accepted.digest != original.digest
            assert accepted.over_fetch_factor == 5
            assert original.over_fetch_factor == 3
            assert accepted.operation_id == original.operation_id
            if not with_writer_metadata:
                assert accepted.operation_id is None
                assert accepted.changed_at is None
            observed_at = provider.health.last_observed_at
            invalid = deepcopy(edited)
            invalid["_etag"] = "etag-c"
            invalid["config"]["retrieval"]["overFetchFactor"] = 0
            container.read_item.return_value = invalid
            assert not await provider.refresh()
            assert provider.snapshot is accepted
            assert provider.health.last_observed_at == observed_at
            assert provider.health.degraded
            restored = deepcopy(item)
            restored["_etag"] = "etag-d"
            container.read_item.return_value = restored
            assert await provider.refresh()
            assert provider.snapshot.digest == original.digest
            assert provider.snapshot.etag == "etag-d"
            assert not provider.health.degraded
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_given_unchanged_config_when_etag_changes_then_generation_is_observed() -> None:
    async def exercise() -> None:
        loader = Mock()
        loader.load.return_value = _snapshot()
        events = []
        now = datetime(2026, 9, 8, tzinfo=timezone.utc)
        provider = RuntimeCatalogProvider(loader, 60, emit=lambda *event: events.append(event), utcnow=lambda: now)
        try:
            assert await provider.refresh()
            previous = provider.snapshot
            now += timedelta(days=2)
            loader.load.return_value = _snapshot("etag-b")
            assert await provider.refresh()
            assert provider.snapshot is not previous
            accepted = provider.snapshot
            assert accepted.etag == "etag-b"
            assert accepted.digest == previous.digest
            assert await provider.refresh()
            assert provider.snapshot is accepted
            assert provider.health.last_observed_at == now
            assert not provider.health.degraded
            assert [name for name, _ in events] == ["catalog_observed"] * 3
            assert all("operation_id" not in fields for _, fields in events)
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_given_interleaved_rejected_generations_when_retried_then_deduplicated_across_recovery() -> None:
    async def exercise() -> None:
        now = datetime(2026, 9, 8, tzinfo=timezone.utc)
        item = build_catalog_item(
            {"config": {
                "retrieval": {
                    "overFetchFactor": 3, "hybridWeights": {"vector": 1, "text": 1},
                    "fullTextScoreScope": "Global",
                },
                "profiles": [], "synonymMaps": [],
            }},
            "test", operation_id="b14c1a67-9fc4-4e78-a818-e5951473d15b",
            changed_at=now, reason="Synthetic rejection test",
        )
        item["_etag"] = "etag-a"
        container = Mock()
        container.read_item.return_value = item
        events = []
        provider = RuntimeCatalogProvider(
            RuntimeCatalogLoader(container, "test"), 60,
            emit=lambda *event: events.append(event), utcnow=lambda: now,
        )
        try:
            assert await provider.refresh()
            previous = provider.snapshot
            observed_at = provider.health.last_observed_at
            rejected = deepcopy(item)
            rejected["config"]["retrieval"]["overFetchFactor"] = "private-invalid-value"
            for etag in ("etag-b", "etag-c", "etag-b"):
                now += timedelta(seconds=60)
                rejected["_etag"] = etag
                container.read_item.return_value = rejected
                assert not await provider.refresh()
                assert provider.snapshot is previous
                assert provider.health.last_observed_at == observed_at
                assert provider.health.degraded
            recovered = deepcopy(item)
            recovered["_etag"] = "etag-d"
            container.read_item.return_value = recovered
            assert await provider.refresh()
            assert not provider.health.degraded
            assert provider.health.last_observed_at == now
            previous = provider.snapshot
            container.read_item.return_value = rejected
            assert not await provider.refresh()
            assert provider.snapshot is previous
            assert provider.health.last_observed_at == now
            rejections = [fields for name, fields in events if name == "catalog_rejected"]
            assert len(rejections) == 2
            assert {fields["etag_hash"] for fields in rejections} == {
                hashlib.sha256(etag.encode("utf-8")).hexdigest()
                for etag in ("etag-b", "etag-c")
            }
            assert {fields["reason"] for fields in rejections} == {
                "read_or_validation_failed"
            }
            assert "private-invalid-value" not in str(events)
            assert "etag-b" not in str(rejections)
            assert "etag-c" not in str(rejections)
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_given_unchanged_generation_when_read_then_snapshot_retained_and_observation_repeated() -> None:
    async def exercise() -> None:
        loader = Mock()
        loader.load.return_value = _snapshot()
        events = []
        provider = RuntimeCatalogProvider(loader, 7200, emit=lambda *event: events.append(event))
        try:
            assert await provider.refresh()
            previous = provider.snapshot
            assert await provider.refresh()
            assert provider.snapshot is previous
            assert [name for name, _ in events] == ["catalog_observed", "catalog_observed"]
            loader.load.side_effect = CatalogError("sensitive content must not escape")
            assert not await provider.refresh()
            assert provider.snapshot is previous
            assert provider.health.degraded
            assert "sensitive" not in str(events)
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_given_invalid_startup_when_started_then_fails_closed() -> None:
    async def exercise() -> None:
        loader = Mock()
        loader.load.side_effect = CatalogError("invalid")
        provider = RuntimeCatalogProvider(loader, 60)
        with pytest.raises(CatalogError, match="startup"):
            await provider.start()
        with pytest.raises(CatalogError, match="not loaded"):
            _ = provider.snapshot
        assert not await provider.refresh()
    asyncio.run(exercise())


def test_given_timed_out_read_when_completed_late_then_never_published(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("retrieval.runtime_catalog.CATALOG_READ_TIMEOUT_SECONDS", 0.02)

    async def exercise() -> None:
        release = Event()
        finished = Event()
        loader = Mock()
        loader.load.return_value = _snapshot()
        provider = RuntimeCatalogProvider(loader, 60)
        try:
            assert await provider.refresh()
            previous = provider.snapshot

            def blocked_read() -> RuntimeCatalogSnapshot:
                try:
                    assert release.wait(2)
                    return _snapshot("etag-b", "operation-b")
                finally:
                    finished.set()

            loader.load.side_effect = blocked_read
            assert not await provider.refresh()
            assert provider.health.reason == "read_timeout"
            assert not await provider.refresh()
            assert loader.load.call_count == 2
            await provider.close()
            release.set()
            assert await asyncio.to_thread(finished.wait, 2)
            assert provider.snapshot is previous
            assert not await provider.refresh()
        finally:
            release.set()
            await provider.close()
    asyncio.run(exercise())


@pytest.mark.parametrize("poll_seconds", [59, 86401, 60.0, True])
def test_given_invalid_poll_interval_when_provider_created_then_rejected(poll_seconds: object) -> None:
    with pytest.raises(ValueError):
        RuntimeCatalogProvider(Mock(), poll_seconds)


def test_given_same_etag_with_different_content_when_read_then_rejected() -> None:
    async def exercise() -> None:
        loader = Mock()
        loader.load.return_value = _snapshot()
        provider = RuntimeCatalogProvider(loader, 60)
        try:
            assert await provider.refresh()
            loader.load.return_value = replace(_snapshot(), digest="sha256:" + "b" * 64)
            assert not await provider.refresh()
            assert provider.health.reason == "inconsistent_generation"
        finally:
            await provider.close()
    asyncio.run(exercise())


@pytest.mark.parametrize("raw,expected", [(None, 7200), ("60", 60), ("7200", 7200), ("86400", 86400)])
def test_given_valid_poll_setting_when_parsed_then_exact_interval(raw: str | None, expected: int) -> None:
    assert parse_catalog_poll_seconds(raw) == expected


@pytest.mark.parametrize("raw", ["", " ", " 60", "60 ", "60.0", "+60", "1e2", "59", "86401", "-60", "nan"])
def test_given_invalid_poll_setting_when_parsed_then_rejected(raw: str) -> None:
    with pytest.raises(ValueError, match="RETRIEVAL_CATALOG_POLL_SECONDS"):
        parse_catalog_poll_seconds(raw)


def test_given_environment_when_loaded_then_no_digest_or_relevance_override_required(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "COSMOS_ENDPOINT", "COSMOS_DATABASE", "AZURE_OPENAI_ENDPOINT", "CHAT_DEPLOYMENT", "TENANT_ID",
        "MANAGED_IDENTITY_CLIENT_ID", "RETRIEVAL_API_AUDIENCE", "RETRIEVAL_GATEWAY_CLIENT_ID",
        "RETRIEVAL_GATEWAY_PRINCIPAL_ID", "DEPLOYMENT_INSTANCE_ID",
    ):
        monkeypatch.setenv(name, "test")
    monkeypatch.delenv("RETRIEVAL_CATALOG_DIGEST", raising=False)
    monkeypatch.delenv("RETRIEVAL_CATALOG_POLL_SECONDS", raising=False)
    monkeypatch.setenv("RETRIEVAL_OVER_FETCH_FACTOR", "ignored")
    monkeypatch.setenv("RETRIEVAL_HYBRID_RRF_WEIGHTS", "ignored")
    config = load_retrieval_config()
    assert config.catalog_poll_seconds == 7200
    assert not hasattr(config, "catalog_digest")
    monkeypatch.setenv("RETRIEVAL_CATALOG_POLL_SECONDS", "86400")
    assert load_retrieval_config().catalog_poll_seconds == 86400


def test_given_read_duration_when_polling_then_start_to_start_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    async def exercise() -> None:
        clock = 0.0
        starts = []
        delays = []

        def read() -> RuntimeCatalogSnapshot:
            nonlocal clock
            starts.append(clock)
            clock += 2
            return _snapshot()

        async def sleep(delay: float) -> None:
            nonlocal clock
            delays.append(delay)
            if len(delays) == 3:
                raise asyncio.CancelledError
            clock += delay

        provider = RuntimeCatalogProvider(Mock(load=read), 60, monotonic=lambda: clock)
        try:
            assert await provider.refresh()
            monkeypatch.setattr("retrieval.runtime_catalog.asyncio.sleep", sleep)
            with pytest.raises(asyncio.CancelledError):
                await provider._poll()
            assert starts == [0, 60, 120]
            assert delays == [58, 58, 58]
        finally:
            await provider.close()
    asyncio.run(exercise())


def test_given_shutdown_during_refresh_when_read_finishes_then_no_publication() -> None:
    async def exercise() -> None:
        entered = Event()
        release = Event()
        loader = Mock()
        loader.load.return_value = _snapshot()
        provider = RuntimeCatalogProvider(loader, 60)
        try:
            assert await provider.refresh()
            previous = provider.snapshot

            def read() -> RuntimeCatalogSnapshot:
                entered.set()
                assert release.wait(2)
                return _snapshot("etag-b", "operation-b")

            loader.load.side_effect = read
            refresh = asyncio.create_task(provider.refresh())
            assert await asyncio.to_thread(entered.wait, 2)
            await provider.close()
            release.set()
            assert not await refresh
            assert provider.snapshot is previous
        finally:
            release.set()
            await provider.close()
    asyncio.run(exercise())


def test_given_running_provider_when_closed_then_poll_task_is_cancelled() -> None:
    async def exercise() -> None:
        provider = RuntimeCatalogProvider(Mock(load=Mock(return_value=_snapshot())), 60)
        await provider.start()
        task = provider._task
        assert task is not None
        await provider.close()
        assert task.done()
        assert provider._task is None
    asyncio.run(exercise())