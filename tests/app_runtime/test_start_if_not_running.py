"""Tests for _start_if_not_running, the shared helper that lets reconciliation_timer,
acl_resync_timer, and webhook_sharepoint mint a fresh, never-reused Durable instance ID
per tick instead of reusing a fixed one. Durable instance-ID reuse is documented as
best-effort/racy at the storage layer (Azure/azure-functions-durable-python#410).

Uses minimal fakes for the Durable client and DocumentLifecycleRepository. Production
code is never modified by these tests.
"""

from __future__ import annotations

import asyncio
from typing import Any

import azure.durable_functions as df

import function_app


class FakeLifecycleRepository:
    def __init__(
        self,
        existing_instance_id: str | None = None,
        *,
        claim_succeeds: bool = True,
    ) -> None:
        self._existing_instance_id = existing_instance_id
        self._claim_succeeds = claim_succeeds
        self.claims: list[tuple[str, str, str | None, str]] = []

    def get_trigger_instance_id(self, source_id: str, control_id: str) -> str | None:
        return self._existing_instance_id

    def try_claim_trigger_instance_id(
        self,
        source_id: str,
        control_id: str,
        expected_instance_id: str | None,
        instance_id: str,
    ) -> bool:
        self.claims.append((source_id, control_id, expected_instance_id, instance_id))
        return self._claim_succeeds


class FakeClient:
    def __init__(self, status: Any | None = None) -> None:
        self._status = status
        self.started: list[tuple[str, str]] = []

    async def get_status(self, instance_id: str) -> Any | None:
        return self._status

    async def start_new(self, orchestrator_name: str, instance_id: str | None = None, client_input: Any = None) -> str:
        self.started.append((orchestrator_name, instance_id))
        return instance_id or ""


def test_starts_fresh_instance_when_nothing_previously_tracked() -> None:
    lifecycle_repository = FakeLifecycleRepository(existing_instance_id=None)
    client = FakeClient(status=None)

    result = asyncio.run(
        function_app._start_if_not_running(client, lifecycle_repository, "source-1", "delta-sync-trigger", "delta_sync_orchestrator")
    )

    assert result is not None
    assert result.startswith("delta-sync-trigger-")
    assert client.started == [("delta_sync_orchestrator", result)]
    assert lifecycle_repository.claims == [
        ("source-1", "delta-sync-trigger", None, result)
    ]


def test_skips_start_when_tracked_instance_is_running() -> None:
    lifecycle_repository = FakeLifecycleRepository(existing_instance_id="delta-sync-trigger-abc")
    status = type("Status", (), {"runtime_status": df.OrchestrationRuntimeStatus.Running})()
    client = FakeClient(status=status)

    result = asyncio.run(
        function_app._start_if_not_running(client, lifecycle_repository, "source-1", "delta-sync-trigger", "delta_sync_orchestrator")
    )

    assert result is None
    assert client.started == []
    assert lifecycle_repository.claims == []


def test_skips_start_when_tracked_instance_is_pending() -> None:
    lifecycle_repository = FakeLifecycleRepository(existing_instance_id="acl-resync-trigger-abc")
    status = type("Status", (), {"runtime_status": df.OrchestrationRuntimeStatus.Pending})()
    client = FakeClient(status=status)

    result = asyncio.run(
        function_app._start_if_not_running(client, lifecycle_repository, "source-1", "acl-resync-trigger", "acl_resync_orchestrator")
    )

    assert result is None
    assert client.started == []


def test_skips_start_when_concurrent_caller_wins_trigger_claim() -> None:
    lifecycle_repository = FakeLifecycleRepository(
        existing_instance_id="delta-sync-trigger-old",
        claim_succeeds=False,
    )
    status = type("Status", (), {"runtime_status": df.OrchestrationRuntimeStatus.Completed})()
    client = FakeClient(status=status)

    result = asyncio.run(
        function_app._start_if_not_running(client, lifecycle_repository, "source-1", "delta-sync-trigger", "delta_sync_orchestrator")
    )

    assert result is None
    assert client.started == []
    assert len(lifecycle_repository.claims) == 1


def test_starts_new_instance_when_tracked_instance_has_completed() -> None:
    lifecycle_repository = FakeLifecycleRepository(existing_instance_id="delta-sync-trigger-old")
    status = type("Status", (), {"runtime_status": df.OrchestrationRuntimeStatus.Completed})()
    client = FakeClient(status=status)

    result = asyncio.run(
        function_app._start_if_not_running(client, lifecycle_repository, "source-1", "delta-sync-trigger", "delta_sync_orchestrator")
    )

    assert result is not None
    assert result != "delta-sync-trigger-old"
    assert client.started == [("delta_sync_orchestrator", result)]


def test_starts_new_instance_when_tracked_instance_no_longer_exists() -> None:
    lifecycle_repository = FakeLifecycleRepository(existing_instance_id="delta-sync-trigger-gone")
    client = FakeClient(status=None)

    result = asyncio.run(
        function_app._start_if_not_running(client, lifecycle_repository, "source-1", "delta-sync-trigger", "delta_sync_orchestrator")
    )

    assert result is not None
    assert client.started == [("delta_sync_orchestrator", result)]
