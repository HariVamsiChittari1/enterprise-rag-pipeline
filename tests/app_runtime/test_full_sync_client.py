"""Tests for _wait_for_terminal_status, the shared helper behind the fix to
start_full_sync/terminate_ingestion's stale-Durable-Task-history handling.

Note: start_full_sync/terminate_ingestion are decorated with
@app.durable_client_input, whose middleware reconstructs a real
DurableOrchestrationClient from a JSON binding payload before calling the user
code — they can't be invoked directly with a plain fake client object without
faking that binding-resolution machinery, which isn't worth the complexity for
what is otherwise thin route-handler glue. Those two functions are covered by
live end-to-end verification instead. Production code is never modified here.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import azure.durable_functions as df
import pytest

import function_app


class FakeStatus:
    def __init__(self, runtime_status: Any) -> None:
        self.runtime_status = runtime_status


class FakeClient:
    def __init__(self, statuses: list[Any] | None = None) -> None:
        self._statuses = list(statuses or [])

    async def get_status(self, instance_id: str) -> FakeStatus | None:
        if not self._statuses:
            return None
        status = self._statuses.pop(0)
        return None if status is None else FakeStatus(status)


def test_wait_for_terminal_status_true_when_already_terminal() -> None:
    client = FakeClient(statuses=[df.OrchestrationRuntimeStatus.Terminated])
    assert asyncio.run(function_app._wait_for_terminal_status(client, "id-1")) is True


def test_wait_for_terminal_status_keeps_missing_instance_unresolved() -> None:
    client = FakeClient(statuses=[None])
    assert asyncio.run(function_app._wait_for_terminal_status(client, "id-1")) is False


def test_wait_for_terminal_status_times_out_when_stuck_running() -> None:
    client = FakeClient(statuses=[df.OrchestrationRuntimeStatus.Running])
    assert asyncio.run(function_app._wait_for_terminal_status(client, "id-1", timeout_seconds=0)) is False


def test_termination_gate_does_not_finalize_while_document_work_is_unresolved() -> None:
    finalized: list[bool] = []
    registry = SimpleNamespace(get_unresolved_document_operation_count=lambda _source_id: 1)

    result = function_app._terminate_run_if_document_work_settled(
        registry, "source-a", lambda: finalized.append(True) or {"status": "terminated"},
    )

    assert result == {"status": "draining", "unresolvedDocumentWork": 1}
    assert finalized == []


def test_termination_gate_finalizes_after_document_work_settles() -> None:
    finalized: list[bool] = []
    registry = SimpleNamespace(get_unresolved_document_operation_count=lambda _source_id: 0)

    result = function_app._terminate_run_if_document_work_settled(
        registry, "source-a", lambda: finalized.append(True) or {"status": "terminated"},
    )

    assert result == {"status": "terminated"}
    assert finalized == [True]


def test_operator_abandonment_requires_named_operator_and_quiescence_attestation() -> None:
    captured: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    class Registry:
        def get_document_operation_kind(self, *args: Any) -> str:
            captured.append((args, {}))
            return "full-sync"

        def abandon_document_operation_for_operator(self, *args: Any, **kwargs: Any):
            captured.append((args, kwargs))
            return {"status": "abandoned", "recoveryId": "recovery-1"}

    common = {
        "source_id": "source-a",
        "owner_instance_id": "owner-1",
        "work_id": "operation-1",
        "attempt_id": "operation-1",
        "source_run_id": "source-a:run-1",
        "document_id": "doc-1",
        "reason": "workers stopped and remote job reconciled",
        "quiescence_evidence": {
            "evidenceId": "ticket-1",
            "workersStopped": "yes",
            "remoteAudioReconciled": "yes",
            "indexedStateReconciled": "yes",
        },
    }
    with pytest.raises(ValueError, match="operator_identity_required"):
        function_app._operator_abandon_document_work(
            Registry(), operator_id="unknown", **common,
        )
    with pytest.raises(ValueError, match="operator_recovery_identity_mismatch"):
        function_app._operator_abandon_document_work(
            Registry(), operator_id="operator@example.com",
            **{**common, "attempt_id": "other-operation"},
        )
    with pytest.raises(ValueError, match="operator_quiescence_evidence_required"):
        function_app._operator_abandon_document_work(
            Registry(), operator_id="operator@example.com",
            **{**common, "quiescence_evidence": {"evidenceId": "ticket-1"}},
        )

    result = function_app._operator_abandon_document_work(
        Registry(), operator_id="operator@example.com", **common,
    )

    assert result == {"status": "abandoned", "recoveryId": "recovery-1"}
    assert captured[-1] == (
        ("source-a", "owner-1", "operation-1", "source-a:run-1", "doc-1", "full-sync"),
        {
            "operator_id": "operator@example.com",
            "reason": "workers stopped and remote job reconciled",
            "quiescence_evidence": common["quiescence_evidence"],
        },
    )

