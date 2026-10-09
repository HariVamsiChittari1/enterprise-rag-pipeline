"""Regression tests for process_document_activity error sanitization and retry boundaries.

Monkeypatches only the dependency-construction points (_build_repository,
_build_graph_client, etc.) and ingestion.services.process_document — the same
established pattern as tests/app_runtime/test_retire_prior_version.py. Production code
is never modified by these tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import function_app


class FakeConfig:
    extraction_enabled = False
    enrichment_enabled = False
    audio_writer_enabled = False
    sharepoint_site_url = ""
    drive_id = "drive-1"
    source_id = "source-1"


class FakeRepository:
    def get_document(self, source_run_id: str, document_id: str) -> SimpleNamespace:
        return SimpleNamespace(record=SimpleNamespace(
            status=SimpleNamespace(value="ready"),
            stage=SimpleNamespace(value="terminal"),
            document_id=document_id,
            drive_id="drive-1",
            item_id="item-1",
            source_run_id=source_run_id,
            document_key="source-1:run-1:doc-1",
            e_tag="etag-1",
            lifecycle_generation=1,
            processing_profile_digest="f" * 64,
        ), etag="etag-1")


class FakeRegistry:
    def __init__(self) -> None:
        self.work_state = "claimed"
        self.outcome: dict[str, Any] | None = None
        self.settlements: list[tuple[str, dict[str, Any]]] = []
        self.claim_requests: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def begin_document_operation(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        self.claim_requests.append((_args, _kwargs))
        if self.work_state in {"succeeded", "failed"}:
            return {"status": "completed", "outcome": self.outcome}
        if self.work_state in {"active", "pending"}:
            return {"status": "unresolved" if self.work_state == "active" else "pending"}
        self.work_state = "active"
        return {"status": "acquired", "operationId": "operation-1"}

    def start_document_operation(
        self, *_args: Any, **_kwargs: Any,
    ) -> dict[str, Any]:
        if self.work_state in {"succeeded", "failed"}:
            return {"status": "completed", "outcome": self.outcome}
        if self.work_state == "pending":
            return {"status": "pending"}
        if self.work_state == "active":
            return {"status": "unresolved"}
        self.work_state = "active"
        return {"status": "started"}

    def settle_document_operation(
        self, _source_id: str, _owner: str, _operation_id: str,
        *, physical_run_id: str, item_id: str, status: str,
        outcome: dict[str, Any], **_kwargs: Any,
    ) -> None:
        assert physical_run_id == "source-1:run-1"
        assert item_id == "doc-1"
        self.settlements.append((status, outcome))
        self.work_state = status
        self.outcome = outcome

    def record_document_winner(self, *_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {"recordType": "document-winner"}


@pytest.fixture(autouse=True)
def patch_activity_dependencies(monkeypatch: pytest.MonkeyPatch) -> FakeRegistry:
    import config

    registry = FakeRegistry()
    monkeypatch.setattr(config, "load_config", lambda: FakeConfig())
    monkeypatch.setattr(
        function_app,
        "_resolve_source_config",
        lambda _payload: (FakeConfig(), SimpleNamespace(source_id="source-1"), registry),
    )
    monkeypatch.setattr(function_app, "_build_repository", lambda config: FakeRepository())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda config: object())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda config: object())
    monkeypatch.setattr(function_app, "_build_extraction_clients", lambda config: (None, None))
    monkeypatch.setattr(function_app, "_build_openai_client", lambda config: object())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda config: object())
    monkeypatch.setattr(function_app, "_build_sharepoint_client", lambda config: None)
    return registry
    monkeypatch.setattr(function_app, "_build_openai_client", lambda config: object())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda config: object())


SIGNED_URL_MESSAGE = (
    "download failed: https://contoso.sharepoint.com/_api/download"
    "?tempauth=eyJhbGciOiJIUzI1NiJ9.fake&sig=aB3dE%2FfG5hI%2BjK7lM9nO%3D"
)


def test_claim_activity_persists_work_before_worker_scheduling(
    patch_activity_dependencies: FakeRegistry,
) -> None:
    payload = {
        "document": {"sourceRunId": "source-1:run-1", "documentId": "doc-1"},
        "coordinatorOwnerId": "orchestration-1",
        "operationKind": "full-sync",
    }

    result = function_app.claim_document_work_activity(payload)

    assert result == {
        "status": "acquired", "operationId": "operation-1",
        "workId": "operation-1", "attemptId": "operation-1",
    }
    assert patch_activity_dependencies.claim_requests[0][1] == {
        "owner_instance_id": "orchestration-1",
        "physical_run_id": "source-1:run-1",
        "item_id": "doc-1",
        "operation_kind": "full-sync",
    }


def test_unexpected_retryable_error_returns_safe_code_without_raw_url(monkeypatch: pytest.MonkeyPatch) -> None:
    import ingestion.services as services

    def _raise_timeout(*args: Any, **kwargs: Any) -> None:
        raise TimeoutError(SIGNED_URL_MESSAGE)

    monkeypatch.setattr(services, "process_document", _raise_timeout)

    result = function_app.process_document_activity(
        {
            "document": {"sourceRunId": "run-1", "documentId": "doc-1"},
            "coordinatorOwnerId": "orchestration-1",
            "workId": "work-1",
            "attemptId": "attempt-1",
        }
    )

    assert result["status"] == "failed"
    assert "sig=" not in result["error"]
    assert "tempauth" not in result["error"]
    assert SIGNED_URL_MESSAGE not in result["error"]


def test_nonretryable_error_returns_safe_code_without_raw_url(monkeypatch: pytest.MonkeyPatch) -> None:
    import ingestion.services as services

    def _raise_value_error(*args: Any, **kwargs: Any) -> None:
        raise ValueError(SIGNED_URL_MESSAGE)

    monkeypatch.setattr(services, "process_document", _raise_value_error)

    result = function_app.process_document_activity({
        "document": {"sourceRunId": "run-1", "documentId": "doc-1"},
        "coordinatorOwnerId": "orchestration-1",
        "workId": "work-1",
        "attemptId": "attempt-1",
    })

    assert result["status"] == "failed"
    assert "sig=" not in result["error"]
    assert "tempauth" not in result["error"]
    assert SIGNED_URL_MESSAGE not in result["error"]


def test_interrupted_document_activity_keeps_permit_and_duplicate_does_not_repeat_side_effect(
    monkeypatch: pytest.MonkeyPatch,
    patch_activity_dependencies: FakeRegistry,
) -> None:
    import ingestion.services as services

    calls = 0

    def _interrupt(*args: Any, **kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        raise TimeoutError("worker interrupted")

    monkeypatch.setattr(services, "process_document", _interrupt)
    payload = {
        "document": {"sourceRunId": "source-1:run-1", "documentId": "doc-1"},
        "coordinatorOwnerId": "orchestration-1",
        "workId": "work-1",
        "attemptId": "attempt-1",
    }

    first = function_app.process_document_activity(payload)
    duplicate = function_app.process_document_activity(payload)

    assert first["status"] == "failed"
    assert duplicate == {
        "documentId": "doc-1", "status": "pending", "error": "document_operation_unresolved",
    }
    assert calls == 1
    assert patch_activity_dependencies.work_state == "active"
    assert patch_activity_dependencies.settlements == []


def test_completed_document_activity_replay_returns_recorded_outcome(
    monkeypatch: pytest.MonkeyPatch,
    patch_activity_dependencies: FakeRegistry,
) -> None:
    import ingestion.services as services

    calls = 0

    def _succeed(*args: Any, **kwargs: Any) -> SimpleNamespace:
        nonlocal calls
        calls += 1
        return SimpleNamespace(
            document_id="doc-1", status=SimpleNamespace(value="succeeded"),
            chunks_written=2, error=None,
        )

    monkeypatch.setattr(services, "process_document", _succeed)
    monkeypatch.setattr(function_app, "_retire_prior_version", lambda *args: None)
    payload = {
        "document": {"sourceRunId": "source-1:run-1", "documentId": "doc-1"},
        "coordinatorOwnerId": "orchestration-1",
        "workId": "work-1",
        "attemptId": "attempt-1",
    }

    first = function_app.process_document_activity(payload)
    replay = function_app.process_document_activity(payload)

    assert first["status"] == "succeeded"
    assert replay == first
    assert calls == 1
    assert patch_activity_dependencies.settlements == [("succeeded", first)]


def test_process_document_publishes_winner_before_prior_version_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    patch_activity_dependencies: FakeRegistry,
) -> None:
    import ingestion.services as services

    events: list[str] = []
    monkeypatch.setattr(
        services,
        "process_document",
        lambda *_args, **_kwargs: SimpleNamespace(
            document_id="doc-1", status=SimpleNamespace(value="succeeded"),
            chunks_written=2, error=None,
        ),
    )
    monkeypatch.setattr(
        patch_activity_dependencies,
        "record_document_winner",
        lambda *_args, **_kwargs: events.append("winner"),
    )
    monkeypatch.setattr(
        patch_activity_dependencies,
        "settle_document_operation",
        lambda *_args, **_kwargs: events.append("settle"),
    )
    monkeypatch.setattr(
        function_app,
        "_retire_prior_version",
        lambda *_args, **_kwargs: events.append("cleanup"),
    )

    result = function_app.process_document_activity({
        "document": {"sourceRunId": "source-1:run-1", "documentId": "doc-1"},
        "coordinatorOwnerId": "orchestration-1",
        "workId": "work-1",
        "attemptId": "attempt-1",
    })

    assert result["status"] == "succeeded"
    assert events == ["winner", "cleanup", "settle"]


def test_pending_audio_activity_retains_document_permit(
    monkeypatch: pytest.MonkeyPatch,
    patch_activity_dependencies: FakeRegistry,
) -> None:
    import ingestion.services as services

    monkeypatch.setattr(
        services,
        "process_document",
        lambda *args, **kwargs: SimpleNamespace(
            document_id="doc-1", status=SimpleNamespace(value="pending"),
            chunks_written=0, error=None,
        ),
    )
    payload = {
        "document": {"sourceRunId": "source-1:run-1", "documentId": "doc-1"},
        "coordinatorOwnerId": "orchestration-1",
        "workId": "work-1",
        "attemptId": "attempt-1",
    }

    result = function_app.process_document_activity(payload)

    assert result["status"] == "pending"
    assert patch_activity_dependencies.work_state == "unresolved"
    assert patch_activity_dependencies.settlements == [("unresolved", result)]
