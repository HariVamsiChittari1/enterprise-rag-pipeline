from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import function_app
from ingestion.models import ActivityOutcome, ActivityStatus, SafeError


def test_orchestrators_do_not_use_legacy_retry_actions() -> None:
    source = (Path(__file__).resolve().parents[2] / "app" / "function_app.py").read_text(
        encoding="utf-8"
    )

    assert "call_activity_with_retry" not in source


def test_process_document_activity_retries_inside_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import config
    import ingestion.services as services

    failed_record = SimpleNamespace(
        status=SimpleNamespace(value="failed"),
        to_cosmos_item=lambda: {
            "id": "doc-1",
            "sourceRunId": "run-1",
            "status": "failed",
        },
    )

    class FakeRepository:
        def __init__(self) -> None:
            self.resets = 0
            self.ready = False

        def get_document(self, source_run_id, document_id):
            record = SimpleNamespace(
                status=SimpleNamespace(value="ready"),
                stage=SimpleNamespace(value="terminal"),
                document_id="ab-doc-1",
                drive_id="drive-1",
                item_id="item-1",
                source_run_id=source_run_id,
                document_key="source-1:run-1:ab-doc-1",
                e_tag="source-etag-1",
                lifecycle_generation=1,
                processing_profile_digest="c" * 64,
            ) if self.ready else failed_record
            return SimpleNamespace(record=record, etag=f"etag-{self.resets}")

        def reset_failed_to_discovered(self, item):
            self.resets += 1
            return item

    class FakeRegistry:
        def start_document_operation(self, *_args, **_kwargs):
            return {"status": "started"}

        def settle_document_operation(
            self, *_args, physical_run_id, item_id, status, outcome, **_kwargs,
        ):
            assert physical_run_id == "run-1"
            assert item_id == "ab-doc-1"
            settlements.append((status, outcome))

        def record_document_winner(self, *_args, **_kwargs):
            return {"recordType": "document-winner"}

    repository = FakeRepository()
    settlements: list[tuple[str, dict[str, object]]] = []

    registry = FakeRegistry()
    outcomes = iter((
        ActivityOutcome(
            document_id="ab-doc-1",
            status=ActivityStatus.FAILED,
            chunks_written=0,
            retry_count=0,
            error=SafeError("openai_throttled", "embedding", True),
        ),
        ActivityOutcome(
            document_id="ab-doc-1",
            status=ActivityStatus.SUCCEEDED,
            chunks_written=3,
            retry_count=1,
        ),
    ))
    fake_config = SimpleNamespace(
        extraction_enabled=False,
        enrichment_enabled=False,
        sharepoint_site_url="",
        drive_id="drive-1",
        source_id="source-1",
        audio_writer_enabled=False,
    )

    monkeypatch.setattr(config, "load_config", lambda: fake_config)
    monkeypatch.setattr(
        function_app, "_resolve_source_config",
        lambda _payload: (fake_config, SimpleNamespace(source_id="source-1"), registry),
    )
    monkeypatch.setattr(function_app, "_build_repository", lambda config: repository)
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda config: object())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda config: object())
    monkeypatch.setattr(function_app, "_build_sharepoint_client", lambda config: None)
    monkeypatch.setattr(function_app, "_build_openai_client", lambda config: object())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda config: object())
    monkeypatch.setattr(services, "process_document", lambda *args, **kwargs: next(outcomes))
    original_process_document = services.process_document

    def process_and_mark_ready(*args, **kwargs):
        outcome = original_process_document(*args, **kwargs)
        if outcome.status is ActivityStatus.SUCCEEDED:
            repository.ready = True
        return outcome

    monkeypatch.setattr(services, "process_document", process_and_mark_ready)
    monkeypatch.setattr(function_app, "_retire_prior_version", lambda *args, **kwargs: None)
    monkeypatch.setattr(function_app.time, "sleep", lambda seconds: None)

    result = function_app.process_document_activity(
        {
            "document": {"sourceRunId": "run-1", "documentId": "ab-doc-1"},
            "sourcePlan": {"sourceId": "source-1"},
            "coordinatorOwnerId": "orchestration-1",
            "workId": "work-1",
            "attemptId": "attempt-1",
        }
    )

    assert result == {
        "documentId": "ab-doc-1",
        "status": "succeeded",
        "chunksWritten": 3,
        "error": None,
    }
    assert repository.resets == 1
    assert settlements == [("succeeded", result)]


def test_prepare_retry_page_claims_before_reset_and_persists_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    plan = SimpleNamespace(source_id="source-1")
    failed_doc = {"id": "doc-1", "sourceRunId": "source-1:run-1", "_etag": "etag-1"}

    class Registry:
        def list_unresolved_document_operations_page(self, *_args, **_kwargs):
            return ()

        def begin_document_operation(self, _plan, **kwargs):
            events.append("claim")
            assert kwargs["metadata"] == {"retryContinuationToken": "page-2"}
            return {"status": "acquired", "operationId": "operation-1"}

    class Repository:
        def list_failed_documents_page(self, *_args, **_kwargs):
            return SimpleNamespace(items=(failed_doc,), continuation_token="page-2")

        def reset_failed_to_discovered(self, document):
            assert document is failed_doc
            events.append("reset")
            return document

    registry = Registry()
    monkeypatch.setattr(
        function_app, "_resolve_source_config",
        lambda _payload: (SimpleNamespace(source_id="source-1"), plan, registry),
    )
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: Repository())

    result = function_app.prepare_retry_failed_page_activity({
        "sourcePlan": {}, "coordinatorOwnerId": "retry-owner",
        "runId": "run-1", "operationKind": "retry-failed:retry-owner",
    })

    assert events == ["claim", "reset"]
    assert result == {
        "status": "ready",
        "document": {"documentId": "doc-1", "sourceRunId": "source-1:run-1"},
        "operationId": "operation-1",
        "workId": "operation-1", "attemptId": "operation-1",
        "operationKind": "retry-failed:retry-owner",
        "continuationToken": "page-2",
    }


def test_prepare_retry_page_recovers_claim_after_reset_activity_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = SimpleNamespace(source_id="source-1")
    discovered = SimpleNamespace(status=SimpleNamespace(value="discovered"))

    class Registry:
        def list_unresolved_document_operations_page(self, *_args, **_kwargs):
            return ({
                "id": "operation-1", "itemId": "doc-1",
                "physicalRunId": "source-1:run-1",
                "state": "claimed",
                "metadata": {"retryContinuationToken": "page-2"},
            },)

    class Repository:
        def get_document(self, _source_run_id, _document_id):
            return SimpleNamespace(record=discovered, etag="etag-2")

        def reset_failed_to_discovered(self, _document):
            pytest.fail("recovered discovered documents must not be reset again")

    registry = Registry()
    monkeypatch.setattr(
        function_app, "_resolve_source_config",
        lambda _payload: (SimpleNamespace(source_id="source-1"), plan, registry),
    )
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: Repository())

    result = function_app.prepare_retry_failed_page_activity({
        "sourcePlan": {}, "coordinatorOwnerId": "retry-owner",
        "runId": "run-1", "operationKind": "retry-failed:retry-owner",
        "continuationToken": None,
    })

    assert result == {
        "status": "ready",
        "document": {"documentId": "doc-1", "sourceRunId": "source-1:run-1"},
        "operationId": "operation-1",
        "workId": "operation-1", "attemptId": "operation-1",
        "operationKind": "retry-failed:retry-owner",
        "continuationToken": "page-2",
    }