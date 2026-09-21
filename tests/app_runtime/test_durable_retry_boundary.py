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

        def get_document(self, source_run_id, document_id):
            return SimpleNamespace(record=failed_record, etag=f"etag-{self.resets}")

        def reset_failed_to_discovered(self, item):
            self.resets += 1
            return item

    repository = FakeRepository()
    outcomes = iter((
        ActivityOutcome(
            document_id="doc-1",
            status=ActivityStatus.FAILED,
            chunks_written=0,
            retry_count=0,
            error=SafeError("openai_throttled", "embedding", True),
        ),
        ActivityOutcome(
            document_id="doc-1",
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
    monkeypatch.setattr(function_app, "_build_repository", lambda config: repository)
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda config: object())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda config: object())
    monkeypatch.setattr(function_app, "_build_sharepoint_client", lambda config: None)
    monkeypatch.setattr(function_app, "_build_openai_client", lambda config: object())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda config: object())
    monkeypatch.setattr(services, "process_document", lambda *args, **kwargs: next(outcomes))
    monkeypatch.setattr(function_app, "_retire_prior_version", lambda *args, **kwargs: None)
    monkeypatch.setattr(function_app.time, "sleep", lambda seconds: None)

    result = function_app.process_document_activity(
        {"document": {"sourceRunId": "run-1", "documentId": "ab-doc-1"}}
    )

    assert result == {
        "documentId": "doc-1",
        "status": "succeeded",
        "chunksWritten": 3,
        "error": None,
    }
    assert repository.resets == 1