from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import function_app


class FakeTask:
    def __init__(self, name: str, payload: object = None) -> None:
        self.name = name
        self.payload = payload
        self.result: object = None
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class FakeOrchestrationContext:
    def __init__(self, input_value: object = None) -> None:
        self.instance_id = "instance-1"
        self.current_utc_datetime = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
        self.input_value = input_value
        self.calls: list[FakeTask] = []
        self.timer = FakeTask("timer")

    def get_input(self) -> object:
        return self.input_value

    def call_activity(self, name: str, payload: object) -> FakeTask:
        task = FakeTask(name, payload)
        self.calls.append(task)
        return task

    def create_timer(self, deadline: datetime) -> FakeTask:
        return self.timer

    def task_all(self, tasks: list[FakeTask]) -> FakeTask:
        return FakeTask("task_all", tasks)

    def task_any(self, tasks: list[FakeTask]) -> FakeTask:
        return FakeTask("task_any", tasks)


def _orchestrator_generator(builder: object, context: FakeOrchestrationContext):
    return builder._function._func.orchestrator_function(context)


def test_timeout_activity_closes_only_supplied_document_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, str]] = []

    class FakeLifecycleRepository:
        def fail_timed_out_document(self, **kwargs: str) -> bool:
            calls.append(kwargs)
            if kwargs["document_id"] == "doc-terminal":
                return False
            return True

    monkeypatch.setattr("config.load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(
        function_app,
        "_build_lifecycle_repository",
        lambda config: FakeLifecycleRepository(),
    )

    result = function_app.timeout_wave_documents_activity({
        "documents": [
            {"sourceRunId": "source:run-1", "documentId": "doc-timeout"},
            {"sourceRunId": "source:run-1", "documentId": "doc-terminal"},
        ],
        "errorCode": "wave_timeout",
    })

    assert result == {"failedCount": 1, "terminalCount": 1}
    assert calls == [
        {
            "source_run_id": "source:run-1",
            "document_id": "doc-timeout",
            "error_code": "wave_timeout",
        },
        {
            "source_run_id": "source:run-1",
            "document_id": "doc-terminal",
            "error_code": "wave_timeout",
        },
    ]


def test_full_sync_timer_win_schedules_exact_wave_timeout_fence() -> None:
    documents = [
        {"sourceRunId": "source:run-1", "documentId": "doc-1"},
        {"sourceRunId": "source:run-1", "documentId": "doc-2"},
    ]
    context = FakeOrchestrationContext()
    orchestrator = _orchestrator_generator(function_app.full_sync_orchestrator, context)

    activation_task = next(orchestrator)
    assert activation_task.name == "activate_run_activity"
    discovery_task = orchestrator.send({"runId": "run-1", "runEtag": "etag-1"})
    assert discovery_task.name == "discover_all_activity"
    any_task = orchestrator.send({"documents": documents, "itemsScanned": 2})
    assert any_task.name == "task_any"
    timeout_task = orchestrator.send(context.timer)

    assert timeout_task.name == "timeout_wave_documents_activity"
    assert timeout_task.payload == {
        "documents": documents,
        "errorCode": "wave_timeout",
    }


def test_full_sync_task_failure_schedules_exact_wave_timeout_fence() -> None:
    documents = [
        {"sourceRunId": "source:run-1", "documentId": "doc-1"},
        {"sourceRunId": "source:run-1", "documentId": "doc-2"},
    ]
    context = FakeOrchestrationContext()
    orchestrator = _orchestrator_generator(function_app.full_sync_orchestrator, context)
    next(orchestrator)
    orchestrator.send({"runId": "run-1", "runEtag": "etag-1"})
    orchestrator.send({"documents": documents, "itemsScanned": 2})

    timeout_task = orchestrator.throw(RuntimeError("activity failed"))

    assert timeout_task.name == "timeout_wave_documents_activity"
    assert timeout_task.payload == {
        "documents": documents,
        "errorCode": "wave_retry_exhausted",
    }


def test_retry_timer_win_schedules_exact_document_timeout_fence() -> None:
    document = {"sourceRunId": "source:run-1", "documentId": "doc-1"}
    context = FakeOrchestrationContext([document])
    orchestrator = _orchestrator_generator(function_app.retry_failed_orchestrator, context)

    any_task = next(orchestrator)
    assert any_task.name == "task_any"
    timeout_task = orchestrator.send(context.timer)

    assert timeout_task.name == "timeout_wave_documents_activity"
    assert timeout_task.payload == {
        "documents": [document],
        "errorCode": "retry_timeout",
    }