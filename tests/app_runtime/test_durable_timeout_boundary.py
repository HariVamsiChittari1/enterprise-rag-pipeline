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
        self.continued_input: object | None = None

    def get_input(self) -> object:
        return self.input_value

    def call_activity(self, name: str, payload: object) -> FakeTask:
        task = FakeTask(name, payload)
        self.calls.append(task)
        return task

    def call_sub_orchestrator(self, name: str, input_value: object, instance_id: str) -> FakeTask:
        task = FakeTask(name, {"input": input_value, "instanceId": instance_id})
        self.calls.append(task)
        return task

    def create_timer(self, deadline: datetime) -> FakeTask:
        return self.timer

    def task_all(self, tasks: list[FakeTask]) -> FakeTask:
        return FakeTask("task_all", tasks)

    def task_any(self, tasks: list[FakeTask]) -> FakeTask:
        return FakeTask("task_any", tasks)

    def continue_as_new(self, input_value: object) -> None:
        self.continued_input = input_value


def _orchestrator_generator(builder: object, context: FakeOrchestrationContext):
    return builder._function._func.orchestrator_function(context)


BASELINE_CANDIDATE = "baseline-link"


def _discovery_phase(source_plan: dict, run_id: str = "run-1", run_etag: str = "etag-1"):
    """Full sync resumes in the discover phase after the baseline continue-as-new."""
    context = FakeOrchestrationContext({
        "orchestrationPhase": "discover",
        "sourcePlan": source_plan,
        "runId": run_id,
        "runEtag": run_etag,
        "discoveredCount": 0,
        "itemsScanned": 0,
        "rejectedCount": 0,
        "baselineCandidate": BASELINE_CANDIDATE,
        "discoveryState": None,
    })
    return context, _orchestrator_generator(function_app.full_sync_orchestrator, context)


def test_timeout_activity_verifies_only_supplied_operation_identities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_calls: list[tuple[str, ...]] = []
    drain_calls: list[tuple[str, str]] = []

    class FakeRegistry:
        def assert_document_operation_unresolved(self, *args: str) -> None:
            operation_calls.append(args)

        def mark_drive_coordinator_draining(self, *_args: str) -> None:
            drain_calls.append(_args)

    config = SimpleNamespace(source_id="source-a")
    source_plan = {"sourceId": "source-a"}
    monkeypatch.setattr(
        function_app, "_resolve_source_config",
        lambda payload: (config, source_plan, FakeRegistry()),
    )
    monkeypatch.setattr(
        function_app,
        "_build_lifecycle_repository",
        lambda _config: pytest.fail("timeout verification must not mutate document state"),
    )

    result = function_app.timeout_wave_documents_activity({
        "documents": [
            {
                "sourceRunId": "source:run-1", "documentId": "doc-timeout",
                "operationId": "operation-timeout",
                "operationKind": "full-sync",
            },
            {
                "sourceRunId": "source:run-1", "documentId": "doc-terminal",
                "workId": "operation-terminal", "attemptId": "legacy-attempt",
                "operationKind": "full-sync",
            },
        ],
        "errorCode": "wave_timeout",
        "sourcePlan": source_plan,
        "coordinatorOwnerId": "instance-1",
    })

    assert result == {"verifiedCount": 2, "draining": True}
    assert drain_calls == [("source-a", "instance-1")]
    assert operation_calls == [
        ("source-a", "instance-1", "operation-timeout", "source:run-1", "doc-timeout", "full-sync"),
        ("source-a", "instance-1", "operation-terminal", "source:run-1", "doc-terminal", "full-sync"),
    ]


def test_timeout_activity_drains_when_operation_identity_cannot_be_verified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class FakeRegistry:
        def assert_document_operation_unresolved(self, *_args: str) -> None:
            raise RuntimeError("operation mismatch")

        def mark_drive_coordinator_draining(self, source_id: str, owner: str) -> None:
            events.append(f"drain:{source_id}:{owner}")

    class FakeLifecycleRepository:
        def fail_timed_out_document(self, **_kwargs: str) -> bool:
            pytest.fail("unverified operations cannot mutate document state")

    config = SimpleNamespace(source_id="source-a")
    source_plan = {"sourceId": "source-a"}
    monkeypatch.setattr(
        function_app,
        "_resolve_source_config",
        lambda _payload: (config, source_plan, FakeRegistry()),
    )
    monkeypatch.setattr(
        function_app,
        "_build_lifecycle_repository",
        lambda _config: FakeLifecycleRepository(),
    )

    result = function_app.timeout_wave_documents_activity({
        "documents": [{
            "sourceRunId": "source:run-1",
            "documentId": "doc-1",
            "operationId": "operation-1",
            "operationKind": "full-sync",
        }],
        "errorCode": "wave_timeout",
        "sourcePlan": source_plan,
        "coordinatorOwnerId": "owner-1",
    })

    assert result["error"] == "timeout_fence_incomplete"
    assert result["errorCount"] == 1
    assert events == ["drain:source-a:owner-1"]


def test_legacy_bulk_failure_activity_refuses_unregistered_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        function_app,
        "_build_repository",
        lambda _config: pytest.fail("bulk failure must not mutate documents"),
    )

    assert function_app.fail_wave_documents_activity({"runId": "run-1"}) == {
        "error": "unregistered_bulk_failure_disabled",
        "failedCount": 0,
    }


def test_full_sync_timer_win_schedules_exact_wave_timeout_fence() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    documents = [
        {"sourceRunId": "source:run-1", "documentId": "doc-1"},
        {"sourceRunId": "source:run-1", "documentId": "doc-2"},
    ]
    context, orchestrator = _discovery_phase(source_plan)

    discovery_task = next(orchestrator)
    assert discovery_task.name == "discover_all_activity"
    page_task = orchestrator.send({"discoveredCount": 2, "itemsScanned": 2, "complete": True})
    assert page_task.name == "list_discovered_documents_page_activity"
    claim_task = orchestrator.send({"documents": documents, "continuationToken": None})
    assert claim_task.name == "task_all"
    any_task = orchestrator.send([
        {"status": "acquired", "operationId": "operation-1"},
        {"status": "acquired", "operationId": "operation-2"},
    ])
    assert any_task.name == "task_any"
    timeout_task = orchestrator.send(context.timer)

    assert timeout_task.name == "timeout_wave_documents_activity"
    assert timeout_task.payload == {
        "documents": [
            {
                **documents[0], "operationId": "operation-1",
                "workId": "operation-1", "attemptId": "operation-1",
                "operationKind": "full-sync",
            },
            {
                **documents[1], "operationId": "operation-2",
                "workId": "operation-2", "attemptId": "operation-2",
                "operationKind": "full-sync",
            },
        ],
        "errorCode": "wave_timeout",
        "sourcePlan": source_plan,
        "coordinatorOwnerId": context.instance_id,
    }


def test_full_sync_captures_baseline_before_discovery() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    context = FakeOrchestrationContext(source_plan)
    orchestrator = _orchestrator_generator(function_app.full_sync_orchestrator, context)

    activation_task = next(orchestrator)
    assert activation_task.name == "activate_run_activity"
    assert activation_task.payload["sourcePlan"] == source_plan
    baseline_task = orchestrator.send({"runId": "run-1", "runEtag": "etag-1"})
    assert baseline_task.name == "delta_sync_activity"
    assert baseline_task.payload["fullSyncBaseline"] is True
    assert baseline_task.payload["deferCursorCommit"] is True
    with pytest.raises(StopIteration):
        orchestrator.send({
            "continuationPhase": "baseline_complete",
            "phaseContinuationToken": BASELINE_CANDIDATE,
        })

    assert context.continued_input == {
        "orchestrationPhase": "discover",
        "sourcePlan": source_plan,
        "runId": "run-1",
        "runEtag": "etag-1",
        "discoveredCount": 0,
        "itemsScanned": 0,
        "rejectedCount": 0,
        "baselineCandidate": BASELINE_CANDIDATE,
        "discoveryState": None,
    }


def test_full_sync_discovery_continues_as_new_after_each_graph_page() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    context, orchestrator = _discovery_phase(source_plan)

    discovery = next(orchestrator)
    assert discovery.name == "discover_all_activity"
    assert discovery.payload["discoveryState"] is None
    with pytest.raises(StopIteration):
        orchestrator.send({
            "discoveredCount": 4,
            "itemsScanned": 7,
            "discoveryState": {
                "pendingFolders": [{"itemId": "folder-2", "depth": 1, "nextLink": None}],
                "itemsScanned": 7,
                "foldersDiscovered": 1,
                "pdfsDiscovered": 4,
                "graphPages": 2,
            },
            "complete": False,
        })

    assert context.continued_input == {
        "orchestrationPhase": "discover",
        "sourcePlan": source_plan,
        "runId": "run-1",
        "runEtag": "etag-1",
        "discoveredCount": 4,
        "itemsScanned": 7,
        "rejectedCount": 0,
        "baselineCandidate": BASELINE_CANDIDATE,
        "discoveryState": {
            "pendingFolders": [{"itemId": "folder-2", "depth": 1, "nextLink": None}],
            "itemsScanned": 7,
            "foldersDiscovered": 1,
            "pdfsDiscovered": 4,
            "graphPages": 2,
        },
    }


def test_full_sync_catchup_paginates_before_finalization() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    context = FakeOrchestrationContext({
        "orchestrationPhase": "catchup",
        "sourcePlan": source_plan,
        "runId": "run-1",
        "runEtag": "etag-1",
        "discoveredCount": 2,
        "itemsScanned": 4,
        "rejectedCount": 0,
        "baselineCandidate": BASELINE_CANDIDATE,
        "deltaPhase": "items",
        "phaseContinuationToken": "items-next",
        "totalSucceeded": 2,
        "totalFailed": 0,
        "totalPending": 0,
    })
    orchestrator = _orchestrator_generator(function_app.full_sync_orchestrator, context)

    catchup = next(orchestrator)
    assert catchup.name == "delta_sync_activity"
    assert catchup.payload["reducedItemsContinuationToken"] == "items-next"
    assert catchup.payload["processingPhase"] == "items"
    with pytest.raises(StopIteration):
        orchestrator.send({
            "bootstrapped": True,
            "failed": 0,
            "pending": 0,
            "continuationPhase": "items",
            "phaseContinuationToken": "items-after-next",
        })

    assert context.continued_input == {
        "orchestrationPhase": "catchup",
        "sourcePlan": source_plan,
        "runId": "run-1",
        "runEtag": "etag-1",
        "discoveredCount": 2,
        "itemsScanned": 4,
        "rejectedCount": 0,
        "baselineCandidate": BASELINE_CANDIDATE,
        "deltaPhase": "items",
        "phaseContinuationToken": "items-after-next",
        "totalSucceeded": 2,
        "totalFailed": 0,
        "totalPending": 0,
    }


def test_full_sync_commits_candidate_only_after_absence_and_parent_finalization() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    context = FakeOrchestrationContext({
        "orchestrationPhase": "catchup",
        "sourcePlan": source_plan,
        "runId": "run-1",
        "runEtag": "etag-1",
        "discoveredCount": 2,
        "itemsScanned": 4,
        "baselineCandidate": BASELINE_CANDIDATE,
        "deltaPhase": "items",
        "phaseContinuationToken": None,
        "totalSucceeded": 2,
        "totalFailed": 0,
        "totalPending": 0,
    })
    orchestrator = _orchestrator_generator(function_app.full_sync_orchestrator, context)

    catchup = next(orchestrator)
    assert catchup.name == "delta_sync_activity"
    absence = orchestrator.send({
        "failed": 0,
        "pending": 0,
        "continuationPhase": "commit",
        "phaseContinuationToken": "candidate-link",
    })
    assert absence.name == "full_sync_absence_reconcile_orchestrator"
    finalize = orchestrator.send({"status": "completed", "failed": 0})
    assert finalize.name == "finalize_run_activity"
    commit = orchestrator.send({"runStatus": "completed"})
    assert commit.name == "commit_delta_cursor_activity"
    assert commit.payload["candidateDeltaLink"] == "candidate-link"
    assert commit.payload["deltaExecutionId"] == catchup.payload["deltaExecutionId"]
    with pytest.raises(StopIteration) as stopped:
        orchestrator.send({"committed": True})

    assert stopped.value.value["status"] == "completed"


def test_reprovision_bootstrap_clears_latch_only_after_full_sync_commit() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "paused", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64, "reprovisionBootstrap": True,
    }
    context = FakeOrchestrationContext({
        "orchestrationPhase": "catchup",
        "sourcePlan": source_plan,
        "runId": "run-1",
        "runEtag": "etag-1",
        "discoveredCount": 2,
        "itemsScanned": 4,
        "baselineCandidate": BASELINE_CANDIDATE,
        "deltaPhase": "items",
        "phaseContinuationToken": None,
        "totalSucceeded": 2,
        "totalFailed": 0,
        "totalPending": 0,
    })
    orchestrator = _orchestrator_generator(function_app.full_sync_orchestrator, context)

    next(orchestrator)
    absence = orchestrator.send({
        "failed": 0,
        "pending": 0,
        "continuationPhase": "commit",
        "phaseContinuationToken": "candidate-link",
    })
    assert absence.name == "full_sync_absence_reconcile_orchestrator"
    finalize = orchestrator.send({"status": "completed", "failed": 0})
    assert finalize.name == "finalize_run_activity"
    commit = orchestrator.send({"runStatus": "completed"})
    assert commit.name == "commit_delta_cursor_activity"
    bootstrap = orchestrator.send({"committed": True})
    assert bootstrap.name == "complete_reprovision_full_sync_activity"
    assert bootstrap.payload["sourcePlan"] == source_plan
    with pytest.raises(StopIteration) as stopped:
        orchestrator.send({"completed": True})

    assert stopped.value.value["status"] == "completed"


@pytest.mark.parametrize("source_order", [("a", "b"), ("b", "a")])
def test_two_source_full_sync_completes_across_continuations_before_delta_dispatch(
    source_order: tuple[str, str],
) -> None:
    for suffix in source_order:
        plan = {
            "sourceId": f"source-{suffix}", "siteId": f"site-{suffix}",
            "siteUrl": f"https://tenant.sharepoint.com/sites/{suffix}",
            "driveId": f"drive-{suffix}", "desiredState": "active",
            "catalogEtag": f"catalog-{suffix}", "bindingDigest": suffix * 64,
        }
        owner = f"full-{suffix}"
        run_id = f"run-{suffix}"
        baseline = f"https://graph.microsoft.com/v1.0/drives/drive-{suffix}/root/delta?token=baseline"
        candidate = baseline.replace("token=baseline", "token=catchup")
        context = FakeOrchestrationContext(plan)
        context.instance_id = owner
        generator = _orchestrator_generator(function_app.full_sync_orchestrator, context)
        assert next(generator).name == "activate_run_activity"
        baseline_task = generator.send({"runId": run_id, "runEtag": f"etag-{suffix}"})
        assert baseline_task.payload["sourcePlan"] == plan
        assert baseline_task.payload["enumerateOnly"] is True
        with pytest.raises(StopIteration):
            generator.send({
                "continuationPhase": "baseline_complete", "phaseContinuationToken": baseline,
            })
        assert context.continued_input["baselineCandidate"] == baseline

        context = FakeOrchestrationContext(context.continued_input)
        context.instance_id = owner
        generator = _orchestrator_generator(function_app.full_sync_orchestrator, context)
        assert next(generator).name == "discover_all_activity"
        page = generator.send({"complete": True, "discoveredCount": 1, "itemsScanned": 1})
        assert page.payload["sourcePlan"] == plan
        claims = generator.send({
            "documents": [{"sourceRunId": f"source-{suffix}:{run_id}", "documentId": f"doc-{suffix}"}],
            "continuationToken": "next-page",
        })
        assert claims.name == "task_all"
        wave = generator.send([{"status": "acquired", "operationId": f"operation-{suffix}"}])
        wave_task = wave.payload[0]
        wave_task.result = [{"status": "succeeded"}]
        with pytest.raises(StopIteration):
            generator.send(wave_task)
        assert context.continued_input["orchestrationPhase"] == "process"
        assert context.continued_input["sourcePlan"] == plan
        assert context.continued_input["baselineCandidate"] == baseline
        assert context.continued_input["totalSucceeded"] == 1

        context = FakeOrchestrationContext(context.continued_input)
        context.instance_id = owner
        generator = _orchestrator_generator(function_app.full_sync_orchestrator, context)
        assert next(generator).payload["continuationToken"] == "next-page"
        catchup = generator.send({"documents": [], "continuationToken": None})
        assert catchup.name == "delta_sync_activity"
        assert catchup.payload["sourcePlan"] == plan
        assert catchup.payload["roundStartLink"] == baseline
        assert catchup.payload["deferCursorCommit"] is True
        with pytest.raises(StopIteration):
            generator.send({
                "failed": 0, "pending": 0,
                "continuationPhase": "acl", "phaseContinuationToken": f"acl-{suffix}",
            })

        context = FakeOrchestrationContext(context.continued_input)
        context.instance_id = owner
        generator = _orchestrator_generator(function_app.full_sync_orchestrator, context)
        catchup = next(generator)
        assert catchup.payload["sourcePlan"] == plan
        assert catchup.payload["processingPhase"] == "acl"
        assert catchup.payload["phaseContinuationToken"] == f"acl-{suffix}"
        absence = generator.send({
            "failed": 0, "pending": 0,
            "continuationPhase": "commit", "phaseContinuationToken": candidate,
        })
        assert absence.name == "full_sync_absence_reconcile_orchestrator"
        assert absence.payload["input"]["sourcePlan"] == plan
        finalize = generator.send({"status": "completed", "failed": 0})
        assert finalize.name == "finalize_run_activity"
        assert finalize.payload["runId"] == run_id
        commit = generator.send({"runStatus": "completed"})
        assert commit.name == "commit_delta_cursor_activity"
        assert commit.payload == {
            "sourcePlan": plan, "coordinatorOwnerId": owner,
            "deltaExecutionId": catchup.payload["deltaExecutionId"],
            "candidateDeltaLink": candidate, "parentRunId": run_id,
        }
        with pytest.raises(StopIteration) as stopped:
            generator.send({"committed": True})
        assert stopped.value.value["status"] == "completed"
        assert stopped.value.value["sourceId"] == plan["sourceId"]
        assert stopped.value.value["succeeded"] == 1
        assert stopped.value.value["failed"] == stopped.value.value["pending"] == 0

        delta_context = FakeOrchestrationContext(plan)
        delta_context.instance_id = f"delta-{suffix}"
        delta_generator = _orchestrator_generator(function_app.delta_sync_orchestrator, delta_context)
        delta_task = next(delta_generator)
        assert delta_task.name == "delta_sync_activity"
        assert delta_task.payload["sourcePlan"] == plan
        assert delta_task.payload["deltaExecutionId"] == delta_context.instance_id
        with pytest.raises(StopIteration):
            delta_generator.send({"failed": 0, "pending": 0})


def test_full_sync_timeout_drains_without_finalizing_run() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    documents = [{"sourceRunId": "source:run-1", "documentId": "doc-1"}]
    context, orchestrator = _discovery_phase(source_plan)

    next(orchestrator)
    orchestrator.send({"discoveredCount": 1, "itemsScanned": 1, "complete": True})
    claim_task = orchestrator.send({"documents": documents, "continuationToken": None})
    assert claim_task.name == "task_all"
    orchestrator.send([
        {"status": "acquired", "operationId": "operation-1"},
    ])
    timeout_fence = orchestrator.send(context.timer)
    assert timeout_fence.name == "timeout_wave_documents_activity"
    draining = orchestrator.send({"failedCount": 1})
    assert draining.name == "mark_drive_coordinator_draining_activity"

    with pytest.raises(StopIteration) as stopped:
        orchestrator.send({"draining": True})

    assert stopped.value.value["status"] == "draining"
    assert "finalize_run_activity" not in [task.name for task in context.calls]


def test_full_sync_continues_as_new_after_completed_document_page() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    documents = [{"sourceRunId": "source:run-1", "documentId": "doc-1"}]
    context, orchestrator = _discovery_phase(source_plan)

    next(orchestrator)
    page = orchestrator.send({"discoveredCount": 2, "itemsScanned": 3, "complete": True})
    assert page.name == "list_discovered_documents_page_activity"
    claim_batch = orchestrator.send({"documents": documents, "continuationToken": "next-page"})
    assert claim_batch.name == "task_all"
    wave = orchestrator.send([{
        "status": "acquired", "operationId": "operation-1",
    }])
    assert wave.name == "task_any"
    wave_task = wave.payload[0]
    wave_task.result = [{"status": "succeeded"}]

    with pytest.raises(StopIteration):
        orchestrator.send(wave_task)

    assert context.continued_input == {
        "orchestrationPhase": "process",
        "sourcePlan": source_plan,
        "runId": "run-1",
        "runEtag": "etag-1",
        "discoveredCount": 2,
        "itemsScanned": 3,
        "rejectedCount": 0,
        "baselineCandidate": BASELINE_CANDIDATE,
        "continuationToken": "next-page",
        "totalSucceeded": 1,
        "totalFailed": 0,
        "totalPending": 0,
    }


def test_full_sync_task_failure_schedules_exact_wave_timeout_fence() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    documents = [
        {"sourceRunId": "source:run-1", "documentId": "doc-1"},
        {"sourceRunId": "source:run-1", "documentId": "doc-2"},
    ]
    context, orchestrator = _discovery_phase(source_plan)
    next(orchestrator)
    orchestrator.send({"discoveredCount": 2, "itemsScanned": 2, "complete": True})
    claim_task = orchestrator.send({"documents": documents, "continuationToken": None})
    assert claim_task.name == "task_all"
    orchestrator.send([
        {"status": "acquired", "operationId": "operation-1"},
        {"status": "acquired", "operationId": "operation-2"},
    ])

    timeout_task = orchestrator.throw(RuntimeError("activity failed"))

    assert timeout_task.name == "timeout_wave_documents_activity"
    assert timeout_task.payload == {
        "documents": [
            {
                **documents[0], "operationId": "operation-1",
                "workId": "operation-1", "attemptId": "operation-1",
                "operationKind": "full-sync",
            },
            {
                **documents[1], "operationId": "operation-2",
                "workId": "operation-2", "attemptId": "operation-2",
                "operationKind": "full-sync",
            },
        ],
        "errorCode": "wave_retry_exhausted",
        "sourcePlan": source_plan,
        "coordinatorOwnerId": context.instance_id,
    }


def test_retry_timer_win_schedules_exact_document_timeout_fence() -> None:
    document = {"sourceRunId": "source:run-1", "documentId": "doc-1"}
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    context = FakeOrchestrationContext({
        "documents": [document], "sourcePlan": source_plan,
        "coordinatorOwnerId": "retry-owner",
    })
    orchestrator = _orchestrator_generator(function_app.retry_failed_orchestrator, context)

    claim_task = next(orchestrator)
    assert claim_task.name == "claim_document_work_activity"
    any_task = orchestrator.send({
        "status": "acquired", "operationId": "operation-1",
    })
    assert any_task.name == "task_any"
    timeout_task = orchestrator.send(context.timer)

    assert timeout_task.name == "timeout_wave_documents_activity"
    assert timeout_task.payload == {
        "documents": [{
            **document,
            "operationId": "operation-1",
            "workId": "operation-1",
            "attemptId": "operation-1",
            "operationKind": "retry-failed",
        }],
        "errorCode": "retry_timeout",
        "sourcePlan": source_plan,
        "coordinatorOwnerId": "retry-owner",
    }


def test_retry_orchestrator_continues_after_completed_claim_replay() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    documents = [
        {"sourceRunId": "source:run-1", "documentId": "doc-1"},
        {"sourceRunId": "source:run-1", "documentId": "doc-2"},
    ]
    context = FakeOrchestrationContext({
        "documents": documents, "sourcePlan": source_plan,
        "coordinatorOwnerId": "retry-owner",
    })
    orchestrator = _orchestrator_generator(function_app.retry_failed_orchestrator, context)

    claim_task = next(orchestrator)
    assert claim_task.name == "claim_document_work_activity"
    with pytest.raises(StopIteration):
        orchestrator.send({"status": "completed", "outcome": {"status": "succeeded"}})

    assert context.continued_input == {
        "orchestrationPhase": "retry-failed",
        "documents": documents,
        "sourcePlan": source_plan,
        "coordinatorOwnerId": "retry-owner",
        "nextDocumentIndex": 1,
        "succeeded": 1,
        "failed": 0,
        "pending": 0,
        "draining": False,
    }


def test_paged_retry_orchestrator_compacts_history_after_one_document() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    context = FakeOrchestrationContext({
        "retryPaged": True,
        "sourcePlan": source_plan,
        "coordinatorOwnerId": "retry-owner",
        "runId": "run-1",
        "operationKind": "retry-failed:retry-owner",
        "continuationToken": None,
    })
    orchestrator = _orchestrator_generator(function_app.retry_failed_orchestrator, context)
    prepare = next(orchestrator)
    assert prepare.name == "prepare_retry_failed_page_activity"
    process_race = orchestrator.send({
        "status": "ready",
        "document": {"sourceRunId": "source:run-1", "documentId": "doc-1"},
        "operationId": "operation-1",
        "workId": "operation-1", "attemptId": "operation-1",
        "operationKind": "retry-failed:retry-owner",
        "continuationToken": "failed-page-2",
    })
    assert process_race.name == "task_any"
    process_task = process_race.payload[0]
    process_task.result = {"status": "succeeded"}

    with pytest.raises(StopIteration):
        orchestrator.send(process_task)

    assert context.continued_input == {
        "retryPaged": True,
        "sourcePlan": source_plan,
        "coordinatorOwnerId": "retry-owner",
        "runId": "run-1",
        "operationKind": "retry-failed:retry-owner",
        "continuationToken": "failed-page-2",
        "succeeded": 1,
        "failed": 0,
        "pending": 0,
    }


def test_full_sync_reports_finalization_failure_and_withholds_gate() -> None:
    source_plan = {
        "sourceId": "source-a", "siteId": "site-a",
        "siteUrl": "https://tenant.sharepoint.com/sites/a", "driveId": "drive-a",
        "desiredState": "active", "catalogEtag": "catalog-etag",
        "bindingDigest": "a" * 64,
    }
    context, orchestrator = _discovery_phase(source_plan, "run-a", "run-etag")

    next(orchestrator)
    page_task = orchestrator.send({"discoveredCount": 0, "itemsScanned": 0, "complete": True})
    assert page_task.name == "list_discovered_documents_page_activity"
    catch_up_task = orchestrator.send({"documents": [], "continuationToken": None})
    assert catch_up_task.name == "delta_sync_activity"
    absence_task = orchestrator.send({"failed": 0, "pending": 0})
    assert absence_task.name == "full_sync_absence_reconcile_orchestrator"
    finalize_task = orchestrator.send({"status": "completed", "failed": 0})
    assert finalize_task.name == "finalize_run_activity"
    settlement_task = orchestrator.send({"runStatus": "finalization_failed"})
    assert settlement_task.name == "mark_drive_coordinator_draining_activity"

    with pytest.raises(StopIteration) as stopped:
        orchestrator.send({"settled": True})

    assert stopped.value.value["status"] == "failed"
    assert stopped.value.value["failed"] == 1