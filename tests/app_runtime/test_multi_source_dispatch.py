from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import azure.functions as func
import pytest

import config as config_module
import function_app
from ingestion.source_registry import SourcePlan, SourceRegistryConflictError, SourceRegistryError
from source_registry import SharePointSource, source_binding_digest


TENANT_ID = "tenant-id"


def _no_graph(*_args: Any, **_kwargs: Any) -> None:
    pytest.fail("triggers must not call Graph validation")


def _plan(source_id: str, site_id: str, site_path: str, drive_id: str, state: str = "active") -> SourcePlan:
    source = SharePointSource(
        source_id=source_id,
        site_id=site_id,
        site_url=f"https://tenant.sharepoint.com/sites/{site_path}",
        drive_id=drive_id,
        desired_state=state,
    )
    return SourcePlan(
        source,
        f"catalog-{source_id}",
        source_binding_digest(source, TENANT_ID),
    )


def test_source_preflight_checks_acceptance_without_graph_and_stages_a_bounded_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plans = (
        _plan("source-a", "graph-site-a", "a", "drive-a"),
        _plan("source-b", "graph-site-a", "a", "drive-b", "paused"),
        _plan("source-c", "graph-site-b", "b", "drive-c", "retired"),
    )
    config = SimpleNamespace(acl_max_pages=7)
    staged: list[tuple[str, str, dict[str, str] | None]] = []

    class Registry:
        def begin_source_dispatch(self, _dispatch_id):
            return None

        def list_source_inventory_page(self, **kwargs):
            assert kwargs == {"page_size": function_app.SOURCE_DISPATCH_PAGE_SIZE, "continuation_token": None}
            return SimpleNamespace(
                plans=plans, rejected_source_ids=(), unattributed_errors=0,
                duplicate_drive_source_ids=(), continuation_token=None,
            )

        def read_source_plan(self, source_id):
            return next(plan for plan in plans if plan.source_id == source_id)

        def stage_source_dispatch_item(self, dispatch_id, source_id, *, status, source_payload=None, outcome=None):
            staged.append((source_id, status, source_payload))

    monkeypatch.setattr(config_module, "load_config", lambda: config)
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())

    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", _no_graph)

    result = function_app.prepare_source_inventory_page_activity({"dispatchId": "dispatch-a"})

    assert result["counts"] == {
        "enumerated": 3, "active": 1, "paused": 1, "retired": 1,
        "failed": 0, "unattributed": 0,
    }
    assert [source_id for source_id, _, _ in staged] == ["source-a", "source-b", "source-c"]
    assert [status for _, status, _ in staged] == ["prepared", "paused", "retired"]
    assert [payload for _, _, payload in staged] == [plan.to_payload() for plan in plans]


def test_source_preflight_duplicate_drive_fails_only_the_duplicate_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = _plan("source-a", "site-a", "a", "drive-a")
    other = _plan("source-c", "site-c", "c", "drive-c")
    staged: list[tuple[str, str, dict | None]] = []

    class Registry:
        def begin_source_dispatch(self, _dispatch_id):
            return None

        def list_source_inventory_page(self, **_kwargs):
            return SimpleNamespace(
                plans=(owner, other), rejected_source_ids=(), unattributed_errors=0,
                duplicate_drive_source_ids=("source-b",), continuation_token=None,
            )

        def read_source_plan(self, source_id):
            return {owner.source_id: owner, other.source_id: other}[source_id]

        def stage_source_dispatch_item(self, _dispatch_id, source_id, *, status, outcome=None, **_kwargs):
            staged.append((source_id, status, outcome))

        def fail_source_dispatch(self, *_args):
            pytest.fail("a duplicate drive must not fail the whole dispatch")

    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace(acl_max_pages=7))
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", _no_graph)

    result = function_app.prepare_source_inventory_page_activity({"dispatchId": "dispatch-a"})

    assert result["counts"] == {
        "enumerated": 3, "active": 2, "paused": 0, "retired": 0,
        "failed": 1, "unattributed": 0,
    }
    assert [(source_id, status) for source_id, status, _ in staged] == [
        ("source-a", "prepared"), ("source-c", "prepared"), ("source-b", "failed"),
    ]
    assert staged[2][2] == {"status": "failed", "error": "sharepoint_drive_already_registered"}


def test_source_plan_loader_scans_inventory_once_and_checks_acceptance_without_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _plan("source-a", "site-a", "a", "drive-a")
    second = _plan("source-b", "site-b", "b", "drive-b")
    scans: list[str | None] = []
    accepted: list[str] = []

    class Registry:
        def list_source_inventory_page(self, *, page_size, continuation_token):
            scans.append(continuation_token)
            if continuation_token is None:
                return SimpleNamespace(
                    plans=(first,), duplicate_drive_source_ids=(),
                    continuation_token="page-2",
                )
            return SimpleNamespace(
                plans=(second,), duplicate_drive_source_ids=(),
                continuation_token=None,
            )

        def read_source_plan(self, source_id):
            accepted.append(source_id)
            return {first.source_id: first, second.source_id: second}[source_id]

    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", _no_graph)

    plans = tuple(function_app._load_validated_source_plans(SimpleNamespace(acl_max_pages=5)))

    assert [plan.source_id for plan in plans] == ["source-a", "source-b"]
    assert scans == [None, "page-2"]
    assert accepted == ["source-a", "source-b"]


def test_source_plan_loader_skips_duplicate_drive_source_and_yields_others(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = _plan("source-a", "site-a", "a", "drive-a")
    other = _plan("source-c", "site-c", "c", "drive-c")

    class Registry:
        def list_source_inventory_page(self, *, page_size, continuation_token):
            if continuation_token is None:
                return SimpleNamespace(
                    plans=(owner,), duplicate_drive_source_ids=(),
                    continuation_token="page-2",
                )
            return SimpleNamespace(
                plans=(other,), duplicate_drive_source_ids=("source-b",),
                continuation_token=None,
            )

        def read_source_plan(self, source_id):
            return {owner.source_id: owner, other.source_id: other}[source_id]

    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", _no_graph)

    plans = tuple(function_app._load_validated_source_plans(SimpleNamespace(acl_max_pages=5)))

    assert [plan.source_id for plan in plans] == ["source-a", "source-c"]


def test_source_plan_loader_skips_unaccepted_source_without_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accepted = _plan("source-a", "site-a", "a", "drive-a")
    unaccepted = _plan("source-b", "site-b", "b", "drive-b")

    class Registry:
        def list_source_inventory_page(self, **_kwargs):
            return SimpleNamespace(
                plans=(unaccepted, accepted), duplicate_drive_source_ids=(),
                continuation_token=None,
            )

        def read_source_plan(self, source_id):
            if source_id == "source-b":
                raise SourceRegistryError("source_binding_not_accepted")
            return accepted

    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_graph_client", _no_graph)
    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", _no_graph)

    plans = tuple(function_app._load_validated_source_plans(SimpleNamespace(acl_max_pages=5)))

    assert plans == (accepted,)


def test_source_plan_loader_propagates_transient_registry_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a")

    class Registry:
        def list_source_inventory_page(self, **_kwargs):
            return SimpleNamespace(
                plans=(plan,), duplicate_drive_source_ids=(), continuation_token=None,
            )

        def read_source_plan(self, _source_id):
            raise RuntimeError("cosmos unavailable")

    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())

    with pytest.raises(RuntimeError, match="cosmos unavailable"):
        tuple(function_app._load_validated_source_plans(SimpleNamespace()))


@pytest.mark.parametrize("handler_name", ["webhook_sharepoint", "webhook_lifecycle"])
def test_webhook_returns_503_on_transient_registry_failure_so_graph_retries(
    monkeypatch: pytest.MonkeyPatch, handler_name: str,
) -> None:
    def failing_plans(_config):
        raise RuntimeError("cosmos unavailable")
        yield

    class LifecycleRepository:
        def get_webhook_subscription_id(self, _source_id):
            return "subscription-a"

    monkeypatch.setattr(function_app, "WEBHOOK_CLIENT_STATE", "client-state")
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_load_validated_source_plans", failing_plans)
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: LifecycleRepository())
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setenv("FUNCTION_PUBLIC_BASE_URL", "https://function.invalid")
    monkeypatch.setattr("ingestion.subscription.validate_webhook_notification", lambda *_args: True)
    body = {"value": [{"subscriptionId": "subscription-a", "lifecycleEvent": "reauthorizationRequired"}]}
    request = func.HttpRequest(
        method="POST",
        url=f"https://function.invalid/api/{handler_name}",
        body=json.dumps(body).encode(),
    )

    handler = inspect.unwrap(getattr(function_app, handler_name)._function._func)
    args = (request,) if handler_name == "webhook_lifecycle" else (request, object())
    response = asyncio.run(handler(*args))

    assert response.status_code == 503


def test_lifecycle_webhook_ignores_unknown_subscription_and_handles_known_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a")
    renewed: list[str] = []

    class LifecycleRepository:
        def get_webhook_subscription_id(self, _source_id):
            return "subscription-a"

    monkeypatch.setattr(function_app, "WEBHOOK_CLIENT_STATE", "client-state")
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: LifecycleRepository())
    monkeypatch.setattr(function_app, "_load_validated_source_plans", lambda _config: (plan,))
    monkeypatch.setenv("FUNCTION_PUBLIC_BASE_URL", "https://function.invalid")
    monkeypatch.setattr("ingestion.subscription.validate_webhook_notification", lambda *_args: True)
    monkeypatch.setattr(
        "ingestion.subscription.renew_subscription",
        lambda _client, subscription_id: renewed.append(subscription_id),
    )
    monkeypatch.setattr(
        "ingestion.source_registry.config_for_source",
        lambda _config, item: SimpleNamespace(drive_id=item.source.drive_id),
    )
    body = {"value": [
        {"subscriptionId": "unknown", "lifecycleEvent": "reauthorizationRequired"},
        {"subscriptionId": "subscription-a", "lifecycleEvent": "reauthorizationRequired"},
    ]}
    request = func.HttpRequest(
        method="POST",
        url="https://function.invalid/api/webhook/lifecycle",
        body=json.dumps(body).encode(),
    )

    handler = inspect.unwrap(function_app.webhook_lifecycle._function._func)
    response = asyncio.run(handler(request))

    assert response.status_code == 200
    assert renewed == ["subscription-a"]


def test_one_unaccepted_source_stages_failure_without_blocking_other_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plans = (
        _plan("source-a", "site-a", "a", "drive-a"),
        _plan("source-b", "site-b", "b", "drive-b"),
    )
    config = SimpleNamespace(acl_max_pages=7)

    staged: list[tuple[str, str]] = []

    class Registry:
        def begin_source_dispatch(self, _dispatch_id):
            return None

        def list_source_inventory_page(self, **_kwargs):
            return SimpleNamespace(
                plans=plans, rejected_source_ids=(), unattributed_errors=0,
                duplicate_drive_source_ids=(), continuation_token=None,
            )

        def read_source_plan(self, source_id):
            if source_id == "source-b":
                raise SourceRegistryConflictError("source_binding_is_immutable")
            return next(plan for plan in plans if plan.source_id == source_id)

        def stage_source_dispatch_item(self, _dispatch_id, source_id, *, status, **_kwargs):
            staged.append((source_id, status))

    monkeypatch.setattr(config_module, "load_config", lambda: config)
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())

    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", _no_graph)

    result = function_app.prepare_source_inventory_page_activity({"dispatchId": "dispatch-a"})

    assert result["counts"]["active"] == 1
    assert result["counts"]["failed"] == 1
    assert staged == [("source-a", "prepared"), ("source-b", "failed")]


def test_invalid_catalog_record_isolated_and_activity_output_stays_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid = _plan("source-a", "site-a", "a", "drive-a")
    staged: list[tuple[str, str]] = []

    class Registry:
        def begin_source_dispatch(self, _dispatch_id):
            return None

        def list_source_inventory_page(self, **_kwargs):
            return SimpleNamespace(
                plans=(valid,),
                rejected_source_ids=("source-invalid",),
                unattributed_errors=1,
                duplicate_drive_source_ids=(),
                continuation_token="page-2",
            )

        def read_source_plan(self, source_id):
            assert source_id == valid.source_id
            return valid

        def stage_source_dispatch_item(self, _dispatch_id, source_id, *, status, **_kwargs):
            staged.append((source_id, status))

    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace(acl_max_pages=7))
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr("ingestion.graph.validate_sharepoint_drive_site", _no_graph)

    result = function_app.prepare_source_inventory_page_activity({
        "dispatchId": "dispatch-a",
        "continuationToken": None,
    })

    assert result == {
        "counts": {
            "enumerated": 3,
            "active": 1,
            "paused": 0,
            "retired": 0,
            "failed": 1,
            "unattributed": 1,
        },
        "continuationToken": "page-2",
    }
    assert staged == [("source-a", "prepared"), ("source-invalid", "failed")]


def test_subscription_renewal_failure_isolated_to_one_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plans = (
        _plan("source-a", "site-a", "a", "drive-a"),
        _plan("source-b", "site-b", "b", "drive-b"),
    )
    saved: list[tuple[str, str]] = []

    class LifecycleRepository:
        def get_webhook_subscription_id(self, _source_id):
            return None

        def save_webhook_subscription_id(self, source_id, subscription_id):
            saved.append((source_id, subscription_id))

    monkeypatch.setattr(function_app, "WEBHOOK_CLIENT_STATE", "client-state")
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: LifecycleRepository())
    monkeypatch.setattr(function_app, "_load_validated_source_plans", lambda _config: plans)
    monkeypatch.setenv("FUNCTION_PUBLIC_BASE_URL", "https://function.invalid")
    monkeypatch.setattr(
        "ingestion.source_registry.config_for_source",
        lambda _config, plan: SimpleNamespace(drive_id=plan.source.drive_id),
    )

    def reconcile(_client, drive_id, *_args):
        if drive_id == "drive-a":
            raise RuntimeError("subscription API unavailable")
        return SimpleNamespace(subscription_id="subscription-b", expiration="2026-10-01")

    monkeypatch.setattr("ingestion.subscription.reconcile_drive_subscription", reconcile)

    asyncio.run(function_app.subscription_renew_timer(SimpleNamespace(isPastDue=False)))

    assert saved == [("source-b", "subscription-b")]


@pytest.mark.parametrize(
    ("timer_name", "orchestrator_name"),
    [
        ("acl_resync_timer", "acl_resync_orchestrator"),
        ("reconciliation_timer", "delta_sync_orchestrator"),
    ],
)
def test_timer_starts_only_active_sources_without_running_full_sync(
    monkeypatch: pytest.MonkeyPatch,
    timer_name: str,
    orchestrator_name: str,
) -> None:
    plans = (
        _plan("source-a", "site-a", "a", "drive-a"),
        _plan("source-paused", "site-p", "p", "drive-p", state="paused"),
        _plan("source-full", "site-f", "f", "drive-f"),
        _plan("source-b", "site-b", "b", "drive-b"),
    )
    started: list[tuple[str, str]] = []
    source_registry = object()

    async def start(_client, _repository, source_id, _trigger, orchestrator, **kwargs):
        assert kwargs["source_registry"] is source_registry
        started.append((source_id, orchestrator))

    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: object())
    monkeypatch.setattr(
        function_app, "_build_source_registry_repository", lambda _config: source_registry
    )
    monkeypatch.setattr(function_app, "_load_validated_source_plans", lambda _config: plans)
    monkeypatch.setattr(
        "ingestion.source_registry.config_for_source",
        lambda _config, plan: SimpleNamespace(source_id=plan.source_id),
    )
    monkeypatch.setattr(
        function_app, "_full_sync_is_running", lambda _repo, source_id: source_id == "source-full"
    )
    monkeypatch.setattr(function_app, "_start_if_not_running", start)

    inner = inspect.unwrap(getattr(function_app, timer_name)._function._func)
    asyncio.run(inner(SimpleNamespace(), object()))

    assert started == [
        ("source-a", orchestrator_name),
        ("source-b", orchestrator_name),
    ]


@pytest.mark.parametrize(
    ("timer_name", "orchestrator_name"),
    [
        ("acl_resync_timer", "acl_resync_orchestrator"),
        ("reconciliation_timer", "delta_sync_orchestrator"),
        ("lifecycle_reconcile_timer", "lifecycle_reconcile_orchestrator"),
    ],
)
def test_timer_source_dispatch_failure_does_not_block_later_source(
    monkeypatch: pytest.MonkeyPatch,
    timer_name: str,
    orchestrator_name: str,
) -> None:
    plans = (
        _plan("source-a", "site-a", "a", "drive-a"),
        _plan("source-b", "site-b", "b", "drive-b"),
    )
    started: list[str] = []

    async def start(_client, _repository, source_id, _trigger, orchestrator, **_kwargs):
        assert orchestrator == orchestrator_name
        if source_id == "source-a":
            raise RuntimeError("source-a unavailable")
        started.append(source_id)

    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_load_validated_source_plans", lambda _config: plans)
    monkeypatch.setattr(
        "ingestion.source_registry.config_for_source",
        lambda _config, plan: SimpleNamespace(source_id=plan.source_id),
    )
    monkeypatch.setattr(function_app, "_full_sync_is_running", lambda *_args: False)
    monkeypatch.setattr(function_app, "_start_if_not_running", start)

    inner = inspect.unwrap(getattr(function_app, timer_name)._function._func)
    asyncio.run(inner(SimpleNamespace(), object()))

    assert started == ["source-b"]


def test_webhook_source_dispatch_failure_does_not_block_later_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plans = (
        _plan("source-a", "site-a", "a", "drive-a"),
        _plan("source-b", "site-b", "b", "drive-b"),
    )
    started: list[str] = []

    class LifecycleRepository:
        def get_webhook_subscription_id(self, source_id):
            return f"subscription-{source_id}"

    async def start(_client, _repository, source_id, *_args, **_kwargs):
        if source_id == "source-a":
            raise RuntimeError("source-a unavailable")
        started.append(source_id)

    monkeypatch.setattr(function_app, "WEBHOOK_CLIENT_STATE", "client-state")
    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_load_validated_source_plans", lambda _config: plans)
    monkeypatch.setattr(
        function_app, "_build_lifecycle_repository", lambda _config: LifecycleRepository(),
    )
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: object())
    monkeypatch.setattr(
        "ingestion.source_registry.config_for_source",
        lambda _config, plan: SimpleNamespace(source_id=plan.source_id),
    )
    monkeypatch.setattr(function_app, "_full_sync_is_running", lambda *_args: False)
    monkeypatch.setattr(function_app, "_start_if_not_running", start)
    monkeypatch.setattr(function_app, "_write_webhook_audit", lambda *_args: None)
    monkeypatch.setattr("ingestion.subscription.validate_webhook_notification", lambda *_args: True)
    request = func.HttpRequest(
        method="POST",
        url="https://function.invalid/api/webhook/sharepoint",
        body=json.dumps({"value": [
            {"subscriptionId": "subscription-source-a"},
            {"subscriptionId": "subscription-source-b"},
        ]}).encode(),
    )

    handler = inspect.unwrap(function_app.webhook_sharepoint._function._func)
    response = asyncio.run(handler(request, object()))

    assert response.status_code == 503
    assert started == ["source-b"]


class _Task:
    def __init__(self, name: str, payload: Any) -> None:
        self.name = name
        self.payload = payload
        self.result: Any = None


class _AnyTask:
    name = "task_any"

    def __init__(self, tasks: list[_Task]) -> None:
        self.tasks = tasks


class _Context:
    instance_id = "root-instance"

    def __init__(self, input_value: Any = None) -> None:
        self.children: list[tuple[str, Any, str]] = []
        self.input_value = input_value
        self.continued_input: Any = None

    def get_input(self) -> Any:
        return self.input_value

    def call_activity(self, name: str, payload: Any) -> _Task:
        return _Task(name, payload)

    def call_sub_orchestrator(self, name: str, input_: Any, instance_id: str) -> _Task:
        self.children.append((name, input_, instance_id))
        return _Task(name, input_)

    def task_any(self, tasks: list[_Task]) -> _AnyTask:
        return _AnyTask(list(tasks))

    def continue_as_new(self, input_value: Any) -> None:
        self.continued_input = input_value


def test_preflight_continues_without_dispatch_until_catalog_scan_finishes() -> None:
    context = _Context()
    orchestrator = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(context)
    page_task = next(orchestrator)
    assert page_task.name == "prepare_source_inventory_page_activity"
    with pytest.raises(StopIteration):
        orchestrator.send({
            "counts": {"enumerated": 1, "active": 1, "paused": 0, "retired": 0, "failed": 0, "unattributed": 0},
            "continuationToken": "next-page",
        })

    assert context.children == []
    assert context.continued_input == {
        "phase": "preflight",
        "continuationToken": "next-page",
        "inventoryCounts": {"enumerated": 1, "active": 1, "paused": 0, "retired": 0, "failed": 0, "unattributed": 0},
    }


def test_multi_page_inventory_dispatches_each_active_source_once_after_validation() -> None:
    first_preflight = _Context()
    first = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(
        first_preflight,
    )
    assert next(first).name == "prepare_source_inventory_page_activity"
    with pytest.raises(StopIteration):
        first.send({
            "counts": {
                "enumerated": 1,
                "active": 1,
                "paused": 0,
                "retired": 0,
                "failed": 0,
                "unattributed": 0,
            },
            "continuationToken": "inventory-page-2",
        })

    second_preflight = _Context(first_preflight.continued_input)
    second = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(
        second_preflight,
    )
    assert next(second).name == "prepare_source_inventory_page_activity"
    complete = second.send({
        "counts": {
            "enumerated": 2,
            "active": 1,
            "paused": 0,
            "retired": 0,
            "failed": 1,
            "unattributed": 0,
        },
        "continuationToken": None,
    })
    assert complete.name == "complete_source_dispatch_activity"
    with pytest.raises(StopIteration):
        second.send({"status": "ready"})

    assert first_preflight.children == []
    assert second_preflight.children == []
    assert second_preflight.continued_input["inventoryCounts"] == {
        "enumerated": 3,
        "active": 2,
        "paused": 0,
        "retired": 0,
        "failed": 1,
        "unattributed": 0,
    }

    dispatch_context = _Context(second_preflight.continued_input)
    dispatch = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(
        dispatch_context,
    )
    assert next(dispatch).name == "list_source_dispatch_page_activity"
    source_a = _plan("source-a", "site-a", "a", "drive-a").to_payload()
    source_b = _plan("source-b", "site-b", "b", "drive-b").to_payload()
    second_page = dispatch.send({
        "items": [{"sourceId": "source-a", "sourcePlan": source_a, "status": "prepared"}],
        "continuationToken": "dispatch-page-2",
    })
    assert second_page.name == "list_source_dispatch_page_activity"
    waiting = dispatch.send({
        "items": [{"sourceId": "source-b", "sourcePlan": source_b, "status": "prepared"}],
        "continuationToken": None,
    })
    assert waiting.name == "task_any"
    _finish_children(dispatch, waiting, {"source-a": {"status": "completed"}, "source-b": {"status": "completed"}})

    assert [child[1]["sourceId"] for child in dispatch_context.children] == ["source-a", "source-b"]
    assert len({child[2] for child in dispatch_context.children}) == 2
    assert dispatch_context.continued_input is None


def _finish_children(orchestrator: Any, waiting: Any, outcomes: dict[str, Any]) -> dict:
    """Complete in-flight children in the given order and return the orchestrator result."""
    recorded: list[dict] = []
    for source_id, outcome in outcomes.items():
        assert waiting.name == "task_any", waiting.name
        winner = next(task for task in waiting.tasks if task.payload["sourceId"] == source_id)
        winner.result = outcome
        record = orchestrator.send(winner)
        assert record.name == "record_source_dispatch_outcome_activity"
        recorded.append(record.payload)
        try:
            waiting = orchestrator.send({"status": "recorded"})
        except StopIteration as stopped:
            assert source_id == list(outcomes)[-1]
            return {"result": stopped.value, "recorded": recorded}
    raise AssertionError(f"orchestrator still running: {waiting.name}")


def test_late_duplicate_catalog_page_rejects_before_any_source_dispatch() -> None:
    first_context = _Context()
    first = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(
        first_context,
    )
    assert next(first).name == "prepare_source_inventory_page_activity"
    with pytest.raises(StopIteration):
        first.send({
            "counts": {
                "enumerated": 1,
                "active": 1,
                "paused": 0,
                "retired": 0,
                "failed": 0,
                "unattributed": 0,
            },
            "continuationToken": "page-2",
        })

    second_context = _Context(first_context.continued_input)
    second = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(
        second_context,
    )
    assert next(second).name == "prepare_source_inventory_page_activity"
    with pytest.raises(StopIteration) as stopped:
        second.send({"error": "source_registry_validation_failed"})

    assert stopped.value.value == {
        "status": "failed",
        "phase": "source_registry",
        "error": "source_registry_validation_failed",
    }
    assert first_context.children == []
    assert second_context.children == []


def test_empty_registry_fails_before_entering_dispatch_phase() -> None:
    context = _Context()
    orchestrator = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(context)
    prepare = next(orchestrator)
    assert prepare.name == "prepare_source_inventory_page_activity"
    complete = orchestrator.send({
        "counts": {"enumerated": 0, "active": 0, "paused": 0, "retired": 0, "failed": 0, "unattributed": 0},
        "continuationToken": None,
    })
    assert complete.name == "complete_source_dispatch_activity"
    with pytest.raises(StopIteration) as stopped:
        orchestrator.send({"status": "ready"})

    assert stopped.value.value == {
        "status": "failed", "phase": "source_registry", "error": "source_registry_empty",
    }
    assert context.children == []


def _dispatch_context(continuation_token: str | None = None) -> _Context:
    return _Context({
        "phase": "dispatch",
        "inventoryCounts": {
            "enumerated": 3, "active": 3, "paused": 0, "retired": 0,
            "failed": 0, "unattributed": 0,
        },
        "dispatchCounts": {"completed": 0, "failed": 0, "pending": 0},
        "continuationToken": continuation_token,
    })


def _prepared(*source_ids: str) -> list[dict]:
    items = []
    for index, source_id in enumerate(source_ids):
        plan = _plan(source_id, f"site-{index}", str(index), f"drive-{source_id}").to_payload()
        items.append({"sourceId": source_id, "sourcePlan": plan, "status": "prepared"})
    return items


def test_dispatch_starts_next_page_source_while_earlier_source_still_runs() -> None:
    context = _dispatch_context()
    orchestrator = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(context)
    assert next(orchestrator).name == "list_source_dispatch_page_activity"
    next_page = orchestrator.send({"items": _prepared("source-a"), "continuationToken": "page-2"})

    assert next_page.name == "list_source_dispatch_page_activity"
    assert next_page.payload["continuationToken"] == "page-2"
    waiting = orchestrator.send({"items": _prepared("source-b"), "continuationToken": None})
    assert [child[1]["sourceId"] for child in context.children] == ["source-a", "source-b"]

    finished = _finish_children(orchestrator, waiting, {
        "source-b": {"status": "completed"},
        "source-a": {"status": "completed"},
    })

    assert [item["sourceId"] for item in finished["recorded"]] == ["source-b", "source-a"]
    assert finished["result"]["drives"] == {"completed": 2, "failed": 0, "pending": 0}
    assert context.continued_input is None


def test_failed_source_is_recorded_without_blocking_other_sources() -> None:
    context = _dispatch_context()
    orchestrator = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(context)
    assert next(orchestrator).name == "list_source_dispatch_page_activity"
    waiting = orchestrator.send({"items": _prepared("source-a", "source-b"), "continuationToken": None})

    finished = _finish_children(orchestrator, waiting, {
        "source-a": RuntimeError("child failed"),
        "source-b": {"status": "completed", "sourceId": "source-b"},
    })

    assert [(item["sourceId"], item["status"]) for item in finished["recorded"]] == [
        ("source-a", "failed"), ("source-b", "completed"),
    ]
    assert finished["recorded"][0]["outcome"] == {"status": "failed", "error": "source_orchestration_failed"}
    assert len({call[2] for call in context.children}) == 2
    assert finished["result"] == {
        "status": "partial",
        "dispatchId": "root-instance",
        "inventory": context.input_value["inventoryCounts"],
        "drives": {"completed": 1, "failed": 1, "pending": 0},
        "outcomePageSize": function_app.SOURCE_DISPATCH_PAGE_SIZE,
    }


def test_dispatch_fan_out_is_bounded_by_page_size(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(function_app, "SOURCE_DISPATCH_PAGE_SIZE", 2)
    context = _dispatch_context()
    orchestrator = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(context)
    assert next(orchestrator).name == "list_source_dispatch_page_activity"
    waiting = orchestrator.send({
        "items": _prepared("source-a", "source-b", "source-c"), "continuationToken": "page-2",
    })

    assert waiting.name == "task_any"
    assert [child[1]["sourceId"] for child in context.children] == ["source-a", "source-b"]
    winner = next(task for task in waiting.tasks if task.payload["sourceId"] == "source-a")
    winner.result = {"status": "completed"}
    assert orchestrator.send(winner).name == "record_source_dispatch_outcome_activity"
    waiting = orchestrator.send({"status": "recorded"})

    assert waiting.name == "task_any"
    assert [child[1]["sourceId"] for child in context.children] == ["source-a", "source-b", "source-c"]
    assert len(waiting.tasks) == 2


def test_dispatch_page_error_drains_and_records_in_flight_sources() -> None:
    context = _dispatch_context()
    orchestrator = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(context)
    assert next(orchestrator).name == "list_source_dispatch_page_activity"
    assert orchestrator.send({
        "items": _prepared("source-a"), "continuationToken": "page-2",
    }).name == "list_source_dispatch_page_activity"
    waiting = orchestrator.send({"error": "source_dispatch_page_unavailable"})

    finished = _finish_children(orchestrator, waiting, {"source-a": {"status": "completed"}})

    assert [item["sourceId"] for item in finished["recorded"]] == ["source-a"]
    assert finished["result"] == {
        "status": "failed", "phase": "source_dispatch", "error": "source_dispatch_page_unavailable",
    }


def test_replayed_dispatch_page_does_not_redispatch_settled_source() -> None:
    inventory_counts = {
        "enumerated": 1,
        "active": 1,
        "paused": 0,
        "retired": 0,
        "failed": 0,
        "unattributed": 0,
    }
    context = _Context({
        "phase": "dispatch",
        "inventoryCounts": inventory_counts,
        "dispatchCounts": {"completed": 1, "failed": 0, "pending": 0},
        "continuationToken": None,
    })
    orchestrator = function_app.multi_source_full_sync_orchestrator._function._func.orchestrator_function(
        context,
    )
    assert next(orchestrator).name == "list_source_dispatch_page_activity"
    with pytest.raises(StopIteration) as stopped:
        orchestrator.send({
            "items": [{
                "sourceId": "source-a",
                "sourcePlan": _plan("source-a", "site-a", "a", "drive-a").to_payload(),
                "status": "completed",
            }],
            "continuationToken": None,
        })

    assert context.children == []
    assert stopped.value.value["drives"] == {
        "completed": 1,
        "failed": 0,
        "pending": 0,
    }


def test_delta_orchestrator_compacts_each_reduced_item_page() -> None:
    source_plan = _plan("source-a", "site-a", "a", "drive-a").to_payload()
    context = _Context(source_plan)
    orchestrator = function_app.delta_sync_orchestrator._function._func.orchestrator_function(context)

    first_page = next(orchestrator)
    assert first_page.name == "delta_sync_activity"
    assert first_page.payload["reducedItemsContinuationToken"] is None
    with pytest.raises(StopIteration):
        orchestrator.send({
            "bootstrapped": True,
            "createdOrUpdated": 2,
            "deleted": 1,
            "aclResynced": 0,
            "failed": 0,
            "pending": 0,
            "itemsSeen": 90,
            "reducedItemsContinuationToken": "items-page-2",
            "continuationPhase": "items",
            "phaseContinuationToken": "items-page-2",
        })
    assert context.continued_input == {
        "orchestrationPhase": "delta",
        "sourcePlan": source_plan,
        "deltaPhase": "items",
        "phaseContinuationToken": "items-page-2",
        "totals": {
            "createdOrUpdated": 2,
            "deleted": 1,
            "aclResynced": 0,
            "failed": 0,
            "pending": 0,
            "bootstrapped": True,
            "itemsSeen": 90,
        },
    }

    resumed_context = _Context(context.continued_input)
    resumed = function_app.delta_sync_orchestrator._function._func.orchestrator_function(resumed_context)
    final_page = next(resumed)
    assert final_page.payload["reducedItemsContinuationToken"] == "items-page-2"
    assert final_page.payload["processingPhase"] == "items"
    with pytest.raises(StopIteration) as stopped:
        resumed.send({
            "bootstrapped": True,
            "createdOrUpdated": 1,
            "deleted": 0,
            "aclResynced": 3,
            "failed": 0,
            "pending": 0,
            "itemsSeen": 90,
            "reducedItemsContinuationToken": None,
            "continuationPhase": None,
            "phaseContinuationToken": None,
        })

    assert stopped.value.value == {
        "status": "completed",
        "createdOrUpdated": 3,
        "deleted": 1,
        "aclResynced": 3,
        "failed": 0,
        "pending": 0,
        "bootstrapped": True,
        "itemsSeen": 90,
    }


@dataclass(frozen=True)
class _Config:
    source_id: str
    drive_id: str
    sharepoint_site_url: str


def test_activity_rejects_changed_registry_etag_before_binding_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a")
    monkeypatch.setattr(config_module, "load_config", lambda: _Config("legacy", "legacy-drive", "https://tenant.sharepoint.com/sites/legacy"))

    class Registry:
        def read_source_plan(self, _source_id):
            return SourcePlan(plan.source, "changed-etag", plan.binding_digest)

    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())

    with pytest.raises(SourceRegistryConflictError, match="catalog_changed"):
        function_app._resolve_source_config({"sourcePlan": plan.to_payload()})


def test_webhook_maps_and_deduplicates_registered_drive_subscriptions() -> None:
    plans = (
        _plan("source-a", "site-a", "a", "drive-a"),
        _plan("source-b", "site-b", "b", "drive-b"),
        _plan("source-paused", "site-p", "p", "drive-p", "paused"),
    )

    class LifecycleRepository:
        def get_webhook_subscription_id(self, source_id: str) -> str:
            return {
                "source-a": "subscription-a",
                "source-b": "subscription-b",
                "source-paused": "subscription-paused",
            }[source_id]

    matched = function_app._match_webhook_source_plans(
        [
            {"subscriptionId": "subscription-a"},
            {"subscriptionId": "subscription-a"},
            {"subscriptionId": "subscription-b"},
            {"subscriptionId": "subscription-paused"},
        ],
        plans,
        LifecycleRepository(),
    )

    assert [plan.source_id for plan in matched] == ["source-a", "source-b"]


def test_webhook_skips_unknown_subscription_and_rejects_malformed_notification() -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a")

    class LifecycleRepository:
        def get_webhook_subscription_id(self, _source_id: str) -> str:
            return "subscription-a"

    matched = function_app._match_webhook_source_plans(
        [{"subscriptionId": "unknown"}, {"subscriptionId": "subscription-a"}],
        [plan],
        LifecycleRepository(),
    )

    assert matched == (plan,)
    with pytest.raises(ValueError, match="notification_invalid"):
        function_app._match_webhook_source_plans(
            [None], [plan], LifecycleRepository(),
        )


@pytest.mark.parametrize(
    ("state", "event", "expected"),
    [
        ("retired", "reauthorizationRequired", "delete"),
        ("paused", "reauthorizationRequired", "ignore"),
        ("active", "reauthorizationRequired", "reauthorize"),
        ("active", "missed", "unsupported"),
    ],
)
def test_lifecycle_subscription_action_respects_source_state(
    state: str,
    event: str,
    expected: str,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a", state)

    assert function_app._lifecycle_subscription_action(plan, event) == expected


def test_audio_poll_orchestrator_reports_failed_job_as_failed() -> None:
    class Task:
        def __init__(self, name: str) -> None:
            self.name = name

    class Context:
        instance_id = "audio-owner"

        def get_input(self):
            return _plan("source-a", "site-a", "a", "drive-a").to_payload()

        def call_activity(self, _name, _payload):
            return Task(_name)

    context = Context()
    orchestrator = function_app.audio_transcription_poll_orchestrator._function._func.orchestrator_function(context)
    task = next(orchestrator)
    assert task.name == "audio_transcription_poll_page_activity"
    settle_task = orchestrator.send({
        "checked": 1, "succeeded": 0, "failed": 1,
        "pending": 0, "continuationToken": None,
    })
    assert settle_task.name == "settle_drive_coordinator_activity"
    with pytest.raises(StopIteration) as stopped:
        orchestrator.send({"settled": True})

    assert stopped.value.value["status"] == "failed"
    assert stopped.value.value["failed"] == 1


def test_delta_orchestrator_reuses_instance_id_as_stable_work_run_key() -> None:
    class Task:
        def __init__(self, name: str, payload: dict[str, Any]) -> None:
            self.name = name
            self.payload = payload

    class Context:
        instance_id = "delta-owner-123"

        def get_input(self):
            return _plan("source-a", "site-a", "a", "drive-a").to_payload()

        def call_activity(self, name: str, payload: dict[str, Any]) -> Task:
            return Task(name, payload)

    context = Context()
    orchestrator = function_app.delta_sync_orchestrator._function._func.orchestrator_function(context)
    task = next(orchestrator)

    assert task.name == "delta_sync_activity"
    assert task.payload["deltaExecutionId"] == context.instance_id
    assert task.payload["coordinatorOwnerId"] == context.instance_id


def test_delta_activity_passes_registry_owner_and_stable_run_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ingestion.services as services

    plan = _plan("source-a", "site-a", "a", "drive-a")
    config = SimpleNamespace(
        source_id="source-a", drive_id="drive-a", sharepoint_site_url=plan.source.site_url,
        enrichment_enabled=False, audio_writer_enabled=False,
    )
    acquired: list[tuple[Any, dict[str, Any]]] = []
    service_calls: list[dict[str, Any]] = []

    class Registry:
        def acquire_drive_coordinator(self, selected_plan, **kwargs):
            acquired.append((selected_plan, kwargs))

    class Repository:
        def get_source_control(self, _source_id):
            return None

    registry = Registry()
    monkeypatch.setattr(function_app, "_resolve_source_config", lambda _payload: (config, plan, registry))
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: Repository())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_extraction_clients", lambda _config: (None, None))
    monkeypatch.setattr(function_app, "_build_openai_client", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_sharepoint_client", lambda _config: None)
    monkeypatch.setattr(
        "ingestion.source_connector.SharePointConnector",
        lambda *_args, **_kwargs: object(),
    )

    def run_delta(*_args, **kwargs):
        service_calls.append(kwargs)
        return SimpleNamespace(
            bootstrapped=False, created_or_updated=0, deleted=0,
            acl_resynced=0, failed=0, pending=1, items_seen=1,
            continuation_token=None, continuation_phase=None,
        )

    monkeypatch.setattr(services, "run_delta_sync", run_delta)
    result = function_app.delta_sync_activity({
        "sourcePlan": plan.to_payload(),
        "coordinatorOwnerId": "durable-owner",
        "deltaExecutionId": "stable-delta-run",
        "settleCoordinator": False,
    })

    assert result["pending"] == 1
    assert acquired == [(plan, {"owner_instance_id": "durable-owner", "operation": "delta-sync"})]
    assert service_calls[0]["work_registry"] is registry
    assert service_calls[0]["source_plan"] == plan
    assert service_calls[0]["owner_instance_id"] == "durable-owner"
    assert service_calls[0]["delta_execution_id"] == "stable-delta-run"


def test_deferred_delta_cursor_commit_requires_ready_candidate_and_completes_round(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ingestion.models import RunStatus

    plan = _plan("source-a", "site-a", "a", "drive-a")
    config = SimpleNamespace(source_id="source-a")
    events: list[str] = []

    class Registry:
        def commit_full_sync_progress(self, selected_plan, owner, **kwargs):
            assert selected_plan == plan
            assert owner == "fullsync-owner"
            assert kwargs == {
                "round_id": "fullsync-round",
                "candidate_delta_link": "candidate-link",
            }
            events.append("atomic-commit")

    class Repository:
        def get_source_control(self, source_id):
            assert source_id == "source-a"
            return SimpleNamespace(record=SimpleNamespace(current_run_id="source-a:run-parent"))

        def get_run(self, source_id, run_id):
            assert source_id == "source-a"
            assert run_id == "source-a:run-parent"
            return SimpleNamespace(record=SimpleNamespace(status=RunStatus.COMPLETED))

    registry = Registry()
    monkeypatch.setattr(
        function_app,
        "_resolve_source_config",
        lambda _payload: (config, plan, registry),
    )
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: Repository())

    result = function_app.commit_delta_cursor_activity({
        "sourcePlan": plan.to_payload(),
        "coordinatorOwnerId": "fullsync-owner",
        "deltaExecutionId": "fullsync-round",
        "candidateDeltaLink": "candidate-link",
        "parentRunId": "source-a:run-parent",
    })

    assert result == {"committed": True}
    assert events == ["atomic-commit"]


def test_audio_poll_releases_ended_owner_before_reading_coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a")
    config = SimpleNamespace(
        audio_writer_enabled=True,
        audio_transcription_provider="speech_batch",
    )
    events = []

    class Registry:
        def get_drive_coordinator(self, _source_id):
            events.append("read")
            return None

    async def release(_client, _registry, source_id):
        assert source_id == "source-a"
        events.append("release")

    async def start(_client, _repo, source_id, _control, _orchestrator, **_kwargs):
        assert source_id == "source-a"
        events.append("start")

    monkeypatch.setattr(config_module, "load_config", lambda: config)
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_load_validated_source_plans", lambda _config: (plan,))
    monkeypatch.setattr("ingestion.source_registry.config_for_source", lambda _config, _plan: config)
    monkeypatch.setattr(function_app, "_release_coordinator_of_ended_owner", release)
    monkeypatch.setattr(function_app, "_start_if_not_running", start)

    asyncio.run(inspect.unwrap(function_app.audio_transcription_poll_timer._function._func)(
        SimpleNamespace(), object(),
    ))

    assert events == ["release", "read", "start"]


@pytest.mark.parametrize(
    ("desired_state", "coordinator_state", "expected"),
    [
        ("active", None, "normal"),
        ("active", "draining", "settlement-only"),
        ("paused", "active", "normal"),
        ("paused", "draining", "settlement-only"),
        ("retired", "active", None),
        ("retired", "draining", "settlement-only"),
    ],
)
def test_audio_poll_dispatch_mode(
    desired_state: str,
    coordinator_state: str | None,
    expected: str | None,
) -> None:
    coordinator = (
        {"state": coordinator_state, "ownerInstanceId": "owner"}
        if coordinator_state is not None else None
    )

    assert function_app._audio_poll_dispatch_mode(desired_state, coordinator) == expected


@pytest.mark.parametrize("desired_state", ["paused", "retired"])
def test_draining_work_resolution_allows_inactive_source_only_for_matching_owner(
    monkeypatch: pytest.MonkeyPatch,
    desired_state: str,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a", desired_state)
    config = SimpleNamespace()

    class Registry:
        def read_source_plan(self, _source_id):
            return plan

        def get_drive_coordinator(self, _source_id):
            return {"state": "draining", "ownerInstanceId": "draining-owner"}

    registry = Registry()
    monkeypatch.setattr(config_module, "load_config", lambda: config)
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: registry)
    monkeypatch.setattr(
        "ingestion.source_registry.config_for_source",
        lambda _config, resolved_plan: SimpleNamespace(bound_plan=resolved_plan),
    )
    payload = {
        "sourcePlan": plan.to_payload(),
        "coordinatorOwnerId": "draining-owner",
        "allowDrainingWork": True,
    }

    resolved = function_app._resolve_source_config(payload)
    assert resolved[0].bound_plan == plan
    assert resolved[1:] == (plan, registry)

    payload["coordinatorOwnerId"] = "other-owner"
    with pytest.raises(SourceRegistryConflictError, match="source_not_active"):
        function_app._resolve_source_config(payload)


def test_settlement_only_audio_orchestrator_does_not_start_delta_or_settle_new_owner() -> None:
    class Task:
        def __init__(self, name: str, payload: Any) -> None:
            self.name = name
            self.payload = payload

    class Context:
        instance_id = "audio-poller-owner"

        def __init__(self) -> None:
            self.calls: list[str] = []

        def get_input(self):
            return {
                "sourcePlan": _plan("source-a", "site-a", "a", "drive-a").to_payload(),
                "coordinatorOwnerId": "draining-owner",
                "allowDrainingWork": True,
            }

        def call_activity(self, name, payload):
            self.calls.append(name)
            return Task(name, payload)

    context = Context()
    orchestrator = function_app.audio_transcription_poll_orchestrator._function._func.orchestrator_function(context)
    page_task = next(orchestrator)
    assert page_task.name == "audio_transcription_poll_page_activity"
    assert page_task.payload["allowDrainingWork"] is True
    assert page_task.payload["coordinatorOwnerId"] == "draining-owner"

    with pytest.raises(StopIteration) as stopped:
        orchestrator.send({
            "checked": 1, "succeeded": 1, "failed": 0,
            "pending": 0, "settlementOnly": True, "continuationToken": None,
        })

    assert stopped.value.value["status"] == "completed"
    assert context.calls == ["audio_transcription_poll_page_activity"]


def test_audio_poll_orchestrator_carries_page_state_to_new_history() -> None:
    class Task:
        def __init__(self, name: str, payload: dict[str, Any]) -> None:
            self.name = name
            self.payload = payload

    class Context:
        instance_id = "audio-owner"

        def __init__(self) -> None:
            self.continued_input: dict[str, Any] | None = None

        def get_input(self):
            return {
                "sourcePlan": _plan("source-a", "site-a", "a", "drive-a").to_payload(),
                "coordinatorOwnerId": "audio-owner",
                "allowDrainingWork": True,
            }

        def call_activity(self, name, payload):
            return Task(name, payload)

        def continue_as_new(self, input_value):
            self.continued_input = input_value

    context = Context()
    orchestrator = function_app.audio_transcription_poll_orchestrator._function._func.orchestrator_function(context)
    page_task = next(orchestrator)

    with pytest.raises(StopIteration):
        orchestrator.send({
            "checked": 2, "succeeded": 1, "failed": 0, "pending": 1,
            "settlementOnly": True, "continuationToken": "audio-page-2",
        })

    assert context.continued_input == {
        "orchestrationPhase": "audio-poll",
        "sourcePlan": page_task.payload["sourcePlan"],
        "coordinatorOwnerId": "audio-owner",
        "allowPausedAudio": False,
        "allowDrainingWork": True,
        "continuationToken": "audio-page-2",
        "checked": 2,
        "succeeded": 1,
        "failed": 0,
        "pending": 1,
        "settlementOnly": True,
    }


def test_audio_poll_rolls_to_delta_without_carrying_document_collection() -> None:
    class Task:
        def __init__(self, name: str, payload: dict[str, Any]) -> None:
            self.name = name
            self.payload = payload

    class Context:
        instance_id = "audio-owner"

        def __init__(self) -> None:
            self.continued_input: dict[str, Any] | None = None

        def get_input(self):
            return _plan("source-a", "site-a", "a", "drive-a").to_payload()

        def call_activity(self, name, payload):
            return Task(name, payload)

        def continue_as_new(self, input_value):
            self.continued_input = input_value

    context = Context()
    orchestrator = function_app.audio_transcription_poll_orchestrator._function._func.orchestrator_function(context)
    page_task = next(orchestrator)

    with pytest.raises(StopIteration):
        orchestrator.send({
            "checked": 3, "succeeded": 2, "failed": 0, "pending": 0,
            "settlementOnly": False, "continuationToken": None,
        })

    assert page_task.name == "audio_transcription_poll_page_activity"
    assert context.continued_input == {
        "orchestrationPhase": "audio-poll",
        "audioPhase": "delta",
        "sourcePlan": page_task.payload["sourcePlan"],
        "coordinatorOwnerId": "audio-owner",
        "allowPausedAudio": False,
        "allowDrainingWork": False,
        "deltaPhase": "items",
        "deltaContinuationToken": None,
        "checked": 3,
        "succeeded": 2,
        "failed": 0,
        "pending": 0,
        "settlementOnly": False,
    }
    assert "documents" not in context.continued_input


def test_audio_delta_page_rolls_history_with_token_and_counters() -> None:
    source_plan = _plan("source-a", "site-a", "a", "drive-a").to_payload()

    class Task:
        def __init__(self, name: str, payload: dict[str, Any]) -> None:
            self.name = name
            self.payload = payload

    class Context:
        instance_id = "audio-owner"

        def __init__(self) -> None:
            self.continued_input: dict[str, Any] | None = None

        def get_input(self):
            return {
                "orchestrationPhase": "audio-poll",
                "audioPhase": "delta",
                "sourcePlan": source_plan,
                "coordinatorOwnerId": "audio-owner",
                "checked": 3,
                "succeeded": 2,
                "failed": 0,
                "pending": 0,
                "settlementOnly": False,
                "deltaPhase": "items",
                "deltaContinuationToken": "delta-page-1",
            }

        def call_activity(self, name, payload):
            return Task(name, payload)

        def continue_as_new(self, input_value):
            self.continued_input = input_value

    context = Context()
    orchestrator = function_app.audio_transcription_poll_orchestrator._function._func.orchestrator_function(context)
    delta_task = next(orchestrator)

    with pytest.raises(StopIteration):
        orchestrator.send({
            "failed": 0,
            "pending": 0,
            "continuationPhase": "delete",
            "phaseContinuationToken": "delta-page-2",
        })

    assert delta_task.name == "delta_sync_activity"
    assert delta_task.payload["reducedItemsContinuationToken"] == "delta-page-1"
    assert delta_task.payload["deltaExecutionId"] == "audio-catchup-audio-owner"
    assert context.continued_input == {
        "orchestrationPhase": "audio-poll",
        "audioPhase": "delta",
        "sourcePlan": source_plan,
        "coordinatorOwnerId": "audio-owner",
        "allowPausedAudio": False,
        "allowDrainingWork": False,
        "deltaPhase": "delete",
        "deltaContinuationToken": "delta-page-2",
        "checked": 3,
        "succeeded": 2,
        "failed": 0,
        "pending": 0,
        "settlementOnly": False,
    }
    assert "documents" not in context.continued_input


def test_acl_resync_orchestrator_continues_with_cursor_and_counters() -> None:
    class Task:
        def __init__(self, name: str, payload: dict[str, Any]) -> None:
            self.name = name
            self.payload = payload

    class Context:
        instance_id = "acl-owner"

        def __init__(self) -> None:
            self.continued_input: dict[str, Any] | None = None

        def get_input(self):
            return _plan("source-a", "site-a", "a", "drive-a").to_payload()

        def call_activity(self, name, payload):
            return Task(name, payload)

        def continue_as_new(self, input_value):
            self.continued_input = input_value

    context = Context()
    orchestrator = function_app.acl_resync_orchestrator._function._func.orchestrator_function(context)
    page_task = next(orchestrator)

    with pytest.raises(StopIteration):
        orchestrator.send({
            "checked": 4, "updated": 2, "retired": 1,
            "continuationToken": "acl-page-2",
        })

    assert page_task.name == "acl_resync_page_activity"
    assert context.continued_input == {
        "orchestrationPhase": "acl-resync",
        "sourcePlan": page_task.payload["sourcePlan"],
        "continuationToken": "acl-page-2",
        "totalChecked": 4,
        "totalUpdated": 2,
        "totalRetired": 1,
    }


@pytest.mark.parametrize("state", [None, "settled", "active", "draining", "maintenance"])
def test_lifecycle_timer_dispatches_retirement_only_after_writer_settlement(
    monkeypatch: pytest.MonkeyPatch, state: str | None,
) -> None:
    plans = (
        _plan("retired", "site-a", "a", "drive-a", "retired"),
        _plan("active", "site-b", "b", "drive-b"),
        _plan("paused", "site-c", "c", "drive-c", "paused"),
    )
    started = []
    drained = []

    class Registry:
        def get_drive_coordinator(self, _source_id):
            return {"state": state, "ownerInstanceId": "old-owner"} if state else None

        def mark_drive_coordinator_draining(self, source_id, owner):
            drained.append((source_id, owner))

    async def release(*_args):
        pass

    async def start(_client, _repo, source_id, _control, orchestrator, **_kwargs):
        started.append((source_id, orchestrator))

    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_load_validated_source_plans", lambda _config: plans)
    monkeypatch.setattr(function_app, "_release_coordinator_of_ended_owner", release)
    monkeypatch.setattr(function_app, "_start_if_not_running", start)
    asyncio.run(inspect.unwrap(function_app.lifecycle_reconcile_timer._function._func)(
        SimpleNamespace(), object(),
    ))
    expected = [("active", "lifecycle_reconcile_orchestrator")]
    if state in {None, "settled"}:
        expected.insert(0, ("retired", "source_retirement_orchestrator"))
    assert started == expected
    assert drained == ([("retired", "old-owner")] if state == "active" else [])


@pytest.mark.parametrize("deleted_count", [0, 2])
def test_source_retirement_activity_uses_maintenance_and_deletes_orphans_last(
    monkeypatch: pytest.MonkeyPatch, deleted_count: int,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a", "retired")
    calls = []

    class Registry:
        def begin_operator_maintenance(self, received, **kwargs):
            assert received == plan
            assert kwargs == {"owner_instance_id": "owner", "operation": "source-retirement"}
            calls.append("maintenance")

        def read_source_plan(self, _source_id):
            return plan

    class Lifecycle:
        def delete_source_chunks_page(self, **kwargs):
            assert kwargs["source_id"] == "source-a"
            calls.append("chunks")
            return 1

    def delete_page(_repo, _audit, **kwargs):
        assert kwargs["source_id"] == "source-a"
        assert kwargs["owner_instance_id"] == "owner"
        calls.append("documents")
        return deleted_count

    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: Lifecycle())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda _config: object())
    monkeypatch.setattr("ingestion.services.run_source_retirement_page", delete_page)
    result = function_app.source_retirement_page_activity({
        "sourcePlan": plan.to_payload(), "coordinatorOwnerId": "owner",
    })
    assert result == {
        "deletedDocuments": deleted_count, "deletedOrphanChunks": 0 if deleted_count else 1,
        "hasMore": True,
    }
    assert calls == (["maintenance", "documents"] if deleted_count else ["maintenance", "documents", "chunks"])


@pytest.mark.parametrize("result", [
    {"deletedDocuments": 2, "deletedOrphanChunks": 0, "hasMore": True},
    {"deletedDocuments": 0, "deletedOrphanChunks": 0, "hasMore": False},
    {"error": "source_retirement_failed"},
])
def test_source_retirement_orchestrator_pages_and_settles_on_success_or_failure(result: dict) -> None:
    class Context:
        instance_id = "retire-owner"
        continued = None

        def get_input(self):
            return _plan("source-a", "site-a", "a", "drive-a", "retired").to_payload()

        def call_activity(self, name, payload):
            return SimpleNamespace(name=name, payload=payload)

        def continue_as_new(self, value):
            self.continued = value

    context = Context()
    orchestrator = function_app.source_retirement_orchestrator._function._func.orchestrator_function(context)
    assert next(orchestrator).name == "source_retirement_page_activity"
    if result.get("hasMore"):
        with pytest.raises(StopIteration):
            orchestrator.send(result)
        assert context.continued["deletedDocuments"] == 2
    else:
        assert orchestrator.send(result).name == "settle_source_retirement_activity"
        with pytest.raises(StopIteration) as done:
            orchestrator.send({"settled": True})
        assert done.value.value["status"] == ("failed" if result.get("error") else "completed")


@pytest.mark.parametrize("failure", ["maintenance", "documents", "chunks"])
def test_source_retirement_activity_reports_failure_without_success_shaped_fallback(
    monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a", "retired")
    calls = []

    def step(name):
        calls.append(name)
        if name == failure:
            raise RuntimeError("storage failure")
        return 0

    class Registry:
        def begin_operator_maintenance(self, *_args, **_kwargs):
            step("maintenance")

        def read_source_plan(self, _source_id):
            return plan

    class Lifecycle:
        def delete_source_chunks_page(self, **_kwargs):
            return step("chunks")

    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: Lifecycle())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda _config: object())
    monkeypatch.setattr(
        "ingestion.services.run_source_retirement_page", lambda *_args, **_kwargs: step("documents"),
    )
    assert function_app.source_retirement_page_activity({
        "sourcePlan": plan.to_payload(), "coordinatorOwnerId": "owner",
    }) == {"error": "source_retirement_failed"}
    assert calls[-1] == failure


@pytest.mark.parametrize("operation", ["source-retirement", "operator-purge"])
def test_source_retirement_settlement_does_not_release_another_maintenance_operation(
    monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    settled = []

    class Registry:
        def get_drive_coordinator(self, _source_id):
            return {"state": "maintenance", "operation": operation, "ownerInstanceId": "owner"}

        def settle_drive_coordinator(self, source_id, owner):
            settled.append((source_id, owner))

    monkeypatch.setattr(config_module, "load_config", lambda: SimpleNamespace())
    monkeypatch.setattr(function_app, "_build_source_registry_repository", lambda _config: Registry())
    result = function_app.settle_source_retirement_activity({
        "sourcePlan": _plan("source-a", "site-a", "a", "drive-a", "retired").to_payload(),
        "coordinatorOwnerId": "owner",
    })
    assert result == (
        {"settled": True} if operation == "source-retirement"
        else {"error": "source_retirement_settlement_failed"}
    )
    assert settled == ([("source-a", "owner")] if operation == "source-retirement" else [])


def test_lifecycle_orchestrator_continues_with_phase_and_counters() -> None:
    class Task:
        def __init__(self, name: str, payload: dict[str, Any]) -> None:
            self.name = name
            self.payload = payload

    class Context:
        instance_id = "lifecycle-owner"

        def __init__(self) -> None:
            self.continued_input: dict[str, Any] | None = None

        def get_input(self):
            return _plan("source-a", "site-a", "a", "drive-a").to_payload()

        def call_activity(self, name, payload):
            return Task(name, payload)

        def continue_as_new(self, input_value):
            self.continued_input = input_value

    context = Context()
    orchestrator = function_app.lifecycle_reconcile_orchestrator._function._func.orchestrator_function(context)
    first_page = next(orchestrator)

    with pytest.raises(StopIteration):
        orchestrator.send({
            "checked": 2, "repaired": 1, "failed": 0,
            "continuationToken": "life-page-2",
        })

    assert first_page.name == "lifecycle_reconcile_page_activity"
    assert context.continued_input == {
        "orchestrationPhase": "lifecycle-reconcile",
        "sourcePlan": first_page.payload["sourcePlan"],
        "phase": "lifecycle",
        "continuationToken": "life-page-2",
        "totalChecked": 2,
        "totalRepaired": 1,
        "totalFailed": 0,
    }


@pytest.mark.parametrize(
    ("terminal_status", "coordinator_state", "settlement_owner"),
    [
        ("succeeded", "active", "audio-owner"),
        ("failed", "active", "audio-owner"),
        ("pending", "active", "audio-owner"),
        ("succeeded", "draining", "full-sync-owner"),
    ],
)
def test_audio_poll_activity_settles_only_terminal_matching_operation(
    monkeypatch: pytest.MonkeyPatch,
    terminal_status: str,
    coordinator_state: str,
    settlement_owner: str,
) -> None:
    import ingestion.services as services

    plan = _plan("source-a", "site-a", "a", "drive-a")
    config = SimpleNamespace(
        source_id="source-a", enrichment_enabled=False, drive_id="drive-a",
        sharepoint_site_url=plan.source.site_url,
    )
    calls: dict[str, list[Any]] = {"settled": [], "cleared": []}

    class Registry:
        def acquire_drive_coordinator(self, *_args, **_kwargs):
            calls.setdefault("acquired", []).append(True)
            return {"ownerInstanceId": settlement_owner, "state": "active"}

        def get_drive_coordinator(self, _source_id):
            return {
                "ownerInstanceId": "full-sync-owner" if coordinator_state == "draining" else "audio-owner",
                "state": coordinator_state,
            }

        def settle_document_operation(self, *args, **kwargs):
            calls["settled"].append((args, kwargs))

        def get_document_operation_kind(self, *_args):
            return "full-sync"

    class Repository:
        def get_source_control(self, _source_id):
            if coordinator_state == "draining":
                return SimpleNamespace(record=SimpleNamespace(current_run_id="run-a"))
            return None

        def get_run(self, _source_id, _run_id):
            return SimpleNamespace(record=SimpleNamespace(
                status=SimpleNamespace(value="running"),
            ))

        def clear_transcription_work_identity(self, *args):
            calls["cleared"].append(args)
            return True

    work = services.AudioWorkSettlement(
        "source-a:run-1", "doc-1", "operation-1", "operation-1", terminal_status,
    )
    outcome = SimpleNamespace(
        checked=1, succeeded=int(terminal_status == "succeeded"),
        failed=int(terminal_status == "failed"),
        pending=int(terminal_status == "pending"),
        terminal_work=() if terminal_status == "pending" else (work,),
    )
    registry = Registry()
    poll_call: dict[str, Any] = {}
    monkeypatch.setattr(function_app, "_resolve_source_config", lambda _payload: (config, plan, registry))
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: Repository())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_sharepoint_client", lambda _config: None)
    monkeypatch.setattr(function_app, "_build_openai_client", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_speech_token_provider", lambda _config: lambda: "token")
    monkeypatch.setattr(function_app, "_build_audio_staging_client", lambda _config: object())
    monkeypatch.setattr(
        services,
        "run_audio_transcription_poll_page",
        lambda *args, **kwargs: (poll_call.update(kwargs) or outcome, None),
    )
    monkeypatch.setattr(
        "ingestion.source_connector.SharePointConnector",
        lambda *_args, **_kwargs: object(),
    )

    result = function_app.audio_transcription_poll_page_activity({
        "sourcePlan": plan.to_payload(), "coordinatorOwnerId": "audio-owner",
    })

    assert result["succeeded"] == int(terminal_status == "succeeded")
    assert result["failed"] == int(terminal_status == "failed")
    assert result["pending"] == int(terminal_status == "pending")
    if terminal_status == "pending":
        assert calls["settled"] == []
        assert calls["cleared"] == []
    else:
        assert calls["settled"] == [(("source-a", settlement_owner, "operation-1"), {
            "physical_run_id": "source-a:run-1",
            "item_id": "doc-1",
            "operation_kind": "full-sync",
            "status": "succeeded" if terminal_status == "succeeded" else "failed",
            "outcome": {"documentId": "doc-1", "status": terminal_status},
        })]
        assert calls["cleared"] == [
            ("source-a:run-1", "doc-1", "operation-1", "operation-1"),
        ]
    assert poll_call["settlement_only"] is (coordinator_state == "draining")
    assert len(calls.get("acquired", [])) == int(coordinator_state == "active")


def test_audio_poll_activity_defers_with_complete_contract_during_full_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan("source-a", "site-a", "a", "drive-a")
    config = SimpleNamespace(source_id="source-a")

    class Registry:
        def get_drive_coordinator(self, _source_id):
            return {"ownerInstanceId": "audio-owner", "state": "active"}

        def acquire_drive_coordinator(self, *_args, **_kwargs):
            return None

    class Repository:
        def get_source_control(self, _source_id):
            return SimpleNamespace(record=SimpleNamespace(current_run_id="run-a"))

        def get_run(self, _source_id, _run_id):
            return SimpleNamespace(record=SimpleNamespace(
                status=SimpleNamespace(value="running"),
            ))

    monkeypatch.setattr(
        function_app, "_resolve_source_config", lambda _payload: (config, plan, Registry()),
    )
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: Repository())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: object())

    result = function_app.audio_transcription_poll_page_activity({
        "sourcePlan": plan.to_payload(),
        "coordinatorOwnerId": "audio-owner",
    })

    assert result == {
        "checked": 0,
        "succeeded": 0,
        "failed": 0,
        "pending": 1,
        "settlementOnly": False,
        "continuationToken": None,
    }


@pytest.mark.parametrize("cleanup_succeeds", [True, False])
def test_delta_audio_winner_publishes_before_retirement_and_settlement(
    monkeypatch: pytest.MonkeyPatch,
    cleanup_succeeds: bool,
) -> None:
    import ingestion.services as services

    plan = _plan("source-a", "site-a", "a", "drive-a")
    config = SimpleNamespace(
        source_id="source-a", enrichment_enabled=False, drive_id="drive-a",
        sharepoint_site_url=plan.source.site_url,
    )
    events: list[str] = []

    class Registry:
        def acquire_drive_coordinator(self, *_args, **_kwargs):
            return {"ownerInstanceId": "delta-owner", "state": "active"}

        def get_drive_coordinator(self, _source_id):
            return {"ownerInstanceId": "delta-owner", "state": "active"}

        def get_document_operation_kind(self, *_args):
            return "delta-sync"

        def settle_document_operation(self, *_args, **_kwargs):
            events.append("settle")

        def record_document_winner(self, *_args, **_kwargs):
            events.append("winner")

    class Repository:
        def get_source_control(self, _source_id):
            return None

        def get_document(self, source_run_id, document_id):
            return SimpleNamespace(record=SimpleNamespace(
                status=SimpleNamespace(value="ready"),
                stage=SimpleNamespace(value="terminal"),
                transcription_work_id="operation-1",
                transcription_attempt_id="operation-1",
                document_id=document_id,
                drive_id="drive-a",
                item_id="item-1",
                source_run_id=source_run_id,
                document_key="source-a:delta-run:doc-1",
                e_tag="etag-1",
                lifecycle_generation=1,
                processing_profile_digest="e" * 64,
            ))

        def clear_transcription_work_identity(self, *_args):
            events.append("clear")
            return True

    work = services.AudioWorkSettlement(
        "source-a:delta-run", "doc-1", "operation-1", "operation-1", "succeeded",
    )
    outcome = SimpleNamespace(
        checked=1, succeeded=1, failed=0, pending=0, terminal_work=(work,),
    )
    monkeypatch.setattr(function_app, "_resolve_source_config", lambda _payload: (config, plan, Registry()))
    monkeypatch.setattr(function_app, "_build_repository", lambda _config: Repository())
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_graph_client", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_sharepoint_client", lambda _config: None)
    monkeypatch.setattr(function_app, "_build_openai_client", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_audit_container", lambda _config: object())
    monkeypatch.setattr(function_app, "_build_speech_token_provider", lambda _config: lambda: "token")
    monkeypatch.setattr(function_app, "_build_audio_staging_client", lambda _config: object())
    monkeypatch.setattr(services, "run_audio_transcription_poll_page", lambda *a, **k: (outcome, None))
    monkeypatch.setattr(
        "ingestion.source_connector.SharePointConnector",
        lambda *_args, **_kwargs: object(),
    )
    def retire_prior_version(*_args, **kwargs):
        if not cleanup_succeeds:
            raise RuntimeError("superseded cleanup failed")
        events.append(f"retire:{kwargs['method']}")

    monkeypatch.setattr(function_app, "_retire_prior_version", retire_prior_version)

    result = function_app.audio_transcription_poll_page_activity({
        "sourcePlan": plan.to_payload(), "coordinatorOwnerId": "delta-owner",
    })

    if cleanup_succeeds:
        assert result["succeeded"] == 1
        assert events == ["winner", "retire:delta_sync", "settle", "clear"]
    else:
        assert result == {"error": "audio_transcription_poll_failed"}
        assert events == ["winner"]