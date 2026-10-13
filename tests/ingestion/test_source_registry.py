from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
import httpx
from azure.core import MatchConditions
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from ingestion.source_registry import (
    SourcePlan,
    SourceRegistryConflictError,
    SourceRegistryRepository,
)
from source_registry import SharePointSource, SourceRegistryError
from ingestion.lifecycle_repository import DocumentLifecycleRepository
from ingestion.source_connector import SharePointConnector


TENANT_ID = "tenant-id"


def _source(source_id: str, drive_id: str = "drive-a", **overrides: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": source_id,
        "sourceId": source_id,
        "recordType": "ingestion-source",
        "schemaVersion": 1,
        "sourceKind": "sharepoint-drive",
        "desiredState": "active",
        "config": {
            "siteId": f"site-{source_id}",
            "siteUrl": f"https://tenant.sharepoint.com/sites/{source_id}",
            "driveId": drive_id,
        },
        "_etag": f"etag-{source_id}",
    }
    item.update(overrides)
    return item


class _Conflict(Exception):
    status_code = 409


class _PreconditionFailed(Exception):
    status_code = 412


class FakeSources:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = {item["sourceId"]: deepcopy(item) for item in items}

    def query_items(self, **kwargs: Any) -> Any:
        assert "@recordType" in {p["name"] for p in kwargs["parameters"]}
        if "c.config.driveId = @driveId" in kwargs["query"]:
            parameters = {item["name"]: item["value"] for item in kwargs["parameters"]}
            return [
                item for item in self.items.values()
                if item.get("recordType") == parameters["@recordType"]
                and item.get("config", {}).get("driveId") == parameters["@driveId"]
            ][:2]
        return _RunsPageIterator(list(self.items.values()), kwargs["max_item_count"])

    def create_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        if body["sourceId"] in self.items:
            raise _Conflict()
        stored = deepcopy(body) | {"_etag": f"etag-{body['sourceId']}"}
        self.items[body["sourceId"]] = stored
        return deepcopy(stored)

    def replace_item(
        self, *, item: str, body: dict[str, Any], etag: str, match_condition,
    ) -> dict[str, Any]:
        assert match_condition is MatchConditions.IfNotModified
        if self.items[item]["_etag"] != etag:
            raise _PreconditionFailed()
        stored = deepcopy(body) | {"_etag": f"{etag}+"}
        self.items[item] = stored
        return deepcopy(stored)

    def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
        try:
            return deepcopy(self.items[partition_key])
        except KeyError:
            raise CosmosResourceNotFoundError(message="missing") from None


class _RunsPageIterator:
    def __init__(self, rows: list[dict[str, Any]], page_size: int) -> None:
        self._rows = rows
        self._page_size = page_size
        self.continuation_token: str | None = None

    def by_page(self, continuation_token: str | None = None):
        start = int(continuation_token or "0")
        page = self._rows[start:start + self._page_size]
        next_index = start + len(page)
        next_token = str(next_index) if next_index < len(self._rows) else None
        return _RunsPager([page], next_token)


class _RunsPager:
    def __init__(self, pages: list[list[dict[str, Any]]], continuation_token: str | None) -> None:
        self._pages = iter(pages)
        self.continuation_token = continuation_token

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._pages)


class FakeRuns:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self._etag = 0
        self.before_batch: Any = None
        self.after_batch: Any = None
        self.batch_calls: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.query_calls: list[dict[str, Any]] = []

    def create_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        key = (body["sourceId"], body["id"])
        if key in self.items:
            raise _Conflict()
        self._etag += 1
        stored = deepcopy(body) | {"_etag": f"etag-{self._etag}"}
        self.items[key] = stored
        return deepcopy(stored)

    def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
        try:
            return deepcopy(self.items[(partition_key, item)])
        except KeyError:
            raise CosmosResourceNotFoundError(status_code=404, message="missing") from None

    def query_items(
        self,
        *,
        query: str,
        parameters: list[dict[str, Any]],
        max_item_count: int,
        partition_key: str | None = None,
        **_kwargs: Any,
    ):
        values = {parameter["name"]: parameter["value"] for parameter in parameters}
        self.query_calls.append({
            "query": query,
            "parameters": deepcopy(parameters),
            "partition_key": partition_key,
            "max_item_count": max_item_count,
        })
        rows = [
            deepcopy(item)
            for (source_id, _item_id), item in self.items.items()
            if (partition_key is None or source_id == partition_key)
            and item.get("id") not in {
                values.get("@sourceBinding"),
                values.get("@sourceServing"),
                values.get("@driveCoordinator"),
                values.get("@sourceReprovision"),
            }
            and all(
                item.get(field) == values[parameter]
                for parameter, field in (
                    ("@recordType", "recordType"),
                    ("@owner", "ownerInstanceId"),
                    ("@operation", "operationKind"),
                    ("@receiptId", "receiptId"),
                    ("@roundId", "roundId"),
                    ("@itemId", "itemId"),
                    ("@dispatchId", "dispatchId"),
                    ("@status", "status"),
                )
                if parameter in values
            )
            and ("@states" not in values or item.get("state") in values["@states"])
        ]
        return _RunsPageIterator(rows, max_item_count)

    def replace_item(
        self, *, item: str, body: dict[str, Any], etag: str, match_condition,
    ) -> dict[str, Any]:
        # Mirrors azure-cosmos: replace_item has no partition_key; it comes from the body.
        key = (body["sourceId"], item)
        current = self.items[key]
        assert etag == current["_etag"]
        assert match_condition is MatchConditions.IfNotModified
        self._etag += 1
        stored = deepcopy(body) | {"_etag": f"etag-{self._etag}"}
        self.items[key] = stored
        return deepcopy(stored)

    def execute_item_batch(
        self,
        *,
        batch_operations: list[tuple[Any, ...]],
        partition_key: str,
    ) -> list[dict[str, int]]:
        self.batch_calls.append((partition_key, deepcopy(batch_operations)))
        if self.before_batch is not None:
            callback = self.before_batch
            self.before_batch = None
            callback()
        snapshot = deepcopy(self.items)
        etag = self._etag
        try:
            for operation, arguments, *options in batch_operations:
                if operation == "create":
                    self.create_item(body=arguments[0])
                elif operation == "replace":
                    item_id, body = arguments
                    key = (partition_key, item_id)
                    if self.items.get(key, {}).get("_etag") != options[0]["if_match_etag"]:
                        raise _PreconditionFailed()
                    self._etag += 1
                    self.items[key] = deepcopy(body) | {"_etag": f"etag-{self._etag}"}
                elif operation == "delete":
                    item_id = arguments[0]
                    key = (partition_key, item_id)
                    if self.items[key].get("_etag") != options[0]["if_match_etag"]:
                        raise _PreconditionFailed()
                    del self.items[key]
                else:
                    raise AssertionError(f"unexpected batch operation: {operation}")
        except Exception:
            self.items = snapshot
            self._etag = etag
            raise
        if self.after_batch is not None:
            callback = self.after_batch
            self.after_batch = None
            callback()
        return [
            {"statusCode": 201 if operation[0] == "create" else 204 if operation[0] == "delete" else 200}
            for operation in batch_operations
        ]


def _repository(items: list[dict[str, Any]]) -> tuple[SourceRegistryRepository, FakeSources, FakeRuns]:
    sources = FakeSources(items)
    runs = FakeRuns()
    return SourceRegistryRepository(sources, runs, TENANT_ID), sources, runs


def _accept_source(repository: SourceRegistryRepository, source_id: str):
    page = repository.list_source_inventory_page(page_size=100)
    plan = next(plan for plan in page.plans if plan.source_id == source_id)
    return repository.accept_source_plan(plan)


def test_registry_reads_multiple_sources_and_reserves_each_drive() -> None:
    first, second = _source("source-a"), _source("source-b", "drive-b")
    repository, _, runs = _repository([first, second])

    page = repository.list_source_inventory_page(page_size=100)
    for plan in page.plans:
        repository.accept_source_plan(plan)

    assert {plan.source_id for plan in page.plans} == {"source-a", "source-b"}
    assert page.rejected_source_ids == ()
    assert page.unattributed_errors == 0
    assert page.continuation_token is None
    assert runs.items[("source-a", "source-binding")]["driveId"] == "drive-a"
    reservations = [item for item in runs.items.values() if item["recordType"] == "drive-binding"]
    assert len(reservations) == 2


def test_drive_coordinator_serializes_owners_until_explicit_settlement() -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")

    first = repository.acquire_drive_coordinator(
        plan, owner_instance_id="orchestration-a", operation="full-sync",
    )
    replay = repository.acquire_drive_coordinator(
        plan, owner_instance_id="orchestration-a", operation="full-sync",
    )

    assert replay["_etag"] == first["_etag"]
    with pytest.raises(SourceRegistryConflictError, match="already_active"):
        repository.acquire_drive_coordinator(
            plan, owner_instance_id="orchestration-b", operation="delta-sync",
        )

    repository.settle_drive_coordinator("source-a", "orchestration-a")
    replacement = repository.acquire_drive_coordinator(
        plan, owner_instance_id="orchestration-b", operation="delta-sync",
    )
    assert replacement["ownerInstanceId"] == "orchestration-b"
    assert runs.items[("source-a", "drive-coordinator")]["state"] == "active"


def test_draining_drive_coordinator_has_no_expiry_takeover() -> None:
    repository, _, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="orchestration-a", operation="full-sync",
    )
    repository.mark_drive_coordinator_draining("source-a", "orchestration-a")

    with pytest.raises(SourceRegistryConflictError, match="draining"):
        repository.acquire_drive_coordinator(
            plan, owner_instance_id="orchestration-b", operation="delta-sync",
        )


def test_delta_operation_uses_existing_source_partition_and_settles() -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="delta-owner", operation="delta-sync",
    )
    claim = repository.begin_document_operation(
        plan, owner_instance_id="delta-owner",
        physical_run_id="source-a:delta-run",
        item_id="document-a", operation_kind="delta-sync",
    )
    started = repository.start_document_operation(
        "source-a", "delta-owner", claim["operationId"],
        "source-a:delta-run", "document-a", "delta-sync",
    )
    settled = repository.settle_document_operation(
        "source-a", "delta-owner", claim["operationId"],
        physical_run_id="source-a:delta-run", item_id="document-a",
        operation_kind="delta-sync", status="succeeded",
        outcome={"documentId": "document-a", "status": "succeeded"},
    )

    assert claim["status"] == "acquired"
    assert started["status"] == "started"
    assert settled["status"] == "completed"
    assert repository.get_unresolved_document_operation_count("source-a") == 0
    assert all(partition_key == "source-a" for partition_key, _ in runs.batch_calls)


def test_unresolved_operation_query_is_source_scoped_and_bounded() -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="retry-owner", operation="retry-failed",
    )
    for document_id in ("doc-a", "doc-b"):
        repository.begin_document_operation(
            plan, owner_instance_id="retry-owner",
            physical_run_id="source-a:run-a",
            item_id=document_id, operation_kind="retry-failed:attempt-a",
        )

    page = repository.list_unresolved_document_operations_page(
        "source-a", "retry-owner", "retry-failed:attempt-a", page_size=1,
    )

    assert len(page) == 1
    assert page[0]["state"] == "claimed"
    assert runs.query_calls[0]["partition_key"] == "source-a"
    assert runs.query_calls[0]["max_item_count"] == 1


def test_delta_page_capture_seals_fragments_and_replays_exact_response() -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="delta-owner", operation="delta-sync",
    )
    capture = repository.delta_page_capture(plan, "delta-owner", "round-1")
    request_url = "https://graph.microsoft.com/v1.0/drives/drive-a/root/delta"
    items = [{"id": "item-1", "name": "one.pdf"}, {"id": "item-2", "name": "two.pdf"}]

    claim = capture.begin_page(request_url, 0, None)
    receipt_id = capture.seal_page(
        request_url, 0, None, items, "https://graph.microsoft.com/v1.0/next", None,
    )
    replay = capture.begin_page(request_url, 0, None)

    assert claim == {"state": "capture", "receiptId": receipt_id}
    assert replay == {
        "state": "sealed", "receiptId": receipt_id, "items": items,
        "nextLink": "https://graph.microsoft.com/v1.0/next", "deltaLink": None,
    }
    receipts = [item for item in runs.items.values() if item["recordType"] == "delta-page-receipt"]
    fragments = [item for item in runs.items.values() if item["recordType"] == "delta-page-fragment"]
    assert len(receipts) == 1 and receipts[0]["state"] == "sealed"
    assert len(fragments) == len(items)


def test_delta_page_capture_persists_reset_anchor_for_replay() -> None:
    repository, _, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="delta-owner", operation="delta-sync",
    )
    repository.begin_delta_round(
        plan, "delta-owner", "round-reset", "https://graph.microsoft.com/v1.0/drives/drive-a/root/delta?token=old",
    )
    capture = repository.delta_page_capture(plan, "delta-owner", "round-reset")
    request_url = "https://graph.microsoft.com/v1.0/drives/drive-a/root/delta?token=old"
    reset_location = "https://graph.microsoft.com/v1.0/drives/drive-a/root/delta?token=reset"

    claim = capture.begin_page(request_url, 0, None)
    capture.record_reset(request_url, 0, reset_location)
    replay = capture.begin_page(request_url, 0, None)
    capture.begin_page(reset_location, 0, None)
    capture.seal_page(
        reset_location,
        0,
        None,
        [{"id": "item-1"}],
        None,
        "https://graph.microsoft.com/v1.0/drives/drive-a/root/delta?token=fresh",
    )

    assert claim["state"] == "capture"
    assert replay["state"] == "reset"
    assert replay["location"] == reset_location
    assert capture.count_observed_entries() == 1


def test_delta_page_reduction_uses_last_page_and_entry_occurrence() -> None:
    repository, _, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="delta-owner", operation="delta-sync",
    )
    capture = repository.delta_page_capture(plan, "delta-owner", "round-reduce")
    first_url = "https://graph.microsoft.com/v1.0/drives/drive-a/root/delta"
    next_url = "https://graph.microsoft.com/v1.0/next"
    final_url = "https://graph.microsoft.com/v1.0/drives/drive-a/root/delta?token=end"
    first = capture.begin_page(first_url, 0, None)
    capture.seal_page(
        first_url, 0, None,
        [{"id": "item-1", "name": "first.pdf"}, {"id": "item-2", "name": "two.pdf"}],
        next_url, None,
    )
    capture.begin_page(next_url, 1, first["receiptId"])
    capture.seal_page(
        next_url, 1, first["receiptId"],
        [{"id": "item-1", "name": "latest.pdf"}],
        None, final_url,
    )

    reduced = capture.list_reduced_items()

    assert reduced == [
        {"id": "item-1", "name": "latest.pdf"},
        {"id": "item-2", "name": "two.pdf"},
    ]
    page = capture.list_reduced_items_page(page_size=1)
    assert page.items == ({"id": "item-1", "name": "latest.pdf"},)
    assert page.continuation_token == "1"
    next_page = capture.list_reduced_items_page(
        page_size=1,
        continuation_token=page.continuation_token,
    )
    assert next_page.items == ({"id": "item-2", "name": "two.pdf"},)
    assert next_page.continuation_token is None
    assert repository.has_reduced_delta_item("source-a", "round-reduce", "item-1") is True
    assert repository.has_reduced_delta_item("source-a", "round-reduce", "missing") is False


def test_delta_round_cursor_ready_replay_recovers_commit_without_refetch() -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="delta-owner", operation="delta-sync",
    )
    round_state = repository.begin_delta_round(
        plan, "delta-owner", "delta-execution-1", "https://graph.microsoft.com/old",
    )
    outcome = {
        "bootstrapped": False, "created_or_updated": 1, "deleted": 0,
        "acl_resynced": 0, "failed": 0, "pending": 0, "items_seen": 1,
    }
    repository.mark_delta_round_cursor_ready(
        "source-a", "delta-execution-1", "delta-owner",
        "https://graph.microsoft.com/new", outcome,
    )

    replay = repository.begin_delta_round(
        plan, "delta-owner", "delta-execution-1", "https://graph.microsoft.com/new",
    )

    assert round_state["state"] == "active"
    assert replay["state"] == "cursor_ready"
    assert replay["candidateDeltaLink"] == "https://graph.microsoft.com/new"
    assert replay["outcome"] == outcome
    committed = repository.complete_delta_round("source-a", "delta-execution-1", "delta-owner")
    assert committed["state"] == "committed"
    assert repository.begin_delta_round(
        plan, "delta-owner", "delta-execution-1", "https://graph.microsoft.com/new",
    )["state"] == "committed"
    assert runs.items[("source-a", round_state["id"])]["state"] == "committed"


def test_full_sync_progress_commits_cursor_round_and_owner_atomically() -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="full-owner", operation="full-sync",
    )
    round_state = repository.begin_delta_round(
        plan, "full-owner", "full-round", None,
    )
    repository.mark_delta_round_cursor_ready(
        "source-a",
        "full-round",
        "full-owner",
        "https://graph.microsoft.com/v1.0/drives/drive-a/root/delta?token=final",
        {"items_seen": 2, "failed": 0, "pending": 0},
    )
    assert repository.read_source_plan("source-a") == plan
    assert repository.get_drive_coordinator("source-a")["state"] == "active"
    assert runs.items[("source-a", round_state["id"])]["candidateDeltaLink"].endswith("token=final")

    result = repository.commit_full_sync_progress(
        plan,
        "full-owner",
        round_id="full-round",
        candidate_delta_link="https://graph.microsoft.com/v1.0/drives/drive-a/root/delta?token=final",
    )

    assert result == {"committed": True, "replayed": False}
    assert runs.items[("source-a", "delta-control")]["deltaLink"].endswith("token=final")
    assert runs.items[("source-a", round_state["id"])]["state"] == "committed"
    assert runs.items[("source-a", "drive-coordinator")]["state"] == "settled"
    batch = runs.batch_calls[-1][1]
    assert len(batch) == 3
    assert repository.commit_full_sync_progress(
        plan,
        "full-owner",
        round_id="full-round",
        candidate_delta_link="https://graph.microsoft.com/v1.0/drives/drive-a/root/delta?token=final",
    ) == {"committed": True, "replayed": True}


@pytest.mark.parametrize("source_order", [("a", "b"), ("b", "a")])
def test_two_source_full_sync_committed_cursors_feed_only_their_drive_delta(
    source_order: tuple[str, str],
) -> None:
    repository, _, runs = _repository([
        _source(f"source-{suffix}", f"drive-{suffix}") for suffix in source_order
    ])
    lifecycle = DocumentLifecycleRepository(runs, None, None)
    plans = {suffix: _accept_source(repository, f"source-{suffix}") for suffix in source_order}
    candidates = {
        suffix: f"https://graph.microsoft.com/v1.0/drives/drive-{suffix}/root/delta?token=catchup"
        for suffix in source_order
    }
    for suffix, plan in plans.items():
        repository.acquire_drive_coordinator(
            plan, owner_instance_id=f"full-{suffix}", operation="full-sync",
        )
        repository.begin_delta_round(plan, f"full-{suffix}", f"full-{suffix}", None)
        repository.mark_delta_round_cursor_ready(
            plan.source_id, f"full-{suffix}", f"full-{suffix}", candidates[suffix],
            {"items_seen": 1, "failed": 0, "pending": 0},
        )
        assert lifecycle.get_delta_cursor(plan.source_id) is None

    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        assert str(request.url) in candidates.values()
        return httpx.Response(200, json={
            "value": [], "@odata.deltaLink": str(request.url).replace("token=catchup", "token=next"),
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        for suffix, plan in plans.items():
            other_source = next(other.source_id for other in plans.values() if other != plan)
            other_items = deepcopy({
                key: value for key, value in runs.items.items() if key[0] == other_source
            })
            result = repository.commit_full_sync_progress(
                plan, f"full-{suffix}", round_id=f"full-{suffix}",
                candidate_delta_link=candidates[suffix],
            )
            assert result == {"committed": True, "replayed": False}
            assert {
                key: value for key, value in runs.items.items() if key[0] == other_source
            } == other_items
            assert repository.get_drive_coordinator(plan.source_id)["state"] == "settled"
            cursor = lifecycle.get_delta_cursor(plan.source_id)
            assert cursor == candidates[suffix]
            delta = SharePointConnector(client, plan.source.drive_id).read_drive_delta(1, delta_link=cursor)
            assert delta.delta_link == candidates[suffix].replace("token=catchup", "token=next")
            assert lifecycle.get_delta_cursor(plan.source_id) == cursor

    assert requests == [candidates[suffix] for suffix in source_order]


def test_document_winner_is_source_scoped_and_idempotent() -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="full-owner", operation="full-sync",
    )
    winner = {
        "document_id": "document-a",
        "drive_id": "drive-a",
        "item_id": "item-a",
        "source_run_id": "source-a:run-a",
        "document_key": "source-a:run-a:document-a",
        "source_etag": "source-etag-a",
        "lifecycle_generation": 3,
        "processing_profile_digest": "a" * 64,
    }

    first = repository.record_document_winner(
        "source-a", "full-owner", **winner,
    )
    replay = repository.record_document_winner(
        "source-a", "full-owner", **winner,
    )
    stored = repository.get_document_winner("source-a", "document-a")

    assert first["recordType"] == "document-winner"
    assert replay["documentKey"] == winner["document_key"]
    assert stored is not None and stored["processingProfileDigest"] == "a" * 64
    assert stored["sourceRunId"] == winner["source_run_id"]
    assert any(
        partition_key == "source-a"
        and any("document-winner" in str(operation) for operation in operations)
        for partition_key, operations in runs.batch_calls
    )


def test_document_winner_clear_is_identity_bound_and_idempotent() -> None:
    repository, _, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="full-owner", operation="full-sync",
    )
    repository.record_document_winner(
        "source-a",
        "full-owner",
        document_id="document-a",
        drive_id="drive-a",
        item_id="item-a",
        source_run_id="source-a:run-a",
        document_key="source-a:run-a:document-a",
        source_etag="etag-a",
        lifecycle_generation=2,
        processing_profile_digest="b" * 64,
    )

    with pytest.raises(SourceRegistryConflictError, match="clear_identity_mismatch"):
        repository.clear_document_winner(
            "source-a", "full-owner", document_id="document-a",
            expected_source_run_id="source-a:other-run",
            expected_document_key="source-a:other-run:document-a",
        )
    assert repository.get_document_winner("source-a", "document-a") is not None

    assert repository.clear_document_winner(
        "source-a", "full-owner", document_id="document-a",
        expected_source_run_id="source-a:run-a",
        expected_document_key="source-a:run-a:document-a",
    ) is True
    assert repository.get_document_winner("source-a", "document-a") is None
    assert repository.clear_document_winner(
        "source-a", "full-owner", document_id="document-a",
        expected_source_run_id="source-a:run-a",
        expected_document_key="source-a:run-a:document-a",
    ) is False


@pytest.mark.parametrize("operation", ["source-retirement", "operator-purge"])
def test_winner_clear_only_allows_source_retirement_maintenance(operation: str) -> None:
    repository, sources, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(plan, owner_instance_id="sync-owner", operation="full-sync")
    repository.record_document_winner(
        "source-a", "sync-owner", document_id="document-a", drive_id="drive-a",
        item_id="item-a", source_run_id="source-a:run-a",
        document_key="source-a:run-a:document-a", source_etag="etag-a",
        lifecycle_generation=0, processing_profile_digest="b" * 64,
    )
    repository.settle_drive_coordinator("source-a", "sync-owner")
    sources.items["source-a"]["desiredState"] = "retired"
    sources.items["source-a"]["_etag"] = "retired-etag"
    retired_plan = repository.read_source_plan("source-a")
    repository.observe_lifecycle(retired_plan)
    repository.begin_operator_maintenance(
        retired_plan, owner_instance_id="maintenance-owner", operation=operation,
    )
    kwargs = {
        "document_id": "document-a",
        "expected_source_run_id": "source-a:run-a",
        "expected_document_key": "source-a:run-a:document-a",
    }
    if operation == "source-retirement":
        assert repository.clear_document_winner("source-a", "maintenance-owner", **kwargs)
        assert repository.get_document_winner("source-a", "document-a") is None
    else:
        with pytest.raises(SourceRegistryConflictError, match="clear_identity_mismatch"):
            repository.clear_document_winner("source-a", "maintenance-owner", **kwargs)
        assert repository.get_document_winner("source-a", "document-a") is not None
    assert repository.read_source_plan("source-a") == retired_plan
    assert repository.observe_lifecycle(retired_plan)["retired"] is True


def test_operation_replay_is_idempotent_and_does_not_repeat_side_effects() -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="orchestration-a", operation="full-sync",
    )
    claim = repository.begin_document_operation(
        plan,
        owner_instance_id="orchestration-a",
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
    )
    assert repository.start_document_operation(
        "source-a",
        "orchestration-a",
        claim["operationId"],
        "source-a:run-a",
        "document-a",
        "full-sync",
    ) == {"status": "started"}

    replay_while_running = repository.begin_document_operation(
        plan,
        owner_instance_id="orchestration-a",
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
    )
    outcome = {"documentId": "document-a", "status": "succeeded"}
    repository.settle_document_operation(
        "source-a",
        "orchestration-a",
        claim["operationId"],
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
        status="succeeded",
        outcome=outcome,
    )
    terminal_replay = repository.begin_document_operation(
        plan,
        owner_instance_id="orchestration-a",
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
    )

    assert replay_while_running["status"] == "unresolved"
    assert terminal_replay["status"] == "completed"
    assert terminal_replay["terminalStatus"] == "succeeded"
    assert terminal_replay["outcome"] == outcome
    operations = [
        item for item in runs.items.values()
        if item["recordType"] == "document-operation"
    ]
    assert len(operations) == 1
    assert not [
        item for item in runs.items.values()
        if item["recordType"] == "document-attempt"
    ]


def test_operation_identity_mismatch_cannot_settle_another_item() -> None:
    repository, _, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="orchestration-a", operation="delta-sync",
    )
    claim = repository.begin_document_operation(
        plan,
        owner_instance_id="orchestration-a",
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="delta-delete",
    )
    repository.start_document_operation(
        "source-a",
        "orchestration-a",
        claim["operationId"],
        "source-a:run-a",
        "document-a",
        "delta-delete",
    )

    with pytest.raises(SourceRegistryConflictError, match="identity_mismatch"):
        repository.settle_document_operation(
            "source-a",
            "orchestration-a",
            claim["operationId"],
            physical_run_id="source-a:run-a",
            item_id="document-b",
            operation_kind="delta-delete",
            status="succeeded",
            outcome={"status": "succeeded"},
        )
    assert repository.get_unresolved_document_operation_count("source-a") == 1


def test_interrupted_operation_blocks_only_its_parent_source() -> None:
    repository, _, _ = _repository([
        _source("source-a", "drive-a"),
        _source("source-b", "drive-b"),
    ])
    plan_a = _accept_source(repository, "source-a")
    plan_b = _accept_source(repository, "source-b")
    repository.acquire_drive_coordinator(
        plan_a, owner_instance_id="owner-a", operation="full-sync",
    )
    repository.acquire_drive_coordinator(
        plan_b, owner_instance_id="owner-b", operation="full-sync",
    )
    operation_a = repository.begin_document_operation(
        plan_a,
        owner_instance_id="owner-a",
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
    )
    repository.start_document_operation(
        "source-a",
        "owner-a",
        operation_a["operationId"],
        "source-a:run-a",
        "document-a",
        "full-sync",
    )
    unresolved_outcome = {"documentId": "document-a", "status": "pending"}
    assert repository.settle_document_operation(
        "source-a",
        "owner-a",
        operation_a["operationId"],
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
        status="unresolved",
        outcome=unresolved_outcome,
    ) == {"status": "unresolved", "outcome": unresolved_outcome}
    assert repository.begin_document_operation(
        plan_a,
        owner_instance_id="owner-a",
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
    )["status"] == "unresolved"
    operation_b = repository.begin_document_operation(
        plan_b,
        owner_instance_id="owner-b",
        physical_run_id="source-b:run-b",
        item_id="document-b",
        operation_kind="full-sync",
    )
    repository.start_document_operation(
        "source-b",
        "owner-b",
        operation_b["operationId"],
        "source-b:run-b",
        "document-b",
        "full-sync",
    )
    repository.settle_document_operation(
        "source-b",
        "owner-b",
        operation_b["operationId"],
        physical_run_id="source-b:run-b",
        item_id="document-b",
        operation_kind="full-sync",
        status="succeeded",
        outcome={"status": "succeeded"},
    )

    with pytest.raises(SourceRegistryConflictError, match="work_unresolved"):
        repository.settle_drive_coordinator("source-a", "owner-a")
    assert repository.get_unresolved_document_operation_count("source-a") == 1
    assert repository.get_unresolved_document_operation_count("source-b") == 0
    assert repository.settle_drive_coordinator("source-b", "owner-b")["state"] == "settled"


@pytest.mark.parametrize("terminal_status", ["succeeded", "failed"])
def test_operation_terminal_outcome_is_explicit_and_idempotent(
    terminal_status: str,
) -> None:
    repository, _, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="orchestration-a", operation="full-sync",
    )
    claim = repository.begin_document_operation(
        plan,
        owner_instance_id="orchestration-a",
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
    )
    repository.start_document_operation(
        "source-a",
        "orchestration-a",
        claim["operationId"],
        "source-a:run-a",
        "document-a",
        "full-sync",
    )
    outcome = {"documentId": "document-a", "status": terminal_status}
    first = repository.settle_document_operation(
        "source-a",
        "orchestration-a",
        claim["operationId"],
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
        status=terminal_status,
        outcome=outcome,
    )
    replay = repository.settle_document_operation(
        "source-a",
        "orchestration-a",
        claim["operationId"],
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind="full-sync",
        status=terminal_status,
        outcome=outcome,
    )

    assert first == replay == {"status": "completed", "outcome": outcome}
    assert repository.get_unresolved_document_operation_count("source-a") == 0


@pytest.mark.parametrize(
    "operation_kind",
    [
        "full-sync",
        "delta-sync",
        "delta-delete",
        "delta-acl",
        "delta-cleanup",
        "acl-resync",
        "lifecycle-reconcile",
        "retry-failed:repair",
        "duplicate-reconcile",
        "orphan-reconcile",
    ],
)
def test_supported_writer_operation_kinds_share_one_idempotent_contract(
    operation_kind: str,
) -> None:
    repository, _, runs = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="owner-a", operation="full-sync",
    )

    claim = repository.begin_document_operation(
        plan,
        owner_instance_id="owner-a",
        physical_run_id="source-a:run-a",
        item_id="document-a",
        operation_kind=operation_kind,
    )

    operation = runs.items[("source-a", claim["operationId"])]
    assert operation["recordType"] == "document-operation"
    assert operation["physicalRunId"] == "source-a:run-a"
    assert operation["itemId"] == "document-a"
    assert operation["operationKind"] == operation_kind


def test_operator_maintenance_excludes_drive_writers_until_settled() -> None:
    repository, _, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="sync-owner", operation="full-sync",
    )
    with pytest.raises(SourceRegistryConflictError, match="not_quiescent"):
        repository.begin_operator_maintenance(
            plan, owner_instance_id="purge-owner", operation="operator-purge",
        )
    repository.settle_drive_coordinator("source-a", "sync-owner")

    maintenance = repository.begin_operator_maintenance(
        plan, owner_instance_id="purge-owner", operation="operator-purge",
    )
    assert maintenance["state"] == "maintenance"
    with pytest.raises(SourceRegistryConflictError, match="in_maintenance"):
        repository.acquire_drive_coordinator(
            plan, owner_instance_id="replacement-owner", operation="delta-sync",
        )
    repository.settle_drive_coordinator("source-a", "purge-owner")

    assert repository.acquire_drive_coordinator(
        plan, owner_instance_id="replacement-owner", operation="delta-sync",
    )["state"] == "active"


def test_source_retirement_maintenance_requires_quiescence_and_retries_same_owner() -> None:
    repository, sources, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    with pytest.raises(SourceRegistryConflictError, match="source_not_retired"):
        repository.begin_operator_maintenance(
            plan, owner_instance_id="retire-owner", operation="source-retirement",
        )
    repository.acquire_drive_coordinator(plan, owner_instance_id="sync-owner", operation="full-sync")
    sources.items["source-a"]["desiredState"] = "retired"
    sources.items["source-a"]["_etag"] = "retired-etag"
    retired_plan = repository.read_source_plan("source-a")
    with pytest.raises(SourceRegistryConflictError, match="not_quiescent"):
        repository.begin_operator_maintenance(
            retired_plan, owner_instance_id="retire-owner", operation="source-retirement",
        )
    repository.mark_drive_coordinator_draining("source-a", "sync-owner")
    with pytest.raises(SourceRegistryConflictError, match="not_quiescent"):
        repository.begin_operator_maintenance(
            retired_plan, owner_instance_id="retire-owner", operation="source-retirement",
        )
    repository.settle_drive_coordinator("source-a", "sync-owner")
    first = repository.begin_operator_maintenance(
        retired_plan, owner_instance_id="retire-owner", operation="source-retirement",
    )
    assert repository.begin_operator_maintenance(
        retired_plan, owner_instance_id="retire-owner", operation="source-retirement",
    ) == first
    with pytest.raises(SourceRegistryConflictError, match="not_quiescent"):
        repository.begin_operator_maintenance(
            retired_plan, owner_instance_id="other-owner", operation="source-retirement",
        )
    repository.settle_drive_coordinator("source-a", "retire-owner")
    assert repository.get_drive_coordinator("source-a")["state"] == "settled"


def test_existing_owner_cannot_continue_under_changed_catalog_for_new_work() -> None:
    repository, _, _ = _repository([_source("source-a")])
    plan = _accept_source(repository, "source-a")
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="orchestration-a", operation="full-sync",
    )
    changed_plan = SourcePlan(plan.source, "new-catalog-etag", plan.binding_digest)

    with pytest.raises(SourceRegistryConflictError, match="catalog_changed"):
        repository.acquire_drive_coordinator(
            changed_plan,
            owner_instance_id="orchestration-a",
            operation="delta-sync",
        )

    current_owner = repository.assert_drive_coordinator_owner(
        "source-a", "orchestration-a",
    )
    assert current_owner["ownerInstanceId"] == "orchestration-a"


def test_unreserved_duplicate_drive_rejects_each_duplicate_and_keeps_other_sources() -> None:
    repository, _, runs = _repository([
        _source("source-a"), _source("source-b"), _source("source-c", "drive-c"),
    ])

    page = repository.list_source_inventory_page(page_size=100)

    assert [plan.source_id for plan in page.plans] == ["source-c"]
    assert page.duplicate_drive_source_ids == ("source-a", "source-b")
    assert page.rejected_source_ids == ()
    assert not any(key[1] == "drive-binding" for key in runs.items)


def test_duplicate_drive_keeps_reservation_owner_regardless_of_catalog_order() -> None:
    repository, sources, _ = _repository([_source("source-a")])
    _accept_source(repository, "source-a")
    sources.items = {
        "source-b": _source("source-b"),
        "source-a": sources.items["source-a"],
    }

    page = repository.list_source_inventory_page(page_size=100)

    assert [plan.source_id for plan in page.plans] == ["source-a"]
    assert page.duplicate_drive_source_ids == ("source-b",)


def test_duplicate_drive_is_detected_across_inventory_pages() -> None:
    repository, sources, _ = _repository([_source("source-a")])
    _accept_source(repository, "source-a")
    sources.items["source-b"] = _source("source-b")

    first = repository.list_source_inventory_page(page_size=1)
    second = repository.list_source_inventory_page(
        page_size=1, continuation_token=first.continuation_token,
    )

    assert [plan.source_id for plan in first.plans] == ["source-a"]
    assert first.duplicate_drive_source_ids == ()
    assert second.plans == ()
    assert second.duplicate_drive_source_ids == ("source-b",)


def test_source_binding_cannot_be_repointed_to_another_drive() -> None:
    repository, sources, _ = _repository([_source("source-a")])
    _accept_source(repository, "source-a")
    sources.items["source-a"]["config"]["driveId"] = "drive-repointed"
    sources.items["source-a"]["_etag"] = "etag-repointed"

    with pytest.raises(SourceRegistryConflictError, match="immutable"):
        repository.read_source_plan("source-a")


def test_pause_creates_non_retired_latch_that_resume_preserves() -> None:
    paused = _source("source-a", desiredState="paused")
    repository, sources, runs = _repository([paused])

    _accept_source(repository, "source-a")
    gate = runs.items[("source-a", "source-serving")]
    assert gate["retired"] is False

    sources.items["source-a"]["desiredState"] = "active"
    sources.items["source-a"]["_etag"] = "etag-resumed"
    resumed = repository.read_source_plan("source-a")
    assert resumed.catalog_etag == "etag-resumed"
    assert runs.items[("source-a", "source-serving")] == gate


def test_retirement_is_terminal_even_if_catalog_is_reactivated() -> None:
    retired = _source("source-a", desiredState="retired")
    repository, sources, runs = _repository([retired])
    _accept_source(repository, "source-a")
    assert runs.items[("source-a", "source-serving")]["retired"] is True

    sources.items["source-a"]["desiredState"] = "active"
    sources.items["source-a"]["_etag"] = "etag-reactivated"
    with pytest.raises(SourceRegistryConflictError, match="cannot_be_reactivated"):
        repository.read_source_plan("source-a")


def _settled_retirement(
    repository: SourceRegistryRepository,
    source_id: str,
) -> SourcePlan:
    plan = repository.read_source_plan(source_id)
    repository.begin_operator_maintenance(
        plan,
        owner_instance_id="retirement-owner",
        operation="source-retirement",
    )
    repository.settle_drive_coordinator(source_id, "retirement-owner")
    return plan


def test_reprovision_clears_operational_state_and_returns_same_binding_paused() -> None:
    repository, sources, runs = _repository([_source("source-a", desiredState="retired")])
    _accept_source(repository, "source-a")
    plan = _settled_retirement(repository, "source-a")
    runs.create_item(body={
        "id": "delta-round:old",
        "sourceId": "source-a",
        "recordType": "delta-round",
    })
    runs.create_item(body={
        "id": "document-winner:old",
        "sourceId": "source-a",
        "recordType": "document-winner",
    })

    assert repository.inspect_source_reprovision(
        source_id="source-a",
        if_match=plan.catalog_etag,
    ) == plan
    first = repository.delete_source_reprovision_page(
        plan,
        owner_instance_id="reprovision-owner",
        operator_object_id="operator-object-id",
        operator="operator@example.com",
        page_size=1,
    )
    second = repository.delete_source_reprovision_page(
        plan,
        owner_instance_id="reprovision-owner",
        operator_object_id="operator-object-id",
        operator="operator@example.com",
        page_size=10,
    )
    completed = repository.complete_source_reprovision(
        plan,
        owner_instance_id="reprovision-owner",
    )

    assert first == {"deletedRecords": 1, "hasMore": True}
    assert second == {"deletedRecords": 2, "hasMore": False}
    assert completed.source.desired_state == "paused"
    assert completed.source.drive_id == plan.source.drive_id
    assert sources.items["source-a"]["desiredState"] == "paused"
    assert runs.items[("source-a", "source-serving")]["retired"] is False
    assert runs.items[("source-a", "source-serving")]["fullSyncRequired"] is True
    assert runs.items[("source-a", "drive-coordinator")]["state"] == "settled"
    assert runs.items[("source-a", "drive-coordinator")]["operation"] == "source-reprovision"
    assert ("source-a", "source-binding") in runs.items
    assert all(
        item_id in {
            "source-binding", "source-serving", "drive-coordinator", "source-reprovision",
        }
        for source_id, item_id in runs.items
        if source_id == "source-a"
    )
    assert repository.read_source_plan("source-a") == completed
    with pytest.raises(SourceRegistryConflictError, match="source_full_sync_required"):
        repository.change_source_state(
            source_id="source-a",
            desired_state="active",
            if_match=completed.catalog_etag,
        )

    repository.acquire_drive_coordinator(
        completed,
        owner_instance_id="bootstrap-owner",
        operation="full-sync",
        allow_reprovision_bootstrap=True,
    )
    repository.settle_drive_coordinator("source-a", "bootstrap-owner")
    repository.complete_reprovision_full_sync(
        completed,
        owner_instance_id="bootstrap-owner",
    )
    assert runs.items[("source-a", "source-serving")]["fullSyncRequired"] is False
    activated = repository.change_source_state(
        source_id="source-a",
        desired_state="active",
        if_match=completed.catalog_etag,
    )
    assert activated.source.desired_state == "active"


def test_reprovision_refuses_unknown_source_partition_record() -> None:
    repository, _, runs = _repository([_source("source-a", desiredState="retired")])
    _accept_source(repository, "source-a")
    plan = _settled_retirement(repository, "source-a")
    runs.create_item(body={
        "id": "future-safety-record",
        "sourceId": "source-a",
        "recordType": "future-safety-record",
    })

    with pytest.raises(SourceRegistryConflictError, match="unknown_record"):
        repository.delete_source_reprovision_page(
            plan,
            owner_instance_id="reprovision-owner",
            operator_object_id="operator-object-id",
            operator="operator@example.com",
            page_size=10,
        )

    assert ("source-a", "future-safety-record") in runs.items


def test_reprovision_replaces_prepared_intent_that_never_acquired_maintenance() -> None:
    repository, _, runs = _repository([_source("source-a", desiredState="retired")])
    _accept_source(repository, "source-a")
    plan = _settled_retirement(repository, "source-a")
    runs.create_item(body={
        "id": "source-reprovision",
        "sourceId": "source-a",
        "recordType": "source-reprovision",
        "schemaVersion": 1,
        "ownerInstanceId": "ended-owner",
        "originalCatalogEtag": plan.catalog_etag,
        "acceptedBindingDigest": plan.binding_digest,
        "operatorObjectId": "old-operator",
        "operator": "old@example.com",
        "phase": "prepared",
        "deletedOperationalRecords": 0,
        "createdAt": "2026-10-08T00:00:00Z",
        "updatedAt": "2026-10-08T00:00:00Z",
    })

    result = repository.delete_source_reprovision_page(
        plan,
        owner_instance_id="replacement-owner",
        operator_object_id="replacement-operator",
        operator="replacement@example.com",
        page_size=10,
    )

    assert result == {"deletedRecords": 0, "hasMore": False}
    intent = runs.items[("source-a", "source-reprovision")]
    assert intent["ownerInstanceId"] == "replacement-owner"
    assert intent["operatorObjectId"] == "replacement-operator"


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (lambda _sources, runs: runs.items.__setitem__(
            ("source-a", "source-serving"),
            runs.items[("source-a", "source-serving")] | {"retired": False},
        ), "source_retirement_incomplete"),
        (lambda _sources, runs: runs.items.__setitem__(
            ("source-a", "drive-coordinator"),
            runs.items[("source-a", "drive-coordinator")] | {"state": "draining"},
        ), "drive_coordinator_not_quiescent"),
        (lambda sources, _runs: sources.items["source-a"].__setitem__("desiredState", "active"),
         "source_not_retired"),
    ],
)
def test_reprovision_preflight_fails_closed(mutate, error: str) -> None:
    repository, sources, runs = _repository([_source("source-a", desiredState="retired")])
    _accept_source(repository, "source-a")
    plan = _settled_retirement(repository, "source-a")
    mutate(sources, runs)

    with pytest.raises(SourceRegistryConflictError, match=error):
        repository.inspect_source_reprovision(
            source_id="source-a",
            if_match=plan.catalog_etag,
        )


def test_reprovision_finalize_failure_stays_fail_closed_and_can_retry() -> None:
    repository, sources, runs = _repository([_source("source-a", desiredState="retired")])
    _accept_source(repository, "source-a")
    plan = _settled_retirement(repository, "source-a")
    repository.delete_source_reprovision_page(
        plan,
        owner_instance_id="reprovision-owner",
        operator_object_id="operator-object-id",
        operator="operator@example.com",
        page_size=10,
    )
    runs.before_batch = lambda: (_ for _ in ()).throw(RuntimeError("batch unavailable"))

    with pytest.raises(SourceRegistryError, match="source_reprovision_finalize_failed"):
        repository.complete_source_reprovision(
            plan,
            owner_instance_id="reprovision-owner",
        )

    assert sources.items["source-a"]["desiredState"] == "paused"
    assert runs.items[("source-a", "source-serving")]["retired"] is True
    assert runs.items[("source-a", "drive-coordinator")]["state"] == "maintenance"

    completed = repository.complete_source_reprovision(
        plan,
        owner_instance_id="reprovision-owner",
    )
    assert completed.source.desired_state == "paused"
    assert runs.items[("source-a", "source-serving")]["fullSyncRequired"] is True


def test_reprovision_can_resume_partial_finalization_after_ended_owner_settles() -> None:
    repository, sources, runs = _repository([_source("source-a", desiredState="retired")])
    _accept_source(repository, "source-a")
    plan = _settled_retirement(repository, "source-a")
    repository.delete_source_reprovision_page(
        plan,
        owner_instance_id="ended-owner",
        operator_object_id="operator-object-id",
        operator="operator@example.com",
        page_size=10,
    )
    runs.before_batch = lambda: (_ for _ in ()).throw(RuntimeError("batch unavailable"))
    with pytest.raises(SourceRegistryError, match="source_reprovision_finalize_failed"):
        repository.complete_source_reprovision(
            plan,
            owner_instance_id="ended-owner",
        )
    repository.settle_drive_coordinator("source-a", "ended-owner")

    recovery = repository.inspect_source_reprovision(
        source_id="source-a",
        if_match=sources.items["source-a"]["_etag"],
    )
    repository.delete_source_reprovision_page(
        recovery,
        owner_instance_id="recovery-owner",
        operator_object_id="recovery-operator",
        operator="recovery@example.com",
        page_size=10,
    )
    completed = repository.complete_source_reprovision(
        recovery,
        owner_instance_id="recovery-owner",
    )

    assert completed.source.desired_state == "paused"
    assert runs.items[("source-a", "source-serving")]["fullSyncRequired"] is True
    assert runs.items[("source-a", "source-reprovision")]["operatorObjectId"] == "recovery-operator"


def test_reprovision_page_recovers_when_batch_commits_but_response_is_lost() -> None:
    repository, _, runs = _repository([_source("source-a", desiredState="retired")])
    _accept_source(repository, "source-a")
    plan = _settled_retirement(repository, "source-a")
    runs.create_item(body={
        "id": "delta-round:old",
        "sourceId": "source-a",
        "recordType": "delta-round",
    })
    runs.after_batch = lambda: (_ for _ in ()).throw(RuntimeError("response lost"))

    result = repository.delete_source_reprovision_page(
        plan,
        owner_instance_id="reprovision-owner",
        operator_object_id="operator-object-id",
        operator="operator@example.com",
        page_size=10,
    )

    assert result == {"deletedRecords": 1, "hasMore": False}
    assert ("source-a", "delta-round:old") not in runs.items
    assert runs.items[("source-a", "source-reprovision")][
        "deletedOperationalRecords"
    ] == 1


def test_reprovision_finalize_recovers_when_batch_commits_but_response_is_lost() -> None:
    repository, _, runs = _repository([_source("source-a", desiredState="retired")])
    _accept_source(repository, "source-a")
    plan = _settled_retirement(repository, "source-a")
    repository.delete_source_reprovision_page(
        plan,
        owner_instance_id="reprovision-owner",
        operator_object_id="operator-object-id",
        operator="operator@example.com",
        page_size=10,
    )
    runs.after_batch = lambda: (_ for _ in ()).throw(RuntimeError("response lost"))

    completed = repository.complete_source_reprovision(
        plan,
        owner_instance_id="reprovision-owner",
    )

    assert completed.source.desired_state == "paused"
    assert runs.items[("source-a", "source-serving")]["fullSyncRequired"] is True
    assert runs.items[("source-a", "source-reprovision")]["phase"] == "awaiting-full-sync"


def test_source_plan_payload_round_trip_preserves_identity() -> None:
    item = _source("source-a")
    source = SharePointSource.from_item(item)
    plan = SourcePlan(source, item["_etag"], "a" * 64)

    assert SourcePlan.from_payload(plan.to_payload()) == plan


def test_registering_existing_source_id_preserves_source_and_drive_identity() -> None:
    from ingestion.models import create_document_id

    repository, sources, runs = _repository([])
    winner = {
        "id": "document-winner:legacy",
        "sourceId": "legacy-source",
        "recordType": "document-winner",
        "schemaVersion": 1,
        "sourceRunId": "legacy-source:run-old",
        "documentKey": "legacy-source:run-old:document-old",
    }
    subscription = {
        "id": "webhook-subscription",
        "sourceId": "legacy-source",
        "subscriptionId": "subscription-legacy",
    }
    runs.items[("legacy-source", winner["id"])] = deepcopy(winner)
    runs.items[("legacy-source", subscription["id"])] = deepcopy(subscription)
    plan = repository.register_sharepoint_source(
        source_id="legacy-source",
        site_id="graph-site-id",
        site_url="https://tenant.sharepoint.com/sites/legacy",
        drive_id="legacy-drive-id",
    )
    plan = repository.change_source_state(
        source_id="legacy-source", desired_state="active", if_match=plan.catalog_etag,
    )

    assert plan.source_id == "legacy-source"
    assert plan.source.drive_id == "legacy-drive-id"
    assert sources.items["legacy-source"]["desiredState"] == "active"
    assert runs.items[("legacy-source", "source-binding")]["driveId"] == "legacy-drive-id"
    assert create_document_id("legacy-source", "legacy-drive-id", "item-1") == create_document_id(
        plan.source.source_id, plan.source.drive_id, "item-1",
    )
    assert runs.items[("legacy-source", winner["id"])] == winner
    assert runs.items[("legacy-source", subscription["id"])] == subscription
    repository.acquire_drive_coordinator(
        plan, owner_instance_id="legacy-owner", operation="full-sync",
    )
    with pytest.raises(SourceRegistryConflictError, match="already_active"):
        repository.acquire_drive_coordinator(
            plan, owner_instance_id="replacement-owner", operation="delta-sync",
        )


def _register(repository: SourceRegistryRepository, source_id: str = "source-new", drive_id: str = "drive-new"):
    return repository.register_sharepoint_source(
        source_id=source_id,
        site_id=f"site-{source_id}",
        site_url=f"https://tenant.sharepoint.com/sites/{source_id}/",
        drive_id=drive_id,
    )


def test_register_creates_paused_accepted_source() -> None:
    repository, sources, runs = _repository([])

    plan = _register(repository)

    assert plan.source.desired_state == "paused"
    assert sources.items["source-new"]["config"]["siteUrl"] == "https://tenant.sharepoint.com/sites/source-new"
    assert repository.read_source_plan("source-new") == plan
    assert runs.items[("source-new", "source-serving")]["retired"] is False


def test_register_exact_retry_returns_existing_plan() -> None:
    repository, _, _ = _repository([])
    first = _register(repository)

    assert _register(repository) == first


@pytest.mark.parametrize("failing_step", ["catalog", "source-binding", "drive-binding", "source-serving"])
def test_register_retry_completes_after_failure_at_each_persisted_step(failing_step: str) -> None:
    repository, sources, runs = _repository([])
    container = sources if failing_step == "catalog" else runs
    original_create = container.create_item
    failed = []

    def create_once_failing(*, body: dict[str, Any]) -> dict[str, Any]:
        if not failed and (failing_step == "catalog" or body["id"] == failing_step):
            failed.append(body["id"])
            raise RuntimeError("cosmos unavailable")
        return original_create(body=body)

    container.create_item = create_once_failing
    with pytest.raises(RuntimeError, match="cosmos unavailable"):
        _register(repository)

    plan = _register(repository)

    assert repository.read_source_plan("source-new") == plan
    with pytest.raises(SourceRegistryConflictError, match="reservation_conflict"):
        _register(repository, source_id="source-other")


def test_register_rejects_existing_source_id_with_different_drive() -> None:
    repository, _, _ = _repository([])
    _register(repository)

    with pytest.raises(SourceRegistryConflictError, match="source_id_conflict"):
        _register(repository, drive_id="drive-other")


def test_register_rejects_drive_reserved_by_another_source_before_writing() -> None:
    repository, sources, _ = _repository([])
    _register(repository)

    with pytest.raises(SourceRegistryConflictError, match="reservation_conflict"):
        _register(repository, source_id="source-other")

    assert "source-other" not in sources.items


def test_concurrent_same_drive_registration_leaves_no_usable_second_source() -> None:
    repository, _, runs = _repository([])
    original_create = runs.create_item

    def create_with_concurrent_winner(*, body: dict[str, Any]) -> dict[str, Any]:
        if body["id"] == "drive-binding" and body["ownerSourceId"] == "source-loser":
            runs.create_item = original_create
            _register(repository, source_id="source-winner")
        return original_create(body=body)

    runs.create_item = create_with_concurrent_winner
    with pytest.raises(SourceRegistryConflictError, match="reservation_conflict"):
        _register(repository, source_id="source-loser")

    assert repository.read_source_plan("source-winner").source.drive_id == "drive-new"
    with pytest.raises(SourceRegistryConflictError):
        repository.read_source_plan("source-loser")


def _change(repository: SourceRegistryRepository, sources: FakeSources, state: str, etag: str | None = None):
    return repository.change_source_state(
        source_id="source-new",
        desired_state=state,
        if_match=etag or sources.items["source-new"]["_etag"],
    )


@pytest.mark.parametrize(
    ("start", "target", "retired"),
    [
        ("paused", "active", False),
        ("active", "paused", False),
        ("active", "retired", True),
        ("paused", "retired", True),
    ],
)
def test_change_state_applies_allowed_transition(
    start: str, target: str, retired: bool,
) -> None:
    repository, sources, runs = _repository([])
    _register(repository)
    if start == "active":
        _change(repository, sources, "active")

    plan = _change(repository, sources, target)

    assert sources.items["source-new"]["desiredState"] == target
    assert plan.catalog_etag == sources.items["source-new"]["_etag"]
    assert repository.read_source_plan("source-new") == plan
    gate = runs.items[("source-new", "source-serving")]
    assert gate["retired"] is retired


def test_incomplete_registration_cannot_be_activated() -> None:
    repository, sources, runs = _repository([])
    original_create = runs.create_item

    def fail_drive_binding(*, body: dict[str, Any]) -> dict[str, Any]:
        if body["id"] == "drive-binding":
            raise RuntimeError("cosmos unavailable")
        return original_create(body=body)

    runs.create_item = fail_drive_binding
    with pytest.raises(RuntimeError, match="cosmos unavailable"):
        _register(repository)

    with pytest.raises(SourceRegistryError, match="source_binding_not_accepted"):
        _change(repository, sources, "active")

    assert sources.items["source-new"]["desiredState"] == "paused"


def test_change_state_rejects_stale_etag_without_writing() -> None:
    repository, sources, _ = _repository([])
    _register(repository)
    before = deepcopy(sources.items["source-new"])

    with pytest.raises(SourceRegistryConflictError, match="source_etag_mismatch"):
        _change(repository, sources, "active", etag="etag-stale")

    assert sources.items["source-new"] == before


def test_change_state_maps_concurrent_replace_to_etag_mismatch() -> None:
    repository, sources, _ = _repository([])
    _register(repository)

    def concurrent_replace(**_kwargs: Any) -> dict[str, Any]:
        raise _PreconditionFailed()

    sources.replace_item = concurrent_replace

    with pytest.raises(SourceRegistryConflictError, match="source_etag_mismatch"):
        _change(repository, sources, "active")


@pytest.mark.parametrize("target", ["active", "paused"])
def test_retired_source_cannot_change_state(target: str) -> None:
    repository, sources, _ = _repository([])
    _register(repository)
    _change(repository, sources, "retired")

    with pytest.raises(SourceRegistryConflictError, match="retired_source_cannot_be_reactivated"):
        _change(repository, sources, target)

    assert sources.items["source-new"]["desiredState"] == "retired"


def test_retired_flag_blocks_reactivation_when_catalog_was_edited() -> None:
    repository, sources, _ = _repository([])
    _register(repository)
    _change(repository, sources, "retired")
    sources.items["source-new"]["desiredState"] = "paused"

    with pytest.raises(SourceRegistryConflictError, match="retired_source_cannot_be_reactivated"):
        _change(repository, sources, "active")


def test_change_state_of_unknown_source_is_missing() -> None:
    repository, sources, _ = _repository([])

    with pytest.raises(SourceRegistryError, match="source_registration_missing"):
        repository.change_source_state(source_id="source-new", desired_state="active", if_match="etag")


def test_retire_lifecycle_failure_is_completed_by_same_state_retry() -> None:
    from retrieval.source_policy import SourcePolicyReader

    repository, sources, runs = _repository([])
    _register(repository)
    _change(repository, sources, "active")
    original_replace = runs.replace_item

    def failing_gate_replace(**kwargs: Any) -> dict[str, Any]:
        if kwargs["item"] == "source-serving":
            raise RuntimeError("cosmos unavailable")
        return original_replace(**kwargs)

    runs.replace_item = failing_gate_replace
    with pytest.raises(SourceRegistryError, match="source_lifecycle_pending"):
        _change(repository, sources, "retired")

    assert sources.items["source-new"]["desiredState"] == "retired"
    catalog = type("Catalog", (), {"query_items": lambda _self, **_kw: list(sources.items.values())})()
    assert SourcePolicyReader(catalog).capture().eligible_source_ids == []
    assert runs.items[("source-new", "source-serving")]["retired"] is False

    runs.replace_item = original_replace
    _change(repository, sources, "retired")

    assert runs.items[("source-new", "source-serving")]["retired"] is True


@pytest.mark.parametrize("source_id", ["a/b", "a\\b", "a\nb", "x" * 1024])
def test_source_id_rejects_cosmos_unsafe_values(source_id: str) -> None:
    source = _source("source-a")
    source["id"] = source["sourceId"] = source_id

    with pytest.raises(SourceRegistryError, match="sourceId_invalid"):
        SharePointSource.from_item(source)


def test_unknown_source_kind_fails_closed() -> None:
    source = _source("source-a", sourceKind="azure-blob-container")

    with pytest.raises(SourceRegistryError, match="record_invalid"):
        SharePointSource.from_item(source)


def test_source_registry_rejects_unrecognized_fields() -> None:
    source = _source("source-a", operatorOverride=True)

    with pytest.raises(SourceRegistryError, match="record_invalid"):
        SharePointSource.from_item(source)


def test_source_config_rejects_unknown_provider_fields() -> None:
    source = _source("source-a")
    source["config"]["endpoint"] = "https://untrusted.invalid"

    with pytest.raises(SourceRegistryError, match="config_invalid"):
        SharePointSource.from_item(source)


def test_source_registry_accepts_standard_https_port() -> None:
    source = _source("source-a")
    source["config"]["siteUrl"] = "https://tenant.sharepoint.com:443/sites/source-a"

    parsed = SharePointSource.from_item(source)

    assert parsed.site_url == "https://tenant.sharepoint.com:443/sites/source-a"


def test_inventory_reports_one_invalid_source_without_blocking_valid_sources() -> None:
    valid = _source("source-a")
    invalid = _source("source-b", "drive-b", sourceKind="unknown-provider")
    repository, _, _ = _repository([valid, invalid])

    inventory = repository.list_source_inventory_page(page_size=100)

    assert [plan.source_id for plan in inventory.plans] == ["source-a"]
    assert inventory.rejected_source_ids == ("source-b",)
    assert inventory.unattributed_errors == 0


def test_source_inventory_page_forwards_and_returns_continuation_token() -> None:
    first = _source("source-a", "drive-a")
    second = _source("source-b", "drive-b")
    repository, sources, _ = _repository([first, second])
    requested_tokens: list[str | None] = []
    original_query = sources.query_items

    class Page:
        def __init__(self, rows: list[dict[str, Any]], token: str | None) -> None:
            self.rows = rows
            self.continuation_token = token
            self.yielded = False

        def __iter__(self):
            return self

        def __next__(self):
            if self.yielded:
                raise StopIteration
            self.yielded = True
            return self.rows

    class QueryIterator:
        def by_page(self, token: str | None = None) -> Page:
            requested_tokens.append(token)
            return Page([first] if token is None else [second], "page-2" if token is None else None)

    def query_items(**kwargs: Any):
        if "c.config.driveId = @driveId" in kwargs["query"]:
            return original_query(**kwargs)
        return QueryIterator()

    sources.query_items = query_items  # type: ignore[method-assign]
    first_page = repository.list_source_inventory_page(page_size=1)
    second_page = repository.list_source_inventory_page(
        page_size=1,
        continuation_token=first_page.continuation_token,
    )

    assert requested_tokens == [None, "page-2"]
    assert [plan.source_id for plan in first_page.plans] == ["source-a"]
    assert [plan.source_id for plan in second_page.plans] == ["source-b"]
    assert first_page.continuation_token == "page-2"
    assert second_page.continuation_token is None


def test_source_inventory_refuses_unpaged_catalog_materialization() -> None:
    repository, sources, _ = _repository([_source("source-a")])

    class UnpagedIterator:
        def __iter__(self):
            raise AssertionError("full catalog must not be materialized")

    sources.query_items = lambda **_kwargs: UnpagedIterator()  # type: ignore[method-assign]

    with pytest.raises(SourceRegistryError, match="paging_unavailable"):
        repository.list_source_inventory_page(page_size=1)


def test_source_inventory_rejects_provider_page_larger_than_requested_bound() -> None:
    repository, sources, _ = _repository([
        _source("source-a", "drive-a"),
        _source("source-b", "drive-b"),
    ])

    class OversizedIterator:
        def by_page(self, _continuation_token: str | None = None):
            return _RunsPager(
                [[_source("source-a", "drive-a"), _source("source-b", "drive-b")]],
                None,
            )

    sources.query_items = lambda **_kwargs: OversizedIterator()  # type: ignore[method-assign]

    with pytest.raises(SourceRegistryError, match="page_unbounded"):
        repository.list_source_inventory_page(page_size=1)


def test_source_dispatch_pages_are_unavailable_until_inventory_is_complete() -> None:
    repository, _, runs = _repository([])
    repository.begin_source_dispatch("dispatch-a")
    source_a = _source("source-a")
    source_b = _source("source-b", "drive-b")
    plan_a = SharePointSource.from_item(source_a)
    plan_b = SharePointSource.from_item(source_b)
    payload_a = SourcePlan(plan_a, source_a["_etag"], "a" * 64).to_payload()
    payload_b = SourcePlan(plan_b, source_b["_etag"], "b" * 64).to_payload()
    repository.stage_source_dispatch_item(
        "dispatch-a", "source-a", status="prepared", source_payload=payload_a,
    )
    repository.stage_source_dispatch_item(
        "dispatch-a", "source-b", status="prepared", source_payload=payload_b,
    )

    with pytest.raises(SourceRegistryConflictError, match="inventory_incomplete"):
        repository.list_source_dispatch_page("dispatch-a", page_size=1)

    repository.complete_source_dispatch("dispatch-a", {"valid": 2, "failed": 0})
    page = repository.list_source_dispatch_page("dispatch-a", page_size=1)

    assert len(page.items) == 1
    assert page.items[0]["sourceId"] == "source-a"
    assert page.items[0]["sourcePlan"] == payload_a
    assert page.continuation_token == "1"
    next_page = repository.list_source_dispatch_page(
        "dispatch-a", page_size=1, continuation_token=page.continuation_token,
    )
    assert next_page.items[0]["sourceId"] == "source-b"
    assert next_page.continuation_token is None
    repository.record_source_dispatch_outcome(
        "dispatch-a",
        "source-a",
        status="completed",
        child_instance_id="child-a",
        outcome={"status": "completed", "succeeded": 3},
    )
    outcome_page = repository.list_source_dispatch_outcomes_page(
        "dispatch-a", page_size=1,
    )
    assert outcome_page.items[0]["status"] == "completed"
    assert outcome_page.items[0]["childInstanceId"] == "child-a"
    assert outcome_page.items[0]["outcome"] == {"status": "completed", "succeeded": 3}
    repository.record_source_dispatch_outcome(
        "dispatch-a",
        "source-a",
        status="completed",
        child_instance_id="child-a",
        outcome={"status": "completed", "succeeded": 3},
    )
    repository.record_source_dispatch_outcome(
        "dispatch-a",
        "source-b",
        status="failed",
        child_instance_id="child-b",
        outcome={"status": "failed", "error": "source_orchestration_failed"},
    )
    next_outcome_page = repository.list_source_dispatch_outcomes_page(
        "dispatch-a", page_size=1, continuation_token=outcome_page.continuation_token,
    )
    assert next_outcome_page.items[0]["sourceId"] == "source-b"
    assert next_outcome_page.items[0]["status"] == "failed"
    manifest = next(
        item for item in runs.items.values()
        if item["recordType"] == "source-dispatch-manifest"
    )
    assert manifest["state"] == "ready"