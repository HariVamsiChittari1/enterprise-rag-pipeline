"""Tests for the post-ingestion lifecycle repository (ACL resync and delta sync)."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock

import pytest
from azure.core import MatchConditions

from ingestion.lifecycle_repository import (
    DocumentLifecycleRepository,
    LifecycleConflictError,
    LifecycleRepositoryError,
)
from ingestion.models import DocumentStatus, create_audio_control_partition_id
from ingestion.services import run_source_retirement_page


class FakeCosmosError(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__("sensitive sdk response")
        self.status_code = status_code


class FakePager:
    def __init__(self, page: list[dict[str, Any]], continuation_token: str | None) -> None:
        self._page = page
        self._returned = False
        self.continuation_token = continuation_token

    def __next__(self) -> list[dict[str, Any]]:
        if self._returned:
            raise StopIteration
        self._returned = True
        return self._page


class FakeQueryIterator:
    def __init__(self, page: list[dict[str, Any]], continuation_token: str | None) -> None:
        self._page = page
        self._continuation_token = continuation_token
        self.requested_continuation_token: str | None = None

    def by_page(self, continuation_token: str | None = None) -> FakePager:
        self.requested_continuation_token = continuation_token
        return FakePager(self._page, self._continuation_token)

    def __iter__(self):
        return iter(self._page)


class SplitPager:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self._pages = iter(pages)
        self.continuation_token = None

    def __next__(self) -> list[dict[str, Any]]:
        return next(self._pages)


class SplitIterator:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self._pages = pages

    def by_page(self, continuation_token: str | None = None) -> SplitPager:
        assert continuation_token is None
        return SplitPager(self._pages)


class FakeContainer:
    """A minimal fake covering read_item/patch_item/upsert_item/query_items/
    execute_item_batch/delete_item, enough to exercise DocumentLifecycleRepository."""

    def __init__(self, partition_field: str, items: dict[tuple[str, str], dict[str, Any]] | None = None) -> None:
        self.partition_field = partition_field
        self.items = items or {}
        self.patch_calls: list[dict[str, Any]] = []
        self.batch_calls: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.delete_calls: list[tuple[str, str]] = []
        self.query_pages: dict[str, list[list[dict[str, Any]]]] = {}
        self.before_patch: Any = None
        self.before_replace: Any = None
        self.batch_error: Exception | None = None

    def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
        key = (partition_key, item)
        if key not in self.items:
            raise FakeCosmosError(404)
        return deepcopy(self.items[key])

    def upsert_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        key = (body[self.partition_field], body["id"])
        self.items[key] = deepcopy(body)
        return deepcopy(body)

    def create_item(self, body: dict[str, Any]) -> dict[str, Any]:
        key = (body[self.partition_field], body["id"])
        if key in self.items:
            raise FakeCosmosError(409)
        stored = {**deepcopy(body), "_etag": "etag-created"}
        self.items[key] = stored
        return deepcopy(stored)

    def replace_item(
        self,
        *,
        item: str,
        body: dict[str, Any],
        etag: str | None = None,
        match_condition: MatchConditions | None = None,
    ) -> dict[str, Any]:
        if self.before_replace is not None:
            callback = self.before_replace
            self.before_replace = None
            callback()
        key = (body[self.partition_field], item)
        if key not in self.items:
            raise FakeCosmosError(404)
        if etag is not None and self.items[key].get("_etag") != etag:
            raise FakeCosmosError(412)
        stored = {**deepcopy(body), "_etag": "etag-replaced"}
        self.items[key] = stored
        return deepcopy(stored)

    def patch_item(
        self,
        *,
        item: str,
        partition_key: str,
        patch_operations: list[dict[str, Any]],
        filter_predicate: str | None = None,
        etag: str | None = None,
        match_condition: MatchConditions | None = None,
    ) -> dict[str, Any]:
        self.patch_calls.append(
            {"item": item, "partition_key": partition_key, "patch_operations": patch_operations}
        )
        if self.before_patch is not None:
            callback = self.before_patch
            self.before_patch = None
            callback()
        key = (partition_key, item)
        if key not in self.items:
            raise FakeCosmosError(404)
        stored = self.items[key]
        if etag is not None and stored.get("_etag") != etag:
            raise FakeCosmosError(412)
        if filter_predicate is not None and "status = 'ready' OR" in filter_predicate:
            is_acl_restoration = (
                "retiredReason = 'acl_revoked'" in filter_predicate
                and stored.get("status") == "retired"
                and stored.get("retiredReason") == "acl_revoked"
            )
            allowed_statuses = ("ready", "acl_refreshing")
            if "OR c.status = 'retired'" in filter_predicate:
                allowed_statuses += ("retired",)
            if "OR c.status = 'failed'" in filter_predicate:
                allowed_statuses += ("failed",)
            if stored.get("status") not in allowed_statuses and not is_acl_restoration:
                raise FakeCosmosError(412)
        elif filter_predicate is not None and "status = 'acl_refreshing'" in filter_predicate:
            if stored.get("status") != "acl_refreshing":
                raise FakeCosmosError(412)
        for operation in patch_operations:
            path = operation["path"].lstrip("/")
            if operation["op"] == "remove":
                stored.pop(path, None)
            else:
                stored[path] = operation["value"]
        return deepcopy(stored)

    def execute_item_batch(
        self,
        *,
        batch_operations: list[tuple[Any, ...]],
        partition_key: str,
        no_response: bool = False,
    ) -> list[dict[str, Any]]:
        self.batch_calls.append((partition_key, deepcopy(batch_operations)))
        if self.batch_error is not None:
            error = self.batch_error
            self.batch_error = None
            raise error
        for operation in batch_operations:
            kind, args = operation[:2]
            if kind != "patch":
                raise AssertionError(f"unsupported batch op: {kind}")
            item_id, patch_operations = args
            key = (partition_key, item_id)
            stored = self.items[key]
            options = operation[2] if len(operation) > 2 else {}
            predicate = options.get("filter_predicate", "")
            if "lifecycleGeneration <=" in predicate:
                maximum_generation = int(predicate.rsplit(" ", 1)[-1])
                if stored["lifecycleGeneration"] > maximum_generation:
                    raise FakeCosmosError(412)
            for operation in patch_operations:
                stored[operation["path"].lstrip("/")] = operation["value"]
        return [{"statusCode": 200} for _ in batch_operations]

    def delete_item(
        self,
        *,
        item: str,
        partition_key: str,
        etag: str | None = None,
        match_condition: MatchConditions | None = None,
    ) -> None:
        self.delete_calls.append((item, partition_key))
        key = (partition_key, item)
        if key not in self.items:
            raise FakeCosmosError(404)
        if etag is not None and self.items[key].get("_etag") != etag:
            raise FakeCosmosError(412)
        del self.items[key]

    def query_items(
        self,
        *,
        query: str,
        parameters: list[dict[str, Any]],
        max_item_count: int,
        partition_key: str | None = None,
        enable_cross_partition_query: bool | None = None,
    ) -> FakeQueryIterator:
        parameter_values = {parameter["name"]: parameter["value"] for parameter in parameters}
        rows = []
        for (pk, item_id), stored in self.items.items():
            if partition_key is not None and pk != partition_key:
                continue
            if "@statuses" in parameter_values:
                if stored.get("status") not in parameter_values["@statuses"]:
                    is_acl_revoked = (
                        stored.get("status") == parameter_values.get("@retiredStatus")
                        and stored.get("retiredReason") == parameter_values.get("@retiredReason")
                    )
                    if not is_acl_revoked:
                        continue
            elif "@retiredStatus" in parameter_values:
                if (
                    stored.get("status") != parameter_values["@retiredStatus"]
                    or stored.get("retiredReason") != parameter_values.get("@retiredReason")
                ):
                    continue
            if "@documentId" in parameter_values and stored.get("documentId") != parameter_values["@documentId"]:
                continue
            if "@status" in parameter_values and stored.get("status") != parameter_values["@status"]:
                continue
            if "@stage" in parameter_values and stored.get("stage") != parameter_values["@stage"]:
                continue
            if "@processing" in parameter_values:
                in_progress = (
                    stored.get("status") == parameter_values["@processing"]
                    and stored.get("stage") == parameter_values["@transcribing"]
                )
                terminal_permit = (
                    stored.get("status") in {
                        parameter_values["@ready"], parameter_values["@failed"],
                    }
                    and stored.get("stage") == parameter_values["@terminal"]
                    and stored.get("transcriptionWorkId") is not None
                    and stored.get("transcriptionAttemptId") is not None
                )
                if not (in_progress or terminal_permit):
                    continue
                if (
                    parameter_values.get("@settlementOnly") is True
                    and in_progress
                    and (
                        stored.get("transcriptionWorkId") is None
                        or stored.get("transcriptionAttemptId") is None
                    )
                ):
                    continue
            if "@sourceId" in parameter_values and stored.get("sourceId") != parameter_values["@sourceId"]:
                continue
            if "@documentKey" in parameter_values and stored.get("documentKey") != parameter_values["@documentKey"]:
                continue
            if "@recordType" in parameter_values and stored.get("recordType") != parameter_values["@recordType"]:
                continue
            if "c.isRetrievable = true" in query and stored.get("isRetrievable") is not True:
                continue
            rows.append(deepcopy(stored))
        if "@pageSize" in parameter_values:
            rows = rows[:parameter_values["@pageSize"]]
        return FakeQueryIterator(rows, None)


def _ready_document(document_id: str = "doc-1", **overrides: Any) -> dict[str, Any]:
    base = {
        "id": document_id,
        "schemaVersion": 1,
        "recordType": "source_document",
        "sourceId": "source",
        "driveId": "drive",
        "documentId": document_id,
        "sourceRunId": "source:run-a",
        "documentKey": f"source:run-a:{document_id}",
        "itemId": "item-1",
        "status": "ready",
        "lifecycleGeneration": 0,
        "_ts": 1,
        "allowedGroupIds": ["group-a"],
        "aclHash": "hash-a",
        "_etag": "etag-1",
    }
    base.update(overrides)
    return base


def test_cross_partition_scans_consume_internal_pages_without_continuation() -> None:
    documents = FakeContainer("sourceRunId")
    chunks = FakeContainer("documentKey")
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)

    documents.query_items = lambda **_: SplitIterator([  # type: ignore[method-assign]
        [_ready_document("doc-1")],
        [_ready_document("doc-2", itemId="item-2")],
    ])
    ready_page = repo.list_ready_documents_page(source_id="source", page_size=50)
    assert tuple(item.document_id for item in ready_page.items) == ("doc-1", "doc-2")
    assert ready_page.continuation_token is None

    documents.query_items = lambda **_: SplitIterator([  # type: ignore[method-assign]
        [_ready_document("doc-1", status="acl_refreshing")],
        [_ready_document("doc-2", status="retiring")],
    ])
    transition_page = repo.list_lifecycle_transitions_page(source_id="source", page_size=50)
    assert tuple(item.document_id for item in transition_page.items) == ("doc-1", "doc-2")
    assert transition_page.continuation_token is None

    documents.query_items = lambda **_: SplitIterator([  # type: ignore[method-assign]
        [{"documentId": "doc-1"}],
        [{"documentId": "doc-2"}],
    ])
    duplicate_page = repo.list_duplicate_ready_document_ids_page(source_id="source", page_size=50)
    assert duplicate_page.document_ids == ("doc-1", "doc-2")
    assert duplicate_page.continuation_token is None

    chunks.query_items = lambda **_: SplitIterator([  # type: ignore[method-assign]
        [{"id": "chunk-1", "documentKey": "key-1", "sourceRunId": "run-1", "documentId": "doc-1"}],
        [{"id": "chunk-2", "documentKey": "key-2", "sourceRunId": "run-2", "documentId": "doc-2"}],
    ])
    manifest_page = repo.list_chunk_manifest_refs_page(source_id="source", page_size=50)
    assert tuple(item.chunk_id for item in manifest_page.items) == ("chunk-1", "chunk-2")
    assert manifest_page.continuation_token is None


def test_duplicate_ready_scan_is_streamable_and_deduplicates_each_page() -> None:
    documents = FakeContainer("sourceRunId")
    captured: dict[str, Any] = {}
    iterator = FakeQueryIterator(
        [
            {"documentId": "doc-1"},
            {"documentId": "doc-1"},
            {"documentId": "doc-2"},
        ],
        "next-page",
    )

    def _query_items(**kwargs: Any) -> FakeQueryIterator:
        captured.update(kwargs)
        return iterator

    documents.query_items = _query_items  # type: ignore[method-assign]
    repo = DocumentLifecycleRepository(
        FakeContainer("sourceId"), documents, FakeContainer("documentKey")
    )

    page = repo.list_duplicate_ready_document_ids_page(
        source_id="source",
        page_size=10, continuation_token="current-page"
    )

    assert page.document_ids == ("doc-1", "doc-2")
    assert page.continuation_token == "next-page"
    assert iterator.requested_continuation_token == "current-page"
    assert captured["query"] == (
        "SELECT c.documentId FROM c WHERE c.sourceId = @sourceId "
        "AND c.recordType = @recordType "
        "AND c.status = 'ready'"
    )
    assert captured["parameters"] == [
        {"name": "@sourceId", "value": "source"},
        {"name": "@recordType", "value": "source_document"}
    ]
    assert "GROUP BY" not in captured["query"]
    assert "DISTINCT" not in captured["query"]
    assert "ORDER BY" not in captured["query"]
    assert captured["enable_cross_partition_query"] is True


@pytest.mark.parametrize("row", [{}, {"documentId": ""}, {"documentId": 42}])
def test_duplicate_ready_scan_rejects_malformed_rows(row: dict[str, Any]) -> None:
    documents = FakeContainer("sourceRunId")
    documents.query_items = lambda **_: FakeQueryIterator([row], None)  # type: ignore[method-assign]
    repo = DocumentLifecycleRepository(
        FakeContainer("sourceId"), documents, FakeContainer("documentKey")
    )

    with pytest.raises(LifecycleRepositoryError, match="duplicate-ready row"):
        repo.list_duplicate_ready_document_ids_page(source_id="source", page_size=10)


def test_retire_document_flips_status_and_requires_ready_precondition() -> None:
    runs = FakeContainer("sourceId")
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    repo = DocumentLifecycleRepository(runs, documents, FakeContainer("documentKey"))

    repo.retire_document(
        source_run_id="source:run-a", document_id="doc-1",
        document_key="source:run-a:doc-1", etag="etag-1", reason="acl_revoked",
    )

    stored = documents.items[("source:run-a", "doc-1")]
    assert stored["status"] == "retired"
    assert stored["retiredReason"] == "acl_revoked"
    assert stored["retiredAt"]


def test_retire_document_rejects_unsupported_reason() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, FakeContainer("documentKey"))

    with pytest.raises(ValueError, match="retired_reason"):
        repo.retire_document(
            source_run_id="source:run-a", document_id="doc-1",
            document_key="source:run-a:doc-1", etag="etag-1", reason="nope",
        )


def test_retire_document_conflict_when_already_retired_or_stale_etag() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document(status="retired")})
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, FakeContainer("documentKey"))

    with pytest.raises(LifecycleConflictError):
        repo.retire_document(
            source_run_id="source:run-a", document_id="doc-1",
            document_key="source:run-a:doc-1", etag="etag-1", reason="superseded",
        )


def test_refresh_document_acl_patches_document_and_every_chunk() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    chunks = FakeContainer(
        "documentKey",
        {
            ("source:run-a:doc-1", "chunk:000000"): {
                "id": "chunk:000000", "documentKey": "source:run-a:doc-1", "allowedGroupIds": ["group-a"],
                "isRetrievable": True, "lifecycleGeneration": 0,
            },
            ("source:run-a:doc-1", "chunk:000001"): {
                "id": "chunk:000001", "documentKey": "source:run-a:doc-1", "allowedGroupIds": ["group-a"],
                "isRetrievable": True, "lifecycleGeneration": 0,
            },
        },
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)

    repo.refresh_document_acl(
        source_run_id="source:run-a", document_id="doc-1", document_key="source:run-a:doc-1",
        etag="etag-1", allowed_group_ids=("group-a", "group-b"), acl_hash="hash-b",
    )

    assert documents.items[("source:run-a", "doc-1")]["allowedGroupIds"] == ["group-a", "group-b"]
    assert documents.items[("source:run-a", "doc-1")]["aclHash"] == "hash-b"
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["allowedGroupIds"] == ["group-a", "group-b"]
    assert chunks.items[("source:run-a:doc-1", "chunk:000001")]["allowedGroupIds"] == ["group-a", "group-b"]
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["isRetrievable"] is True
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["lifecycleGeneration"] == 1
    assert len(chunks.batch_calls) == 2


def test_refresh_document_acl_restores_only_acl_revoked_document() -> None:
    documents = FakeContainer(
        "sourceRunId",
        {("source:run-a", "doc-1"): _ready_document(
            status="retired",
            retiredAt="2026-08-28T00:00:00Z",
            retiredReason="acl_revoked",
        )},
    )
    chunks = FakeContainer(
        "documentKey",
        {("source:run-a:doc-1", "chunk:000000"): {
            "id": "chunk:000000",
            "documentKey": "source:run-a:doc-1",
            "allowedGroupIds": ["group-a"],
            "isRetrievable": False,
            "lifecycleGeneration": 1,
        }},
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)

    repo.refresh_document_acl(
        source_run_id="source:run-a",
        document_id="doc-1",
        document_key="source:run-a:doc-1",
        etag="etag-1",
        allowed_group_ids=("group-b",),
        acl_hash="hash-b",
    )

    stored = documents.items[("source:run-a", "doc-1")]
    assert stored["status"] == "ready"
    assert "retiredAt" not in stored
    assert "retiredReason" not in stored
    assert stored["allowedGroupIds"] == ["group-b"]
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["isRetrievable"] is True
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["allowedGroupIds"] == ["group-b"]


def test_acl_revoked_restoration_converges_after_chunk_failure() -> None:
    documents = FakeContainer(
        "sourceRunId",
        {("source:run-a", "doc-1"): _ready_document(
            status="retired",
            lifecycleGeneration=1,
            retiredAt="2026-08-28T00:00:00Z",
            retiredReason="acl_revoked",
        )},
    )
    chunks = FakeContainer(
        "documentKey",
        {("source:run-a:doc-1", "chunk:000000"): {
            "id": "chunk:000000",
            "documentKey": "source:run-a:doc-1",
            "allowedGroupIds": ["group-a"],
            "isRetrievable": False,
            "lifecycleGeneration": 1,
        }},
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)
    original_batch = chunks.execute_item_batch
    chunks.execute_item_batch = lambda **_: (_ for _ in ()).throw(FakeCosmosError(503))  # type: ignore[method-assign]

    with pytest.raises(LifecycleRepositoryError, match="chunk lifecycle batch"):
        repo.refresh_document_acl(
            source_run_id="source:run-a",
            document_id="doc-1",
            document_key="source:run-a:doc-1",
            etag="etag-1",
            allowed_group_ids=("group-b",),
            acl_hash="hash-b",
        )

    stored = documents.items[("source:run-a", "doc-1")]
    assert stored["status"] == "acl_refreshing"
    assert "retiredAt" not in stored
    assert "retiredReason" not in stored
    assert stored["pendingAllowedGroupIds"] == ["group-b"]
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["isRetrievable"] is False

    stored["_etag"] = "etag-2"
    chunks.execute_item_batch = original_batch  # type: ignore[method-assign]
    with pytest.raises(LifecycleConflictError):
        repo.refresh_document_acl(
            source_run_id="source:run-a",
            document_id="doc-1",
            document_key="source:run-a:doc-1",
            etag="etag-1",
            allowed_group_ids=("group-b",),
            acl_hash="hash-b",
        )

    repo.refresh_document_acl(
        source_run_id="source:run-a",
        document_id="doc-1",
        document_key="source:run-a:doc-1",
        etag="etag-2",
        allowed_group_ids=("group-b",),
        acl_hash="hash-b",
    )

    assert stored["status"] == "ready"
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["isRetrievable"] is True
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["allowedGroupIds"] == ["group-b"]


@pytest.mark.parametrize("retired_reason", ["deleted", "superseded"])
def test_refresh_document_acl_rejects_non_restorable_retirement(retired_reason: str) -> None:
    documents = FakeContainer(
        "sourceRunId",
        {("source:run-a", "doc-1"): _ready_document(
            status="retired",
            retiredAt="2026-08-28T00:00:00Z",
            retiredReason=retired_reason,
        )},
    )
    repo = DocumentLifecycleRepository(
        FakeContainer("sourceId"), documents, FakeContainer("documentKey")
    )

    with pytest.raises(LifecycleConflictError):
        repo.refresh_document_acl(
            source_run_id="source:run-a",
            document_id="doc-1",
            document_key="source:run-a:doc-1",
            etag="etag-1",
            allowed_group_ids=("group-b",),
            acl_hash="hash-b",
        )


def test_refresh_document_acl_stays_fail_closed_and_is_retryable_after_chunk_failure() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    chunks = FakeContainer(
        "documentKey",
        {
            ("source:run-a:doc-1", "chunk:000000"): {
                "id": "chunk:000000", "documentKey": "source:run-a:doc-1",
                "allowedGroupIds": ["group-a"],
                "isRetrievable": True, "lifecycleGeneration": 0,
            },
        },
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)
    original_batch = chunks.execute_item_batch

    def _fail_batch(**kwargs: Any) -> None:
        raise FakeCosmosError(503)

    chunks.execute_item_batch = _fail_batch  # type: ignore[method-assign]
    with pytest.raises(LifecycleRepositoryError, match="chunk lifecycle batch"):
        repo.refresh_document_acl(
            source_run_id="source:run-a", document_id="doc-1",
            document_key="source:run-a:doc-1", etag="etag-1",
            allowed_group_ids=("group-b",), acl_hash="hash-b",
        )

    stored = documents.items[("source:run-a", "doc-1")]
    assert stored["status"] == "acl_refreshing"
    assert stored["aclHash"] == "hash-a"
    assert repo.list_ready_documents_page(source_id="source", page_size=10).items[0].status.value == "acl_refreshing"

    chunks.execute_item_batch = original_batch  # type: ignore[method-assign]
    repo.refresh_document_acl(
        source_run_id="source:run-a", document_id="doc-1",
        document_key="source:run-a:doc-1", etag="etag-1",
        allowed_group_ids=("group-b",), acl_hash="hash-b",
    )

    assert stored["status"] == "ready"
    assert stored["aclHash"] == "hash-b"
    assert chunks.items[("source:run-a:doc-1", "chunk:000000")]["allowedGroupIds"] == ["group-b"]


def test_chunk_transition_rejects_stale_generation() -> None:
    chunks = FakeContainer(
        "documentKey",
        {("source:run-a:doc-1", "chunk:000000"): {
            "id": "chunk:000000", "documentKey": "source:run-a:doc-1",
            "allowedGroupIds": ["group-a"],
            "isRetrievable": False, "lifecycleGeneration": 2,
        }},
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), FakeContainer("sourceRunId"), chunks)

    with pytest.raises(LifecycleConflictError, match="newer chunk lifecycle generation"):
        repo.set_document_chunks_retrievable(
            document_key="source:run-a:doc-1",
            lifecycle_generation=1,
            is_retrievable=True,
        )

    assert chunks.batch_calls == []


def test_chunk_transition_inspects_batch_response_status() -> None:
    chunks = FakeContainer(
        "documentKey",
        {("source:run-a:doc-1", "chunk:000000"): {
            "id": "chunk:000000", "documentKey": "source:run-a:doc-1",
            "allowedGroupIds": ["group-a"],
            "isRetrievable": False, "lifecycleGeneration": 1,
        }},
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), FakeContainer("sourceRunId"), chunks)
    chunks.execute_item_batch = lambda **_: [{"statusCode": 412}]  # type: ignore[method-assign]

    with pytest.raises(LifecycleConflictError, match="generation changed"):
        repo.set_document_chunks_retrievable(
            document_key="source:run-a:doc-1",
            lifecycle_generation=1,
            is_retrievable=True,
        )


def test_chunk_transition_suppresses_large_batch_write_responses() -> None:
    document_key = "source:run-a:doc-1"
    chunks = FakeContainer(
        "documentKey",
        {(document_key, "chunk:000000"): {
            "id": "chunk:000000", "documentKey": document_key,
            "allowedGroupIds": ["group-a"],
            "isRetrievable": False, "lifecycleGeneration": 1,
        }},
    )
    repo = DocumentLifecycleRepository(
        FakeContainer("sourceId"), FakeContainer("sourceRunId"), chunks
    )
    captured: dict[str, Any] = {}

    def _execute(**kwargs: Any) -> list[dict[str, Any]]:
        captured.update(kwargs)
        for operation in kwargs["batch_operations"]:
            _, (item_id, patch_operations), _ = operation
            stored = chunks.items[(kwargs["partition_key"], item_id)]
            for patch in patch_operations:
                stored[patch["path"].lstrip("/")] = patch["value"]
        return [{"statusCode": 200}]

    chunks.execute_item_batch = _execute  # type: ignore[method-assign]

    repo.set_document_chunks_retrievable(
        document_key=document_key,
        lifecycle_generation=1,
        is_retrievable=True,
    )

    assert captured["no_response"] is True


@pytest.mark.parametrize("chunk_count", [1, 99, 100, 101, 205, 2000])
def test_chunk_transition_converges_across_batch_boundaries(chunk_count: int) -> None:
    document_key = "source:run-a:doc-1"
    chunks = FakeContainer(
        "documentKey",
        {(document_key, f"chunk:{index:06d}"): {
            "id": f"chunk:{index:06d}",
            "documentKey": document_key,
            "allowedGroupIds": ["group-a"],
            "isRetrievable": False,
            "lifecycleGeneration": 0,
        } for index in range(chunk_count)},
    )
    repo = DocumentLifecycleRepository(
        FakeContainer("sourceId"), FakeContainer("sourceRunId"), chunks
    )

    transitioned = repo.set_document_chunks_retrievable(
        document_key=document_key,
        lifecycle_generation=1,
        is_retrievable=True,
        allowed_group_ids=("group-b",),
        expected_count=chunk_count,
    )

    assert transitioned == chunk_count
    assert len(chunks.batch_calls) == (chunk_count + 99) // 100
    assert all(item["isRetrievable"] is True for item in chunks.items.values())
    assert all(item["lifecycleGeneration"] == 1 for item in chunks.items.values())
    assert all(item["allowedGroupIds"] == ["group-b"] for item in chunks.items.values())


def test_chunk_transition_replay_converges_after_committed_response_is_lost() -> None:
    document_key = "source:run-a:doc-1"
    chunks = FakeContainer(
        "documentKey",
        {(document_key, "chunk:000000"): {
            "id": "chunk:000000",
            "documentKey": document_key,
            "allowedGroupIds": ["group-a"],
            "isRetrievable": False,
            "lifecycleGeneration": 0,
        }},
    )
    repo = DocumentLifecycleRepository(
        FakeContainer("sourceId"), FakeContainer("sourceRunId"), chunks
    )
    committed_batch = chunks.execute_item_batch
    response_lost = True

    def _commit_then_lose_response(**kwargs: Any) -> list[dict[str, Any]]:
        nonlocal response_lost
        result = committed_batch(**kwargs)
        if response_lost:
            response_lost = False
            raise FakeCosmosError(503)
        return result

    chunks.execute_item_batch = _commit_then_lose_response  # type: ignore[method-assign]
    assert repo.set_document_chunks_retrievable(
        document_key=document_key,
        lifecycle_generation=1,
        is_retrievable=True,
        expected_count=1,
    ) == 1


def test_delete_document_and_chunks_removes_everything() -> None:
    documents = FakeContainer(
        "sourceRunId",
        {
            ("source:run-a", "doc-1"): _ready_document(),
            ("source:run-a", "visual-manifest:doc-1:000000"): {
                "id": "visual-manifest:doc-1:000000",
                "recordType": "visual_manifest_page",
                "sourceRunId": "source:run-a",
                "documentId": "doc-1",
            },
        },
    )
    chunks = FakeContainer(
        "documentKey",
        {("source:run-a:doc-1", "chunk:000000"): {
            "id": "chunk:000000", "documentKey": "source:run-a:doc-1",
            "isRetrievable": True, "lifecycleGeneration": 0,
        }},
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)

    repo.delete_document_and_chunks(
        source_run_id="source:run-a", document_id="doc-1", document_key="source:run-a:doc-1", etag="etag-1"
    )

    assert ("source:run-a", "doc-1") not in documents.items
    assert ("source:run-a", "visual-manifest:doc-1:000000") not in documents.items
    assert ("source:run-a:doc-1", "chunk:000000") not in chunks.items


@pytest.mark.parametrize(
    "status", ["ready", "failed", "processing", "admitting", "acl_refreshing", "retiring", "deleting", "retired"],
)
def test_source_retirement_deletes_each_document_state_and_audits(status: str) -> None:
    document = _ready_document(status=status, retiredReason="acl_revoked")
    documents = FakeContainer("sourceRunId", {
        ("source:run-a", "doc-1"): document,
        ("source:run-a", "visual-page"): {
            "id": "visual-page", "sourceRunId": "source:run-a",
            "recordType": "visual_manifest_page", "documentId": "doc-1",
        },
    })
    chunks = FakeContainer("documentKey", {
        ("source:run-a:doc-1", "chunk-1"): {
            "id": "chunk-1", "documentKey": "source:run-a:doc-1",
            "sourceId": "source", "content": "retained text", "embedding": [0.1],
            "isRetrievable": True, "lifecycleGeneration": 0, "_etag": "chunk-etag",
        },
    })
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)

    audit = MagicMock()
    assert _delete_source_page(repo, audit) == 1
    assert _delete_source_page(repo, audit) == 0
    assert ("source:run-a", "doc-1") not in documents.items
    assert ("source:run-a", "visual-page") not in documents.items
    assert ("source:run-a:doc-1", "chunk-1") not in chunks.items
    audit.create_item.assert_called_once()
    record = audit.create_item.call_args.args[0]
    assert record["operation"] == "document_deleted"
    assert record["sourceId"] == "source"
    assert record["documentId"] == "doc-1"
    assert record["reason"] == "source_retired"
    assert record["method"] == "source_retirement"
    assert "content" not in record


def _delete_source_page(repo: DocumentLifecycleRepository, audit: Any = None) -> int:
    registry = MagicMock()
    registry.get_document_winner.return_value = None
    return run_source_retirement_page(
        repo, audit if audit is not None else MagicMock(),
        source_id="source", page_size=1, work_registry=registry,
        owner_instance_id="retirement-owner",
    )


def test_source_retirement_pages_are_bounded_and_source_isolated() -> None:
    documents = FakeContainer("sourceRunId", {
        ("source:run-a", "doc-1"): _ready_document("doc-1"),
        ("source:run-a", "doc-2"): _ready_document("doc-2"),
        ("other:run", "other"): _ready_document(
            "other", sourceId="other", sourceRunId="other:run", documentKey="other:run:other",
        ),
    })
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, FakeContainer("documentKey"))
    assert _delete_source_page(repo) == 1
    assert documents.items[("source:run-a", "doc-2")]["status"] == "ready"
    assert _delete_source_page(repo) == 1
    assert _delete_source_page(repo) == 0
    assert documents.items[("other:run", "other")]["status"] == "ready"


def test_source_retirement_retries_interrupted_deletion_without_success_audit() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    chunks = FakeContainer("documentKey", {
        ("source:run-a:doc-1", "chunk-1"): {
            "id": "chunk-1", "documentKey": "source:run-a:doc-1",
            "isRetrievable": True, "lifecycleGeneration": 0,
        },
    })
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)
    audit = MagicMock()
    chunks.batch_error = FakeCosmosError(412)
    with pytest.raises(LifecycleRepositoryError, match="chunk lifecycle batch"):
        _delete_source_page(repo, audit)
    assert documents.items[("source:run-a", "doc-1")]["status"] == "deleting"
    audit.create_item.assert_not_called()
    assert _delete_source_page(repo, audit) == 1
    assert ("source:run-a", "doc-1") not in documents.items
    assert ("source:run-a:doc-1", "chunk-1") not in chunks.items
    audit.create_item.assert_called_once()


def test_source_retirement_deletes_orphans_without_touching_other_sources() -> None:
    chunks = FakeContainer("documentKey", {
        ("orphan", "chunk-1"): {
            "id": "chunk-1", "documentKey": "orphan", "sourceId": "source",
            "isRetrievable": True, "lifecycleGeneration": 2, "_etag": "etag-1",
        },
        ("orphan", "chunk-disabled"): {
            "id": "chunk-disabled", "documentKey": "orphan", "sourceId": "source",
            "isRetrievable": False, "lifecycleGeneration": 0, "_etag": "etag-disabled",
        },
        ("other", "chunk-2"): {
            "id": "chunk-2", "documentKey": "other", "sourceId": "other",
            "isRetrievable": True, "lifecycleGeneration": 0, "_etag": "etag-2",
        },
    })
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), FakeContainer("sourceRunId"), chunks)
    assert repo.delete_source_chunks_page(source_id="source", page_size=1) == 1
    assert ("orphan", "chunk-disabled") in chunks.items
    assert repo.delete_source_chunks_page(source_id="source", page_size=1) == 1
    assert repo.delete_source_chunks_page(source_id="source", page_size=1) == 0
    assert ("orphan", "chunk-1") not in chunks.items
    assert chunks.items[("other", "chunk-2")]["isRetrievable"] is True
    assert chunks.delete_calls == [("chunk-1", "orphan"), ("chunk-disabled", "orphan")]


def test_source_retirement_retries_partial_physical_delete_with_nonretrievable_tombstone() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    chunks = FakeContainer("documentKey", {
        ("source:run-a:doc-1", "chunk-1"): {
            "id": "chunk-1", "documentKey": "source:run-a:doc-1",
            "isRetrievable": True, "lifecycleGeneration": 0,
        },
    })
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)
    audit = MagicMock()
    original_delete = chunks.delete_item
    chunks.delete_item = MagicMock(side_effect=FakeCosmosError(503))
    with pytest.raises(LifecycleRepositoryError, match="Cosmos delete failed"):
        _delete_source_page(repo, audit)
    assert documents.items[("source:run-a", "doc-1")]["status"] == "deleting"
    assert chunks.items[("source:run-a:doc-1", "chunk-1")]["isRetrievable"] is False
    audit.create_item.assert_not_called()
    chunks.delete_item = original_delete
    assert _delete_source_page(repo, audit) == 1
    assert documents.items == chunks.items == {}
    audit.create_item.assert_called_once()


def test_source_retirement_preserves_delta_best_effort_audit_failure_semantics(
    caplog: pytest.LogCaptureFixture,
) -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, FakeContainer("documentKey"))
    audit = MagicMock()
    audit.create_item.side_effect = FakeCosmosError(503)
    assert _delete_source_page(repo, audit) == 1
    assert documents.items == {}
    assert "audit_write_failed" in caplog.text


def test_source_retirement_clears_matching_winner_and_retains_prior_audit_rows() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, FakeContainer("documentKey"))
    audit = FakeContainer("sourceId", {
        ("source", "previous-audit"): {"id": "previous-audit", "sourceId": "source"},
    })
    registry = MagicMock()
    registry.get_document_winner.return_value = {
        "sourceRunId": "source:run-a", "documentKey": "source:run-a:doc-1",
    }
    assert run_source_retirement_page(
        repo, audit, source_id="source", page_size=1,
        work_registry=registry, owner_instance_id="retire-owner",
    ) == 1
    registry.clear_document_winner.assert_called_once_with(
        "source", "retire-owner", document_id="doc-1",
        expected_source_run_id="source:run-a",
        expected_document_key="source:run-a:doc-1",
    )
    assert documents.items == {}
    assert ("source", "previous-audit") in audit.items
    assert len(audit.items) == 2
    assert audit.delete_calls == []


def test_retire_failure_leaves_manifest_non_ready() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    chunks = FakeContainer(
        "documentKey",
        {("source:run-a:doc-1", "chunk:000000"): {
            "id": "chunk:000000", "documentKey": "source:run-a:doc-1",
            "allowedGroupIds": ["group-a"],
            "isRetrievable": True, "lifecycleGeneration": 0,
        }},
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)
    chunks.execute_item_batch = lambda **_: (_ for _ in ()).throw(FakeCosmosError(503))  # type: ignore[method-assign]

    with pytest.raises(LifecycleRepositoryError, match="chunk lifecycle batch"):
        repo.retire_document(
            source_run_id="source:run-a", document_id="doc-1",
            document_key="source:run-a:doc-1", etag="etag-1", reason="acl_revoked",
        )

    stored = documents.items[("source:run-a", "doc-1")]
    assert stored["status"] == "retiring"
    assert stored["lifecycleGeneration"] == 1


def test_delete_failure_leaves_tombstone_non_ready() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    chunks = FakeContainer(
        "documentKey",
        {("source:run-a:doc-1", "chunk:000000"): {
            "id": "chunk:000000", "documentKey": "source:run-a:doc-1",
            "allowedGroupIds": ["group-a"],
            "isRetrievable": True, "lifecycleGeneration": 0,
        }},
    )
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)
    chunks.execute_item_batch = lambda **_: (_ for _ in ()).throw(FakeCosmosError(503))  # type: ignore[method-assign]

    with pytest.raises(LifecycleRepositoryError, match="chunk lifecycle batch"):
        repo.delete_document_and_chunks(
            source_run_id="source:run-a", document_id="doc-1",
            document_key="source:run-a:doc-1", etag="etag-1",
        )

    stored = documents.items[("source:run-a", "doc-1")]
    assert stored["status"] == "deleting"
    assert stored["lifecycleGeneration"] == 1


def test_delete_document_and_chunks_removes_failed_prior_version() -> None:
    documents = FakeContainer(
        "sourceRunId", {("source:run-a", "doc-1"): _ready_document(status="failed")}
    )
    chunks = FakeContainer("documentKey")
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)

    repo.delete_document_and_chunks(
        source_run_id="source:run-a", document_id="doc-1", document_key="source:run-a:doc-1", etag="etag-1"
    )

    assert ("source:run-a", "doc-1") not in documents.items


def test_delete_document_and_chunks_is_idempotent_when_already_gone() -> None:
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), FakeContainer("sourceRunId"), FakeContainer("documentKey"))

    repo.delete_document_and_chunks(
        source_run_id="source:run-a", document_id="doc-1", document_key="source:run-a:doc-1", etag="etag-1"
    )


def test_delete_document_and_chunks_conflict_on_stale_etag() -> None:
    documents = FakeContainer("sourceRunId", {("source:run-a", "doc-1"): _ready_document()})
    chunks = FakeContainer("documentKey")
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)

    with pytest.raises(LifecycleConflictError):
        repo.delete_document_and_chunks(
            source_run_id="source:run-a", document_id="doc-1", document_key="source:run-a:doc-1", etag="stale-etag"
        )

    # Document survives a rejected delete; only a matching etag removes it.
    assert ("source:run-a", "doc-1") in documents.items


@pytest.mark.parametrize("record_type", ["audio_operation", "audio_permit", "audio_transcript"])
@pytest.mark.parametrize("status", ["ready", "acl_refreshing", "retired"])
def test_list_ready_documents_page_and_find_by_document_id(record_type: str, status: str) -> None:
    control_partition = create_audio_control_partition_id("source")
    documents = FakeContainer(
        "sourceRunId",
        {
            ("source:run-a", "doc-1"): _ready_document("doc-1"),
            ("source:run-b", "doc-2"): _ready_document("doc-2", sourceRunId="source:run-b", documentKey="source:run-b:doc-2"),
            ("source:run-a", "doc-3"): _ready_document("doc-3", status="failed"),
            ("source:run-a", "doc-4"): _ready_document("doc-4", status="retired", retiredReason="acl_revoked"),
            ("source:run-a", "doc-5"): _ready_document("doc-5", status="retired", retiredReason="deleted"),
            ("source:run-a", "doc-6"): _ready_document("doc-6", status="retired", retiredReason="superseded"),
            ("source:run-a", "visual-manifest:doc-1:000000"): {
                "id": "visual-manifest:doc-1:000000",
                "recordType": "visual_manifest_page",
                "documentId": "doc-1",
                "sourceRunId": "source:run-a",
                "status": "ready",
            },
            (control_partition, "audio-operation:sentinel"): _ready_document(
                "audio-operation:sentinel", recordType=record_type,
                sourceRunId=control_partition, status=status, retiredReason="acl_revoked",
            ),
        },
    )
    captured: list[dict[str, Any]] = []
    original_query = documents.query_items

    def capture_query(**kwargs: Any) -> FakeQueryIterator:
        captured.append(kwargs)
        return original_query(**kwargs)

    documents.query_items = capture_query  # type: ignore[method-assign]
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, FakeContainer("documentKey"))

    page = repo.list_ready_documents_page(source_id="source", page_size=10)
    assert {ref.document_id for ref in page.items} == {"doc-1", "doc-2", "doc-4"}

    found = repo.find_ready_document_by_document_id("source", "doc-2")
    assert found is not None
    assert found.source_run_id == "source:run-b"

    assert repo.find_ready_document_by_document_id("source", "doc-missing") is None
    assert repo.find_ready_document_by_document_id("source", "audio-operation:sentinel") is None
    assert len(captured) == 4
    for query in captured:
        assert "WHERE c.sourceId = @sourceId AND c.recordType = @recordType" in query["query"]
        assert {"name": "@sourceId", "value": "source"} in query["parameters"]
        assert {"name": "@recordType", "value": "source_document"} in query["parameters"]

def test_list_ready_document_versions_page_forwards_continuation_token() -> None:
    documents = FakeContainer("sourceRunId")
    first = _ready_document(document_id="doc-1", sourceRunId="source:run-a")
    second = _ready_document(
        document_id="doc-1", sourceRunId="source:run-b",
        status="failed", stage="terminal", failedAt="2026-08-05T12:00:00Z",
    )
    requested_tokens: list[str | None] = []

    class TokenAwareIterator:
        def by_page(self, continuation_token: str | None = None) -> FakePager:
            requested_tokens.append(continuation_token)
            if continuation_token is None:
                return FakePager([first], "versions-next")
            assert continuation_token == "versions-next"
            return FakePager([second], None)

    documents.query_items = lambda **_kwargs: TokenAwareIterator()  # type: ignore[method-assign]
    repository = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, FakeContainer("sourceId"))

    first_page = repository.list_ready_document_versions_page(
        source_id="source", document_id="doc-1", page_size=1,
    )
    second_page = repository.list_ready_document_versions_page(
        source_id="source", document_id="doc-1", page_size=1,
        continuation_token=first_page.continuation_token,
    )

    assert requested_tokens == [None, "versions-next"]
    assert first_page.items[0].source_run_id == "source:run-a"
    assert second_page.items[0].source_run_id == "source:run-b"
    assert second_page.items[0].status is DocumentStatus.FAILED
    assert second_page.continuation_token is None


def test_list_transcribing_documents_page_includes_terminal_unsettled_audio_work() -> None:
    documents = FakeContainer("sourceRunId", {
        ("source:run-a", "processing"): _ready_document(
            "processing", status="processing", stage="transcribing",
            transcriptionWorkId="work-processing", transcriptionAttemptId="attempt-processing",
        ),
        ("source:run-a", "unowned-processing"): _ready_document(
            "unowned-processing", status="processing", stage="transcribing",
        ),
        ("source:run-a", "ready"): _ready_document(
            "ready", status="ready", stage="terminal",
            transcriptionWorkId="work-ready", transcriptionAttemptId="attempt-ready",
        ),
        ("source:run-a", "failed"): _ready_document(
            "failed", status="failed", stage="terminal",
            transcriptionWorkId="work-failed", transcriptionAttemptId="attempt-failed",
        ),
        ("source:run-a", "unowned-ready"): _ready_document(
            "unowned-ready", status="ready", stage="terminal",
        ),
    })
    repository = DocumentLifecycleRepository(
        FakeContainer("sourceId"), documents, FakeContainer("documentKey"),
    )

    page = repository.list_transcribing_documents_page(source_id="source", page_size=10)

    assert {(item.document_id, item.status.value, item.transcription_work_id) for item in page.items} == {
        ("processing", "processing", "work-processing"),
        ("unowned-processing", "processing", None),
        ("ready", "ready", "work-ready"),
        ("failed", "failed", "work-failed"),
    }
    draining_page = repository.list_transcribing_documents_page(
        source_id="source", page_size=10, settlement_only=True,
    )
    assert {item.document_id for item in draining_page.items} == {
        "processing", "ready", "failed",
    }


def test_acl_revoked_lookup_and_authority_select_latest_matching_version() -> None:
    documents = FakeContainer(
        "sourceRunId",
        {
            ("source:run-a", "doc-1"): _ready_document(
                status="retired",
                sourceRunId="source:run-a",
                documentKey="source:run-a:doc-1",
                retiredReason="acl_revoked",
                eTag="source-etag",
                _ts=1,
            ),
            ("source:run-b", "doc-1"): _ready_document(
                status="retired",
                sourceRunId="source:run-b",
                documentKey="source:run-b:doc-1",
                retiredReason="acl_revoked",
                eTag="source-etag",
                _ts=2,
            ),
        },
    )
    repo = DocumentLifecycleRepository(
        FakeContainer("sourceId"), documents, FakeContainer("documentKey")
    )

    selected = repo.find_acl_revoked_document_by_document_id("source", "doc-1")

    assert selected is not None
    assert selected.source_run_id == "source:run-b"
    assert repo.is_authoritative_document_version(
        source_id="source",
        document_id="doc-1",
        source_run_id="source:run-b",
        source_etag="source-etag",
    ) is True
    assert repo.is_authoritative_document_version(
        source_id="source",
        document_id="doc-1",
        source_run_id="source:run-a",
        source_etag="source-etag",
    ) is False


def test_lifecycle_ready_queries_are_isolated_by_source_id() -> None:
    documents = FakeContainer(
        "sourceRunId",
        {
            ("source-a:run", "doc-a"): _ready_document(
                "doc-a", sourceId="source-a", sourceRunId="source-a:run",
                documentKey="source-a:run:doc-a",
            ),
            ("source-b:run", "doc-b"): _ready_document(
                "doc-b", sourceId="source-b", sourceRunId="source-b:run",
                documentKey="source-b:run:doc-b",
            ),
        },
    )
    captured: list[dict[str, Any]] = []
    query_items = documents.query_items

    def capture_query(**kwargs: Any) -> FakeQueryIterator:
        captured.append(kwargs)
        return query_items(**kwargs)

    documents.query_items = capture_query  # type: ignore[method-assign]
    repository = DocumentLifecycleRepository(
        FakeContainer("sourceId"), documents, FakeContainer("documentKey"),
    )

    page = repository.list_ready_documents_page(source_id="source-a", page_size=10)

    assert [item.document_id for item in page.items] == ["doc-a"]
    assert page.items[0].source_id == "source-a"
    assert page.items[0].drive_id == "drive"
    assert "c.sourceId = @sourceId" in captured[0]["query"]
    assert {p["name"]: p["value"] for p in captured[0]["parameters"]}["@sourceId"] == "source-a"


def test_pending_audio_lookup_is_scoped_to_source_and_document() -> None:
    documents = FakeContainer(
        "sourceRunId",
        {
            ("source-a:run", "audio-a"): _ready_document(
                "audio-a", sourceId="source-a", sourceRunId="source-a:run",
                status="processing", stage="transcribing", eTag="etag-a",
            ),
            ("source-b:run", "audio-b"): _ready_document(
                "audio-b", sourceId="source-b", sourceRunId="source-b:run",
                status="processing", stage="transcribing", eTag="etag-b",
            ),
        },
    )
    captured: list[dict[str, Any]] = []
    query_items = documents.query_items

    def capture_query(**kwargs: Any) -> FakeQueryIterator:
        captured.append(kwargs)
        return query_items(**kwargs)

    documents.query_items = capture_query  # type: ignore[method-assign]
    repository = DocumentLifecycleRepository(
        FakeContainer("sourceId"), documents, FakeContainer("documentKey"),
    )

    found = repository.find_transcribing_document_by_document_id("source-a", "audio-a")

    assert found is not None and found["eTag"] == "etag-a"
    assert repository.find_transcribing_document_by_document_id("source-a", "audio-b") is None
    assert "c.sourceId = @sourceId" in captured[0]["query"]


def test_active_document_blocks_acl_revoked_version_restoration() -> None:
    documents = FakeContainer(
        "sourceRunId",
        {
            ("source:run-a", "doc-1"): _ready_document(
                status="retired",
                sourceRunId="source:run-a",
                documentKey="source:run-a:doc-1",
                retiredReason="acl_revoked",
                eTag="source-etag",
                _ts=2,
            ),
            ("source:run-b", "doc-1"): _ready_document(
                sourceRunId="source:run-b",
                documentKey="source:run-b:doc-1",
                eTag="source-etag",
                _ts=1,
            ),
        },
    )
    repo = DocumentLifecycleRepository(
        FakeContainer("sourceId"), documents, FakeContainer("documentKey")
    )

    assert repo.is_authoritative_document_version(
        source_id="source",
        document_id="doc-1",
        source_run_id="source:run-a",
        source_etag="source-etag",
    ) is False


def test_delta_cursor_round_trip() -> None:
    runs = FakeContainer("sourceId")
    repo = DocumentLifecycleRepository(runs, FakeContainer("sourceRunId"), FakeContainer("documentKey"))

    assert repo.get_delta_cursor("source") is None

    repo.save_delta_cursor("source", "https://graph.microsoft.com/v1.0/drives/d/root/delta?token=abc")
    assert repo.get_delta_cursor("source") == "https://graph.microsoft.com/v1.0/drives/d/root/delta?token=abc"


def test_clear_webhook_subscription_is_source_partitioned_and_conditional() -> None:
    subscription = {
        "id": "webhook-subscription", "sourceId": "source-a",
        "subscriptionId": "sub-a", "_etag": "etag-sub-a",
    }
    runs = FakeContainer("sourceId", {("source-a", "webhook-subscription"): subscription})
    repository = DocumentLifecycleRepository(
        runs, FakeContainer("sourceRunId"), FakeContainer("documentKey"),
    )

    repository.clear_webhook_subscription_id("source-a")

    assert ("source-a", "webhook-subscription") not in runs.items
    assert runs.delete_calls == [("webhook-subscription", "source-a")]


def test_save_delta_cursor_rejects_empty_link() -> None:
    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), FakeContainer("sourceRunId"), FakeContainer("documentKey"))

    with pytest.raises(ValueError):
        repo.save_delta_cursor("source", "")


def test_trigger_instance_id_round_trip_is_independent_per_control_id() -> None:
    from ingestion.lifecycle_repository import ACL_RESYNC_TRIGGER_ID, DELTA_SYNC_TRIGGER_ID

    runs = FakeContainer("sourceId")
    repo = DocumentLifecycleRepository(runs, FakeContainer("sourceRunId"), FakeContainer("documentKey"))

    assert repo.get_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID) is None
    assert repo.get_trigger_instance_id("source", ACL_RESYNC_TRIGGER_ID) is None

    repo.save_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID, "delta-sync-trigger-abc123")
    repo.save_trigger_instance_id("source", ACL_RESYNC_TRIGGER_ID, "acl-resync-trigger-def456")

    assert repo.get_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID) == "delta-sync-trigger-abc123"
    assert repo.get_trigger_instance_id("source", ACL_RESYNC_TRIGGER_ID) == "acl-resync-trigger-def456"

    # Overwriting one control_id must not clobber the other or the unrelated delta cursor.
    repo.save_delta_cursor("source", "https://graph.microsoft.com/v1.0/drives/d/root/delta?token=abc")
    repo.save_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID, "delta-sync-trigger-xyz789")
    assert repo.get_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID) == "delta-sync-trigger-xyz789"
    assert repo.get_trigger_instance_id("source", ACL_RESYNC_TRIGGER_ID) == "acl-resync-trigger-def456"
    assert repo.get_delta_cursor("source") == "https://graph.microsoft.com/v1.0/drives/d/root/delta?token=abc"


def test_save_trigger_instance_id_rejects_empty_id() -> None:
    from ingestion.lifecycle_repository import DELTA_SYNC_TRIGGER_ID

    repo = DocumentLifecycleRepository(FakeContainer("sourceId"), FakeContainer("sourceRunId"), FakeContainer("documentKey"))

    with pytest.raises(ValueError):
        repo.save_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID, "")


def test_trigger_instance_claim_creates_missing_control() -> None:
    from ingestion.lifecycle_repository import DELTA_SYNC_TRIGGER_ID

    runs = FakeContainer("sourceId")
    repo = DocumentLifecycleRepository(runs, FakeContainer("sourceRunId"), FakeContainer("documentKey"))

    claimed = repo.try_claim_trigger_instance_id(
        "source",
        DELTA_SYNC_TRIGGER_ID,
        None,
        "delta-sync-trigger-new",
    )

    assert claimed is True
    assert repo.get_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID) == "delta-sync-trigger-new"


def test_trigger_instance_claim_replaces_expected_control() -> None:
    from ingestion.lifecycle_repository import DELTA_SYNC_TRIGGER_ID

    runs = FakeContainer(
        "sourceId",
        {
            ("source", DELTA_SYNC_TRIGGER_ID): {
                "id": DELTA_SYNC_TRIGGER_ID,
                "sourceId": "source",
                "currentInstanceId": "delta-sync-trigger-old",
                "_etag": "etag-old",
            }
        },
    )
    repo = DocumentLifecycleRepository(runs, FakeContainer("sourceRunId"), FakeContainer("documentKey"))

    claimed = repo.try_claim_trigger_instance_id(
        "source",
        DELTA_SYNC_TRIGGER_ID,
        "delta-sync-trigger-old",
        "delta-sync-trigger-new",
    )

    assert claimed is True
    assert repo.get_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID) == "delta-sync-trigger-new"


def test_trigger_instance_claim_loses_concurrent_etag_race() -> None:
    from ingestion.lifecycle_repository import DELTA_SYNC_TRIGGER_ID

    runs = FakeContainer(
        "sourceId",
        {
            ("source", DELTA_SYNC_TRIGGER_ID): {
                "id": DELTA_SYNC_TRIGGER_ID,
                "sourceId": "source",
                "currentInstanceId": "delta-sync-trigger-old",
                "_etag": "etag-old",
            }
        },
    )
    runs.before_replace = lambda: runs.items[("source", DELTA_SYNC_TRIGGER_ID)].update(
        {"currentInstanceId": "delta-sync-trigger-winner", "_etag": "etag-winner"}
    )
    repo = DocumentLifecycleRepository(runs, FakeContainer("sourceRunId"), FakeContainer("documentKey"))

    claimed = repo.try_claim_trigger_instance_id(
        "source",
        DELTA_SYNC_TRIGGER_ID,
        "delta-sync-trigger-old",
        "delta-sync-trigger-loser",
    )

    assert claimed is False
    assert repo.get_trigger_instance_id("source", DELTA_SYNC_TRIGGER_ID) == "delta-sync-trigger-winner"
