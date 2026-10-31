"""Tests for delta-sync and ACL-resync orchestration logic in services.py.

process_document() itself (extract/chunk/embed/write) is already exercised by the
full-sync path; these tests focus on the incremental orchestration: adapting delta
items, retiring superseded versions, hard-deleting on Graph deletion, and the
ACL-resync unchanged/updated/retired decision tree.
"""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from config import ExecutionBatchConfig, IngestionConfig
from ingestion.errors import TerminalDocumentError
from ingestion.graph import (
    DeltaResetRequired,
    DiscoveredPdf,
    DriveDelta,
    GraphPageLimitExceeded,
    VerifiedAcl,
)
from ingestion.lifecycle_repository import (
    ChunkManifestPage,
    ChunkManifestRef,
    DocumentLifecycleRepository,
    DuplicateDocumentPage,
    LifecycleConflictError,
    LifecycleDocumentPage,
    LifecycleDocumentRef,
    ReadyDocumentRef,
)
from ingestion.models import (
    AclFeedback,
    AclStatus,
    ActivityOutcome,
    ActivityStatus,
    DocumentStatus,
    SourceAdmissionConfig,
    SourceDocumentRecord,
    create_document_id,
    create_document_key,
    create_source_run_id,
)
from ingestion.repository import VersionedRecord
from ingestion.source_connector import SharePointConnector
import ingestion.services as services


def build_config(**overrides: Any) -> IngestionConfig:
    values = dict(
        extraction_enabled=True,
        enrichment_enabled=True,
        summary_enabled=False,
        key_phrases_enabled=True,
        entities_enabled=True,
        allowed_extensions=(".pdf",),
        source_id="source",
        drive_id="drive",
        tenant_id="tenant",
        app_client_id="app",
        certificate_secret_name="cert",
        key_vault_uri="https://kv.example",
        cosmos_endpoint="https://cosmos.example",
        cosmos_database="db",
        cosmos_ingestion_runs_container="ingestion-runs",
        cosmos_source_documents_container="source-documents",
        cosmos_search_chunks_container="search-chunks",
        document_intelligence_endpoint="https://di.example",
        content_understanding_endpoint="https://cu.example",
        content_understanding_analyzer_id="rag-document-search-v1",
        language_endpoint="https://lang.example",
        openai_endpoint="https://openai.example",
        embedding_deployment="test-embedding",
        vision_deployment="gpt-5.4",
        managed_identity_client_id="mi",
        admission=SourceAdmissionConfig(),
        execution=ExecutionBatchConfig(),
        chunk_max_tokens=800,
        chunk_overlap_tokens=100,
        vision_max_output_tokens=400,
        sharepoint_site_url="",
    )
    values.update(overrides)
    return IngestionConfig(**values)


def test_processing_profile_digest_is_stable_and_tracks_ingestion_settings() -> None:
    config = build_config()
    digest = services._processing_profile_digest(config)

    assert len(digest) == 64
    assert services._processing_profile_digest(config) == digest
    assert services._processing_profile_digest(replace(
        config, summary_enabled=not config.summary_enabled,
    )) != digest
    assert services._processing_profile_digest(replace(
        config, embedding_deployment="other-embedding",
    )) != digest
    assert services._processing_profile_digest(replace(
        config, chunk_max_tokens=config.chunk_max_tokens - 1,
    )) != digest
    assert services._processing_profile_digest(
        replace(
            config,
            admission=replace(config.admission, max_pages_per_source=1_999),
        )
    ) != digest
    assert services._processing_profile_digest(
        replace(
            config,
            admission=replace(config.admission, max_eligible_sources=9_999),
        )
    ) == digest
    assert services._processing_profile_digest(
        replace(
            config,
            execution=replace(
                config.execution,
                embedding_batch_size=64,
                wave_size=config.execution.wave_size + 1,
            ),
        )
    ) == digest
    assert services._processing_profile_digest(replace(
        config, business_name="Renamed Business",
    )) == digest


def test_discovered_document_snapshots_configured_business_name() -> None:
    config = build_config(business_name="Business A")
    source = DiscoveredPdf(
        item_id="item-a",
        parent_item_id="parent-a",
        name="guide.pdf",
        source_path="/guide.pdf",
        source_url="https://example.invalid/guide.pdf",
        e_tag="etag-a",
        mime_type="application/pdf",
        size_bytes=100,
        discovery_ordinal=1,
    )

    document = services._source_to_document(source, config, "run-a")

    assert document.business_name == "Business A"
    assert document.to_cosmos_item()["businessName"] == "Business A"


class FakeRepository:
    def __init__(self) -> None:
        self.created: list[Any] = []

    def create_discovered_document(self, document: Any) -> VersionedRecord:
        self.created.append(document)
        return VersionedRecord(document, "doc-etag-1")


class FakeLifecycleRepository:
    def __init__(self, cursor: str | None = None) -> None:
        self._cursor = cursor
        self.saved_cursors: list[str] = []
        self.retired: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.refreshed: list[dict[str, Any]] = []
        self.feedback_updated: list[dict[str, Any]] = []
        self.ready_by_document_id: dict[str, ReadyDocumentRef] = {}
        self.acl_revoked_by_document_id: dict[str, ReadyDocumentRef] = {}
        self.transcribing_by_document_id: dict[str, dict[str, Any]] = {}
        self.versions_by_document_id: dict[str, list[ReadyDocumentRef]] = {}
        self.ready_inventory_pages: list[list[ReadyDocumentRef]] = []
        self.authoritative_document_version = True

    def get_delta_cursor(self, source_id: str) -> str | None:
        return self._cursor

    def save_delta_cursor(self, source_id: str, delta_link: str) -> None:
        self.saved_cursors.append(delta_link)
        self._cursor = delta_link

    def find_ready_document_by_document_id(self, source_id: str, document_id: str) -> ReadyDocumentRef | None:
        return self.ready_by_document_id.get(document_id)

    def find_acl_revoked_document_by_document_id(self, source_id: str, document_id: str) -> ReadyDocumentRef | None:
        return self.acl_revoked_by_document_id.get(document_id)

    def find_transcribing_document_by_document_id(self, source_id: str, document_id: str):
        return self.transcribing_by_document_id.get(document_id)

    def list_ready_document_versions(self, source_id: str, document_id: str):
        versions = self.versions_by_document_id.get(document_id)
        if versions is not None:
            return tuple(versions)
        ref = self.ready_by_document_id.get(document_id)
        return (ref,) if ref is not None else ()

    def list_ready_documents_page(
        self, *, source_id: str, page_size: int, continuation_token: str | None = None,
    ):
        items = self.ready_inventory_pages.pop(0) if self.ready_inventory_pages else []
        return SimpleNamespace(items=tuple(items), continuation_token=None)

    def is_authoritative_document_version(self, **kwargs: Any) -> bool:
        return self.authoritative_document_version

    def delete_document_and_chunks(self, *, source_run_id: str, document_id: str, document_key: str, etag: str) -> None:
        self.deleted.append({"source_run_id": source_run_id, "document_id": document_id, "document_key": document_key, "etag": etag})

    def retire_document(
        self,
        *,
        source_run_id: str,
        document_id: str,
        document_key: str,
        etag: str,
        reason: str,
    ) -> None:
        self.retired.append({
            "source_run_id": source_run_id,
            "document_id": document_id,
            "document_key": document_key,
            "reason": reason,
        })

    def refresh_document_acl(self, **kwargs: Any) -> None:
        self.refreshed.append(kwargs)

    def update_document_acl_feedback(self, **kwargs: Any) -> None:
        self.feedback_updated.append(kwargs)


class RecordingWorkRegistry:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.operations: list[str] = []

    def begin_document_operation(self, _plan: Any, **kwargs: Any) -> dict[str, Any]:
        self.events.append("claim")
        self.operations.append(kwargs["operation_kind"])
        return {"status": "acquired", "operationId": "repair-operation"}

    def start_document_operation(self, *_args: Any, **_kwargs: Any) -> dict[str, str]:
        self.events.append("start")
        return {"status": "started"}

    def settle_document_operation(self, *_args: Any, **_kwargs: Any) -> None:
        self.events.append("settle")


class DeltaQuarantineRegistry:
    def __init__(
        self,
        winner: dict[str, Any] | None = None,
        replay_outcome: dict[str, Any] | None = None,
    ) -> None:
        self.winner = winner
        self.replay_outcome = replay_outcome
        self.unresolved = 0
        self.settlements: list[dict[str, Any]] = []
        self.cleared_winners: list[dict[str, Any]] = []
        self.cursor_outcomes: list[dict[str, Any]] = []

    def begin_delta_round(self, *_args: Any) -> dict[str, str]:
        return {"state": "active"}

    def delta_page_capture(self, *_args: Any) -> None:
        return None

    def get_document_winner(
        self, _source_id: str, _document_id: str
    ) -> dict[str, Any] | None:
        return self.winner

    def clear_document_winner(self, *_args: Any, **kwargs: Any) -> bool:
        self.cleared_winners.append(kwargs)
        self.winner = None
        return True

    def begin_document_operation(
        self, _plan: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        if self.replay_outcome is not None:
            return {
                "status": "completed",
                "terminalStatus": "quarantined",
                "outcome": self.replay_outcome,
            }
        self.unresolved = 1
        return {"status": "acquired", "operationId": "quarantine-operation"}

    def start_document_operation(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, str]:
        return {"status": "started"}

    def settle_document_operation(
        self, *_args: Any, **kwargs: Any
    ) -> dict[str, str]:
        self.settlements.append(kwargs)
        self.unresolved = 0
        return {"status": "completed"}

    def get_unresolved_document_operation_count(self, _source_id: str) -> int:
        return self.unresolved

    def mark_delta_round_cursor_ready(
        self, _source_id: str, _round_id: str, _owner_id: str,
        _delta_link: str, outcome: dict[str, Any],
    ) -> None:
        self.cursor_outcomes.append(outcome)

    def complete_delta_round(self, *_args: Any) -> dict[str, str]:
        return {"state": "committed"}


def _delta_pdf_item(
    item_id: str = "item-1",
    *,
    deleted: bool = False,
    name: str = "a.pdf",
    mime_type: str = "application/pdf",
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": item_id,
        "name": name,
        "eTag": "etag-x",
        "size": 100,
        "file": {"mimeType": mime_type},
        "parentReference": {"id": "parent", "path": "/drive/root:"},
        "webUrl": "https://example.invalid/a.pdf",
    }
    if deleted:
        item["deleted"] = {"state": "deleted"}
    return item


@pytest.mark.parametrize(
    ("name", "mime_type"),
    [
        ("guide.md", "text/markdown"),
        ("report.pdf", "application/pdf"),
        (
            "report.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        (
            "briefing.pptx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ),
        (
            "forecast.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
    ],
)
def test_delta_document_serializes_native_source_mime(
    name: str,
    mime_type: str,
) -> None:
    config = build_config(
        allowed_extensions=(".md", ".pdf", ".docx", ".pptx", ".xlsx")
    )

    document = services._delta_item_to_document(
        _delta_pdf_item(name=name, mime_type=mime_type),
        config,
        "run-1",
        0,
    )

    assert document is not None
    assert document.to_cosmos_item()["mimeType"] == mime_type


def _mock_connector(handler, drive_id: str = "drive") -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


# --- run_delta_sync ---

def test_run_delta_sync_initializes_from_full_delta_enumeration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(cursor=None)
    repository = FakeRepository()
    item = _delta_pdf_item()
    monkeypatch.setattr(
        services,
        "process_document",
        lambda *a, **k: ActivityOutcome(
            document_id="doc", status=ActivityStatus.SUCCEEDED,
            chunks_written=3, retry_count=0,
        ),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/drives/drive/root/delta")
        assert "token=latest" not in str(request.url)
        return httpx.Response(200, json={
            "value": [item],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=abc",
        })

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, repository, lifecycle, connector, None, None, None)

    assert outcome.bootstrapped is True
    assert outcome.created_or_updated == 1
    assert len(repository.created) == 1
    assert lifecycle.saved_cursors == ["https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=abc"]


def test_run_delta_sync_processes_add_and_advances_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old")
    repository = FakeRepository()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"value": [_delta_pdf_item("item-1")], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"},
        )

    monkeypatch.setattr(
        services, "process_document",
        lambda *a, **k: ActivityOutcome(document_id="doc", status=ActivityStatus.SUCCEEDED, chunks_written=3, retry_count=0),
    )

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, repository, lifecycle, connector, None, None, None)

    assert outcome.created_or_updated == 1
    assert outcome.failed == 0
    assert len(repository.created) == 1
    assert lifecycle.saved_cursors == ["https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"]
    assert lifecycle.retired == []  # no prior ready version -> pure add, nothing to retire


@pytest.mark.parametrize("source_order", [("source-a", "source-b"), ("source-b", "source-a")])
@pytest.mark.parametrize("fail_source_a", [False, True])
def test_two_source_discovery_then_delta_isolates_identity_cursors_and_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    source_order: tuple[str, str],
    fail_source_a: bool,
) -> None:
    configs = {
        source: build_config(
            source_id=source,
            drive_id=f"drive-{source}",
            sharepoint_site_url=f"https://tenant.sharepoint.com/sites/{source}",
        )
        for source in source_order
    }
    old_links = {
        source: f"https://graph.microsoft.com/v1.0/drives/{config.drive_id}/root/delta?token=old"
        for source, config in configs.items()
    }
    new_links = {source: link.replace("token=old", "token=new") for source, link in old_links.items()}
    cursor_items: dict[tuple[str, str], dict[str, Any]] = {}

    def save_cursor_item(*, body: dict[str, Any]) -> dict[str, Any]:
        cursor_items[(body["sourceId"], body["id"])] = dict(body)
        return body

    runs = MagicMock()
    runs.upsert_item.side_effect = save_cursor_item
    runs.read_item.side_effect = lambda *, item, partition_key: cursor_items[(partition_key, item)]
    cursor_repository = DocumentLifecycleRepository(runs, None, None)
    lifecycle = FakeLifecycleRepository()
    monkeypatch.setattr(lifecycle, "get_delta_cursor", cursor_repository.get_delta_cursor)
    monkeypatch.setattr(lifecycle, "save_delta_cursor", cursor_repository.save_delta_cursor)
    repository = FakeRepository()
    monkeypatch.setattr(repository, "get_source_control", lambda _source: None, raising=False)
    processed: list[tuple[str, str]] = []

    def process_delta_document(
        config: IngestionConfig, document: SourceDocumentRecord, *_args: Any, **_kwargs: Any,
    ) -> ActivityOutcome:
        processed.append((config.source_id, document.document_id))
        return ActivityOutcome(
            document_id=document.document_id,
            status=(
                ActivityStatus.FAILED
                if fail_source_a and config.source_id == "source-a"
                else ActivityStatus.SUCCEEDED
            ),
            chunks_written=0,
            retry_count=0,
        )

    monkeypatch.setattr(services, "process_document", process_delta_document)
    requested_delta_links: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        config = next(
            config for config in configs.values()
            if request.url.path.startswith(f"/v1.0/drives/{config.drive_id}/")
        )
        item = _delta_pdf_item("same-item")
        item["webUrl"] = f"{config.sharepoint_site_url}/a.pdf"
        if request.url.path.endswith("/root/children"):
            return httpx.Response(200, json={"value": [item]})
        assert str(request.url) == old_links[config.source_id]
        requested_delta_links.append(str(request.url))
        return httpx.Response(200, json={
            "value": [item],
            "@odata.deltaLink": new_links[config.source_id],
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        for source in source_order:
            config = configs[source]
            documents, scanned = services.discover_all(
                config, "full-run", repository, SharePointConnector(client, config.drive_id),
            )
            assert scanned == 1
            assert len(documents) == 1
            document = documents[0]
            assert (document.source_id, document.drive_id, document.item_id) == (
                source, config.drive_id, "same-item",
            )
            assert document.document_id == create_document_id(source, config.drive_id, "same-item")
            assert document.source_run_id == create_source_run_id(source, "full-run")
            cursor_repository.save_delta_cursor(source, old_links[source])

        full_documents = tuple(repository.created)
        assert full_documents[0].document_id != full_documents[1].document_id
        assert full_documents[0].document_key != full_documents[1].document_key
        expected_links = dict(old_links)
        for source in source_order:
            config = configs[source]
            outcome = services.run_delta_sync(
                config, repository, lifecycle, SharePointConnector(client, config.drive_id),
                None, None, None,
            )
            failed = fail_source_a and source == "source-a"
            assert outcome.failed == int(failed)
            assert outcome.created_or_updated == int(not failed)
            assert outcome.pending == outcome.deleted == outcome.acl_resynced == 0
            if not failed:
                expected_links[source] = new_links[source]
            assert {
                key: cursor_repository.get_delta_cursor(key) for key in source_order
            } == expected_links
            delta_document = repository.created[-1]
            assert (delta_document.source_id, delta_document.drive_id) == (source, config.drive_id)
            assert delta_document.document_id == create_document_id(source, config.drive_id, "same-item")
            assert delta_document.source_run_id.startswith(f"{source}:")
            assert delta_document.source_run_id != create_source_run_id(source, "full-run")

    assert requested_delta_links == [old_links[source] for source in source_order]
    assert processed == [
        (source, create_document_id(source, configs[source].drive_id, "same-item"))
        for source in source_order
    ]
    assert tuple(repository.created[:2]) == full_documents


def test_delta_sync_returns_durable_enumeration_continuation_without_committing_cursor() -> None:
    config = build_config(execution=ExecutionBatchConfig(delta_max_pages=1))
    committed_cursor = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old"
    lifecycle = FakeLifecycleRepository(cursor=committed_cursor)
    first_next = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?skip=2"
    second_next = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?skip=3"

    class Capture:
        def __init__(self) -> None:
            self.resume_calls = 0

        def resume_state(self):
            self.resume_calls += 1
            next_link = first_next if self.resume_calls == 1 else second_next
            return {
                "roundStartKind": "incremental",
                "roundStartUrl": committed_cursor,
                "receiptId": "receipt-1",
                "pageOrdinal": self.resume_calls,
                "requestUrl": first_next,
                "predecessorReceiptId": None,
                "nextLink": next_link,
                "deltaLink": None,
            }

        def count_observed_entries(self):
            return 2

    capture = Capture()

    class Registry:
        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return capture

    class Connector:
        def read_drive_delta(self, _max_pages, delta_link=None, *, page_capture=None):
            assert delta_link == first_next
            assert page_capture is capture
            raise GraphPageLimitExceeded(second_next)

    outcome = services.run_delta_sync(
        config,
        FakeRepository(),
        lifecycle,
        Connector(),
        None,
        None,
        None,
        work_registry=Registry(),
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="delta-owner",
        delta_execution_id="delta-instance",
    )

    assert outcome.continuation_phase == "enumerating"
    assert outcome.continuation_token == second_next
    assert outcome.items_seen == 2
    assert lifecycle.saved_cursors == []


@pytest.mark.parametrize(
    "reduced_items",
    [
        pytest.param((), id="zero-items"),
        pytest.param(({"id": "folder-1", "folder": {}},), id="single-page"),
    ],
)
def test_delta_sync_terminal_enumeration_runs_baseline_and_acl_before_cursor_ready(
    monkeypatch: pytest.MonkeyPatch,
    reduced_items: tuple[dict[str, Any], ...],
) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    next_link = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?skip=200"
    candidate = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"
    events: list[str] = []

    class Capture:
        def resume_state(self):
            return {
                "roundStartKind": "reset",
                "roundStartUrl": next_link,
                "receiptId": "receipt-199",
                "pageOrdinal": 199,
                "requestUrl": next_link,
                "predecessorReceiptId": "receipt-198",
                "nextLink": next_link,
                "deltaLink": None,
            }

        def list_reduced_items_page(self, *, page_size, continuation_token):
            assert page_size == 50
            assert continuation_token is None
            return SimpleNamespace(items=reduced_items, continuation_token=None)

        def has_reduced_item(self, item_id):
            return any(item["id"] == item_id for item in reduced_items)

        def count_observed_entries(self):
            return len(reduced_items)

    capture = Capture()

    class Registry:
        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return capture

        def get_unresolved_document_operation_count(self, _source_id):
            return 0

        def mark_delta_round_cursor_ready(self, *_args):
            events.append("cursor_ready")

    class Connector:
        def read_drive_delta(self, _max_pages, delta_link=None, *, page_capture=None):
            assert delta_link == next_link
            assert page_capture is capture
            return DriveDelta([], candidate, False)

    def list_ready_documents_page(**_kwargs):
        events.append("baseline")
        return SimpleNamespace(items=(), continuation_token=None)

    def run_acl_resync_page(*_args, **_kwargs):
        events.append("acl")
        return SimpleNamespace(updated=0, retired=0), None

    lifecycle.list_ready_documents_page = list_ready_documents_page  # type: ignore[method-assign]
    monkeypatch.setattr(services, "run_acl_resync_page", run_acl_resync_page)

    outcome = services.run_delta_sync(
        config,
        FakeRepository(),
        lifecycle,
        Connector(),
        None,
        None,
        None,
        work_registry=Registry(),
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="delta-owner",
        delta_execution_id="delta-instance",
        processing_phase="enumerating",
        phase_continuation_token=next_link,
        defer_cursor_commit=True,
    )

    assert events == ["baseline", "acl", "cursor_ready"]
    assert outcome.continuation_phase == "commit"
    assert outcome.continuation_token == candidate
    assert lifecycle.saved_cursors == []


def test_delta_sync_returns_after_one_reduced_item_page_without_committing_cursor() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    folder_item = {"id": "folder-1", "folder": {}}

    class Capture:
        def begin_page(self, *_args):
            return {
                "state": "sealed",
                "receiptId": "receipt-1",
                "items": [folder_item],
                "nextLink": None,
                "deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
            }

        def list_reduced_items_page(self, *, page_size, continuation_token):
            assert page_size == 50
            assert continuation_token is None
            return SimpleNamespace(items=(folder_item,), continuation_token="reduced-next")

        def count_observed_entries(self):
            return 3

    class Registry:
        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return Capture()

        def mark_delta_round_cursor_ready(self, *_args):
            pytest.fail("a non-final reduced-item page cannot make the cursor ready")

    def handler(_request: httpx.Request) -> httpx.Response:
        pytest.fail("sealed Graph pages should replay without refetch")

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config,
            FakeRepository(),
            lifecycle,
            SharePointConnector(client, config.drive_id),
            None,
            None,
            None,
            work_registry=Registry(),
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="delta-owner",
            delta_execution_id="delta-instance",
        )

    assert outcome.continuation_token == "reduced-next"
    assert outcome.items_seen == 3
    assert outcome.failed == outcome.pending == 0
    assert lifecycle.saved_cursors == []


@pytest.mark.parametrize(
    ("processing_phase", "cursor", "page_token", "expected_phase"),
    [
        ("baseline", None, "baseline-next", "baseline"),
        ("acl", "https://graph.microsoft.com/v1.0/delta?token=old", "acl-next", "acl"),
    ],
)
def test_delta_sync_pages_baseline_and_acl_phases_before_cursor_commit(
    processing_phase: str,
    cursor: str | None,
    page_token: str,
    expected_phase: str,
) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(cursor=cursor)

    class Capture:
        def begin_page(self, *_args):
            return {
                "state": "sealed", "receiptId": "receipt-1", "items": [],
                "nextLink": None,
                "deltaLink": "https://graph.microsoft.com/v1.0/delta?token=candidate",
            }

        def has_reduced_item(self, _item_id):
            return False

        def count_observed_entries(self):
            return 0

    class Registry:
        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return Capture()

        def get_unresolved_document_operation_count(self, _source_id):
            return 0

    lifecycle.list_ready_documents_page = lambda **_kwargs: SimpleNamespace(  # type: ignore[method-assign]
        items=(), continuation_token=page_token,
    )
    registry = Registry()

    def handler(_request: httpx.Request) -> httpx.Response:
        pytest.fail("sealed delta pages should not refetch Graph")

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config,
            FakeRepository(),
            lifecycle,
            SharePointConnector(client, config.drive_id),
            None,
            None,
            None,
            work_registry=registry,
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="delta-owner",
            delta_execution_id="delta-instance",
            processing_phase=processing_phase,
        )

    assert outcome.continuation_phase == expected_phase
    assert outcome.continuation_token == page_token
    assert lifecycle.saved_cursors == []


def test_full_sync_catchup_defers_cursor_commit_until_parent_authorization() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )

    class Registry:
        round_state: dict[str, Any] = {"state": "active"}

        def begin_delta_round(self, *_args):
            return self.round_state

        def delta_page_capture(self, *_args):
            return None

        def get_unresolved_document_operation_count(self, _source_id):
            return 0

        def mark_delta_round_cursor_ready(self, _source_id, _round_id, _owner, link, outcome):
            self.round_state = {
                "state": "cursor_ready",
                "candidateDeltaLink": link,
                "outcome": outcome,
            }

    registry = Registry()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=candidate",
        })

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config,
            FakeRepository(),
            lifecycle,
            SharePointConnector(client, config.drive_id),
            None,
            None,
            None,
            work_registry=registry,
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="fullsync-owner",
            delta_execution_id="fullsync-round",
            defer_cursor_commit=True,
        )

    assert outcome.continuation_phase == "commit"
    assert outcome.continuation_token == "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=candidate"
    assert registry.round_state["state"] == "cursor_ready"
    assert lifecycle.saved_cursors == []


def test_full_sync_catchup_uses_tokenless_delta_baseline_without_overwriting_old_cursor() -> None:
    config = build_config()
    old_cursor = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old"
    final_link = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=fresh"
    lifecycle = FakeLifecycleRepository(cursor=old_cursor)
    requested_urls: list[str] = []
    round_anchors: list[str | None] = []

    class Registry:
        def begin_delta_round(self, _plan, _owner, _round_id, committed_link):
            round_anchors.append(committed_link)
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return None

        def get_unresolved_document_operation_count(self, _source_id):
            return 0

        def mark_delta_round_cursor_ready(self, *_args):
            return None

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        return httpx.Response(200, json={"value": [], "@odata.deltaLink": final_link})

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config,
            FakeRepository(),
            lifecycle,
            SharePointConnector(client, config.drive_id),
            None,
            None,
            None,
            work_registry=Registry(),
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="fullsync-owner",
            delta_execution_id="fullsync-round",
            defer_cursor_commit=True,
            force_full_baseline=True,
        )

    assert round_anchors == [None]
    assert len(requested_urls) == 1
    assert "token=old" not in requested_urls[0]
    assert outcome.continuation_phase == "commit"
    assert outcome.continuation_token == final_link
    assert lifecycle.get_delta_cursor("source") == old_cursor
    assert lifecycle.saved_cursors == []


@pytest.mark.parametrize(
    (
        "claim_status", "activity_status", "expected_settlement",
        "unresolved_after_settlement", "expected_cursor",
    ),
    [
        ("acquired", ActivityStatus.SUCCEEDED, "completed", 0, True),
        ("acquired", ActivityStatus.SUCCEEDED, "completed", 1, False),
        ("acquired", ActivityStatus.PENDING, "pending", 1, False),
        ("duplicate_active", ActivityStatus.SUCCEEDED, None, 1, False),
        ("draining", ActivityStatus.SUCCEEDED, None, 1, False),
        ("completed", ActivityStatus.SUCCEEDED, None, 0, True),
    ],
)
def test_delta_document_work_claim_gates_mutations_and_cursor(
    monkeypatch: pytest.MonkeyPatch,
    claim_status: str,
    activity_status: ActivityStatus,
    expected_settlement: str | None,
    unresolved_after_settlement: int,
    expected_cursor: bool,
) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    events: list[str] = []

    class Repository(FakeRepository):
        def create_discovered_document(self, document: Any) -> VersionedRecord:
            events.append("create")
            return super().create_discovered_document(document)

        def get_document(self, _source_run_id: str, document_id: str):
            document = next((item for item in self.created if item.document_id == document_id), None)
            if document is None:
                return None
            return VersionedRecord(replace(
                document, status=DocumentStatus.READY, stage=services.DocumentStage.TERMINAL,
            ), "ready-etag")

    class WorkRegistry:
        def __init__(self) -> None:
            self.unresolved_count = 1 if claim_status != "completed" else 0

        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return None

        def mark_delta_round_cursor_ready(self, *_args):
            return None

        def complete_delta_round(self, *_args):
            return {"state": "committed"}

        def begin_document_operation(self, _plan, **kwargs):
            events.append("claim")
            assert kwargs == {
                "owner_instance_id": "delta-owner",
                "physical_run_id": "source:delta-delta-instance",
                "item_id": create_document_id("source", "drive", "item-1"),
                "operation_kind": "delta-sync",
            }
            if claim_status == "acquired":
                return {"status": claim_status, "operationId": "operation-1"}
            if claim_status == "completed":
                return {
                    "status": "completed",
                    "outcome": {"documentId": "doc", "status": "succeeded"},
                }
            return {"status": claim_status}

        def start_document_operation(self, *_args, **kwargs):
            events.append("start")
            assert kwargs == {}
            return {"status": "started"}

        def settle_document_operation(self, *_args, **kwargs):
            events.append(f"settle:{kwargs['status']}")
            self.unresolved_count = (
                unresolved_after_settlement
                if kwargs["status"] == "succeeded"
                else 1
            )

        def record_document_winner(self, *_args, **_kwargs):
            events.append("winner")

        def get_unresolved_document_operation_count(self, _source_id):
            return self.unresolved_count

    repository = Repository()
    monkeypatch.setattr(
        services,
        "process_document",
        lambda *args, **kwargs: (
            events.append("process")
            or _assert_delta_work_context(kwargs)
            or ActivityOutcome(
                document_id="doc", status=activity_status,
                chunks_written=3 if activity_status is ActivityStatus.SUCCEEDED else 0,
                retry_count=0,
            )
        ),
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("item-1")],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, repository, lifecycle, SharePointConnector(client, config.drive_id),
            None, None, None,
            work_registry=WorkRegistry(),
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="delta-owner",
            delta_execution_id="delta-instance",
        )

    if claim_status == "acquired":
        assert events[:3] == ["claim", "start", "create"]
        assert "process" in events
        expected_operation_settlement = {
            "completed": "succeeded",
            "pending": "unresolved",
        }[expected_settlement]
        assert f"settle:{expected_operation_settlement}" in events
    elif claim_status == "completed":
        assert events == ["claim"]
        assert repository.created == []
        assert outcome.created_or_updated == 1
    else:
        assert events == ["claim"]
        assert outcome.pending == 1
        assert repository.created == []
    assert bool(lifecycle.saved_cursors) is expected_cursor
    if not expected_cursor:
        assert lifecycle.saved_cursors == []


@pytest.mark.parametrize("delete_succeeds", [True, False])
def test_delta_deletion_work_settles_only_after_all_version_cleanup(
    delete_succeeds: bool,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    ready = ReadyDocumentRef(
        document_id, "source:run-old", f"source:run-old:{document_id}",
        "item-1", ("group-a",), "hash-a", "etag-a", source_etag="etag-old",
        source_id="source", drive_id="drive",
    )
    events: list[str] = []

    class Lifecycle(FakeLifecycleRepository):
        def delete_document_and_chunks(self, **kwargs: Any) -> None:
            events.append("delete")
            if not delete_succeeds:
                raise LifecycleConflictError("delete conflict")
            super().delete_document_and_chunks(**kwargs)

    class WorkRegistry:
        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return None

        def mark_delta_round_cursor_ready(self, *_args):
            return None

        def complete_delta_round(self, *_args):
            return {"state": "committed"}

        def begin_document_operation(self, _plan, **kwargs):
            events.append("claim")
            assert kwargs["operation_kind"] == "delta-delete"
            assert kwargs["item_id"] == document_id
            return {"status": "acquired", "operationId": "delete-operation"}

        def start_document_operation(self, *_args, **_kwargs):
            events.append("start")
            return {"status": "started"}

        def settle_document_operation(self, *_args, **kwargs):
            events.append(f"settle:{kwargs['status']}")

        def get_unresolved_document_operation_count(self, _source_id):
            return 0 if "settle:succeeded" in events else 1

    lifecycle = Lifecycle(cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old")
    lifecycle.versions_by_document_id[document_id] = [ready]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("item-1", deleted=True)],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, FakeRepository(), lifecycle, SharePointConnector(client, config.drive_id),
            None, None, None, work_registry=WorkRegistry(),
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="delta-owner", delta_execution_id="delta-instance",
        )

    assert events[:3] == ["claim", "start", "delete"]
    assert lifecycle.saved_cursors == (
        ["https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"]
        if delete_succeeds else []
    )
    assert ("settle:succeeded" in events) is delete_succeeds
    assert outcome.failed == (0 if delete_succeeds else 1)


def test_delta_all_version_cleanup_partial_failure_keeps_operation_and_cursor() -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    versions = [
        ReadyDocumentRef(
            document_id, f"source:run-{suffix}", f"source:run-{suffix}:{document_id}",
            "item-1", ("group-a",), f"hash-{suffix}", f"etag-{suffix}",
            source_id="source", drive_id="drive",
        )
        for suffix in ("a", "b")
    ]
    events: list[str] = []

    class Lifecycle(FakeLifecycleRepository):
        def delete_document_and_chunks(self, **kwargs: Any) -> None:
            events.append(kwargs["source_run_id"])
            if kwargs["source_run_id"] == "source:run-b":
                raise LifecycleConflictError("second version changed")
            super().delete_document_and_chunks(**kwargs)

    lifecycle = Lifecycle(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    lifecycle.versions_by_document_id[document_id] = versions

    class Registry:
        settlements: list[str] = []

        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return None

        def begin_document_operation(self, *_args, **_kwargs):
            return {"status": "acquired", "operationId": "delete-operation"}

        def start_document_operation(self, *_args, **_kwargs):
            return {"status": "started"}

        def settle_document_operation(self, *_args, **kwargs):
            self.settlements.append(kwargs["status"])

        def get_unresolved_document_operation_count(self, _source_id):
            return 1 if not self.settlements else 0

    registry = Registry()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("item-1", deleted=True)],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config,
            FakeRepository(),
            lifecycle,
            SharePointConnector(client, config.drive_id),
            None,
            None,
            None,
            work_registry=registry,
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="delta-owner",
            delta_execution_id="delta-instance",
        )

    assert events == ["source:run-a", "source:run-b"]
    assert registry.settlements == []
    assert outcome.failed == 1
    assert lifecycle.saved_cursors == []


def test_full_sync_absence_cleanup_pages_and_claims_before_delete() -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "old-item")
    ref = ReadyDocumentRef(
        document_id,
        "source:run-legacy",
        "source:run-legacy:" + document_id,
        "old-item",
        ("group-a",),
        "acl-hash",
        "doc-etag",
        source_id="source",
        drive_id="drive",
    )
    lifecycle = FakeLifecycleRepository()
    lifecycle.list_ready_documents_page = lambda **_kwargs: SimpleNamespace(  # type: ignore[method-assign]
        items=(ref,), continuation_token="next-token",
    )
    lifecycle.versions_by_document_id[document_id] = [ref]
    events: list[str] = []

    class Registry:
        def begin_document_operation(self, _plan, **kwargs):
            assert kwargs["item_id"] == document_id
            assert kwargs["operation_kind"] == "delta-delete"
            events.append("claim")
            return {"status": "acquired", "operationId": "operation-1"}

        def start_document_operation(self, *args, **_kwargs):
            assert args[4] == document_id
            events.append("start")
            return {"status": "started"}

        def settle_document_operation(self, *_args, **kwargs):
            assert kwargs["item_id"] == document_id
            assert kwargs["operation_kind"] == "delta-delete"
            events.append("settle")

    class Connector:
        def read_item(self, item_id):
            assert item_id == "old-item"
            events.append("verify-source-absent")
            return None

    outcome, token = services.run_full_sync_absence_reconciliation_page(
        config,
        lifecycle,
        Connector(),
        current_source_run_id="source:run-current",
        page_size=1,
        continuation_token=None,
        work_registry=Registry(),
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="owner-1",
    )

    assert events == ["verify-source-absent", "claim", "start", "settle"]
    assert outcome.repaired == 1
    assert lifecycle.deleted[0]["source_run_id"] == ref.source_run_id
    assert token == "next-token"


def _assert_delta_work_context(kwargs: dict[str, Any]) -> None:
    assert kwargs["document_work_id"] == "operation-1"
    assert kwargs["document_work_attempt_id"] == "operation-1"


@pytest.mark.parametrize("retryable", [True, False])
def test_run_delta_sync_retains_cursor_when_any_item_fails(
    monkeypatch: pytest.MonkeyPatch,
    retryable: bool,
) -> None:
    config = build_config(allowed_extensions=(".pdf", ".wav"))
    old_cursor = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old"
    lifecycle = FakeLifecycleRepository(cursor=old_cursor)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "value": [_delta_pdf_item("item-1")],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
            },
        )

    monkeypatch.setattr(
        services, "process_document",
        lambda *a, **k: ActivityOutcome(
            document_id="doc", status=ActivityStatus.FAILED, chunks_written=0, retry_count=0,
            error=services.SafeError("processing_failed", "embedding", retryable),
        ),
    )
    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, FakeRepository(), lifecycle,
            SharePointConnector(client, config.drive_id), None, None, None,
        )

    assert outcome.failed == 1
    assert lifecycle.saved_cursors == []
    assert lifecycle.get_delta_cursor(config.source_id) == old_cursor


@pytest.mark.parametrize("prior_version_count", [0, 1, 2])
def test_terminal_delta_quarantines_replacement_and_retires_prior_versions(
    monkeypatch: pytest.MonkeyPatch,
    prior_version_count: int,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    prior_versions = [
        ReadyDocumentRef(
            document_id=document_id,
            source_run_id=f"source:prior-{index}",
            document_key=f"source:prior-{index}:{document_id}",
            item_id="item-1",
            allowed_group_ids=("group-a",),
            acl_hash="acl-hash",
            etag=f"prior-etag-{index}",
            source_etag="prior-source-etag",
            source_id="source",
            drive_id="drive",
        )
        for index in range(prior_version_count)
    ]
    lifecycle.versions_by_document_id[document_id] = prior_versions
    winner = None
    if prior_versions:
        first = prior_versions[0]
        winner = {
            "sourceRunId": first.source_run_id,
            "documentKey": first.document_key,
            "sourceEtag": first.source_etag,
            "processingProfileDigest": services._processing_profile_digest(config),
        }
    registry = DeltaQuarantineRegistry(winner)
    audit_container = MagicMock()

    class Repository(FakeRepository):
        failed_replacement: VersionedRecord | None = None

        def get_document(
            self, _source_run_id: str, _document_id: str
        ) -> VersionedRecord | None:
            return self.failed_replacement

    repository = Repository()

    def quarantine_document(
        _config: IngestionConfig,
        document: SourceDocumentRecord,
        *_args: Any,
        **_kwargs: Any,
    ) -> ActivityOutcome:
        error = services.SafeError("unsupported_source_format", "terminal", False)
        repository.failed_replacement = VersionedRecord(
            replace(
                document,
                status=DocumentStatus.FAILED,
                stage=services.DocumentStage.TERMINAL,
                error=error,
            ),
            "failed-etag",
        )
        return ActivityOutcome(
            document_id=document.document_id,
            status=ActivityStatus.QUARANTINED,
            chunks_written=0,
            retry_count=0,
            error=error,
        )

    monkeypatch.setattr(services, "process_document", quarantine_document)

    class Connector:
        def read_drive_delta(self, *_args: Any, **_kwargs: Any) -> DriveDelta:
            return DriveDelta(
                [_delta_pdf_item("item-1")],
                "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
                False,
            )

    outcome = services.run_delta_sync(
        config,
        repository,
        lifecycle,
        Connector(),
        None,
        None,
        None,
        audit_container=audit_container,
        work_registry=registry,
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="delta-owner",
        delta_execution_id="delta-instance",
    )

    assert outcome.quarantined == 1
    assert outcome.failed == 0
    assert [item["reason"] for item in lifecycle.retired] == [
        "replacement_failed"
    ] * prior_version_count
    assert len(registry.cleared_winners) == min(prior_version_count, 1)
    assert registry.settlements[-1]["status"] == "quarantined"
    assert registry.settlements[-1]["outcome"]["status"] == "quarantined"
    assert registry.cursor_outcomes[-1]["quarantined"] == 1
    assert lifecycle.saved_cursors == [
        "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"
    ]
    audit_item = audit_container.create_item.call_args[0][0]
    assert audit_item["operation"] == "document_quarantined"
    assert audit_item["errorCode"] == "unsupported_source_format"
    assert "content" not in audit_item


@pytest.mark.parametrize("failure_mode", ["retirement", "identity", "winner"])
def test_terminal_delta_keeps_cursor_when_quarantine_settlement_fails(
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    previous = ReadyDocumentRef(
        document_id=document_id,
        source_run_id="source:prior",
        document_key=f"source:prior:{document_id}",
        item_id="item-1",
        allowed_group_ids=("group-a",),
        acl_hash="acl-hash",
        etag="prior-etag",
        source_etag="prior-source-etag",
        source_id="source",
        drive_id="other-drive" if failure_mode == "identity" else "drive",
    )
    lifecycle.versions_by_document_id[document_id] = [previous]
    if failure_mode == "retirement":
        def fail_retirement(**_kwargs: Any) -> None:
            raise LifecycleConflictError("etag_conflict")

        monkeypatch.setattr(
            lifecycle,
            "retire_document",
            fail_retirement,
        )
    winner = None
    if failure_mode == "winner":
        winner = {
            "sourceRunId": previous.source_run_id,
            "documentKey": previous.document_key,
            "sourceEtag": previous.source_etag,
            "processingProfileDigest": services._processing_profile_digest(config),
        }
    registry = DeltaQuarantineRegistry(winner)
    if failure_mode == "winner":
        monkeypatch.setattr(
            registry,
            "clear_document_winner",
            lambda *_args, **_kwargs: False,
        )

    class Repository(FakeRepository):
        failed_replacement: VersionedRecord | None = None

        def get_document(
            self, _source_run_id: str, _document_id: str
        ) -> VersionedRecord | None:
            return self.failed_replacement

    repository = Repository()

    def quarantine_document(
        _config: IngestionConfig,
        document: SourceDocumentRecord,
        *_args: Any,
        **_kwargs: Any,
    ) -> ActivityOutcome:
        error = services.SafeError("unsupported_source_format", "terminal", False)
        repository.failed_replacement = VersionedRecord(
            replace(
                document,
                status=DocumentStatus.FAILED,
                stage=services.DocumentStage.TERMINAL,
                error=error,
            ),
            "failed-etag",
        )
        return ActivityOutcome(
            document_id=document.document_id,
            status=ActivityStatus.QUARANTINED,
            chunks_written=0,
            retry_count=0,
            error=error,
        )

    monkeypatch.setattr(services, "process_document", quarantine_document)

    class Connector:
        def read_drive_delta(self, *_args: Any, **_kwargs: Any) -> DriveDelta:
            return DriveDelta(
                [_delta_pdf_item("item-1")],
                "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
                False,
            )

    outcome = services.run_delta_sync(
        config,
        repository,
        lifecycle,
        Connector(),
        None,
        None,
        None,
        work_registry=registry,
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="delta-owner",
        delta_execution_id="delta-instance",
    )

    assert outcome.quarantined == 0
    assert outcome.failed == 1
    assert registry.settlements[-1]["status"] == "failed"
    assert lifecycle.saved_cursors == []


def test_terminal_delta_replays_quarantine_without_reprocessing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    registry = DeltaQuarantineRegistry({
        "sourceRunId": "source:prior",
        "documentKey": "source:prior:document",
        "sourceEtag": "different-etag",
        "processingProfileDigest": services._processing_profile_digest(config),
    }, replay_outcome={
        "documentId": create_document_id("source", "drive", "item-1"),
        "status": "quarantined",
    })
    monkeypatch.setattr(
        services,
        "process_document",
        lambda *_args, **_kwargs: pytest.fail(
            "completed quarantine must not be reprocessed"
        ),
    )

    class Connector:
        def read_drive_delta(self, *_args: Any, **_kwargs: Any) -> DriveDelta:
            return DriveDelta(
                [_delta_pdf_item("item-1")],
                "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
                False,
            )

    outcome = services.run_delta_sync(
        config,
        FakeRepository(),
        lifecycle,
        Connector(),
        None,
        None,
        None,
        work_registry=registry,
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="delta-owner",
        delta_execution_id="delta-instance",
    )

    assert outcome.quarantined == 1
    assert outcome.failed == 0
    assert lifecycle.saved_cursors == [
        "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"
    ]


def test_run_delta_sync_keeps_cursor_when_audio_transcription_is_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config(allowed_extensions=(".pdf", ".wav"))
    old_cursor = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old"
    lifecycle = FakeLifecycleRepository(cursor=old_cursor)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("audio-item", name="recording.wav", mime_type="audio/wav")],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    monkeypatch.setattr(
        services,
        "process_document",
        lambda *a, **k: ActivityOutcome(
            document_id="audio-doc", status=ActivityStatus.PENDING,
            chunks_written=0, retry_count=0,
        ),
    )
    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, FakeRepository(), lifecycle,
            SharePointConnector(client, config.drive_id), None, None, None,
        )

    assert outcome.pending == 1
    assert outcome.failed == 0
    assert lifecycle.saved_cursors == []
    assert lifecycle.get_delta_cursor(config.source_id) == old_cursor


def test_duplicate_delta_delivery_does_not_resubmit_pending_audio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config(allowed_extensions=(".pdf", ".wav"))
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    lifecycle.transcribing_by_document_id[create_document_id("source", "drive", "audio-item")] = {
        "sourceId": "source", "driveId": "drive", "eTag": "etag-x",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("audio-item", name="recording.wav", mime_type="audio/wav")],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    monkeypatch.setattr(
        services, "process_document",
        lambda *_a, **_k: pytest.fail("pending audio must not be submitted twice"),
    )
    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, FakeRepository(), lifecycle,
            SharePointConnector(client, config.drive_id), None, None, None,
        )

    assert outcome.pending == 1
    assert lifecycle.saved_cursors == []


def test_duplicate_delta_delivery_reuses_same_ready_source_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    lifecycle.ready_by_document_id[document_id] = ReadyDocumentRef(
        document_id=document_id, source_run_id="source:run-old",
        document_key="source:run-old:" + document_id, item_id="item-1",
        allowed_group_ids=("group-a",), acl_hash="hash-a", etag="doc-etag",
        source_etag="etag-x", source_id="source", drive_id="drive",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("item-1")],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    monkeypatch.setattr(
        services, "process_document",
        lambda *_a, **_k: pytest.fail("unchanged ready content must be reused"),
    )
    repository = FakeRepository()
    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, repository, lifecycle,
            SharePointConnector(client, config.drive_id), None, None, None,
        )

    assert outcome.created_or_updated == 0
    assert repository.created == []
    assert lifecycle.saved_cursors == ["https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"]


def test_delta_reuse_reverifies_acl_and_source_before_reusing_winner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    profile_digest = services._processing_profile_digest(config)
    ready_ref = ReadyDocumentRef(
        document_id=document_id,
        source_run_id="source:run-old",
        document_key="source:run-old:" + document_id,
        item_id="item-1",
        allowed_group_ids=("group-a",),
        acl_hash="acl-hash",
        etag="document-etag",
        source_etag="etag-x",
        source_id="source",
        drive_id="drive",
        processing_profile_digest=profile_digest,
    )
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    lifecycle.ready_by_document_id[document_id] = ready_ref
    lifecycle.versions_by_document_id[document_id] = [ready_ref]
    events: list[str] = []

    class Registry:
        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return None

        def get_document_winner(self, _source_id, _document_id):
            return {
                "recordType": "document-winner",
                "schemaVersion": 1,
                "sourceId": "source",
                "documentId": document_id,
                "driveId": "drive",
                "itemId": "item-1",
                "sourceRunId": ready_ref.source_run_id,
                "documentKey": ready_ref.document_key,
                "sourceEtag": "etag-x",
                "lifecycleGeneration": 0,
                "processingProfileDigest": profile_digest,
            }

        def begin_document_operation(self, _plan, **kwargs):
            events.append(f"claim:{kwargs['operation_kind']}")
            return {"status": "acquired", "operationId": "acl-operation"}

        def start_document_operation(self, *_args, **_kwargs):
            events.append("start")
            return {"status": "started"}

        def settle_document_operation(self, *_args, **kwargs):
            events.append(f"settle:{kwargs['status']}")

        def record_document_winner(self, *_args, **_kwargs):
            events.append("publish-winner")

        def get_unresolved_document_operation_count(self, _source_id):
            return 0

        def mark_delta_round_cursor_ready(self, *_args):
            return None

        def complete_delta_round(self, *_args):
            return {"state": "committed"}

    class Connector:
        def read_drive_delta(self, *_args, **_kwargs):
            return DriveDelta(
                [_delta_pdf_item("item-1")],
                "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
                False,
            )

        def read_item(self, item_id):
            events.append("verify-source")
            return {"id": item_id, "eTag": "etag-x"}

        def read_verified_acl(self, item_id, _max_pages):
            events.append("verify-acl")
            return VerifiedAcl(("group-a",), "acl-hash")

    monkeypatch.setattr(
        services,
        "process_document",
        lambda *_args, **_kwargs: pytest.fail("verified winner should be reused"),
    )
    outcome = services.run_delta_sync(
        config,
        FakeRepository(),
        lifecycle,
        Connector(),
        None,
        None,
        None,
        work_registry=Registry(),
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="delta-owner",
        delta_execution_id="delta-instance",
    )

    assert outcome.created_or_updated == 0
    assert events == [
        "claim:delta-acl", "start", "verify-source", "verify-acl",
        "verify-source", "publish-winner", "settle:succeeded",
    ]
    assert lifecycle.saved_cursors == ["https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"]


def test_cursorless_delta_reprocesses_legacy_ready_item_without_winner_pointer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    lifecycle = FakeLifecycleRepository()
    previous = ReadyDocumentRef(
        document_id=document_id,
        source_run_id="source:run-legacy",
        document_key="source:run-legacy:" + document_id,
        item_id="item-1",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
        etag="legacy-etag",
        source_etag="etag-x",
        source_id="source",
        drive_id="drive",
    )
    lifecycle.ready_by_document_id[document_id] = previous
    events: list[str] = []

    class Registry:
        unresolved = 0

        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return None

        def get_document_winner(self, *_args):
            return None

        def begin_document_operation(self, *_args, **_kwargs):
            self.unresolved += 1
            return {"status": "acquired", "operationId": "operation-1"}

        def start_document_operation(self, *_args, **_kwargs):
            return {"status": "started"}

        def record_document_winner(self, *_args, **_kwargs):
            events.append("winner")

        def settle_document_operation(self, *_args, **_kwargs):
            events.append("settle")
            self.unresolved = 0

        def get_unresolved_document_operation_count(self, _source_id):
            return self.unresolved

        def mark_delta_round_cursor_ready(self, *_args):
            return None

        def complete_delta_round(self, *_args):
            return {"state": "committed"}

    registry = Registry()

    class Repository(FakeRepository):
        def get_document(self, source_run_id: str, requested_id: str):
            document = next(item for item in self.created if item.document_id == requested_id)
            return VersionedRecord(replace(
                document,
                status=DocumentStatus.READY,
                stage=services.DocumentStage.TERMINAL,
            ), "ready-etag")

    repository = Repository()
    monkeypatch.setattr(
        services,
        "process_document",
        lambda *_args, **_kwargs: ActivityOutcome(
            document_id=document_id,
            status=ActivityStatus.SUCCEEDED,
            chunks_written=1,
            retry_count=0,
        ),
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("item-1")],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config,
            repository,
            lifecycle,
            SharePointConnector(client, config.drive_id),
            None,
            None,
            None,
            work_registry=registry,
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="delta-owner",
            delta_execution_id="delta-instance",
        )

    assert outcome.created_or_updated == 1
    assert [item.document_id for item in repository.created] == [document_id]
    assert lifecycle.deleted[0]["source_run_id"] == previous.source_run_id
    assert events == ["winner", "settle"]
    assert lifecycle.saved_cursors == ["https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"]


def test_run_delta_sync_deletes_superseded_version_after_successful_update(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    prev_ref = ReadyDocumentRef(
        document_id=document_id,
        source_run_id=create_source_run_id("source", "run-old"),
        document_key=create_document_key("source", "run-old", document_id),
        item_id="item-1",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
        etag="etag-old",
    )
    lifecycle = FakeLifecycleRepository(cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old")
    lifecycle.ready_by_document_id[document_id] = prev_ref
    repository = FakeRepository()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"value": [_delta_pdf_item("item-1")], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"},
        )

    monkeypatch.setattr(
        services, "process_document",
        lambda *a, **k: ActivityOutcome(document_id="doc", status=ActivityStatus.SUCCEEDED, chunks_written=3, retry_count=0),
    )

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, repository, lifecycle, connector, None, None, None)

    assert outcome.created_or_updated == 1
    assert lifecycle.retired == []
    assert len(lifecycle.deleted) == 1
    assert lifecycle.deleted[0]["source_run_id"] == prev_ref.source_run_id
    assert lifecycle.deleted[0]["document_key"] == prev_ref.document_key
    assert lifecycle.deleted[0]["etag"] == prev_ref.etag


def test_run_delta_sync_deletes_document_and_chunks_on_graph_deletion() -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    ref = ReadyDocumentRef(
        document_id=document_id, source_run_id="source:run-a", document_key="source:run-a:" + document_id,
        item_id="item-1", allowed_group_ids=("group-a",), acl_hash="hash-a", etag="etag-a",
        source_etag="etag-x", source_id="source", drive_id="drive",
    )
    lifecycle = FakeLifecycleRepository(cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old")
    lifecycle.ready_by_document_id[document_id] = ref

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"value": [_delta_pdf_item("item-1", deleted=True)], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"},
        )

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, FakeRepository(), lifecycle, connector, None, None, None)

    assert outcome.deleted == 1
    assert lifecycle.retired == []
    assert lifecycle.deleted == [{
        "source_run_id": "source:run-a", "document_id": document_id,
        "document_key": ref.document_key, "etag": ref.etag,
    }]


def test_supported_file_renamed_to_unsupported_type_is_removed_from_index() -> None:
    config = build_config(allowed_extensions=(".pdf",))
    document_id = create_document_id("source", "drive", "item-1")
    previous = ReadyDocumentRef(
        document_id, "source:run-a", "source:run-a:" + document_id,
        "item-1", ("group-a",), "hash-a", "etag-a", source_etag="etag-old",
        source_id="source", drive_id="drive",
    )
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    lifecycle.versions_by_document_id[document_id] = [previous]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("item-1", name="renamed.txt")],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, FakeRepository(), lifecycle,
            SharePointConnector(client, config.drive_id), None, None, None,
        )

    assert outcome.deleted == 1
    assert lifecycle.deleted[0]["document_key"] == previous.document_key
    assert lifecycle.saved_cursors == ["https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"]


def test_graph_deletion_removes_every_source_version() -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    first = ReadyDocumentRef(
        document_id, "source:run-a", "source:run-a:" + document_id, "item-1",
        ("group-a",), "hash-a", "etag-a", source_etag="old-etag",
        source_id="source", drive_id="drive",
    )
    second = ReadyDocumentRef(
        document_id, "source:run-b", "source:run-b:" + document_id, "item-1",
        ("group-a",), "hash-b", "etag-b", source_etag="new-etag",
        source_id="source", drive_id="drive",
    )
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    lifecycle.versions_by_document_id[document_id] = [first, second]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [_delta_pdf_item("item-1", deleted=True)],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, FakeRepository(), lifecycle,
            SharePointConnector(client, config.drive_id), None, None, None,
        )

    assert outcome.deleted == 1
    assert {item["source_run_id"] for item in lifecycle.deleted} == {
        "source:run-a", "source:run-b",
    }


def test_rejected_cursor_reconciles_absent_source_items_before_commit() -> None:
    config = build_config()
    old_cursor = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=expired"
    reset_url = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?reset=1"
    fresh_link = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=fresh"
    document_id = create_document_id("source", "drive", "item-old")
    old = ReadyDocumentRef(
        document_id, "source:run-old", "source:run-old:" + document_id, "item-old",
        ("group-a",), "hash-old", "etag-old", source_etag="etag-source",
    )
    lifecycle = FakeLifecycleRepository(cursor=old_cursor)
    lifecycle.versions_by_document_id[document_id] = [old]
    lifecycle.ready_inventory_pages = [[old], []]

    def handler(request: httpx.Request) -> httpx.Response:
        if "token=expired" in str(request.url):
            return httpx.Response(410, headers={"location": reset_url})
        if str(request.url) == reset_url:
            return httpx.Response(200, json={"value": [], "@odata.deltaLink": fresh_link})
        if request.url.path.endswith("/items/item-old"):
            return httpx.Response(404)
        raise AssertionError(f"unexpected request: {request.url}")

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, FakeRepository(), lifecycle,
            SharePointConnector(client, config.drive_id), None, None, None,
        )

    assert outcome.bootstrapped is True
    assert outcome.deleted == 1
    assert lifecycle.deleted[0]["source_run_id"] == old.source_run_id
    assert lifecycle.saved_cursors == [fresh_link]


def test_delta_reset_follows_graph_location_before_absence_reconciliation() -> None:
    config = build_config()
    old_cursor = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=expired"
    reset_location = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?reset=opaque"
    fresh_link = "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=fresh"
    document_id = create_document_id("source", "drive", "item-old")
    old = ReadyDocumentRef(
        document_id, "source:run-old", "source:run-old:" + document_id, "item-old",
        ("group-a",), "hash-old", "etag-old", source_etag="etag-source",
    )
    lifecycle = FakeLifecycleRepository(cursor=old_cursor)
    lifecycle.versions_by_document_id[document_id] = [old]
    lifecycle.ready_inventory_pages = [[old], []]

    class ResetConnector:
        def __init__(self) -> None:
            self.delta_links: list[str | None] = []

        def read_drive_delta(self, _max_pages: int, delta_link: str | None = None) -> DriveDelta:
            self.delta_links.append(delta_link)
            if delta_link == old_cursor:
                raise DeltaResetRequired(reset_location)
            assert delta_link == reset_location
            return DriveDelta([], fresh_link, False)

        def read_item(self, _item_id: str) -> None:
            return None

    connector = ResetConnector()
    outcome = services.run_delta_sync(
        config, FakeRepository(), lifecycle, connector, None, None, None,
    )

    assert connector.delta_links == [old_cursor, reset_location]
    assert outcome.bootstrapped is True
    assert outcome.deleted == 1
    assert lifecycle.saved_cursors == [fresh_link]


def test_run_delta_sync_deletion_of_untracked_item_is_a_no_op() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"value": [_delta_pdf_item("item-1", deleted=True)], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"},
        )

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, FakeRepository(), lifecycle, connector, None, None, None)

    assert outcome.deleted == 0
    assert lifecycle.deleted == []


def test_run_delta_sync_routes_permission_change_to_acl_resync() -> None:
    """Items annotated with @microsoft.graph.sharedChanged trigger ACL resync, not full reprocess."""
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    ref = ReadyDocumentRef(
        document_id=document_id, source_run_id="source:run-a", document_key="source:run-a:" + document_id,
        item_id="item-1", allowed_group_ids=("group-a",), acl_hash="hash-a", etag="etag-a",
        source_etag="etag-x",
    )
    lifecycle = FakeLifecycleRepository(cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old")
    lifecycle.ready_by_document_id[document_id] = ref

    # Item with permission change annotation (not a content change, not deleted)
    permission_item = {
        "id": "item-1", "name": "a.pdf", "eTag": "etag-x", "size": 100,
        "file": {}, "parentReference": {"id": "parent", "path": "/drive/root:"},
        "@microsoft.graph.sharedChanged": True,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"value": [permission_item], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"},
        )

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, FakeRepository(), lifecycle, connector, None, None, None)

    assert outcome.acl_resynced == 1
    assert outcome.created_or_updated == 0
    # Verify the ACL was actually refreshed (FakeConnector returns same hash → "unchanged")
    # The important thing is that it DIDN'T trigger full process_document


@pytest.mark.parametrize("acl_succeeds", [True, False])
def test_delta_acl_change_uses_operation_and_settles_only_after_resync(
    monkeypatch: pytest.MonkeyPatch,
    acl_succeeds: bool,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    ref = ReadyDocumentRef(
        document_id, "source:run-old", f"source:run-old:{document_id}",
        "item-1", ("group-a",), "hash-a", "etag-a", source_etag="same-etag",
    )
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old",
    )
    lifecycle.ready_by_document_id[document_id] = ref
    events: list[str] = []

    class WorkRegistry:
        def begin_delta_round(self, *_args):
            return {"state": "active"}

        def delta_page_capture(self, *_args):
            return None

        def mark_delta_round_cursor_ready(self, *_args):
            return None

        def complete_delta_round(self, *_args):
            return {"state": "committed"}

        def begin_document_operation(self, _plan, **kwargs):
            events.append("claim")
            assert kwargs["operation_kind"] == "delta-acl"
            return {"status": "acquired", "operationId": "acl-operation"}

        def start_document_operation(self, *_args, **_kwargs):
            events.append("start")
            return {"status": "started"}

        def settle_document_operation(self, *_args, **kwargs):
            events.append(f"settle:{kwargs['status']}")

        def get_unresolved_document_operation_count(self, _source_id):
            return 0 if "settle:succeeded" in events else 1

    item = _delta_pdf_item("item-1")
    item["@microsoft.graph.sharedChanged"] = True
    ref = replace(ref, source_etag=item["eTag"])
    lifecycle.ready_by_document_id[document_id] = ref
    monkeypatch.setattr(
        services,
        "resync_document_acl",
        lambda *_args, **_kwargs: (
            events.append("acl")
            or ("updated" if acl_succeeds else (_ for _ in ()).throw(LifecycleConflictError("acl conflict")))
        ),
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [item],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config, FakeRepository(), lifecycle, SharePointConnector(client, config.drive_id),
            None, None, None,
            work_registry=WorkRegistry(),
            source_plan=SimpleNamespace(source_id="source"),
            owner_instance_id="delta-owner", delta_execution_id="delta-acl-run",
        )

    assert events[:3] == ["claim", "start", "acl"]
    assert ("settle:succeeded" in events) is acl_succeeds
    assert bool(lifecycle.saved_cursors) is acl_succeeds
    assert outcome.failed == (0 if acl_succeeds else 1)


def test_run_delta_sync_processes_combined_permission_and_content_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    ref = ReadyDocumentRef(
        document_id=document_id,
        source_run_id="source:run-a",
        document_key="source:run-a:" + document_id,
        item_id="item-1",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
        etag="etag-a",
        source_etag="etag-old",
    )
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old"
    )
    lifecycle.ready_by_document_id[document_id] = ref
    item = _delta_pdf_item("item-1")
    item["@microsoft.graph.sharedChanged"] = True
    repository = FakeRepository()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "value": [item],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
            },
        )

    monkeypatch.setattr(
        services,
        "process_document",
        lambda *a, **k: ActivityOutcome(
            document_id="doc", status=ActivityStatus.SUCCEEDED, chunks_written=1, retry_count=0,
        ),
    )
    monkeypatch.setattr(services, "resync_document_acl", lambda *a, **k: "unchanged")
    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config,
            repository,
            lifecycle,
            SharePointConnector(client, config.drive_id),
            None,
            None,
            None,
        )

    assert outcome.acl_resynced == 1
    assert outcome.created_or_updated == 1
    assert len(repository.created) == 1


def test_run_delta_sync_reprocesses_changed_acl_revoked_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    document_id = create_document_id("source", "drive", "item-1")
    lifecycle = FakeLifecycleRepository(
        cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old"
    )
    lifecycle.acl_revoked_by_document_id[document_id] = ReadyDocumentRef(
        document_id=document_id,
        source_run_id="source:run-old",
        document_key="source:run-old:" + document_id,
        item_id="item-1",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
        etag="etag-a",
        source_etag="etag-old",
        status=DocumentStatus.RETIRED,
    )
    item = _delta_pdf_item("item-1")
    item["@microsoft.graph.sharedChanged"] = True
    repository = FakeRepository()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [item],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new",
        })

    monkeypatch.setattr(services, "resync_document_acl", lambda *a, **k: "unchanged")
    monkeypatch.setattr(
        services,
        "process_document",
        lambda *a, **k: ActivityOutcome(
            document_id=document_id,
            status=ActivityStatus.SUCCEEDED,
            chunks_written=1,
            retry_count=0,
        ),
    )

    with _mock_connector(handler) as client:
        outcome = services.run_delta_sync(
            config,
            repository,
            lifecycle,
            SharePointConnector(client, config.drive_id),
            None,
            None,
            None,
        )

    assert outcome.acl_resynced == 1
    assert outcome.created_or_updated == 1
    assert len(repository.created) == 1


def test_run_delta_sync_permission_change_on_untracked_item_is_noop() -> None:
    """Permission change on a document not yet ingested should be skipped."""
    config = build_config()
    lifecycle = FakeLifecycleRepository(cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old")

    permission_item = {
        "id": "item-unknown", "name": "b.pdf", "eTag": "etag-y", "size": 50,
        "file": {}, "parentReference": {"id": "parent", "path": "/drive/root:"},
        "@microsoft.graph.sharedChanged": True,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"value": [permission_item], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"},
        )

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, FakeRepository(), lifecycle, connector, None, None, None)

    assert outcome.acl_resynced == 0
    assert outcome.failed == 0


def test_run_delta_sync_skips_folders_and_disallowed_extensions(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(cursor="https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=old")
    folder_item = {"id": "folder-1", "name": "folder", "folder": {}}
    other_ext_item = _delta_pdf_item("item-2", name="notes.txt")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"value": [folder_item, other_ext_item], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=new"},
        )

    called = False

    def _unexpected_process_document(*a: Any, **k: Any) -> ActivityOutcome:
        nonlocal called
        called = True
        raise AssertionError("process_document should not be called for folders/disallowed extensions")

    monkeypatch.setattr(services, "process_document", _unexpected_process_document)

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, FakeRepository(), lifecycle, connector, None, None, None)

    assert called is False
    assert outcome.created_or_updated == 0
    assert outcome.failed == 0


# --- resync_document_acl / run_acl_resync_page ---

def _ready_ref(**overrides: Any) -> ReadyDocumentRef:
    values = dict(
        document_id="doc-1", source_run_id="source:run-a", document_key="source:run-a:doc-1",
        item_id="item-1", allowed_group_ids=("group-a",), acl_hash="hash-a", etag="etag-1",
    )
    values.update(overrides)
    return ReadyDocumentRef(**values)


class FakeConnector:
    """A SourceConnector stub for ACL-resync tests that don't need real Graph HTTP."""

    def __init__(self, read_verified_acl: Any, read_item: Any | None = None) -> None:
        self._read_verified_acl = read_verified_acl
        self._read_item = read_item or (lambda item_id: {"id": item_id, "eTag": "etag-x"})

    def read_verified_acl(self, item_id: str, max_pages: int) -> VerifiedAcl:
        return self._read_verified_acl(item_id, max_pages)

    def read_item(self, item_id: str) -> dict[str, Any] | None:
        return self._read_item(item_id)


def test_resync_document_acl_is_a_noop_when_hash_unchanged() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()
    connector = FakeConnector(lambda *a, **k: VerifiedAcl(("group-a",), "hash-a"))

    result = services.resync_document_acl(config, _ready_ref(), lifecycle, connector)

    assert result == "unchanged"
    assert lifecycle.refreshed == []
    assert lifecycle.retired == []


def test_resync_document_acl_updates_feedback_without_refreshing_chunks() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()
    connector = FakeConnector(
        lambda *a, **k: VerifiedAcl(
            ("group-a",),
            "hash-a",
            (AclFeedback.SHARED_LINK,),
        )
    )

    result = services.resync_document_acl(config, _ready_ref(), lifecycle, connector)

    assert result == "updated"
    assert lifecycle.refreshed == []
    assert lifecycle.feedback_updated == [{
        "source_run_id": "source:run-a",
        "document_id": "doc-1",
        "etag": "etag-1",
        "acl_status": AclStatus.VERIFIED,
        "acl_feedback": (AclFeedback.SHARED_LINK,),
    }]


@pytest.mark.parametrize("mode", ["delta", "periodic"])
@pytest.mark.parametrize("revoke_source_a", [False, True])
@pytest.mark.parametrize("source_order", [("source-a", "source-b"), ("source-b", "source-a")])
def test_two_source_acl_changes_and_reconciliation_do_not_mutate_other_source(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    revoke_source_a: bool,
    source_order: tuple[str, str],
) -> None:
    configs = {
        source: build_config(source_id=source, drive_id=f"drive-{source}") for source in source_order
    }
    refs = {
        source: _ready_ref(
            source_id=source, drive_id=config.drive_id, item_id="same-item",
            document_id=create_document_id(source, config.drive_id, "same-item"),
            source_run_id=create_source_run_id(source, "full-run"),
            document_key=create_document_key(
                source, "full-run", create_document_id(source, config.drive_id, "same-item"),
            ),
            source_etag="etag-x",
            allowed_group_ids=(f"group-{source}",), acl_hash=f"hash-{source}",
        )
        for source, config in configs.items()
    }
    lifecycle = FakeLifecycleRepository()
    lifecycle.ready_by_document_id = {ref.document_id: ref for ref in refs.values()}
    cursors = {
        source: f"https://graph.microsoft.com/v1.0/drives/{config.drive_id}/root/delta?token=old"
        for source, config in configs.items()
    }
    monkeypatch.setattr(lifecycle, "get_delta_cursor", cursors.get)
    monkeypatch.setattr(lifecycle, "save_delta_cursor", cursors.__setitem__)

    def ready_page(*, source_id: str, page_size: int, continuation_token: str | None) -> Any:
        assert page_size == (50 if mode == "delta" else 10) and continuation_token is None
        ref = refs[source_id]
        return SimpleNamespace(
            items=(ref,) if ref.status is DocumentStatus.READY else (), continuation_token=None,
        )

    def refresh_acl(**kwargs: Any) -> None:
        lifecycle.refreshed.append(kwargs)
        source = next(source for source, ref in refs.items() if ref.document_key == kwargs["document_key"])
        refs[source] = replace(
            refs[source], allowed_group_ids=kwargs["allowed_group_ids"], acl_hash=kwargs["acl_hash"],
        )

    def retire_acl(**kwargs: Any) -> None:
        lifecycle.retired.append(kwargs)
        source = next(source for source, ref in refs.items() if ref.document_key == kwargs["document_key"])
        refs[source] = replace(refs[source], status=DocumentStatus.RETIRED)

    monkeypatch.setattr(lifecycle, "list_ready_documents_page", ready_page)
    monkeypatch.setattr(lifecycle, "refresh_document_acl", refresh_acl)
    monkeypatch.setattr(lifecycle, "retire_document", retire_acl)

    def handler(request: httpx.Request) -> httpx.Response:
        source = next(source for source, link in cursors.items() if link == str(request.url))
        assert request.url.path.endswith(f"/drives/{configs[source].drive_id}/root/delta")
        item = _delta_pdf_item("same-item")
        item["@microsoft.graph.sharedChanged"] = True
        return httpx.Response(200, json={
            "value": [item], "@odata.deltaLink": str(request.url).replace("token=old", "token=new"),
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        for source in source_order:
            config = configs[source]
            connector = SharePointConnector(client, config.drive_id)

            def read_acl(
                selected_connector: SharePointConnector, item_id: str, max_pages: int,
            ) -> VerifiedAcl:
                assert selected_connector.drive_id == config.drive_id
                assert (
                    item_id == "same-item"
                    and max_pages == config.execution.acl_max_pages
                )
                if source == "source-a":
                    if revoke_source_a:
                        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")
                    return VerifiedAcl(("replacement-group-a",), "replacement-hash-a")
                return VerifiedAcl(refs[source].allowed_group_ids, refs[source].acl_hash)

            monkeypatch.setattr(SharePointConnector, "read_verified_acl", read_acl)
            if mode == "delta":
                result = services.run_delta_sync(
                    config, FakeRepository(), lifecycle, connector, None, None, None,
                )
                assert result.acl_resynced == 1
                assert result.created_or_updated == result.failed == result.pending == 0
            else:
                result, token = services.run_acl_resync_page(
                    config, lifecycle, connector, page_size=10, continuation_token=None,
                )
                assert token is None and result.checked == 1
                assert result.unchanged == int(source == "source-b")
                assert result.updated == int(source == "source-a" and not revoke_source_a)
                assert result.retired == int(source == "source-a" and revoke_source_a)
            assert all(
                mutation["source_run_id"] == refs["source-a"].source_run_id
                for mutation in lifecycle.refreshed + lifecycle.retired
            )

    if revoke_source_a:
        assert lifecycle.refreshed == []
        assert len(lifecycle.retired) == 1
        assert lifecycle.retired[0]["document_key"] == refs["source-a"].document_key
        assert lifecycle.retired[0]["reason"] == "acl_revoked"
    else:
        assert lifecycle.retired == []
        assert len(lifecycle.refreshed) == 1
        assert lifecycle.refreshed[0]["document_key"] == refs["source-a"].document_key
        assert lifecycle.refreshed[0]["allowed_group_ids"] == ("replacement-group-a",)


def test_resync_document_acl_patches_when_hash_changed() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()
    connector = FakeConnector(lambda *a, **k: VerifiedAcl(("group-a", "group-b"), "hash-b"))

    result = services.resync_document_acl(config, _ready_ref(), lifecycle, connector)

    assert result == "updated"
    assert len(lifecycle.refreshed) == 1
    assert lifecycle.refreshed[0]["allowed_group_ids"] == ("group-a", "group-b")


def test_resync_document_acl_restores_acl_revoked_document_as_updated() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()
    connector = FakeConnector(lambda *a, **k: VerifiedAcl(("group-b",), "hash-b"))

    result = services.resync_document_acl(
        config,
        _ready_ref(status=DocumentStatus.RETIRED, source_etag="etag-x"),
        lifecycle,
        connector,
    )

    assert result == "updated"
    assert len(lifecycle.refreshed) == 1
    assert lifecycle.refreshed[0]["allowed_group_ids"] == ("group-b",)
    assert lifecycle.retired == []


def test_resync_document_acl_rejects_historical_or_stale_retired_version() -> None:
    config = build_config()
    connector = FakeConnector(lambda *a, **k: VerifiedAcl(("group-b",), "hash-b"))

    historical = FakeLifecycleRepository()
    historical.authoritative_document_version = False
    historical_result = services.resync_document_acl(
        config,
        _ready_ref(status=DocumentStatus.RETIRED, source_etag="etag-x"),
        historical,
        connector,
    )

    stale = FakeLifecycleRepository()
    stale_result = services.resync_document_acl(
        config,
        _ready_ref(status=DocumentStatus.RETIRED, source_etag="etag-old"),
        stale,
        connector,
    )

    assert historical_result == "unchanged"
    assert stale_result == "unchanged"
    assert historical.refreshed == []
    assert stale.refreshed == []


def test_resync_document_acl_retries_incomplete_refresh_even_when_hash_is_unchanged() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()
    connector = FakeConnector(lambda *a, **k: VerifiedAcl(("group-a",), "hash-a"))

    result = services.resync_document_acl(
        config, _ready_ref(status=DocumentStatus.ACL_REFRESHING), lifecycle, connector,
    )

    assert result == "updated"
    assert len(lifecycle.refreshed) == 1


def test_resync_document_acl_retires_on_terminal_acl_error() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()

    def _raise(*a: Any, **k: Any) -> None:
        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")

    connector = FakeConnector(_raise)

    result = services.resync_document_acl(config, _ready_ref(), lifecycle, connector)

    assert result == "retired"
    assert lifecycle.retired == [{
        "source_run_id": "source:run-a",
        "document_id": "doc-1",
        "document_key": "source:run-a:doc-1",
        "reason": "acl_revoked",
    }]


def test_resync_document_acl_leaves_still_revoked_document_unchanged() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()

    def _raise(*a: Any, **k: Any) -> None:
        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")

    result = services.resync_document_acl(
        config,
        _ready_ref(status=DocumentStatus.RETIRED),
        lifecycle,
        FakeConnector(_raise),
    )

    assert result == "unchanged"
    assert lifecycle.retired == []
    assert lifecycle.refreshed == []


def test_resync_document_acl_propagates_transient_dependency_failure() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()

    def _raise(*a: Any, **k: Any) -> None:
        raise RuntimeError("SharePoint unavailable")

    with pytest.raises(RuntimeError, match="SharePoint unavailable"):
        services.resync_document_acl(
            config,
            _ready_ref(),
            lifecycle,
            FakeConnector(_raise),
        )

    assert lifecycle.retired == []
    assert lifecycle.refreshed == []


def test_resync_document_acl_writes_audit_record_on_retire() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()
    audit_container = MagicMock()

    def _raise(*a: Any, **k: Any) -> None:
        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")

    connector = FakeConnector(_raise)

    result = services.resync_document_acl(config, _ready_ref(), lifecycle, connector, audit_container=audit_container)

    assert result == "retired"
    audit_container.create_item.assert_called_once()
    item = audit_container.create_item.call_args[0][0]
    assert item["operation"] == "document_retired"
    assert item["documentId"] == "doc-1"
    assert item["retiredReason"] == "acl_revoked"
    assert item["runId"] == "source:run-a"


def test_resync_document_acl_skips_audit_when_container_is_none() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()

    def _raise(*a: Any, **k: Any) -> None:
        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")

    connector = FakeConnector(_raise)

    result = services.resync_document_acl(config, _ready_ref(), lifecycle, connector, audit_container=None)

    assert result == "retired"


def test_lifecycle_reconciliation_isolates_poison_documents() -> None:
    config = build_config()
    acl_ref = LifecycleDocumentRef(
        document_id="doc-acl",
        source_run_id="source:run-a",
        document_key="source:run-a:doc-acl",
        status=DocumentStatus.ACL_REFRESHING,
        lifecycle_generation=1,
        etag="etag-acl",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
        pending_allowed_group_ids=("group-b",),
        pending_acl_hash="hash-b",
        pending_acl_status=AclStatus.VERIFIED,
        pending_acl_feedback=(),
    )
    poison_ref = LifecycleDocumentRef(
        document_id="doc-retire",
        source_run_id="source:run-a",
        document_key="source:run-a:doc-retire",
        status=DocumentStatus.RETIRING,
        lifecycle_generation=1,
        etag="etag-retire",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
    )
    delete_ref = LifecycleDocumentRef(
        document_id="doc-delete",
        source_run_id="source:run-a",
        document_key="source:run-a:doc-delete",
        status=DocumentStatus.DELETING,
        lifecycle_generation=1,
        etag="etag-delete",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
    )
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_lifecycle_transitions_page.return_value = LifecycleDocumentPage(
        (acl_ref, poison_ref, delete_ref),
        "next-page",
    )
    repository = MagicMock()
    repository.get_document.return_value = VersionedRecord(
        SimpleNamespace(
            item_id="item-acl",
            status=DocumentStatus.ACL_REFRESHING,
            acl_status=AclStatus.VERIFIED,
            acl_feedback=(),
            e_tag="source-etag",
        ),
        "etag-acl",
    )
    connector = FakeConnector(
        lambda *_args, **_kwargs: VerifiedAcl(("group-c",), "hash-c")
    )

    outcome, continuation = services.run_lifecycle_reconciliation_page(
        config,
        repository,
        lifecycle,
        lambda: connector,
        source_id="source",
        page_size=10,
    )

    assert outcome.checked == 3
    assert outcome.repaired == 2
    assert outcome.failed == 1
    assert continuation == "next-page"
    lifecycle.refresh_document_acl.assert_called_once_with(
        source_run_id="source:run-a",
        document_id="doc-acl",
        document_key="source:run-a:doc-acl",
        etag="etag-acl",
        allowed_group_ids=("group-c",),
        acl_hash="hash-c",
        acl_status=AclStatus.VERIFIED,
        acl_feedback=(),
    )
    lifecycle.delete_document_and_chunks.assert_called_once_with(
        source_run_id="source:run-a",
        document_id="doc-delete",
        document_key="source:run-a:doc-delete",
        etag="etag-delete",
    )


@pytest.mark.parametrize("transient_failure", [False, True])
def test_interrupted_acl_recovery_uses_real_document_hydration(
    monkeypatch: pytest.MonkeyPatch, transient_failure: bool,
) -> None:
    from copy import deepcopy

    from ingestion.models import DocumentStage, content_sha256
    from ingestion.repository import IngestionRepository
    from test_lifecycle_repository import FakeContainer
    from test_repository import build_document

    document = replace(
        build_document(), status=DocumentStatus.READY, stage=DocumentStage.TERMINAL,
        acl_status=AclStatus.VERIFIED,
    )
    key = (document.source_run_id, document.document_id)
    documents = FakeContainer(
        "sourceRunId", {key: {**document.to_cosmos_item(), "_etag": "etag-ready"}},
    )
    chunks = FakeContainer("documentKey", {(document.document_key, "chunk-0"): {
        "id": "chunk-0", "documentKey": document.document_key,
        "allowedGroupIds": list(document.allowed_group_ids),
        "isRetrievable": True, "lifecycleGeneration": 0,
    }})
    lifecycle = DocumentLifecycleRepository(FakeContainer("sourceId"), documents, chunks)

    def interrupt(**_kwargs: Any) -> None:
        raise TimeoutError("synthetic interruption")

    with monkeypatch.context() as scoped:
        scoped.setattr(lifecycle, "set_document_chunks_retrievable", interrupt)
        with pytest.raises(TimeoutError, match="synthetic interruption"):
            lifecycle.refresh_document_acl(
                source_run_id=document.source_run_id, document_id=document.document_id,
                document_key=document.document_key, etag="etag-ready",
                allowed_group_ids=("group-b",), acl_hash=content_sha256("group-b"),
            )
    before = deepcopy(documents.items[key])
    assert before["status"] == "acl_refreshing" and before["pendingAllowedGroupIds"] == ["group-b"]

    acl_calls = []

    def acl(item_id: str, max_pages: int) -> VerifiedAcl:
        acl_calls.append((item_id, max_pages))
        if transient_failure:
            raise TimeoutError("synthetic Graph throttling")
        return VerifiedAcl(("group-b",), content_sha256("group-b"))

    repository = IngestionRepository(None, documents, chunks)
    outcome, continuation = services.run_lifecycle_reconciliation_page(
        build_config(), repository, lifecycle, lambda: FakeConnector(acl),
        source_id=document.source_id, page_size=10,
    )

    assert continuation is None
    assert outcome.checked == 1
    assert acl_calls == [(document.item_id, build_config().execution.acl_max_pages)]
    assert (outcome.repaired, outcome.failed) == ((0, 1) if transient_failure else (1, 0))
    if transient_failure:
        assert documents.items[key] == before
    else:
        stored = repository.get_document(document.source_run_id, document.document_id)
        assert stored is not None and stored.record.status is DocumentStatus.READY
        assert stored.record.allowed_group_ids == ("group-b",)
        assert stored.record.lifecycle_generation == 1
        assert not any(field.startswith("pending") for field in documents.items[key])
        assert chunks.items[(document.document_key, "chunk-0")]["isRetrievable"] is True
        assert chunks.items[(document.document_key, "chunk-0")]["allowedGroupIds"] == ["group-b"]


def test_lifecycle_reconciliation_retires_when_pending_acl_is_no_longer_verifiable() -> None:
    ref = LifecycleDocumentRef(
        document_id="doc-acl",
        source_run_id="source:run-a",
        document_key="source:run-a:doc-acl",
        status=DocumentStatus.ACL_REFRESHING,
        lifecycle_generation=1,
        etag="etag-acl",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
        pending_allowed_group_ids=("group-b",),
        pending_acl_hash="hash-b",
        pending_acl_status=AclStatus.VERIFIED,
        pending_acl_feedback=(),
    )
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_lifecycle_transitions_page.return_value = LifecycleDocumentPage(
        (ref,),
        None,
    )
    repository = MagicMock()
    repository.get_document.return_value = VersionedRecord(
        SimpleNamespace(
            item_id="item-acl",
            status=DocumentStatus.ACL_REFRESHING,
            acl_status=AclStatus.VERIFIED,
            acl_feedback=(),
            e_tag="source-etag",
        ),
        "etag-acl",
    )

    def _raise_terminal(*_args: Any, **_kwargs: Any) -> None:
        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")

    outcome, continuation = services.run_lifecycle_reconciliation_page(
        build_config(),
        repository,
        lifecycle,
        lambda: FakeConnector(_raise_terminal),
        source_id="source",
        page_size=10,
    )

    assert outcome.repaired == 1
    assert outcome.failed == 0
    assert continuation is None
    lifecycle.retire_document.assert_called_once_with(
        source_run_id="source:run-a",
        document_id="doc-acl",
        document_key="source:run-a:doc-acl",
        etag="etag-acl",
        reason="acl_revoked",
    )
    lifecycle.refresh_document_acl.assert_not_called()


def test_lifecycle_reconciliation_keeps_pending_acl_on_transient_failure() -> None:
    ref = LifecycleDocumentRef(
        document_id="doc-acl",
        source_run_id="source:run-a",
        document_key="source:run-a:doc-acl",
        status=DocumentStatus.ACL_REFRESHING,
        lifecycle_generation=1,
        etag="etag-acl",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
        pending_allowed_group_ids=("group-b",),
        pending_acl_hash="hash-b",
        pending_acl_status=AclStatus.VERIFIED,
        pending_acl_feedback=(),
    )
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_lifecycle_transitions_page.return_value = LifecycleDocumentPage(
        (ref,),
        None,
    )
    repository = MagicMock()
    repository.get_document.return_value = VersionedRecord(
        SimpleNamespace(
            item_id="item-acl",
            status=DocumentStatus.ACL_REFRESHING,
            acl_status=AclStatus.VERIFIED,
            acl_feedback=(),
            e_tag="source-etag",
        ),
        "etag-acl",
    )

    def _raise_transient(*_args: Any, **_kwargs: Any) -> None:
        raise TimeoutError("graph_throttled")

    outcome, continuation = services.run_lifecycle_reconciliation_page(
        build_config(),
        repository,
        lifecycle,
        lambda: FakeConnector(_raise_transient),
        source_id="source",
        page_size=10,
    )

    assert outcome.repaired == 0
    assert outcome.failed == 1
    assert continuation is None
    lifecycle.retire_document.assert_not_called()
    lifecycle.refresh_document_acl.assert_not_called()


def test_lifecycle_reconciliation_rejects_stale_acl_refresh_fence() -> None:
    ref = LifecycleDocumentRef(
        document_id="doc-acl",
        source_run_id="source:run-a",
        document_key="source:run-a:doc-acl",
        status=DocumentStatus.ACL_REFRESHING,
        lifecycle_generation=1,
        etag="etag-stale",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
        pending_allowed_group_ids=("group-b",),
        pending_acl_hash="hash-b",
        pending_acl_status=AclStatus.VERIFIED,
        pending_acl_feedback=(),
    )
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_lifecycle_transitions_page.return_value = LifecycleDocumentPage(
        (ref,),
        None,
    )
    repository = MagicMock()
    repository.get_document.return_value = VersionedRecord(
        SimpleNamespace(
            item_id="item-acl",
            status=DocumentStatus.ACL_REFRESHING,
            acl_status=AclStatus.VERIFIED,
            acl_feedback=(),
            e_tag="source-etag",
        ),
        "etag-current",
    )

    def _unexpected_connector() -> FakeConnector:
        pytest.fail("a stale ACL-refreshing fence must not build a Graph connector")

    outcome, continuation = services.run_lifecycle_reconciliation_page(
        build_config(),
        repository,
        lifecycle,
        _unexpected_connector,
        source_id="source",
        page_size=10,
    )

    assert outcome.repaired == 0
    assert outcome.failed == 1
    assert continuation is None
    lifecycle.retire_document.assert_not_called()
    lifecycle.refresh_document_acl.assert_not_called()


def test_lifecycle_reconciliation_does_not_require_graph_for_non_acl_repair() -> None:
    ref = LifecycleDocumentRef(
        document_id="doc-delete",
        source_run_id="source:run-a",
        document_key="source:run-a:doc-delete",
        status=DocumentStatus.DELETING,
        lifecycle_generation=1,
        etag="etag-delete",
        allowed_group_ids=("group-a",),
        acl_hash="hash-a",
    )
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_lifecycle_transitions_page.return_value = LifecycleDocumentPage(
        (ref,),
        None,
    )

    def _unexpected_connector() -> FakeConnector:
        pytest.fail("non-ACL reconciliation must not build a Graph connector")

    outcome, continuation = services.run_lifecycle_reconciliation_page(
        build_config(),
        MagicMock(),
        lifecycle,
        _unexpected_connector,
        source_id="source",
        page_size=10,
    )

    assert outcome.repaired == 1
    assert outcome.failed == 0
    assert continuation is None
    lifecycle.delete_document_and_chunks.assert_called_once()


def test_acl_resync_page_claims_each_mutation_before_acl_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config()
    ref = _ready_ref()
    lifecycle = FakeLifecycleRepository()
    lifecycle.ready_inventory_pages = [[ref]]
    events: list[str] = []
    registry = RecordingWorkRegistry(events)
    monkeypatch.setattr(
        services, "resync_document_acl",
        lambda *_args, **_kwargs: events.append("acl") or "updated",
    )

    outcome, token = services.run_acl_resync_page(
        config, lifecycle,
        FakeConnector(lambda *_args, **_kwargs: VerifiedAcl(("group-a",), "hash-a")),
        page_size=10, continuation_token=None,
        work_registry=registry, source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="acl-owner", work_scope_id="acl-owner",
    )

    assert registry.operations == ["acl-resync"]
    assert events == ["claim", "start", "acl", "settle"]
    assert outcome.updated == 1 and token is None


def test_lifecycle_reconciliation_claims_each_repair_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ref = LifecycleDocumentRef(
        document_id="doc-repair", source_run_id="source:run-a",
        document_key="source:run-a:doc-repair", status=DocumentStatus.ADMITTING,
        lifecycle_generation=1, etag="etag", allowed_group_ids=("group-a",), acl_hash="hash-a",
    )
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_lifecycle_transitions_page.return_value = LifecycleDocumentPage((ref,), None)
    events: list[str] = []
    registry = RecordingWorkRegistry(events)
    monkeypatch.setattr(
        services, "_reconcile_lifecycle_document",
        lambda *_args: events.append("repair"),
    )

    outcome, token = services.run_lifecycle_reconciliation_page(
        build_config(),
        MagicMock(),
        lifecycle,
        lambda: FakeConnector(
            lambda *_args, **_kwargs: VerifiedAcl(("group-a",), "hash-a")
        ),
        source_id="source",
        page_size=10,
        work_registry=registry, source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="lifecycle-owner", work_scope_id="lifecycle-owner",
    )

    assert registry.operations == ["lifecycle-reconcile"]
    assert events == ["claim", "start", "repair", "settle"]
    assert outcome.repaired == 1 and outcome.failed == 0 and token is None


def test_duplicate_version_cleanup_is_operation_protected() -> None:
    config = build_config()
    document_id = "doc-duplicate"
    current_run_id = create_source_run_id("source", "run-current")
    current = _ready_ref(source_run_id=current_run_id, document_key=f"{current_run_id}:{document_id}")
    previous = _ready_ref(source_run_id="source:run-old", document_key="source:run-old:doc-duplicate")
    repository = MagicMock()
    repository.get_source_control.return_value.record.current_run_id = "run-current"
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_duplicate_ready_document_ids_page.return_value = DuplicateDocumentPage((document_id,), None)
    lifecycle.list_ready_document_versions_page.side_effect = [
        SimpleNamespace(items=(previous,), continuation_token="versions-next"),
        SimpleNamespace(items=(current,), continuation_token=None),
        SimpleNamespace(items=(previous,), continuation_token="versions-next"),
        SimpleNamespace(items=(current,), continuation_token=None),
    ]
    events: list[str] = []
    registry = RecordingWorkRegistry(events)
    lifecycle.delete_document_and_chunks.side_effect = lambda **_kwargs: events.append("delete")

    outcome, token = services.run_duplicate_version_reconciliation_page(
        config, repository, lifecycle, page_size=10,
        work_registry=registry, source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="repair-owner", work_scope_id="repair-owner",
    )

    assert registry.operations == ["duplicate-reconcile"]
    assert events == ["claim", "start", "delete", "settle"]
    assert outcome.repaired == 1 and outcome.failed == 0 and token is None


def test_orphan_chunk_cleanup_gets_distinct_operation_per_chunk() -> None:
    repository = MagicMock()
    repository.get_document.return_value = None
    first = ChunkManifestRef("chunk-1", "source:run-a:doc", "source:run-a", "doc")
    second = ChunkManifestRef("chunk-2", "source:run-a:doc", "source:run-a", "doc")
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_chunk_manifest_refs_page.return_value = ChunkManifestPage((first, second), None)
    events: list[str] = []
    registry = RecordingWorkRegistry(events)
    lifecycle.delete_orphan_chunk.side_effect = lambda **_kwargs: events.append("delete")

    outcome, token = services.run_orphan_chunk_reconciliation_page(
        repository, lifecycle, source_id="source", page_size=10,
        work_registry=registry, source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="repair-owner", work_scope_id="repair-owner",
    )

    assert registry.operations == ["orphan-reconcile", "orphan-reconcile"]
    assert events == ["claim", "start", "delete", "settle"] * 2
    assert outcome.repaired == 2 and outcome.failed == 0 and token is None


def test_duplicate_reconciliation_keeps_current_ready_winner() -> None:
    config = build_config()
    repository = MagicMock()
    repository.get_source_control.return_value.record.current_run_id = "run-current"
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_duplicate_ready_document_ids_page.return_value = DuplicateDocumentPage(
        ("doc-1",), None
    )
    current = _ready_ref(
        source_run_id=create_source_run_id("source", "run-current"),
        document_key="source:run-current:doc-1",
        etag="etag-current",
    )
    previous = _ready_ref(
        source_run_id=create_source_run_id("source", "run-previous"),
        document_key="source:run-previous:doc-1",
        etag="etag-previous",
    )
    lifecycle.list_ready_document_versions_page.side_effect = [
        SimpleNamespace(items=(previous,), continuation_token="versions-next"),
        SimpleNamespace(items=(current,), continuation_token=None),
        SimpleNamespace(items=(previous,), continuation_token="versions-next"),
        SimpleNamespace(items=(current,), continuation_token=None),
    ]

    outcome, continuation = services.run_duplicate_version_reconciliation_page(
        config, repository, lifecycle, page_size=10
    )

    assert outcome.checked == 1
    assert outcome.repaired == 1
    assert outcome.failed == 0
    assert continuation is None
    lifecycle.delete_document_and_chunks.assert_called_once_with(
        source_run_id=previous.source_run_id,
        document_id=previous.document_id,
        document_key=previous.document_key,
        etag=previous.etag,
    )


def test_duplicate_reconciliation_ignores_single_current_version_candidate() -> None:
    config = build_config()
    repository = MagicMock()
    repository.get_source_control.return_value.record.current_run_id = "run-current"
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_duplicate_ready_document_ids_page.return_value = DuplicateDocumentPage(
        ("doc-1",), None
    )
    current = _ready_ref(
        source_run_id=create_source_run_id("source", "run-current"),
        document_key="source:run-current:doc-1",
        etag="etag-current",
    )
    lifecycle.list_ready_document_versions_page.return_value = SimpleNamespace(
        items=(current,), continuation_token=None,
    )

    outcome, continuation = services.run_duplicate_version_reconciliation_page(
        config, repository, lifecycle, page_size=10
    )

    assert outcome.checked == 1
    assert outcome.repaired == 0
    assert outcome.failed == 0
    assert continuation is None
    lifecycle.delete_document_and_chunks.assert_not_called()


def test_orphan_reconciliation_deletes_only_missing_manifest_chunks() -> None:
    repository = MagicMock()
    repository.get_document.side_effect = [None, object()]
    lifecycle = MagicMock(spec=DocumentLifecycleRepository)
    lifecycle.list_chunk_manifest_refs_page.return_value = ChunkManifestPage(
        (
            ChunkManifestRef("chunk:000000", "source:run-a:doc-1", "source:run-a", "doc-1"),
            ChunkManifestRef("chunk:000001", "source:run-a:doc-2", "source:run-a", "doc-2"),
        ),
        None,
    )

    outcome, continuation = services.run_orphan_chunk_reconciliation_page(
        repository, lifecycle, source_id="source", page_size=10,
    )

    assert outcome.checked == 2
    assert outcome.repaired == 1
    assert outcome.failed == 0
    assert continuation is None
    lifecycle.delete_orphan_chunk.assert_called_once_with(
        chunk_id="chunk:000000",
        document_key="source:run-a:doc-1",
    )


def test_resync_document_acl_surfaces_unverified_concurrent_retirement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()
    audit_container = MagicMock()

    def _raise(*a: Any, **k: Any) -> None:
        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")

    def _retire_conflict(**kwargs: Any) -> None:
        raise LifecycleConflictError("already retired")

    connector = FakeConnector(_raise)
    monkeypatch.setattr(lifecycle, "retire_document", _retire_conflict)

    with pytest.raises(LifecycleConflictError, match="already retired"):
        services.resync_document_acl(
            config, _ready_ref(), lifecycle, connector, audit_container=audit_container,
        )

    audit_container.create_item.assert_not_called()
