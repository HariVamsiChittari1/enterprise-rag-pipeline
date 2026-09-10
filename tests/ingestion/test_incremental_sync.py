"""Tests for delta-sync and ACL-resync orchestration logic in services.py.

process_document() itself (extract/chunk/embed/write) is already exercised by the
full-sync path; these tests focus on the incremental orchestration: adapting delta
items, retiring superseded versions, hard-deleting on Graph deletion, and the
ACL-resync unchanged/updated/retired decision tree.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from config import IngestionConfig
from ingestion.errors import TerminalDocumentError
from ingestion.graph import VerifiedAcl
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
    ActivityOutcome,
    ActivityStatus,
    DocumentStatus,
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
        vision_deployment="gpt-5.4",
        managed_identity_client_id="mi",
        chunk_max_tokens=800,
        chunk_overlap_tokens=100,
        acl_max_pages=10,
        download_timeout_seconds=120.0,
        delta_max_pages=200,
        embedding_batch_size=100,
        max_pdf_pages=500,
        vision_max_output_tokens=400,
        vision_max_image_bytes=2 * 1024 * 1024,
        vision_max_figures=60,
        query_proxy_timeout_seconds=30.0,
        sharepoint_site_url="",
    )
    values.update(overrides)
    return IngestionConfig(**values)


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
        self.ready_by_document_id: dict[str, ReadyDocumentRef] = {}
        self.acl_revoked_by_document_id: dict[str, ReadyDocumentRef] = {}
        self.authoritative_document_version = True

    def get_delta_cursor(self, source_id: str) -> str | None:
        return self._cursor

    def save_delta_cursor(self, source_id: str, delta_link: str) -> None:
        self.saved_cursors.append(delta_link)
        self._cursor = delta_link

    def find_ready_document_by_document_id(self, document_id: str) -> ReadyDocumentRef | None:
        return self.ready_by_document_id.get(document_id)

    def find_acl_revoked_document_by_document_id(self, document_id: str) -> ReadyDocumentRef | None:
        return self.acl_revoked_by_document_id.get(document_id)

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

def test_run_delta_sync_bootstraps_cursor_on_first_tick_without_processing() -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository(cursor=None)

    def handler(request: httpx.Request) -> httpx.Response:
        assert "token=latest" in str(request.url)
        return httpx.Response(200, json={"@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive/root/delta?token=abc"})

    with _mock_connector(handler) as client:
        connector = SharePointConnector(client, config.drive_id)
        outcome = services.run_delta_sync(config, FakeRepository(), lifecycle, connector, None, None, None)

    assert outcome.bootstrapped is True
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


def test_run_delta_sync_retains_cursor_when_any_item_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config()
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
            error=services.SafeError("processing_failed", "embedding", True),
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
        source_etag="etag-x",
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

    outcome, continuation = services.run_lifecycle_reconciliation_page(
        MagicMock(),
        lifecycle,
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
        allowed_group_ids=("group-b",),
        acl_hash="hash-b",
    )
    lifecycle.delete_document_and_chunks.assert_called_once_with(
        source_run_id="source:run-a",
        document_id="doc-delete",
        document_key="source:run-a:doc-delete",
        etag="etag-delete",
    )


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
    lifecycle.list_ready_document_versions.return_value = (previous, current)

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
    lifecycle.list_ready_document_versions.return_value = (current,)

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
        repository, lifecycle, page_size=10
    )

    assert outcome.checked == 2
    assert outcome.repaired == 1
    assert outcome.failed == 0
    assert continuation is None
    lifecycle.delete_orphan_chunk.assert_called_once_with(
        chunk_id="chunk:000000",
        document_key="source:run-a:doc-1",
    )


def test_resync_document_acl_tolerates_concurrent_retirement(monkeypatch: pytest.MonkeyPatch) -> None:
    config = build_config()
    lifecycle = FakeLifecycleRepository()
    audit_container = MagicMock()

    def _raise(*a: Any, **k: Any) -> None:
        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")

    def _retire_conflict(**kwargs: Any) -> None:
        raise LifecycleConflictError("already retired")

    connector = FakeConnector(_raise)
    monkeypatch.setattr(lifecycle, "retire_document", _retire_conflict)

    result = services.resync_document_acl(config, _ready_ref(), lifecycle, connector, audit_container=audit_container)

    assert result == "retired"
    audit_container.create_item.assert_not_called()
