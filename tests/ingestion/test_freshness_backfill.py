from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from ingestion.graph import DiscoveredPdf, DiscoveryState, DiscoveryStep, VerifiedAcl
from ingestion.models import DocumentStage, DocumentStatus, create_document_id, create_document_key
from ingestion.repository import VersionedRecord
from ingestion.services import _processing_profile_digest, discover_all


CONFIG = SimpleNamespace(
    source_id="source",
    drive_id="drive",
    allowed_extensions=(".md", ".pdf", ".docx", ".pptx", ".xlsx"),
    acl_max_pages=10,
)


class FakeDiscoveryConnector:
    def __init__(self, pdf: DiscoveredPdf) -> None:
        self._pdf = pdf

    def discover_next_page(self, state, limits, allowed_extensions):
        return DiscoveryStep(DiscoveryState((), items_scanned=1, pdfs_discovered=1), (self._pdf,))

    def read_item(self, item_id: str):
        return {"id": item_id, "eTag": "etag-1"}

    def read_verified_acl(self, _item_id: str, _max_pages: int) -> VerifiedAcl:
        return VerifiedAcl(("group-a",), "acl-hash")


class FakeDiscoveryRepository:
    def __init__(self, previous_modified: str | None) -> None:
        self.previous_modified = previous_modified
        self.created: list[Any] = []

    def get_source_control(self, source_id: str):
        return SimpleNamespace(record=SimpleNamespace(last_completed_run_id="old"))

    def get_document(self, source_run_id: str, document_id: str):
        return SimpleNamespace(
            record=SimpleNamespace(
                status=DocumentStatus.READY,
                e_tag="etag-1",
                source_modified_at=self.previous_modified,
            )
        )

    def create_discovered_document(self, document):
        self.created.append(document)
        return VersionedRecord(document, "etag")


def _source(
    modified: str | None,
    *,
    name: str = "policy.pdf",
    mime_type: str = "application/pdf",
) -> DiscoveredPdf:
    return DiscoveredPdf(
        item_id="item-1",
        parent_item_id="parent",
        name=name,
        source_path=f"/{name}",
        source_url=f"https://example.invalid/{name}",
        e_tag="etag-1",
        mime_type=mime_type,
        size_bytes=100,
        discovery_ordinal=0,
        last_modified_date_time=modified,
    )


@pytest.mark.parametrize(
    ("name", "mime_type"),
    [
        ("guide.md", "text/markdown"),
        ("policy.pdf", "application/pdf"),
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
def test_full_sync_document_serializes_native_source_mime(
    name: str,
    mime_type: str,
) -> None:
    repository = FakeDiscoveryRepository(previous_modified=None)

    documents, _ = discover_all(
        CONFIG,
        "new",
        repository,
        FakeDiscoveryConnector(
            _source("2026-08-01T00:00:00Z", name=name, mime_type=mime_type)
        ),
    )

    assert documents[0].to_cosmos_item()["mimeType"] == mime_type


def test_full_sync_reprocesses_legacy_document_missing_source_modified_at() -> None:
    repository = FakeDiscoveryRepository(previous_modified=None)
    documents, _ = discover_all(
        CONFIG, "new", repository, FakeDiscoveryConnector(_source("2026-08-01T00:00:00Z")),
    )
    assert len(documents) == 1
    assert documents[0].source_modified_at == "2026-08-01T00:00:00Z"


def test_full_sync_reprocesses_changed_source_modified_at_with_same_etag() -> None:
    repository = FakeDiscoveryRepository(previous_modified="2026-07-01T00:00:00Z")
    documents, _ = discover_all(
        CONFIG, "new", repository, FakeDiscoveryConnector(_source("2026-08-01T00:00:00Z")),
    )
    assert len(documents) == 1


def test_full_sync_skips_when_etag_and_source_modified_at_match() -> None:
    repository = FakeDiscoveryRepository(previous_modified="2026-08-01T00:00:00Z")
    documents, _ = discover_all(
        CONFIG, "new", repository, FakeDiscoveryConnector(_source("2026-08-01T00:00:00Z")),
    )
    assert documents == []
    assert repository.created == []


def test_full_sync_uses_etag_when_graph_omits_source_modified_at() -> None:
    repository = FakeDiscoveryRepository(previous_modified="2026-08-01T00:00:00Z")
    documents, _ = discover_all(
        CONFIG, "new", repository, FakeDiscoveryConnector(_source(None)),
    )
    assert documents == []


def test_full_sync_registers_work_before_creating_legacy_discovered_document() -> None:
    events: list[str] = []

    class LegacyRepository(FakeDiscoveryRepository):
        def get_document(self, source_run_id: str, document_id: str):
            return SimpleNamespace(record=SimpleNamespace(
                status=DocumentStatus.READY,
                stage=SimpleNamespace(value="terminal"),
                e_tag="etag-1",
                source_modified_at="2026-08-01T00:00:00Z",
                source_id="source",
                drive_id="drive",
                document_id=document_id,
                item_id="item-1",
                source_run_id=source_run_id,
                document_key=f"{source_run_id}:{document_id}",
                lifecycle_generation=0,
            ))

        def create_discovered_document(self, document):
            events.append("create")
            return super().create_discovered_document(document)

    class WorkRegistry:
        def get_document_winner(self, *_args):
            return None

        def begin_document_operation(self, *_args, **_kwargs):
            events.append("claim")
            return {"status": "acquired", "operationId": "operation-1"}

        def start_document_operation(self, *_args, **_kwargs):
            events.append("start")
            return {"status": "started"}

    repository = LegacyRepository(previous_modified="2026-08-01T00:00:00Z")
    documents, _ = discover_all(
        CONFIG,
        "new",
        repository,
        FakeDiscoveryConnector(_source("2026-08-01T00:00:00Z")),
        work_registry=WorkRegistry(),
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="owner",
    )

    assert len(documents) == 1
    assert events == ["claim", "create"]
    assert len(repository.created) == 1


def test_full_sync_reuses_winner_only_after_current_source_and_acl_verification() -> None:
    source = _source("2026-08-01T00:00:00Z")
    document_id = create_document_id("source", "drive", "item-1")
    profile_digest = _processing_profile_digest(CONFIG)
    previous = SimpleNamespace(
        status=DocumentStatus.READY,
        stage=DocumentStage.TERMINAL,
        e_tag="etag-1",
        source_modified_at="2026-08-01T00:00:00Z",
        source_id="source",
        drive_id="drive",
        item_id="item-1",
        document_id=document_id,
        source_run_id="source:old",
        document_key=create_document_key("source", "old", document_id),
        lifecycle_generation=0,
        acl_hash="acl-hash",
        processing_profile_digest=profile_digest,
    )
    winner = {
        "recordType": "document-winner",
        "schemaVersion": 1,
        "sourceId": "source",
        "documentId": document_id,
        "driveId": "drive",
        "itemId": "item-1",
        "sourceRunId": previous.source_run_id,
        "documentKey": previous.document_key,
        "sourceEtag": "etag-1",
        "lifecycleGeneration": 0,
        "processingProfileDigest": profile_digest,
    }

    class Repository(FakeDiscoveryRepository):
        def get_document(self, _source_run_id: str, _document_id: str):
            return SimpleNamespace(record=previous)

    class WorkRegistry:
        def get_document_winner(self, _source_id: str, _document_id: str):
            return winner

        def begin_document_operation(self, *_args, **_kwargs):
            pytest.fail("verified same-version winner should be reused")

    repository = Repository(previous_modified="2026-08-01T00:00:00Z")
    documents, _ = discover_all(
        CONFIG,
        "new",
        repository,
        FakeDiscoveryConnector(source),
        work_registry=WorkRegistry(),
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="owner",
    )

    assert documents == []
    assert repository.created == []


def test_full_sync_reuses_winner_after_previous_skip_only_run() -> None:
    source = _source("2026-08-01T00:00:00Z")
    document_id = create_document_id("source", "drive", "item-1")
    profile_digest = _processing_profile_digest(CONFIG)
    previous = SimpleNamespace(
        status=DocumentStatus.READY,
        stage=DocumentStage.TERMINAL,
        e_tag="etag-1",
        source_modified_at="2026-08-01T00:00:00Z",
        source_id="source",
        drive_id="drive",
        item_id="item-1",
        document_id=document_id,
        source_run_id="source:ready-run",
        document_key=create_document_key("source", "ready-run", document_id),
        lifecycle_generation=0,
        acl_hash="acl-hash",
        processing_profile_digest=profile_digest,
    )
    winner = {
        "recordType": "document-winner",
        "schemaVersion": 1,
        "sourceId": "source",
        "documentId": document_id,
        "driveId": "drive",
        "itemId": "item-1",
        "sourceRunId": previous.source_run_id,
        "documentKey": previous.document_key,
        "sourceEtag": "etag-1",
        "lifecycleGeneration": 0,
        "processingProfileDigest": profile_digest,
    }

    class Repository(FakeDiscoveryRepository):
        def get_source_control(self, _source_id: str):
            return SimpleNamespace(record=SimpleNamespace(
                last_completed_run_id="skip-only-run",
            ))

        def get_document(self, source_run_id: str, _document_id: str):
            if source_run_id == previous.source_run_id:
                return SimpleNamespace(record=previous)
            return None

    class WorkRegistry:
        def get_document_winner(self, _source_id: str, _document_id: str):
            return winner

        def begin_document_operation(self, *_args, **_kwargs):
            pytest.fail("unchanged winner must survive a skip-only run")

    repository = Repository(previous_modified="2026-08-01T00:00:00Z")
    documents, _ = discover_all(
        CONFIG,
        "new",
        repository,
        FakeDiscoveryConnector(source),
        work_registry=WorkRegistry(),
        source_plan=SimpleNamespace(source_id="source"),
        owner_instance_id="owner",
    )

    assert documents == []
    assert repository.created == []