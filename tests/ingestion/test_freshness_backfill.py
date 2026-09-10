from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from ingestion.graph import DiscoveredPdf, DiscoveryState, DiscoveryStep
from ingestion.models import DocumentStatus
from ingestion.repository import VersionedRecord
from ingestion.services import discover_all


CONFIG = SimpleNamespace(
    source_id="source",
    drive_id="drive",
    allowed_extensions=(".md", ".pdf", ".docx", ".pptx", ".xlsx"),
)


class FakeDiscoveryConnector:
    def __init__(self, pdf: DiscoveredPdf) -> None:
        self._pdf = pdf

    def discover_next_page(self, state, limits, allowed_extensions):
        return DiscoveryStep(DiscoveryState((), items_scanned=1, pdfs_discovered=1), (self._pdf,))


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