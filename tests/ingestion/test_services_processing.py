from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from config import IngestionConfig
from ingestion.graph import ResolvedMarkdownImage, VerifiedAcl
from ingestion.models import (
    ActivityStatus,
    CanonicalExtractionResult,
    CanonicalSegment,
    ContentModality,
    DocumentStage,
    DocumentStatus,
    ExtractionProvenance,
    LocatorKind,
    Page,
    ScaleLimits,
    SourceLocator,
    SourceDocumentRecord,
    VisualDisposition,
    VisualCoverage,
    VisualCoverageStatus,
    VisualManifestEntry,
    VisualRelevance,
    content_sha256,
    create_document_id,
    create_document_key,
    create_source_run_id,
    visual_manifest_hash,
)
from ingestion.repository import VersionedRecord
from ingestion.services import process_document
from ingestion.office_visuals import (
    OfficeContentUnit,
    OfficeVisualInventory,
    OfficeVisualObject,
    RenderedVisualDescription,
)
import ingestion.services as services

UTC = "2026-09-02T07:00:00Z"
GROUP_A = "22222222-2222-4222-8222-222222222222"
ACL_A = VerifiedAcl((GROUP_A,), content_sha256(GROUP_A))


def _canonical(page: Page) -> CanonicalExtractionResult:
    locator = page.locator or SourceLocator(
        LocatorKind.PAGE,
        f"Page {page.number}",
        page.number,
        page.number,
    )
    return CanonicalExtractionResult(
        segments=(
            CanonicalSegment(
                ordinal=0,
                text=page.text,
                locator=locator,
                modalities=(ContentModality.TEXT,),
                provenance=(ExtractionProvenance.DIRECT,),
            ),
        ),
        visual_coverage=VisualCoverage(
            status=VisualCoverageStatus.NOT_REQUIRED,
            inventory_count=0,
            required_count=0,
            described_count=0,
            excluded_count=0,
            unsupported_count=0,
            uncovered_count=0,
        ),
    )


def _canonical_visual(page: Page) -> CanonicalExtractionResult:
    locator = page.locator or SourceLocator(
        LocatorKind.PAGE,
        f"Page {page.number}",
        page.number,
        page.number,
    )
    return CanonicalExtractionResult(
        segments=(
            CanonicalSegment(
                ordinal=0,
                text=page.text,
                locator=locator,
                modalities=(ContentModality.TEXT, ContentModality.VISUAL_DESCRIPTION),
                provenance=(ExtractionProvenance.RENDERED,),
            ),
        ),
        visual_coverage=VisualCoverage(
            status=VisualCoverageStatus.COMPLETE,
            inventory_count=1,
            required_count=1,
            described_count=1,
            excluded_count=0,
            unsupported_count=0,
            uncovered_count=0,
        ),
        visual_manifest_entries=(
            VisualManifestEntry(
                ordinal=0,
                visual_id="figure-1",
                object_type="figure",
                source_locator=locator,
                relevance=VisualRelevance.REQUIRED,
                disposition=VisualDisposition.DESCRIBED,
                description="A process flow.",
                provenance=(ExtractionProvenance.RENDERED,),
                derivative_locator=locator,
            ),
        ),
    )


def _config(**overrides: Any) -> IngestionConfig:
    values: dict[str, Any] = dict(
        extraction_enabled=True,
        enrichment_enabled=False,
        summary_enabled=False,
        key_phrases_enabled=False,
        entities_enabled=False,
        allowed_extensions=(".md", ".pdf", ".docx", ".pptx", ".xlsx"),
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
        content_understanding_analyzer_id="prebuilt-documentSearch",
        language_endpoint="",
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


def _document(name: str) -> SourceDocumentRecord:
    document_id = create_document_id("source", "drive", "item-1")
    return SourceDocumentRecord(
        source_id="source",
        run_id="run-1",
        drive_id="drive",
        item_id="item-1",
        parent_item_id="parent-1",
        source_name=name,
        source_path=f"/documents/{name}",
        source_url=f"https://example.invalid/{name}",
        e_tag="source-etag",
        mime_type="application/pdf",
        size_bytes=100,
        discovery_ordinal=1,
        allowed_group_ids=("pending",),
        acl_hash=content_sha256("pending"),
        acl_evaluated_at=UTC,
        status=DocumentStatus.DISCOVERED,
        stage=DocumentStage.DISCOVERED,
        attempt_count=0,
        discovered_at=UTC,
        updated_at=UTC,
        id=document_id,
        document_id=document_id,
        source_run_id=create_source_run_id("source", "run-1"),
        document_key=create_document_key("source", "run-1", document_id),
    )


class _Repository:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.chunks: list[Any] = []
        self.manifest_pages: list[Any] = []
        self.admitted_document: SourceDocumentRecord | None = None

    def _stored(self, document: SourceDocumentRecord) -> VersionedRecord:
        return VersionedRecord(document, f"etag-{len(self.calls)}")

    def mark_document_processing(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("mark_document_processing")
        return self._stored(document)

    def update_processing_document(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("update_processing_document")
        return self._stored(document)

    def write_chunks(self, chunks: Any) -> int:
        self.calls.append("write_chunks")
        self.chunks = list(chunks)
        return len(self.chunks)

    def write_visual_manifest_pages(self, pages: Any) -> int:
        self.calls.append("write_visual_manifest_pages")
        self.manifest_pages = list(pages)
        return len(self.manifest_pages)

    def begin_document_admission(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("begin_document_admission")
        self.admitted_document = document
        return self._stored(document)

    def verify_and_mark_document_ready(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("verify_and_mark_document_ready")
        return self._stored(document)

    def mark_document_failed(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("mark_document_failed")
        return self._stored(document)


class _LifecycleRepository:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def set_document_chunks_retrievable(self, **kwargs: Any) -> int:
        self.calls.append(kwargs)
        return kwargs["expected_count"]


class _Connector:
    def __init__(self, name: str, mime_type: str, content: bytes) -> None:
        self.name = name
        self.mime_type = mime_type
        self.content = content
        self.item_etags = ["source-etag", "source-etag"]
        self.item_mime_types = [mime_type, mime_type]
        self.document_acls = [ACL_A, ACL_A]
        self.image_acls = [ACL_A, ACL_A]
        self.image_etags = ["image-etag"]
        self.original_downloads = 0
        self.image_downloads = 0
        self.conversions = 0
        self.relative_paths: list[str] = []
        self.source_download_limits: list[int] = []
        self.rendered_download_limits: list[int] = []

    def read_item(self, item_id: str) -> dict[str, Any]:
        if item_id == "image-1":
            return {
                "id": "image-1",
                "name": "chart.png",
                "eTag": self.image_etags.pop(0),
                "size": 13,
                "file": {"mimeType": "image/png"},
            }
        return {
            "id": "item-1",
            "name": self.name,
            "eTag": self.item_etags.pop(0),
            "size": 100,
            "file": {"mimeType": self.item_mime_types.pop(0)},
        }

    def read_verified_acl(self, item_id: str, _max_pages: int) -> VerifiedAcl:
        if item_id == "image-1":
            return self.image_acls.pop(0)
        return self.document_acls.pop(0)

    def download_content_sync(self, item_id: str, max_bytes: int, _timeout: float) -> bytes:
        if item_id == "image-1":
            self.image_downloads += 1
            return b"\x89PNG\r\n\x1a\nimage"
        self.original_downloads += 1
        self.source_download_limits.append(max_bytes)
        return self.content

    def download_content_as_pdf_sync(self, _item_id: str, max_bytes: int, _timeout: float) -> bytes:
        self.conversions += 1
        self.rendered_download_limits.append(max_bytes)
        return b"%PDF-converted"

    def download_relative_content_sync(self, _parent_id: str, path: str, _max_bytes: int, _timeout: float) -> bytes:
        self.relative_paths.append(path)
        return b"\x89PNG\r\n\x1a\nimage"

    def resolve_relative_markdown_image_sync(
        self,
        _parent_id: str,
        path: str,
        _max_bytes: int,
    ) -> ResolvedMarkdownImage:
        self.relative_paths.append(path)
        return ResolvedMarkdownImage(
            item_id="image-1",
            name="chart.png",
            e_tag="image-etag",
            size_bytes=13,
            mime_type="image/png",
        )


def _run(
    monkeypatch: Any,
    document: SourceDocumentRecord,
    connector: _Connector,
    *,
    config: IngestionConfig | None = None,
    di_client: Any | None = None,
    cu_client: Any | None = None,
) -> tuple[_Repository, _LifecycleRepository, Any]:
    repository = _Repository()
    lifecycle = _LifecycleRepository()
    monkeypatch.setattr(
        services,
        "embed_texts",
        lambda _client, texts, **_kwargs: [(0.0,) * 3072 for _ in texts],
    )
    outcome = process_document(
        config or _config(),
        document,
        "etag-0",
        repository,  # type: ignore[arg-type]
        lifecycle,  # type: ignore[arg-type]
        connector,  # type: ignore[arg-type]
        object() if di_client is None else di_client,
        None,
        object(),
        cu_client=object() if cu_client is None else cu_client,
    )
    return repository, lifecycle, outcome


def _stub_markdown_image_extraction(monkeypatch: Any) -> None:
    def fake_extract_markdown(_content: bytes, **kwargs: Any) -> CanonicalExtractionResult:
        kwargs["image_loader"]("chart.png")
        return _canonical(
            Page(1, "Extracted Markdown and image description with enough text for one chunk.")
        )

    monkeypatch.setattr(services, "extract_markdown", fake_extract_markdown)


@pytest.mark.parametrize(
    ("name", "mime_type", "locator_kind", "locator_label"),
    [
        (
            "report.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            LocatorKind.SECTION,
            "Document",
        ),
        (
            "briefing.pptx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            LocatorKind.SLIDE,
            "Slide 1",
        ),
        (
            "forecast.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            LocatorKind.WORKSHEET,
            "Summary",
        ),
    ],
)
def test_given_office_visual_when_processing_then_merges_rendered_description(
    monkeypatch: Any,
    name: str,
    mime_type: str,
    locator_kind: LocatorKind,
    locator_label: str,
) -> None:
    connector = _Connector(
        name,
        mime_type,
        b"PK\x03\x04original-office-content",
    )
    locator = SourceLocator(locator_kind, locator_label, 1, 1)
    inventory = OfficeVisualInventory(
        content_units=(OfficeContentUnit("office/unit1.xml", locator, False),),
        required=(
            OfficeVisualObject(
                "office/unit1.xml#visual1",
                "image",
                locator,
                "office/media/image1.png",
            ),
        ),
        excluded=(),
        unsupported=(),
    )
    extracted: list[tuple[bytes, str]] = []
    rendered: list[bytes] = []
    monkeypatch.setattr(
        services,
        "inventory_office_visuals",
        lambda content, content_type, **_kwargs: inventory
        if (content, content_type) == (connector.content, mime_type)
        else None,
    )
    monkeypatch.setattr(
        services,
        "extract_office_document_intelligence",
        lambda _client, content, content_type, **_kwargs: extracted.append(
            (content, content_type)
        )
        or _canonical(
            Page(
                1,
                "Extracted Office content with enough text for one production chunk.",
                locator=locator,
            )
        ),
    )
    monkeypatch.setattr(
        services,
        "extract_rendered_pdf_visuals",
        lambda _client, content, **_kwargs: rendered.append(content)
        or (RenderedVisualDescription(0, 1, "[Figure 1] A process flow."),),
    )

    repository, lifecycle, outcome = _run(
        monkeypatch,
        _document(name),
        connector,
    )

    assert outcome.status is ActivityStatus.SUCCEEDED
    assert connector.original_downloads == 1
    assert connector.conversions == 1
    assert connector.source_download_limits == [100 * 1024 * 1024]
    assert connector.rendered_download_limits == [200 * 1024 * 1024]
    assert extracted == [
        (
            b"PK\x03\x04original-office-content",
            mime_type,
        )
    ]
    assert rendered == [b"%PDF-converted"]
    assert any(
        ExtractionProvenance.RENDERED in chunk.provenance
        and chunk.visual_coverage is VisualCoverageStatus.COMPLETE
        for chunk in repository.chunks
    )
    assert repository.calls[-1] == "verify_and_mark_document_ready"
    assert repository.calls.index("write_visual_manifest_pages") < repository.calls.index(
        "begin_document_admission"
    )
    assert len(repository.manifest_pages) == 1
    assert repository.admitted_document is not None
    assert repository.admitted_document.visual_manifest_page_count == 1
    assert repository.admitted_document.visual_manifest_hash == visual_manifest_hash(
        tuple(repository.manifest_pages)
    )
    assert len(lifecycle.calls) == 1


def test_given_markdown_when_processing_then_uses_direct_extraction_and_relative_loader(monkeypatch: Any) -> None:
    connector = _Connector("guide.md", "text/markdown", b"# Guide\n\nDirect Markdown content")

    def fake_extract_markdown(content: bytes, **kwargs: Any) -> CanonicalExtractionResult:
        kwargs["image_loader"]("images/chart.png")
        return _canonical(Page(1, content.decode("utf-8") + " with enough text for extraction."))

    monkeypatch.setattr(services, "extract_markdown", fake_extract_markdown)

    repository, _, outcome = _run(
        monkeypatch,
        _document("guide.md"),
        connector,
    )

    assert outcome.status is ActivityStatus.SUCCEEDED
    assert connector.original_downloads == 1
    assert connector.conversions == 0
    assert connector.relative_paths == ["images/chart.png"]
    assert connector.image_downloads == 1
    assert repository.chunks[0].is_retrievable is False


def test_given_pdf_when_processing_then_uses_original_without_conversion(monkeypatch: Any) -> None:
    connector = _Connector("report.pdf", "application/pdf", b"%PDF-original")
    extracted: list[bytes] = []
    monkeypatch.setattr(
        services,
        "extract_pdf",
        lambda _client, content, **_kwargs: extracted.append(content)
        or _canonical(Page(1, "Extracted PDF content with enough text for one production chunk.")),
    )

    _, _, outcome = _run(
        monkeypatch,
        _document("report.pdf"),
        connector,
    )

    assert outcome.status is ActivityStatus.SUCCEEDED
    assert connector.conversions == 0
    assert extracted == [b"%PDF-original"]


def test_approved_document_capacity_limits_are_fixed() -> None:
    limits = ScaleLimits()

    assert limits.max_source_bytes == 100 * 1024 * 1024
    assert limits.max_rendered_pdf_bytes == 200 * 1024 * 1024
    assert limits.max_document_units == 300
    assert limits.max_office_characters == 8_000_000
    assert limits.max_visual_descriptions == 60


def test_given_office_over_aggregate_character_limit_then_fails_before_chunking(
    monkeypatch: Any,
) -> None:
    connector = _Connector(
        "briefing.pptx",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        b"PK\x03\x04original-office-content",
    )
    locator = SourceLocator(LocatorKind.SLIDE, "Slide 1", 1, 1)
    monkeypatch.setattr(
        services,
        "inventory_office_visuals",
        lambda *_args, **_kwargs: OfficeVisualInventory(
            content_units=(OfficeContentUnit("ppt/slides/slide1.xml", locator, False),),
            required=(),
            excluded=(),
            unsupported=(),
        ),
    )
    monkeypatch.setattr(
        services,
        "extract_office_document_intelligence",
        lambda *_args, **_kwargs: CanonicalExtractionResult(
            segments=(
                CanonicalSegment(
                    ordinal=0,
                    text="x" * (ScaleLimits().max_office_characters // 2 + 1),
                    locator=locator,
                    modalities=(ContentModality.TEXT,),
                    provenance=(ExtractionProvenance.DIRECT,),
                ),
                CanonicalSegment(
                    ordinal=1,
                    text="y" * (ScaleLimits().max_office_characters // 2 + 1),
                    locator=locator,
                    modalities=(ContentModality.TEXT,),
                    provenance=(ExtractionProvenance.DIRECT,),
                ),
            ),
            visual_coverage=VisualCoverage(
                status=VisualCoverageStatus.NOT_REQUIRED,
                inventory_count=0,
                required_count=0,
                described_count=0,
                excluded_count=0,
                unsupported_count=0,
                uncovered_count=0,
            ),
        ),
    )

    repository, _, outcome = _run(monkeypatch, _document("briefing.pptx"), connector)

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "office_character_limit_exceeded"
    assert repository.chunks == []


def test_given_provider_over_visual_limit_then_fails_before_chunking(
    monkeypatch: Any,
) -> None:
    connector = _Connector("report.pdf", "application/pdf", b"%PDF-original")
    locator = SourceLocator(LocatorKind.PAGE, "Page 1", 1, 1)
    monkeypatch.setattr(
        services,
        "extract_content_understanding",
        lambda *_args, **_kwargs: CanonicalExtractionResult(
            segments=(
                CanonicalSegment(
                    ordinal=0,
                    text="CU content with enough text for one production chunk.",
                    locator=locator,
                    modalities=(ContentModality.TEXT, ContentModality.VISUAL_DESCRIPTION),
                    provenance=(ExtractionProvenance.DIRECT,),
                ),
            ),
            visual_coverage=VisualCoverage(
                status=VisualCoverageStatus.COMPLETE,
                inventory_count=61,
                required_count=61,
                described_count=61,
                excluded_count=0,
                unsupported_count=0,
                uncovered_count=0,
            ),
            visual_manifest_entries=tuple(
                VisualManifestEntry(
                    ordinal=ordinal,
                    visual_id=f"figure-{ordinal}",
                    object_type="figure",
                    source_locator=locator,
                    relevance=VisualRelevance.REQUIRED,
                    disposition=VisualDisposition.DESCRIBED,
                    description=f"Figure {ordinal}",
                    provenance=(ExtractionProvenance.DIRECT,),
                )
                for ordinal in range(61)
            ),
        ),
    )

    repository, _, outcome = _run(
        monkeypatch,
        _document("report.pdf"),
        connector,
        config=_config(
            document_intelligence_enabled=False,
            content_understanding_enabled=True,
        ),
    )

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "visual_description_limit_exceeded"
    assert repository.chunks == []


def test_given_pdf_when_cu_selected_then_uses_cu_without_di(
    monkeypatch: Any,
) -> None:
    connector = _Connector("report.pdf", "application/pdf", b"%PDF-original")
    analyzed: list[tuple[Any, bytes, str, str]] = []
    cu_client = object()
    monkeypatch.setattr(
        services,
        "extract_content_understanding",
        lambda client, content, content_type, *, analyzer_id, **_kwargs: analyzed.append(
            (client, content, content_type, analyzer_id)
        )
        or _canonical(Page(1, "CU PDF content with enough text for one production chunk.")),
    )
    monkeypatch.setattr(
        services,
        "extract_pdf",
        lambda *_args, **_kwargs: pytest.fail("DI must not run when CU is selected"),
    )

    _, _, outcome = _run(
        monkeypatch,
        _document("report.pdf"),
        connector,
        config=_config(
            document_intelligence_enabled=True,
            content_understanding_enabled=True,
        ),
        cu_client=cu_client,
    )

    assert outcome.status is ActivityStatus.SUCCEEDED
    assert analyzed == [
        (
            cu_client,
            b"%PDF-original",
            "application/pdf",
            "prebuilt-documentSearch",
        )
    ]
    assert connector.conversions == 0


@pytest.mark.parametrize(
    ("name", "mime_type"),
    [
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
def test_given_text_only_office_when_cu_selected_then_uses_rendered_pdf(
    monkeypatch: Any,
    name: str,
    mime_type: str,
) -> None:
    connector = _Connector(name, mime_type, b"PK\x03\x04office-content")
    inventory = OfficeVisualInventory((), (), (), ())
    analyzed: list[tuple[bytes, str]] = []
    monkeypatch.setattr(
        services,
        "inventory_office_visuals",
        lambda *_args, **_kwargs: inventory,
    )
    monkeypatch.setattr(
        services,
        "extract_content_understanding",
        lambda _client, content, content_type, **_kwargs: analyzed.append(
            (content, content_type)
        )
        or _canonical(Page(1, "CU Office content with enough text for one production chunk.")),
    )

    _, _, outcome = _run(
        monkeypatch,
        _document(name),
        connector,
        config=_config(content_understanding_enabled=True),
    )

    assert outcome.status is ActivityStatus.SUCCEEDED
    assert analyzed == [(b"%PDF-converted", "application/pdf")]
    assert connector.conversions == 1
    assert connector.rendered_download_limits == [200 * 1024 * 1024]


def test_given_visual_office_when_cu_selected_then_uses_rendered_pdf_only(
    monkeypatch: Any,
) -> None:
    mime_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    connector = _Connector("briefing.pptx", mime_type, b"PK\x03\x04office-content")
    locator = SourceLocator(LocatorKind.SLIDE, "Slide 1", 1, 1)
    inventory = OfficeVisualInventory(
        content_units=(OfficeContentUnit("ppt/slides/slide1.xml", locator, False),),
        required=(
            OfficeVisualObject(
                "ppt/slides/slide1.xml#visual1",
                "image",
                locator,
                "ppt/media/image1.png",
            ),
        ),
        excluded=(),
        unsupported=(),
    )
    analyzed: list[tuple[bytes, str]] = []
    monkeypatch.setattr(
        services,
        "inventory_office_visuals",
        lambda *_args, **_kwargs: inventory,
    )
    monkeypatch.setattr(
        services,
        "extract_content_understanding",
        lambda _client, content, content_type, **_kwargs: analyzed.append(
            (content, content_type)
        )
        or _canonical_visual(
            Page(1, "CU rendered content with enough text for one production chunk.")
        ),
    )
    monkeypatch.setattr(
        services,
        "extract_office_document_intelligence",
        lambda *_args, **_kwargs: pytest.fail("DI must not run when CU is selected"),
    )

    repository, _, outcome = _run(
        monkeypatch,
        _document("briefing.pptx"),
        connector,
        config=_config(content_understanding_enabled=True),
    )

    assert outcome.status is ActivityStatus.SUCCEEDED
    assert analyzed == [(b"%PDF-converted", "application/pdf")]
    assert connector.conversions == 1
    assert repository.manifest_pages[0].entries[0].visual_id == (
        "ppt/slides/slide1.xml#visual1"
    )
    assert repository.manifest_pages[0].entries[0].source_locator == locator
    assert repository.manifest_pages[0].entries[0].derivative_locator == SourceLocator(
        LocatorKind.PAGE,
        "Page 1",
        1,
        1,
    )


def test_given_sparse_office_text_with_visual_when_processing_then_applies_minimum_after_merge(monkeypatch: Any) -> None:
    mime_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    connector = _Connector("visual-only.pptx", mime_type, b"PK\x03\x04original-office-content")
    locator = SourceLocator(LocatorKind.SLIDE, "Slide 1", 1, 1)
    inventory = OfficeVisualInventory(
        content_units=(OfficeContentUnit("ppt/slides/slide1.xml", locator, False),),
        required=(
            OfficeVisualObject(
                "ppt/slides/slide1.xml#native-drawing",
                "drawing",
                locator,
                "ppt/slides/slide1.xml",
            ),
        ),
        excluded=(),
        unsupported=(),
    )
    monkeypatch.setattr(
        services,
        "inventory_office_visuals",
        lambda *_args, **_kwargs: inventory,
    )
    monkeypatch.setattr(
        services,
        "extract_office_document_intelligence",
        lambda *_args, **_kwargs: CanonicalExtractionResult(
            segments=(
                CanonicalSegment(
                    ordinal=0,
                    text="Flow",
                    locator=locator,
                    modalities=(ContentModality.TEXT,),
                    provenance=(ExtractionProvenance.DIRECT,),
                ),
            ),
            visual_coverage=VisualCoverage(
                status=VisualCoverageStatus.NOT_REQUIRED,
                inventory_count=0,
                required_count=0,
                described_count=0,
                excluded_count=0,
                unsupported_count=0,
                uncovered_count=0,
            ),
        ),
    )
    monkeypatch.setattr(
        services,
        "extract_rendered_pdf_visuals",
        lambda *_args, **_kwargs: (
            RenderedVisualDescription(
                0,
                1,
                "[Figure 1] A rendered process flow with approval and review steps.",
            ),
        ),
    )

    repository, _, outcome = _run(monkeypatch, _document("visual-only.pptx"), connector)

    assert outcome.status is ActivityStatus.SUCCEEDED
    assert repository.chunks
    assert any("rendered process flow" in chunk.content for chunk in repository.chunks)


def test_given_etag_drift_after_chunk_write_when_processing_then_does_not_admit(monkeypatch: Any) -> None:
    connector = _Connector("report.pdf", "application/pdf", b"%PDF-original")
    connector.item_etags = ["source-etag", "changed-etag"]
    monkeypatch.setattr(
        services,
        "extract_pdf",
        lambda *_args, **_kwargs: _canonical(Page(1, "Extracted PDF content with enough text for one production chunk.")),
    )

    repository, lifecycle, outcome = _run(
        monkeypatch,
        _document("report.pdf"),
        connector,
    )

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "source_etag_changed_during_processing"
    assert "begin_document_admission" not in repository.calls
    assert lifecycle.calls == []
    assert repository.chunks and all(not chunk.is_retrievable for chunk in repository.chunks)


def test_given_acl_drift_after_chunk_write_when_processing_then_does_not_admit(monkeypatch: Any) -> None:
    connector = _Connector("report.pdf", "application/pdf", b"%PDF-original")
    connector.document_acls = [
        ACL_A,
        VerifiedAcl(
            ("33333333-3333-4333-8333-333333333333",),
            content_sha256("changed-group"),
        ),
    ]
    monkeypatch.setattr(
        services,
        "extract_pdf",
        lambda *_args, **_kwargs: _canonical(Page(1, "Extracted PDF content with enough text for one production chunk.")),
    )

    repository, lifecycle, outcome = _run(
        monkeypatch,
        _document("report.pdf"),
        connector,
    )

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "source_acl_changed_during_processing"
    assert "begin_document_admission" not in repository.calls
    assert lifecycle.calls == []
    assert repository.chunks and all(not chunk.is_retrievable for chunk in repository.chunks)


def test_given_markdown_mime_drift_after_chunk_write_when_processing_then_does_not_admit(monkeypatch: Any) -> None:
    connector = _Connector("guide.md", "text/markdown", b"# Guide\n\nDirect Markdown content with enough text for extraction.")
    connector.item_mime_types = ["text/markdown", "text/plain"]

    repository, lifecycle, outcome = _run(
        monkeypatch,
        _document("guide.md"),
        connector,
    )

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "source_mime_changed_during_processing"
    assert "begin_document_admission" not in repository.calls
    assert lifecycle.calls == []
    assert repository.chunks and all(not chunk.is_retrievable for chunk in repository.chunks)


def test_given_referenced_image_acl_excludes_document_group_when_processing_then_fails_before_image_download(monkeypatch: Any) -> None:
    connector = _Connector("guide.md", "text/markdown", b"# Guide\n\n![chart](chart.png) with enough direct text for extraction.")
    _stub_markdown_image_extraction(monkeypatch)
    connector.image_acls = [
        VerifiedAcl(
            ("33333333-3333-4333-8333-333333333333",),
            content_sha256("image-group"),
        )
    ]

    repository, lifecycle, outcome = _run(
        monkeypatch,
        _document("guide.md"),
        connector,
    )

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "markdown_image_acl_not_authorized"
    assert connector.image_downloads == 0
    assert repository.chunks == []
    assert lifecycle.calls == []


def test_given_referenced_image_etag_drift_after_chunk_write_when_processing_then_does_not_admit(monkeypatch: Any) -> None:
    connector = _Connector("guide.md", "text/markdown", b"# Guide\n\n![chart](chart.png) with enough direct text for extraction.")
    _stub_markdown_image_extraction(monkeypatch)
    connector.image_etags = ["changed-image-etag"]

    repository, lifecycle, outcome = _run(
        monkeypatch,
        _document("guide.md"),
        connector,
    )

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "markdown_image_etag_changed_during_processing"
    assert "begin_document_admission" not in repository.calls
    assert lifecycle.calls == []
    assert repository.chunks and all(not chunk.is_retrievable for chunk in repository.chunks)


def test_given_referenced_image_acl_drift_after_chunk_write_when_processing_then_does_not_admit(monkeypatch: Any) -> None:
    connector = _Connector("guide.md", "text/markdown", b"# Guide\n\n![chart](chart.png) with enough direct text for extraction.")
    _stub_markdown_image_extraction(monkeypatch)
    connector.image_acls = [
        ACL_A,
        VerifiedAcl(
            (GROUP_A, "33333333-3333-4333-8333-333333333333"),
            content_sha256("expanded-image-groups"),
        ),
    ]

    repository, lifecycle, outcome = _run(
        monkeypatch,
        _document("guide.md"),
        connector,
    )

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code == "markdown_image_acl_changed_during_processing"
    assert "begin_document_admission" not in repository.calls
    assert lifecycle.calls == []
    assert repository.chunks and all(not chunk.is_retrievable for chunk in repository.chunks)


def test_given_unsupported_live_format_when_processing_then_fails_before_download(monkeypatch: Any) -> None:
    connector = _Connector("legacy.doc", "application/msword", b"legacy")

    repository, lifecycle, outcome = _run(
        monkeypatch,
        replace(_document("report.pdf"), source_name="legacy.doc"),
        connector,
    )

    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None
    assert outcome.error.code.startswith("source_extension_not_allowed")
    assert connector.original_downloads == 0
    assert repository.chunks == []
    assert lifecycle.calls == []