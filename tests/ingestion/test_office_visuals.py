from __future__ import annotations

import io
import zipfile

import pytest

from ingestion.errors import TerminalDocumentError
from ingestion.office_visuals import (
    DOCX_MIME,
    PPTX_MIME,
    OfficeContentUnit,
    OfficeVisualInventory,
    OfficeVisualObject,
    RenderedVisualDescription,
    XLSX_MIME,
    inventory_office_visuals,
    merge_office_visuals,
)
from ingestion.models import (
    CanonicalExtractionResult,
    CanonicalSegment,
    ContentModality,
    ExtractionProvenance,
    LocatorKind,
    SourceLocator,
    VisualCoverage,
    VisualCoverageStatus,
)

REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
OFFICE_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def _archive(parts: dict[str, str | bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, value in parts.items():
            archive.writestr(name, value)
    return output.getvalue()


def _relationships(*relationships: str) -> str:
    return f'<Relationships xmlns="{REL_NS}">{"".join(relationships)}</Relationships>'


def _relationship(
    relationship_id: str,
    kind: str,
    target: str,
    *,
    external: bool = False,
) -> str:
    target_mode = ' TargetMode="External"' if external else ""
    return (
        f'<Relationship Id="{relationship_id}" Type="{OFFICE_REL_NS}/{kind}" '
        f'Target="{target}"{target_mode}/>'
    )


def test_presentation_inventory_owns_images_charts_and_native_drawings() -> None:
    content = _archive(
        {
            "ppt/slides/slide1.xml": (
                '<p:sld xmlns:p="p" xmlns:a="a"><p:cSld><p:spTree>'
                '<p:grpSp/><a:prstGeom prst="flowChartProcess"/>'
                "</p:spTree></p:cSld></p:sld>"
            ),
            "ppt/slides/_rels/slide1.xml.rels": _relationships(
                _relationship("rId1", "image", "../media/image1.png"),
                _relationship("rId2", "chart", "../charts/chart1.xml"),
            ),
            "ppt/media/image1.png": b"image",
            "ppt/charts/chart1.xml": "<chart/>",
        }
    )

    inventory = inventory_office_visuals(content, PPTX_MIME)

    assert [(item.kind, item.owner.label) for item in inventory.required] == [
        ("image", "Slide 1"),
        ("chart", "Slide 1"),
        ("drawing", "Slide 1"),
    ]
    assert inventory.excluded == ()
    assert inventory.unsupported == ()


def test_hidden_slide_visuals_are_excluded() -> None:
    content = _archive(
        {
            "ppt/slides/slide1.xml": '<p:sld xmlns:p="p" show="0"><p:cSld/></p:sld>',
            "ppt/slides/_rels/slide1.xml.rels": _relationships(
                _relationship("rId1", "image", "../media/image1.png")
            ),
            "ppt/media/image1.png": b"image",
        }
    )

    inventory = inventory_office_visuals(content, PPTX_MIME)

    assert inventory.required == ()
    assert len(inventory.excluded) == 1
    assert inventory.excluded[0].reason == "hidden_owner"


def test_merge_creates_rendered_segment_when_visual_owner_has_no_native_text() -> None:
    locator = SourceLocator(LocatorKind.SLIDE, "Slide 1", 1, 1)
    native = CanonicalExtractionResult(
        segments=(),
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

    merged = merge_office_visuals(
        native,
        inventory,
        (RenderedVisualDescription(0, 1, "[Figure 1] A rendered flow diagram."),),
        PPTX_MIME,
    )

    assert len(merged.segments) == 1
    assert merged.segments[0].locator == locator
    assert merged.segments[0].text == "[Figure 1] A rendered flow diagram."
    assert merged.segments[0].modalities == (
        ContentModality.TEXT,
        ContentModality.VISUAL_DESCRIPTION,
    )
    assert merged.segments[0].provenance == (ExtractionProvenance.RENDERED,)
    assert merged.visual_coverage.status is VisualCoverageStatus.COMPLETE


def test_workbook_inventory_excludes_hidden_sheet_and_audits_external_image() -> None:
    workbook = (
        f'<workbook xmlns:r="{OFFICE_REL_NS}"><sheets>'
        '<sheet name="Visible" sheetId="1" r:id="rId1"/>'
        '<sheet name="Hidden" sheetId="2" state="hidden" r:id="rId2"/>'
        "</sheets></workbook>"
    )
    content = _archive(
        {
            "xl/workbook.xml": workbook,
            "xl/_rels/workbook.xml.rels": _relationships(
                _relationship("rId1", "worksheet", "worksheets/sheet1.xml"),
                _relationship("rId2", "worksheet", "worksheets/sheet2.xml"),
            ),
            "xl/worksheets/sheet1.xml": "<worksheet/>",
            "xl/worksheets/_rels/sheet1.xml.rels": _relationships(
                _relationship(
                    "rIdImage",
                    "image",
                    "https://example.invalid/image.png",
                    external=True,
                )
            ),
            "xl/worksheets/sheet2.xml": "<worksheet/>",
            "xl/worksheets/_rels/sheet2.xml.rels": _relationships(
                _relationship("rIdChart", "chart", "../charts/chart1.xml")
            ),
            "xl/charts/chart1.xml": "<chart/>",
        }
    )

    inventory = inventory_office_visuals(content, XLSX_MIME)

    assert inventory.required == ()
    assert len(inventory.excluded) == 1
    assert inventory.excluded[0].owner.label == "Hidden"
    assert len(inventory.unsupported) == 1
    assert inventory.unsupported[0].reason == "external_relationship"


def test_inventory_rejects_archive_traversal_path() -> None:
    content = _archive({"../ppt/slides/slide1.xml": "<slide/>"})

    with pytest.raises(TerminalDocumentError, match="office_archive_path_invalid"):
        inventory_office_visuals(content, PPTX_MIME)


def test_inventory_rejects_uncompressed_archive_over_limit() -> None:
    content = _archive({"ppt/slides/slide1.xml": '<p:sld xmlns:p="p"/>'})

    with pytest.raises(
        TerminalDocumentError,
        match="office_archive_size_limit_exceeded",
    ):
        inventory_office_visuals(
            content,
            PPTX_MIME,
            max_uncompressed_bytes=4,
        )


def test_inventory_rejects_visual_object_over_limit() -> None:
    content = _archive(
        {
            "ppt/slides/slide1.xml": '<p:sld xmlns:p="p"/>',
            "ppt/slides/_rels/slide1.xml.rels": _relationships(
                _relationship("rId1", "image", "../media/image1.png"),
                _relationship("rId2", "image", "../media/image2.png"),
            ),
            "ppt/media/image1.png": b"one",
            "ppt/media/image2.png": b"two",
        }
    )

    with pytest.raises(
        TerminalDocumentError,
        match="office_visual_object_limit_exceeded",
    ):
        inventory_office_visuals(content, PPTX_MIME, max_objects=1)


def test_inventory_rejects_content_units_over_limit() -> None:
    content = _archive(
        {
            "ppt/slides/slide1.xml": '<p:sld xmlns:p="p"/>',
            "ppt/slides/slide2.xml": '<p:sld xmlns:p="p"/>',
        }
    )

    with pytest.raises(
        TerminalDocumentError,
        match="office_content_unit_limit_exceeded",
    ):
        inventory_office_visuals(content, PPTX_MIME, max_content_units=1)


def test_inventory_rejects_malformed_or_entity_declaring_packages() -> None:
    with pytest.raises(TerminalDocumentError, match="office_package_invalid"):
        inventory_office_visuals(b"not-an-office-package", PPTX_MIME)

    content = _archive(
        {
            "ppt/slides/slide1.xml": (
                '<!DOCTYPE sld [<!ENTITY x "unsafe">]>'
                '<p:sld xmlns:p="p">&x;</p:sld>'
            )
        }
    )
    with pytest.raises(
        TerminalDocumentError,
        match="office_xml_declaration_unsupported",
    ):
        inventory_office_visuals(content, PPTX_MIME)


def test_merge_presentation_filters_hidden_slide_and_attaches_visual_to_owner() -> None:
    slide_1 = SourceLocator(LocatorKind.SLIDE, "Slide 1", 1, 1)
    slide_2 = SourceLocator(LocatorKind.SLIDE, "Slide 2", 2, 2)
    native = _native_result(
        CanonicalSegment(
            0,
            "Visible slide semantics.",
            slide_1,
            (ContentModality.TEXT,),
            (ExtractionProvenance.DIRECT,),
        ),
        CanonicalSegment(
            1,
            "Hidden slide semantics.",
            slide_2,
            (ContentModality.TEXT,),
            (ExtractionProvenance.DIRECT,),
        ),
    )
    inventory = OfficeVisualInventory(
        content_units=(
            OfficeContentUnit("ppt/slides/slide1.xml", slide_1, False),
            OfficeContentUnit("ppt/slides/slide2.xml", slide_2, True),
        ),
        required=(
            OfficeVisualObject("slide1#image", "image", slide_1, "ppt/media/image.png"),
        ),
        excluded=(),
        unsupported=(),
    )

    merged = merge_office_visuals(
        native,
        inventory,
        (RenderedVisualDescription(0, 1, "[Figure 1] A product screenshot."),),
        PPTX_MIME,
    )

    assert len(merged.segments) == 1
    assert "Hidden slide semantics" not in merged.segments[0].text
    assert "product screenshot" in merged.segments[0].text
    assert merged.segments[0].provenance == (
        ExtractionProvenance.DIRECT,
        ExtractionProvenance.RENDERED,
    )
    assert merged.visual_coverage.status is VisualCoverageStatus.COMPLETE


def test_merge_docx_discards_derivative_text_and_adds_rendered_page_locator() -> None:
    document = SourceLocator(LocatorKind.SECTION, "Document", 1, 1)
    native = _native_result(
        CanonicalSegment(
            0,
            "Authoritative native document semantics.",
            document,
            (ContentModality.TEXT,),
            (ExtractionProvenance.DIRECT,),
        )
    )
    inventory = OfficeVisualInventory(
        content_units=(OfficeContentUnit("word/document.xml", document, False),),
        required=(
            OfficeVisualObject("document#chart", "chart", document, "word/charts/chart1.xml"),
        ),
        excluded=(),
        unsupported=(),
    )

    merged = merge_office_visuals(
        native,
        inventory,
        (RenderedVisualDescription(0, 3, "[Figure 1] Revenue bars increase."),),
        DOCX_MIME,
    )

    assert [segment.locator.label for segment in merged.segments] == [
        "Document",
        "Rendered page 3",
    ]
    assert "Authoritative native" in merged.segments[0].text
    assert "Revenue bars" in merged.segments[1].text
    assert merged.segments[1].provenance == (ExtractionProvenance.RENDERED,)


def test_merge_fails_when_rendered_visual_does_not_cover_required_object() -> None:
    worksheet = SourceLocator(LocatorKind.WORKSHEET, "Summary", 1, 1)
    native = _native_result(
        CanonicalSegment(
            0,
            "Worksheet values and formulas.",
            worksheet,
            (ContentModality.TEXT,),
            (ExtractionProvenance.DIRECT,),
        )
    )
    inventory = OfficeVisualInventory(
        content_units=(OfficeContentUnit("xl/worksheets/sheet1.xml", worksheet, False),),
        required=(
            OfficeVisualObject("sheet1#chart", "chart", worksheet, "xl/charts/chart1.xml"),
        ),
        excluded=(),
        unsupported=(),
    )

    with pytest.raises(TerminalDocumentError, match="office_visual_coverage_incomplete"):
        merge_office_visuals(native, inventory, (), XLSX_MIME)


def _native_result(*segments: CanonicalSegment) -> CanonicalExtractionResult:
    return CanonicalExtractionResult(
        segments=segments,
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