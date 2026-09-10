from ingestion.chunking import chunk_pages
from ingestion.models import (
    ContentModality,
    ExtractionProvenance,
    LocatorKind,
    Page,
    SourceLocator,
    VisualCoverageStatus,
)


def test_chunk_pages_inherits_canonical_source_metadata() -> None:
    page = Page(
        number=3,
        text="Quarterly revenue increased after the product launch. " * 30,
        locator=SourceLocator(LocatorKind.SLIDE, "Slide 3", 3, 3),
        modalities=(ContentModality.TEXT, ContentModality.VISUAL_DESCRIPTION),
        provenance=(ExtractionProvenance.DIRECT, ExtractionProvenance.RENDERED),
        visual_coverage=VisualCoverageStatus.COMPLETE,
    )

    chunks = chunk_pages([page], max_tokens=40, overlap_tokens=5)

    assert len(chunks) > 1
    assert all(chunk.locator == page.locator for chunk in chunks)
    assert all(chunk.modalities == page.modalities for chunk in chunks)
    assert all(chunk.provenance == page.provenance for chunk in chunks)
    assert all(chunk.visual_coverage is VisualCoverageStatus.COMPLETE for chunk in chunks)


def test_chunk_pages_does_not_mix_source_unit_metadata() -> None:
    pages = [
        Page(
            number=1,
            text="First worksheet values and formulas. " * 20,
            locator=SourceLocator(LocatorKind.WORKSHEET, "Summary", 1, 1),
        ),
        Page(
            number=2,
            text="Second worksheet values and formulas. " * 20,
            locator=SourceLocator(LocatorKind.WORKSHEET, "Forecast", 2, 2),
        ),
    ]

    chunks = chunk_pages(pages, max_tokens=35, overlap_tokens=5)

    first_labels = {chunk.locator.label for chunk in chunks if chunk.page_number == 1}
    second_labels = {chunk.locator.label for chunk in chunks if chunk.page_number == 2}
    assert first_labels == {"Summary"}
    assert second_labels == {"Forecast"}


def test_chunk_pages_builds_page_locator_for_existing_extractors() -> None:
    chunk = chunk_pages([Page(number=4, text="A source page with extractable content.")])[0]

    assert chunk.locator == SourceLocator(LocatorKind.PAGE, "Page 4", 4, 4)