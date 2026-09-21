from dataclasses import replace

import pytest
import tiktoken

from ingestion.chunking import chunk_audio_segments, chunk_pages
from ingestion.errors import TerminalDocumentError
from ingestion.models import (
    AudioMetadata,
    AudioTranscriptSegment,
    ContentModality,
    ExtractionProvenance,
    LocatorKind,
    Page,
    SourceLocator,
    VisualCoverageStatus,
)


def audio_metadata() -> AudioMetadata:
    return AudioMetadata(3000, 1, "en-US", "fast", "2025-10-15", "1", "etag", "a" * 64)


def test_chunk_audio_preserves_source_order_and_enclosing_phrase_times() -> None:
    audio = audio_metadata()
    segments = (
        AudioTranscriptSegment(0, "First phrase", 1000, 2000),
        AudioTranscriptSegment(1, "Second phrase", 100, 2500),
        AudioTranscriptSegment(2, "Third phrase", 500, 1500),
    )
    chunks = chunk_audio_segments(segments, audio)

    assert len(chunks) == 1
    chunk = chunks[0]
    assert chunk.content == "First phrase\nSecond phrase\nThird phrase"
    assert chunk.locator == SourceLocator(LocatorKind.TIME, "100-2500 ms", 1, 3, 100, 2500)
    assert chunk.ordinal == 0
    assert chunk.page_number == 1
    assert chunk.modalities == (ContentModality.TEXT, ContentModality.AUDIO_TRANSCRIPT)
    assert chunk.provenance == (ExtractionProvenance.TRANSCRIBED,)
    assert chunk.visual_coverage is VisualCoverageStatus.NOT_REQUIRED


@pytest.mark.parametrize("text", [
    "A long phrase with many words. " * 80,
    "\u4f60\u597d\u4e16\u754c \U0001f642 caf\u00e9 e\u0301 " * 80,
    '<|endoftext|> # heading\r\n<!-- PageHeader --> "quoted"\\path ' * 40,
])
def test_chunk_audio_split_phrase_preserves_text_and_full_timing(text: str) -> None:
    segment = AudioTranscriptSegment(0, text, 100, 2700)
    chunks = chunk_audio_segments((segment,), audio_metadata(), max_tokens=12, overlap_tokens=0)
    encoding = tiktoken.get_encoding("cl100k_base")

    assert len(chunks) > 1
    assert "".join(chunk.content for chunk in chunks) == text
    assert [chunk.ordinal for chunk in chunks] == list(range(len(chunks)))
    assert all(len(encoding.encode_ordinary(chunk.content)) <= 12 for chunk in chunks)
    assert all(chunk.locator == SourceLocator(LocatorKind.TIME, "100-2700 ms", 1, 1, 100, 2700) for chunk in chunks)
    assert all("\ufffd" not in chunk.content for chunk in chunks)


@pytest.mark.parametrize("overlap", [0, 3, 10])
def test_chunk_audio_overlap_maps_all_contributing_phrases(overlap: int) -> None:
    segments = (
        AudioTranscriptSegment(0, "alpha bravo charlie delta echo foxtrot golf hotel india juliet", 900, 2400),
        AudioTranscriptSegment(1, "kilo lima mike november oscar papa quebec romeo sierra tango", 100, 1600),
        AudioTranscriptSegment(2, "uniform victor whiskey xray yankee zulu amber bronze copper diamond", 700, 2800),
    )
    source = "\n".join(segment.text for segment in segments)
    spans = []
    position = 0
    for segment in segments:
        spans.append((position, position + len(segment.text), segment))
        position += len(segment.text) + 1
    chunks = chunk_audio_segments(segments, audio_metadata(), max_tokens=12, overlap_tokens=overlap)
    encoding = tiktoken.get_encoding("cl100k_base")
    previous_start = -1
    previous_end = 0
    saw_overlap = False
    saw_cross_phrase = False
    for chunk in chunks:
        start = source.index(chunk.content, previous_start + 1)
        end = start + len(chunk.content)
        assert start <= previous_end
        assert end > previous_end
        assert len(encoding.encode_ordinary(chunk.content)) <= 12
        assert len(encoding.encode_ordinary(source[start:previous_end])) <= overlap
        contributors = [segment for lower, upper, segment in spans if lower < end and upper > start]
        assert chunk.locator.start_ms == min(segment.start_ms for segment in contributors)
        assert chunk.locator.end_ms == max(segment.end_ms for segment in contributors)
        assert chunk.locator.ordinal_start == contributors[0].ordinal + 1
        assert chunk.locator.ordinal_end == contributors[-1].ordinal + 1
        saw_overlap |= start < previous_end
        saw_cross_phrase |= len(contributors) > 1
        previous_start, previous_end = start, end
    assert previous_end == len(source)
    assert saw_overlap == bool(overlap)
    assert saw_cross_phrase


@pytest.mark.parametrize("max_tokens,overlap_tokens", [(4, 3), (5, 4), (8, 7)])
def test_chunk_audio_unicode_overlap_reduces_safely_and_makes_progress(
    max_tokens: int, overlap_tokens: int,
) -> None:
    text = "alpha\U0001f642bravo\U0001f643charlie\U0001f680delta\u4e16echo\u754cfoxtrot e\u0301 golf"
    chunks = chunk_audio_segments(
        (AudioTranscriptSegment(0, text, 100, 2700),), audio_metadata(),
        max_tokens=max_tokens, overlap_tokens=overlap_tokens,
    )
    encoding = tiktoken.get_encoding("cl100k_base")
    previous_start = -1
    previous_end = 0
    reduced_overlap = False
    for chunk in chunks:
        start = text.index(chunk.content, previous_start + 1)
        end = start + len(chunk.content)
        assert start <= previous_end < end
        assert len(encoding.encode_ordinary(chunk.content)) <= max_tokens
        actual_overlap = len(encoding.encode_ordinary(text[start:previous_end]))
        assert actual_overlap <= overlap_tokens
        reduced_overlap |= previous_end > 0 and actual_overlap < overlap_tokens
        assert "\ufffd" not in chunk.content
        assert (chunk.locator.start_ms, chunk.locator.end_ms) == (100, 2700)
        previous_start, previous_end = start, end
    assert previous_end == len(text)
    assert reduced_overlap


@pytest.mark.parametrize("max_tokens,overlap_tokens", [
    (True, 0), (0, 0), (-1, 0), (801, 0), (4.0, 0), ("4", 0),
    (10, True), (10, -1), (10, 10), (10, 11), (800, 101), (10, 1.0), (10, None),
])
def test_chunk_audio_rejects_invalid_limits(max_tokens: int, overlap_tokens: int) -> None:
    with pytest.raises(ValueError, match="audio chunk"):
        chunk_audio_segments(
            (AudioTranscriptSegment(0, "hello", 0, 100),), audio_metadata(),
            max_tokens=max_tokens, overlap_tokens=overlap_tokens,
        )


@pytest.mark.parametrize("case", ["list", "invalid_segment", "gap", "duplicate", "reversed", "duration", "metadata"])
def test_chunk_audio_rejects_invalid_transcript_contract(case: str) -> None:
    segments = (AudioTranscriptSegment(0, "first", 0, 100), AudioTranscriptSegment(1, "second", 100, 200))
    audio = audio_metadata()
    if case == "list":
        segments = list(segments)
    elif case == "invalid_segment":
        segments = ({"text": "invalid"},)
    elif case == "gap":
        segments = (segments[0], replace(segments[1], ordinal=2))
    elif case == "duplicate":
        segments = (segments[0], segments[0])
    elif case == "reversed":
        segments = tuple(reversed(segments))
    elif case == "duration":
        audio = replace(audio, duration_ms=150)
    else:
        audio = None
    with pytest.raises(ValueError, match="audio chunking"):
        chunk_audio_segments(segments, audio)


def test_chunk_audio_empty_transcript_is_not_retrievable() -> None:
    with pytest.raises(TerminalDocumentError, match="^chunking_no_output$"):
        chunk_audio_segments((), audio_metadata())


def test_chunk_audio_rejects_surrogates_without_exposing_text() -> None:
    with pytest.raises(TerminalDocumentError, match="^chunking_invalid_text$"):
        chunk_audio_segments((AudioTranscriptSegment(0, "private\ud800", 0, 100),), audio_metadata())


def test_chunk_audio_fails_when_budget_cannot_hold_unicode_character() -> None:
    with pytest.raises(TerminalDocumentError, match="^chunking_token_boundary$"):
        chunk_audio_segments((AudioTranscriptSegment(0, "\U0001f642", 0, 100),), audio_metadata(), max_tokens=1, overlap_tokens=0)


@pytest.mark.parametrize("count", [800, 801])
def test_chunk_audio_enforces_default_token_ceiling(count: int) -> None:
    text = " hello" * count
    chunks = chunk_audio_segments((AudioTranscriptSegment(0, text, 0, 100),), audio_metadata())
    encoding = tiktoken.get_encoding("cl100k_base")
    assert len(chunks) == (1 if count == 800 else 2)
    assert len(encoding.encode_ordinary(chunks[0].content)) == 800
    if count == 801:
        assert len(encoding.encode_ordinary(chunks[1].content)) == 101


@pytest.mark.parametrize("count", [2000, 2001])
def test_chunk_audio_enforces_exact_chunk_ceiling(count: int) -> None:
    segment = AudioTranscriptSegment(0, " hello" * count, 0, 100)
    if count == 2001:
        with pytest.raises(TerminalDocumentError, match="^chunking_limit_exceeded$"):
            chunk_audio_segments((segment,), audio_metadata(), max_tokens=1, overlap_tokens=0)
    else:
        chunks = chunk_audio_segments((segment,), audio_metadata(), max_tokens=1, overlap_tokens=0)
        assert len(chunks) == 2000
        assert chunks[-1].ordinal == 1999


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