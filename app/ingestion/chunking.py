"""Token-based page-aware chunking with configurable window and overlap."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
import re

import tiktoken

from ingestion.errors import TerminalDocumentError
from ingestion.models import (
    AudioMetadata,
    AudioTranscriptSegment,
    Chunk,
    ContentModality,
    ExtractionProvenance,
    LocatorKind,
    Page,
    SourceLocator,
    VisualCoverageStatus,
)

DEFAULT_MAX_TOKENS = 800
DEFAULT_OVERLAP_TOKENS = 100
DEFAULT_TOKENIZER = "cl100k_base"
MAX_CHUNKS_PER_DOCUMENT = 2_000
MIN_MERGE_TOKENS = 50

_DI_MARKER_RE = re.compile(
    r"<!--\s*(?:PageHeader|PageFooter|PageNumber|PageBreak).*?-->",
    re.DOTALL,
)


def chunk_pages(
    pages: list[Page],
    max_tokens: int = DEFAULT_MAX_TOKENS,
    overlap_tokens: int = DEFAULT_OVERLAP_TOKENS,
    tokenizer_name: str = DEFAULT_TOKENIZER,
) -> list[Chunk]:
    """Split extracted pages into token-bounded chunks with overlap."""
    encoding = tiktoken.get_encoding(tokenizer_name)
    chunks: list[Chunk] = []
    step = max_tokens - overlap_tokens

    for page in pages:
        locator = page.locator or SourceLocator(
            kind=LocatorKind.PAGE,
            label=f"Page {page.number}",
            ordinal_start=page.number,
            ordinal_end=page.number,
        )
        segments = _page_segments(page.text)
        merged = _merge_small_segments(segments, encoding)
        for segment in merged:
            tokens = encoding.encode(segment)
            for offset in range(0, len(tokens), step):
                token_slice = tokens[offset : offset + max_tokens]
                if not token_slice:
                    break
                text = encoding.decode(token_slice).strip()
                if text:
                    chunks.append(
                        Chunk(
                            ordinal=len(chunks),
                            page_number=page.number,
                            content=text,
                            locator=locator,
                            modalities=page.modalities,
                            provenance=page.provenance,
                            visual_coverage=page.visual_coverage,
                        )
                    )
                    if len(chunks) > MAX_CHUNKS_PER_DOCUMENT:
                        raise TerminalDocumentError("chunking_limit_exceeded")
                if offset + max_tokens >= len(tokens):
                    break

    if not chunks:
        raise TerminalDocumentError("chunking_no_output")
    return chunks


def chunk_audio_segments(
    segments: tuple[AudioTranscriptSegment, ...],
    audio: AudioMetadata,
    *,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    overlap_tokens: int = DEFAULT_OVERLAP_TOKENS,
    tokenizer_name: str = DEFAULT_TOKENIZER,
) -> list[Chunk]:
    """Chunk a complete transcript in source order with enclosing phrase timings.

    Phrase text is joined with newlines. Whitespace-only windows are omitted;
    other text is preserved. Overlap may shrink to preserve UTF-8 and progress.
    Locator ordinals and page_number identify phrases, not physical pages.
    """
    if type(max_tokens) is not int or not 1 <= max_tokens <= DEFAULT_MAX_TOKENS:
        raise ValueError("audio chunk token limit is invalid")
    if type(overlap_tokens) is not int or not 0 <= overlap_tokens <= DEFAULT_OVERLAP_TOKENS or overlap_tokens >= max_tokens:
        raise ValueError("audio chunk overlap is invalid")
    if not isinstance(audio, AudioMetadata) or not isinstance(segments, tuple):
        raise ValueError("audio chunking requires validated metadata and a segment tuple")
    if any(
        not isinstance(segment, AudioTranscriptSegment) or segment.ordinal != ordinal
        or segment.end_ms > audio.duration_ms
        for ordinal, segment in enumerate(segments)
    ):
        raise ValueError("audio chunking requires ordered recording-relative segments")
    if not segments:
        raise TerminalDocumentError("chunking_no_output")
    try:
        phrase_bytes = [segment.text.encode("utf-8") for segment in segments]
    except UnicodeEncodeError:
        raise TerminalDocumentError("chunking_invalid_text") from None
    source = b"\n".join(phrase_bytes)
    phrase_starts: list[int] = []
    phrase_ends: list[int] = []
    position = 0
    for phrase in phrase_bytes:
        phrase_starts.append(position)
        position += len(phrase)
        phrase_ends.append(position)
        position += 1
    encoding = tiktoken.get_encoding(tokenizer_name)
    tokens = encoding.encode_ordinary(source.decode("utf-8"))
    offsets = [0]
    for token in tokens:
        offsets.append(offsets[-1] + len(encoding.decode_single_token_bytes(token)))
    boundaries = [
        index for index, offset in enumerate(offsets)
        if offset == len(source) or source[offset] & 0xC0 != 0x80
    ]
    chunks: list[Chunk] = []
    start = 0
    previous_end = 0
    while start < len(tokens):
        boundary_index = bisect_right(boundaries, start + max_tokens) - 1
        end = boundaries[boundary_index]
        text = source[offsets[start]:offsets[end]].decode("utf-8")
        while end > start and len(encoding.encode_ordinary(text)) > max_tokens:
            boundary_index -= 1
            end = boundaries[boundary_index]
            text = source[offsets[start]:offsets[end]].decode("utf-8")
        if end <= previous_end:
            if start < previous_end:
                start = previous_end
                continue
            raise TerminalDocumentError("chunking_token_boundary")
        if text.strip():
            first = bisect_right(phrase_ends, offsets[start])
            last = bisect_left(phrase_starts, offsets[end])
            contributors = segments[first:last]
            start_ms = min(segment.start_ms for segment in contributors)
            end_ms = max(segment.end_ms for segment in contributors)
            if len(chunks) >= MAX_CHUNKS_PER_DOCUMENT:
                raise TerminalDocumentError("chunking_limit_exceeded")
            chunks.append(Chunk(
                ordinal=len(chunks), page_number=first + 1, content=text,
                locator=SourceLocator(
                    LocatorKind.TIME, f"{start_ms}-{end_ms} ms", first + 1, last,
                    start_ms=start_ms, end_ms=end_ms,
                ),
                modalities=(ContentModality.TEXT, ContentModality.AUDIO_TRANSCRIPT),
                provenance=(ExtractionProvenance.TRANSCRIBED,),
                visual_coverage=VisualCoverageStatus.NOT_REQUIRED,
            ))
        if end == len(tokens):
            break
        next_start = end
        if text.strip() and overlap_tokens:
            overlap_index = bisect_left(boundaries, max(start + 1, end - overlap_tokens))
            next_start = boundaries[overlap_index]
            while next_start < end and len(encoding.encode_ordinary(
                source[offsets[next_start]:offsets[end]].decode("utf-8"),
            )) > overlap_tokens:
                overlap_index += 1
                next_start = boundaries[overlap_index]
        previous_end = end
        start = next_start
    if not chunks:
        raise TerminalDocumentError("chunking_no_output")
    return chunks


def token_count(text: str, tokenizer_name: str = DEFAULT_TOKENIZER) -> int:
    encoding = tiktoken.get_encoding(tokenizer_name)
    return len(encoding.encode(text))


def _merge_small_segments(segments: list[str], encoding: tiktoken.Encoding) -> list[str]:
    """Merge consecutive segments until each reaches MIN_MERGE_TOKENS."""
    merged: list[str] = []
    buffer: list[str] = []
    buffer_tokens = 0
    for segment in segments:
        seg_tokens = len(encoding.encode(segment))
        buffer.append(segment)
        buffer_tokens += seg_tokens
        if buffer_tokens >= MIN_MERGE_TOKENS:
            merged.append("\n\n".join(buffer))
            buffer = []
            buffer_tokens = 0
    if buffer:
        if merged:
            merged[-1] = merged[-1] + "\n\n" + "\n\n".join(buffer)
        else:
            merged.append("\n\n".join(buffer))
    return merged


def _page_segments(text: str) -> list[str]:
    """Split page text into segments by headings and paragraph breaks, filtering DI markers."""
    cleaned = _DI_MARKER_RE.sub("", text)
    normalized = cleaned.replace("\r\n", "\n").strip()
    if not normalized:
        return []
    segments: list[str] = []
    buffer: list[str] = []
    for raw_line in normalized.splitlines():
        line = raw_line.strip()
        if not line:
            if buffer:
                segments.append(" ".join(buffer).strip())
                buffer = []
            continue
        if line.startswith("#") and buffer:
            segments.append(" ".join(buffer).strip())
            buffer = [line]
            continue
        if re.match(r"^(?:[-*]|\d+\.)\s+", line) and buffer and buffer[-1].startswith("#"):
            segments.append(" ".join(buffer).strip())
            buffer = [line]
            continue
        buffer.append(line)
    if buffer:
        segments.append(" ".join(buffer).strip())
    return [s for s in segments if s]
