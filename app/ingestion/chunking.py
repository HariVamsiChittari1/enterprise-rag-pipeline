"""Token-based page-aware chunking with configurable window and overlap."""

from __future__ import annotations

import re

import tiktoken

from ingestion.errors import TerminalDocumentError
from ingestion.models import Chunk, LocatorKind, Page, SourceLocator

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
