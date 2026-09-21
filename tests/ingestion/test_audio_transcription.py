import json
import traceback
from dataclasses import replace
from typing import Any

import pytest

from ingestion.audio_transcription import parse_audio_transcription
from ingestion.chunking import chunk_audio_segments
from ingestion.errors import TerminalDocumentError
from ingestion.models import AudioMetadata, AudioTranscriptSegment


def audio_metadata() -> AudioMetadata:
    return AudioMetadata(3000, 1, "en-US", "fast", "2025-10-15", "1", "etag", "a" * 64)


def phrase(**overrides: Any) -> dict[str, Any]:
    return {
        "offsetMilliseconds": 100, "durationMilliseconds": 500,
        "text": "Synthetic phrase.", "locale": "en-US", **overrides,
    }


def body(phrases: Any, **overrides: Any) -> bytes:
    return json.dumps({
        "durationMilliseconds": 3000, "phrases": phrases, **overrides,
    }).encode("utf-8")


def parse(payload: bytes, **limits: int) -> tuple[AudioTranscriptSegment, ...]:
    return parse_audio_transcription(
        payload, audio_metadata(),
        **{"max_response_bytes": 100_000, "max_phrases": 10, "max_words": 20, **limits},
    )


@pytest.mark.parametrize("mode,confidence", [("fast", 0.8), ("enhanced", 0)])
def test_parse_preserves_phrase_order_text_and_overlapping_times(mode: str, confidence: float) -> None:
    payload = body([
        phrase(offsetMilliseconds=1000, durationMilliseconds=1500, confidence=confidence),
        phrase(offsetMilliseconds=100, durationMilliseconds=2000, locale="en-us", text="  Second.\n"),
    ], combinedPhrases=[{"text": "Do not replace source phrases."}])
    segments = parse_audio_transcription(
        payload, replace(audio_metadata(), mode=mode),
        max_response_bytes=len(payload), max_phrases=2, max_words=1,
    )
    assert segments == (
        AudioTranscriptSegment(0, "Synthetic phrase.", 1000, 2500),
        AudioTranscriptSegment(1, "  Second.\n", 100, 2100),
    )


def test_parse_accepts_exact_media_boundary_and_unicode() -> None:
    assert parse(body([phrase(offsetMilliseconds=0, durationMilliseconds=3000, text="caf\u00e9 \U0001f642")])) == (
        AudioTranscriptSegment(0, "caf\u00e9 \U0001f642", 0, 3000),
    )


@pytest.mark.parametrize("payload", [
    b"", b"{", b"\xff", b'{} {}', b'{"secret": NaN}', b'{"secret": Infinity}',
    b'{"secret": -Infinity}', b'{"secret": 1e999}',
    b'{"phrases": [], "phrases": []}', b'{"nested": {"key": 1, "key": 2}}',
])
def test_parse_rejects_invalid_json_safely(payload: bytes) -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_invalid_json$") as caught:
        parse(payload)
    assert caught.value.__suppress_context__


def test_parse_rejects_deeply_nested_nonobject_response() -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_invalid_(json|shape)$"):
        parse(b"[" * 2000 + b"0" + b"]" * 2000)


@pytest.mark.parametrize("payload", [b"null", b"true", b"1", b'"value"', b"[]"])
def test_parse_rejects_nonobject_response(payload: bytes) -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_invalid_shape$"):
        parse(payload)


@pytest.mark.parametrize("duration", [None, True, "3000", 3000.0, 0, -1, 2999, 3001])
def test_parse_rejects_duration_mismatch(duration: Any) -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_duration_mismatch$"):
        parse(body([phrase()], durationMilliseconds=duration))


@pytest.mark.parametrize("phrases", [None, {}, "text", 1, True])
def test_parse_rejects_invalid_phrase_collection(phrases: Any) -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_invalid_shape$"):
        parse(body(phrases))


def test_parse_does_not_fall_back_to_combined_text() -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_no_speech$"):
        parse(body([], combinedPhrases=[{"text": "No timestamped evidence."}]))


@pytest.mark.parametrize("field,value", [
    ("offsetMilliseconds", None), ("offsetMilliseconds", True), ("offsetMilliseconds", 0.5),
    ("offsetMilliseconds", "100"), ("offsetMilliseconds", -1), ("offsetMilliseconds", 2501),
    ("durationMilliseconds", None), ("durationMilliseconds", True), ("durationMilliseconds", 1.0),
    ("durationMilliseconds", "500"), ("durationMilliseconds", 0), ("durationMilliseconds", -1),
    ("text", None), ("text", 1), ("text", ""), ("text", " \n"), ("text", "\ud800"),
    ("words", None), ("words", {}), ("words", ["word"]),
])
def test_parse_rejects_invalid_phrase_fields(field: str, value: Any) -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_invalid_phrase$"):
        parse(body([phrase(**{field: value})]))


@pytest.mark.parametrize("value", [None, 1, "en-GB", "fr-FR", " en-US", "en", "", "en-U\u017f"])
def test_parse_rejects_mismatching_locale(value: Any) -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_locale_mismatch$"):
        parse(body([phrase(locale=value)]))


@pytest.mark.parametrize("value", [None, [], "phrase", 1])
def test_parse_rejects_nonobject_phrase(value: Any) -> None:
    with pytest.raises(TerminalDocumentError, match="^audio_response_invalid_phrase$"):
        parse(body([value]))


def test_parse_response_byte_limit_precedes_json_decode() -> None:
    payload = body([phrase()])
    assert len(parse(payload, max_response_bytes=len(payload))) == 1
    with pytest.raises(TerminalDocumentError, match="^audio_response_limit_exceeded$"):
        parse(payload, max_response_bytes=len(payload) - 1)
    with pytest.raises(TerminalDocumentError, match="^audio_response_limit_exceeded$"):
        parse(b"{{", max_response_bytes=1)


def test_parse_phrase_and_aggregate_word_boundaries() -> None:
    payload = body([phrase(words=[{"text": "first"}]), phrase(words=[{"text": "second"}])])
    assert len(parse(payload, max_phrases=2, max_words=2)) == 2
    for limits in ({"max_phrases": 1}, {"max_words": 1}):
        with pytest.raises(TerminalDocumentError, match="^audio_response_limit_exceeded$"):
            parse(payload, **limits)


@pytest.mark.parametrize("name", ["max_response_bytes", "max_phrases", "max_words"])
@pytest.mark.parametrize("value", [0, -1, True, 1.0, "1", None])
def test_parse_rejects_invalid_limits(name: str, value: Any) -> None:
    with pytest.raises(ValueError, match="^audio transcription limits"):
        parse(body([phrase()]), **{name: value})


def test_parse_suppresses_source_text_in_formatted_failure() -> None:
    marker = "SYNTHETIC_PRIVATE_SENTINEL"
    with pytest.raises(TerminalDocumentError) as caught:
        parse(body([phrase(text=marker + "\ud800")]))
    assert marker not in "".join(traceback.format_exception(caught.value))


def test_parsed_phrases_feed_existing_timed_chunker() -> None:
    segments = parse(body([
        phrase(text="Later first.", offsetMilliseconds=1000, durationMilliseconds=1000),
        phrase(text="Earlier second.", offsetMilliseconds=100, durationMilliseconds=1000),
    ]))
    chunks = chunk_audio_segments(segments, audio_metadata())
    assert len(chunks) == 1
    assert chunks[0].content == "Later first.\nEarlier second."
    assert (chunks[0].locator.start_ms, chunks[0].locator.end_ms) == (100, 2000)


@pytest.mark.parametrize("field", ["offsetMilliseconds", "durationMilliseconds", "text", "locale"])
def test_parse_rejects_missing_phrase_fields(field: str) -> None:
    incomplete = phrase()
    del incomplete[field]
    with pytest.raises(TerminalDocumentError, match="^audio_response_(invalid_phrase|locale_mismatch)$"):
        parse(body([incomplete]))


@pytest.mark.parametrize("field", ["durationMilliseconds", "phrases"])
def test_parse_rejects_missing_result_fields(field: str) -> None:
    incomplete = {"durationMilliseconds": 3000, "phrases": [phrase()]}
    del incomplete[field]
    with pytest.raises(TerminalDocumentError, match="^audio_response_(duration_mismatch|invalid_shape)$"):
        parse(json.dumps(incomplete).encode("utf-8"))


@pytest.mark.parametrize("payload", [None, "{}", {}, bytearray(b"{}")])
def test_parse_requires_bytes(payload: Any) -> None:
    with pytest.raises(ValueError, match="^audio transcription response must be bytes$"):
        parse(payload)


def test_parse_requires_validated_metadata() -> None:
    with pytest.raises(ValueError, match="^audio transcription requires validated media metadata$"):
        parse_audio_transcription(b"{}", None, max_response_bytes=2, max_phrases=1, max_words=1)