"""Offline phrase projection for Speech transcribe API 2025-10-15 responses.

Pure, I/O-free projection of a fast transcription response body into ordered timed
segments. ``parse_audio_transcription`` verifies a caller-supplied duration;
``build_transcript`` instead treats the Speech response as authoritative for duration
and omits channel count (the default transcribe response does not report the source
channel count). Combined text, confidence, and speaker/channel details are not projected.
Failure after submission must not be read as permission to resubmit.
"""

from __future__ import annotations

import json
import math
from typing import Any

from .errors import TerminalDocumentError
from .models import AudioMetadata, AudioTranscriptSegment

# Provider identity pinned by AudioMetadata; reused for response-derived construction.
_TRANSCRIPTION_MODE = "fast"
_API_VERSION = "2025-10-15"
_MAX_DURATION_MS = 1_800_000


def parse_audio_transcription(
    body: bytes,
    audio: AudioMetadata,
    *,
    max_response_bytes: int,
    max_phrases: int,
    max_words: int,
) -> tuple[AudioTranscriptSegment, ...]:
    """Project timed phrases against a caller-known duration, without I/O.

    Limits are caller-owned, not production defaults. The byte cap precedes JSON
    decoding; collection caps follow materialization and do not prove worker safety.
    """
    if not isinstance(audio, AudioMetadata):
        raise ValueError("audio transcription requires validated media metadata")
    _validate_limits(max_response_bytes, max_phrases, max_words)
    response = _decode_response(body, max_response_bytes)
    duration = response.get("durationMilliseconds")
    if type(duration) is not int or duration != audio.duration_ms:
        raise TerminalDocumentError("audio_response_duration_mismatch")
    return _project_phrases(response, audio, max_phrases, max_words)


def build_transcript(
    body: bytes,
    *,
    locale: str,
    profile_version: str,
    source_version: str,
    source_content_hash: str,
    max_response_bytes: int,
    max_phrases: int,
    max_words: int,
) -> tuple[AudioMetadata, tuple[AudioTranscriptSegment, ...]]:
    """Build validated audio metadata and segments from a fast transcription response.

    Duration comes from the Speech response (the authority on the audio it transcribed).
    Channel count is omitted: the default transcribe response does not report the source
    channel count. Returns the metadata plus ordered segments for chunking and publishing.
    """
    _validate_limits(max_response_bytes, max_phrases, max_words)
    response = _decode_response(body, max_response_bytes)
    duration = response.get("durationMilliseconds")
    if type(duration) is not int or not 0 < duration <= _MAX_DURATION_MS:
        raise TerminalDocumentError("audio_response_duration_invalid")
    audio = AudioMetadata(
        duration, None, locale, _TRANSCRIPTION_MODE, _API_VERSION,
        profile_version, source_version, source_content_hash,
    )
    segments = _project_phrases(response, audio, max_phrases, max_words)
    return audio, segments


_BATCH_TRANSCRIPTION_MODE = "batch"
_BATCH_API_VERSION = "2024-11-15"
_TICKS_PER_MS = 10_000  # one tick is 100 nanoseconds


def build_transcript_from_batch(
    body: bytes,
    *,
    locale: str,
    profile_version: str,
    source_version: str,
    source_content_hash: str,
    max_response_bytes: int,
    max_phrases: int,
    max_words: int,
) -> tuple[AudioMetadata, tuple[AudioTranscriptSegment, ...]]:
    """Build validated metadata and segments from a batch transcription result file.

    The batch result schema (``recognizedPhrases`` + ``nBest``) is projected onto the fast
    phrase shape and validated by the shared projector, so both providers share one strictness
    path. Duration comes from the response; channel count is omitted.
    """
    _validate_limits(max_response_bytes, max_phrases, max_words)
    response = _decode_response(body, max_response_bytes)
    duration = response.get("durationMilliseconds")
    if type(duration) is not int or not 0 < duration <= _MAX_DURATION_MS:
        raise TerminalDocumentError("audio_response_duration_invalid")
    audio = AudioMetadata(
        duration, None, locale, _BATCH_TRANSCRIPTION_MODE, _BATCH_API_VERSION,
        profile_version, source_version, source_content_hash,
    )
    projected = _batch_result_to_fast_shape(response, locale, duration)
    segments = _project_phrases(projected, audio, max_phrases, max_words)
    return audio, segments


def _batch_result_to_fast_shape(response: dict[str, Any], locale: str, duration_ms: int) -> dict[str, Any]:
    """Map a batch result's recognizedPhrases onto the fast phrase shape (Success phrases only)."""
    recognized = response.get("recognizedPhrases")
    if not isinstance(recognized, list):
        raise TerminalDocumentError("audio_response_invalid_shape")
    phrases: list[dict[str, Any]] = []
    for phrase in recognized:
        if not isinstance(phrase, dict):
            raise TerminalDocumentError("audio_response_invalid_phrase")
        if phrase.get("recognitionStatus") != "Success":
            continue
        offset_ticks = phrase.get("offsetInTicks")
        duration_ticks = phrase.get("durationInTicks")
        n_best = phrase.get("nBest")
        if (
            not _is_number(offset_ticks) or not _is_number(duration_ticks)
            or not isinstance(n_best, list) or not n_best or not isinstance(n_best[0], dict)
        ):
            raise TerminalDocumentError("audio_response_invalid_phrase")
        best = n_best[0]
        text = best.get("display") or best.get("lexical")
        words = best.get("displayWords") or best.get("words") or []
        phrases.append({
            "locale": locale,
            "offsetMilliseconds": int(round(offset_ticks / _TICKS_PER_MS)),
            "durationMilliseconds": int(round(duration_ticks / _TICKS_PER_MS)),
            "text": text if isinstance(text, str) else "",
            "words": words if isinstance(words, list) else [],
        })
    return {"durationMilliseconds": duration_ms, "phrases": phrases}


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_limits(max_response_bytes: int, max_phrases: int, max_words: int) -> None:
    if any(type(limit) is not int or limit <= 0 for limit in (
        max_response_bytes, max_phrases, max_words,
    )):
        raise ValueError("audio transcription limits must be positive integers")


def _decode_response(body: bytes, max_response_bytes: int) -> dict[str, Any]:
    if not isinstance(body, bytes):
        raise ValueError("audio transcription response must be bytes")
    if len(body) > max_response_bytes:
        raise TerminalDocumentError("audio_response_limit_exceeded")
    try:
        response = json.loads(
            body.decode("utf-8"), object_pairs_hook=_unique_object,
            parse_constant=_reject_constant, parse_float=_finite_float,
        )
    except (ValueError, RecursionError):
        raise TerminalDocumentError("audio_response_invalid_json") from None
    if not isinstance(response, dict):
        raise TerminalDocumentError("audio_response_invalid_shape")
    return response


def _project_phrases(
    response: dict[str, Any], audio: AudioMetadata, max_phrases: int, max_words: int,
) -> tuple[AudioTranscriptSegment, ...]:
    phrases = response.get("phrases")
    if not isinstance(phrases, list):
        raise TerminalDocumentError("audio_response_invalid_shape")
    if len(phrases) > max_phrases:
        raise TerminalDocumentError("audio_response_limit_exceeded")
    if not phrases:
        raise TerminalDocumentError("audio_response_no_speech")

    segments: list[AudioTranscriptSegment] = []
    word_count = 0
    for ordinal, phrase in enumerate(phrases):
        if not isinstance(phrase, dict):
            raise TerminalDocumentError("audio_response_invalid_phrase")
        locale = phrase.get("locale")
        if (
            not isinstance(locale, str) or not locale.isascii()
            or locale.casefold() != audio.locale.casefold()
        ):
            raise TerminalDocumentError("audio_response_locale_mismatch")
        words = phrase.get("words", [])
        if not isinstance(words, list) or any(not isinstance(word, dict) for word in words):
            raise TerminalDocumentError("audio_response_invalid_phrase")
        word_count += len(words)
        if word_count > max_words:
            raise TerminalDocumentError("audio_response_limit_exceeded")
        offset = phrase.get("offsetMilliseconds")
        length = phrase.get("durationMilliseconds")
        text = phrase.get("text")
        if (
            type(offset) is not int or type(length) is not int
            or offset < 0 or length <= 0 or offset + length > audio.duration_ms
            or not isinstance(text, str) or not text.strip()
        ):
            raise TerminalDocumentError("audio_response_invalid_phrase")
        try:
            text.encode("utf-8", errors="strict")
            segment = AudioTranscriptSegment(ordinal, text, offset, offset + length)
        except ValueError:
            raise TerminalDocumentError("audio_response_invalid_phrase") from None
        segments.append(segment)
    return tuple(segments)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError("nonfinite JSON number")


def _finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("nonfinite JSON number")
    return number