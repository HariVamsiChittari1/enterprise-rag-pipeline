import json
from typing import Any

import pytest

from ingestion.audio_transcription import build_transcript
from ingestion.errors import TerminalDocumentError
from ingestion.models import AudioMetadata, AudioTranscriptSegment

_IDENTITY = dict(
    locale="en-US", profile_version="speech_fast",
    source_version="etag-1", source_content_hash="a" * 64,
)
_LIMITS = dict(max_response_bytes=100_000, max_phrases=10, max_words=50)


def body(phrases: Any, **overrides: Any) -> bytes:
    return json.dumps({"durationMilliseconds": 3000, "phrases": phrases, **overrides}).encode("utf-8")


def phrase(**overrides: Any) -> dict[str, Any]:
    return {
        "offsetMilliseconds": 100, "durationMilliseconds": 500,
        "text": "Synthetic phrase.", "locale": "en-US", **overrides,
    }


def test_build_transcript_derives_duration_from_response_and_omits_channel_count() -> None:
    audio, segments = build_transcript(
        body([phrase(), phrase(offsetMilliseconds=800, durationMilliseconds=400, text="Second.")]),
        **_IDENTITY, **_LIMITS,
    )
    assert isinstance(audio, AudioMetadata)
    assert (audio.duration_ms, audio.channel_count, audio.locale) == (3000, None, "en-US")
    assert (audio.mode, audio.api_version) == ("fast", "2025-10-15")
    assert (audio.source_version, audio.source_content_hash) == ("etag-1", "a" * 64)
    assert segments == (
        AudioTranscriptSegment(0, "Synthetic phrase.", 100, 600),
        AudioTranscriptSegment(1, "Second.", 800, 1200),
    )


@pytest.mark.parametrize("duration", [None, True, "3000", 3000.0, 0, -1, 1_800_001])
def test_build_transcript_rejects_invalid_duration(duration: Any) -> None:
    with pytest.raises(TerminalDocumentError, match="audio_response_duration_invalid"):
        build_transcript(body([phrase()], durationMilliseconds=duration), **_IDENTITY, **_LIMITS)


def test_build_transcript_rejects_locale_mismatch_in_phrase() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_response_locale_mismatch"):
        build_transcript(body([phrase(locale="fr-FR")]), **_IDENTITY, **_LIMITS)


def test_build_transcript_rejects_phrase_beyond_response_duration() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_response_invalid_phrase"):
        build_transcript(
            body([phrase(offsetMilliseconds=2900, durationMilliseconds=200)]),
            **_IDENTITY, **_LIMITS,
        )


def test_build_transcript_rejects_no_speech() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_response_no_speech"):
        build_transcript(body([]), **_IDENTITY, **_LIMITS)
