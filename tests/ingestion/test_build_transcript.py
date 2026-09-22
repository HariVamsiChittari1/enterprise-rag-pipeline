import json
from typing import Any

import pytest

from ingestion.audio_transcription import build_transcript, build_transcript_from_batch
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


_BATCH_IDENTITY = dict(
    locale="en-US", profile_version="speech_batch",
    source_version="etag-1", source_content_hash="a" * 64,
)


def batch_body(recognized: Any, **overrides: Any) -> bytes:
    return json.dumps(
        {"durationMilliseconds": 3000, "recognizedPhrases": recognized, **overrides}
    ).encode("utf-8")


def recognized(**overrides: Any) -> dict[str, Any]:
    return {
        "recognitionStatus": "Success", "channel": 0,
        "offsetInTicks": 1_000_000.0, "durationInTicks": 5_000_000.0,
        "nBest": [{"confidence": 0.9, "lexical": "hello world", "display": "Hello world."}],
        **overrides,
    }


def test_build_transcript_from_batch_projects_recognized_phrases() -> None:
    audio, segments = build_transcript_from_batch(
        batch_body([
            recognized(),
            recognized(offsetInTicks=8_000_000.0, durationInTicks=4_000_000.0,
                       nBest=[{"display": "Second."}]),
        ]),
        **_BATCH_IDENTITY, **_LIMITS,
    )
    assert (audio.mode, audio.api_version) == ("batch", "2024-11-15")
    assert (audio.duration_ms, audio.channel_count, audio.profile_version) == (3000, None, "speech_batch")
    # Ticks (100 ns) convert to ms; display form is preferred over lexical.
    assert segments == (
        AudioTranscriptSegment(0, "Hello world.", 100, 600),
        AudioTranscriptSegment(1, "Second.", 800, 1200),
    )


def test_build_transcript_from_batch_prefers_lexical_when_no_display() -> None:
    _, segments = build_transcript_from_batch(
        batch_body([recognized(nBest=[{"lexical": "hi there"}])]), **_BATCH_IDENTITY, **_LIMITS,
    )
    assert segments[0].text == "hi there"


def test_build_transcript_from_batch_skips_non_success_phrases() -> None:
    _, segments = build_transcript_from_batch(
        batch_body([
            recognized(recognitionStatus="Failure", nBest=[{"display": "dropped"}]),
            recognized(nBest=[{"display": "kept"}]),
        ]),
        **_BATCH_IDENTITY, **_LIMITS,
    )
    assert [segment.text for segment in segments] == ["kept"]


@pytest.mark.parametrize("recognized_value", [[], [dict(recognized(), recognitionStatus="Failure")]])
def test_build_transcript_from_batch_rejects_no_speech(recognized_value: Any) -> None:
    with pytest.raises(TerminalDocumentError, match="audio_response_no_speech"):
        build_transcript_from_batch(batch_body(recognized_value), **_BATCH_IDENTITY, **_LIMITS)


@pytest.mark.parametrize("duration", [None, True, "3000", 0, 1_800_001])
def test_build_transcript_from_batch_rejects_invalid_duration(duration: Any) -> None:
    with pytest.raises(TerminalDocumentError, match="audio_response_duration_invalid"):
        build_transcript_from_batch(
            batch_body([recognized()], durationMilliseconds=duration), **_BATCH_IDENTITY, **_LIMITS,
        )


@pytest.mark.parametrize("phrase_override", [
    {"nBest": []}, {"nBest": "bad"}, {"offsetInTicks": "x"}, {"durationInTicks": None},
    {"nBest": [{"display": ""}]},
])
def test_build_transcript_from_batch_rejects_invalid_phrase(phrase_override: dict) -> None:
    with pytest.raises(TerminalDocumentError, match="audio_response_invalid_phrase"):
        build_transcript_from_batch(
            batch_body([recognized(**phrase_override)]), **_BATCH_IDENTITY, **_LIMITS,
        )


def test_build_transcript_from_batch_rejects_phrase_beyond_duration() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_response_invalid_phrase"):
        build_transcript_from_batch(
            batch_body([recognized(offsetInTicks=29_000_000.0, durationInTicks=2_000_000.0)]),
            **_BATCH_IDENTITY, **_LIMITS,
        )
