import json

import httpx
import pytest

from ingestion.errors import TerminalDocumentError
from ingestion.speech_fast import transcribe_audio

_ENDPOINT = "https://speech-fixture.cognitiveservices.azure.com"
_SUCCESS_BODY = b'{"durationMilliseconds":3000,"phrases":[]}'


def _call(handler, **overrides):
    kwargs = dict(
        endpoint=_ENDPOINT,
        audio=b"RIFFsynthetic-audio-bytes",
        filename="clip.wav",
        content_type="audio/wav",
        locale="en-US",
        token_provider=lambda: "synthetic-token",
        max_response_bytes=4096,
        timeout_seconds=5.0,
        transport=httpx.MockTransport(handler),
    )
    kwargs.update(overrides)
    return transcribe_audio(**kwargs)


def test_success_posts_multipart_transcribe_request_and_returns_body() -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, content=_SUCCESS_BODY)

    result = _call(handler)

    assert result == _SUCCESS_BODY
    request = captured["request"]
    assert request.method == "POST"
    assert request.url.path == "/speechtotext/transcriptions:transcribe"
    assert request.url.params.get("api-version") == "2025-10-15"
    assert request.headers["Authorization"] == "Bearer synthetic-token"
    body = request.content
    assert b'name="audio"' in body and b"clip.wav" in body
    assert b'name="definition"' in body
    assert json.dumps({"locales": ["en-US"]}, separators=(",", ":")).encode() in body


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retryable_status_raises_transient_timeout(status: int) -> None:
    with pytest.raises(TimeoutError, match="audio_transcription_transient"):
        _call(lambda request: httpx.Response(status, content=b"retry later"))


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
def test_client_error_is_terminal_and_does_not_leak_body(status: int) -> None:
    with pytest.raises(TerminalDocumentError) as excinfo:
        _call(lambda request: httpx.Response(status, content=b"SECRET-PROVIDER-DETAIL"))
    assert "SECRET-PROVIDER-DETAIL" not in str(excinfo.value)
    assert str(status) in str(excinfo.value)


def test_oversize_response_fails_closed() -> None:
    big = b"x" * 100
    with pytest.raises(TerminalDocumentError, match="audio_response_limit_exceeded"):
        _call(lambda request: httpx.Response(200, content=big), max_response_bytes=10)


def test_empty_success_body_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_transcription_empty_response"):
        _call(lambda request: httpx.Response(200, content=b""))


@pytest.mark.parametrize("error", [
    httpx.ConnectTimeout("slow"), httpx.ReadTimeout("slow"), httpx.ConnectError("down"),
])
def test_transport_and_timeout_errors_are_transient(error: Exception) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    with pytest.raises(TimeoutError, match="audio_transcription_transient"):
        _call(handler)


@pytest.mark.parametrize("overrides,message", [
    ({"endpoint": "https://evil.example.com"}, "audio_transcription_endpoint_invalid"),
    ({"endpoint": "http://speech-fixture.cognitiveservices.azure.com"}, "audio_transcription_endpoint_invalid"),
    ({"audio": b""}, "audio_transcription_empty_source"),
    ({"audio": "notbytes"}, "audio_transcription_empty_source"),
    ({"locale": "fr-FR"}, "audio_transcription_locale_invalid"),
    ({"locale": "en-us"}, "audio_transcription_locale_invalid"),
    ({"filename": ""}, "audio_transcription_media_descriptor_invalid"),
    ({"content_type": ""}, "audio_transcription_media_descriptor_invalid"),
])
def test_invalid_inputs_are_terminal(overrides: dict, message: str) -> None:
    with pytest.raises(TerminalDocumentError, match=message):
        _call(lambda request: httpx.Response(200, content=_SUCCESS_BODY), **overrides)


def test_empty_token_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_transcription_token_unavailable"):
        _call(lambda request: httpx.Response(200, content=_SUCCESS_BODY), token_provider=lambda: "")


@pytest.mark.parametrize("overrides", [
    {"max_response_bytes": 0}, {"max_response_bytes": -1}, {"timeout_seconds": 0},
    {"timeout_seconds": -1.0}, {"timeout_seconds": True},
])
def test_non_positive_bounds_raise_value_error(overrides: dict) -> None:
    with pytest.raises(ValueError):
        _call(lambda request: httpx.Response(200, content=_SUCCESS_BODY), **overrides)
