import json

import httpx
import pytest

from ingestion.errors import TerminalDocumentError
from ingestion.speech_batch import (
    BatchTranscriptionFile,
    delete_transcription,
    download_transcription_result,
    get_transcription,
    list_transcription_files,
    submit_transcription,
)

_ENDPOINT = "https://speech-fixture.cognitiveservices.azure.com"
_SELF = "https://speech-fixture.cognitiveservices.azure.com/speechtotext/transcriptions/abc?api-version=2024-11-15"
_FILES = "https://speech-fixture.cognitiveservices.azure.com/speechtotext/transcriptions/abc/files?api-version=2024-11-15"
_CONTENT = "https://mm.blob.core.windows.net/results/abc/transcript.json?sv=2024&sig=redacted"
_AUDIO = "https://acct.blob.core.windows.net/audio-staging/clip.wav"


def _transport(handler) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


# ---- submit ---------------------------------------------------------------

def _submit(handler, **overrides):
    kwargs = dict(
        endpoint=_ENDPOINT,
        content_urls=[_AUDIO],
        locale="en-US",
        display_name="doc-v1",
        time_to_live_hours=48,
        token_provider=lambda: "synthetic-token",
        timeout_seconds=5.0,
        transport=_transport(handler),
    )
    kwargs.update(overrides)
    return submit_transcription(**kwargs)


def test_submit_posts_job_and_returns_self_uri() -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(201, json={"self": _SELF, "status": "NotStarted"})

    result = _submit(handler)

    assert result == _SELF
    request = captured["request"]
    assert request.method == "POST"
    assert request.url.path == "/speechtotext/transcriptions:submit"
    assert request.url.params.get("api-version") == "2024-11-15"
    assert request.headers["Authorization"] == "Bearer synthetic-token"
    body = json.loads(request.content)
    assert body["contentUrls"] == [_AUDIO]
    assert body["locale"] == "en-US"
    assert body["model"] is None
    assert body["properties"]["timeToLiveHours"] == 48
    assert body["properties"]["wordLevelTimestampsEnabled"] is True
    assert "destinationContainerUrl" not in body["properties"]


def test_submit_accepts_200_status() -> None:
    assert _submit(lambda request: httpx.Response(200, json={"self": _SELF, "status": "NotStarted"})) == _SELF


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_submit_retryable_status_is_transient(status: int) -> None:
    with pytest.raises(TimeoutError, match="audio_batch_submit_transient"):
        _submit(lambda request: httpx.Response(status, content=b"retry"))


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422])
def test_submit_client_error_is_terminal_and_hides_body(status: int) -> None:
    with pytest.raises(TerminalDocumentError) as excinfo:
        _submit(lambda request: httpx.Response(status, content=b"SECRET-PROVIDER-DETAIL"))
    assert "SECRET-PROVIDER-DETAIL" not in str(excinfo.value)
    assert str(status) in str(excinfo.value)


def test_submit_missing_self_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_submit_missing_self"):
        _submit(lambda request: httpx.Response(201, json={"status": "NotStarted"}))


def test_submit_malformed_json_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_submit_malformed"):
        _submit(lambda request: httpx.Response(201, content=b"not-json"))


@pytest.mark.parametrize("overrides,message", [
    ({"endpoint": "https://evil.example.com"}, "audio_batch_endpoint_invalid"),
    ({"endpoint": "http://speech-fixture.cognitiveservices.azure.com"}, "audio_batch_endpoint_invalid"),
    ({"content_urls": []}, "audio_batch_content_urls_invalid"),
    ({"content_urls": ["https://evil.example.com/clip.wav"]}, "audio_batch_content_urls_invalid"),
    ({"content_urls": ["https://acct.blob.core.windows.net@evil.com/clip.wav"]}, "audio_batch_content_urls_invalid"),
    ({"locale": "fr-FR"}, "audio_batch_locale_invalid"),
    ({"locale": "en-us"}, "audio_batch_locale_invalid"),
    ({"display_name": ""}, "audio_batch_display_name_invalid"),
])
def test_submit_invalid_inputs_are_terminal(overrides: dict, message: str) -> None:
    with pytest.raises(TerminalDocumentError, match=message):
        _submit(lambda request: httpx.Response(201, json={"self": _SELF, "status": "NotStarted"}), **overrides)


@pytest.mark.parametrize("ttl", [5, 745, 48.0, True])
def test_submit_invalid_ttl_raises_value_error(ttl) -> None:
    with pytest.raises(ValueError):
        _submit(lambda request: httpx.Response(201, json={"self": _SELF}), time_to_live_hours=ttl)


def test_submit_empty_token_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_token_unavailable"):
        _submit(lambda request: httpx.Response(201, json={"self": _SELF}), token_provider=lambda: "")


@pytest.mark.parametrize("error", [httpx.ConnectTimeout("slow"), httpx.ReadTimeout("slow"), httpx.ConnectError("down")])
def test_submit_transport_errors_are_transient(error: Exception) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    with pytest.raises(TimeoutError, match="audio_batch_submit_transient"):
        _submit(handler)


# ---- get ------------------------------------------------------------------

def test_get_returns_status_and_files_link() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"self": _SELF, "status": "Succeeded", "links": {"files": _FILES}})

    result = get_transcription(transcription_url=_SELF, token_provider=lambda: "t", timeout_seconds=5.0, transport=_transport(handler))
    assert result.status == "Succeeded"
    assert result.files_url == _FILES
    assert result.self_url == _SELF


def test_get_tolerates_missing_files_link_while_running() -> None:
    result = get_transcription(
        transcription_url=_SELF, token_provider=lambda: "t", timeout_seconds=5.0,
        transport=_transport(lambda request: httpx.Response(200, json={"self": _SELF, "status": "Running"})),
    )
    assert result.status == "Running"
    assert result.files_url is None


def test_get_missing_status_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_get_missing_status"):
        get_transcription(
            transcription_url=_SELF, token_provider=lambda: "t", timeout_seconds=5.0,
            transport=_transport(lambda request: httpx.Response(200, json={"self": _SELF})),
        )


def test_get_foreign_files_link_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_get_files_link_invalid"):
        get_transcription(
            transcription_url=_SELF, token_provider=lambda: "t", timeout_seconds=5.0,
            transport=_transport(lambda request: httpx.Response(200, json={"status": "Succeeded", "links": {"files": "https://evil.example.com/x"}})),
        )


def test_get_rejects_non_speech_url() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_transcription_url_invalid"):
        get_transcription(transcription_url="https://evil.example.com/x", token_provider=lambda: "t", timeout_seconds=5.0)


# ---- list files -----------------------------------------------------------

def test_list_files_returns_transcript_and_report_entries() -> None:
    payload = {"values": [
        {"kind": "Transcription", "links": {"contentUrl": _CONTENT}},
        {"kind": "TranscriptionReport", "links": {"contentUrl": "https://mm.blob.core.windows.net/results/report.json?sig=x"}},
        {"kind": "Transcription"},  # no links -> skipped
    ]}
    files = list_transcription_files(
        files_url=_FILES, token_provider=lambda: "t", timeout_seconds=5.0,
        transport=_transport(lambda request: httpx.Response(200, json=payload)),
    )
    assert BatchTranscriptionFile(kind="Transcription", content_url=_CONTENT) in files
    assert len(files) == 2


def test_list_files_foreign_content_url_is_terminal() -> None:
    payload = {"values": [{"kind": "Transcription", "links": {"contentUrl": "https://evil.example.com/x.json"}}]}
    with pytest.raises(TerminalDocumentError, match="audio_batch_files_content_url_invalid"):
        list_transcription_files(
            files_url=_FILES, token_provider=lambda: "t", timeout_seconds=5.0,
            transport=_transport(lambda request: httpx.Response(200, json=payload)),
        )


def test_list_files_malformed_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_files_malformed"):
        list_transcription_files(
            files_url=_FILES, token_provider=lambda: "t", timeout_seconds=5.0,
            transport=_transport(lambda request: httpx.Response(200, json={"values": "nope"})),
        )


# ---- download result ------------------------------------------------------

def test_download_result_returns_body_without_authorization_header() -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["request"] = request
        return httpx.Response(200, content=b'{"combinedRecognizedPhrases":[]}')

    body = download_transcription_result(content_url=_CONTENT, max_response_bytes=4096, timeout_seconds=5.0, transport=_transport(handler))
    assert body == b'{"combinedRecognizedPhrases":[]}'
    assert "authorization" not in {k.lower() for k in captured["request"].headers}


def test_download_result_oversize_fails_closed() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_response_limit_exceeded"):
        download_transcription_result(
            content_url=_CONTENT, max_response_bytes=10, timeout_seconds=5.0,
            transport=_transport(lambda request: httpx.Response(200, content=b"x" * 100)),
        )


def test_download_result_rejects_non_blob_url() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_result_url_invalid"):
        download_transcription_result(content_url="https://speech-fixture.cognitiveservices.azure.com/x", max_response_bytes=4096, timeout_seconds=5.0)


@pytest.mark.parametrize("status", [429, 503])
def test_download_result_retryable_is_transient(status: int) -> None:
    with pytest.raises(TimeoutError, match="audio_batch_result_transient"):
        download_transcription_result(
            content_url=_CONTENT, max_response_bytes=4096, timeout_seconds=5.0,
            transport=_transport(lambda request: httpx.Response(status, content=b"retry")),
        )


# ---- delete ---------------------------------------------------------------

@pytest.mark.parametrize("status", [200, 202, 204, 404])
def test_delete_success_and_idempotent(status: int) -> None:
    delete_transcription(
        transcription_url=_SELF, token_provider=lambda: "t", timeout_seconds=5.0,
        transport=_transport(lambda request: httpx.Response(status)),
    )


def test_delete_retryable_is_transient() -> None:
    with pytest.raises(TimeoutError, match="audio_batch_delete_transient"):
        delete_transcription(
            transcription_url=_SELF, token_provider=lambda: "t", timeout_seconds=5.0,
            transport=_transport(lambda request: httpx.Response(503)),
        )


def test_delete_client_error_is_terminal() -> None:
    with pytest.raises(TerminalDocumentError, match="audio_batch_delete_rejected"):
        delete_transcription(
            transcription_url=_SELF, token_provider=lambda: "t", timeout_seconds=5.0,
            transport=_transport(lambda request: httpx.Response(400)),
        )
