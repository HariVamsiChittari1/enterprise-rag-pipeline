"""Azure Speech batch transcription adapter: the async audio provider port's I/O boundary.

Drives the batch transcription REST control plane (`speechtotext/transcriptions`,
api-version 2024-11-15) as four thin, non-blocking calls so the long transcription runs inside
the Speech service instead of blocking a Function worker (see docs/adr/0001):

- ``submit_transcription`` posts one job (source audio via a plain storage URL / Trusted Azure
  services; no ``destinationContainerUrl``) and returns the transcription ``self`` URI.
- ``get_transcription`` polls the job's status and result-files link.
- ``list_transcription_files`` enumerates the result files hosted in the Microsoft-managed
  container.
- ``download_transcription_result`` fetches one result file's raw JSON bytes for the offline
  parser in ``audio_transcription``.
- ``delete_transcription`` removes a finished job.

This module performs no JSON projection of transcripts, retry, or persistence; the caller's bounded
activity owns retry and the parser owns response validation. Transient conditions raise
``TimeoutError`` (retryable); permanent rejections raise ``TerminalDocumentError`` (not retryable),
matching ``safe_error_from_exception``. Response payloads are never logged.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlsplit

import httpx

from .errors import TerminalDocumentError

_SUBMIT_PATH = "/speechtotext/transcriptions:submit"
_API_VERSION = "2024-11-15"
# Retry on throttling and server-side errors; everything else 4xx is terminal.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_ENDPOINT_PATTERN = re.compile(
    r"https://[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.cognitiveservices\.azure\.com/?"
)
_LOCALE_PATTERN = re.compile(r"en-[A-Z]{2}")
_SPEECH_HOST_PATTERN = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.cognitiveservices\.azure\.com"
)
_BLOB_HOST_SUFFIX = ".blob.core.windows.net"
# Job/status/file-listing payloads are small metadata; the transcript itself is bounded separately.
_MAX_METADATA_BYTES = 1 * 1024 * 1024
_TTL_MIN_HOURS = 6
_TTL_MAX_HOURS = 31 * 24


@dataclass(frozen=True)
class BatchTranscription:
    """The routing fields of a batch transcription job; the transcript stays out of band."""

    self_url: str
    status: str
    files_url: str | None


@dataclass(frozen=True)
class BatchTranscriptionFile:
    """One result file entry from the job's files listing (``kind`` selects the transcript)."""

    kind: str
    content_url: str


def submit_transcription(
    *,
    endpoint: str,
    content_urls: list[str],
    locale: str,
    display_name: str,
    time_to_live_hours: int,
    token_provider: Callable[[], str],
    timeout_seconds: float,
    word_level_timestamps: bool = True,
    transport: httpx.BaseTransport | None = None,
) -> str:
    """Submit one batch job and return its transcription ``self`` URI.

    ``content_urls`` are plain ``*.blob.core.windows.net`` URLs read by the Speech resource's
    system-assigned MI (Trusted Azure services); results go to the Microsoft-managed container.
    ``token_provider`` returns a bearer for ``cognitiveservices.azure.com/.default``.
    """
    if not _ENDPOINT_PATTERN.fullmatch(endpoint):
        raise TerminalDocumentError("audio_batch_endpoint_invalid")
    if not isinstance(content_urls, list) or not content_urls:
        raise TerminalDocumentError("audio_batch_content_urls_invalid")
    for url in content_urls:
        if not _is_blob_url(url):
            raise TerminalDocumentError("audio_batch_content_urls_invalid")
    if not _LOCALE_PATTERN.fullmatch(locale):
        raise TerminalDocumentError("audio_batch_locale_invalid")
    if not isinstance(display_name, str) or not display_name:
        raise TerminalDocumentError("audio_batch_display_name_invalid")
    if type(time_to_live_hours) is not int or not (_TTL_MIN_HOURS <= time_to_live_hours <= _TTL_MAX_HOURS):
        raise ValueError("time_to_live_hours must be between 6 and 744")
    _validate_timeout(timeout_seconds)

    url = f"{endpoint.rstrip('/')}{_SUBMIT_PATH}"
    body = {
        "contentUrls": content_urls,
        "locale": locale,
        "displayName": display_name,
        "model": None,
        "properties": {
            "wordLevelTimestampsEnabled": bool(word_level_timestamps),
            "timeToLiveHours": time_to_live_hours,
        },
    }
    payload = _request_json(
        method="POST",
        url=url,
        token_provider=token_provider,
        timeout_seconds=timeout_seconds,
        transport=transport,
        params={"api-version": _API_VERSION},
        json_body=body,
        success_status=frozenset({200, 201}),
        error_prefix="audio_batch_submit",
    )
    self_url = payload.get("self")
    if not isinstance(self_url, str) or not _is_speech_url(self_url):
        raise TerminalDocumentError("audio_batch_submit_missing_self")
    return self_url


def get_transcription(
    *,
    transcription_url: str,
    token_provider: Callable[[], str],
    timeout_seconds: float,
    transport: httpx.BaseTransport | None = None,
) -> BatchTranscription:
    """Poll a job and return its status plus result-files link."""
    if not _is_speech_url(transcription_url):
        raise TerminalDocumentError("audio_batch_transcription_url_invalid")
    _validate_timeout(timeout_seconds)

    payload = _request_json(
        method="GET",
        url=transcription_url,
        token_provider=token_provider,
        timeout_seconds=timeout_seconds,
        transport=transport,
        params=None,
        json_body=None,
        success_status=frozenset({200}),
        error_prefix="audio_batch_get",
    )
    status = payload.get("status")
    if not isinstance(status, str) or not status:
        raise TerminalDocumentError("audio_batch_get_missing_status")
    links = payload.get("links")
    files_url = links.get("files") if isinstance(links, dict) else None
    if files_url is not None and not (isinstance(files_url, str) and _is_speech_url(files_url)):
        raise TerminalDocumentError("audio_batch_get_files_link_invalid")
    self_url = payload.get("self")
    return BatchTranscription(
        self_url=self_url if isinstance(self_url, str) else transcription_url,
        status=status,
        files_url=files_url,
    )


def list_transcription_files(
    *,
    files_url: str,
    token_provider: Callable[[], str],
    timeout_seconds: float,
    transport: httpx.BaseTransport | None = None,
) -> list[BatchTranscriptionFile]:
    """Enumerate a finished job's result files (``kind`` distinguishes transcript from report)."""
    if not _is_speech_url(files_url):
        raise TerminalDocumentError("audio_batch_files_url_invalid")
    _validate_timeout(timeout_seconds)

    payload = _request_json(
        method="GET",
        url=files_url,
        token_provider=token_provider,
        timeout_seconds=timeout_seconds,
        transport=transport,
        params=None,
        json_body=None,
        success_status=frozenset({200}),
        error_prefix="audio_batch_files",
    )
    values = payload.get("values")
    if not isinstance(values, list):
        raise TerminalDocumentError("audio_batch_files_malformed")
    files: list[BatchTranscriptionFile] = []
    for entry in values:
        if not isinstance(entry, dict):
            continue
        kind = entry.get("kind")
        links = entry.get("links")
        content_url = links.get("contentUrl") if isinstance(links, dict) else None
        if not isinstance(kind, str) or not isinstance(content_url, str):
            continue
        if not _is_blob_url(content_url):
            raise TerminalDocumentError("audio_batch_files_content_url_invalid")
        files.append(BatchTranscriptionFile(kind=kind, content_url=content_url))
    return files


def download_transcription_result(
    *,
    content_url: str,
    max_response_bytes: int,
    timeout_seconds: float,
    transport: httpx.BaseTransport | None = None,
) -> bytes:
    """Download one result file's raw JSON bytes for the offline parser.

    ``content_url`` is a Microsoft-hosted, SAS-embedded blob URL; no bearer is attached (the SAS is
    the credential, and the host is not our Speech resource), and the host is validated first.
    """
    if not _is_blob_url(content_url):
        raise TerminalDocumentError("audio_batch_result_url_invalid")
    if type(max_response_bytes) is not int or max_response_bytes <= 0:
        raise ValueError("max_response_bytes must be a positive integer")
    _validate_timeout(timeout_seconds)

    try:
        with httpx.Client(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        ) as client:
            with client.stream("GET", content_url, headers={"Accept": "application/json"}) as response:
                if response.status_code == 200:
                    return _read_bounded(response, max_response_bytes)
                if response.status_code in _RETRYABLE_STATUS:
                    raise TimeoutError(f"audio_batch_result_transient:{response.status_code}")
                raise TerminalDocumentError(f"audio_batch_result_rejected:{response.status_code}")
    except (httpx.TimeoutException, httpx.TransportError) as error:
        raise TimeoutError("audio_batch_result_transient") from error


def delete_transcription(
    *,
    transcription_url: str,
    token_provider: Callable[[], str],
    timeout_seconds: float,
    transport: httpx.BaseTransport | None = None,
) -> None:
    """Delete a finished job; a missing job (404) is treated as already deleted (idempotent)."""
    if not _is_speech_url(transcription_url):
        raise TerminalDocumentError("audio_batch_transcription_url_invalid")
    _validate_timeout(timeout_seconds)

    token = _bearer(token_provider)
    try:
        with httpx.Client(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        ) as client:
            response = client.request(
                "DELETE", transcription_url, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code in (200, 202, 204, 404):
                return
            if response.status_code in _RETRYABLE_STATUS:
                raise TimeoutError(f"audio_batch_delete_transient:{response.status_code}")
            raise TerminalDocumentError(f"audio_batch_delete_rejected:{response.status_code}")
    except (httpx.TimeoutException, httpx.TransportError) as error:
        raise TimeoutError("audio_batch_delete_transient") from error


def _request_json(
    *,
    method: str,
    url: str,
    token_provider: Callable[[], str],
    timeout_seconds: float,
    transport: httpx.BaseTransport | None,
    params: dict[str, str] | None,
    json_body: dict | None,
    success_status: frozenset[int],
    error_prefix: str,
) -> dict:
    """Issue one authenticated control-plane call and return the bounded, parsed JSON object."""
    token = _bearer(token_provider)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if json_body is not None:
        headers["Content-Type"] = "application/json"
    try:
        with httpx.Client(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        ) as client:
            with client.stream(
                method, url, params=params, headers=headers, json=json_body
            ) as response:
                if response.status_code in success_status:
                    raw = _read_bounded(response, _MAX_METADATA_BYTES)
                    return _parse_object(raw, error_prefix)
                if response.status_code in _RETRYABLE_STATUS:
                    raise TimeoutError(f"{error_prefix}_transient:{response.status_code}")
                raise TerminalDocumentError(f"{error_prefix}_rejected:{response.status_code}")
    except (httpx.TimeoutException, httpx.TransportError) as error:
        raise TimeoutError(f"{error_prefix}_transient") from error


def _parse_object(raw: bytes, error_prefix: str) -> dict:
    try:
        payload = json.loads(raw)
    except ValueError as error:
        raise TerminalDocumentError(f"{error_prefix}_malformed") from error
    if not isinstance(payload, dict):
        raise TerminalDocumentError(f"{error_prefix}_malformed")
    return payload


def _read_bounded(response: httpx.Response, max_bytes: int) -> bytes:
    """Accumulate the streamed body, failing closed if it exceeds the byte ceiling."""
    buffer = bytearray()
    for part in response.iter_bytes():
        buffer.extend(part)
        if len(buffer) > max_bytes:
            raise TerminalDocumentError("audio_batch_response_limit_exceeded")
    if not buffer:
        raise TerminalDocumentError("audio_batch_empty_response")
    return bytes(buffer)


def _bearer(token_provider: Callable[[], str]) -> str:
    token = token_provider()
    if not isinstance(token, str) or not token:
        raise TerminalDocumentError("audio_batch_token_unavailable")
    return token


def _validate_timeout(timeout_seconds: float) -> None:
    if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be a positive number")


def _is_speech_url(url: str) -> bool:
    return _match_https_host(url, lambda host: bool(_SPEECH_HOST_PATTERN.fullmatch(host)))


def _is_blob_url(url: str) -> bool:
    return _match_https_host(url, lambda host: host.endswith(_BLOB_HOST_SUFFIX) and len(host) > len(_BLOB_HOST_SUFFIX))


def _match_https_host(url: str, host_ok: Callable[[str], bool]) -> bool:
    if not isinstance(url, str) or not url:
        return False
    if any(character.isspace() or ord(character) < 32 for character in url):
        return False
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    if "@" in parts.netloc:  # reject userinfo (e.g. https://evil.com@speech...) to block SSRF
        return False
    return parts.scheme == "https" and host_ok(parts.hostname or "")
