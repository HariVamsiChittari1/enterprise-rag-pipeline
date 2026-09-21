"""Azure Speech fast transcription adapter: the audio provider port's only I/O boundary.

Sends one synchronous multipart request to the fast transcription API
(`speechtotext/transcriptions:transcribe`, api-version 2025-10-15) and returns the raw
response bytes for the offline parser in `audio_transcription`. This module performs no
JSON projection, retry, or persistence; the caller's bounded activity owns retry and the
parser owns response validation. Transient conditions raise ``TimeoutError`` (retryable);
permanent rejections raise ``TerminalDocumentError`` (not retryable), matching
``safe_error_from_exception``. Response payloads are never logged.
"""

from __future__ import annotations

import json
import re
from typing import Callable

import httpx

from .errors import TerminalDocumentError

_TRANSCRIBE_PATH = "/speechtotext/transcriptions:transcribe"
_API_VERSION = "2025-10-15"
# Retry on throttling and server-side errors; everything else 4xx is terminal.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_ENDPOINT_PATTERN = re.compile(
    r"https://[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.cognitiveservices\.azure\.com/?"
)
_LOCALE_PATTERN = re.compile(r"en-[A-Z]{2}")


def transcribe_audio(
    *,
    endpoint: str,
    audio: bytes,
    filename: str,
    content_type: str,
    locale: str,
    token_provider: Callable[[], str],
    max_response_bytes: int,
    timeout_seconds: float,
    transport: httpx.BaseTransport | None = None,
) -> bytes:
    """Transcribe one immutable audio version and return the raw JSON response bytes.

    ``token_provider`` returns a bearer token for the Speech data plane
    (``cognitiveservices.azure.com/.default``); it is invoked once per call so an
    expired token is refreshed by the caller's credential. ``transport`` is a test seam.
    """
    if not _ENDPOINT_PATTERN.fullmatch(endpoint):
        raise TerminalDocumentError("audio_transcription_endpoint_invalid")
    if not isinstance(audio, bytes) or not audio:
        raise TerminalDocumentError("audio_transcription_empty_source")
    if not _LOCALE_PATTERN.fullmatch(locale):
        raise TerminalDocumentError("audio_transcription_locale_invalid")
    if not filename or not content_type:
        raise TerminalDocumentError("audio_transcription_media_descriptor_invalid")
    if type(max_response_bytes) is not int or max_response_bytes <= 0:
        raise ValueError("max_response_bytes must be a positive integer")
    if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be a positive number")

    url = f"{endpoint.rstrip('/')}{_TRANSCRIBE_PATH}"
    token = token_provider()
    if not isinstance(token, str) or not token:
        raise TerminalDocumentError("audio_transcription_token_unavailable")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    # httpx sets the multipart boundary and Content-Type for the files/data parts.
    files = {"audio": (filename, audio, content_type)}
    data = {"definition": json.dumps({"locales": [locale]}, separators=(",", ":"))}

    try:
        with httpx.Client(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        ) as client:
            with client.stream(
                "POST", url, params={"api-version": _API_VERSION},
                headers=headers, files=files, data=data,
            ) as response:
                if response.status_code == 200:
                    return _read_bounded(response, max_response_bytes)
                if response.status_code in _RETRYABLE_STATUS:
                    raise TimeoutError(f"audio_transcription_transient:{response.status_code}")
                raise TerminalDocumentError(
                    f"audio_transcription_rejected:{response.status_code}"
                )
    except (httpx.TimeoutException, httpx.TransportError) as error:
        raise TimeoutError("audio_transcription_transient") from error


def _read_bounded(response: httpx.Response, max_bytes: int) -> bytes:
    """Accumulate the streamed body, failing closed if it exceeds the byte ceiling."""
    buffer = bytearray()
    for part in response.iter_bytes():
        buffer.extend(part)
        if len(buffer) > max_bytes:
            raise TerminalDocumentError("audio_response_limit_exceeded")
    if not buffer:
        raise TerminalDocumentError("audio_transcription_empty_response")
    return bytes(buffer)
