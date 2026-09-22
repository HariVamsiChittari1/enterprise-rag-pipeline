"""Blob upload/delete for batch-transcription source staging.

Stages source audio in the dedicated audio-staging container so Azure Speech batch
transcription can read it via its system-assigned managed identity (Trusted Azure services).
The ``BlobServiceClient`` is injected (test seam); this module never constructs credentials
and never logs blob contents. Blob URLs are returned without a SAS — access is granted by the
Speech resource's managed identity, not a token in the URL.
"""

from __future__ import annotations

from typing import Any

from .errors import TerminalDocumentError


def upload_audio(
    *,
    blob_service_client: Any,
    container: str,
    blob_name: str,
    data: bytes,
) -> str:
    """Upload one immutable audio blob (overwrite-safe re-submit) and return its plain URL."""
    if not isinstance(container, str) or not container or not isinstance(blob_name, str) or not blob_name:
        raise TerminalDocumentError("audio_staging_target_invalid")
    if not isinstance(data, bytes) or not data:
        raise TerminalDocumentError("audio_staging_empty_source")
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)
    return blob_client.url


def delete_audio(
    *,
    blob_service_client: Any,
    container: str,
    blob_name: str,
) -> None:
    """Delete a staged audio blob; a missing blob is treated as already deleted (idempotent)."""
    if not isinstance(container, str) or not container or not isinstance(blob_name, str) or not blob_name:
        raise TerminalDocumentError("audio_staging_target_invalid")
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_name)
    try:
        blob_client.delete_blob()
    except Exception as error:
        if getattr(error, "status_code", None) == 404:
            return
        raise
