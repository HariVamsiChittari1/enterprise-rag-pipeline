from __future__ import annotations

import io
import json
import wave
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from config import IngestionConfig
from ingestion.graph import VerifiedAcl
from ingestion.models import (
    ActivityStatus,
    AudioMetadata,
    DocumentStage,
    DocumentStatus,
    LocatorKind,
    SourceDocumentRecord,
    content_sha256,
    content_sha256_bytes,
    create_document_id,
    create_document_key,
    create_source_run_id,
)
from ingestion.repository import VersionedRecord
import ingestion.services as services
from ingestion.services import process_document

UTC = "2026-09-02T07:00:00Z"
GROUP_A = "22222222-2222-4222-8222-222222222222"
ACL_A = VerifiedAcl((GROUP_A,), content_sha256(GROUP_A))


def make_wav(channels: int = 1, rate: int = 16000, frames: int = 16000, width: int = 2) -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as writer:
        writer.setnchannels(channels)
        writer.setsampwidth(width)
        writer.setframerate(rate)
        writer.writeframes(b"\x00" * frames * channels * width)
    return buffer.getvalue()


WAV = make_wav()  # 1000 ms, mono
WAV_HASH = content_sha256_bytes(WAV)


def transcript_body(duration_ms: int = 1000) -> bytes:
    return json.dumps({
        "durationMilliseconds": duration_ms,
        "phrases": [
            {"offsetMilliseconds": 0, "durationMilliseconds": 500, "text": "Hello there.", "locale": "en-US"},
            {"offsetMilliseconds": 500, "durationMilliseconds": 500, "text": "Second phrase here.", "locale": "en-US"},
        ],
    }).encode("utf-8")


def _config(**overrides: Any) -> IngestionConfig:
    values: dict[str, Any] = dict(
        extraction_enabled=True, enrichment_enabled=False, summary_enabled=False,
        key_phrases_enabled=False, entities_enabled=False,
        allowed_extensions=(".pdf", ".wav"),
        source_id="source", drive_id="drive", tenant_id="tenant", app_client_id="app",
        certificate_secret_name="cert", key_vault_uri="https://kv.example",
        cosmos_endpoint="https://cosmos.example", cosmos_database="db",
        cosmos_ingestion_runs_container="ingestion-runs",
        cosmos_source_documents_container="source-documents",
        cosmos_search_chunks_container="search-chunks",
        document_intelligence_endpoint="https://di.example",
        content_understanding_endpoint="https://cu.example",
        content_understanding_analyzer_id="prebuilt-documentSearch",
        language_endpoint="", openai_endpoint="https://openai.example",
        vision_deployment="gpt-5.4", managed_identity_client_id="mi",
        chunk_max_tokens=800, chunk_overlap_tokens=100, acl_max_pages=10,
        download_timeout_seconds=120.0, delta_max_pages=200, embedding_batch_size=100,
        max_pdf_pages=500, vision_max_output_tokens=400, vision_max_image_bytes=2 * 1024 * 1024,
        vision_max_figures=60, query_proxy_timeout_seconds=30.0, sharepoint_site_url="",
        audio_writer_enabled=True,
        speech_endpoint="https://speech-fixture.cognitiveservices.azure.com",
        speech_region="eastus2", audio_deployment_region="eastus2", audio_locale="en-US",
    )
    values.update(overrides)
    return IngestionConfig(**values)


def _document(mime: str = "audio/wav", name: str = "clip.wav") -> SourceDocumentRecord:
    document_id = create_document_id("source", "drive", "item-1")
    return SourceDocumentRecord(
        source_id="source", run_id="run-1", drive_id="drive", item_id="item-1",
        parent_item_id="parent-1", source_name=name, source_path=f"/audio/{name}",
        source_url=f"https://example.invalid/{name}", e_tag="source-etag",
        mime_type=mime, size_bytes=len(WAV), discovery_ordinal=1,
        allowed_group_ids=("pending",), acl_hash=content_sha256("pending"), acl_evaluated_at=UTC,
        status=DocumentStatus.DISCOVERED, stage=DocumentStage.DISCOVERED, attempt_count=0,
        discovered_at=UTC, updated_at=UTC, id=document_id, document_id=document_id,
        source_run_id=create_source_run_id("source", "run-1"),
        document_key=create_document_key("source", "run-1", document_id),
    )


class _Repository:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.chunks: list[Any] = []
        self.admitted_document: SourceDocumentRecord | None = None
        self.ready_document: SourceDocumentRecord | None = None
        self.updated_document: SourceDocumentRecord | None = None
        self.stored_document: VersionedRecord | None = None

    def _stored(self, document: SourceDocumentRecord) -> VersionedRecord:
        return VersionedRecord(document, f"etag-{len(self.calls)}")

    def get_document(self, _source_run_id: str, _document_id: str) -> VersionedRecord | None:
        return self.stored_document

    def mark_document_processing(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("mark_document_processing")
        return self._stored(document)

    def update_processing_document(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("update_processing_document")
        self.updated_document = document
        return self._stored(document)

    def write_chunks(self, chunks: Any) -> int:
        self.calls.append("write_chunks")
        self.chunks = list(chunks)
        return len(self.chunks)

    def write_visual_manifest_pages(self, pages: Any) -> int:
        self.calls.append("write_visual_manifest_pages")
        return len(list(pages))

    def begin_document_admission(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("begin_document_admission")
        self.admitted_document = document
        return self._stored(document)

    def verify_and_mark_document_ready(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("verify_and_mark_document_ready")
        self.ready_document = document
        return self._stored(document)

    def mark_document_failed(self, document: SourceDocumentRecord, _etag: str) -> VersionedRecord:
        self.calls.append("mark_document_failed")
        return self._stored(document)


class _LifecycleRepository:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def set_document_chunks_retrievable(self, **kwargs: Any) -> int:
        self.calls.append(kwargs)
        return kwargs["expected_count"]


class _Connector:
    def __init__(self, content: bytes = WAV, mime: str = "audio/wav", name: str = "clip.wav") -> None:
        self.content = content
        self.mime = mime
        self.name = name
        self.item_etags = ["source-etag", "source-etag"]
        self.acls = [ACL_A, ACL_A]

    def read_item(self, _item_id: str) -> dict[str, Any]:
        return {
            "id": "item-1", "name": self.name, "eTag": self.item_etags.pop(0),
            "size": len(WAV), "file": {"mimeType": self.mime},
        }

    def read_verified_acl(self, _item_id: str, _max_pages: int) -> VerifiedAcl:
        return self.acls.pop(0)

    def download_content_sync(self, _item_id: str, _max_bytes: int, _timeout: float) -> bytes:
        return self.content


def _run(monkeypatch: Any, *, config: IngestionConfig | None = None,
         token_provider: Any = lambda: "synthetic-token",
         transcribe: Any = None, mime: str = "audio/wav",
         name: str = "clip.wav") -> tuple[_Repository, _LifecycleRepository, Any]:
    repository = _Repository()
    lifecycle = _LifecycleRepository()
    monkeypatch.setattr(services, "embed_texts", lambda _c, texts, **_k: [(0.0,) * 3072 for _ in texts])
    monkeypatch.setattr(
        services, "transcribe_audio",
        transcribe or (lambda **_kwargs: transcript_body()),
    )
    outcome = process_document(
        config or _config(), _document(mime, name), "etag-0",
        repository, lifecycle, _Connector(mime=mime, name=name), None, None, object(),
        speech_token_provider=token_provider,
    )
    return repository, lifecycle, outcome


@pytest.mark.parametrize("mime,name", [
    ("audio/wav", "clip.wav"), ("audio/mpeg", "clip.mp3"), ("audio/flac", "clip.flac"),
])
def test_audio_document_is_transcribed_chunked_and_admitted(monkeypatch: Any, mime: str, name: str) -> None:
    repository, lifecycle, outcome = _run(monkeypatch, mime=mime, name=name)

    assert outcome.status is ActivityStatus.SUCCEEDED
    assert outcome.chunks_written == len(repository.chunks) >= 1
    assert "write_visual_manifest_pages" not in repository.calls

    chunk = repository.chunks[0]
    assert chunk.locator_kind is LocatorKind.TIME
    assert chunk.page_start is None and chunk.page_end is None
    assert chunk.start_ms == 0 and chunk.end_ms == 1000
    assert isinstance(chunk.audio, AudioMetadata)
    assert (chunk.audio.duration_ms, chunk.audio.channel_count) == (1000, None)
    assert "channelCount" not in chunk.to_cosmos_item()["audio"]
    assert chunk.audio.source_content_hash == WAV_HASH
    assert chunk.audio.source_version == "source-etag"

    admitted = repository.ready_document
    assert admitted is not None
    assert admitted.status is DocumentStatus.READY
    assert admitted.page_count is None
    assert admitted.content_hash == WAV_HASH
    assert admitted.audio is not None and admitted.source_verified_at is not None
    assert lifecycle.calls and lifecycle.calls[0]["is_retrievable"] is True


def test_audio_requires_writer_enabled(monkeypatch: Any) -> None:
    _, _, outcome = _run(monkeypatch, config=_config(
        audio_writer_enabled=False, speech_endpoint="", speech_region="",
        audio_deployment_region="", audio_locale="",
    ))
    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None and outcome.error.code == "audio_writer_disabled"


def test_audio_requires_token_provider(monkeypatch: Any) -> None:
    _, _, outcome = _run(monkeypatch, token_provider=None)
    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None and outcome.error.code == "audio_transcription_token_provider_missing"


def test_audio_provider_terminal_failure_fails_closed(monkeypatch: Any) -> None:
    from ingestion.errors import TerminalDocumentError

    def _reject(**_kwargs: Any) -> bytes:
        raise TerminalDocumentError("audio_transcription_rejected:400")

    repository, _, outcome = _run(monkeypatch, transcribe=_reject)
    assert outcome.status is ActivityStatus.FAILED
    assert repository.ready_document is None
    assert "write_chunks" not in repository.calls


# --- Batch provider (submit + poll-driven finalize) -------------------------

_JOB_URL = "https://speech-fixture.cognitiveservices.azure.com/speechtotext/transcriptions/abc?api-version=2024-11-15"
_FILES_URL = "https://speech-fixture.cognitiveservices.azure.com/speechtotext/transcriptions/abc/files?api-version=2024-11-15"


def _batch_config(**overrides: Any) -> IngestionConfig:
    return _config(
        audio_transcription_provider="speech_batch",
        audio_staging_blob_endpoint="https://astg.blob.core.windows.net/",
        audio_staging_container="audio-staging",
        **overrides,
    )


class _BlobClient:
    def __init__(self) -> None:
        self.url = "https://astg.blob.core.windows.net/audio-staging/blob.wav"
        self.uploaded: Any = None
        self.deleted = False

    def upload_blob(self, data: bytes, overwrite: bool = False) -> None:
        self.uploaded = (data, overwrite)

    def delete_blob(self) -> None:
        self.deleted = True


class _BlobService:
    def __init__(self) -> None:
        self.blob = _BlobClient()

    def get_blob_client(self, container: str, blob: str) -> _BlobClient:
        return self.blob


def _transcribing_doc() -> SourceDocumentRecord:
    return replace(
        _document(), status=DocumentStatus.PROCESSING, stage=DocumentStage.TRANSCRIBING,
        content_hash=WAV_HASH, transcription_job_url=_JOB_URL, staging_blob_name="key.wav",
    )


def batch_transcript_body(duration_ms: int = 1000) -> bytes:
    return json.dumps({
        "durationMilliseconds": duration_ms,
        "recognizedPhrases": [
            {"recognitionStatus": "Success", "channel": 0, "offsetInTicks": 0.0,
             "durationInTicks": 5_000_000.0, "nBest": [{"display": "Hello there.", "lexical": "hello there"}]},
            {"recognitionStatus": "Success", "channel": 0, "offsetInTicks": 5_000_000.0,
             "durationInTicks": 5_000_000.0, "nBest": [{"display": "Second phrase here."}]},
        ],
    }).encode("utf-8")


def test_audio_batch_submit_stages_source_and_records_job(monkeypatch: Any) -> None:
    repository, lifecycle, blob = _Repository(), _LifecycleRepository(), _BlobService()
    monkeypatch.setattr(services, "submit_transcription", lambda **_k: _JOB_URL)
    outcome = process_document(
        _batch_config(), _document(), "etag-0", repository, lifecycle, _Connector(),
        None, None, object(), speech_token_provider=lambda: "tok", blob_service_client=blob,
    )
    assert outcome.status is ActivityStatus.SUCCEEDED
    assert outcome.chunks_written == 0
    assert "write_chunks" not in repository.calls
    assert "begin_document_admission" not in repository.calls
    submitted = repository.updated_document
    assert submitted is not None and submitted.stage is DocumentStage.TRANSCRIBING
    assert submitted.transcription_job_url == _JOB_URL
    assert submitted.staging_blob_name and submitted.content_hash == WAV_HASH
    assert blob.blob.uploaded is not None and blob.blob.uploaded[1] is True


def test_audio_batch_submit_requires_blob_client(monkeypatch: Any) -> None:
    repository, lifecycle = _Repository(), _LifecycleRepository()
    outcome = process_document(
        _batch_config(), _document(), "etag-0", repository, lifecycle, _Connector(),
        None, None, object(), speech_token_provider=lambda: "tok", blob_service_client=None,
    )
    assert outcome.status is ActivityStatus.FAILED
    assert outcome.error is not None and outcome.error.code == "audio_staging_client_missing"


def _finalize(monkeypatch: Any, *, status: str) -> tuple[_Repository, _LifecycleRepository, dict[str, Any], str]:
    repository, lifecycle, blob = _Repository(), _LifecycleRepository(), _BlobService()
    doc = _transcribing_doc()
    repository.stored_document = VersionedRecord(doc, "etag-x")
    deleted: dict[str, Any] = {}
    monkeypatch.setattr(services, "embed_texts", lambda _c, texts, **_k: [(0.0,) * 3072 for _ in texts])
    monkeypatch.setattr(services, "get_transcription", lambda **_k: SimpleNamespace(
        status=status, files_url=_FILES_URL, self_url=_JOB_URL))
    monkeypatch.setattr(services, "list_transcription_files", lambda **_k: [
        SimpleNamespace(kind="TranscriptionReport", content_url="https://mm.blob.core.windows.net/r.json?s"),
        SimpleNamespace(kind="Transcription", content_url="https://mm.blob.core.windows.net/x.json?s"),
    ])
    monkeypatch.setattr(services, "download_transcription_result", lambda **_k: batch_transcript_body())
    monkeypatch.setattr(services, "delete_transcription", lambda **_k: deleted.update(job=True))
    monkeypatch.setattr(services, "delete_audio", lambda **k: deleted.update(blob=k["blob_name"]))
    result = services.finalize_audio_transcription(
        _batch_config(), doc.source_run_id, doc.document_id, repository, lifecycle, _Connector(),
        None, object(), speech_token_provider=lambda: "tok", blob_service_client=blob,
    )
    return repository, lifecycle, deleted, result


def test_finalize_audio_transcription_succeeds(monkeypatch: Any) -> None:
    repository, lifecycle, deleted, result = _finalize(monkeypatch, status="Succeeded")
    assert result == "succeeded"
    assert "write_chunks" in repository.calls and "verify_and_mark_document_ready" in repository.calls
    assert repository.ready_document is not None and repository.ready_document.status is DocumentStatus.READY
    assert repository.ready_document.audio is not None
    assert repository.ready_document.transcription_job_url is None
    assert repository.chunks[0].start_ms == 0 and repository.chunks[-1].end_ms == 1000
    assert deleted.get("job") is True and deleted.get("blob") == "key.wav"
    assert lifecycle.calls and lifecycle.calls[0]["is_retrievable"] is True


@pytest.mark.parametrize("status", ["NotStarted", "Running"])
def test_finalize_audio_transcription_pending_while_running(monkeypatch: Any, status: str) -> None:
    repository, _, deleted, result = _finalize(monkeypatch, status=status)
    assert result == "pending"
    assert "write_chunks" not in repository.calls
    assert deleted == {}


def test_finalize_audio_transcription_marks_failed_on_failed_job(monkeypatch: Any) -> None:
    repository, _, deleted, result = _finalize(monkeypatch, status="Failed")
    assert result == "failed"
    assert "mark_document_failed" in repository.calls
    assert deleted.get("job") is True


def test_finalize_audio_transcription_skips_non_awaiting_document() -> None:
    repository, lifecycle = _Repository(), _LifecycleRepository()
    repository.stored_document = VersionedRecord(_document(), "etag-x")  # DISCOVERED, not awaiting
    result = services.finalize_audio_transcription(
        _batch_config(), "srr", "did", repository, lifecycle, _Connector(), None, object(),
        speech_token_provider=lambda: "tok", blob_service_client=_BlobService(),
    )
    assert result == "skipped"
    assert repository.calls == []


def test_run_audio_transcription_poll_page_tallies_results(monkeypatch: Any) -> None:
    from ingestion.lifecycle_repository import AudioTranscribingPage, AudioTranscribingRef

    refs = (
        AudioTranscribingRef("d1", "s1"),
        AudioTranscribingRef("d2", "s1"),
        AudioTranscribingRef("d3", "s1"),
    )

    class _Lifecycle:
        def list_transcribing_documents_page(self, *, page_size: int, continuation_token: Any = None) -> AudioTranscribingPage:
            return AudioTranscribingPage(refs, "next-token")

    results = iter(["succeeded", "pending", "failed"])
    monkeypatch.setattr(services, "finalize_audio_transcription", lambda *a, **k: next(results))
    outcome, token = services.run_audio_transcription_poll_page(
        _batch_config(), _Repository(), _Lifecycle(), _Connector(), None, object(),
        speech_token_provider=lambda: "tok", blob_service_client=_BlobService(),
        page_size=20, continuation_token=None,
    )
    assert (outcome.checked, outcome.succeeded, outcome.pending, outcome.failed) == (3, 1, 1, 1)
    assert token == "next-token"
