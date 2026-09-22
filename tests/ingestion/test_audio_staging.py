import pytest

from ingestion.audio_staging import delete_audio, upload_audio
from ingestion.errors import TerminalDocumentError

_ENDPOINT = "https://astg.blob.core.windows.net"


class _FakeBlobClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self.uploaded: tuple[bytes, bool] | None = None
        self.deleted = False
        self.delete_error: Exception | None = None

    def upload_blob(self, data: bytes, overwrite: bool = False) -> None:
        self.uploaded = (data, overwrite)

    def delete_blob(self) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted = True


class _FakeBlobServiceClient:
    def __init__(self) -> None:
        self.requested: tuple[str, str] | None = None
        self.blob = _FakeBlobClient(f"{_ENDPOINT}/audio-staging/source/clip.wav")

    def get_blob_client(self, container: str, blob: str) -> _FakeBlobClient:
        self.requested = (container, blob)
        return self.blob


class _NotFound(Exception):
    status_code = 404


def test_upload_returns_plain_url_and_overwrites() -> None:
    service = _FakeBlobServiceClient()
    url = upload_audio(
        blob_service_client=service, container="audio-staging",
        blob_name="source/clip.wav", data=b"RIFFaudio",
    )
    assert url == f"{_ENDPOINT}/audio-staging/source/clip.wav"
    assert "?" not in url  # no SAS token
    assert service.requested == ("audio-staging", "source/clip.wav")
    assert service.blob.uploaded == (b"RIFFaudio", True)


@pytest.mark.parametrize("overrides", [
    {"container": ""}, {"blob_name": ""}, {"container": None}, {"blob_name": None},
])
def test_upload_rejects_invalid_target(overrides: dict) -> None:
    kwargs = dict(
        blob_service_client=_FakeBlobServiceClient(), container="audio-staging",
        blob_name="source/clip.wav", data=b"RIFFaudio",
    )
    kwargs.update(overrides)
    with pytest.raises(TerminalDocumentError, match="audio_staging_target_invalid"):
        upload_audio(**kwargs)


@pytest.mark.parametrize("data", [b"", "notbytes"])
def test_upload_rejects_empty_or_non_bytes(data) -> None:
    with pytest.raises(TerminalDocumentError, match="audio_staging_empty_source"):
        upload_audio(
            blob_service_client=_FakeBlobServiceClient(), container="audio-staging",
            blob_name="source/clip.wav", data=data,
        )


def test_delete_removes_blob() -> None:
    service = _FakeBlobServiceClient()
    delete_audio(blob_service_client=service, container="audio-staging", blob_name="source/clip.wav")
    assert service.blob.deleted is True


def test_delete_is_idempotent_on_missing_blob() -> None:
    service = _FakeBlobServiceClient()
    service.blob.delete_error = _NotFound()
    delete_audio(blob_service_client=service, container="audio-staging", blob_name="source/clip.wav")


def test_delete_reraises_non_not_found_errors() -> None:
    service = _FakeBlobServiceClient()
    service.blob.delete_error = RuntimeError("transient")
    with pytest.raises(RuntimeError, match="transient"):
        delete_audio(blob_service_client=service, container="audio-staging", blob_name="source/clip.wav")
