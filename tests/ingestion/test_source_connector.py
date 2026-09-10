"""Tests for the SourceConnector boundary: SharePointConnector must delegate to the
same graph.py functions unchanged, just with drive_id/client pre-bound."""

from __future__ import annotations

import httpx
import pytest

from ingestion.errors import TerminalDocumentError
from ingestion.graph import (
    DiscoveryState,
    download_content_as_pdf_sync,
    download_relative_content_sync,
    resolve_source_format,
    validate_source_signature,
)
from ingestion.models import ScaleLimits
from ingestion.source_connector import SharePointConnector


def _connector(handler) -> tuple[httpx.Client, SharePointConnector]:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return client, SharePointConnector(client, "drive-1")


def test_discover_next_page_delegates_with_bound_drive_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "/drives/drive-1/root/children" in str(request.url)
        return httpx.Response(200, json={"value": []})

    client, connector = _connector(handler)
    with client:
        step = connector.discover_next_page(DiscoveryState.initial(), ScaleLimits())

    assert step.state.complete is True


def test_read_verified_acl_delegates_with_bound_drive_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "/permissions" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "roles": ["read"],
                            "grantedToV2": {"group": {"id": "22222222-2222-2222-2222-222222222222"}},
                        }
                    ]
                },
            )
        if "/groups/" in str(request.url):
            return httpx.Response(200, json={"id": "22222222-2222-2222-2222-222222222222", "securityEnabled": True})
        raise AssertionError(f"unexpected request: {request.url}")

    client, connector = _connector(handler)
    with client:
        acl = connector.read_verified_acl("item-1", max_pages=5)

    assert acl.allowed_group_ids == ("22222222-2222-2222-2222-222222222222",)


def test_read_item_delegates_with_bound_drive_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "/drives/drive-1/items/item-1" in str(request.url)
        return httpx.Response(200, json={
            "id": "item-1",
            "name": "document.pdf",
            "eTag": "source-etag",
            "file": {"mimeType": "application/pdf"},
        })

    client, connector = _connector(handler)
    with client:
        item = connector.read_item("item-1")

    assert item is not None
    assert item["eTag"] == "source-etag"


def test_download_content_as_pdf_uses_conversion_endpoint_and_untrusted_client() -> None:
    def graph_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/drives/drive-1/items/item-1/content")
        assert request.url.params["format"] == "pdf"
        assert request.headers["authorization"] == "Bearer graph-token"
        return httpx.Response(
            302,
            headers={"location": "https://download.files.1drv.com/converted"},
        )

    def download_handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        return httpx.Response(200, content=b"%PDF-converted")

    with httpx.Client(
        headers={"Authorization": "Bearer graph-token"},
        transport=httpx.MockTransport(graph_handler),
    ) as graph_client:
        content = download_content_as_pdf_sync(
            graph_client,
            "drive-1",
            "item-1",
            1024,
            5,
            download_transport=httpx.MockTransport(download_handler),
        )

    assert content == b"%PDF-converted"


def test_given_graph_throttling_when_converting_pdf_then_honors_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph_attempts = 0
    sleep_delays: list[float] = []

    def graph_handler(request: httpx.Request) -> httpx.Response:
        nonlocal graph_attempts
        graph_attempts += 1
        assert request.headers["authorization"] == "Bearer graph-token"
        if graph_attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        return httpx.Response(
            302,
            headers={"location": "https://download.files.1drv.com/converted"},
        )

    def download_handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        return httpx.Response(200, content=b"%PDF-converted")

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with httpx.Client(
        headers={"Authorization": "Bearer graph-token"},
        transport=httpx.MockTransport(graph_handler),
    ) as graph_client:
        content = download_content_as_pdf_sync(
            graph_client,
            "drive-1",
            "item-1",
            1024,
            5,
            download_transport=httpx.MockTransport(download_handler),
        )

    assert content == b"%PDF-converted"
    assert graph_attempts == 2
    assert sleep_delays == [3.0]


@pytest.mark.parametrize("retry_after", [None, "not-a-delay"])
def test_given_invalid_retry_after_when_converting_pdf_then_uses_exponential_fallback(
    monkeypatch: pytest.MonkeyPatch,
    retry_after: str | None,
) -> None:
    graph_attempts = 0
    sleep_delays: list[float] = []

    def graph_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal graph_attempts
        graph_attempts += 1
        if graph_attempts == 1:
            headers = {"Retry-After": retry_after} if retry_after is not None else {}
            return httpx.Response(429, headers=headers)
        return httpx.Response(200, content=b"%PDF-converted")

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with httpx.Client(transport=httpx.MockTransport(graph_handler)) as graph_client:
        content = download_content_as_pdf_sync(
            graph_client,
            "drive-1",
            "item-1",
            1024,
            5,
        )

    assert content == b"%PDF-converted"
    assert graph_attempts == 2
    assert sleep_delays == [1.0]


def test_given_repeated_graph_throttling_when_converting_pdf_then_fails_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph_attempts = 0
    sleep_delays: list[float] = []

    def graph_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal graph_attempts
        graph_attempts += 1
        return httpx.Response(429, headers={"Retry-After": "0"})

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with httpx.Client(transport=httpx.MockTransport(graph_handler)) as graph_client:
        with pytest.raises(TimeoutError, match="graph_pdf_conversion_transient"):
            download_content_as_pdf_sync(
                graph_client,
                "drive-1",
                "item-1",
                1024,
                5,
            )

    assert graph_attempts == 3
    assert sleep_delays == [0.0, 0.0]


@pytest.mark.parametrize("throttle_stage", ["conversion", "download"])
def test_given_retry_after_exceeds_bound_when_converting_pdf_then_fails_without_sleep(
    monkeypatch: pytest.MonkeyPatch,
    throttle_stage: str,
) -> None:
    graph_attempts = 0
    download_attempts = 0
    sleep_delays: list[float] = []

    def graph_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal graph_attempts
        graph_attempts += 1
        if throttle_stage == "conversion":
            return httpx.Response(429, headers={"Retry-After": "31"})
        return httpx.Response(
            302,
            headers={"location": "https://download.files.1drv.com/converted"},
        )

    def download_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal download_attempts
        download_attempts += 1
        return httpx.Response(429, headers={"Retry-After": "31"})

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with httpx.Client(transport=httpx.MockTransport(graph_handler)) as graph_client:
        with pytest.raises(TimeoutError, match="graph_pdf_conversion_transient"):
            download_content_as_pdf_sync(
                graph_client,
                "drive-1",
                "item-1",
                1024,
                5,
                download_transport=httpx.MockTransport(download_handler),
            )

    assert graph_attempts == 1
    assert download_attempts == (1 if throttle_stage == "download" else 0)
    assert sleep_delays == []


def test_given_redirect_throttling_when_downloading_pdf_then_retries_without_graph_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    download_attempts = 0
    sleep_delays: list[float] = []

    def graph_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer graph-token"
        return httpx.Response(
            302,
            headers={"location": "https://download.files.1drv.com/converted"},
        )

    def download_handler(request: httpx.Request) -> httpx.Response:
        nonlocal download_attempts
        download_attempts += 1
        assert "authorization" not in request.headers
        if download_attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "2"})
        return httpx.Response(200, content=b"%PDF-converted")

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with httpx.Client(
        headers={"Authorization": "Bearer graph-token"},
        transport=httpx.MockTransport(graph_handler),
    ) as graph_client:
        content = download_content_as_pdf_sync(
            graph_client,
            "drive-1",
            "item-1",
            1024,
            5,
            download_transport=httpx.MockTransport(download_handler),
        )

    assert content == b"%PDF-converted"
    assert download_attempts == 2
    assert sleep_delays == [2.0]


def test_given_repeated_redirect_throttling_when_downloading_pdf_then_fails_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    download_attempts = 0
    sleep_delays: list[float] = []

    def graph_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer graph-token"
        return httpx.Response(
            302,
            headers={"location": "https://download.files.1drv.com/converted"},
        )

    def download_handler(request: httpx.Request) -> httpx.Response:
        nonlocal download_attempts
        download_attempts += 1
        assert "authorization" not in request.headers
        return httpx.Response(429, headers={"Retry-After": "0"})

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with httpx.Client(
        headers={"Authorization": "Bearer graph-token"},
        transport=httpx.MockTransport(graph_handler),
    ) as graph_client:
        with pytest.raises(TimeoutError, match="graph_pdf_conversion_transient"):
            download_content_as_pdf_sync(
                graph_client,
                "drive-1",
                "item-1",
                1024,
                5,
                download_transport=httpx.MockTransport(download_handler),
            )

    assert download_attempts == 3
    assert sleep_delays == [0.0, 0.0]


def test_download_content_as_pdf_rejects_unsafe_redirect() -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            302, headers={"location": "https://evil.example/converted"}
        )
    )
    with httpx.Client(transport=transport) as client:
        with pytest.raises(ValueError, match="redirect location is not allowed"):
            download_content_as_pdf_sync(client, "drive-1", "item-1", 1024, 5)


def test_download_content_as_pdf_rejects_invalid_or_oversized_content() -> None:
    def graph_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            302,
            headers={"location": "https://download.files.1drv.com/converted"},
        )

    with httpx.Client(transport=httpx.MockTransport(graph_handler)) as graph_client:
        with pytest.raises(TerminalDocumentError, match="pdf_too_large"):
            download_content_as_pdf_sync(
                graph_client,
                "drive-1",
                "item-1",
                5,
                5,
                download_transport=httpx.MockTransport(
                    lambda _request: httpx.Response(200, content=b"%PDF-too-large")
                ),
            )

        with pytest.raises(
            TerminalDocumentError, match="graph_pdf_conversion_invalid_content"
        ):
            download_content_as_pdf_sync(
                graph_client,
                "drive-1",
                "item-1",
                1024,
                5,
                download_transport=httpx.MockTransport(
                    lambda _request: httpx.Response(200, content=b"not-a-pdf")
                ),
            )


@pytest.mark.parametrize(
    ("name", "mime", "expected"),
    [
        ("guide.md", "text/markdown", ".md"),
        ("report.pdf", "application/pdf", ".pdf"),
        (
            "report.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".docx",
        ),
        (
            "slides.pptx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            ".pptx",
        ),
        (
            "data.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xlsx",
        ),
    ],
)
def test_resolve_source_format_accepts_exact_production_formats(
    name: str, mime: str, expected: str
) -> None:
    assert resolve_source_format(name, mime) == expected


@pytest.mark.parametrize(
    ("name", "mime", "error"),
    [
        ("legacy.doc", "application/msword", "source_extension_not_allowed"),
        ("report.pdf", None, "source_mime_missing"),
        ("report.docx", "application/pdf", "source_mime_mismatch"),
    ],
)
def test_resolve_source_format_rejects_unsupported_or_spoofed_sources(
    name: str, mime: str | None, error: str
) -> None:
    with pytest.raises(TerminalDocumentError, match=error):
        resolve_source_format(name, mime)


@pytest.mark.parametrize(
    ("source_format", "content"),
    [
        (".md", b"# Guide\n"),
        (".pdf", b"%PDF-document"),
        (".docx", b"PK\x03\x04document"),
        (".pptx", b"PK\x05\x06slides"),
        (".xlsx", b"PK\x07\x08workbook"),
    ],
)
def test_validate_source_signature_accepts_exact_production_formats(
    source_format: str, content: bytes
) -> None:
    validate_source_signature(source_format, content)


def test_validate_source_signature_rejects_mismatched_content() -> None:
    with pytest.raises(TerminalDocumentError, match="source_signature_invalid:.docx"):
        validate_source_signature(".docx", b"%PDF-spoofed")


def test_download_relative_content_uses_encoded_parent_relative_path() -> None:
    def graph_handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer graph-token"
        if request.url.path.endswith(
            "/drives/drive-1/items/parent-1:/images/chart one.png:"
        ):
            assert request.url.params["$select"] == "id,name,eTag,size,file"
            return httpx.Response(
                200,
                json={
                    "id": "image-1",
                    "name": "chart one.png",
                    "eTag": "image-etag",
                    "size": 11,
                    "file": {"mimeType": "image/png"},
                },
            )
        assert request.url.path.endswith("/drives/drive-1/items/image-1/content")
        return httpx.Response(
            302, headers={"location": "https://download.files.1drv.com/image"}
        )

    def download_handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        return httpx.Response(200, content=b"image-bytes")

    with httpx.Client(
        headers={"Authorization": "Bearer graph-token"},
        transport=httpx.MockTransport(graph_handler),
    ) as graph_client:
        content = download_relative_content_sync(
            graph_client,
            "drive-1",
            "parent-1",
            "images/chart%20one.png",
            1024,
            5,
            download_transport=httpx.MockTransport(download_handler),
        )

    assert content == b"image-bytes"


def test_download_relative_content_rejects_spoofed_png_before_content_request() -> None:
    requests: list[httpx.Request] = []

    def graph_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "id": "image-1",
                "name": "chart.png",
                "eTag": "image-etag",
                "size": 11,
                "file": {"mimeType": "image/jpeg"},
            },
        )

    with httpx.Client(transport=httpx.MockTransport(graph_handler)) as client:
        with pytest.raises(TerminalDocumentError, match="markdown_image_mime_not_allowed"):
            download_relative_content_sync(
                client,
                "drive-1",
                "parent-1",
                "chart.png",
                1024,
                5,
            )

    assert len(requests) == 1
    assert not requests[0].url.path.endswith("/content")


@pytest.mark.parametrize(
    "path",
    [
        "https://example.com/image.png",
        "//example.com/image.png",
        "/images/image.png",
        "../image.png",
        "images/../image.png",
        "images\\image.png",
        "image.png?token=secret",
        "image.png#fragment",
    ],
)
def test_download_relative_content_rejects_nonlocal_or_escaping_paths(path: str) -> None:
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda _request: pytest.fail("Graph must not be called")
        )
    ) as client:
        with pytest.raises(TerminalDocumentError, match="markdown_image_path_not_local"):
            download_relative_content_sync(client, "drive-1", "parent-1", path, 1024, 5)


def test_read_verified_acl_delegates_site_group_context() -> None:
    nested_group_id = "33333333-3333-4333-8333-333333333333"

    def graph_handler(request: httpx.Request) -> httpx.Response:
        if "/permissions" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "roles": ["read"],
                            "grantedToV2": {"siteGroup": {"id": "4"}},
                        }
                    ]
                },
            )
        if "/groups/" in str(request.url):
            return httpx.Response(
                200, json={"id": nested_group_id, "securityEnabled": True}
            )
        raise AssertionError(f"unexpected Graph request: {request.url}")

    def sharepoint_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/_api/web/sitegroups(4)/users")
        return httpx.Response(
            200,
            json={
                "value": [
                    {
                        "PrincipalType": 4,
                        "LoginName": f"c:0t.c|tenant|{nested_group_id}",
                    }
                ]
            },
        )

    with (
        httpx.Client(transport=httpx.MockTransport(graph_handler)) as graph_client,
        httpx.Client(transport=httpx.MockTransport(sharepoint_handler)) as sp_client,
    ):
        connector = SharePointConnector(
            graph_client,
            "drive-1",
            sp_client=sp_client,
            site_url="https://contoso.sharepoint.com/sites/docs",
        )
        acl = connector.read_verified_acl("item-1", max_pages=5)

    assert acl.allowed_group_ids == (nested_group_id,)


def test_bootstrap_delta_cursor_delegates_with_bound_drive_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "/drives/drive-1/root/delta" in str(request.url)
        assert "token=latest" in str(request.url)
        return httpx.Response(200, json={"@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=abc"})

    client, connector = _connector(handler)
    with client:
        cursor = connector.bootstrap_delta_cursor()

    assert cursor == "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=abc"


def test_read_drive_delta_delegates_with_bound_drive_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "/drives/drive-1/root/delta" in str(request.url)
        return httpx.Response(
            200,
            json={"value": [{"id": "item-1"}], "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=new"},
        )

    client, connector = _connector(handler)
    with client:
        delta = connector.read_drive_delta(max_pages=10)

    assert delta.delta_link == "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=new"
    assert [item["id"] for item in delta.items] == ["item-1"]
