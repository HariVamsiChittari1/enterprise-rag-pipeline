from __future__ import annotations

import json
from dataclasses import replace

import httpx
import pytest
from ingestion.errors import TerminalDocumentError
from ingestion.graph import (
    CHILDREN_SELECT,
    ChildrenPage,
    DiscoveryState,
    FolderCursor,
    GraphDiscoveryLimitExceeded,
    VerifiedAcl,
    advance_discovery,
    canonical_source_mime,
    discover_next_page,
    discovered_pdf_from_item,
    read_verified_acl,
    resolve_relative_markdown_image_sync,
    resolve_site_group_security_groups,
    resolve_source_format,
    validate_sharepoint_drive_site,
)
from ingestion.models import (
    ACL_POLICY_VERSION,
    AclFeedback,
    AclStatus,
    SourceAdmissionConfig,
    verified_acl_hash,
)


def pdf_item(item_id: str, name: str | None = None, size: int = 100) -> dict:
    pdf_name = name or f"{item_id}.pdf"
    return {
        "id": item_id,
        "name": pdf_name,
        "eTag": f"etag-{item_id}",
        "size": size,
        "webUrl": f"https://contoso.sharepoint.com/{pdf_name}",
        "file": {"mimeType": "application/pdf"},
        "parentReference": {"id": "parent", "path": "/drives/drive/root:/Docs"},
    }


@pytest.mark.parametrize("ctag", [None, "content-v1"])
def test_discovery_preserves_optional_content_tag(ctag: str | None) -> None:
    item = pdf_item("pdf")
    if ctag is not None:
        item["cTag"] = ctag
    discovered = discovered_pdf_from_item(item, 0)
    assert discovered.c_tag == ctag
    assert discovered.to_dict().get("cTag") == ctag
    assert ("cTag" in discovered.to_dict()) is (ctag is not None)
    assert "cTag" in CHILDREN_SELECT.split(",")


@pytest.mark.parametrize("ctag", ["", " ", 1, "x" * 501])
def test_discovery_rejects_malformed_content_tag(ctag: object) -> None:
    with pytest.raises(ValueError, match="cTag"):
        discovered_pdf_from_item({**pdf_item("pdf"), "cTag": ctag}, 0)


@pytest.mark.parametrize("ctag", [None, "content-v1", "", " ", 1, "x" * 501])
def test_relative_image_captures_or_rejects_content_tag(ctag: object) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "cTag" in request.url.params["$select"].split(",")
        item = {"id": "image", "name": "chart.png", "eTag": "image-v1",
                "size": 10, "file": {"mimeType": "image/png"}}
        if ctag is not None:
            item["cTag"] = ctag
        return httpx.Response(200, json=item)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        if ctag is None or ctag == "content-v1":
            image = resolve_relative_markdown_image_sync(client, "drive", "parent", "chart.png", 100)
            assert image.c_tag == ctag
        else:
            with pytest.raises(TerminalDocumentError, match="graph_asset_metadata_invalid"):
                resolve_relative_markdown_image_sync(client, "drive", "parent", "chart.png", 100)


def test_verified_acl_keeps_feedback_out_of_authorization_hash() -> None:
    groups = ("group-a",)
    acl_hash = verified_acl_hash(groups, ACL_POLICY_VERSION)
    clean = VerifiedAcl(groups, acl_hash)
    observed = VerifiedAcl(
        groups,
        acl_hash,
        (AclFeedback.SHARED_LINK,),
    )

    assert clean.acl_feedback == ()
    assert clean.acl_status is AclStatus.VERIFIED
    assert observed.allowed_group_ids == clean.allowed_group_ids
    assert observed.acl_hash == clean.acl_hash


def test_verified_acl_requires_canonical_groups_and_feedback() -> None:
    with pytest.raises(ValueError, match="allowed_group_ids"):
        VerifiedAcl(("group-b", "group-a"), "hash")
    with pytest.raises(ValueError, match="acl_feedback"):
        VerifiedAcl(
            ("group-a",),
            "hash",
            (AclFeedback.SHARED_LINK, AclFeedback.DIRECT_USER),
        )
    with pytest.raises(ValueError, match="invalid value"):
        VerifiedAcl(
            ("group-a",),
            "hash",
            ("SharedLink",),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("name", "mime_type"),
    [
        ("guide.md", "text/markdown"),
        ("report.pdf", "application/pdf"),
        (
            "report.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        (
            "briefing.pptx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ),
        (
            "forecast.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
    ],
)
def test_discovery_preserves_validated_source_mime(
    name: str,
    mime_type: str,
) -> None:
    item = pdf_item("source", name=name)
    item["file"]["mimeType"] = f"{mime_type}; charset=binary"

    step = advance_discovery(
        DiscoveryState.initial(),
        ChildrenPage((item,), None),
        SourceAdmissionConfig(),
        allowed_extensions=(".md", ".pdf", ".docx", ".pptx", ".xlsx"),
    )

    assert step.pdfs[0].mime_type == mime_type
    assert step.pdfs[0].to_dict()["mimeType"] == mime_type


@pytest.mark.parametrize(("name", "server_mime", "canonical"), [
    ("meeting.wav", "audio/x-wav", "audio/wav"),
    ("meeting.wav", "application/octet-stream", "audio/wav"),
    ("call.mp3", "audio/mp3", "audio/mpeg"),
    ("call.mp3", "audio/mpeg", "audio/mpeg"),
    ("song.flac", "audio/x-flac", "audio/flac"),
    ("song.flac", "audio/flac", "audio/flac"),
])
def test_discovery_admits_audio_by_extension_and_canonicalizes_mime(
    name: str, server_mime: str, canonical: str,
) -> None:
    item = pdf_item("audio", name=name)
    item["file"]["mimeType"] = server_mime

    step = advance_discovery(
        DiscoveryState.initial(),
        ChildrenPage((item,), None),
        SourceAdmissionConfig(),
        allowed_extensions=(".wav", ".mp3", ".flac"),
    )

    assert step.pdfs[0].mime_type == canonical
    assert step.pdfs[0].to_dict()["mimeType"] == canonical


@pytest.mark.parametrize("graph_mime", ["audio/x-wav", "application/octet-stream", None, ""])
def test_audio_is_resolved_and_canonicalized_regardless_of_server_mime(graph_mime: str | None) -> None:
    assert resolve_source_format("meeting.wav", graph_mime) == ".wav"
    assert canonical_source_mime("meeting.wav", graph_mime) == "audio/wav"


def test_document_mime_cross_check_is_unchanged() -> None:
    with pytest.raises(TerminalDocumentError, match="source_mime_mismatch"):
        resolve_source_format("report.pdf", "text/plain")


def test_discovery_skips_source_mime_mismatch_and_keeps_valid_items() -> None:
    mismatched = pdf_item("bad", name="report.docx")
    valid = pdf_item("good", name="report.pdf")

    step = advance_discovery(
        DiscoveryState.initial(),
        ChildrenPage((mismatched, valid), None),
        SourceAdmissionConfig(),
        allowed_extensions=(".docx", ".pdf"),
    )

    assert [pdf.name for pdf in step.pdfs] == ["report.pdf"]
    assert step.state.pdfs_discovered == 1
    assert step.rejected_count == 1
    assert step.state.rejected_files == 1


def test_discovery_pages_root_then_nested_folders_and_preserves_next_link() -> None:
    opaque_next = (
        "https://graph.microsoft.com/v1.0/drives/drive/root/children"
        "?$skiptoken=a%2Fb%2Bc%3D&custom=%24opaque"
    )
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        if len(requested_urls) == 1:
            assert request.url.params["$select"] == CHILDREN_SELECT
            assert request.url.params["$orderby"] == "name asc"
            assert request.url.params["$top"] == "50"
            return httpx.Response(
                200,
                json={
                    "value": [
                        pdf_item("root-1"),
                        {"id": "folder", "name": "Nested", "folder": {}},
                        {"id": "ignored", "name": "notes.txt", "file": {"mimeType": "text/plain"}},
                    ],
                    "@odata.nextLink": opaque_next,
                },
            )
        if request.url.path.endswith("/items/folder/children"):
            return httpx.Response(200, json={"value": [pdf_item("nested")]})
        assert str(request.url) == opaque_next
        return httpx.Response(200, json={"value": [pdf_item("root-2")]})

    state = DiscoveryState.initial()
    discovered = []
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        while not state.complete:
            step = discover_next_page(
                client, "drive", state, SourceAdmissionConfig()
            )
            state = step.state
            discovered.extend(step.pdfs)

    assert requested_urls[2] == opaque_next
    assert [item.item_id for item in discovered] == ["root-1", "nested", "root-2"]
    assert [item.discovery_ordinal for item in discovered] == [0, 1, 2]
    assert discovered[0].source_path == "/drives/drive/root:/Docs/root-1.pdf"
    assert state == DiscoveryState((), 5, 1, 3, 3, 1)


def test_discovery_state_and_step_are_json_serializable() -> None:
    state = DiscoveryState(
        (FolderCursor("folder", 2, "https://graph.microsoft.com/v1.0/next"),),
        items_scanned=10,
        folders_discovered=3,
        pdfs_discovered=4,
        graph_pages=2,
    )
    step = advance_discovery(
        DiscoveryState.initial(),
        ChildrenPage((pdf_item("pdf"),), None),
        SourceAdmissionConfig(),
    )

    assert DiscoveryState.from_dict(json.loads(json.dumps(state.to_dict()))) == state
    assert json.loads(json.dumps(step.to_dict()))["pdfs"][0]["itemId"] == "pdf"


def test_discovery_traverses_package_facets_as_containers() -> None:
    step = advance_discovery(
        DiscoveryState.initial(),
        ChildrenPage(({"id": "package", "name": "Bundle", "package": {}},), None),
        SourceAdmissionConfig(),
    )
    assert step.state.pending_folders == (FolderCursor("package", 1),)


def test_discovery_state_uses_depth_first_folder_queue() -> None:
    root_state = DiscoveryState.initial()
    root_page = ChildrenPage((
        {"id": "folder-a", "name": "A", "folder": {}},
        {"id": "folder-b", "name": "B", "folder": {}},
    ), None)
    after_root = advance_discovery(
        root_state, root_page, SourceAdmissionConfig()
    ).state
    after_first_child = advance_discovery(
        after_root,
        ChildrenPage(({"id": "nested-a", "name": "Nested", "folder": {}},), None),
        SourceAdmissionConfig(),
    ).state

    assert after_root.pending_folders == (
        FolderCursor("folder-a", 1), FolderCursor("folder-b", 1),
    )
    assert after_first_child.pending_folders == (
        FolderCursor("nested-a", 2), FolderCursor("folder-b", 1),
    )


def test_discovery_processes_bounded_child_cursors_before_parent_next_page() -> None:
    next_link = "https://graph.microsoft.com/v1.0/drives/drive/root/children?$skiptoken=next"
    folders = tuple(
        {"id": f"folder-{index}", "name": f"Folder {index}", "folder": {}}
        for index in range(50)
    )

    state = advance_discovery(
        DiscoveryState.initial(),
        ChildrenPage(folders, next_link),
        SourceAdmissionConfig(max_folders=1_000),
    ).state

    assert len(state.pending_folders) == 51
    assert state.pending_folders[:2] == (
        FolderCursor("folder-0", 1),
        FolderCursor("folder-1", 1),
    )
    assert state.pending_folders[-1] == FolderCursor("root", 0, next_link)

    state = advance_discovery(
        state,
        ChildrenPage((), None),
        SourceAdmissionConfig(max_folders=1_000),
    ).state

    assert len(state.pending_folders) == 50
    assert state.pending_folders[0] == FolderCursor("folder-1", 1)
    assert state.pending_folders[-1] == FolderCursor("root", 0, next_link)


@pytest.mark.parametrize(
    ("limits", "state", "page", "code"),
    [
        (SourceAdmissionConfig(max_drive_items=1), DiscoveryState.initial(), ChildrenPage(({"id": "one"}, {"id": "two"}), None), "max_drive_items_exceeded"),
        (SourceAdmissionConfig(max_folders=1), DiscoveryState.initial(), ChildrenPage(({"id": "one", "folder": {}}, {"id": "two", "folder": {}}), None), "max_folders_exceeded"),
        (SourceAdmissionConfig(max_folder_depth=1), DiscoveryState((FolderCursor("parent", 1),)), ChildrenPage(({"id": "child", "folder": {}},), None), "max_folder_depth_exceeded"),
        (SourceAdmissionConfig(max_eligible_sources=1), DiscoveryState.initial(), ChildrenPage((pdf_item("one"), pdf_item("two")), None), "max_eligible_sources_exceeded"),
        (SourceAdmissionConfig(max_source_bytes=99), DiscoveryState.initial(), ChildrenPage((pdf_item("large", size=100),), None), "max_source_bytes_exceeded"),
    ],
)
def test_discovery_enforces_guards(limits, state, page, code: str) -> None:
    with pytest.raises(GraphDiscoveryLimitExceeded, match=code):
        advance_discovery(state, page, limits)


def test_discovery_enforces_page_guard_before_request() -> None:
    requests: list[httpx.Request] = []
    state = replace(DiscoveryState.initial(), graph_pages=1)
    with httpx.Client(transport=httpx.MockTransport(lambda request: requests.append(request) or httpx.Response(200, json={}))) as client:
        with pytest.raises(GraphDiscoveryLimitExceeded, match="max_graph_pages_exceeded"):
            discover_next_page(
                client, "drive", state, SourceAdmissionConfig(max_graph_pages=1)
            )
    assert requests == []


@pytest.mark.parametrize("next_link", ["http://graph.microsoft.com/v1.0/next", "https://evil.example/v1.0/next", "https://user@graph.microsoft.com/v1.0/next", "https://graph.microsoft.com:444/v1.0/next"])
def test_discovery_rejects_unsafe_next_link(next_link: str) -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json={"value": [], "@odata.nextLink": next_link}))
    with httpx.Client(transport=transport) as client:
        with pytest.raises(ValueError, match="Graph paging URL is not allowed"):
            discover_next_page(
                client, "drive", DiscoveryState.initial(), SourceAdmissionConfig()
            )


def test_read_verified_acl_returns_sorted_verified_security_groups() -> None:
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{"roles": ["read"], "grantedToIdentitiesV2": [{"group": {"id": "group-b"}}, {"group": {"id": "group-a"}}]}]})
        group_id = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(
            200,
            json={"id": group_id, "securityEnabled": True, "groupTypes": []},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acl = read_verified_acl(client, "drive", "item", 2)
    assert acl.allowed_group_ids == ("group-a", "group-b")
    assert acl.acl_hash == "686e6912b96e03ca356929119443b69a2fafe8815791d169dd09cf7f82fd0835"
    assert requests[0].endswith("/permissions")
    assert all("/content" not in path for path in requests)


def test_read_verified_acl_rejects_security_enabled_microsoft_365_group() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{
                "roles": ["read"],
                "grantedToV2": {"group": {"id": "microsoft-365-group"}},
            }]})
        return httpx.Response(200, json={
            "id": "microsoft-365-group",
            "securityEnabled": True,
            "groupTypes": ["Unified"],
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(
            TerminalDocumentError,
            match="unsafe_acl:no_verified_security_groups",
        ):
            read_verified_acl(client, "drive", "item", 2)


@pytest.mark.parametrize("group_id", [" ", "x" * 501, "group\x1finvalid"])
def test_read_verified_acl_rejects_malformed_verified_ids_safely(group_id: str) -> None:
    from ingestion.errors import TerminalDocumentError

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{
                "roles": ["read"], "grantedToV2": {"group": {"id": group_id}},
            }]})
        return httpx.Response(
            200,
            json={"id": group_id, "securityEnabled": True, "groupTypes": []},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(
            TerminalDocumentError,
            match="^unsafe_acl:no_verified_security_groups$",
        ):
            read_verified_acl(client, "drive", "item", 2)


def test_read_verified_acl_combines_direct_and_site_group_security_groups() -> None:
    direct_group_id = "11111111-1111-4111-8111-111111111111"
    nested_group_id = "22222222-2222-4222-8222-222222222222"

    def graph_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [
                {
                    "roles": ["write"],
                    "grantedToV2": {"group": {"id": direct_group_id}},
                },
                {
                    "roles": ["read"],
                    "grantedToV2": {"siteGroup": {"id": "4"}},
                },
            ]})
        group_id = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(
            200,
            json={"id": group_id, "securityEnabled": True, "groupTypes": []},
        )

    def sharepoint_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/_api/web/sitegroups(4)/users")
        return httpx.Response(200, json={"value": [
            {
                "PrincipalType": 4,
                "LoginName": f"c:0t.c|tenant|{nested_group_id}",
            },
            {
                "PrincipalType": 1,
                "LoginName": "i:0#.f|membership|user@contoso.com",
            },
        ]})

    with (
        httpx.Client(transport=httpx.MockTransport(graph_handler)) as graph_client,
        httpx.Client(transport=httpx.MockTransport(sharepoint_handler)) as sp_client,
    ):
        acl = read_verified_acl(
            graph_client,
            "drive",
            "item",
            2,
            sp_client=sp_client,
            site_url="https://contoso.sharepoint.com/sites/docs",
        )

    assert acl.allowed_group_ids == (direct_group_id, nested_group_id)
    assert acl.acl_feedback == (AclFeedback.SITE_GROUP,)
    assert len(acl.acl_hash) == 64


SITE_URL = "https://contoso.sharepoint.com/sites/docs"
NESTED_GROUP_ID = "22222222-2222-4222-8222-222222222222"


def _site_group_users_response() -> httpx.Response:
    return httpx.Response(200, json={"value": [
        {"PrincipalType": 4, "LoginName": f"c:0t.c|tenant|{NESTED_GROUP_ID}"},
    ]})


@pytest.mark.parametrize("status_code", [429, 503])
def test_site_group_resolution_retries_throttling_honoring_retry_after(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
) -> None:
    attempts = 0
    sleep_delays: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(status_code, headers={"Retry-After": "2"})
        return _site_group_users_response()

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with httpx.Client(transport=httpx.MockTransport(handler)) as sp_client:
        groups = resolve_site_group_security_groups(sp_client, SITE_URL, {"3"})

    assert groups == {NESTED_GROUP_ID}
    assert attempts == 2
    assert sleep_delays == [2.0]


def test_site_group_resolution_raises_after_capped_throttling_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    sleep_delays: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503)

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as sp_client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        resolve_site_group_security_groups(sp_client, SITE_URL, {"3"})

    assert attempts == 3
    assert sleep_delays == [1.0, 2.0]


def test_site_group_resolution_does_not_wait_beyond_retry_after_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_delays: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "120"})

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as sp_client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        resolve_site_group_security_groups(sp_client, SITE_URL, {"3"})

    assert sleep_delays == []


def test_site_group_resolution_does_not_retry_other_server_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    sleep_delays: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(500)

    monkeypatch.setattr("time.sleep", sleep_delays.append)
    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as sp_client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        resolve_site_group_security_groups(sp_client, SITE_URL, {"3"})

    assert attempts == 1
    assert sleep_delays == []


def test_validate_sharepoint_drive_site_requires_drive_in_site_collection() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/drives"):
            return httpx.Response(200, json={"value": [{
                "id": "drive-id",
                "driveType": "documentLibrary",
            }]})
        if "/sites/" in request.url.path:
            return httpx.Response(200, json={
                "id": "tenant.sharepoint.com,site,web",
                "webUrl": "https://tenant.sharepoint.com/sites/site",
            })
        raise AssertionError(f"unexpected request: {request.url}")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        site_id = validate_sharepoint_drive_site(
            client,
            "drive-id",
            "https://tenant.sharepoint.com/sites/site/",
            2,
        )

    assert site_id == "tenant.sharepoint.com,site,web"


def test_validate_sharepoint_drive_site_rejects_mismatched_site() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/drives"):
            return httpx.Response(200, json={"value": [{
                "id": "different-drive",
                "driveType": "documentLibrary",
            }]})
        if "/sites/" in request.url.path:
            return httpx.Response(200, json={
                "id": "tenant.sharepoint.com,other-site,other-web",
            })
        raise AssertionError(f"unexpected request: {request.url}")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError, match="does not own"):
            validate_sharepoint_drive_site(
                client,
                "drive-id",
                "https://tenant.sharepoint.com/sites/wrong",
                2,
            )


def test_validate_sharepoint_drive_site_checks_registered_site_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/drives"):
            return httpx.Response(200, json={"value": [{
                "id": "drive-id",
                "driveType": "documentLibrary",
            }]})
        if "/sites/" in request.url.path:
            return httpx.Response(200, json={
                "id": "tenant.sharepoint.com,actual-site,web",
            })
        raise AssertionError(f"unexpected request: {request.url}")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError, match="registered site ID"):
            validate_sharepoint_drive_site(
                client,
                "drive-id",
                "https://tenant.sharepoint.com/sites/site",
                2,
                expected_site_id="tenant.sharepoint.com,other-site,web",
            )


@pytest.mark.parametrize(
    "site_url",
    [
        "http://tenant.sharepoint.com/sites/site",
        "https://user@tenant.sharepoint.com/sites/site",
        "https://tenant.sharepoint.com:444/sites/site",
        "https://tenant.sharepoint.com/sites/site?query=value",
    ],
)
def test_validate_sharepoint_drive_site_rejects_unsafe_url(site_url: str) -> None:
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(500))) as client:
        with pytest.raises(ValueError, match="HTTPS site URL"):
            validate_sharepoint_drive_site(client, "drive-id", site_url, 2)


@pytest.mark.parametrize("permission", [
    {"roles": ["read"], "grantedToV2": {"user": {"id": "user"}}},
    {"roles": ["read"], "grantedToV2": {"sharePointGroup": {"id": "4"}}},
    {"roles": ["read"], "link": {"type": "view"}},
    {"roles": ["unknown"], "grantedToV2": {"group": {"id": "group"}}},
    {"roles": ["read"], "grantedToV2": {"group": {}}},
])
def test_read_verified_acl_rejects_when_no_verified_group_remains(
    permission: dict,
) -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json={"value": [permission]}))
    with httpx.Client(transport=transport) as client:
        with pytest.raises(
            TerminalDocumentError,
            match="unsafe_acl:no_verified_security_groups",
        ):
            read_verified_acl(client, "drive", "item", 1)


@pytest.mark.parametrize("reverse_permissions", [False, True])
def test_read_verified_acl_collects_mixed_permissions_before_deciding(
    reverse_permissions: bool,
) -> None:
    permissions = [
        {"roles": ["read"], "grantedToV2": {"group": {"id": "valid-group"}}},
        {"roles": ["read"], "link": {"scope": "anonymous", "webUrl": "sensitive"}},
        {"roles": ["read"], "grantedToV2": {"user": {"id": "user"}}},
        {"roles": ["read"], "grantedToV2": {"group": {"id": "unified-group"}}},
        {"roles": ["read"], "grantedToV2": {"group": {"id": "deleted-group"}}},
        {"roles": ["read"], "grantedToV2": {"sharePointGroup": {"id": "4"}}},
        {"roles": ["unknown"], "grantedToV2": {"group": {"id": "ignored-group"}}},
        {"roles": ["read"], "grantedToV2": "malformed"},
    ]
    if reverse_permissions:
        permissions.reverse()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": permissions})
        group_id = request.url.path.rsplit("/", 1)[-1]
        if group_id == "deleted-group":
            return httpx.Response(404)
        return httpx.Response(200, json={
            "id": group_id,
            "securityEnabled": True,
            "groupTypes": ["Unified"] if group_id == "unified-group" else [],
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acl = read_verified_acl(client, "drive", "item", 1)

    assert acl.allowed_group_ids == ("valid-group",)
    assert acl.acl_feedback == (
        AclFeedback.DIRECT_USER,
        AclFeedback.INVALID_GROUP,
        AclFeedback.INVALID_IDENTITY,
        AclFeedback.SHARED_LINK,
        AclFeedback.SITE_GROUP,
        AclFeedback.UNIFIED_GROUP,
        AclFeedback.UNSUPPORTED_ROLE,
    )
    assert "sensitive" not in repr(acl)


def test_read_verified_acl_accepts_group_from_specific_people_link() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{
                "roles": ["read"],
                "link": {"scope": "users"},
                "grantedToIdentitiesV2": [{"group": {"id": "group"}}],
            }]})
        return httpx.Response(200, json={
            "id": "group",
            "securityEnabled": True,
            "groupTypes": [],
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acl = read_verified_acl(client, "drive", "item", 1)

    assert acl.allowed_group_ids == ("group",)
    assert acl.acl_feedback == (AclFeedback.SHARED_LINK,)


@pytest.mark.parametrize(
    ("permission", "expected_feedback"),
    [
        (
            {
                "roles": ["read"],
                "link": {
                    "scope": "anonymous",
                    "webUrl": "https://sensitive.invalid/anonymous-token",
                },
            },
            AclFeedback.SHARED_LINK,
        ),
        (
            {
                "roles": ["read"],
                "link": {
                    "scope": "organization",
                    "webUrl": "https://sensitive.invalid/organization-token",
                },
            },
            AclFeedback.SHARED_LINK,
        ),
        (
            {"roles": ["read"], "grantedToV2": {"user": {"id": "direct-user"}}},
            AclFeedback.DIRECT_USER,
        ),
        (
            {"roles": ["read"], "grantedToV2": {"group": {}}},
            AclFeedback.INVALID_IDENTITY,
        ),
        (
            {
                "roles": ["unknown"],
                "grantedToV2": {"group": {"id": "unsupported-role-group"}},
            },
            AclFeedback.UNSUPPORTED_ROLE,
        ),
    ],
)
def test_read_verified_acl_accepts_group_despite_non_authorizing_permission(
    permission: dict,
    expected_feedback: AclFeedback,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [
                {
                    "roles": ["read"],
                    "grantedToV2": {"group": {"id": "valid-group"}},
                },
                permission,
            ]})
        return httpx.Response(200, json={
            "id": "valid-group",
            "securityEnabled": True,
            "groupTypes": [],
            "mailEnabled": False,
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acl = read_verified_acl(client, "drive", "item", 1)

    assert acl.allowed_group_ids == ("valid-group",)
    assert acl.acl_feedback == (expected_feedback,)
    assert "sensitive.invalid" not in repr(acl)


@pytest.mark.parametrize(
    ("group_types", "mail_enabled"),
    [
        (["DynamicMembership"], False),
        ([], True),
    ],
)
def test_read_verified_acl_accepts_supported_modern_security_group(
    group_types: list[str],
    mail_enabled: bool,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{
                "roles": ["read"],
                "grantedToV2": {"group": {"id": "modern-security-group"}},
            }]})
        return httpx.Response(200, json={
            "id": "modern-security-group",
            "securityEnabled": True,
            "groupTypes": group_types,
            "mailEnabled": mail_enabled,
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acl = read_verified_acl(client, "drive", "item", 1)

    assert acl.allowed_group_ids == ("modern-security-group",)
    assert acl.acl_feedback == ()


def test_read_verified_acl_deduplicates_security_group_grants() -> None:
    group_lookups = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal group_lookups
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [
                {
                    "roles": ["read"],
                    "grantedToV2": {"group": {"id": "duplicate-group"}},
                },
                {
                    "roles": ["write"],
                    "grantedToV2": {"group": {"id": "duplicate-group"}},
                },
            ]})
        group_lookups += 1
        return httpx.Response(200, json={
            "id": "duplicate-group",
            "securityEnabled": True,
            "groupTypes": [],
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acl = read_verified_acl(client, "drive", "item", 1)

    assert acl.allowed_group_ids == ("duplicate-group",)
    assert acl.acl_feedback == ()
    assert group_lookups == 1


@pytest.mark.parametrize("invalid_group_state", ["unified", "disabled", "deleted"])
def test_read_verified_acl_accepts_valid_group_when_other_group_is_invalid(
    invalid_group_state: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [
                {
                    "roles": ["read"],
                    "grantedToV2": {"group": {"id": "valid-group"}},
                },
                {
                    "roles": ["read"],
                    "grantedToV2": {"group": {"id": "invalid-group"}},
                },
            ]})
        group_id = request.url.path.rsplit("/", 1)[-1]
        if group_id == "invalid-group" and invalid_group_state == "deleted":
            return httpx.Response(404)
        return httpx.Response(200, json={
            "id": group_id,
            "securityEnabled": invalid_group_state != "disabled"
            if group_id == "invalid-group"
            else True,
            "groupTypes": ["Unified"]
            if group_id == "invalid-group" and invalid_group_state == "unified"
            else [],
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        acl = read_verified_acl(client, "drive", "item", 1)

    assert acl.allowed_group_ids == ("valid-group",)
    expected_feedback = (
        AclFeedback.UNIFIED_GROUP
        if invalid_group_state == "unified"
        else AclFeedback.INVALID_GROUP
    )
    assert acl.acl_feedback == (expected_feedback,)


def test_read_verified_acl_rejects_invalid_permission_collection() -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(200, json={"value": "invalid"})
    )

    with httpx.Client(transport=transport) as client, pytest.raises(
        TerminalDocumentError,
        match="unsafe_acl:invalid_permissions_response",
    ):
        read_verified_acl(client, "drive", "item", 1)


@pytest.mark.parametrize("status_code", [403, 404])
def test_read_verified_acl_rejects_definitively_unverifiable_permissions(
    status_code: int,
) -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(status_code)
    )

    with httpx.Client(transport=transport) as client, pytest.raises(
        TerminalDocumentError,
        match="unsafe_acl:permissions_unverifiable",
    ):
        read_verified_acl(client, "drive", "item", 1)


def test_read_verified_acl_rejects_forbidden_group_verification() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{
                "roles": ["read"],
                "grantedToV2": {"group": {"id": "group"}},
            }]})
        return httpx.Response(403)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client, pytest.raises(
        TerminalDocumentError,
        match="unsafe_acl:group_unverifiable",
    ):
        read_verified_acl(client, "drive", "item", 1)


@pytest.mark.parametrize("status_code", [429, 503])
def test_read_verified_acl_preserves_permission_dependency_failure(
    status_code: int,
) -> None:
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            status_code,
            json={"error": {"code": "unavailable"}},
        )
    )

    with (
        httpx.Client(transport=transport) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        read_verified_acl(client, "drive", "item", 1)


def test_read_verified_acl_fails_closed_on_incomplete_permission_paging() -> None:
    next_link = (
        "https://graph.microsoft.com/v1.0/drives/drive/items/item/"
        "permissions?$skiptoken=next"
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [{
                "roles": ["read"],
                "grantedToV2": {"group": {"id": "group"}},
            }],
            "@odata.nextLink": next_link,
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(
            TerminalDocumentError,
            match="unsafe_acl:incomplete_permissions_traversal",
        ):
            read_verified_acl(client, "drive", "item", 1)


def test_read_verified_acl_preserves_group_lookup_throttling_as_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{
                "roles": ["read"],
                "grantedToV2": {"group": {"id": "group"}},
            }]})
        return httpx.Response(429)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(TimeoutError, match="graph_throttled_group"):
            read_verified_acl(client, "drive", "item", 1)


@pytest.mark.parametrize("security_enabled", [False])
def test_read_verified_acl_rejects_unverified_groups(security_enabled: bool) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{"roles": ["read"], "grantedToV2": {"group": {"id": "group"}}}]})
        return httpx.Response(200, json={
            "id": "group",
            "securityEnabled": security_enabled,
            "groupTypes": [],
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(TerminalDocumentError, match="unsafe_acl:no_verified_security_groups"):
            read_verified_acl(client, "drive", "item", 1)


def test_read_verified_acl_skips_group_not_found_in_graph() -> None:
    """Groups returning 404 are skipped (deleted from Entra)."""
    import pytest
    from ingestion.errors import TerminalDocumentError

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/permissions"):
            return httpx.Response(200, json={"value": [{"roles": ["read"], "grantedToV2": {"group": {"id": "group"}}}]})
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(TerminalDocumentError, match="no_verified_security_groups"):
            read_verified_acl(client, "drive", "item", 1)