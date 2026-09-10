"""Microsoft Graph operations for one controlled SharePoint drive item."""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, unquote, unquote_to_bytes, urlparse, urlsplit

import httpx

from ingestion.errors import TerminalDocumentError
from ingestion.models import ScaleLimits


GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
GRAPH_ORIGIN = httpx.URL(GRAPH_ROOT)
DELTA_PREFER = "deltashowremovedasdeleted, deltatraversepermissiongaps, deltashowsharingchanges"
DOWNLOAD_HOST_SUFFIXES = (
    "files.1drv.com",
    "livefilestore.com",
    "onedrive.live.com",
    "sharepoint.com",
    "sharepointonline.com",
    "storage.live.com",
    "svc.ms",
)
CHILDREN_SELECT = "id,name,eTag,size,file,folder,package,parentReference,webUrl,lastModifiedDateTime"
SUPPORTED_ACL_ROLES = frozenset({"read", "write", "owner"})
ACL_POLICY_VERSION = "verified-entra-security-groups-v1"
SUPPORTED_SOURCE_MIME_TYPES: dict[str, frozenset[str]] = {
    ".md": frozenset({"text/markdown", "text/plain", "application/octet-stream"}),
    ".pdf": frozenset({"application/pdf"}),
    ".docx": frozenset(
        {"application/vnd.openxmlformats-officedocument.wordprocessingml.document"}
    ),
    ".pptx": frozenset(
        {"application/vnd.openxmlformats-officedocument.presentationml.presentation"}
    ),
    ".xlsx": frozenset(
        {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
    ),
}
SUPPORTED_SOURCE_EXTENSIONS = tuple(SUPPORTED_SOURCE_MIME_TYPES)
PDF_SIGNATURE = b"%PDF-"
ZIP_SIGNATURES = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
MARKDOWN_IMAGE_MIME_TYPE = "image/png"
GRAPH_PDF_MAX_ATTEMPTS = 3
GRAPH_PDF_MAX_RETRY_DELAY_SECONDS = 30.0


@dataclass(frozen=True)
class FolderCursor:
    item_id: str
    depth: int
    next_link: str | None = None

    def __post_init__(self) -> None:
        if not self.item_id:
            raise ValueError("folder item_id is required")
        if self.depth < 0:
            raise ValueError("folder depth cannot be negative")
        if self.next_link is not None:
            validate_graph_paging_url(self.next_link)

    def to_dict(self) -> dict[str, Any]:
        return {
            "itemId": self.item_id,
            "depth": self.depth,
            "nextLink": self.next_link,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FolderCursor:
        item_id = value.get("itemId")
        depth = value.get("depth")
        next_link = value.get("nextLink")
        if not isinstance(item_id, str) or isinstance(depth, bool) or not isinstance(depth, int):
            raise ValueError("folder cursor is invalid")
        if next_link is not None and not isinstance(next_link, str):
            raise ValueError("folder cursor nextLink is invalid")
        return cls(item_id, depth, next_link)


@dataclass(frozen=True)
class DiscoveryState:
    pending_folders: tuple[FolderCursor, ...]
    items_scanned: int = 0
    folders_discovered: int = 0
    pdfs_discovered: int = 0
    graph_pages: int = 0

    def __post_init__(self) -> None:
        counters = (
            self.items_scanned,
            self.folders_discovered,
            self.pdfs_discovered,
            self.graph_pages,
        )
        if any(value < 0 for value in counters):
            raise ValueError("discovery counters cannot be negative")

    @classmethod
    def initial(cls, root_item_id: str = "root") -> DiscoveryState:
        return cls((FolderCursor(root_item_id, 0),))

    @property
    def complete(self) -> bool:
        return not self.pending_folders

    def to_dict(self) -> dict[str, Any]:
        return {
            "pendingFolders": [cursor.to_dict() for cursor in self.pending_folders],
            "itemsScanned": self.items_scanned,
            "foldersDiscovered": self.folders_discovered,
            "pdfsDiscovered": self.pdfs_discovered,
            "graphPages": self.graph_pages,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> DiscoveryState:
        pending = value.get("pendingFolders")
        counter_names = (
            "itemsScanned",
            "foldersDiscovered",
            "pdfsDiscovered",
            "graphPages",
        )
        counters = [value.get(name) for name in counter_names]
        if not isinstance(pending, list) or not all(
            isinstance(cursor, dict) for cursor in pending
        ):
            raise ValueError("discovery pending folders are invalid")
        if any(isinstance(counter, bool) or not isinstance(counter, int) for counter in counters):
            raise ValueError("discovery counters are invalid")
        return cls(
            tuple(FolderCursor.from_dict(cursor) for cursor in pending),
            *counters,
        )


@dataclass(frozen=True)
class DiscoveredPdf:
    item_id: str
    parent_item_id: str
    name: str
    source_path: str
    source_url: str
    e_tag: str
    mime_type: str
    size_bytes: int
    discovery_ordinal: int
    last_modified_date_time: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "itemId": self.item_id,
            "parentItemId": self.parent_item_id,
            "name": self.name,
            "sourcePath": self.source_path,
            "sourceUrl": self.source_url,
            "eTag": self.e_tag,
            "mimeType": self.mime_type,
            "sizeBytes": self.size_bytes,
            "discoveryOrdinal": self.discovery_ordinal,
            "lastModifiedDateTime": self.last_modified_date_time,
        }


@dataclass(frozen=True)
class ChildrenPage:
    items: tuple[dict[str, Any], ...]
    next_link: str | None


@dataclass(frozen=True)
class DiscoveryStep:
    state: DiscoveryState
    pdfs: tuple[DiscoveredPdf, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.to_dict(),
            "pdfs": [pdf.to_dict() for pdf in self.pdfs],
        }


@dataclass(frozen=True)
class VerifiedAcl:
    allowed_group_ids: tuple[str, ...]
    acl_hash: str


@dataclass(frozen=True)
class ResolvedMarkdownImage:
    item_id: str
    name: str
    e_tag: str
    size_bytes: int
    mime_type: str


class GraphDiscoveryLimitExceeded(RuntimeError):
    pass


@dataclass(frozen=True)
class DriveDelta:
    items: list[dict[str, Any]]
    delta_link: str
    is_full_baseline: bool


class DeltaResetRequired(RuntimeError):
    def __init__(self, location: str) -> None:
        super().__init__("graph_delta_reset_required")
        self.location = location


class GraphPageLimitExceeded(RuntimeError):
    pass


class GraphCredentialAuth(httpx.Auth):
    def __init__(self, credential: Any, scope: str) -> None:
        self._credential = credential
        self._scope = scope

    def auth_flow(self, request: httpx.Request):
        request.headers["Authorization"] = (
            f"Bearer {self._credential.get_token(self._scope).token}"
        )
        yield request


def validate_graph_paging_url(value: str) -> str:
    try:
        url = httpx.URL(value)
    except (TypeError, ValueError) as error:
        raise ValueError("Graph paging URL is invalid") from error
    if (
        url.scheme != GRAPH_ORIGIN.scheme
        or url.host != GRAPH_ORIGIN.host
        or url.username
        or url.password
        or url.port not in (None, GRAPH_ORIGIN.port, 443)
    ):
        raise ValueError("Graph paging URL is not allowed")
    return str(url)


def _children_url(drive_id: str, cursor: FolderCursor) -> str:
    if cursor.next_link is not None:
        return validate_graph_paging_url(cursor.next_link)
    encoded_drive_id = quote(drive_id, safe="")
    if cursor.item_id == "root":
        return f"{GRAPH_ROOT}/drives/{encoded_drive_id}/root/children"
    return (
        f"{GRAPH_ROOT}/drives/{encoded_drive_id}/items/"
        f"{quote(cursor.item_id, safe='')}/children"
    )


def read_children_page(
    client: httpx.Client,
    drive_id: str,
    cursor: FolderCursor,
) -> ChildrenPage:
    url = _children_url(drive_id, cursor)
    params = (
        None
        if cursor.next_link is not None
        else {"$select": CHILDREN_SELECT, "$orderby": "name asc"}
    )
    response = client.get(url, params=params)
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as error:
        raise ValueError("Graph children response is invalid") from error
    if not isinstance(payload, dict):
        raise ValueError("Graph children response is invalid")
    values = payload.get("value")
    if not isinstance(values, list) or not all(isinstance(item, dict) for item in values):
        raise ValueError("Graph children page has an invalid value collection")
    if "@odata.deltaLink" in payload:
        raise ValueError("Graph children page returned an unexpected deltaLink")
    next_link = payload.get("@odata.nextLink")
    if next_link is not None:
        if not isinstance(next_link, str) or not next_link:
            raise ValueError("Graph children nextLink is invalid")
        next_link = validate_graph_paging_url(next_link)
    return ChildrenPage(tuple(values), next_link)


def _require_item_text(item: dict[str, Any], field_name: str) -> str:
    value = item.get(field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Graph drive item is missing {field_name}")
    return value


def _optional_item_text(item: dict[str, Any], field_name: str) -> str | None:
    value = item.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        return None
    return value


def discovered_pdf_from_item(item: dict[str, Any], ordinal: int) -> DiscoveredPdf:
    item_id = _require_item_text(item, "id")
    name = _require_item_text(item, "name")
    file_metadata = item.get("file")
    graph_mime = (
        file_metadata.get("mimeType") if isinstance(file_metadata, dict) else None
    )
    resolve_source_format(name, graph_mime)
    assert isinstance(graph_mime, str)
    normalized_mime = graph_mime.split(";", 1)[0].strip().lower()
    size_bytes = item.get("size")
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
        raise ValueError("Graph PDF has an invalid size")
    parent = item.get("parentReference")
    if not isinstance(parent, dict):
        raise ValueError("Graph PDF is missing parentReference")
    parent_path = _require_item_text(parent, "path").rstrip("/")
    return DiscoveredPdf(
        item_id=item_id,
        parent_item_id=_require_item_text(parent, "id"),
        name=name,
        source_path=f"{parent_path}/{name}",
        source_url=_require_item_text(item, "webUrl"),
        e_tag=_require_item_text(item, "eTag"),
        mime_type=normalized_mime,
        size_bytes=size_bytes,
        discovery_ordinal=ordinal,
        last_modified_date_time=_optional_item_text(item, "lastModifiedDateTime"),
    )


def resolve_source_format(name: str, graph_mime: str | None) -> str:
    """Validate one supported source extension against Graph's MIME metadata."""
    if not isinstance(name, str) or "." not in name:
        raise TerminalDocumentError("source_extension_missing")
    extension = f".{name.rsplit('.', 1)[-1].strip().lower()}"
    allowed_mimes = SUPPORTED_SOURCE_MIME_TYPES.get(extension)
    if allowed_mimes is None:
        raise TerminalDocumentError(f"source_extension_not_allowed:{extension}")
    if not isinstance(graph_mime, str) or not graph_mime.strip():
        raise TerminalDocumentError("source_mime_missing")
    normalized_mime = graph_mime.split(";", 1)[0].strip().lower()
    if normalized_mime not in allowed_mimes:
        raise TerminalDocumentError(
            f"source_mime_mismatch:{extension}:{normalized_mime}"
        )
    return extension


def validate_source_signature(source_format: str, content: bytes) -> None:
    """Reject source bytes that do not match their validated source format."""
    if not content:
        raise TerminalDocumentError(f"source_content_empty:{source_format}")
    if source_format == ".pdf":
        if not content.startswith(PDF_SIGNATURE):
            raise TerminalDocumentError("source_signature_invalid:.pdf")
        return
    if source_format in {".docx", ".pptx", ".xlsx"}:
        if not content.startswith(ZIP_SIGNATURES):
            raise TerminalDocumentError(f"source_signature_invalid:{source_format}")
        return
    if source_format == ".md":
        try:
            content.decode("utf-8")
        except UnicodeDecodeError as error:
            raise TerminalDocumentError("source_signature_invalid:.md") from error
        return
    raise TerminalDocumentError(f"source_signature_unknown:{source_format}")


def advance_discovery(
    state: DiscoveryState,
    page: ChildrenPage,
    limits: ScaleLimits,
    allowed_extensions: tuple[str, ...] = (".pdf",),
) -> DiscoveryStep:
    if state.complete:
        raise ValueError("discovery is already complete")
    if state.graph_pages >= limits.max_graph_pages:
        raise GraphDiscoveryLimitExceeded("max_graph_pages_exceeded")
    current = state.pending_folders[0]
    remaining = list(state.pending_folders[1:])
    child_folders: list[FolderCursor] = []
    pdfs: list[DiscoveredPdf] = []
    items_scanned = state.items_scanned
    folders_discovered = state.folders_discovered
    pdfs_discovered = state.pdfs_discovered

    for item in page.items:
        items_scanned += 1
        if items_scanned > limits.max_drive_items:
            raise GraphDiscoveryLimitExceeded("max_drive_items_exceeded")

        folder_facet = item.get("folder")
        package_facet = item.get("package")
        if folder_facet is not None or package_facet is not None:
            if folder_facet is not None and not isinstance(folder_facet, dict):
                raise ValueError("Graph folder facet is invalid")
            if package_facet is not None and not isinstance(package_facet, dict):
                raise ValueError("Graph package facet is invalid")
            child_depth = current.depth + 1
            if child_depth > limits.max_folder_depth:
                raise GraphDiscoveryLimitExceeded("max_folder_depth_exceeded")
            folders_discovered += 1
            if folders_discovered > limits.max_folders:
                raise GraphDiscoveryLimitExceeded("max_folders_exceeded")
            child_folders.append(
                FolderCursor(_require_item_text(item, "id"), child_depth)
            )
            continue

        file_facet = item.get("file")
        if file_facet is None:
            continue
        if not isinstance(file_facet, dict):
            raise ValueError("Graph file facet is invalid")
        name = item.get("name")
        if not isinstance(name, str):
            continue
        name_lower = name.lower()
        if not any(name_lower.endswith(ext) for ext in allowed_extensions):
            continue
        if pdfs_discovered >= limits.max_eligible_pdfs:
            raise GraphDiscoveryLimitExceeded("max_eligible_pdfs_exceeded")
        try:
            discovered = discovered_pdf_from_item(item, pdfs_discovered)
        except TerminalDocumentError:
            # Skip a single mislabeled/mismatched file rather than abort the whole traversal.
            continue
        if discovered.size_bytes > limits.max_source_bytes:
            raise GraphDiscoveryLimitExceeded("max_source_bytes_exceeded")
        pdfs.append(discovered)
        pdfs_discovered += 1

    if page.next_link is not None:
        pending = [FolderCursor(current.item_id, current.depth, page.next_link)]
        pending.extend(remaining)
    else:
        pending = remaining
    pending.extend(child_folders)
    return DiscoveryStep(
        DiscoveryState(
            pending_folders=tuple(pending),
            items_scanned=items_scanned,
            folders_discovered=folders_discovered,
            pdfs_discovered=pdfs_discovered,
            graph_pages=state.graph_pages + 1,
        ),
        tuple(pdfs),
    )


def discover_next_page(
    client: httpx.Client,
    drive_id: str,
    state: DiscoveryState,
    limits: ScaleLimits,
    allowed_extensions: tuple[str, ...] = (".pdf",),
) -> DiscoveryStep:
    if state.complete:
        return DiscoveryStep(state, ())
    if state.graph_pages >= limits.max_graph_pages:
        raise GraphDiscoveryLimitExceeded("max_graph_pages_exceeded")
    page = read_children_page(client, drive_id, state.pending_folders[0])
    return advance_discovery(state, page, limits, allowed_extensions)


def read_json_pages(
    client: httpx.Client,
    initial_url: str,
    max_pages: int,
    *,
    headers: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    if max_pages < 1:
        raise ValueError("max_pages must be positive")

    values: list[dict[str, Any]] = []
    url: str | None = initial_url
    delta_link: str | None = None
    page_count = 0
    while url:
        if page_count >= max_pages:
            raise GraphPageLimitExceeded(
                "Graph page guard reached before traversal completed"
            )
        response = client.get(validate_graph_paging_url(url), headers=headers or {})
        if response.status_code == 410:
            location = response.headers.get("location")
            if not location:
                raise ValueError("Graph delta reset omitted its location")
            raise DeltaResetRequired(validate_graph_paging_url(location))
        response.raise_for_status()
        payload = response.json()
        page_values = payload.get("value")
        if not isinstance(page_values, list) or not all(
            isinstance(item, dict) for item in page_values
        ):
            raise ValueError("Graph page has an invalid value collection")
        values.extend(page_values)
        page_count += 1

        next_link = payload.get("@odata.nextLink")
        current_delta = payload.get("@odata.deltaLink")
        if next_link is not None and not isinstance(next_link, str):
            raise ValueError("Graph nextLink is invalid")
        if current_delta is not None and not isinstance(current_delta, str):
            raise ValueError("Graph deltaLink is invalid")
        if next_link and current_delta:
            raise ValueError("Graph page returned both nextLink and deltaLink")
        if next_link:
            next_link = validate_graph_paging_url(next_link)
        if current_delta:
            delta_link = validate_graph_paging_url(current_delta)
        url = next_link

    return values, delta_link


def _read_delta_pages(
    client: httpx.Client,
    initial_url: str,
    max_pages: int,
    *,
    allow_reset: bool,
) -> tuple[list[dict[str, Any]], str, bool]:
    delta_headers = {"Prefer": DELTA_PREFER}
    try:
        values, final_delta_link = read_json_pages(client, initial_url, max_pages, headers=delta_headers)
        is_reset = False
    except DeltaResetRequired as reset:
        if not allow_reset:
            raise
        try:
            values, final_delta_link = read_json_pages(client, reset.location, max_pages, headers=delta_headers)
        except DeltaResetRequired as second_reset:
            # Graph occasionally double-410s during server-side state transitions
            values, final_delta_link = read_json_pages(client, second_reset.location, max_pages, headers=delta_headers)
        is_reset = True
    if not final_delta_link:
        raise ValueError("Graph delta traversal completed without a deltaLink")
    return values, final_delta_link, is_reset


def read_delta_item(
    client: httpx.Client,
    drive_id: str,
    item_id: str,
    max_pages: int,
    delta_link: str | None = None,
) -> tuple[dict[str, Any] | None, str]:
    initial_url = delta_link or f"{GRAPH_ROOT}/drives/{drive_id}/root/delta"
    values, final_delta_link, _is_reset = _read_delta_pages(
        client,
        initial_url,
        max_pages,
        allow_reset=delta_link is not None,
    )
    matches = [item for item in values if item.get("id") == item_id]
    return (matches[-1] if matches else None), final_delta_link


def read_drive_delta(
    client: httpx.Client,
    drive_id: str,
    max_pages: int,
    delta_link: str | None = None,
) -> DriveDelta:
    initial_url = delta_link or f"{GRAPH_ROOT}/drives/{drive_id}/root/delta"
    values, final_delta_link, is_reset = _read_delta_pages(
        client,
        initial_url,
        max_pages,
        allow_reset=delta_link is not None,
    )

    latest_by_id: dict[str, dict[str, Any]] = {}
    for item in values:
        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id:
            raise ValueError("Graph delta item is missing its id")
        latest_by_id[item_id] = item
    return DriveDelta(
        list(latest_by_id.values()),
        final_delta_link,
        delta_link is None or is_reset,
    )


def bootstrap_delta_cursor(client: httpx.Client, drive_id: str) -> str:
    """Fetch the current deltaLink without enumerating existing items (token=latest).

    Used for delta-sync's first tick, since full-sync already established the initial
    corpus via its own tree walk; delta-sync only needs a starting cursor, not a replay.
    """
    url = f"{GRAPH_ROOT}/drives/{quote(drive_id, safe='')}/root/delta?token=latest"
    response = client.get(url, headers={"Prefer": DELTA_PREFER})
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as error:
        raise ValueError("Graph delta bootstrap response is invalid") from error
    delta_link = payload.get("@odata.deltaLink") if isinstance(payload, dict) else None
    if not isinstance(delta_link, str) or not delta_link:
        raise ValueError("Graph delta bootstrap response is missing a deltaLink")
    return validate_graph_paging_url(delta_link)


def read_drive_item(
    client: httpx.Client,
    drive_id: str,
    item_id: str,
) -> dict[str, Any] | None:
    response = client.get(
        f"{GRAPH_ROOT}/drives/{quote(drive_id, safe='')}/items/{quote(item_id, safe='')}",
        params={"$select": "id,name,eTag,cTag,size,file,folder,webUrl"},
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("id") != item_id:
        raise ValueError("Graph drive item response is invalid")
    return payload


def validate_sharepoint_drive_site(
    client: httpx.Client,
    drive_id: str,
    site_url: str,
    max_pages: int,
) -> None:
    try:
        parsed = urlparse(site_url)
        port = parsed.port
    except ValueError as error:
        raise ValueError("SharePoint site URL is invalid") from error
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("SharePoint site URL must be an HTTPS site URL")

    hostname = parsed.hostname.lower()
    site_path = unquote(parsed.path).rstrip("/")
    if site_path:
        site_endpoint = (
            f"{GRAPH_ROOT}/sites/{quote(hostname, safe='.')}:"
            f"{quote(site_path, safe='/')}"
        )
    else:
        site_endpoint = f"{GRAPH_ROOT}/sites/{quote(hostname, safe='.')}"

    site_response = client.get(
        site_endpoint,
        params={"$select": "id,sharepointIds,webUrl"},
    )
    site_response.raise_for_status()
    site = site_response.json()
    site_id = site.get("id") if isinstance(site, dict) else None
    if not isinstance(site_id, str) or not site_id:
        raise ValueError("Graph SharePoint site response is invalid")

    drives, _ = read_json_pages(
        client,
        f"{GRAPH_ROOT}/sites/{quote(site_id, safe=',')}/drives?$select=id,driveType",
        max_pages,
    )
    if not any(
        drive.get("id") == drive_id and drive.get("driveType") == "documentLibrary"
        for drive in drives
    ):
        raise ValueError("Configured SharePoint site does not own the configured drive")


def read_permissions(
    client: httpx.Client,
    drive_id: str,
    item_id: str,
    max_pages: int,
) -> list[dict[str, Any]]:
    try:
        values, _ = read_json_pages(
            client,
            f"{GRAPH_ROOT}/drives/{drive_id}/items/{item_id}/permissions",
            max_pages,
        )
    except GraphPageLimitExceeded as error:
        raise TerminalDocumentError(
            "unsafe_acl:incomplete_permissions_traversal"
        ) from error
    except ValueError as error:
        raise TerminalDocumentError("unsafe_acl:invalid_permissions_response") from error
    return values


def read_security_enabled_group_ids(
    client: httpx.Client,
    group_ids: set[str],
) -> set[str]:
    security_group_ids: set[str] = set()
    for group_id in sorted(group_ids):
        response = client.get(
            f"{GRAPH_ROOT}/groups/{quote(group_id, safe='')}",
            params={"$select": "id,securityEnabled,groupTypes,mailEnabled"},
        )
        if response.status_code == 404:
            # Group deleted from Entra — skip it
            continue
        if response.status_code == 429:
            raise TimeoutError(
                f"graph_throttled_group_{group_id[:8]}"
            )
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as error:
            raise TerminalDocumentError("unsafe_acl:invalid_group_response") from error
        if not isinstance(payload, dict) or payload.get("id") != group_id:
            raise TerminalDocumentError("unsafe_acl:invalid_group_response")
        security_enabled = payload.get("securityEnabled")
        if not isinstance(security_enabled, bool):
            raise TerminalDocumentError("unsafe_acl:invalid_group_security_enabled")
        if security_enabled:
            security_group_ids.add(group_id)
    return security_group_ids


@dataclass(frozen=True)
class PermissionIdentities:
    entra_group_ids: set[str]
    site_group_ids: set[str]


def _permission_group_ids_strict(permissions: list[dict[str, Any]]) -> PermissionIdentities:
    if not permissions:
        raise TerminalDocumentError("unsafe_acl:empty_permissions")
    group_ids: set[str] = set()
    site_group_ids: set[str] = set()
    for permission in permissions:
        roles = permission.get("roles")
        if (
            not isinstance(roles, list)
            or not roles
            or not all(isinstance(role, str) for role in roles)
        ):
            raise TerminalDocumentError("unsafe_acl:missing_roles")
        if any(role not in SUPPORTED_ACL_ROLES for role in roles):
            raise TerminalDocumentError("unsafe_acl:unsupported_role")
        if permission.get("link") is not None:
            raise TerminalDocumentError("unsafe_acl:sharing_link")

        identities: list[dict[str, Any]] = []
        single = permission.get("grantedToV2")
        multiple = permission.get("grantedToIdentitiesV2")
        if single is not None:
            if not isinstance(single, dict):
                raise TerminalDocumentError("unsafe_acl:incomplete_identity_set")
            identities.append(single)
        if multiple is not None:
            if not isinstance(multiple, list) or not all(
                isinstance(identity, dict) for identity in multiple
            ):
                raise TerminalDocumentError("unsafe_acl:incomplete_identity_set")
            identities.extend(multiple)
        if not identities:
            raise TerminalDocumentError("unsafe_acl:missing_identity_set")

        for identity in identities:
            group = identity.get("group")
            site_group = identity.get("siteGroup")
            if group is not None:
                if not isinstance(group, dict):
                    raise TerminalDocumentError(
                        "unsafe_acl:ambiguous_or_missing_entra_principal"
                    )
                group_id = group.get("id")
                if not isinstance(group_id, str) or not group_id:
                    raise TerminalDocumentError("unsafe_acl:missing_principal_id")
                group_ids.add(group_id)
            elif site_group is not None:
                if isinstance(site_group, dict):
                    sg_id = site_group.get("id")
                    if isinstance(sg_id, (str, int)) and sg_id:
                        site_group_ids.add(str(sg_id))
    if not group_ids and not site_group_ids:
        raise TerminalDocumentError("unsafe_acl:no_entra_groups_found")
    return PermissionIdentities(entra_group_ids=group_ids, site_group_ids=site_group_ids)


def resolve_site_group_security_groups(
    sp_client: httpx.Client,
    site_url: str,
    site_group_ids: set[str],
) -> set[str]:
    """Resolve Entra SGs nested inside SharePoint site groups via REST API."""
    entra_ids: set[str] = set()
    base = site_url.rstrip("/")
    for sg_id in sorted(site_group_ids):
        url = f"{base}/_api/web/sitegroups({sg_id})/users"
        response = sp_client.get(url, headers={"Accept": "application/json;odata=nometadata"})
        if response.status_code == 404:
            continue
        response.raise_for_status()
        payload = response.json()
        users = payload.get("value")
        if not isinstance(users, list):
            continue
        for user in users:
            # PrincipalType 4 = SecurityGroup in SharePoint
            if user.get("PrincipalType") == 4:
                login = user.get("LoginName", "")
                # Format: c:0t.c|tenant|{guid} or c:0o.c|...|{guid}_o
                parts = login.rsplit("|", 1)
                if len(parts) == 2 and parts[1]:
                    raw_id = parts[1].rstrip("_o").rstrip("_m")
                    if len(raw_id) == 36 and "-" in raw_id:
                        entra_ids.add(raw_id)
    return entra_ids


def read_verified_acl(
    client: httpx.Client,
    drive_id: str,
    item_id: str,
    max_pages: int,
    *,
    sp_client: httpx.Client | None = None,
    site_url: str = "",
) -> VerifiedAcl:
    permissions = read_permissions(client, drive_id, item_id, max_pages)
    perm_ids = _permission_group_ids_strict(permissions)
    all_group_ids = set(perm_ids.entra_group_ids)
    if perm_ids.site_group_ids and sp_client and site_url:
        sp_groups = resolve_site_group_security_groups(sp_client, site_url, perm_ids.site_group_ids)
        all_group_ids.update(sp_groups)
    if not all_group_ids:
        raise TerminalDocumentError("unsafe_acl:no_entra_groups_found")
    verified_group_ids = read_security_enabled_group_ids(client, all_group_ids)
    if not verified_group_ids:
        raise TerminalDocumentError("unsafe_acl:no_verified_security_groups")
    canonical = tuple(sorted(verified_group_ids))
    digest = hashlib.sha256(
        "\x1f".join((*canonical, ACL_POLICY_VERSION)).encode("utf-8")
    ).hexdigest()
    return VerifiedAcl(canonical, digest)


async def read_bounded_content(response: httpx.Response, max_bytes: int) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length:
        try:
            declared_length = int(content_length)
        except ValueError:
            declared_length = None
        if declared_length is not None and declared_length > max_bytes:
            raise TerminalDocumentError("pdf_too_large")
    parts: list[bytes] = []
    downloaded = 0
    async for part in response.aiter_bytes():
        downloaded += len(part)
        if downloaded > max_bytes:
            raise TerminalDocumentError("pdf_too_large")
        parts.append(part)
    content = b"".join(parts)
    if not content:
        raise TerminalDocumentError("empty_content")
    return content


def validate_download_url(value: str) -> str:
    try:
        url = httpx.URL(value)
    except (TypeError, ValueError) as error:
        raise ValueError("Graph content redirect location is invalid") from error
    host = (url.host or "").lower().rstrip(".")
    allowed_host = any(
        host == suffix or host.endswith(f".{suffix}") for suffix in DOWNLOAD_HOST_SUFFIXES
    )
    if (
        url.scheme != "https"
        or not allowed_host
        or url.username
        or url.password
        or url.port not in (None, 443)
    ):
        raise ValueError("Graph content redirect location is not allowed")
    return str(url)


async def _download_content_async(
    graph_headers: dict[str, str],
    graph_auth: httpx.Auth | None,
    drive_id: str,
    item_id: str,
    max_bytes: int,
) -> bytes:
    async with httpx.AsyncClient(
        auth=graph_auth,
        headers=graph_headers,
        timeout=None,
        follow_redirects=False,
    ) as graph_client:
        async with graph_client.stream(
            "GET",
            f"{GRAPH_ROOT}/drives/{drive_id}/items/{item_id}/content",
        ) as response:
            if response.status_code not in (301, 302, 303, 307, 308):
                response.raise_for_status()
                return await read_bounded_content(response, max_bytes)
            download_url = response.headers.get("location")
    if not download_url:
        raise ValueError("Graph content redirect omitted its location")
    validated_url = validate_download_url(download_url)
    async with httpx.AsyncClient(timeout=None, follow_redirects=False) as download_client:
        async with download_client.stream("GET", validated_url) as download_response:
            if download_response.is_redirect:
                raise ValueError("Graph content download returned an unexpected redirect")
            download_response.raise_for_status()
            return await read_bounded_content(download_response, max_bytes)


def download_content(
    client: httpx.Client,
    drive_id: str,
    item_id: str,
    max_bytes: int,
    timeout_seconds: float,
    graph_auth: httpx.Auth | None = None,
) -> bytes:
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    headers = {
        name: value
        for name, value in client.headers.items()
        if name.lower() in {"authorization", "accept", "prefer"}
    }

    async def run_with_deadline() -> bytes:
        try:
            async with asyncio.timeout(timeout_seconds):
                return await _download_content_async(
                    headers,
                    graph_auth,
                    drive_id,
                    item_id,
                    max_bytes,
                )
        except TimeoutError as error:
            raise TimeoutError("content_download_deadline_exceeded") from error

    return asyncio.run(run_with_deadline())


def download_content_sync(
    client: httpx.Client,
    drive_id: str,
    item_id: str,
    max_bytes: int,
    timeout_seconds: float,
) -> bytes:
    """Synchronous download that works inside Azure Functions (no asyncio.run)."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    url = f"{GRAPH_ROOT}/drives/{quote(drive_id, safe='')}/items/{quote(item_id, safe='')}/content"
    with client.stream("GET", url, follow_redirects=False) as response:
        if response.status_code in (301, 302, 303, 307, 308):
            download_url = response.headers.get("location")
            if not download_url:
                raise ValueError("Graph content redirect omitted its location")
            validated_url = validate_download_url(download_url)
        else:
            response.raise_for_status()
            return _read_bounded_sync(response, max_bytes)
    with httpx.Client(timeout=timeout_seconds, follow_redirects=False) as dl_client:
        with dl_client.stream("GET", validated_url) as dl_response:
            if dl_response.is_redirect:
                raise ValueError("Graph content download returned an unexpected redirect")
            dl_response.raise_for_status()
            return _read_bounded_sync(dl_response, max_bytes)


def download_content_as_pdf_sync(
    client: httpx.Client,
    drive_id: str,
    item_id: str,
    max_bytes: int,
    timeout_seconds: float,
    *,
    download_transport: httpx.BaseTransport | None = None,
) -> bytes:
    """Convert one drive item to PDF and download it through a trusted redirect."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    url = (
        f"{GRAPH_ROOT}/drives/{quote(drive_id, safe='')}/items/"
        f"{quote(item_id, safe='')}/content?format=pdf"
    )
    validated_url: str | None = None
    for attempt in range(GRAPH_PDF_MAX_ATTEMPTS):
        retry_delay: float | None = None
        with client.stream("GET", url, follow_redirects=False) as response:
            if response.status_code in (301, 302, 303, 307, 308):
                download_url = response.headers.get("location")
                if not download_url:
                    raise TerminalDocumentError("graph_pdf_conversion_redirect_missing")
                validated_url = validate_download_url(download_url)
                break
            if response.status_code == 429 or response.status_code >= 500:
                retry_delay = _graph_pdf_retry_delay(response, attempt)
            elif 400 <= response.status_code < 500:
                raise TerminalDocumentError("graph_pdf_conversion_rejected")
            else:
                response.raise_for_status()
                return _validate_pdf_content(_read_bounded_sync(response, max_bytes))
        if retry_delay is not None:
            time.sleep(retry_delay)
    if validated_url is None:
        raise TimeoutError("graph_pdf_conversion_transient")

    with httpx.Client(
        timeout=timeout_seconds,
        follow_redirects=False,
        transport=download_transport,
    ) as download_client:
        for attempt in range(GRAPH_PDF_MAX_ATTEMPTS):
            retry_delay = None
            with download_client.stream("GET", validated_url) as download_response:
                if download_response.is_redirect:
                    raise TerminalDocumentError(
                        "graph_pdf_conversion_unexpected_redirect"
                    )
                if (
                    download_response.status_code == 429
                    or download_response.status_code >= 500
                ):
                    retry_delay = _graph_pdf_retry_delay(download_response, attempt)
                elif 400 <= download_response.status_code < 500:
                    raise TerminalDocumentError(
                        "graph_pdf_conversion_download_rejected"
                    )
                else:
                    download_response.raise_for_status()
                    return _validate_pdf_content(
                        _read_bounded_sync(download_response, max_bytes)
                    )
            if retry_delay is not None:
                time.sleep(retry_delay)
    raise TimeoutError("graph_pdf_conversion_transient")


def _graph_pdf_retry_delay(response: httpx.Response, attempt: int) -> float:
    if attempt >= GRAPH_PDF_MAX_ATTEMPTS - 1:
        raise TimeoutError("graph_pdf_conversion_transient")
    retry_after = response.headers.get("Retry-After")
    if retry_after is not None:
        try:
            delay_seconds = int(retry_after)
        except ValueError:
            delay_seconds = -1
        if delay_seconds >= 0:
            if delay_seconds > GRAPH_PDF_MAX_RETRY_DELAY_SECONDS:
                raise TimeoutError("graph_pdf_conversion_transient")
            return float(delay_seconds)
    return min(
        float(2**attempt),
        GRAPH_PDF_MAX_RETRY_DELAY_SECONDS,
    )


def download_relative_content_sync(
    client: httpx.Client,
    drive_id: str,
    parent_item_id: str,
    relative_path: str,
    max_bytes: int,
    timeout_seconds: float,
    *,
    download_transport: httpx.BaseTransport | None = None,
) -> bytes:
    """Download one local path relative to a known folder drive item."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    image = resolve_relative_markdown_image_sync(
        client,
        drive_id,
        parent_item_id,
        relative_path,
        max_bytes,
    )
    url = (
        f"{GRAPH_ROOT}/drives/{quote(drive_id, safe='')}/items/"
        f"{quote(image.item_id, safe='')}/content"
    )
    with client.stream("GET", url, follow_redirects=False) as response:
        if response.status_code in (301, 302, 303, 307, 308):
            download_url = response.headers.get("location")
            if not download_url:
                raise TerminalDocumentError("graph_asset_redirect_missing")
            validated_url = validate_download_url(download_url)
        elif response.status_code == 429 or response.status_code >= 500:
            raise TimeoutError("graph_asset_download_transient")
        elif 400 <= response.status_code < 500:
            raise TerminalDocumentError("graph_asset_download_rejected")
        else:
            response.raise_for_status()
            return _read_bounded_sync(response, max_bytes)

    with httpx.Client(
        timeout=timeout_seconds,
        follow_redirects=False,
        transport=download_transport,
    ) as download_client:
        with download_client.stream("GET", validated_url) as download_response:
            if download_response.is_redirect:
                raise TerminalDocumentError("graph_asset_unexpected_redirect")
            if download_response.status_code == 429 or download_response.status_code >= 500:
                raise TimeoutError("graph_asset_download_transient")
            if 400 <= download_response.status_code < 500:
                raise TerminalDocumentError("graph_asset_download_rejected")
            download_response.raise_for_status()
            return _read_bounded_sync(download_response, max_bytes)


def resolve_relative_markdown_image_sync(
    client: httpx.Client,
    drive_id: str,
    parent_item_id: str,
    relative_path: str,
    max_bytes: int,
) -> ResolvedMarkdownImage:
    """Resolve and validate one parent-relative Markdown PNG without reading bytes."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    encoded_path = _encode_relative_drive_path(relative_path)
    item_url = (
        f"{GRAPH_ROOT}/drives/{quote(drive_id, safe='')}/items/"
        f"{quote(parent_item_id, safe='')}:/{encoded_path}:"
    )
    metadata_response = client.get(
        item_url,
        params={"$select": "id,name,eTag,size,file"},
    )
    if metadata_response.status_code == 429 or metadata_response.status_code >= 500:
        raise TimeoutError("graph_asset_metadata_transient")
    if 400 <= metadata_response.status_code < 500:
        raise TerminalDocumentError("graph_asset_metadata_rejected")
    metadata_response.raise_for_status()
    try:
        item = metadata_response.json()
    except ValueError as error:
        raise TerminalDocumentError("graph_asset_metadata_invalid") from error
    return _validate_markdown_image_metadata(relative_path, item, max_bytes)


def _encode_relative_drive_path(value: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise TerminalDocumentError("markdown_image_path_invalid")
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
        raise TerminalDocumentError("markdown_image_path_not_local")
    try:
        decoded = unquote_to_bytes(parsed.path).decode("utf-8")
    except UnicodeDecodeError as error:
        raise TerminalDocumentError("markdown_image_path_invalid") from error
    if decoded.startswith(("/", "\\")) or "\\" in decoded:
        raise TerminalDocumentError("markdown_image_path_not_local")
    segments = decoded.split("/")
    if not segments or any(segment in {"", ".", ".."} for segment in segments):
        raise TerminalDocumentError("markdown_image_path_not_local")
    return "/".join(quote(segment, safe="!$&'()*+,;=@-._~") for segment in segments)


def _validate_markdown_image_metadata(
    relative_path: str,
    item: Any,
    max_bytes: int,
) -> ResolvedMarkdownImage:
    if not isinstance(item, dict):
        raise TerminalDocumentError("graph_asset_metadata_invalid")
    expected_name = unquote(urlsplit(relative_path).path).rsplit("/", 1)[-1]
    item_id = item.get("id")
    name = item.get("name")
    e_tag = item.get("eTag")
    size = item.get("size")
    file_metadata = item.get("file")
    mime_type = file_metadata.get("mimeType") if isinstance(file_metadata, dict) else None
    if not isinstance(name, str) or name != expected_name or not name.lower().endswith(".png"):
        raise TerminalDocumentError("markdown_image_extension_not_allowed")
    if not isinstance(mime_type, str) or mime_type.lower() != MARKDOWN_IMAGE_MIME_TYPE:
        raise TerminalDocumentError("markdown_image_mime_not_allowed")
    if not isinstance(item_id, str) or not item_id or not isinstance(e_tag, str) or not e_tag:
        raise TerminalDocumentError("graph_asset_metadata_invalid")
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise TerminalDocumentError("markdown_image_size_invalid")
    if size > max_bytes:
        raise TerminalDocumentError("vision_image_too_large")
    return ResolvedMarkdownImage(
        item_id=item_id,
        name=name,
        e_tag=e_tag,
        size_bytes=size,
        mime_type=MARKDOWN_IMAGE_MIME_TYPE,
    )


def _validate_pdf_content(content: bytes) -> bytes:
    if not content.startswith(PDF_SIGNATURE):
        raise TerminalDocumentError("graph_pdf_conversion_invalid_content")
    return content


def _read_bounded_sync(response: httpx.Response, max_bytes: int) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length:
        try:
            declared = int(content_length)
        except ValueError:
            declared = None
        if declared is not None and declared > max_bytes:
            raise TerminalDocumentError("pdf_too_large")
    parts: list[bytes] = []
    downloaded = 0
    for part in response.iter_bytes():
        downloaded += len(part)
        if downloaded > max_bytes:
            raise TerminalDocumentError("pdf_too_large")
        parts.append(part)
    content = b"".join(parts)
    if not content:
        raise TerminalDocumentError("empty_content")
    return content
