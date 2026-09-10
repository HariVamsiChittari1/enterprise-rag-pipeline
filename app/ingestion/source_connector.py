"""Thin connector boundary between ingestion's business logic (services.py) and the
source-specific discovery/ACL/download/delta mechanism.

Deliberately NOT a rewrite of the Cosmos schema or graph.py's internals -- SourceDocumentRecord
still uses SharePoint-shaped fields (drive_id, item_id) since that schema is already
permanently baked into deployed Cosmos data. This module only abstracts WHICH API produces
the DiscoveredPdf/VerifiedAcl/DriveDelta results services.py already consumes, so a future
connector (e.g. Azure Storage) can be added without changing services.py's orchestration logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import httpx

from ingestion.graph import (
    DiscoveryState,
    DiscoveryStep,
    DriveDelta,
    ResolvedMarkdownImage,
    ScaleLimits,
    VerifiedAcl,
    bootstrap_delta_cursor,
    discover_next_page,
    download_content_as_pdf_sync,
    download_relative_content_sync,
    download_content_sync,
    read_drive_delta,
    read_drive_item,
    read_verified_acl,
    resolve_relative_markdown_image_sync,
)


class SourceConnector(Protocol):
    """One controlled source (e.g. one SharePoint drive) for full-sync and delta-sync."""

    def discover_next_page(
        self,
        state: DiscoveryState,
        limits: ScaleLimits,
        allowed_extensions: tuple[str, ...] = (".pdf",),
    ) -> DiscoveryStep: ...

    def read_verified_acl(self, item_id: str, max_pages: int) -> VerifiedAcl: ...

    def read_item(self, item_id: str) -> dict | None: ...

    def download_content_sync(self, item_id: str, max_bytes: int, timeout_seconds: float) -> bytes: ...

    def download_content_as_pdf_sync(
        self, item_id: str, max_bytes: int, timeout_seconds: float
    ) -> bytes: ...

    def download_relative_content_sync(
        self,
        parent_item_id: str,
        relative_path: str,
        max_bytes: int,
        timeout_seconds: float,
    ) -> bytes: ...

    def resolve_relative_markdown_image_sync(
        self,
        parent_item_id: str,
        relative_path: str,
        max_bytes: int,
    ) -> ResolvedMarkdownImage: ...

    def read_drive_delta(self, max_pages: int, delta_link: str | None = None) -> DriveDelta: ...

    def bootstrap_delta_cursor(self) -> str: ...


@dataclass(frozen=True)
class SharePointConnector:
    """Binds an authenticated Graph client to one drive_id; wraps graph.py unchanged."""

    client: httpx.Client
    drive_id: str
    sp_client: httpx.Client | None = None
    site_url: str = ""

    def discover_next_page(
        self,
        state: DiscoveryState,
        limits: ScaleLimits,
        allowed_extensions: tuple[str, ...] = (".pdf",),
    ) -> DiscoveryStep:
        return discover_next_page(self.client, self.drive_id, state, limits, allowed_extensions)

    def read_verified_acl(self, item_id: str, max_pages: int) -> VerifiedAcl:
        return read_verified_acl(self.client, self.drive_id, item_id, max_pages, sp_client=self.sp_client, site_url=self.site_url)

    def read_item(self, item_id: str) -> dict | None:
        return read_drive_item(self.client, self.drive_id, item_id)

    def download_content_sync(self, item_id: str, max_bytes: int, timeout_seconds: float) -> bytes:
        return download_content_sync(self.client, self.drive_id, item_id, max_bytes, timeout_seconds)

    def download_content_as_pdf_sync(
        self, item_id: str, max_bytes: int, timeout_seconds: float
    ) -> bytes:
        return download_content_as_pdf_sync(
            self.client, self.drive_id, item_id, max_bytes, timeout_seconds
        )

    def download_relative_content_sync(
        self,
        parent_item_id: str,
        relative_path: str,
        max_bytes: int,
        timeout_seconds: float,
    ) -> bytes:
        return download_relative_content_sync(
            self.client,
            self.drive_id,
            parent_item_id,
            relative_path,
            max_bytes,
            timeout_seconds,
        )

    def resolve_relative_markdown_image_sync(
        self,
        parent_item_id: str,
        relative_path: str,
        max_bytes: int,
    ) -> ResolvedMarkdownImage:
        return resolve_relative_markdown_image_sync(
            self.client,
            self.drive_id,
            parent_item_id,
            relative_path,
            max_bytes,
        )

    def read_drive_delta(self, max_pages: int, delta_link: str | None = None) -> DriveDelta:
        return read_drive_delta(self.client, self.drive_id, max_pages, delta_link=delta_link)

    def bootstrap_delta_cursor(self) -> str:
        return bootstrap_delta_cursor(self.client, self.drive_id)
