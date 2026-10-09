"""Shared schema-v1 contract for explicitly registered ingestion sources."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from urllib.parse import urlsplit


SOURCE_RECORD_TYPE = "ingestion-source"
SOURCE_SCHEMA_VERSION = 1
SOURCE_SERVING_ID = "source-serving"
SOURCE_SERVING_RECORD_TYPE = "source-serving"
SOURCE_SERVING_SCHEMA_VERSION = 1
DRIVE_COORDINATOR_ID = "drive-coordinator"


class SourceRegistryError(ValueError):
    """A persisted source registration is not a supported SharePoint contract."""


@dataclass(frozen=True)
class SharePointSource:
    source_id: str
    site_id: str
    site_url: str
    drive_id: str
    desired_state: str

    @classmethod
    def from_item(cls, item: object) -> "SharePointSource":
        if not isinstance(item, dict):
            raise SourceRegistryError("source_record_invalid")
        allowed_fields = {
            "id", "sourceId", "recordType", "schemaVersion", "sourceKind",
            "desiredState", "config", "_etag", "_rid", "_self", "_attachments", "_ts",
        }
        if set(item) - allowed_fields:
            raise SourceRegistryError("source_record_invalid")
        source_id = item.get("sourceId")
        if (
            item.get("id") != source_id
            or not isinstance(source_id, str)
            or not source_id.strip()
            or item.get("recordType") != SOURCE_RECORD_TYPE
            or type(item.get("schemaVersion")) is not int
            or item["schemaVersion"] != SOURCE_SCHEMA_VERSION
            or item.get("sourceKind") != "sharepoint-drive"
            or item.get("desiredState") not in {"active", "paused", "retired"}
        ):
            raise SourceRegistryError("source_record_invalid")
        validate_source_id(source_id)
        config = item.get("config")
        if not isinstance(config, dict) or set(config) != {"siteId", "siteUrl", "driveId"}:
            raise SourceRegistryError("source_config_invalid")
        site_id = _required_text(config.get("siteId"), "siteId")
        site_url = validate_site_url(_required_text(config.get("siteUrl"), "siteUrl"))
        drive_id = _required_text(config.get("driveId"), "driveId")
        return cls(
            source_id=source_id,
            site_id=site_id,
            site_url=site_url,
            drive_id=drive_id,
            desired_state=item["desiredState"],
        )


def validate_source_id(source_id: object) -> str:
    """Enforce Cosmos DB item-id limits: at most 1,023 bytes, no '/' or '\\'."""
    if (
        not isinstance(source_id, str)
        or not source_id.strip()
        or len(source_id.encode("utf-8")) > 1023
        or any(character in "/\\" or ord(character) < 32 for character in source_id)
    ):
        raise SourceRegistryError("source_sourceId_invalid")
    return source_id


def validate_site_url(site_url: str) -> str:
    """Return the HTTPS site URL without a trailing slash."""
    try:
        parsed = urlsplit(site_url)
        port = parsed.port
    except ValueError as error:
        raise SourceRegistryError("source_site_url_invalid") from error
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or any(character.isspace() or ord(character) < 32 for character in site_url)
    ):
        raise SourceRegistryError("source_site_url_invalid")
    return site_url.rstrip("/")


def source_binding_digest(source: SharePointSource, tenant_id: str) -> str:
    """Return the stable digest used to bind registry and serving-control records."""
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise SourceRegistryError("source_tenant_id_invalid")
    parsed = urlsplit(source.site_url)
    canonical_binding = [
        source.source_id,
        tenant_id.strip().casefold(),
        source.site_id,
        f"https://{parsed.hostname.casefold()}{parsed.path.rstrip('/')}".casefold(),
        source.drive_id,
    ]
    payload = json.dumps(canonical_binding, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def drive_reservation_digest(drive_id: str, tenant_id: str) -> str:
    if not isinstance(drive_id, str) or not drive_id.strip():
        raise SourceRegistryError("source_drive_id_invalid")
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        raise SourceRegistryError("source_tenant_id_invalid")
    payload = json.dumps(
        [tenant_id.strip().casefold(), drive_id.strip()],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SourceRegistryError(f"source_{field}_invalid")
    return value.strip()