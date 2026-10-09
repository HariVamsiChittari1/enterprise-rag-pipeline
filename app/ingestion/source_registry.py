"""Cosmos-backed source registration and serving-control operations."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Iterable

from azure.core import MatchConditions
from azure.cosmos.exceptions import CosmosResourceNotFoundError

from source_registry import (
    DRIVE_COORDINATOR_ID,
    SOURCE_RECORD_TYPE,
    SOURCE_SERVING_ID,
    SOURCE_SERVING_RECORD_TYPE,
    SOURCE_SERVING_SCHEMA_VERSION,
    SharePointSource,
    SourceRegistryError,
    drive_reservation_digest,
    source_binding_digest,
)


class SourceRegistryConflictError(RuntimeError):
    """A source binding or serving-control transition conflicted."""


@dataclass(frozen=True)
class SourcePlan:
    source: SharePointSource
    catalog_etag: str
    binding_digest: str

    @property
    def source_id(self) -> str:
        return self.source.source_id

    def to_payload(self) -> dict[str, str]:
        return {
            "sourceId": self.source.source_id,
            "siteId": self.source.site_id,
            "siteUrl": self.source.site_url,
            "driveId": self.source.drive_id,
            "desiredState": self.source.desired_state,
            "catalogEtag": self.catalog_etag,
            "bindingDigest": self.binding_digest,
        }

    @classmethod
    def from_payload(cls, payload: object) -> "SourcePlan":
        if not isinstance(payload, dict):
            raise SourceRegistryError("source_plan_invalid")
        source_item = {
            "id": payload.get("sourceId"),
            "sourceId": payload.get("sourceId"),
            "recordType": SOURCE_RECORD_TYPE,
            "schemaVersion": 1,
            "sourceKind": "sharepoint-drive",
            "desiredState": payload.get("desiredState"),
            "config": {
                "siteId": payload.get("siteId"),
                "siteUrl": payload.get("siteUrl"),
                "driveId": payload.get("driveId"),
            },
        }
        source = SharePointSource.from_item(source_item)
        catalog_etag = payload.get("catalogEtag")
        binding_digest = payload.get("bindingDigest")
        if not isinstance(catalog_etag, str) or not catalog_etag:
            raise SourceRegistryError("source_plan_etag_invalid")
        if not isinstance(binding_digest, str) or re.fullmatch(r"[0-9a-f]{64}", binding_digest) is None:
            raise SourceRegistryError("source_plan_binding_invalid")
        return cls(source, catalog_etag, binding_digest)


@dataclass(frozen=True)
class SourceInventoryPage:
    plans: tuple[SourcePlan, ...]
    rejected_source_ids: tuple[str, ...]
    unattributed_errors: int
    duplicate_drive_source_ids: tuple[str, ...]
    continuation_token: str | None


@dataclass(frozen=True)
class SourceDispatchPage:
    items: tuple[dict[str, Any], ...]
    continuation_token: str | None


@dataclass(frozen=True)
class DeltaReducedItemsPage:
    items: tuple[dict[str, Any], ...]
    continuation_token: str | None


DELTA_PAGE_RECORD_TYPE = "delta-page-receipt"
DELTA_PAGE_FRAGMENT_RECORD_TYPE = "delta-page-fragment"
DELTA_ITEM_RECORD_TYPE = "delta-item-state"
DELTA_ROUND_RECORD_TYPE = "delta-round"
DELTA_PAGE_FRAGMENT_CHARS = 256 * 1024
SOURCE_REPROVISION_ID = "source-reprovision"
SOURCE_REPROVISION_RECORD_TYPE = "source-reprovision"
REPROVISION_OPERATIONAL_RECORD_TYPES = frozenset({
    DELTA_ITEM_RECORD_TYPE,
    DELTA_PAGE_FRAGMENT_RECORD_TYPE,
    DELTA_PAGE_RECORD_TYPE,
    DELTA_ROUND_RECORD_TYPE,
    "document-operation",
    "document-operation-recovery",
    "document-winner",
    "source-dispatch-item",
})
REPROVISION_OPERATIONAL_IDS = frozenset({
    "delta-control",
    "source-control",
    "webhook-subscription",
})


class SourceRegistryRepository:
    """Own source-catalog reads and per-source/binding controls in Cosmos."""

    def __init__(self, sources: Any, ingestion_runs: Any, tenant_id: str) -> None:
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("source_registry_tenant_required")
        self._sources = sources
        self._ingestion_runs = ingestion_runs
        self._tenant_id = tenant_id.strip()

    def register_sharepoint_source(
        self,
        *,
        source_id: str,
        site_id: str,
        site_url: str,
        drive_id: str,
    ) -> SourcePlan:
        """Create a paused source and accept it; an exact retry completes an interrupted one.

        The drive reservation is the uniqueness authority; the pre-check only avoids
        writing a catalog record that acceptance would reject.
        """
        body = {
            "id": source_id,
            "sourceId": source_id,
            "recordType": SOURCE_RECORD_TYPE,
            "schemaVersion": 1,
            "sourceKind": "sharepoint-drive",
            "desiredState": "paused",
            "config": {"siteId": site_id, "siteUrl": site_url, "driveId": drive_id},
        }
        requested = SharePointSource.from_item(body)
        body["config"]["siteUrl"] = requested.site_url
        if self._drive_reservation_owner(requested.drive_id) not in (None, source_id):
            raise SourceRegistryConflictError("sharepoint_drive_reservation_conflict")
        try:
            stored = self._sources.create_item(body=body)
        except Exception as error:
            if getattr(error, "status_code", None) != 409:
                raise
            stored = self._sources.read_item(item=source_id, partition_key=source_id)
        source = SharePointSource.from_item(stored)
        if (source.site_id, source.site_url, source.drive_id) != (
            requested.site_id, requested.site_url, requested.drive_id,
        ):
            raise SourceRegistryConflictError("source_id_conflict")
        etag = stored.get("_etag")
        if not isinstance(etag, str) or not etag:
            raise SourceRegistryError("source_catalog_etag_missing")
        return self.accept_source_plan(
            SourcePlan(source, etag, source_binding_digest(source, self._tenant_id))
        )

    def change_source_state(self, *, source_id: str, desired_state: str, if_match: str) -> SourcePlan:
        """Replace ``desiredState`` under ``If-Match``, then record the lifecycle.

        Retrieval and triggers read the catalog, so the change takes effect after the
        replace. If recording the lifecycle fails, repeating the same state with the new
        ETag completes it.
        """
        try:
            item = self._sources.read_item(item=source_id, partition_key=source_id)
        except CosmosResourceNotFoundError as error:
            raise SourceRegistryError("source_registration_missing") from error
        source = SharePointSource.from_item(item)
        if item.get("_etag") != if_match:
            raise SourceRegistryConflictError("source_etag_mismatch")
        gate = self._read_gate(source_id)
        if desired_state != "retired" and (
            source.desired_state == "retired" or (gate is not None and gate.get("retired") is True)
        ):
            raise SourceRegistryConflictError("retired_source_cannot_be_reactivated")
        if (
            desired_state == "active"
            and gate is not None
            and gate.get("fullSyncRequired") is True
        ):
            raise SourceRegistryConflictError("source_full_sync_required")
        if desired_state == "active":
            self._verify_source_acceptance(
                SourcePlan(source, if_match, source_binding_digest(source, self._tenant_id))
            )
        if source.desired_state != desired_state:
            body = {key: value for key, value in item.items() if not key.startswith("_")}
            body["desiredState"] = desired_state
            try:
                item = self._sources.replace_item(
                    item=source_id,
                    body=body,
                    etag=if_match,
                    match_condition=MatchConditions.IfNotModified,
                )
            except Exception as error:
                if getattr(error, "status_code", None) == 412:
                    raise SourceRegistryConflictError("source_etag_mismatch") from error
                raise
            source = SharePointSource.from_item(item)
        plan = SourcePlan(source, item["_etag"], source_binding_digest(source, self._tenant_id))
        try:
            self.observe_lifecycle(plan)
        except Exception as error:
            raise SourceRegistryError("source_lifecycle_pending") from error
        return plan

    def get_drive_coordinator(self, source_id: str) -> dict[str, Any] | None:
        try:
            return self._ingestion_runs.read_item(
                item=DRIVE_COORDINATOR_ID,
                partition_key=source_id,
            )
        except CosmosResourceNotFoundError:
            return None

    def is_reprovision_full_sync_required(self, source_id: str) -> bool:
        gate = self._read_gate(source_id)
        return (
            gate is not None
            and gate.get("retired") is False
            and gate.get("fullSyncRequired") is True
        )

    def acquire_drive_coordinator(
        self,
        plan: SourcePlan,
        *,
        owner_instance_id: str,
        operation: str,
        parent_run_id: str | None = None,
        allow_reprovision_bootstrap: bool = False,
    ) -> dict[str, Any]:
        if not owner_instance_id or operation not in {
            "full-sync", "delta-sync", "acl-resync", "lifecycle-reconcile",
            "audio-poll", "retry-failed",
        }:
            raise ValueError("drive_coordinator_request_invalid")
        if plan.source.desired_state != "active" and not (
            allow_reprovision_bootstrap
            and operation in {"full-sync", "delta-sync"}
            and plan.source.desired_state == "paused"
            and self.is_reprovision_full_sync_required(plan.source_id)
        ):
            raise SourceRegistryConflictError("source_not_active")
        current_plan = self.read_source_plan(plan.source_id)
        if current_plan != plan:
            raise SourceRegistryConflictError("source_catalog_changed")
        current = self.get_drive_coordinator(plan.source_id)
        if current is not None:
            if current.get("state") == "maintenance":
                raise SourceRegistryConflictError("drive_coordinator_in_maintenance")
            if current.get("state") == "draining":
                raise SourceRegistryConflictError("drive_coordinator_draining")
            if current.get("state") == "active":
                if current.get("ownerInstanceId") == owner_instance_id:
                    if current.get("acceptedBindingDigest") != plan.binding_digest:
                        raise SourceRegistryConflictError("drive_coordinator_binding_changed")
                    if (
                        current.get("catalogEtag") != plan.catalog_etag
                        and operation != "audio-poll"
                    ):
                        raise SourceRegistryConflictError("drive_coordinator_catalog_changed")
                    return current
                raise SourceRegistryConflictError("drive_writer_already_active")
        body = {
            "id": DRIVE_COORDINATOR_ID,
            "sourceId": plan.source_id,
            "recordType": "drive-coordinator",
            "schemaVersion": 1,
            "acceptedBindingDigest": plan.binding_digest,
            "catalogEtag": plan.catalog_etag,
            "ownerInstanceId": owner_instance_id,
            "operation": operation,
            "parentRunId": parent_run_id,
            "state": "active",
            "activeWorkCount": 0,
            "updatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        if current is None:
            try:
                return self._ingestion_runs.create_item(body=body)
            except Exception as error:
                if getattr(error, "status_code", None) != 409:
                    raise SourceRegistryError("drive_coordinator_acquire_failed") from error
                raise SourceRegistryConflictError("drive_writer_already_active") from None
        try:
            return self._ingestion_runs.replace_item(
                item=DRIVE_COORDINATOR_ID,
                body=body,
                etag=current.get("_etag"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            raise SourceRegistryConflictError("drive_coordinator_acquire_conflict") from error

    def assert_drive_coordinator_owner(
        self,
        source_id: str,
        owner_instance_id: str,
    ) -> dict[str, Any]:
        current = self.get_drive_coordinator(source_id)
        if (
            current is None
            or current.get("state") != "active"
            or current.get("ownerInstanceId") != owner_instance_id
        ):
            raise SourceRegistryConflictError("drive_coordinator_owner_mismatch")
        return current

    def mark_drive_coordinator_draining(
        self,
        source_id: str,
        owner_instance_id: str,
    ) -> dict[str, Any]:
        current = self.assert_drive_coordinator_owner(source_id, owner_instance_id)
        body = dict(current)
        body.pop("_etag", None)
        body.update({
            "state": "draining",
            "updatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        })
        return self._replace_coordinator(body, current)

    def settle_drive_coordinator(
        self,
        source_id: str,
        owner_instance_id: str,
    ) -> dict[str, Any]:
        current = self.get_drive_coordinator(source_id)
        if (
            current is None
            or current.get("ownerInstanceId") != owner_instance_id
            or current.get("state") not in {"active", "draining", "maintenance"}
        ):
            raise SourceRegistryConflictError("drive_coordinator_owner_mismatch")
        if self._active_work_count(current) > 0:
            raise SourceRegistryConflictError("drive_coordinator_work_unresolved")
        body = dict(current)
        body.pop("_etag", None)
        body.update({
            "state": "settled",
            "updatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        })
        return self._replace_coordinator(body, current)

    def begin_operator_maintenance(
        self,
        plan: SourcePlan,
        *,
        owner_instance_id: str,
        operation: str,
    ) -> dict[str, Any]:
        if not owner_instance_id or operation not in {
            "operator-purge", "source-retirement", "source-reprovision",
        }:
            raise ValueError("operator_maintenance_request_invalid")
        reprovision_recovery = (
            operation == "source-reprovision"
            and plan.source.desired_state == "paused"
            and (self._read_gate(plan.source_id) or {}).get("retired") is True
        )
        if operation in {"source-retirement", "source-reprovision"} and (
            plan.source.desired_state != "retired" and not reprovision_recovery
        ):
            raise SourceRegistryConflictError("source_not_retired")
        current_plan = (
            self._read_catalog_source_plan(plan.source_id)
            if reprovision_recovery
            else self.read_source_plan(plan.source_id)
        )
        if current_plan != plan:
            raise SourceRegistryConflictError("source_catalog_changed")
        current = self.get_drive_coordinator(plan.source_id)
        if current is not None:
            if (
                current.get("state") == "maintenance"
                and current.get("operation") == operation
                and current.get("ownerInstanceId") == owner_instance_id
                and current.get("acceptedBindingDigest") == plan.binding_digest
                and current.get("catalogEtag") == plan.catalog_etag
                and self._active_work_count(current) == 0
            ):
                return current
            if current.get("state") != "settled" or self._active_work_count(current) != 0:
                raise SourceRegistryConflictError("drive_coordinator_not_quiescent")
            body = dict(current)
            body.pop("_etag", None)
            body.update({
                "ownerInstanceId": owner_instance_id,
                "operation": operation,
                "acceptedBindingDigest": plan.binding_digest,
                "catalogEtag": plan.catalog_etag,
                "state": "maintenance",
                "activeWorkCount": 0,
                "updatedAt": _utc_timestamp(),
            })
            try:
                return self._ingestion_runs.replace_item(
                    item=DRIVE_COORDINATOR_ID,
                    body=body,
                    etag=current.get("_etag"),
                    match_condition=MatchConditions.IfNotModified,
                )
            except Exception as error:
                raise SourceRegistryConflictError("operator_maintenance_acquire_conflict") from error
        body = {
            "id": DRIVE_COORDINATOR_ID,
            "sourceId": plan.source_id,
            "recordType": "drive-coordinator",
            "schemaVersion": 1,
            "acceptedBindingDigest": plan.binding_digest,
            "catalogEtag": plan.catalog_etag,
            "ownerInstanceId": owner_instance_id,
            "operation": operation,
            "parentRunId": None,
            "state": "maintenance",
            "activeWorkCount": 0,
            "updatedAt": _utc_timestamp(),
        }
        try:
            return self._ingestion_runs.create_item(body=body)
        except Exception as error:
            raise SourceRegistryConflictError("operator_maintenance_acquire_conflict") from error

    def inspect_source_reprovision(
        self,
        *,
        source_id: str,
        if_match: str,
    ) -> SourcePlan:
        """Validate that a completed retirement can be safely reprovisioned."""
        plan = self._read_catalog_source_plan(source_id)
        if plan.catalog_etag != if_match:
            raise SourceRegistryConflictError("source_etag_mismatch")
        if plan.source.desired_state not in {"retired", "paused"}:
            raise SourceRegistryConflictError("source_not_retired")
        self._verify_source_acceptance(plan)
        gate = self._read_gate(source_id)
        if gate is None or gate.get("retired") is not True:
            raise SourceRegistryConflictError("source_retirement_incomplete")
        coordinator = self.get_drive_coordinator(source_id)
        if (
            coordinator is None
            or coordinator.get("state") != "settled"
            or coordinator.get("operation") not in {"source-retirement", "source-reprovision"}
            or self._active_work_count(coordinator) != 0
        ):
            raise SourceRegistryConflictError("drive_coordinator_not_quiescent")
        if (
            plan.source.desired_state == "paused"
            and coordinator.get("operation") != "source-reprovision"
        ):
            raise SourceRegistryConflictError("source_reprovision_recovery_invalid")
        return plan

    def delete_source_reprovision_page(
        self,
        plan: SourcePlan,
        *,
        owner_instance_id: str,
        operator_object_id: str,
        operator: str,
        page_size: int,
    ) -> dict[str, int | bool]:
        """Delete one bounded page of source operational state, retaining identity controls."""
        if (
            page_size < 1
            or page_size > 100
            or not owner_instance_id
            or not operator_object_id
        ):
            raise ValueError("source_reprovision_page_size_invalid")
        intent = self._ensure_source_reprovision_intent(
            plan,
            owner_instance_id=owner_instance_id,
            operator_object_id=operator_object_id,
            operator=operator,
        )
        self.begin_operator_maintenance(
            plan,
            owner_instance_id=owner_instance_id,
            operation="source-reprovision",
        )
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT c.id, c._etag, c.recordType FROM c "
                    "WHERE c.id != @sourceBinding AND c.id != @sourceServing "
                    "AND c.id != @driveCoordinator AND c.id != @sourceReprovision"
                ),
                parameters=[
                    {"name": "@sourceBinding", "value": "source-binding"},
                    {"name": "@sourceServing", "value": SOURCE_SERVING_ID},
                    {"name": "@driveCoordinator", "value": DRIVE_COORDINATOR_ID},
                    {"name": "@sourceReprovision", "value": SOURCE_REPROVISION_ID},
                ],
                partition_key=plan.source_id,
                max_item_count=page_size,
            )
            pager = iterator.by_page()
            rows = list(next(pager, []))
        except Exception as error:
            raise SourceRegistryError("source_reprovision_query_failed") from error
        if not rows:
            return {
                "deletedRecords": intent.get("deletedOperationalRecords", 0),
                "hasMore": False,
            }
        if any(not _is_reprovision_operational_record(row) for row in rows):
            raise SourceRegistryConflictError("source_reprovision_unknown_record")
        delete_operations = [
            ("delete", (row["id"],), {"if_match_etag": row["_etag"]})
            for row in rows
            if isinstance(row.get("id"), str) and isinstance(row.get("_etag"), str)
        ]
        if len(delete_operations) != len(rows):
            raise SourceRegistryError("source_reprovision_record_invalid")
        intent_body = {
            key: value for key, value in intent.items() if not key.startswith("_")
        }
        intent_body.update({
            "phase": "cleaning",
            "deletedOperationalRecords": (
                intent.get("deletedOperationalRecords", 0) + len(rows)
            ),
            "updatedAt": _utc_timestamp(),
        })
        operations = delete_operations + [(
            "replace",
            (SOURCE_REPROVISION_ID, intent_body),
            {"if_match_etag": intent["_etag"]},
        )]
        try:
            results = self._ingestion_runs.execute_item_batch(
                batch_operations=operations,
                partition_key=plan.source_id,
            )
        except Exception as error:
            intent_after = self._read_work_item(plan.source_id, SOURCE_REPROVISION_ID)
            if (
                intent_after is not None
                and intent_after.get("ownerInstanceId") == owner_instance_id
                and intent_after.get("deletedOperationalRecords")
                == intent_body["deletedOperationalRecords"]
            ):
                return {
                    "deletedRecords": intent_after["deletedOperationalRecords"],
                    "hasMore": len(rows) == page_size,
                }
            if getattr(error, "status_code", None) in {409, 412}:
                raise SourceRegistryConflictError("source_reprovision_delete_conflict") from None
            raise SourceRegistryError("source_reprovision_delete_failed") from error
        if len(results) != len(operations) or any(
            not isinstance(result.get("statusCode"), int)
            or not 200 <= result["statusCode"] < 300
            for result in results
        ):
            raise SourceRegistryError("source_reprovision_delete_failed")
        return {
            "deletedRecords": intent_body["deletedOperationalRecords"],
            "hasMore": len(rows) == page_size,
        }

    def complete_source_reprovision(
        self,
        plan: SourcePlan,
        *,
        owner_instance_id: str,
    ) -> SourcePlan:
        """Return a cleaned retired source to paused while keeping it fail closed."""
        current = self._read_catalog_source_plan(plan.source_id)
        if current.binding_digest != plan.binding_digest:
            raise SourceRegistryConflictError("source_catalog_changed")
        coordinator = self.get_drive_coordinator(plan.source_id)
        gate = self._read_gate(plan.source_id)
        intent = self._read_work_item(plan.source_id, SOURCE_REPROVISION_ID)
        if (
            current.source.desired_state == "paused"
            and gate is not None
            and gate.get("retired") is False
            and gate.get("fullSyncRequired") is True
            and intent is not None
            and intent.get("phase") == "awaiting-full-sync"
            and intent.get("originalCatalogEtag") == plan.catalog_etag
            and intent.get("acceptedBindingDigest") == plan.binding_digest
            and coordinator is not None
            and coordinator.get("state") == "settled"
            and coordinator.get("ownerInstanceId") == owner_instance_id
        ):
            return current
        if (
            coordinator is None
            or coordinator.get("state") != "maintenance"
            or coordinator.get("operation") != "source-reprovision"
            or coordinator.get("ownerInstanceId") != owner_instance_id
            or self._active_work_count(coordinator) != 0
        ):
            raise SourceRegistryConflictError("drive_coordinator_owner_mismatch")
        if (
            gate is None
            or intent is None
            or intent.get("ownerInstanceId") != owner_instance_id
            or intent.get("originalCatalogEtag") != plan.catalog_etag
            or intent.get("acceptedBindingDigest") != plan.binding_digest
        ):
            raise SourceRegistryConflictError("source_retirement_incomplete")
        if current.source.desired_state == "retired":
            source_item = self._sources.read_item(
                item=plan.source_id,
                partition_key=plan.source_id,
            )
            source_body = {
                key: value for key, value in source_item.items() if not key.startswith("_")
            }
            source_body["desiredState"] = "paused"
            try:
                replaced = self._sources.replace_item(
                    item=plan.source_id,
                    body=source_body,
                    etag=current.catalog_etag,
                    match_condition=MatchConditions.IfNotModified,
                )
            except Exception as error:
                if getattr(error, "status_code", None) == 412:
                    raise SourceRegistryConflictError("source_etag_mismatch") from None
                raise SourceRegistryError("source_reprovision_catalog_update_failed") from error
            current = SourcePlan(
                SharePointSource.from_item(replaced),
                replaced["_etag"],
                plan.binding_digest,
            )
        elif current.source.desired_state != "paused":
            raise SourceRegistryConflictError("source_catalog_changed")
        if gate.get("retired") is not True:
            raise SourceRegistryConflictError("source_retirement_incomplete")

        gate_body = {
            "id": SOURCE_SERVING_ID,
            "sourceId": plan.source_id,
            "recordType": SOURCE_SERVING_RECORD_TYPE,
            "schemaVersion": SOURCE_SERVING_SCHEMA_VERSION,
            "retired": False,
            "fullSyncRequired": True,
        }
        coordinator_body = {
            key: value for key, value in coordinator.items() if not key.startswith("_")
        }
        coordinator_body.update({
            "catalogEtag": current.catalog_etag,
            "state": "settled",
            "updatedAt": _utc_timestamp(),
        })
        intent_body = {
            key: value for key, value in intent.items() if not key.startswith("_")
        }
        intent_body.update({
            "phase": "awaiting-full-sync",
            "catalogEtag": current.catalog_etag,
            "updatedAt": _utc_timestamp(),
        })
        operations = [
            (
                "replace",
                (SOURCE_SERVING_ID, gate_body),
                {"if_match_etag": gate["_etag"]},
            ),
            (
                "replace",
                (DRIVE_COORDINATOR_ID, coordinator_body),
                {"if_match_etag": coordinator["_etag"]},
            ),
            (
                "replace",
                (SOURCE_REPROVISION_ID, intent_body),
                {"if_match_etag": intent["_etag"]},
            ),
        ]
        try:
            results = self._ingestion_runs.execute_item_batch(
                batch_operations=operations,
                partition_key=plan.source_id,
            )
            if len(results) != len(operations) or any(
                not isinstance(result.get("statusCode"), int)
                or not 200 <= result["statusCode"] < 300
                for result in results
            ):
                raise SourceRegistryError("source_reprovision_finalize_failed")
        except Exception as error:
            gate_after = self._read_gate(plan.source_id)
            intent_after = self._read_work_item(plan.source_id, SOURCE_REPROVISION_ID)
            coordinator_after = self.get_drive_coordinator(plan.source_id)
            if not (
                gate_after is not None
                and gate_after.get("retired") is False
                and gate_after.get("fullSyncRequired") is True
                and intent_after is not None
                and intent_after.get("phase") == "awaiting-full-sync"
                and coordinator_after is not None
                and coordinator_after.get("state") == "settled"
            ):
                raise SourceRegistryError("source_reprovision_finalize_failed") from error

        self._verify_source_acceptance(current)
        return current

    def fail_source_reprovision(
        self,
        plan: SourcePlan,
        *,
        owner_instance_id: str,
        error_code: str,
    ) -> None:
        """Persist a failed attempt and release maintenance while retirement remains closed."""
        intent = self._read_work_item(plan.source_id, SOURCE_REPROVISION_ID)
        coordinator = self.get_drive_coordinator(plan.source_id)
        if (
            intent is None
            or coordinator is None
            or intent.get("ownerInstanceId") != owner_instance_id
            or coordinator.get("ownerInstanceId") != owner_instance_id
            or coordinator.get("state") != "maintenance"
        ):
            return
        intent_body = {
            key: value for key, value in intent.items() if not key.startswith("_")
        }
        intent_body.update({
            "phase": "failed",
            "errorCode": error_code,
            "updatedAt": _utc_timestamp(),
        })
        coordinator_body = {
            key: value for key, value in coordinator.items() if not key.startswith("_")
        }
        coordinator_body.update({"state": "settled", "updatedAt": _utc_timestamp()})
        try:
            self._ingestion_runs.execute_item_batch(
                batch_operations=[
                    (
                        "replace",
                        (SOURCE_REPROVISION_ID, intent_body),
                        {"if_match_etag": intent["_etag"]},
                    ),
                    (
                        "replace",
                        (DRIVE_COORDINATOR_ID, coordinator_body),
                        {"if_match_etag": coordinator["_etag"]},
                    ),
                ],
                partition_key=plan.source_id,
            )
        except Exception as error:
            raise SourceRegistryError("source_reprovision_failure_record_failed") from error

    def complete_reprovision_full_sync(
        self,
        plan: SourcePlan,
        *,
        owner_instance_id: str,
    ) -> None:
        """Clear the bootstrap latch only after the paused source's full sync committed."""
        if plan.source.desired_state != "paused":
            raise SourceRegistryConflictError("source_reprovision_bootstrap_not_pending")
        gate = self._read_gate(plan.source_id)
        intent = self._read_work_item(plan.source_id, SOURCE_REPROVISION_ID)
        coordinator = self.get_drive_coordinator(plan.source_id)
        if (
            gate is None
            or gate.get("retired") is not False
            or intent is None
            or coordinator is None
            or coordinator.get("ownerInstanceId") != owner_instance_id
            or coordinator.get("state") != "settled"
        ):
            raise SourceRegistryConflictError("source_reprovision_bootstrap_not_committed")
        if (
            gate.get("fullSyncRequired") is False
            and intent.get("phase") == "completed"
        ):
            return
        if (
            gate.get("fullSyncRequired") is not True
            or intent.get("phase") != "awaiting-full-sync"
        ):
            raise SourceRegistryConflictError("source_reprovision_bootstrap_not_pending")
        gate_body = {
            key: value for key, value in gate.items() if not key.startswith("_")
        }
        gate_body["fullSyncRequired"] = False
        intent_body = {
            key: value for key, value in intent.items() if not key.startswith("_")
        }
        intent_body.update({"phase": "completed", "updatedAt": _utc_timestamp()})
        try:
            self._ingestion_runs.execute_item_batch(
                batch_operations=[
                    (
                        "replace",
                        (SOURCE_SERVING_ID, gate_body),
                        {"if_match_etag": gate["_etag"]},
                    ),
                    (
                        "replace",
                        (SOURCE_REPROVISION_ID, intent_body),
                        {"if_match_etag": intent["_etag"]},
                    ),
                ],
                partition_key=plan.source_id,
            )
        except Exception as error:
            gate_after = self._read_gate(plan.source_id)
            intent_after = self._read_work_item(plan.source_id, SOURCE_REPROVISION_ID)
            if not (
                gate_after is not None
                and gate_after.get("fullSyncRequired") is False
                and intent_after is not None
                and intent_after.get("phase") == "completed"
            ):
                raise SourceRegistryError(
                    "source_reprovision_bootstrap_complete_failed"
                ) from error

    def _ensure_source_reprovision_intent(
        self,
        plan: SourcePlan,
        *,
        owner_instance_id: str,
        operator_object_id: str,
        operator: str,
    ) -> dict[str, Any]:
        existing = self._read_work_item(plan.source_id, SOURCE_REPROVISION_ID)
        if (
            existing is not None
            and existing.get("ownerInstanceId") == owner_instance_id
            and existing.get("originalCatalogEtag") == plan.catalog_etag
            and existing.get("acceptedBindingDigest") == plan.binding_digest
            and existing.get("operatorObjectId") == operator_object_id
        ):
            return existing
        body = {
            "id": SOURCE_REPROVISION_ID,
            "sourceId": plan.source_id,
            "recordType": SOURCE_REPROVISION_RECORD_TYPE,
            "schemaVersion": 1,
            "ownerInstanceId": owner_instance_id,
            "originalCatalogEtag": plan.catalog_etag,
            "acceptedBindingDigest": plan.binding_digest,
            "operatorObjectId": operator_object_id,
            "operator": operator or "unknown",
            "phase": "prepared",
            "deletedOperationalRecords": (
                existing.get("deletedOperationalRecords", 0) if existing is not None else 0
            ),
            "createdAt": (
                existing.get("createdAt", _utc_timestamp())
                if existing is not None
                else _utc_timestamp()
            ),
            "updatedAt": _utc_timestamp(),
        }
        if existing is None:
            return self._ingestion_runs.create_item(body=body)
        coordinator = self.get_drive_coordinator(plan.source_id)
        replaceable_ended_intent = (
            existing.get("phase") in {"prepared", "cleaning", "failed"}
            and coordinator is not None
            and coordinator.get("state") == "settled"
            and self._active_work_count(coordinator) == 0
        )
        if not replaceable_ended_intent:
            raise SourceRegistryConflictError("source_reprovision_already_running")
        try:
            return self._ingestion_runs.replace_item(
                item=SOURCE_REPROVISION_ID,
                body=body,
                etag=existing["_etag"],
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            raise SourceRegistryConflictError("source_reprovision_intent_conflict") from error

    def begin_document_operation(
        self,
        plan: SourcePlan,
        *,
        owner_instance_id: str,
        physical_run_id: str,
        item_id: str,
        operation_kind: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create one stable operation record before its activity performs side effects."""
        if (
            not owner_instance_id
            or not physical_run_id
            or not item_id
            or operation_kind not in {
                "full-sync", "retry-failed", "delta-sync", "delta-delete",
                "delta-acl", "delta-cleanup", "acl-resync", "lifecycle-reconcile",
                "duplicate-reconcile", "orphan-reconcile",
            }
            and not (
                isinstance(operation_kind, str)
                and operation_kind.startswith("retry-failed:")
                and len(operation_kind) > len("retry-failed:")
            )
        ):
            raise ValueError("document_operation_request_invalid")
        operation_id = _document_operation_id(
            plan.source_id, physical_run_id, item_id, operation_kind,
        )
        for _ in range(3):
            coordinator = self.get_drive_coordinator(plan.source_id)
            if (
                coordinator is None
                or coordinator.get("ownerInstanceId") != owner_instance_id
            ):
                raise SourceRegistryConflictError("drive_coordinator_owner_mismatch")
            operation = self._read_work_item(plan.source_id, operation_id)
            if operation is not None:
                self._assert_operation_identity(
                    operation,
                    plan.source_id,
                    physical_run_id,
                    item_id,
                    operation_kind,
                )
                state = operation.get("state")
                if state in {"succeeded", "failed"}:
                    replay = {
                        "status": "completed",
                        "terminalStatus": state,
                        "outcome": operation.get("outcome"),
                        "operationId": operation_id,
                    }
                    if operation.get("metadata") is not None:
                        replay["metadata"] = operation["metadata"]
                    return replay
                if state == "claimed":
                    return {"status": "acquired", "operationId": operation_id}
                return {"status": "unresolved", "operationId": operation_id}
            if coordinator.get("state") != "active":
                return {"status": "draining"}

            body = dict(coordinator)
            body.pop("_etag", None)
            body["activeWorkCount"] = self._active_work_count(coordinator) + 1
            body["updatedAt"] = _utc_timestamp()
            operation = {
                "id": operation_id,
                "sourceId": plan.source_id,
                "recordType": "document-operation",
                "schemaVersion": 1,
                "physicalRunId": physical_run_id,
                "itemId": item_id,
                "operationKind": operation_kind,
                "ownerInstanceId": owner_instance_id,
                "state": "claimed",
                "createdAt": _utc_timestamp(),
                "updatedAt": _utc_timestamp(),
            }
            if metadata is not None:
                operation["metadata"] = metadata
            try:
                self._execute_work_batch(plan.source_id, [
                    (
                        "replace",
                        (DRIVE_COORDINATOR_ID, body),
                        {"if_match_etag": coordinator.get("_etag")},
                    ),
                    ("create", (operation,), {}),
                ])
                return {"status": "acquired", "operationId": operation_id}
            except SourceRegistryConflictError:
                continue
            except Exception as error:
                persisted = self._read_work_item(plan.source_id, operation_id)
                if persisted is not None:
                    self._assert_operation_identity(
                        persisted,
                        plan.source_id,
                        physical_run_id,
                        item_id,
                        operation_kind,
                    )
                    return {"status": "unresolved", "operationId": operation_id}
                raise SourceRegistryError("document_operation_admission_failed") from error
        coordinator = self.get_drive_coordinator(plan.source_id)
        if coordinator is not None and coordinator.get("state") == "draining":
            return {"status": "draining"}
        operation = self._read_work_item(plan.source_id, operation_id)
        if operation is not None:
            self._assert_operation_identity(
                operation,
                plan.source_id,
                physical_run_id,
                item_id,
                operation_kind,
            )
            if operation.get("state") in {"succeeded", "failed"}:
                return {
                    "status": "completed",
                    "terminalStatus": operation["state"],
                    "outcome": operation.get("outcome"),
                    "operationId": operation_id,
                }
            return {"status": "unresolved", "operationId": operation_id}
        raise SourceRegistryConflictError("document_operation_admission_conflict")

    def start_document_operation(
        self,
        source_id: str,
        owner_instance_id: str,
        operation_id: str,
        physical_run_id: str,
        item_id: str,
        operation_kind: str,
    ) -> dict[str, Any]:
        """Move one claimed operation to running exactly once before side effects."""
        for _ in range(3):
            coordinator = self.get_drive_coordinator(source_id)
            operation = self._read_work_item(source_id, operation_id)
            if operation is None:
                raise SourceRegistryConflictError("document_operation_missing")
            self._assert_operation_identity(
                operation, source_id, physical_run_id, item_id, operation_kind,
            )
            if operation.get("state") in {"succeeded", "failed"}:
                return {"status": "completed", "outcome": operation.get("outcome")}
            if operation.get("state") in {"running", "unresolved"}:
                return {"status": "unresolved"}
            if (
                coordinator is None
                or coordinator.get("ownerInstanceId") != owner_instance_id
                or coordinator.get("state") not in {"active", "draining"}
                or operation.get("ownerInstanceId") != owner_instance_id
            ):
                raise SourceRegistryConflictError("document_operation_owner_mismatch")
            if operation.get("state") != "claimed":
                raise SourceRegistryConflictError("document_operation_not_claimed")

            coordinator_body = dict(coordinator)
            coordinator_body.pop("_etag", None)
            coordinator_body["updatedAt"] = _utc_timestamp()
            operation_body = dict(operation)
            operation_body.pop("_etag", None)
            operation_body.update({"state": "running", "startedAt": _utc_timestamp()})
            try:
                self._execute_work_batch(source_id, [
                    (
                        "replace",
                        (DRIVE_COORDINATOR_ID, coordinator_body),
                        {"if_match_etag": coordinator.get("_etag")},
                    ),
                    (
                        "replace", (operation_id, operation_body),
                        {"if_match_etag": operation.get("_etag")},
                    ),
                ])
                return {"status": "started"}
            except SourceRegistryConflictError:
                continue
            except Exception as error:
                persisted = self._read_work_item(source_id, operation_id)
                if persisted is not None and persisted.get("state") == "running":
                    return {"status": "started"}
                raise SourceRegistryError("document_operation_start_failed") from error
        raise SourceRegistryConflictError("document_operation_start_conflict")

    def settle_document_operation(
        self,
        source_id: str,
        owner_instance_id: str,
        operation_id: str,
        *,
        physical_run_id: str,
        item_id: str,
        operation_kind: str,
        status: str,
        outcome: dict[str, Any],
    ) -> dict[str, Any]:
        """Persist an unresolved or terminal result using the operation ETag."""
        if status not in {"unresolved", "succeeded", "failed"} or not isinstance(outcome, dict):
            raise ValueError("document_operation_settlement_invalid")
        for _ in range(3):
            coordinator = self.get_drive_coordinator(source_id)
            operation = self._read_work_item(source_id, operation_id)
            if operation is None:
                raise SourceRegistryConflictError("document_operation_missing")
            self._assert_operation_identity(
                operation, source_id, physical_run_id, item_id, operation_kind,
            )
            if operation.get("state") in {"succeeded", "failed"}:
                if operation.get("state") != status:
                    raise SourceRegistryConflictError("document_operation_terminal_mismatch")
                return {"status": "completed", "outcome": operation.get("outcome")}
            if operation.get("state") == "unresolved" and status == "unresolved":
                return {"status": "unresolved", "outcome": operation.get("outcome")}
            if (
                operation.get("state") not in {"running", "unresolved"}
                or operation.get("ownerInstanceId") != owner_instance_id
            ):
                raise SourceRegistryConflictError("document_operation_not_settleable")
            if (
                coordinator is None
                or coordinator.get("ownerInstanceId") != owner_instance_id
                or coordinator.get("state") not in {"active", "draining"}
            ):
                raise SourceRegistryConflictError("document_operation_owner_mismatch")

            body = dict(coordinator)
            body.pop("_etag", None)
            if status in {"succeeded", "failed"}:
                active_count = self._active_work_count(coordinator)
                if active_count <= 0:
                    raise SourceRegistryConflictError("document_operation_count_invalid")
                body["activeWorkCount"] = active_count - 1
            body["updatedAt"] = _utc_timestamp()
            operation_body = dict(operation)
            operation_body.pop("_etag", None)
            operation_body.update({
                "state": status,
                "outcome": outcome,
                "updatedAt": _utc_timestamp(),
            })
            try:
                self._execute_work_batch(source_id, [
                    (
                        "replace",
                        (DRIVE_COORDINATOR_ID, body),
                        {"if_match_etag": coordinator.get("_etag")},
                    ),
                    (
                        "replace", (operation_id, operation_body),
                        {"if_match_etag": operation.get("_etag")},
                    ),
                ])
                return {
                    "status": "completed" if status in {"succeeded", "failed"} else status,
                    "outcome": outcome,
                }
            except SourceRegistryConflictError:
                continue
            except Exception as error:
                persisted = self._read_work_item(source_id, operation_id)
                if persisted is not None and persisted.get("state") == status:
                    return {
                        "status": "completed" if status in {"succeeded", "failed"} else status,
                        "outcome": persisted.get("outcome"),
                    }
                raise SourceRegistryError("document_operation_settlement_failed") from error
        raise SourceRegistryConflictError("document_operation_settlement_conflict")

    def get_unresolved_document_operation_count(self, source_id: str) -> int:
        coordinator = self.get_drive_coordinator(source_id)
        return 0 if coordinator is None else self._active_work_count(coordinator)

    def get_document_winner(self, source_id: str, document_id: str) -> dict[str, Any] | None:
        return self._read_work_item(source_id, _document_winner_id(document_id))

    def record_document_winner(
        self,
        source_id: str,
        owner_instance_id: str,
        *,
        document_id: str,
        drive_id: str,
        item_id: str,
        source_run_id: str,
        document_key: str,
        source_etag: str,
        lifecycle_generation: int,
        processing_profile_digest: str | None = None,
    ) -> dict[str, Any]:
        """Publish one source-scoped ready winner using the coordinator's ETag boundary."""
        if not all((source_id, owner_instance_id, document_id, drive_id, item_id, source_run_id, document_key, source_etag)):
            raise ValueError("document_winner_identity_invalid")
        if isinstance(lifecycle_generation, bool) or not isinstance(lifecycle_generation, int) or lifecycle_generation < 0:
            raise ValueError("document_winner_generation_invalid")
        winner_id = _document_winner_id(document_id)
        for _ in range(3):
            coordinator = self.get_drive_coordinator(source_id)
            current = self._read_work_item(source_id, winner_id)
            if (
                coordinator is None
                or coordinator.get("ownerInstanceId") != owner_instance_id
                or coordinator.get("state") not in {"active", "draining"}
            ):
                raise SourceRegistryConflictError("document_winner_coordinator_owner_mismatch")
            body = {
                "id": winner_id,
                "sourceId": source_id,
                "recordType": "document-winner",
                "schemaVersion": 1,
                "documentId": document_id,
                "driveId": drive_id,
                "itemId": item_id,
                "sourceRunId": source_run_id,
                "documentKey": document_key,
                "sourceEtag": source_etag,
                "lifecycleGeneration": lifecycle_generation,
                "processingProfileDigest": processing_profile_digest,
                "verifiedAt": _utc_timestamp(),
                "ownerInstanceId": owner_instance_id,
            }
            if current is not None and all(
                current.get(key) == body[key]
                for key in (
                    "sourceId", "documentId", "driveId", "itemId", "sourceRunId",
                    "documentKey", "sourceEtag", "lifecycleGeneration",
                    "processingProfileDigest",
                )
            ):
                return current
            coordinator_body = dict(coordinator)
            coordinator_body.pop("_etag", None)
            coordinator_body["updatedAt"] = _utc_timestamp()
            if current is None:
                winner_operation = ("create", (body,), {})
            else:
                winner_operation = (
                    "replace",
                    (winner_id, body),
                    {"if_match_etag": current.get("_etag")},
                )
            try:
                self._execute_work_batch(source_id, [
                    (
                        "replace", (DRIVE_COORDINATOR_ID, coordinator_body),
                        {"if_match_etag": coordinator.get("_etag")},
                    ),
                    winner_operation,
                ])
                return self._read_work_item(source_id, winner_id) or body
            except SourceRegistryConflictError:
                continue
        raise SourceRegistryConflictError("document_winner_update_conflict")

    def clear_document_winner(
        self,
        source_id: str,
        owner_instance_id: str,
        *,
        document_id: str,
        expected_source_run_id: str,
        expected_document_key: str,
    ) -> bool:
        winner_id = _document_winner_id(document_id)
        for _ in range(3):
            coordinator = self.get_drive_coordinator(source_id)
            current = self._read_work_item(source_id, winner_id)
            if current is None:
                return False
            if (
                coordinator is None
                or coordinator.get("ownerInstanceId") != owner_instance_id
                or (
                    coordinator.get("state") not in {"active", "draining"}
                    and not (
                        coordinator.get("state") == "maintenance"
                        and coordinator.get("operation") == "source-retirement"
                    )
                )
                or current.get("documentId") != document_id
                or current.get("sourceRunId") != expected_source_run_id
                or current.get("documentKey") != expected_document_key
            ):
                raise SourceRegistryConflictError("document_winner_clear_identity_mismatch")
            coordinator_body = dict(coordinator)
            coordinator_body.pop("_etag", None)
            coordinator_body["updatedAt"] = _utc_timestamp()
            try:
                self._execute_work_batch(source_id, [
                    (
                        "replace", (DRIVE_COORDINATOR_ID, coordinator_body),
                        {"if_match_etag": coordinator.get("_etag")},
                    ),
                    (
                        "delete", (winner_id,),
                        {"if_match_etag": current.get("_etag")},
                    ),
                ])
                return True
            except SourceRegistryConflictError:
                continue
        raise SourceRegistryConflictError("document_winner_clear_conflict")

    def list_unresolved_document_operations_page(
        self,
        source_id: str,
        owner_instance_id: str,
        operation_kind: str,
        *,
        page_size: int = 1,
    ) -> tuple[dict[str, Any], ...]:
        """Read a bounded page of unresolved operations for activity recovery."""
        if (
            not source_id or not owner_instance_id or not operation_kind
            or isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1
        ):
            raise ValueError("document_operation_query_invalid")
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT c.id, c.itemId, c.physicalRunId, c.state, c.metadata "
                    "FROM c WHERE c.recordType = @recordType AND c.ownerInstanceId = @owner "
                    "AND c.operationKind = @operation AND ARRAY_CONTAINS(@states, c.state)"
                ),
                parameters=[
                    {"name": "@recordType", "value": "document-operation"},
                    {"name": "@owner", "value": owner_instance_id},
                    {"name": "@operation", "value": operation_kind},
                    {"name": "@states", "value": ["claimed", "running", "unresolved"]},
                ],
                partition_key=source_id,
                max_item_count=page_size,
            )
            pager = iterator.by_page()
            rows = list(next(pager, []))
        except Exception:
            raise SourceRegistryError("document_operation_query_failed") from None
        result: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise SourceRegistryError("document_operation_record_invalid")
            if not all(
                isinstance(row.get(field), str) and row[field]
                for field in ("id", "itemId", "physicalRunId", "state")
            ):
                raise SourceRegistryError("document_operation_identity_invalid")
            result.append(row)
        return tuple(result)

    def delta_page_capture(
        self,
        plan: SourcePlan,
        owner_instance_id: str,
        round_id: str,
    ) -> "DeltaPageCapture":
        return DeltaPageCapture(self, plan, owner_instance_id, round_id)

    def begin_delta_round(
        self,
        plan: SourcePlan,
        owner_instance_id: str,
        round_id: str,
        committed_delta_link: str | None,
    ) -> dict[str, Any]:
        if not owner_instance_id or not round_id:
            raise ValueError("delta_round_identity_required")
        round_item_id = _delta_round_item_id(round_id)
        current = self._read_work_item(plan.source_id, round_item_id)
        if current is not None:
            if (
                current.get("roundId") != round_id
                or current.get("ownerInstanceId") != owner_instance_id
                or current.get("acceptedBindingDigest") != plan.binding_digest
            ):
                raise SourceRegistryConflictError("delta_round_identity_mismatch")
            if current.get("state") in {"cursor_ready", "committed"}:
                return current
            if current.get("committedDeltaLink") != committed_delta_link:
                raise SourceRegistryConflictError("delta_round_anchor_changed")
            return current

        for _ in range(3):
            coordinator = self.get_drive_coordinator(plan.source_id)
            if (
                coordinator is None
                or coordinator.get("state") != "active"
                or coordinator.get("ownerInstanceId") != owner_instance_id
            ):
                raise SourceRegistryConflictError("delta_round_coordinator_owner_mismatch")
            body = dict(coordinator)
            body.pop("_etag", None)
            body["updatedAt"] = _utc_timestamp()
            round_item = {
                "id": round_item_id,
                "sourceId": plan.source_id,
                "recordType": DELTA_ROUND_RECORD_TYPE,
                "schemaVersion": 1,
                "roundId": round_id,
                "ownerInstanceId": owner_instance_id,
                "acceptedBindingDigest": plan.binding_digest,
                "catalogEtag": plan.catalog_etag,
                "committedDeltaLink": committed_delta_link,
                "roundStartKind": "initial" if committed_delta_link is None else "incremental",
                "roundStartUrl": committed_delta_link,
                "phase": "enumerating",
                "state": "active",
                "createdAt": _utc_timestamp(),
                "updatedAt": _utc_timestamp(),
            }
            try:
                self._execute_work_batch(plan.source_id, [
                    (
                        "replace", (DRIVE_COORDINATOR_ID, body),
                        {"if_match_etag": coordinator.get("_etag")},
                    ),
                    ("create", (round_item,), {}),
                ])
                return round_item
            except SourceRegistryConflictError:
                current = self._read_work_item(plan.source_id, round_item_id)
                if current is not None:
                    if current.get("roundId") == round_id and current.get("ownerInstanceId") == owner_instance_id:
                        return current
                    raise SourceRegistryConflictError("delta_round_identity_mismatch")
        raise SourceRegistryConflictError("delta_round_begin_conflict")

    def record_delta_round_reset(
        self,
        source_id: str,
        round_id: str,
        owner_instance_id: str,
        reset_location: str,
    ) -> None:
        self._update_delta_round(
            source_id, round_id, owner_instance_id,
            {"roundStartKind": "reset", "roundStartUrl": reset_location, "phase": "reset"},
        )

    def mark_delta_round_cursor_ready(
        self,
        source_id: str,
        round_id: str,
        owner_instance_id: str,
        candidate_delta_link: str,
        outcome: dict[str, Any],
    ) -> dict[str, Any]:
        return self._update_delta_round(
            source_id, round_id, owner_instance_id,
            {
                "state": "cursor_ready",
                "phase": "cursor_ready",
                "candidateDeltaLink": candidate_delta_link,
                "outcome": outcome,
            },
        )

    def complete_delta_round(
        self,
        source_id: str,
        round_id: str,
        owner_instance_id: str,
    ) -> dict[str, Any]:
        return self._update_delta_round(
            source_id, round_id, owner_instance_id,
            {"state": "committed", "phase": "completed"},
        )

    def commit_full_sync_progress(
        self,
        plan: SourcePlan,
        owner_instance_id: str,
        *,
        round_id: str,
        candidate_delta_link: str,
    ) -> dict[str, Any]:
        """Atomically commit a successful full-sync cursor, round, and owner."""
        if not all((owner_instance_id, round_id, candidate_delta_link)):
            raise ValueError("full_sync_progress_identity_invalid")
        current_plan = self.read_source_plan(plan.source_id)
        bootstrap_required = self.is_reprovision_full_sync_required(plan.source_id)
        if current_plan != plan or (
            plan.source.desired_state != "active"
            and not (plan.source.desired_state == "paused" and bootstrap_required)
        ):
            raise SourceRegistryConflictError("source_catalog_changed")
        coordinator = self.get_drive_coordinator(plan.source_id)
        round_item_id = _delta_round_item_id(round_id)
        round_item = self._read_work_item(plan.source_id, round_item_id)
        cursor = self._read_work_item(plan.source_id, "delta-control")
        if (
            coordinator is not None
            and coordinator.get("ownerInstanceId") == owner_instance_id
            and coordinator.get("state") == "settled"
            and round_item is not None
            and round_item.get("roundId") == round_id
            and round_item.get("state") == "committed"
            and round_item.get("candidateDeltaLink") == candidate_delta_link
            and cursor is not None
            and cursor.get("deltaLink") == candidate_delta_link
        ):
            return {"committed": True, "replayed": True}
        if (
            coordinator is None
            or coordinator.get("ownerInstanceId") != owner_instance_id
            or coordinator.get("state") not in {"active", "draining"}
            or self._active_work_count(coordinator) != 0
            or round_item is None
            or round_item.get("roundId") != round_id
            or round_item.get("ownerInstanceId") != owner_instance_id
            or round_item.get("state") != "cursor_ready"
            or round_item.get("candidateDeltaLink") != candidate_delta_link
        ):
            raise SourceRegistryConflictError("full_sync_progress_precondition_failed")
        cursor_body = {
            "id": "delta-control",
            "sourceId": plan.source_id,
            "deltaLink": candidate_delta_link,
            "updatedAt": _utc_timestamp(),
        }
        round_body = dict(round_item)
        round_body.pop("_etag", None)
        round_body.update({"state": "committed", "phase": "completed", "updatedAt": _utc_timestamp()})
        coordinator_body = dict(coordinator)
        coordinator_body.pop("_etag", None)
        coordinator_body.update({"state": "settled", "updatedAt": _utc_timestamp()})
        cursor_operation = (
            ("create", (cursor_body,), {})
            if cursor is None
            else (
                "replace", ("delta-control", cursor_body),
                {"if_match_etag": cursor.get("_etag")},
            )
        )
        operations = [
            (
                "replace", (DRIVE_COORDINATOR_ID, coordinator_body),
                {"if_match_etag": coordinator.get("_etag")},
            ),
            (
                "replace", (round_item_id, round_body),
                {"if_match_etag": round_item.get("_etag")},
            ),
            cursor_operation,
        ]
        self._execute_work_batch(plan.source_id, operations)
        return {"committed": True, "replayed": False}

    def _update_delta_round(
        self,
        source_id: str,
        round_id: str,
        owner_instance_id: str,
        changes: dict[str, Any],
    ) -> dict[str, Any]:
        round_item_id = _delta_round_item_id(round_id)
        for _ in range(3):
            current = self._read_work_item(source_id, round_item_id)
            if current is None or current.get("roundId") != round_id:
                raise SourceRegistryConflictError("delta_round_missing")
            if current.get("ownerInstanceId") != owner_instance_id:
                raise SourceRegistryConflictError("delta_round_owner_mismatch")
            if current.get("state") == "committed":
                return current
            body = dict(current)
            body.pop("_etag", None)
            body.update(changes)
            body["updatedAt"] = _utc_timestamp()
            try:
                return self._ingestion_runs.replace_item(
                    item=round_item_id,
                    body=body,
                    etag=current.get("_etag"),
                    match_condition=MatchConditions.IfNotModified,
                )
            except Exception as error:
                if getattr(error, "status_code", None) not in {409, 412}:
                    raise SourceRegistryError("delta_round_update_failed") from None
        raise SourceRegistryConflictError("delta_round_update_conflict")

    def begin_delta_page_capture(
        self,
        plan: SourcePlan,
        owner_instance_id: str,
        round_id: str,
        page_ordinal: int,
        request_url: str,
        predecessor_receipt_id: str | None,
    ) -> dict[str, Any]:
        if (
            isinstance(page_ordinal, bool) or not isinstance(page_ordinal, int) or page_ordinal < 0
            or not request_url or not round_id or not owner_instance_id
        ):
            raise ValueError("delta_page_capture_request_invalid")
        url_digest = hashlib.sha256(request_url.encode("utf-8")).hexdigest()
        receipt_id = _delta_page_receipt_id(round_id, page_ordinal, url_digest)
        current = self._read_work_item(plan.source_id, receipt_id)
        if current is not None:
            if (
                current.get("roundId") != round_id
                or current.get("pageOrdinal") != page_ordinal
                or current.get("requestUrlDigest") != url_digest
                or current.get("predecessorReceiptId") != predecessor_receipt_id
            ):
                raise SourceRegistryConflictError("delta_page_receipt_identity_mismatch")
            if current.get("state") == "sealed":
                return self._load_sealed_delta_page(plan.source_id, current)
            if current.get("state") == "reset":
                location = current.get("resetLocation")
                if not isinstance(location, str) or not location:
                    raise SourceRegistryError("delta_page_reset_location_missing")
                return {"state": "reset", "location": location, "receiptId": receipt_id}
            raise SourceRegistryConflictError("delta_page_capture_unresolved")

        for _ in range(3):
            coordinator = self.get_drive_coordinator(plan.source_id)
            if (
                coordinator is None
                or coordinator.get("state") != "active"
                or coordinator.get("ownerInstanceId") != owner_instance_id
            ):
                raise SourceRegistryConflictError("delta_page_coordinator_owner_mismatch")
            body = dict(coordinator)
            body.pop("_etag", None)
            body["updatedAt"] = _utc_timestamp()
            receipt = {
                "id": receipt_id,
                "sourceId": plan.source_id,
                "recordType": DELTA_PAGE_RECORD_TYPE,
                "schemaVersion": 1,
                "roundId": round_id,
                "pageOrdinal": page_ordinal,
                "requestUrlDigest": url_digest,
                "requestUrl": request_url,
                "predecessorReceiptId": predecessor_receipt_id,
                "ownerInstanceId": owner_instance_id,
                "state": "capturing",
                "createdAt": _utc_timestamp(),
                "updatedAt": _utc_timestamp(),
            }
            try:
                self._execute_work_batch(plan.source_id, [
                    (
                        "replace", (DRIVE_COORDINATOR_ID, body),
                        {"if_match_etag": coordinator.get("_etag")},
                    ),
                    ("create", (receipt,), {}),
                ])
                return {"state": "capture", "receiptId": receipt_id}
            except SourceRegistryConflictError:
                current = self._read_work_item(plan.source_id, receipt_id)
                if current is not None:
                    if current.get("state") == "sealed":
                        return self._load_sealed_delta_page(plan.source_id, current)
                    if current.get("state") == "reset":
                        return {"state": "reset", "location": current.get("resetLocation"), "receiptId": receipt_id}
                    raise SourceRegistryConflictError("delta_page_capture_unresolved")
        raise SourceRegistryConflictError("delta_page_capture_conflict")

    def record_delta_page_reset(
        self,
        source_id: str,
        receipt_id: str,
        request_url: str,
        location: str,
    ) -> None:
        for _ in range(3):
            current = self._read_work_item(source_id, receipt_id)
            if current is None or current.get("requestUrl") != request_url:
                raise SourceRegistryConflictError("delta_page_receipt_missing")
            if current.get("state") == "reset":
                if current.get("resetLocation") != location:
                    raise SourceRegistryConflictError("delta_page_reset_location_changed")
                return
            if current.get("state") != "capturing":
                raise SourceRegistryConflictError("delta_page_receipt_not_capturing")
            body = dict(current)
            body.pop("_etag", None)
            body.update({
                "state": "reset",
                "resetLocation": location,
                "updatedAt": _utc_timestamp(),
            })
            try:
                self._ingestion_runs.replace_item(
                    item=receipt_id,
                    body=body,
                    etag=current.get("_etag"),
                    match_condition=MatchConditions.IfNotModified,
                )
                return
            except Exception as error:
                if getattr(error, "status_code", None) not in {409, 412}:
                    raise SourceRegistryError("delta_page_reset_persist_failed") from None
        raise SourceRegistryConflictError("delta_page_reset_conflict")

    def seal_delta_page(
        self,
        source_id: str,
        owner_instance_id: str,
        receipt_id: str,
        items: list[dict[str, Any]],
        next_link: str | None,
        delta_link: str | None,
    ) -> str:
        receipt = self._read_work_item(source_id, receipt_id)
        if (
            receipt is None
            or receipt.get("state") != "capturing"
            or receipt.get("ownerInstanceId") != owner_instance_id
        ):
            raise SourceRegistryConflictError("delta_page_receipt_not_owned")
        fragments: list[dict[str, Any]] = []
        for entry_ordinal, item in enumerate(items):
            encoded = json.dumps(item, ensure_ascii=True, separators=(",", ":"))
            parts = [
                encoded[offset:offset + DELTA_PAGE_FRAGMENT_CHARS]
                for offset in range(0, len(encoded), DELTA_PAGE_FRAGMENT_CHARS)
            ] or [""]
            for fragment_ordinal, content in enumerate(parts):
                fragments.append({
                    "id": _delta_page_fragment_id(receipt_id, entry_ordinal, fragment_ordinal),
                    "sourceId": source_id,
                    "recordType": DELTA_PAGE_FRAGMENT_RECORD_TYPE,
                    "schemaVersion": 1,
                    "receiptId": receipt_id,
                    "entryOrdinal": entry_ordinal,
                    "fragmentOrdinal": fragment_ordinal,
                    "fragmentCount": len(parts),
                    "content": content,
                    "contentDigest": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                })
        for fragment in fragments:
            try:
                self._ingestion_runs.create_item(body=fragment)
            except Exception as error:
                if getattr(error, "status_code", None) != 409:
                    raise SourceRegistryError("delta_page_fragment_write_failed") from None
                stored = self._read_work_item(source_id, fragment["id"])
                if stored is None or any(
                    stored.get(key) != fragment[key]
                    for key in (
                        "receiptId", "entryOrdinal", "fragmentOrdinal", "fragmentCount",
                        "content", "contentDigest",
                    )
                ):
                    raise SourceRegistryConflictError("delta_page_fragment_conflict") from None
        receipt_id = receipt["id"]
        page_ordinal = receipt["pageOrdinal"]
        for entry_ordinal, item in enumerate(items):
            item_id = item.get("id")
            if not isinstance(item_id, str) or not item_id:
                raise SourceRegistryError("delta_page_item_id_missing")
            self._reduce_delta_item(
                source_id, receipt["roundId"], page_ordinal, entry_ordinal, item,
            )
        response_data = {
            "items": items,
            "nextLink": next_link,
            "deltaLink": delta_link,
        }
        response_digest = hashlib.sha256(json.dumps(
            response_data, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        body = dict(receipt)
        body.pop("_etag", None)
        body.update({
            "state": "sealed",
            "itemCount": len(items),
            "fragmentCount": len(fragments),
            "nextLink": next_link,
            "deltaLink": delta_link,
            "responseDigest": response_digest,
            "updatedAt": _utc_timestamp(),
        })
        try:
            self._ingestion_runs.replace_item(
                item=receipt_id,
                body=body,
                etag=receipt.get("_etag"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            current = self._read_work_item(source_id, receipt_id)
            if current is None or (
                current.get("state") != "sealed"
                or current.get("responseDigest") != response_digest
            ):
                if getattr(error, "status_code", None) in {409, 412}:
                    raise SourceRegistryConflictError("delta_page_seal_conflict") from None
                raise SourceRegistryError("delta_page_seal_failed") from None
        return receipt_id

    def _reduce_delta_item(
        self,
        source_id: str,
        round_id: str,
        page_ordinal: int,
        entry_ordinal: int,
        item: dict[str, Any],
    ) -> None:
        item_id = item["id"]
        digest = hashlib.sha256(item_id.encode("utf-8")).hexdigest()
        state_id = f"delta-item:{hashlib.sha256(round_id.encode('utf-8')).hexdigest()[:16]}:{digest}"
        for _ in range(3):
            current = self._read_work_item(source_id, state_id)
            if current is not None:
                current_position = (current.get("pageOrdinal", -1), current.get("entryOrdinal", -1))
                if current.get("roundId") != round_id:
                    raise SourceRegistryConflictError("delta_item_round_mismatch")
                if current_position >= (page_ordinal, entry_ordinal):
                    return
                body = dict(current)
                body.pop("_etag", None)
                body.update({
                    "item": item,
                    "pageOrdinal": page_ordinal,
                    "entryOrdinal": entry_ordinal,
                    "updatedAt": _utc_timestamp(),
                })
                try:
                    self._ingestion_runs.replace_item(
                        item=state_id,
                        body=body,
                        etag=current.get("_etag"),
                        match_condition=MatchConditions.IfNotModified,
                    )
                    return
                except Exception as error:
                    if getattr(error, "status_code", None) not in {409, 412}:
                        raise SourceRegistryError("delta_item_reduction_failed") from None
                    continue
            state = {
                "id": state_id,
                "sourceId": source_id,
                "recordType": DELTA_ITEM_RECORD_TYPE,
                "schemaVersion": 1,
                "roundId": round_id,
                "itemId": item_id,
                "pageOrdinal": page_ordinal,
                "entryOrdinal": entry_ordinal,
                "item": item,
                "createdAt": _utc_timestamp(),
                "updatedAt": _utc_timestamp(),
            }
            try:
                self._ingestion_runs.create_item(body=state)
                return
            except Exception as error:
                if getattr(error, "status_code", None) != 409:
                    raise SourceRegistryError("delta_item_reduction_failed") from None
        raise SourceRegistryConflictError("delta_item_reduction_conflict")

    def list_reduced_delta_items(self, source_id: str, round_id: str) -> list[dict[str, Any]]:
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT c.itemId, c.item FROM c WHERE c.recordType = @recordType "
                    "AND c.roundId = @roundId"
                ),
                parameters=[
                    {"name": "@recordType", "value": DELTA_ITEM_RECORD_TYPE},
                    {"name": "@roundId", "value": round_id},
                ],
                partition_key=source_id,
                max_item_count=100,
            )
            rows = [row for page in iterator.by_page() for row in page]
        except Exception:
            raise SourceRegistryError("delta_item_reduction_read_failed") from None
        if any(not isinstance(row.get("item"), dict) for row in rows):
            raise SourceRegistryError("delta_item_reduction_record_invalid")
        return [row["item"] for row in sorted(rows, key=lambda row: row["itemId"])]

    def list_reduced_delta_items_page(
        self,
        source_id: str,
        round_id: str,
        *,
        page_size: int,
        continuation_token: str | None = None,
    ) -> DeltaReducedItemsPage:
        if (
            not source_id or not round_id
            or isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1
        ):
            raise ValueError("delta_reduced_items_page_request_invalid")
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT c.itemId, c.item FROM c WHERE c.recordType = @recordType "
                    "AND c.roundId = @roundId ORDER BY c.itemId"
                ),
                parameters=[
                    {"name": "@recordType", "value": DELTA_ITEM_RECORD_TYPE},
                    {"name": "@roundId", "value": round_id},
                ],
                partition_key=source_id,
                max_item_count=page_size,
            )
            pager = iterator.by_page(continuation_token)
            rows = list(next(pager, []))
            token = pager.continuation_token
        except Exception:
            raise SourceRegistryError("delta_item_reduction_read_failed") from None
        items = []
        for row in rows:
            if not isinstance(row.get("item"), dict):
                raise SourceRegistryError("delta_item_reduction_record_invalid")
            items.append(row["item"])
        return DeltaReducedItemsPage(tuple(items), token)

    def has_reduced_delta_item(self, source_id: str, round_id: str, item_id: str) -> bool:
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT TOP 1 c.id FROM c WHERE c.recordType = @recordType "
                    "AND c.roundId = @roundId AND c.itemId = @itemId"
                ),
                parameters=[
                    {"name": "@recordType", "value": DELTA_ITEM_RECORD_TYPE},
                    {"name": "@roundId", "value": round_id},
                    {"name": "@itemId", "value": item_id},
                ],
                partition_key=source_id,
                max_item_count=1,
            )
            pager = iterator.by_page()
            rows = list(next(pager, []))
        except Exception:
            raise SourceRegistryError("delta_item_point_read_failed") from None
        return bool(rows)

    def count_delta_round_entries(self, source_id: str, round_id: str) -> int:
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT c.itemCount FROM c WHERE c.recordType = @recordType "
                    "AND c.roundId = @roundId"
                ),
                parameters=[
                    {"name": "@recordType", "value": DELTA_PAGE_RECORD_TYPE},
                    {"name": "@roundId", "value": round_id},
                ],
                partition_key=source_id,
                max_item_count=100,
            )
            total = 0
            for page in iterator.by_page():
                for row in page:
                    count = row.get("itemCount")
                    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                        raise SourceRegistryError("delta_page_entry_count_invalid")
                    total += count
            return total
        except SourceRegistryError:
            raise
        except Exception:
            raise SourceRegistryError("delta_page_entry_count_read_failed") from None

    def _load_sealed_delta_page(
        self,
        source_id: str,
        receipt: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT c.entryOrdinal, c.fragmentOrdinal, c.fragmentCount, "
                    "c.content, c.contentDigest FROM c WHERE c.recordType = @recordType "
                    "AND c.receiptId = @receiptId"
                ),
                parameters=[
                    {"name": "@recordType", "value": DELTA_PAGE_FRAGMENT_RECORD_TYPE},
                    {"name": "@receiptId", "value": receipt["id"]},
                ],
                partition_key=source_id,
                max_item_count=100,
            )
            rows = [row for page in iterator.by_page() for row in page]
        except Exception:
            raise SourceRegistryError("delta_page_fragment_read_failed") from None
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            content = row.get("content")
            if not isinstance(content, str) or hashlib.sha256(content.encode("utf-8")).hexdigest() != row.get("contentDigest"):
                raise SourceRegistryError("delta_page_fragment_digest_invalid")
            grouped.setdefault(row["entryOrdinal"], []).append(row)
        items: list[dict[str, Any]] = []
        for entry_ordinal in range(receipt.get("itemCount", 0)):
            parts = sorted(grouped.get(entry_ordinal, []), key=lambda row: row["fragmentOrdinal"])
            if not parts or len(parts) != parts[0].get("fragmentCount"):
                raise SourceRegistryError("delta_page_fragment_incomplete")
            encoded = "".join(part["content"] for part in parts)
            try:
                item = json.loads(encoded)
            except (TypeError, ValueError):
                raise SourceRegistryError("delta_page_item_invalid") from None
            if not isinstance(item, dict):
                raise SourceRegistryError("delta_page_item_invalid")
            items.append(item)
        response_data = {
            "items": items,
            "nextLink": receipt.get("nextLink"),
            "deltaLink": receipt.get("deltaLink"),
        }
        digest = hashlib.sha256(json.dumps(
            response_data, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        if digest != receipt.get("responseDigest"):
            raise SourceRegistryError("delta_page_response_digest_mismatch")
        return {
            "state": "sealed",
            "receiptId": receipt["id"],
            "items": items,
            "nextLink": receipt.get("nextLink"),
            "deltaLink": receipt.get("deltaLink"),
        }

    def get_document_operation_kind(
        self,
        source_id: str,
        operation_id: str,
        physical_run_id: str,
        item_id: str,
    ) -> str:
        """Read the operation kind only when all supplied identities match."""
        operation = self._read_work_item(source_id, operation_id)
        if operation is None:
            raise SourceRegistryConflictError("document_operation_missing")
        operation_kind = operation.get("operationKind")
        if not isinstance(operation_kind, str) or not operation_kind:
            raise SourceRegistryError("document_operation_kind_missing")
        self._assert_operation_identity(
            operation, source_id, physical_run_id, item_id, operation_kind,
        )
        return operation_kind

    def assert_document_operation_unresolved(
        self,
        source_id: str,
        owner_instance_id: str,
        operation_id: str,
        physical_run_id: str,
        item_id: str,
        operation_kind: str,
        *,
        allow_pending: bool = False,
    ) -> dict[str, Any]:
        """Verify the exact operation is running or explicitly unresolved."""
        coordinator = self.get_drive_coordinator(source_id)
        operation = self._read_work_item(source_id, operation_id)
        if operation is not None:
            self._assert_operation_identity(
                operation, source_id, physical_run_id, item_id, operation_kind,
            )
        if (
            coordinator is None
            or coordinator.get("ownerInstanceId") != owner_instance_id
            or coordinator.get("state") not in {"active", "draining"}
            or operation is None
            or operation.get("ownerInstanceId") != owner_instance_id
            or not (
                operation.get("state") == "running"
                or (allow_pending and operation.get("state") == "unresolved")
            )
        ):
            raise SourceRegistryConflictError("document_operation_unresolved_mismatch")
        return operation

    def abandon_document_operation_for_operator(
        self,
        source_id: str,
        owner_instance_id: str,
        operation_id: str,
        physical_run_id: str,
        item_id: str,
        operation_kind: str,
        *,
        operator_id: str,
        reason: str,
        quiescence_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        """Fail one unresolved operation after external writer quiescence is attested."""
        evidence_fields = {
            "evidenceId": "evidenceId",
            "workersStopped": "workersStopped",
            "remoteAudioReconciled": "remoteAudioReconciled",
            "indexedStateReconciled": "indexedStateReconciled",
        }
        if (
            not operator_id
            or not reason
            or not isinstance(quiescence_evidence, dict)
            or not all(quiescence_evidence.get(key) for key in evidence_fields)
            or any(
                quiescence_evidence.get(key) != "yes"
                for key in ("workersStopped", "remoteAudioReconciled", "indexedStateReconciled")
            )
        ):
            raise ValueError("operator_operation_recovery_evidence_required")
        recovery_id = (
            "document-operation-recovery:"
            f"{hashlib.sha256(operation_id.encode('utf-8')).hexdigest()}"
        )
        for _ in range(3):
            coordinator = self.get_drive_coordinator(source_id)
            operation = self._read_work_item(source_id, operation_id)
            if (
                operation is not None
                and operation.get("state") == "failed"
                and operation.get("recoveryId") == recovery_id
            ):
                self._assert_operation_identity(
                    operation,
                    source_id,
                    physical_run_id,
                    item_id,
                    operation_kind,
                )
                return {"status": "failed", "recoveryId": recovery_id}
            if operation is not None:
                self._assert_operation_identity(
                    operation,
                    source_id,
                    physical_run_id,
                    item_id,
                    operation_kind,
                )
            if (
                coordinator is None
                or coordinator.get("ownerInstanceId") != owner_instance_id
                or coordinator.get("state") != "draining"
                or operation is None
                or operation.get("ownerInstanceId") != owner_instance_id
                or operation.get("state") not in {"claimed", "running", "unresolved"}
            ):
                raise SourceRegistryConflictError(
                    "operator_operation_recovery_identity_mismatch"
                )
            active_count = self._active_work_count(coordinator)
            if active_count <= 0:
                raise SourceRegistryConflictError(
                    "operator_operation_recovery_count_invalid"
                )
            recovery = {
                "id": recovery_id,
                "sourceId": source_id,
                "recordType": "document-operation-recovery",
                "schemaVersion": 1,
                "operationId": operation_id,
                "itemId": item_id,
                "physicalRunId": physical_run_id,
                "operationKind": operation_kind,
                "operatorId": operator_id,
                "reason": reason,
                "quiescenceVerified": True,
                "quiescenceEvidence": quiescence_evidence,
                "createdAt": _utc_timestamp(),
            }
            coordinator_body = dict(coordinator)
            coordinator_body.pop("_etag", None)
            coordinator_body.update({
                "activeWorkCount": active_count - 1,
                "updatedAt": _utc_timestamp(),
            })
            operation_body = dict(operation)
            operation_body.pop("_etag", None)
            operation_body.update({
                "state": "failed",
                "recoveryId": recovery_id,
                "outcome": {
                    "status": "failed",
                    "error": "operator_recovery",
                    "reason": reason,
                },
                "updatedAt": _utc_timestamp(),
            })
            try:
                self._execute_work_batch(source_id, [
                    (
                        "replace", (DRIVE_COORDINATOR_ID, coordinator_body),
                        {"if_match_etag": coordinator.get("_etag")},
                    ),
                    (
                        "replace", (operation_id, operation_body),
                        {"if_match_etag": operation.get("_etag")},
                    ),
                    ("create", (recovery,), {}),
                ])
                return {"status": "failed", "recoveryId": recovery_id}
            except SourceRegistryConflictError:
                continue
            except Exception as error:
                persisted = self._read_work_item(source_id, operation_id)
                if persisted is not None and persisted.get("state") == "failed":
                    return {
                        "status": "failed",
                        "recoveryId": persisted.get("recoveryId"),
                    }
                raise SourceRegistryError(
                    "operator_operation_recovery_failed"
                ) from error
        raise SourceRegistryConflictError("operator_operation_recovery_conflict")

    def _read_work_item(self, source_id: str, item_id: str) -> dict[str, Any] | None:
        try:
            return self._ingestion_runs.read_item(item=item_id, partition_key=source_id)
        except CosmosResourceNotFoundError:
            return None

    @staticmethod
    def _assert_operation_identity(
        operation: dict[str, Any],
        source_id: str,
        physical_run_id: str,
        item_id: str,
        operation_kind: str,
    ) -> None:
        if (
            operation.get("sourceId") != source_id
            or operation.get("physicalRunId") != physical_run_id
            or operation.get("itemId") != item_id
            or operation.get("operationKind") != operation_kind
        ):
            raise SourceRegistryConflictError(
                "document_operation_identity_mismatch"
            )

    def _execute_work_batch(
        self, source_id: str, operations: list[tuple[Any, ...]],
    ) -> None:
        try:
            results = self._ingestion_runs.execute_item_batch(
                batch_operations=operations,
                partition_key=source_id,
            )
        except Exception as error:
            if getattr(error, "status_code", None) in {409, 412}:
                raise SourceRegistryConflictError("document_work_transaction_conflict") from None
            raise
        if not isinstance(results, list) or len(results) != len(operations):
            raise SourceRegistryError("document_work_transaction_invalid")
        for result in results:
            status = result.get("statusCode") if isinstance(result, dict) else None
            if status in {409, 412}:
                raise SourceRegistryConflictError("document_work_transaction_conflict")
            if not isinstance(status, int) or not 200 <= status < 300:
                raise SourceRegistryError("document_work_transaction_failed")

    @staticmethod
    def _active_work_count(coordinator: dict[str, Any]) -> int:
        count = coordinator.get("activeWorkCount")
        if count is None and coordinator.get("state") == "settled":
            return 0
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise SourceRegistryError("drive_coordinator_work_count_invalid")
        return count

    def _replace_coordinator(
        self,
        body: dict[str, Any],
        current: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            return self._ingestion_runs.replace_item(
                item=DRIVE_COORDINATOR_ID,
                body=body,
                etag=current.get("_etag"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            raise SourceRegistryConflictError("drive_coordinator_transition_conflict") from error

    def list_source_inventory_page(
        self,
        *,
        page_size: int = 50,
        continuation_token: str | None = None,
    ) -> SourceInventoryPage:
        if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
            raise ValueError("source_inventory_page_size_invalid")
        query = (
            "SELECT c.id, c.sourceId, c.recordType, c.schemaVersion, c.sourceKind, "
            "c.desiredState, c.config, c._etag FROM c WHERE c.recordType = @recordType"
        )
        try:
            iterator = self._sources.query_items(
                query=query,
                parameters=[{"name": "@recordType", "value": SOURCE_RECORD_TYPE}],
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            pager_factory = getattr(iterator, "by_page", None)
            if not callable(pager_factory):
                raise SourceRegistryError("source_registry_paging_unavailable")
            pager = pager_factory(continuation_token)
            rows = list(next(pager, []))
            if len(rows) > page_size:
                raise SourceRegistryError("source_registry_page_unbounded")
            next_token = pager.continuation_token
        except Exception as error:
            if isinstance(error, SourceRegistryError):
                raise
            raise SourceRegistryError("source_registry_enumeration_failed") from error

        parsed: list[SourcePlan] = []
        rejected: set[str] = set()
        unattributed_errors = 0
        duplicate_drive: set[str] = set()
        for item in rows:
            config = item.get("config") if isinstance(item, dict) else None
            source_id = item.get("sourceId") if isinstance(item, dict) else None
            drive_id = config.get("driveId") if isinstance(config, dict) else None
            if (
                isinstance(source_id, str) and source_id
                and isinstance(drive_id, str) and drive_id
                and self._has_duplicate_drive_mapping(drive_id)
                and self._drive_reservation_owner(drive_id) != source_id
            ):
                duplicate_drive.add(source_id)
        for item in rows:
            if isinstance(item, dict) and item.get("sourceId") in duplicate_drive:
                continue
            try:
                source = SharePointSource.from_item(item)
                etag = item.get("_etag")
                if not isinstance(etag, str) or not etag:
                    raise SourceRegistryError("source_catalog_etag_missing")
                plan = SourcePlan(
                    source, etag, source_binding_digest(source, self._tenant_id),
                )
                parsed.append(plan)
            except (SourceRegistryError, AttributeError, TypeError, ValueError):
                source_id = item.get("sourceId") if isinstance(item, dict) else None
                if isinstance(source_id, str) and source_id:
                    rejected.add(source_id)
                else:
                    unattributed_errors += 1

        return SourceInventoryPage(
            tuple(parsed),
            tuple(sorted(rejected)),
            unattributed_errors,
            tuple(sorted(duplicate_drive)),
            next_token,
        )

    def _has_duplicate_drive_mapping(self, drive_id: str) -> bool:
        try:
            rows = list(self._sources.query_items(
                query=(
                    "SELECT TOP 2 c.sourceId FROM c WHERE c.recordType = @recordType "
                    "AND c.config.driveId = @driveId"
                ),
                parameters=[
                    {"name": "@recordType", "value": SOURCE_RECORD_TYPE},
                    {"name": "@driveId", "value": drive_id},
                ],
                enable_cross_partition_query=True,
                max_item_count=2,
            ))
        except Exception as error:
            raise SourceRegistryError("source_registry_duplicate_check_failed") from error
        return len(rows) > 1

    def _drive_reservation_owner(self, drive_id: str) -> str | None:
        reservation_digest = drive_reservation_digest(drive_id, self._tenant_id)
        try:
            reservation = self._ingestion_runs.read_item(
                item="drive-binding",
                partition_key=f"drive-binding:{reservation_digest}",
            )
        except CosmosResourceNotFoundError:
            return None
        except Exception as error:
            raise SourceRegistryError("source_registry_duplicate_check_failed") from error
        return reservation.get("ownerSourceId")

    def begin_source_dispatch(self, dispatch_id: str) -> dict[str, Any]:
        if not isinstance(dispatch_id, str) or not dispatch_id:
            raise ValueError("source_dispatch_id_invalid")
        source_id = _source_dispatch_partition(dispatch_id)
        try:
            return self._ingestion_runs.create_item(body={
                "id": "source-dispatch-manifest",
                "sourceId": source_id,
                "recordType": "source-dispatch-manifest",
                "schemaVersion": 1,
                "dispatchId": dispatch_id,
                "state": "preparing",
                "createdAt": _utc_timestamp(),
                "updatedAt": _utc_timestamp(),
            })
        except Exception as error:
            if getattr(error, "status_code", None) != 409:
                raise SourceRegistryError("source_dispatch_begin_failed") from error
            existing = self._read_work_item(source_id, "source-dispatch-manifest")
            if existing is None or existing.get("dispatchId") != dispatch_id:
                raise SourceRegistryConflictError("source_dispatch_identity_conflict") from None
            if existing.get("state") == "failed":
                raise SourceRegistryConflictError("source_dispatch_already_failed")
            return existing

    def stage_source_dispatch_item(
        self,
        dispatch_id: str,
        source_id: str,
        *,
        status: str,
        source_payload: dict[str, str] | None = None,
        outcome: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if (
            not dispatch_id or not source_id
            or status not in {"prepared", "paused", "retired", "failed"}
        ):
            raise ValueError("source_dispatch_item_invalid")
        body = {
            "id": _source_dispatch_item_id(dispatch_id, source_id),
            "sourceId": source_id,
            "recordType": "source-dispatch-item",
            "schemaVersion": 1,
            "dispatchId": dispatch_id,
            "status": status,
            "sourcePlan": source_payload,
            "outcome": outcome,
            "updatedAt": _utc_timestamp(),
        }
        try:
            return self._ingestion_runs.create_item(body=body)
        except Exception as error:
            if getattr(error, "status_code", None) != 409:
                raise SourceRegistryError("source_dispatch_stage_failed") from error
            existing = self._read_work_item(source_id, body["id"])
            if existing is None or any(
                existing.get(key) != body[key]
                for key in ("dispatchId", "status", "sourcePlan", "outcome")
            ):
                raise SourceRegistryConflictError("source_dispatch_item_conflict") from None
            return existing

    def complete_source_dispatch(self, dispatch_id: str, counts: dict[str, int]) -> dict[str, Any]:
        manifest = self._read_dispatch_manifest(dispatch_id)
        if manifest.get("state") == "ready":
            if manifest.get("counts") != counts:
                raise SourceRegistryConflictError("source_dispatch_counts_conflict")
            return manifest
        if manifest.get("state") != "preparing":
            raise SourceRegistryConflictError("source_dispatch_not_preparing")
        body = dict(manifest)
        body.pop("_etag", None)
        body.update({"state": "ready", "counts": counts, "updatedAt": _utc_timestamp()})
        try:
            return self._ingestion_runs.replace_item(
                item="source-dispatch-manifest",
                body=body,
                etag=manifest.get("_etag"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            raise SourceRegistryConflictError("source_dispatch_complete_conflict") from error

    def fail_source_dispatch(self, dispatch_id: str, error_code: str) -> None:
        manifest = self._read_dispatch_manifest(dispatch_id)
        if manifest.get("state") == "failed":
            return
        body = dict(manifest)
        body.pop("_etag", None)
        body.update({"state": "failed", "errorCode": error_code, "updatedAt": _utc_timestamp()})
        try:
            self._ingestion_runs.replace_item(
                item="source-dispatch-manifest",
                body=body,
                etag=manifest.get("_etag"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            raise SourceRegistryConflictError("source_dispatch_fail_conflict") from error

    def list_source_dispatch_page(
        self,
        dispatch_id: str,
        *,
        page_size: int,
        continuation_token: str | None = None,
    ) -> SourceDispatchPage:
        if not dispatch_id or isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
            raise ValueError("source_dispatch_page_request_invalid")
        if self._read_dispatch_manifest(dispatch_id).get("state") != "ready":
            raise SourceRegistryConflictError("source_dispatch_inventory_incomplete")
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT c.id, c.sourceId, c.status, c.sourcePlan, c.outcome FROM c "
                    "WHERE c.recordType = @recordType AND c.dispatchId = @dispatchId "
                    "ORDER BY c.sourceId"
                ),
                parameters=[
                    {"name": "@recordType", "value": "source-dispatch-item"},
                    {"name": "@dispatchId", "value": dispatch_id},
                ],
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            pager = iterator.by_page(continuation_token)
            items = tuple(next(pager, []))
            token = pager.continuation_token
        except Exception as error:
            raise SourceRegistryError("source_dispatch_query_failed") from error
        return SourceDispatchPage(items, token)

    def record_source_dispatch_outcome(
        self,
        dispatch_id: str,
        source_id: str,
        *,
        status: str,
        child_instance_id: str,
        outcome: dict[str, Any],
    ) -> dict[str, Any]:
        if (
            status not in {"completed", "failed", "pending"}
            or not child_instance_id
            or not isinstance(outcome, dict)
        ):
            raise ValueError("source_dispatch_outcome_invalid")
        item_id = _source_dispatch_item_id(dispatch_id, source_id)
        item = self._read_work_item(source_id, item_id)
        if item is None or item.get("dispatchId") != dispatch_id:
            raise SourceRegistryConflictError("source_dispatch_item_missing")
        updated = dict(item)
        updated.pop("_etag", None)
        updated.update({
            "status": status,
            "childInstanceId": child_instance_id,
            "outcome": outcome,
            "updatedAt": _utc_timestamp(),
        })
        if item.get("status") != "prepared":
            if all(item.get(key) == updated[key] for key in (
                "status", "childInstanceId", "outcome",
            )):
                return item
            raise SourceRegistryConflictError("source_dispatch_outcome_conflict")
        try:
            return self._ingestion_runs.replace_item(
                item=item_id,
                body=updated,
                etag=item.get("_etag"),
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            raise SourceRegistryConflictError("source_dispatch_outcome_conflict") from error

    def list_source_dispatch_outcomes_page(
        self,
        dispatch_id: str,
        *,
        page_size: int,
        continuation_token: str | None = None,
    ) -> SourceDispatchPage:
        if not dispatch_id or isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
            raise ValueError("source_dispatch_outcome_page_request_invalid")
        if self._read_dispatch_manifest(dispatch_id).get("state") != "ready":
            raise SourceRegistryConflictError("source_dispatch_inventory_incomplete")
        try:
            iterator = self._ingestion_runs.query_items(
                query=(
                    "SELECT c.id, c.sourceId, c.status, c.sourcePlan, c.childInstanceId, c.outcome "
                    "FROM c WHERE c.recordType = @recordType AND c.dispatchId = @dispatchId "
                    "ORDER BY c.sourceId"
                ),
                parameters=[
                    {"name": "@recordType", "value": "source-dispatch-item"},
                    {"name": "@dispatchId", "value": dispatch_id},
                ],
                enable_cross_partition_query=True,
                max_item_count=page_size,
            )
            pager = iterator.by_page(continuation_token)
            items = tuple(next(pager, []))
            token = pager.continuation_token
        except Exception as error:
            raise SourceRegistryError("source_dispatch_outcome_query_failed") from error
        return SourceDispatchPage(items, token)

    def _read_dispatch_manifest(self, dispatch_id: str) -> dict[str, Any]:
        manifest = self._read_work_item(
            _source_dispatch_partition(dispatch_id),
            "source-dispatch-manifest",
        )
        if manifest is None or manifest.get("dispatchId") != dispatch_id:
            raise SourceRegistryConflictError("source_dispatch_manifest_missing")
        return manifest

    def read_source_plan(self, source_id: str) -> SourcePlan:
        plan = self._read_catalog_source_plan(source_id)
        self._verify_source_acceptance(plan)
        gate = self._read_gate(source_id)
        if gate is not None and gate.get("retired") is True and plan.source.desired_state != "retired":
            raise SourceRegistryConflictError("retired_source_cannot_be_reactivated")
        return plan

    def accept_source_plan(self, plan: SourcePlan) -> SourcePlan:
        current = self._read_catalog_source_plan(plan.source_id)
        if current != plan:
            raise SourceRegistryConflictError("source_catalog_changed")
        self.ensure_source_binding(plan)
        self.ensure_drive_reservation(plan)
        self.observe_lifecycle(plan)
        return plan

    def _read_catalog_source_plan(self, source_id: str) -> SourcePlan:
        try:
            item = self._sources.read_item(item=source_id, partition_key=source_id)
        except CosmosResourceNotFoundError as error:
            raise SourceRegistryError("source_registration_missing") from error
        source = SharePointSource.from_item(item)
        etag = item.get("_etag")
        if source.source_id != source_id or not isinstance(etag, str) or not etag:
            raise SourceRegistryError("source_registration_invalid")
        return SourcePlan(source, etag, source_binding_digest(source, self._tenant_id))

    def _verify_source_acceptance(self, plan: SourcePlan) -> None:
        try:
            binding = self._ingestion_runs.read_item(
                item="source-binding", partition_key=plan.source_id,
            )
        except CosmosResourceNotFoundError as error:
            raise SourceRegistryError("source_binding_not_accepted") from error
        if (
            binding.get("acceptedBindingDigest") != plan.binding_digest
            or binding.get("tenantId") != self._tenant_id
            or binding.get("siteId") != plan.source.site_id
            or binding.get("driveId") != plan.source.drive_id
        ):
            raise SourceRegistryConflictError("source_binding_is_immutable")
        try:
            reservation_digest = drive_reservation_digest(
                plan.source.drive_id, self._tenant_id,
            )
            reservation = self._ingestion_runs.read_item(
                item="drive-binding",
                partition_key=f"drive-binding:{reservation_digest}",
            )
        except CosmosResourceNotFoundError as error:
            raise SourceRegistryError("source_binding_not_accepted") from error
        if (
            reservation.get("ownerSourceId") != plan.source_id
            or reservation.get("acceptedBindingDigest") != plan.binding_digest
            or reservation.get("tenantId") != self._tenant_id
            or reservation.get("siteId") != plan.source.site_id
            or reservation.get("driveId") != plan.source.drive_id
        ):
            raise SourceRegistryConflictError("source_binding_acceptance_mismatch")

    def ensure_source_binding(self, plan: SourcePlan) -> None:
        binding = {
            "id": "source-binding",
            "sourceId": plan.source_id,
            "recordType": "source-binding",
            "schemaVersion": 1,
            "acceptedBindingDigest": plan.binding_digest,
            "tenantId": self._tenant_id,
            "siteId": plan.source.site_id,
            "driveId": plan.source.drive_id,
        }
        try:
            self._ingestion_runs.create_item(body=binding)
            return
        except Exception as error:
            if getattr(error, "status_code", None) != 409:
                raise
        existing = self._ingestion_runs.read_item(
            item="source-binding", partition_key=plan.source_id,
        )
        if any(
            existing.get(field) != binding[field]
            for field in ("acceptedBindingDigest", "tenantId", "siteId", "driveId")
        ):
            raise SourceRegistryConflictError("source_binding_is_immutable")

    def ensure_drive_reservation(self, plan: SourcePlan) -> None:
        reservation_digest = drive_reservation_digest(
            plan.source.drive_id, self._tenant_id,
        )
        reservation = {
            "id": "drive-binding",
            "sourceId": f"drive-binding:{reservation_digest}",
            "recordType": "drive-binding",
            "schemaVersion": 1,
            "ownerSourceId": plan.source.source_id,
            "acceptedBindingDigest": plan.binding_digest,
            "tenantId": self._tenant_id,
            "siteId": plan.source.site_id,
            "driveId": plan.source.drive_id,
        }
        partition_key = reservation["sourceId"]
        try:
            self._ingestion_runs.create_item(body=reservation)
            return
        except Exception as error:
            if getattr(error, "status_code", None) != 409:
                raise
        existing = self._ingestion_runs.read_item(
            item="drive-binding", partition_key=partition_key,
        )
        for field in (
            "ownerSourceId", "acceptedBindingDigest", "tenantId", "siteId", "driveId",
        ):
            if existing.get(field) != reservation[field]:
                raise SourceRegistryConflictError("sharepoint_drive_reservation_conflict")

    def observe_lifecycle(self, plan: SourcePlan) -> dict[str, Any] | None:
        gate = self._read_gate(plan.source_id)
        if gate is not None and gate.get("retired") is True:
            if plan.source.desired_state != "retired":
                raise SourceRegistryConflictError("retired_source_cannot_be_reactivated")
            return gate
        if plan.source.desired_state == "retired":
            return self.write_retirement_latch(plan, retired=True)
        if gate is None:
            return self.write_retirement_latch(plan, retired=False)
        return gate

    def write_retirement_latch(
        self,
        plan: SourcePlan,
        *,
        retired: bool,
    ) -> dict[str, Any]:
        current = self.read_source_plan(plan.source_id)
        if current.catalog_etag != plan.catalog_etag or current.binding_digest != plan.binding_digest:
            raise SourceRegistryConflictError("source_catalog_changed")
        existing = self._read_gate(plan.source_id)
        if existing is not None and existing.get("retired") is True and not retired:
            raise SourceRegistryConflictError("retired_source_cannot_be_reactivated")
        body = {
            "id": SOURCE_SERVING_ID,
            "sourceId": plan.source_id,
            "recordType": SOURCE_SERVING_RECORD_TYPE,
            "schemaVersion": SOURCE_SERVING_SCHEMA_VERSION,
            "retired": retired,
        }
        if existing is None:
            self._ingestion_runs.create_item(body=body)
            return body
        try:
            return self._ingestion_runs.replace_item(
                item=SOURCE_SERVING_ID,
                body=body,
                etag=existing["_etag"],
                match_condition=MatchConditions.IfNotModified,
            )
        except Exception as error:
            raise SourceRegistryConflictError("source_serving_gate_conflict") from error

    def _read_gate(self, source_id: str) -> dict[str, Any] | None:
        try:
            return self._ingestion_runs.read_item(
                item=SOURCE_SERVING_ID,
                partition_key=source_id,
            )
        except CosmosResourceNotFoundError:
            return None


class DeltaPageCapture:
    """Bind Graph delta pages to source-scoped durable receipts and fragments."""

    def __init__(
        self,
        repository: SourceRegistryRepository,
        plan: SourcePlan,
        owner_instance_id: str,
        round_id: str,
    ) -> None:
        self._repository = repository
        self._plan = plan
        self._owner_instance_id = owner_instance_id
        self._round_id = round_id

    def begin_page(
        self,
        request_url: str,
        page_ordinal: int,
        predecessor_receipt_id: str | None,
    ) -> dict[str, Any]:
        return self._repository.begin_delta_page_capture(
            self._plan,
            self._owner_instance_id,
            self._round_id,
            page_ordinal,
            request_url,
            predecessor_receipt_id,
        )

    def record_reset(self, request_url: str, page_ordinal: int, location: str) -> None:
        url_digest = hashlib.sha256(request_url.encode("utf-8")).hexdigest()
        receipt_id = _delta_page_receipt_id(self._round_id, page_ordinal, url_digest)
        self._repository.record_delta_page_reset(
            self._plan.source_id, receipt_id, request_url, location,
        )
        self._repository.record_delta_round_reset(
            self._plan.source_id,
            self._round_id,
            self._owner_instance_id,
            location,
        )

    def seal_page(
        self,
        request_url: str,
        page_ordinal: int,
        predecessor_receipt_id: str | None,
        items: list[dict[str, Any]],
        next_link: str | None,
        delta_link: str | None,
    ) -> str:
        url_digest = hashlib.sha256(request_url.encode("utf-8")).hexdigest()
        receipt_id = _delta_page_receipt_id(self._round_id, page_ordinal, url_digest)
        self._repository.seal_delta_page(
            self._plan.source_id,
            self._owner_instance_id,
            receipt_id,
            items,
            next_link,
            delta_link,
        )
        return receipt_id

    def list_reduced_items(self) -> list[dict[str, Any]]:
        return self._repository.list_reduced_delta_items(
            self._plan.source_id,
            self._round_id,
        )

    def list_reduced_items_page(
        self,
        *,
        page_size: int,
        continuation_token: str | None = None,
    ) -> DeltaReducedItemsPage:
        return self._repository.list_reduced_delta_items_page(
            self._plan.source_id,
            self._round_id,
            page_size=page_size,
            continuation_token=continuation_token,
        )

    def has_reduced_item(self, item_id: str) -> bool:
        return self._repository.has_reduced_delta_item(
            self._plan.source_id,
            self._round_id,
            item_id,
        )

    def count_observed_entries(self) -> int:
        return self._repository.count_delta_round_entries(
            self._plan.source_id,
            self._round_id,
        )


def _delta_page_receipt_id(round_id: str, page_ordinal: int, request_url_digest: str) -> str:
    identity = json.dumps(
        [round_id, page_ordinal, request_url_digest],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return f"delta-page:{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"


def _delta_round_item_id(round_id: str) -> str:
    return f"delta-round:{hashlib.sha256(round_id.encode('utf-8')).hexdigest()}"


def _delta_page_fragment_id(receipt_id: str, entry_ordinal: int, fragment_ordinal: int) -> str:
    identity = json.dumps(
        [receipt_id, entry_ordinal, fragment_ordinal],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return f"delta-page-fragment:{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"


def config_for_source(config: Any, plan: SourcePlan) -> Any:
    """Bind deployment-owned settings to an immutable catalog source plan."""
    return replace(
        config,
        source_id=plan.source.source_id,
        drive_id=plan.source.drive_id,
        sharepoint_site_url=plan.source.site_url,
        sharepoint_site_id=plan.source.site_id,
    )


def active_source_payloads(plans: Iterable[SourcePlan]) -> list[dict[str, str]]:
    return [plan.to_payload() for plan in plans if plan.source.desired_state == "active"]


def _document_operation_id(
    source_id: str,
    physical_run_id: str,
    item_id: str,
    operation_kind: str,
) -> str:
    canonical = json.dumps(
        [source_id, physical_run_id, item_id, operation_kind],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return f"document-operation:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _document_winner_id(document_id: str) -> str:
    return f"document-winner:{hashlib.sha256(document_id.encode('utf-8')).hexdigest()}"


def _source_dispatch_partition(dispatch_id: str) -> str:
    digest = hashlib.sha256(dispatch_id.encode("utf-8")).hexdigest()
    return f"source-dispatch:{digest}"


def _source_dispatch_item_id(dispatch_id: str, source_id: str) -> str:
    digest = hashlib.sha256(f"{dispatch_id}:{source_id}".encode("utf-8")).hexdigest()
    return f"source-dispatch-item:{digest}"


def _is_reprovision_operational_record(item: dict[str, Any]) -> bool:
    item_id = item.get("id")
    return (
        isinstance(item_id, str)
        and (
            item_id in REPROVISION_OPERATIONAL_IDS
            or item_id.startswith("run:")
            or item.get("recordType") in REPROVISION_OPERATIONAL_RECORD_TYPES
        )
    )


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")