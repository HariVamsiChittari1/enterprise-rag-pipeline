"""Strict runtime relevance catalogs stored in Azure Cosmos DB for NoSQL."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Mapping
from uuid import UUID

from azure.core import MatchConditions
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosResourceNotFoundError
from jsonschema import Draft202012Validator, ValidationError

from retrieval.config_loader import (
    ConfigLoaderError,
    SCORING_PROFILES_SCHEMA,
    SYNONYM_MAPS_SCHEMA,
    load_scoring_profiles,
    load_synonym_maps,
    validate_profile_synonym_map_references,
)
from retrieval.scoring import ScoringProfile
from retrieval.synonyms import SynonymExpander, SynonymMap


MAX_CATALOG_ITEM_BYTES = 1_572_864
CATALOG_ID = "runtime-catalog"
CATALOG_TYPE = "retrieval-runtime-catalog"
# Sentinel ETag marks the built-in baseline snapshot so it never collides with a Cosmos ETag.
BASELINE_ETAG = "baseline"
# Neutral defaults applied when the catalog item is absent: unweighted RRF, no scoring overlay.
BASELINE_CONFIG: Mapping[str, Any] = {
    "retrieval": {
        "overFetchFactor": 3,
        "hybridWeights": {"vector": 1.0, "text": 1.0},
        "fullTextScoreScope": "Global",
    },
    "profiles": [],
    "synonymMaps": [],
}
_ALLOWED_TEXT_WEIGHT_FIELDS = frozenset(
    {"content", "sourceName", "source_name", "sectionPath", "section_path", "keyPhrases", "key_phrases"}
)
_ALLOWED_FUNCTION_FIELDS = {
    "freshness": frozenset({"sourceModifiedAt", "source_modified_at"}),
}


CATALOG_ITEM_SCHEMA: Mapping[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["id", "deploymentInstanceId", "type", "config"],
    "properties": {
        "id": {"const": CATALOG_ID},
        "deploymentInstanceId": {"type": "string", "minLength": 1, "maxLength": 100},
        "type": {"const": CATALOG_TYPE},
        "change": {
            "type": "object",
            "additionalProperties": False,
            "required": ["operationId", "changedAt", "reason"],
            "properties": {
                "operationId": {"type": "string", "pattern": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"},
                "changedAt": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$"},
                "reason": {"type": "string", "minLength": 1, "maxLength": 500},
            },
        },
        "config": {
            "type": "object",
            "additionalProperties": False,
            "required": ["retrieval", "profiles", "synonymMaps"],
            "properties": {
                "retrieval": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["overFetchFactor", "hybridWeights", "fullTextScoreScope"],
                    "properties": {
                        "overFetchFactor": {"type": "integer", "minimum": 1, "maximum": 50},
                        "hybridWeights": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["vector", "text"],
                            "properties": {
                                "vector": {"type": "number", "exclusiveMinimum": 0},
                                "text": {"type": "number", "exclusiveMinimum": 0},
                            },
                        },
                        "fullTextScoreScope": {"enum": ["Local", "Global"]},
                    },
                },
                "defaultProfile": {"type": "string", "minLength": 1},
                "profiles": SCORING_PROFILES_SCHEMA["properties"]["profiles"],
                "synonymMaps": SYNONYM_MAPS_SCHEMA["properties"]["maps"],
            },
        },
    },
}


class CatalogError(RuntimeError):
    """A catalog cannot be safely written or loaded."""

    def __init__(self, message: str, *, etag: str | None = None) -> None:
        super().__init__(message)
        self.etag = etag


class CatalogConflictError(CatalogError):
    """A persisted catalog conflicts with the reviewed operation."""


class CatalogMissingError(CatalogError):
    """The runtime catalog item does not exist; runtime serving may substitute the baseline."""


class UnknownScoringProfileError(ValueError):
    """A request selected a profile absent from its captured catalog."""


@dataclass(frozen=True)
class RuntimeCatalogSnapshot:
    deployment_instance_id: str
    catalog_id: str
    etag: str | None
    digest: str
    operation_id: str | None
    changed_at: str | None
    accepted_at: datetime
    over_fetch_factor: int
    hybrid_weights: tuple[float, float]
    full_text_score_scope: str
    default_profile: str | None
    profiles: Mapping[str, ScoringProfile]
    synonym_maps: Mapping[str, SynonymMap]
    synonym_expanders: Mapping[str, SynonymExpander]

    def __post_init__(self) -> None:
        object.__setattr__(self, "profiles", MappingProxyType(dict(self.profiles)))
        object.__setattr__(self, "synonym_maps", MappingProxyType(dict(self.synonym_maps)))
        object.__setattr__(self, "synonym_expanders", MappingProxyType(dict(self.synonym_expanders)))


@dataclass(frozen=True)
class RequestPolicy:
    snapshot: RuntimeCatalogSnapshot
    profile: ScoringProfile | None
    expander: SynonymExpander | None

    @classmethod
    def capture(
        cls, snapshot: RuntimeCatalogSnapshot, requested: str | None = None,
        expand_synonyms: bool | None = None,
    ) -> RequestPolicy:
        name = requested if requested is not None else snapshot.default_profile
        profile = snapshot.profiles.get(name) if name is not None else None
        if name is not None and profile is None:
            raise UnknownScoringProfileError("unknown_scoring_profile")
        expander = (
            snapshot.synonym_expanders.get(profile.synonym_map)
            if profile is not None and profile.synonym_map is not None and expand_synonyms is not False
            else None
        )
        return cls(snapshot, profile, expander)

    def metadata(self) -> dict[str, Any]:
        return {
            "catalog_version": self.snapshot.digest,
            "catalog_etag": self.snapshot.etag,
            "catalog_operation_id": self.snapshot.operation_id,
            "scoring_profile": self.profile.name if self.profile is not None else None,
            "synonym_map": self.expander.map_name if self.expander is not None else None,
        }


def build_catalog_item(
    source: Mapping[str, Any],
    deployment_instance_id: str,
    *,
    operation_id: str | None = None,
    changed_at: datetime | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Build a direct-edit item, optionally carrying guarded-writer metadata."""
    if not isinstance(deployment_instance_id, str) or not deployment_instance_id.strip():
        raise CatalogError("deployment instance id is required")
    if not isinstance(source, Mapping) or set(source) != {"config"}:
        raise CatalogError("catalog source contains unknown top-level fields")
    if not isinstance(source.get("config"), dict):
        raise CatalogError("catalog source must contain config")
    _reject_non_finite(source)
    config = _normalize_json_numbers(source["config"])
    item = {
        "id": CATALOG_ID,
        "deploymentInstanceId": deployment_instance_id.strip(),
        "type": CATALOG_TYPE,
        "config": config,
    }
    if any(value is not None for value in (operation_id, changed_at, reason)):
        if not isinstance(changed_at, datetime) or changed_at.tzinfo is None or changed_at.utcoffset() is None:
            raise CatalogError("catalog timestamp must have a timezone")
        item["change"] = {
            "operationId": operation_id,
            "changedAt": changed_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "reason": reason,
        }
    load_catalog_item(item)
    return item


def load_catalog_item(item: Mapping[str, Any]) -> RuntimeCatalogSnapshot:
    """Validate the complete current item and freeze all relevance objects."""
    if not isinstance(item, Mapping):
        raise CatalogError("catalog must be an object")
    values = _without_cosmos_system_properties(item)
    if len(_canonical_json(values)) > MAX_CATALOG_ITEM_BYTES:
        raise CatalogError(
            f"catalog item exceeds {MAX_CATALOG_ITEM_BYTES} UTF-8 bytes"
        )
    _validate_schema(CATALOG_ITEM_SCHEMA, values, "catalog")
    _reject_non_finite(values)
    config = _normalize_json_numbers(values["config"])
    _validate_supported_fields(config)
    change = values.get("change")
    if change is not None:
        try:
            UUID(change["operationId"])
            datetime.fromisoformat(change["changedAt"].replace("Z", "+00:00"))
        except ValueError:
            raise CatalogError("catalog change identity or UTC timestamp is invalid") from None
        if not change["reason"].strip():
            raise CatalogError("catalog change reason must not be blank")
    if not values["deploymentInstanceId"].strip():
        raise CatalogError("catalog identity and change reason must not be blank")
    etag = item.get("_etag")
    if etag is not None and (not isinstance(etag, str) or not etag.strip()):
        raise CatalogError("catalog ETag is invalid")

    try:
        profiles = load_scoring_profiles({"profiles": config["profiles"]})
        synonym_maps = load_synonym_maps({"maps": config["synonymMaps"]})
        validate_profile_synonym_map_references(profiles, synonym_maps)
    except ConfigLoaderError:
        raise CatalogError("catalog profile, synonym rule, or map reference is invalid") from None

    default_profile = config.get("defaultProfile")
    if default_profile is not None and default_profile not in profiles:
        raise CatalogError("default profile is not defined")
    retrieval = config["retrieval"]
    weights = retrieval["hybridWeights"]
    return RuntimeCatalogSnapshot(
        deployment_instance_id=values["deploymentInstanceId"],
        catalog_id=values["id"],
        etag=etag,
        digest="sha256:" + hashlib.sha256(_canonical_json(_normalize_json_numbers(config))).hexdigest(),
        operation_id=change["operationId"] if change is not None else None,
        changed_at=change["changedAt"] if change is not None else None,
        accepted_at=datetime.now(timezone.utc),
        over_fetch_factor=retrieval["overFetchFactor"],
        hybrid_weights=(float(weights["vector"]), float(weights["text"])),
        full_text_score_scope=retrieval["fullTextScoreScope"],
        default_profile=default_profile,
        profiles=profiles,
        synonym_maps=synonym_maps,
        synonym_expanders={name: SynonymExpander(synonym_map) for name, synonym_map in synonym_maps.items()},
    )


class RuntimeCatalogLoader:
    def __init__(
        self,
        container: Any,
        deployment_instance_id: str,
    ) -> None:
        self._container = container
        self._deployment_instance_id = deployment_instance_id
        if not isinstance(deployment_instance_id, str) or not deployment_instance_id.strip():
            raise CatalogError("deployment instance id is required")

    def load(self) -> RuntimeCatalogSnapshot:
        try:
            item = self._container.read_item(
                item=CATALOG_ID,
                partition_key=self._deployment_instance_id,
            )
        except CosmosResourceNotFoundError:
            raise CatalogMissingError("runtime catalog item is missing") from None
        except Exception:
            raise CatalogError("runtime catalog item read failed") from None
        try:
            catalog = load_catalog_item(item)
            if catalog.deployment_instance_id != self._deployment_instance_id or catalog.etag is None:
                raise CatalogError("runtime catalog identity or ETag is inconsistent")
        except Exception:
            etag = item.get("_etag") if isinstance(item, Mapping) else None
            raise CatalogError(
                "runtime catalog validation failed",
                etag=etag if isinstance(etag, str) and etag.strip() else None,
            ) from None
        return catalog

    def baseline(self) -> RuntimeCatalogSnapshot:
        """Return the built-in baseline snapshot for this loader's deployment instance."""
        return _build_baseline_snapshot(self._deployment_instance_id)


def _build_baseline_snapshot(deployment_instance_id: str) -> RuntimeCatalogSnapshot:
    """Return the built-in baseline snapshot used when no catalog item exists."""
    item = {
        "id": CATALOG_ID,
        "deploymentInstanceId": deployment_instance_id,
        "type": CATALOG_TYPE,
        "config": BASELINE_CONFIG,
        "_etag": BASELINE_ETAG,
    }
    return load_catalog_item(item)


def publish_catalog(container: Any, item: dict[str, Any]) -> None:
    """Bootstrap without overwriting any existing item, then verify its full body."""
    expected = load_catalog_item(item)
    try:
        container.create_item(body=item)
    except CosmosResourceExistsError:
        pass
    except Exception:
        raise CatalogError("catalog create failed") from None
    try:
        persisted = container.read_item(
            item=expected.catalog_id,
            partition_key=expected.deployment_instance_id,
        )
    except Exception:
        raise CatalogError("catalog verification read failed") from None
    load_catalog_item(persisted)
    if _canonical_json(_normalize_json_numbers(_without_cosmos_system_properties(persisted))) != _canonical_json(
        _normalize_json_numbers(_without_cosmos_system_properties(item))
    ):
        raise CatalogConflictError("existing runtime catalog differs; explicit disposition required")


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise CatalogError("catalog is not canonical JSON") from error


def _normalize_json_numbers(value: Any) -> Any:
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, Mapping):
        return {key: _normalize_json_numbers(child) for key, child in value.items()}
    if isinstance(value, list):
        return [_normalize_json_numbers(child) for child in value]
    return value


def _validate_schema(schema: Mapping[str, Any], value: Mapping[str, Any], label: str) -> None:
    try:
        Draft202012Validator(schema).validate(value)
    except ValidationError as error:
        raise CatalogError(f"{label} is invalid ({error.validator})") from None


def _reject_non_finite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise CatalogError("catalog numbers must be finite")
    if isinstance(value, Mapping):
        for child in value.values():
            _reject_non_finite(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_non_finite(child)


def _validate_supported_fields(config: Mapping[str, Any]) -> None:
    for profile in config["profiles"]:
        unknown_weights = set(profile.get("textWeights", {})) - _ALLOWED_TEXT_WEIGHT_FIELDS
        if unknown_weights:
            raise CatalogError(f"unsupported text weight field: {sorted(unknown_weights)[0]}")
        for function in profile.get("functions", []):
            allowed_fields = _ALLOWED_FUNCTION_FIELDS.get(function["type"], frozenset())
            if function["fieldName"] not in allowed_fields:
                raise CatalogError(
                    f"unsupported {function['type']} field: {function['fieldName']}"
                )


def _without_cosmos_system_properties(item: Mapping[str, Any]) -> dict[str, Any]:
    system_properties = {"_rid", "_self", "_etag", "_attachments", "_ts"}
    return {key: value for key, value in item.items() if key not in system_properties}


def _validate_operation_manifest(manifest: Mapping[str, Any]) -> datetime:
    fields = {
        "operationId", "createdAt", "deploymentInstanceId", "expectedEtag",
        "candidateDigest", "candidateFileDigest", "reason",
    }
    if not isinstance(manifest, Mapping) or set(manifest) != fields:
        raise CatalogError("operation manifest fields are invalid")
    if any(not isinstance(value, str) or not value.strip() for value in manifest.values()):
        raise CatalogError("operation manifest values are invalid")
    for field in ("candidateDigest", "candidateFileDigest"):
        digest = manifest[field]
        if len(digest) != 71 or not digest.startswith("sha256:") or any(char not in "0123456789abcdef" for char in digest[7:]):
            raise CatalogError("operation manifest digest is invalid")
    _validate_schema(CATALOG_ITEM_SCHEMA["properties"]["deploymentInstanceId"], manifest["deploymentInstanceId"], "operation deployment")
    _validate_schema(CATALOG_ITEM_SCHEMA["properties"]["change"], {
        "operationId": manifest["operationId"], "changedAt": manifest["createdAt"],
        "reason": manifest["reason"],
    }, "operation change")
    try:
        UUID(manifest["operationId"])
        changed_at = datetime.fromisoformat(manifest["createdAt"].replace("Z", "+00:00"))
    except ValueError:
        raise CatalogError("operation identity or timestamp is invalid") from None
    if changed_at.isoformat().replace("+00:00", "Z") != manifest["createdAt"]:
        raise CatalogError("operation timestamp must use canonical UTC")
    return changed_at


def build_operation_candidate(
    manifest: Mapping[str, Any], source: Mapping[str, Any], file_digest: str,
) -> dict[str, Any]:
    changed_at = _validate_operation_manifest(manifest)
    if manifest["candidateFileDigest"] != file_digest:
        raise CatalogConflictError("candidate file changed since preparation")
    item = build_catalog_item(
        source, manifest["deploymentInstanceId"], operation_id=manifest["operationId"],
        changed_at=changed_at, reason=manifest["reason"],
    )
    if load_catalog_item(item).digest != manifest["candidateDigest"]:
        raise CatalogConflictError("candidate configuration changed since preparation")
    return item


def catalog_body(item: Mapping[str, Any]) -> dict[str, Any]:
    load_catalog_item(item)
    return _normalize_json_numbers(_without_cosmos_system_properties(item))


def _history_item(preimage: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = load_catalog_item(preimage)
    if snapshot.etag != manifest["expectedEtag"] or snapshot.deployment_instance_id != manifest["deploymentInstanceId"]:
        raise CatalogConflictError("history source identity does not match reviewed operation")
    body = catalog_body(preimage)
    digest = "sha256:" + hashlib.sha256(_canonical_json(body)).hexdigest()
    identity = {
        "operationId": manifest["operationId"], "sourceEtag": snapshot.etag,
        "preimageDigest": digest,
    }
    return {
        "id": "history:" + hashlib.sha256(_canonical_json(identity)).hexdigest(),
        "deploymentInstanceId": snapshot.deployment_instance_id,
        "type": "retrieval-runtime-catalog-history",
        "sourceEtag": snapshot.etag,
        "preimageDigest": digest,
        "catalogDigest": snapshot.digest,
        "capturedAt": manifest["createdAt"],
        "operation": dict(manifest),
        "preimage": body,
    }


def load_catalog_history(item: Mapping[str, Any], deployment_instance_id: str) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise CatalogError("history must be an object")
    values = _without_cosmos_system_properties(item)
    fields = {"id", "deploymentInstanceId", "type", "sourceEtag", "preimageDigest", "catalogDigest", "capturedAt", "operation", "preimage"}
    if set(values) != fields or values.get("deploymentInstanceId") != deployment_instance_id:
        raise CatalogError("history identity or fields are invalid")
    preimage = values["preimage"]
    if not isinstance(preimage, dict) or any(key.startswith("_") for key in preimage):
        raise CatalogError("history preimage must be a catalog body")
    operation = values["operation"]
    if not isinstance(operation, dict):
        raise CatalogError("history operation is invalid")
    _validate_operation_manifest(operation)
    body = catalog_body(preimage)
    expected = _history_item({**body, "_etag": values["sourceEtag"]}, operation)
    if _canonical_json(values) != _canonical_json(expected):
        raise CatalogConflictError("history content does not match its identity")
    return body


def _verify_history(container: Any, expected: dict[str, Any]) -> None:
    try:
        container.create_item(body=expected)
    except Exception as error:
        if getattr(error, "status_code", None) != 409:
            raise CatalogError("history create failed before replacement") from None
    try:
        actual = container.read_item(item=expected["id"], partition_key=expected["deploymentInstanceId"])
    except Exception:
        raise CatalogError("history verification read failed before replacement") from None
    load_catalog_history(actual, expected["deploymentInstanceId"])
    if _canonical_json(_without_cosmos_system_properties(actual)) != _canonical_json(expected):
        raise CatalogConflictError("history collision does not match prepared operation")


def _find_operation_history(container: Any, manifest: Mapping[str, Any]) -> str | None:
    records = list(container.query_items(
        query="SELECT * FROM c WHERE c.type = @type AND c.operation.operationId = @operationId",
        parameters=[
            {"name": "@type", "value": "retrieval-runtime-catalog-history"},
            {"name": "@operationId", "value": manifest["operationId"]},
        ],
        partition_key=manifest["deploymentInstanceId"],
    ))
    if len(records) != 1:
        return None
    record = records[0]
    load_catalog_history(record, manifest["deploymentInstanceId"])
    if record["operation"] != manifest:
        raise CatalogConflictError("history operation does not match manifest")
    return record["id"]


def _operation_outcome(container: Any, manifest: Mapping[str, Any], result: dict[str, Any], *, create: bool) -> None:
    identity = {
        "id": "runtime-catalog-outcome:" + manifest["operationId"],
        "deploymentInstanceId": manifest["deploymentInstanceId"],
        "type": "retrieval-runtime-catalog-outcome",
        "manifestDigest": "sha256:" + hashlib.sha256(_canonical_json(manifest)).hexdigest(),
        "catalogDigest": result["catalogDigest"], "etag": result["etag"],
        "historyId": result["historyId"],
    }
    if not result["etag"] or not result["historyId"]:
        return
    try:
        if create:
            if not result["activityId"] or not result["resourceId"]:
                return
            try:
                container.create_item(body={**identity, "activityId": result["activityId"], "resourceId": result["resourceId"]})
            except Exception as error:
                if getattr(error, "status_code", None) != 409:
                    raise
        actual = _without_cosmos_system_properties(container.read_item(
            item=identity["id"], partition_key=manifest["deploymentInstanceId"],
        ))
        if set(actual) != set(identity) | {"activityId", "resourceId"}:
            return
        if any(actual[key] != value for key, value in identity.items()):
            return
        if not all(isinstance(actual[key], str) and actual[key] for key in ("activityId", "resourceId")):
            return
        if actual["resourceId"] != result["resourceId"]:
            return
        if create and actual["activityId"] != result["activityId"]:
            return
        result["activityId"] = actual["activityId"]
        result["auditStatus"] = "semantic-outcome-verified"
    except Exception:
        result["auditStatus"] = "semantic-outcome-unverified"


def apply_catalog_operation(
    container: Any, candidate: Mapping[str, Any], manifest: Mapping[str, Any],
    *, reconcile_only: bool = False,
) -> dict[str, Any]:
    expected = build_operation_candidate(manifest, {"config": candidate.get("config")}, manifest.get("candidateFileDigest"))
    if catalog_body(candidate) != expected:
        raise CatalogConflictError("candidate envelope does not match prepared operation")
    snapshot = load_catalog_item(expected)
    result = {
        "status": "applied-but-unassured", "operationId": snapshot.operation_id,
        "catalogDigest": snapshot.digest, "etag": None, "historyId": None,
        "activityId": None, "resourceId": None, "profileCount": len(snapshot.profiles),
        "mapCount": len(snapshot.synonym_maps), "auditStatus": "not-observed",
    }
    try:
        current = container.read_item(item=CATALOG_ID, partition_key=snapshot.deployment_instance_id)
    except Exception:
        raise CatalogError("runtime catalog read failed before replacement") from None
    current_snapshot = load_catalog_item(current)
    if current_snapshot.deployment_instance_id != snapshot.deployment_instance_id or current_snapshot.etag is None:
        raise CatalogConflictError("runtime catalog identity or ETag is invalid")
    if current_snapshot.operation_id == snapshot.operation_id:
        if catalog_body(current) != expected:
            raise CatalogConflictError("operation ID was reused with different content")
        result["etag"] = current_snapshot.etag
        result["resourceId"] = current.get("_rid")
        try:
            result["historyId"] = _find_operation_history(container, manifest)
        except Exception:
            result["auditStatus"] = "history-unverified"
        _operation_outcome(container, manifest, result, create=False)
        return result
    if reconcile_only:
        result["status"] = "not-current"
        return result
    if current_snapshot.etag != manifest["expectedEtag"]:
        raise CatalogConflictError("reviewed ETag is stale; inspect and prepare a new operation")
    history = _history_item(current, manifest)
    _verify_history(container, history)
    result["historyId"] = history["id"]

    response_etag = None

    def capture_response(headers: Mapping[str, Any], body: Any) -> None:
        nonlocal response_etag
        activity = headers.get("x-ms-activity-id")
        if isinstance(activity, str) and activity:
            result["activityId"] = activity
        response_etag = headers.get("etag") or (body.get("_etag") if isinstance(body, Mapping) else None)

    try:
        container.replace_item(
            item=CATALOG_ID, body=expected, etag=manifest["expectedEtag"],
            match_condition=MatchConditions.IfNotModified, response_hook=capture_response,
            retry_write=False,
        )
    except Exception as error:
        if getattr(error, "status_code", None) == 412:
            raise CatalogConflictError("ETag conflict; replacement was not applied and must not be retried") from None
        result["status"] = "outcome-unknown"
    try:
        persisted = container.read_item(item=CATALOG_ID, partition_key=snapshot.deployment_instance_id)
        observed = load_catalog_item(persisted)
        if observed.etag is not None and catalog_body(persisted) == expected:
            result["etag"] = observed.etag
            result["resourceId"] = persisted.get("_rid")
            result["status"] = "applied-but-unassured"
            if response_etag == observed.etag:
                _operation_outcome(container, manifest, result, create=True)
        else:
            result["status"] = "outcome-unknown"
    except Exception:
        result["status"] = "outcome-unknown"
    return result
