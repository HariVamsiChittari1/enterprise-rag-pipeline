from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import FrozenInstanceError
from copy import deepcopy
from typing import Any

import pytest
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosResourceNotFoundError

from retrieval.catalog import (
    BASELINE_ETAG,
    CatalogConflictError,
    CatalogError,
    CatalogMissingError,
    RuntimeCatalogLoader,
    RequestPolicy,
    UnknownScoringProfileError,
    build_catalog_item,
    apply_catalog_operation,
    load_catalog_history,
    load_catalog_item,
    publish_catalog,
)


def _source() -> dict[str, Any]:
    return {
        "config": {
            "retrieval": {
                "overFetchFactor": 3,
                "hybridWeights": {"vector": 2.0, "text": 1.0},
                "fullTextScoreScope": "Global",
            },
            "defaultProfile": "hr",
            "profiles": [
                {
                    "name": "hr",
                    "synonymMap": "hr-en",
                    "textWeights": {"sourceName": 1.5, "content": 1.0},
                    "functionAggregation": "sum",
                    "functions": [
                        {
                            "type": "freshness",
                            "fieldName": "sourceModifiedAt",
                            "boost": 0.15,
                            "interpolation": "linear",
                            "freshness": {"boostingDuration": "P180D"},
                        },
                    ],
                }
            ],
            "synonymMaps": [
                {
                    "name": "hr-en",
                    "format": "solr",
                    "rules": ["annual leave, vacation, paid time off"],
                }
            ],
        },
    }


def test_given_config_source_when_built_for_direct_editing_then_no_writer_metadata_required() -> None:
    item = build_catalog_item(_source(), "dev")

    assert set(item) == {"id", "deploymentInstanceId", "type", "config"}
    snapshot = load_catalog_item(item)
    assert snapshot.operation_id is None
    assert snapshot.changed_at is None
    assert snapshot.default_profile == "hr"
    assert RequestPolicy.capture(snapshot).metadata()["catalog_operation_id"] is None


class FakeContainer:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.etag = "etag-1"

    def create_item(self, *, body: dict[str, Any]) -> dict[str, Any]:
        key = (body["deploymentInstanceId"], body["id"])
        if key in self.items:
            raise CosmosResourceExistsError(status_code=409, message="exists")
        stored = dict(body)
        stored["_etag"] = self.etag
        stored["_rid"] = "synthetic-" + body["id"]
        self.items[key] = stored
        return stored

    def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
        try:
            return dict(self.items[(partition_key, item)])
        except KeyError:
            raise CosmosResourceNotFoundError(status_code=404, message="missing") from None

    def replace_item(
        self, *, item: str, body: dict[str, Any], etag: str, match_condition: Any,
        response_hook=None, retry_write=False,
    ) -> dict[str, Any]:
        key = (body["deploymentInstanceId"], item)
        if key not in self.items:
            raise CosmosResourceNotFoundError(status_code=404, message="missing")
        if etag != self.items[key]["_etag"]:
            error = RuntimeError("conflict")
            error.status_code = 412  # type: ignore[attr-defined]
            raise error
        stored = dict(body)
        stored["_etag"] = "etag-2"
        stored["_rid"] = "synthetic-" + item
        self.items[key] = stored
        if response_hook is not None:
            response_hook({"x-ms-activity-id": "test-activity"}, stored)
        return stored

    def query_items(self, *, query, parameters, partition_key):
        operation_id = parameters[1]["value"]
        return [
            dict(item) for (partition, _identity), item in self.items.items()
            if partition == partition_key and item.get("operation", {}).get("operationId") == operation_id
        ]


def _item(source: dict[str, Any] | None = None) -> dict[str, Any]:
    return build_catalog_item(
        source if source is not None else _source(), "dev",
        operation_id="b14c1a67-9fc4-4e78-a818-e5951473d15b",
        changed_at=datetime(2026, 8, 26, tzinfo=timezone.utc), reason="Test configuration",
    )


def _operation():
    candidate = build_catalog_item(
        _source(), "dev", operation_id="a14c1a67-9fc4-4e78-a818-e5951473d15b",
        changed_at=datetime(2026, 9, 8, tzinfo=timezone.utc), reason="Updated policy",
    )
    manifest = {
        "operationId": candidate["change"]["operationId"], "createdAt": candidate["change"]["changedAt"],
        "deploymentInstanceId": "dev", "expectedEtag": "etag-1",
        "candidateDigest": load_catalog_item(candidate).digest,
        "candidateFileDigest": "sha256:" + "c" * 64, "reason": "Updated policy",
    }
    return candidate, manifest


def test_given_guarded_update_when_retried_then_history_and_replacement_are_not_duplicated():
    container = FakeContainer()
    original = _item()
    publish_catalog(container, original)
    candidate, manifest = _operation()
    first = apply_catalog_operation(container, candidate, manifest)
    assert first["status"] == "applied-but-unassured"
    assert first["activityId"] == "test-activity"
    assert first["etag"] == "etag-2"
    history = container.items[("dev", first["historyId"])]
    assert load_catalog_history(history, "dev") == original
    second = apply_catalog_operation(container, candidate, manifest)
    assert second["status"] == "applied-but-unassured"
    assert second["historyId"] == first["historyId"]
    assert second["activityId"] == first["activityId"]
    assert first["auditStatus"] == second["auditStatus"] == "semantic-outcome-verified"
    assert len(container.items) == 3


def test_given_stale_etag_when_updating_then_no_history_or_replacement():
    container = FakeContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    manifest["expectedEtag"] = "stale"
    with pytest.raises(CatalogConflictError, match="ETag"):
        apply_catalog_operation(container, candidate, manifest)
    assert len(container.items) == 1


@pytest.mark.parametrize("digest", [None, "", "not-a-digest", "sha256:" + "g" * 64])
def test_given_malformed_history_candidate_digest_when_loaded_then_rejected(digest: Any) -> None:
    container = FakeContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    result = apply_catalog_operation(container, candidate, manifest)
    history = deepcopy(container.items[("dev", result["historyId"])])
    history["operation"]["candidateDigest"] = digest

    with pytest.raises(CatalogError):
        load_catalog_history(history, "dev")


def test_given_missing_history_candidate_digest_when_loaded_then_rejected() -> None:
    container = FakeContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    result = apply_catalog_operation(container, candidate, manifest)
    history = deepcopy(container.items[("dev", result["historyId"])])
    del history["operation"]["candidateDigest"]

    with pytest.raises(CatalogError):
        load_catalog_history(history, "dev")


def test_given_unapplied_operation_when_reconciling_then_no_mutation():
    container = FakeContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    result = apply_catalog_operation(container, candidate, manifest, reconcile_only=True)
    assert result["status"] == "not-current"
    assert len(container.items) == 1


@pytest.mark.parametrize("after_replace", [False, True])
def test_given_interrupted_operation_when_resumed_then_only_one_replacement(after_replace):
    class InterruptedContainer(FakeContainer):
        interrupted = False
        replacements = 0

        def replace_item(self, **kwargs):
            if not self.interrupted:
                self.interrupted = True
                if after_replace:
                    self.replacements += 1
                    super().replace_item(**kwargs)
                raise KeyboardInterrupt()
            self.replacements += 1
            return super().replace_item(**kwargs)

    container = InterruptedContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    with pytest.raises(KeyboardInterrupt):
        apply_catalog_operation(container, candidate, manifest)
    result = apply_catalog_operation(container, candidate, manifest)
    assert result["status"] == "applied-but-unassured"
    assert container.replacements == 1
    assert len(container.items) == (2 if after_replace else 3)


@pytest.mark.parametrize("failure", ["missing", "etag", "history", "resource", "manifest"])
def test_writer_replay_does_not_assure_missing_or_changed_outcome(failure):
    container = FakeContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    first = apply_catalog_operation(container, candidate, manifest)
    key = ("dev", "runtime-catalog-outcome:" + manifest["operationId"])
    if failure == "missing":
        del container.items[key]
    else:
        field = {"etag": "etag", "history": "historyId", "resource": "resourceId", "manifest": "manifestDigest"}[failure]
        container.items[key][field] = "other"
    replay = apply_catalog_operation(container, candidate, manifest, reconcile_only=True)
    assert first["auditStatus"] == "semantic-outcome-verified"
    assert replay["auditStatus"] != "semantic-outcome-verified"
    assert replay["status"] == "applied-but-unassured"
    assert replay["activityId"] is None


def test_unchanged_body_with_new_etag_cannot_reuse_writer_outcome():
    container = FakeContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    apply_catalog_operation(container, candidate, manifest)
    container.items[("dev", "runtime-catalog")]["_etag"] = "direct-edit-etag"
    replay = apply_catalog_operation(container, candidate, manifest, reconcile_only=True)
    assert replay["auditStatus"] != "semantic-outcome-verified"
    assert replay["activityId"] is None


@pytest.mark.parametrize("field", ["type", "sourceEtag", "preimageDigest", "operation", "preimage"])
def test_given_history_collision_when_content_differs_then_abort_before_replace(field):
    class CollidingContainer(FakeContainer):
        def create_item(self, *, body):
            if body["type"] == "retrieval-runtime-catalog-history":
                conflicting = deepcopy(body)
                conflicting[field] = "corrupted"
                self.items[(body["deploymentInstanceId"], body["id"])] = conflicting
                raise CosmosResourceExistsError(status_code=409, message="exists")
            return super().create_item(body=body)

    container = CollidingContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    with pytest.raises(CatalogError):
        apply_catalog_operation(container, candidate, manifest)
    assert container.items[("dev", "runtime-catalog")]["_etag"] == "etag-1"


def test_given_replace_412_when_updating_then_conflict_is_terminal():
    class ConflictingContainer(FakeContainer):
        replacements = 0

        def replace_item(self, **kwargs):
            self.replacements += 1
            assert kwargs["retry_write"] is False
            error = RuntimeError("conflict")
            error.status_code = 412
            raise error

    container = ConflictingContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    with pytest.raises(CatalogConflictError, match="must not be retried"):
        apply_catalog_operation(container, candidate, manifest)
    assert container.replacements == 1


def test_given_response_lost_after_replace_when_retried_then_reconcile_without_second_write():
    class UncertainContainer(FakeContainer):
        replacements = 0

        def replace_item(self, **kwargs):
            self.replacements += 1
            super().replace_item(**kwargs)
            raise RuntimeError("response lost")

    container = UncertainContainer()
    publish_catalog(container, _item())
    candidate, manifest = _operation()
    assert apply_catalog_operation(container, candidate, manifest)["status"] == "applied-but-unassured"
    assert apply_catalog_operation(container, candidate, manifest, reconcile_only=True)["status"] == "applied-but-unassured"
    assert container.replacements == 1


def test_build_catalog_is_deterministic_and_loads_runtime_values() -> None:
    first = _item()
    second = _item()
    assert first["id"] == second["id"]
    catalog = load_catalog_item(first)
    assert catalog.over_fetch_factor == 3
    assert catalog.hybrid_weights == (2.0, 1.0)
    assert catalog.default_profile == "hr"
    assert set(catalog.profiles) == {"hr"}
    assert set(catalog.synonym_maps) == {"hr-en"}


def test_given_current_singleton_when_loaded_then_validates_strict_contract() -> None:
    config = _source()["config"]
    del config["defaultProfile"]
    item = {
        "id": "runtime-catalog",
        "deploymentInstanceId": "dev",
        "type": "retrieval-runtime-catalog",
        "change": {
            "operationId": "b14c1a67-9fc4-4e78-a818-e5951473d15b",
            "changedAt": "2026-09-07T15:47:05Z",
            "reason": "Test configuration",
        },
        "config": config,
        "_etag": "etag-1",
    }

    catalog = load_catalog_item(item)

    assert catalog.catalog_id == "runtime-catalog"
    assert catalog.default_profile is None
    with pytest.raises(CatalogError):
        load_catalog_item({**item, "schemaVersion": 1})


def test_build_catalog_normalizes_integral_floats_for_cosmos_persistence() -> None:
    source = _source()

    item = _item(source)

    assert source["config"]["retrieval"]["hybridWeights"] == {
        "vector": 2.0,
        "text": 1.0,
    }
    assert item["config"]["retrieval"]["hybridWeights"] == {
        "vector": 2,
        "text": 1,
    }
    assert item["config"]["profiles"][0]["textWeights"]["content"] == 1
    assert item["config"]["profiles"][0]["textWeights"]["sourceName"] == 1.5
    assert load_catalog_item(item).hybrid_weights == (2.0, 1.0)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("config", "profiles", 0, "textWeights", "unknown"), 1.0, "invalid"),
        (("config", "profiles", 0, "functions", 0, "fieldName"), "createdAt", "invalid"),
    ],
)
def test_catalog_rejects_unsupported_signals(path, value, message) -> None:
    source = _source()
    target: Any = source
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(CatalogError, match=message):
        _item(source)


def test_catalog_rejects_nonfinite_numbers() -> None:
    source = _source()
    source["config"]["retrieval"]["hybridWeights"]["vector"] = float("nan")
    with pytest.raises(CatalogError, match="finite"):
        _item(source)


def test_catalog_rejects_unknown_authoring_top_level_field() -> None:
    source = _source()
    source["guardrails"] = {"rejectUnknownFields": False}
    with pytest.raises(CatalogError, match="unknown top-level"):
        _item(source)


def test_catalog_rejects_item_over_conservative_size_limit() -> None:
    source = _source()
    source["config"]["synonymMaps"][0]["rules"] = [
        "a, " + ("x" * 1_573_000)
    ]
    with pytest.raises(CatalogError, match="exceeds"):
        _item(source)


def test_catalog_loader_rejects_oversized_persisted_item() -> None:
    item = _item()
    item["config"]["synonymMaps"][0]["rules"] = ["a, " + ("x" * 1_573_000)]
    with pytest.raises(CatalogError, match="exceeds"):
        load_catalog_item(item)


def test_catalog_loader_rejects_unknown_underscore_property() -> None:
    item = _item()
    item["_custom"] = "must not be silently discarded"
    with pytest.raises(CatalogError, match="additionalProperties"):
        load_catalog_item(item)


def test_catalog_rejects_unknown_default_profile_and_map_reference() -> None:
    source = _source()
    source["config"]["defaultProfile"] = "missing"
    with pytest.raises(CatalogError, match="default profile"):
        _item(source)

    source = _source()
    source["config"]["profiles"][0]["synonymMap"] = "missing"
    with pytest.raises(CatalogError, match="map reference"):
        _item(source)


def test_given_changed_config_when_loaded_then_digest_changes() -> None:
    item = _item()
    previous = load_catalog_item(item)
    item["config"]["retrieval"]["overFetchFactor"] = 4
    assert load_catalog_item(item).digest != previous.digest


def test_publish_is_idempotent_and_loader_reads_fixed_item() -> None:
    container = FakeContainer()
    item = _item()
    publish_catalog(container, item)
    publish_catalog(container, item)
    loaded = RuntimeCatalogLoader(container, "dev").load()
    assert loaded.digest == load_catalog_item(item).digest
    assert loaded.etag == "etag-1"
    assert set(container.items) == {("dev", "runtime-catalog")}


@pytest.mark.parametrize("field", ["config", "operationId", "changedAt", "reason"])
def test_given_conflicting_bootstrap_when_published_then_existing_item_is_untouched(field: str) -> None:
    container = FakeContainer()
    first_item = _item()
    publish_catalog(container, first_item)
    second = _item()
    if field == "config":
        second["config"]["retrieval"]["overFetchFactor"] = 4
    else:
        second["change"][field] = {
            "operationId": "e8fcd56e-2c25-4010-b8fb-73076bc7b016",
            "changedAt": "2026-09-08T00:00:00Z", "reason": "Different reason",
        }[field]
    with pytest.raises(CatalogConflictError, match="explicit disposition"):
        publish_catalog(container, second)
    assert container.read_item(item="runtime-catalog", partition_key="dev")["config"] == first_item["config"]
    assert container.read_item(item="runtime-catalog", partition_key="dev")["change"] == first_item["change"]


def test_loader_reports_missing_and_baseline_factory_supplies_defaults() -> None:
    container = FakeContainer()
    loader = RuntimeCatalogLoader(container, "dev")
    with pytest.raises(CatalogMissingError, match="missing"):
        loader.load()

    baseline = loader.baseline()
    assert baseline.etag == BASELINE_ETAG
    assert baseline.over_fetch_factor == 3
    assert baseline.hybrid_weights == (1.0, 1.0)
    assert baseline.full_text_score_scope == "Global"
    assert dict(baseline.profiles) == {}
    assert dict(baseline.synonym_maps) == {}
    assert baseline.default_profile is None
    assert baseline.deployment_instance_id == "dev"
    policy = RequestPolicy.capture(baseline)
    assert policy.profile is None
    assert policy.expander is None

    item = _item()
    publish_catalog(container, item)
    del container.items[("dev", item["id"])]
    with pytest.raises(CatalogMissingError, match="missing"):
        loader.load()


def test_loader_fails_closed_on_read_error() -> None:
    class BrokenContainer:
        def read_item(self, *, item: str, partition_key: str) -> dict[str, Any]:
            raise RuntimeError("transient")

    with pytest.raises(CatalogError, match="read failed"):
        RuntimeCatalogLoader(BrokenContainer(), "dev").load()


@pytest.mark.parametrize(
    "field,value",
    [("operationId", "invalid"), ("changedAt", "2026-02-30T00:00:00Z"), ("reason", " "), ("reason", "x" * 501)],
)
def test_given_invalid_envelope_when_loaded_then_rejected(field: str, value: str) -> None:
    item = _item()
    item["change"][field] = value
    with pytest.raises(CatalogError):
        load_catalog_item(item)


def test_given_snapshot_when_nested_state_mutated_then_rejected() -> None:
    snapshot = load_catalog_item(_item())
    with pytest.raises(TypeError):
        snapshot.profiles["other"] = snapshot.profiles["hr"]
    with pytest.raises(TypeError):
        snapshot.profiles["hr"].text_weights["content"] = 2
    with pytest.raises(FrozenInstanceError):
        snapshot.synonym_expanders["hr-en"]._map = snapshot.synonym_maps["hr-en"]
    with pytest.raises(FrozenInstanceError):
        snapshot.synonym_maps["hr-en"].rules = ()


def test_given_same_config_when_envelope_or_numeric_representation_changes_then_digest_is_stable() -> None:
    item = _item()
    previous = load_catalog_item(item)
    item["config"]["retrieval"]["hybridWeights"]["vector"] = 2.0
    item["change"]["operationId"] = "e8fcd56e-2c25-4010-b8fb-73076bc7b016"
    assert load_catalog_item(item).digest == previous.digest


@pytest.mark.parametrize("field", ["type", "fieldName", "boost", "interpolation", "freshness"])
def test_given_incomplete_function_when_loaded_then_catalog_error(field: str) -> None:
    item = _item()
    del item["config"]["profiles"][0]["functions"][0][field]
    with pytest.raises(CatalogError, match="invalid"):
        load_catalog_item(item)


@pytest.mark.parametrize("field", ["synonymsEnabled", "scoring", "freshness", "synonyms", "enabledProfiles"])
def test_given_feature_switch_when_loaded_then_rejected(field: str) -> None:
    item = _item()
    item["config"][field] = True
    with pytest.raises(CatalogError, match="additionalProperties"):
        load_catalog_item(item)


def test_given_no_profiles_or_maps_when_default_absent_then_valid() -> None:
    source = _source()
    del source["config"]["defaultProfile"]
    source["config"]["profiles"] = []
    source["config"]["synonymMaps"] = []
    assert load_catalog_item(_item(source)).default_profile is None


def test_given_null_default_when_loaded_then_rejected() -> None:
    item = _item()
    item["config"]["defaultProfile"] = None
    with pytest.raises(CatalogError):
        load_catalog_item(item)


def test_given_snapshot_when_policy_captured_then_selection_and_opt_out_are_fixed() -> None:
    item = _item()
    snapshot = load_catalog_item(item)
    policy = RequestPolicy.capture(snapshot)
    assert policy.profile is snapshot.profiles["hr"]
    assert policy.expander is snapshot.synonym_expanders["hr-en"]
    assert RequestPolicy.capture(snapshot, expand_synonyms=False).expander is None
    del item["config"]["defaultProfile"]
    unscored = load_catalog_item(item)
    assert RequestPolicy.capture(unscored).profile is None
    assert RequestPolicy.capture(unscored, "hr").profile is unscored.profiles["hr"]
    assert policy.snapshot is snapshot
    with pytest.raises(UnknownScoringProfileError):
        RequestPolicy.capture(unscored, "removed")
