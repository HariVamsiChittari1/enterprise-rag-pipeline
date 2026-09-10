from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from retrieval.catalog import CatalogConflictError
from tools.publish_retrieval_catalog import _load_candidate, _persist_manifest, _prepare_operation
from tools import publish_retrieval_catalog as publisher

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PUBLISHER = PROJECT_ROOT / "tools" / "publish_retrieval_catalog.py"


def _source() -> dict:
    return {
        "config": {
            "retrieval": {
                "overFetchFactor": 3,
                "hybridWeights": {"vector": 2.0, "text": 1.0},
                "fullTextScoreScope": "Global",
            },
            "defaultProfile": "default",
            "profiles": [{
                "name": "default",
                "textWeights": {"content": 1.0},
                "functionAggregation": "sum",
                "functions": [],
            }],
            "synonymMaps": [],
        },
    }


def test_validate_prints_deterministic_catalog_identity(tmp_path: Path) -> None:
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(_source()), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(PUBLISHER), "validate", "--file", str(path), "--deployment-instance-id", "instance-a"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    output = json.loads(result.stdout)
    assert output["catalogId"] == "runtime-catalog"
    assert output["catalogDigest"].startswith("sha256:")


def test_validate_rejects_malformed_catalog(tmp_path: Path) -> None:
    path = tmp_path / "catalog.json"
    path.write_text("{}", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(PUBLISHER), "validate", "--file", str(path), "--deployment-instance-id", "instance-a"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "catalog source" in result.stderr


def test_prepare_is_offline_create_only_and_contains_no_catalog_body(tmp_path: Path) -> None:
    path = tmp_path / "catalog.json"
    manifest = tmp_path / "operation.json"
    path.write_text(json.dumps(_source()), encoding="utf-8")
    command = [
        sys.executable, str(PUBLISHER), "prepare", "--file", str(path),
        "--manifest", str(manifest), "--deployment-instance-id", "instance-a",
        "--expected-etag", "etag-a", "--reason", "test operation",
    ]
    first = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True, text=True)
    assert first.returncode == 0, first.stderr
    original = manifest.read_bytes()
    value = json.loads(original)
    assert value["expectedEtag"] == "etag-a"
    assert value["deploymentInstanceId"] == "instance-a"
    assert "config" not in value
    assert "profiles" not in value
    second = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True, text=True)
    assert second.returncode == 2
    assert manifest.read_bytes() == original


def test_given_changed_candidate_file_when_resuming_then_reject_before_access(tmp_path: Path) -> None:
    source = tmp_path / "catalog.json"
    manifest_path = tmp_path / "operation.json"
    source.write_text(json.dumps(_source()), encoding="utf-8")
    manifest = _prepare_operation(source, "instance-a", "etag-a", "Reviewed test")
    _persist_manifest(manifest_path, manifest)
    source.write_text(json.dumps(_source(), indent=2), encoding="utf-8")
    with pytest.raises(CatalogConflictError, match="file changed"):
        _load_candidate(manifest_path, source, "instance-a")


def test_given_stable_manifest_when_resuming_then_identity_is_unchanged(tmp_path: Path) -> None:
    source = tmp_path / "catalog.json"
    manifest_path = tmp_path / "operation.json"
    source.write_text(json.dumps(_source()), encoding="utf-8")
    manifest = _prepare_operation(source, "instance-a", "etag-a", "Reviewed test")
    _persist_manifest(manifest_path, manifest)
    loaded, candidate = _load_candidate(manifest_path, source, "instance-a")
    assert loaded == manifest
    assert candidate["change"]["operationId"] == manifest["operationId"]
    assert candidate["change"]["changedAt"] == manifest["createdAt"]
    with pytest.raises(CatalogConflictError, match="target"):
        _load_candidate(manifest_path, source, "another-instance")


@pytest.mark.parametrize("failure", ["none", "semantic", "diagnostic", "actor", "activity", "adoption", "current", "inaccessible"])
def test_writer_completion_requires_entire_result_bound_chain(monkeypatch, failure):
    args = SimpleNamespace(
        subscription="sub", resource_group="group", application="app", workspace="workspace",
        actor_principal_id="actor", cosmos_account_resource_id="/synthetic/account", database="rag-db",
        container="retrieval-config", deployment_instance_id="synthetic", observation_timeout=60,
    )
    outcome = {
        "status": "applied-but-unassured", "auditStatus": "semantic-outcome-verified",
        "resourceId": "resource", "activityId": "activity", "etag": "etag-a", "catalogDigest": "sha256:" + "a" * 64,
    }
    records = [{"ActivityId": "activity", "AadPrincipalId": "actor"}]
    if failure == "semantic":
        outcome["auditStatus"] = "not-observed"
    elif failure == "diagnostic":
        records = []
    elif failure == "actor":
        records[0]["AadPrincipalId"] = "other"
    elif failure == "activity":
        records[0]["ActivityId"] = "other"

    def query(command, deadline):
        if failure == "inaccessible":
            raise publisher.CatalogError("unavailable")
        statement = command[command.index("--analytics-query") + 1]
        for field in ("_ResourceId", "DatabaseName", "CollectionName", "RequestResourceId", "ActivityId", "AadPrincipalId", "OperationName", "StatusCode"):
            assert field in statement
        return records

    def observe(observation_args):
        assert observation_args.target_etag == outcome["etag"]
        assert observation_args.target_digest == outcome["catalogDigest"]
        return {"status": "not-converged" if failure == "adoption" else "converged"}

    loader = Mock()
    loader.return_value.load.return_value = SimpleNamespace(
        etag="other" if failure == "current" else "etag-a", digest=outcome["catalogDigest"],
    )
    monkeypatch.setattr(publisher, "_azure_json", query)
    monkeypatch.setattr(publisher, "_observe", observe)
    monkeypatch.setattr(publisher, "RuntimeCatalogLoader", loader)
    result = publisher._assure_operation(args, outcome, {"createdAt": "2026-09-08T00:00:00Z"}, object())
    assert (result["status"] == "complete") is (failure == "none")
    if failure == "current":
        assert result["status"] == "not-current"