"""Validate and operate the singleton catalog using a stable reviewed manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "app"))

from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential

from retrieval.catalog import (
    CATALOG_ID,
    CatalogError,
    CatalogConflictError,
    RuntimeCatalogLoader,
    apply_catalog_operation,
    build_catalog_item,
    build_operation_candidate,
    load_catalog_history,
    load_catalog_item,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "prepare", "inspect", "observe", "update", "reconcile", "rollback"))
    parser.add_argument("--file", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--deployment-instance-id", required=True)
    parser.add_argument("--endpoint")
    parser.add_argument("--database")
    parser.add_argument("--container", default="retrieval-config")
    parser.add_argument("--expected-etag")
    parser.add_argument("--reason")
    parser.add_argument("--history-id")
    parser.add_argument("--target-etag")
    parser.add_argument("--target-digest")
    parser.add_argument("--subscription")
    parser.add_argument("--resource-group")
    parser.add_argument("--application")
    parser.add_argument("--workspace")
    parser.add_argument("--actor-principal-id")
    parser.add_argument("--cosmos-account-resource-id")
    parser.add_argument("--observation-timeout", type=int, default=300)
    return parser


def _read_source(path: Path | None) -> tuple[dict, str]:
    if path is None:
        raise CatalogError("--file is required")
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, ValueError) as error:
        raise CatalogError("catalog source file cannot be read") from error
    if not isinstance(value, dict):
        raise CatalogError("catalog source must be a JSON object")
    return value, "sha256:" + hashlib.sha256(raw).hexdigest()


@contextmanager
def _container(args: argparse.Namespace):
    if not args.endpoint or not args.database:
        raise CatalogError("--endpoint and --database are required")
    with DefaultAzureCredential() as credential, CosmosClient(args.endpoint, credential=credential) as client:
        yield client.get_database_client(args.database).get_container_client(args.container)


def _prepare_operation(path: Path, deployment_instance_id: str, expected_etag: str, reason: str) -> dict:
    if not isinstance(expected_etag, str) or not expected_etag.strip():
        raise CatalogError("--expected-etag is required")
    source, file_digest = _read_source(path)
    item = build_catalog_item(
        source, deployment_instance_id, operation_id=str(uuid4()),
        changed_at=datetime.now(timezone.utc), reason=reason,
    )
    snapshot = load_catalog_item(item)
    return {
        "operationId": snapshot.operation_id,
        "createdAt": snapshot.changed_at,
        "deploymentInstanceId": snapshot.deployment_instance_id,
        "expectedEtag": expected_etag,
        "candidateDigest": snapshot.digest,
        "candidateFileDigest": file_digest,
        "reason": reason,
    }


def _persist_manifest(path: Path | None, manifest: dict) -> None:
    if path is None:
        raise CatalogError("--manifest is required")
    resolved = path.resolve()
    if resolved.is_relative_to(PROJECT_ROOT):
        ignored = subprocess.run(
            ["git", "check-ignore", "--quiet", str(resolved)], cwd=PROJECT_ROOT,
            capture_output=True, check=False,
        )
        if ignored.returncode != 0:
            raise CatalogError("operation manifest must be outside source control")
    try:
        with resolved.open("x", encoding="utf-8") as handle:
            json.dump(manifest, handle, sort_keys=True, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        raise CatalogError("operation manifest cannot be created; never overwrite or regenerate it") from None


def _summary(snapshot) -> dict:
    return {
        "catalogId": CATALOG_ID, "catalogDigest": snapshot.digest,
        "etag": snapshot.etag, "operationId": snapshot.operation_id,
        "profileCount": len(snapshot.profiles), "mapCount": len(snapshot.synonym_maps),
    }


def _utc_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    if timestamp.utcoffset() is None:
        raise ValueError("UTC timestamp required")
    return timestamp.astimezone(timezone.utc)


def _azure_json(arguments: list[str], deadline: float):
    executable = shutil.which("az")
    remaining = deadline - time.monotonic()
    if executable is None or remaining <= 0:
        raise CatalogError("Azure CLI unavailable or observation deadline exceeded")
    try:
        result = subprocess.run(
            [executable, *arguments, "--output", "json", "--only-show-errors"],
            capture_output=True, text=True, check=False, timeout=min(30, remaining),
            env={**os.environ, "AZURE_EXTENSION_USE_DYNAMIC_INSTALL": "no"},
        )
        if result.returncode != 0:
            raise CatalogError("Azure observation query failed; no convergence verified")
        return json.loads(result.stdout)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise CatalogError("Azure observation response unavailable or invalid") from None


def _replica_inventory(args: argparse.Namespace, deadline: float) -> dict:
    started = datetime.now(timezone.utc).isoformat()
    target = ["--subscription", args.subscription, "--resource-group", args.resource_group, "--name", args.application]
    revisions = _azure_json(["containerapp", "revision", "list", *target], deadline)
    members = []
    for revision in revisions:
        if not revision["properties"]["active"]:
            continue
        replicas = _azure_json([
            "containerapp", "replica", "list", *target, "--revision", revision["name"],
        ], deadline)
        if not isinstance(replicas, list) or not replicas:
            raise CatalogError("Active revision replica inventory is incomplete")
        for replica in replicas:
            containers = [container for container in replica["properties"]["containers"] if container["name"] == "retrieval-agent"]
            if len(containers) != 1 or containers[0]["ready"] is not True:
                raise CatalogError("Serving replica inventory is incomplete or not ready")
            container = containers[0]
            members.append({
                "revision": revision["name"], "replica": replica["name"],
                "container": container["containerId"], "restart_count": container["restartCount"],
            })
    return {
        "startedAt": started, "observedAt": datetime.now(timezone.utc).isoformat(),
        "members": sorted(members, key=lambda member: (member["revision"], member["replica"], member["container"])),
    }


def _observation_records(args: argparse.Namespace, since: str, deadline: float) -> list[dict]:
    application = json.dumps(args.application)
    instance_hash = hashlib.sha256(args.deployment_instance_id.encode("utf-8")).hexdigest()
    query = (
        f"AppTraces | where TimeGenerated >= datetime({since}) "
        f"| where tostring(Properties.application) == {application} "
        f"and tostring(Properties.deployment_instance_hash) == '{instance_hash}' "
        "| where Message in ('catalog_observed', 'catalog_rejected', 'catalog_degraded') "
        "| project event=Message, revision=tostring(Properties.revision), replica=tostring(Properties.replica), "
        "process_incarnation=tostring(Properties.process_incarnation), etag_hash=tostring(Properties.etag_hash), "
        "digest=tostring(Properties.digest), accepted_at=tostring(Properties.accepted_at), observed_at=tostring(Properties.observed_at) "
        "| take 10001"
    )
    records = _azure_json([
        "monitor", "log-analytics", "query", "--subscription", args.subscription,
        "--workspace", args.workspace, "--analytics-query", query,
        "--timespan", since + "/" + datetime.now(timezone.utc).isoformat(),
    ], deadline)
    if not isinstance(records, list) or len(records) > 10000:
        raise CatalogError("Observation results are incomplete or exceed the evidence bound")
    return records


def _observe(args: argparse.Namespace) -> dict:
    if (not all((args.subscription, args.resource_group, args.application, args.workspace, args.target_etag))
            or re.fullmatch(r"sha256:[0-9a-f]{64}", args.target_digest or "") is None
            or not 60 <= args.observation_timeout <= 86_460):
        raise CatalogError("Observation needs explicit target, ETag/digest and a timeout from 60 through 86460 seconds")
    deadline = time.monotonic() + args.observation_timeout
    before = None
    for _attempt in range(args.observation_timeout // 30 + 1):
        if time.monotonic() >= deadline:
            break
        try:
            after = _replica_inventory(args, deadline)
            if before is not None and before["members"] == after["members"]:
                records = _observation_records(args, before["observedAt"], deadline)
                members = []
                changed_process = False
                for member in before["members"]:
                    processes = {
                        record["process_incarnation"] for record in records
                        if record["revision"] == member["revision"] and record["replica"] == member["replica"]
                        and _utc_timestamp(before["observedAt"]) <= _utc_timestamp(record["observed_at"]) <= _utc_timestamp(after["startedAt"])
                    }
                    changed_process |= len(processes) > 1
                    members.append({**member, "process_incarnation": next(iter(processes)) if len(processes) == 1 else ""})
                if _cohort_converged(
                    {**before, "members": members},
                    {**after, "observedAt": after["startedAt"], "members": members},
                    records, args.target_etag, args.target_digest,
                ) and time.monotonic() < deadline:
                    return {"status": "converged", "catalogDigest": args.target_digest,
                            "etagHash": hashlib.sha256(args.target_etag.encode("utf-8")).hexdigest(),
                            "memberCount": len(members), "observedAt": after["startedAt"]}
                if changed_process:
                    before = after
            else:
                before = after
        except (CatalogError, KeyError, TypeError, ValueError):
            before = None
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(30, remaining))
    return {"status": "not-converged", "reason": "required-evidence-unavailable-or-unstable"}


def _cohort_converged(before: dict, after: dict, records: list[dict], etag: str, digest: str) -> bool:
    try:
        started = _utc_timestamp(before["observedAt"])
        finished = _utc_timestamp(after["observedAt"])
        if (finished - started).total_seconds() < 30:
            return False
        members = before["members"]
        if not members or members != after["members"]:
            return False
        identities = set()
        for member in members:
            identity = tuple(member[name] for name in ("revision", "replica", "container", "process_incarnation"))
            if not all(isinstance(value, str) and value for value in identity):
                return False
            if type(member["restart_count"]) is not int or member["restart_count"] < 0:
                return False
            if identity[:3] in identities:
                return False
            identities.add(identity[:3])
            observations = [
                record for record in records
                if record["revision"] == member["revision"] and record["replica"] == member["replica"]
                and started <= _utc_timestamp(record["observed_at"]) <= finished
            ]
            if not observations or any(record["process_incarnation"] != identity[3] for record in observations):
                return False
            latest = max(observations, key=lambda record: _utc_timestamp(record["observed_at"]))
            if any(record != latest and _utc_timestamp(record["observed_at"]) == _utc_timestamp(latest["observed_at"])
                   for record in observations):
                return False
            if (latest["event"] != "catalog_observed" or latest["digest"] != digest
                    or latest["etag_hash"] != hashlib.sha256(etag.encode("utf-8")).hexdigest()
                    or _utc_timestamp(latest["accepted_at"]) > _utc_timestamp(latest["observed_at"])):
                return False
        return True
    except (KeyError, TypeError, ValueError):
        return False


def _assure_operation(args: argparse.Namespace, outcome: dict, manifest: dict, container) -> dict:
    if (outcome["status"] != "applied-but-unassured"
            or outcome["auditStatus"] != "semantic-outcome-verified"
            or not all((args.subscription, args.workspace, args.application, args.resource_group,
                        args.actor_principal_id, args.cosmos_account_resource_id))):
        return outcome
    outcome["diagnosticStatus"] = "not-observed"
    filters = {
        "_ResourceId": args.cosmos_account_resource_id, "DatabaseName": args.database,
        "CollectionName": args.container, "RequestResourceId": outcome["resourceId"],
        "ActivityId": outcome["activityId"], "AadPrincipalId": args.actor_principal_id,
    }
    query = "CDBDataPlaneRequests | where " + " and ".join(
        f"{field} {'=~' if field in ('_ResourceId', 'AadPrincipalId') else '=='} {json.dumps(value)}"
        for field, value in filters.items()
    ) + " | where OperationName == 'Replace' and StatusCode == 200 | project ActivityId, AadPrincipalId | take 2"
    try:
        records = _azure_json([
            "monitor", "log-analytics", "query", "--subscription", args.subscription,
            "--workspace", args.workspace, "--analytics-query", query,
            "--timespan", manifest["createdAt"] + "/" + datetime.now(timezone.utc).isoformat(),
        ], time.monotonic() + 30)
        if not isinstance(records, list) or not records or any(
            record["ActivityId"] != outcome["activityId"]
            or record["AadPrincipalId"].lower() != args.actor_principal_id.lower()
            for record in records
        ):
            return outcome
        outcome["diagnosticStatus"] = "verified"
        observation_args = argparse.Namespace(**{
            **vars(args), "target_etag": outcome["etag"], "target_digest": outcome["catalogDigest"],
        })
        observation = _observe(observation_args)
        outcome["adoptionStatus"] = observation["status"]
        if observation["status"] != "converged":
            return outcome
        current = RuntimeCatalogLoader(container, args.deployment_instance_id).load()
        if current.etag != outcome["etag"] or current.digest != outcome["catalogDigest"]:
            outcome["status"] = "not-current"
            return outcome
        outcome["status"] = "complete"
    except (CatalogError, KeyError, TypeError, ValueError):
        outcome["status"] = "applied-but-unassured"
    return outcome


def _load_candidate(manifest_path: Path | None, source_path: Path | None, deployment_instance_id: str):
    if manifest_path is None:
        raise CatalogError("--manifest is required; prepare an operation before mutation")
    manifest, _manifest_file_digest = _read_source(manifest_path)
    source, file_digest = _read_source(source_path)
    candidate = build_operation_candidate(manifest, source, file_digest)
    if candidate["deploymentInstanceId"] != deployment_instance_id:
        raise CatalogConflictError("manifest target does not match requested deployment instance")
    return manifest, candidate


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.action == "observe":
            observation = _observe(args)
            print(json.dumps(observation))
            return 0 if observation["status"] == "converged" else 3
        if args.action == "validate":
            source, _file_digest = _read_source(args.file)
            item = build_catalog_item(
                source, args.deployment_instance_id,
                operation_id="00000000-0000-4000-8000-000000000000",
                changed_at=datetime(2000, 1, 1, tzinfo=timezone.utc), reason="Offline validation",
            )
            snapshot = load_catalog_item(item)
            print(json.dumps({"catalogId": CATALOG_ID, "catalogDigest": snapshot.digest}))
            return 0
        if args.action == "prepare":
            manifest = _prepare_operation(
                args.file, args.deployment_instance_id, args.expected_etag, args.reason,
            )
            _persist_manifest(args.manifest, manifest)
            print(json.dumps({"operationId": manifest["operationId"], "catalogDigest": manifest["candidateDigest"]}))
            return 0
        if args.action != "inspect":
            manifest, candidate = _load_candidate(args.manifest, args.file, args.deployment_instance_id)
        with _container(args) as container:
            if args.action == "inspect":
                snapshot = RuntimeCatalogLoader(container, args.deployment_instance_id).load()
                print(json.dumps(_summary(snapshot)))
                return 0
            if args.action == "rollback":
                if not args.history_id:
                    raise CatalogError("--history-id is required for rollback")
                try:
                    history = container.read_item(item=args.history_id, partition_key=args.deployment_instance_id)
                except Exception:
                    raise CatalogError("rollback history cannot be read") from None
                preimage = load_catalog_history(history, args.deployment_instance_id)
                if history["id"] != args.history_id or preimage["config"] != candidate["config"]:
                    raise CatalogConflictError("rollback candidate does not match reviewed history")
            outcome = apply_catalog_operation(container, candidate, manifest, reconcile_only=args.action == "reconcile")
            outcome = _assure_operation(args, outcome, manifest, container)
            print(json.dumps(outcome))
            return 0 if outcome["status"] == "complete" else 3
    except CatalogError as error:
        print(str(error), file=sys.stderr)
        return 2
    except Exception:
        print("catalog operation could not be verified; preserve the manifest and inspect before retrying", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
