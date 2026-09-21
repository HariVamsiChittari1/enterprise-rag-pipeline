"""Deterministic protected ranking generator.

Loads approved ground truth, principal cases, approval, and dataset manifest;
fetches each profile's authorized candidate pool through the shared
``RagService`` evaluation seam; applies baseline and candidate scoring profiles
under a caller-supplied timezone-aware ``evaluationAsOf``; rejects deny-set
hits; canonical-hashes the full in-memory candidate scoring input without
persisting content; and atomically emits identity-only ranking JSONL files
alongside an experiment manifest bound to every identity.

The deployed retrieval application must never import from this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

_APP_ROOT = Path(__file__).resolve().parents[1] / "app"
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from jsonschema import Draft202012Validator, FormatChecker  # noqa: E402

from retrieval.auth import Principal  # noqa: E402
from retrieval.cosmos import RetrievalMode  # noqa: E402
from retrieval.service import EvaluationPool, RagService  # noqa: E402


SCHEMA_ROOT = Path(__file__).resolve().parent / "schemas"


class GenerateRankingsError(RuntimeError):
    """Raised for any local validation, deny-set, or output failure."""


@dataclass(frozen=True)
class _LoadedDataset:
    ground_truth: list[dict[str, Any]]
    principal_case: dict[str, Any]
    approval: dict[str, Any]
    dataset_manifest: dict[str, Any]
    approval_bytes: bytes
    dataset_manifest_bytes: bytes
    ground_truth_bytes: bytes
    principal_cases_bytes: bytes


def _canonical_json_bytes(value: Any) -> bytes:
    """UTF-8, sorted-keys, compact JSON that rejects NaN and Infinity."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_validator(schema_name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMA_ROOT / schema_name).read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as error:
        raise GenerateRankingsError(f"cannot read {path}: {error}") from None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1,
    ):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            raise GenerateRankingsError(
                f"{path}: line {line_number} is not valid JSON"
            ) from None
        if not isinstance(value, dict):
            raise GenerateRankingsError(
                f"{path}: line {line_number} must be a JSON object"
            )
        records.append(value)
    return records


def _load_and_validate(
    ground_truth_path: Path,
    principal_cases_path: Path,
    approval_path: Path,
    dataset_manifest_path: Path,
    principal_case_label: str,
) -> _LoadedDataset:
    ground_truth_bytes = _read_bytes(ground_truth_path)
    principal_cases_bytes = _read_bytes(principal_cases_path)
    approval_bytes = _read_bytes(approval_path)
    dataset_manifest_bytes = _read_bytes(dataset_manifest_path)

    ground_truth_records = _read_jsonl(ground_truth_path)
    if not ground_truth_records:
        raise GenerateRankingsError("ground truth is empty")

    ground_truth_validator = _load_validator("ground-truth-record.schema.json")
    seen_ids: set[str] = set()
    for record in ground_truth_records:
        ground_truth_validator.validate(record)
        query_id = record["queryId"]
        if query_id in seen_ids:
            raise GenerateRankingsError(f"duplicate queryId '{query_id}' in ground truth")
        seen_ids.add(query_id)

    principals = json.loads(principal_cases_bytes.decode("utf-8"))
    _load_validator("principal-cases.schema.json").validate(principals)
    cases = principals.get("cases") or []
    labels = [candidate.get("label") for candidate in cases]
    if len(labels) != len(set(labels)):
        raise GenerateRankingsError("principal case labels must be unique")
    case = next((c for c in cases if c.get("label") == principal_case_label), None)
    if case is None:
        raise GenerateRankingsError(
            f"principal case '{principal_case_label}' is not in the approved list"
        )
    for record in ground_truth_records:
        if record.get("principalCaseLabel") != principal_case_label:
            raise GenerateRankingsError(
                f"query '{record['queryId']}' principal case label does not match"
            )
    allow = set(case.get("expectedAllowDocumentIds") or [])
    deny = set(case.get("expectedDenyDocumentIds") or [])
    if allow & deny:
        raise GenerateRankingsError(
            f"principal case '{principal_case_label}' has overlapping allow/deny sets"
        )

    approval = json.loads(approval_bytes.decode("utf-8"))
    dataset_manifest = json.loads(dataset_manifest_bytes.decode("utf-8"))
    _load_validator("approval.schema.json").validate(approval)
    _load_validator("dataset-manifest.schema.json").validate(dataset_manifest)

    corpus_ids = [document["itemId"] for document in dataset_manifest["documents"]]
    if len(corpus_ids) != len(set(corpus_ids)):
        raise GenerateRankingsError("dataset manifest document itemIds must be unique")
    corpus = set(corpus_ids)
    if not allow <= corpus or not deny <= corpus:
        raise GenerateRankingsError("principal allow/deny ids must belong to the dataset corpus")
    for record in ground_truth_records:
        judged_ids = {
            context["documentItemId"] for context in record.get("expectedContext", [])
        }
        if not judged_ids <= corpus:
            raise GenerateRankingsError(
                f"query '{record['queryId']}' judgments include documents outside the corpus"
            )
        if not judged_ids <= allow:
            raise GenerateRankingsError(
                f"query '{record['queryId']}' judgments include documents outside the principal allow set"
            )

    return _LoadedDataset(
        ground_truth=ground_truth_records,
        principal_case=case,
        approval=approval,
        dataset_manifest=dataset_manifest,
        approval_bytes=approval_bytes,
        dataset_manifest_bytes=dataset_manifest_bytes,
        ground_truth_bytes=ground_truth_bytes,
        principal_cases_bytes=principal_cases_bytes,
    )


def _canonical_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    # Drop any transient scoring output the reranker may attach; we hash the
    # candidate's raw scoring inputs as they entered the reranker.
    return {k: v for k, v in candidate.items() if k != "_scoring_profile_score"}


def _sorted_hex(items: list[str]) -> list[str]:
    return sorted(items)


def _identity_ranking(chunks: list[Any]) -> list[dict[str, Any]]:
    ranking: list[dict[str, Any]] = []
    for chunk in chunks:
        locator_kind = getattr(chunk.locator_kind, "value", chunk.locator_kind)
        if locator_kind != "page":
            raise GenerateRankingsError(
                "the generic ranking benchmark accepts page locators only"
            )
        if chunk.locator_ordinal_start != chunk.locator_ordinal_end:
            raise GenerateRankingsError(
                "the generic ranking benchmark requires a single-page locator"
            )
        ranking.append(
            {
                "documentItemId": chunk.document_id,
                "pageNumber": chunk.locator_ordinal_start,
            }
        )
    return ranking


def _validate_deny_set(
    query_id: str,
    label: str,
    ranking: list[dict[str, Any]],
    deny_document_ids: set[str],
) -> None:
    for entry in ranking:
        if entry["documentItemId"] in deny_document_ids:
            raise GenerateRankingsError(
                f"query '{query_id}' {label} ranking contains deny-set document "
                f"'{entry['documentItemId']}'"
            )


def _dataset_identity(dataset: _LoadedDataset) -> tuple[str, dict[str, str]]:
    identity_components = {
        "groundTruthHash": _sha256_hex(dataset.ground_truth_bytes),
        "principalCasesHash": _sha256_hex(dataset.principal_cases_bytes),
        "datasetManifestHash": _sha256_hex(dataset.dataset_manifest_bytes),
    }
    return (
        _sha256_hex(_canonical_json_bytes(identity_components)),
        identity_components,
    )


def _parse_aware_timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise GenerateRankingsError(f"{label} must be a valid date-time") from error
    if parsed.tzinfo is None:
        raise GenerateRankingsError(f"{label} must be timezone-aware")
    return parsed


def _validate_approval(dataset: _LoadedDataset, *, as_of: datetime) -> tuple[str, dict[str, str]]:
    dataset_hash, identity_components = _dataset_identity(dataset)
    approval = dataset.approval
    if approval["status"] != "approved":
        raise GenerateRankingsError("dataset approval status is not approved")
    approved_at = _parse_aware_timestamp(approval["approvedAt"], "approvedAt")
    expires_at = _parse_aware_timestamp(approval["expiresAt"], "expiresAt")
    if approved_at > as_of:
        raise GenerateRankingsError("dataset approval is not yet valid")
    if expires_at <= as_of or expires_at <= approved_at:
        raise GenerateRankingsError("dataset approval is expired")
    if approval["datasetHash"] != dataset_hash:
        raise GenerateRankingsError("dataset approval hash does not match dataset identity")
    return dataset_hash, identity_components


def _validate_raw_pool(
    query_id: str,
    pool: EvaluationPool,
    *,
    corpus_ids: set[str],
    allow_ids: set[str],
    deny_ids: set[str],
) -> None:
    for candidate in pool.pool:
        document_id = candidate.get("documentId")
        if not isinstance(document_id, str) or document_id not in corpus_ids:
            raise GenerateRankingsError(
                f"query '{query_id}' raw pool contains an unknown document"
            )
        if document_id in deny_ids:
            raise GenerateRankingsError(
                f"query '{query_id}' raw pool contains a deny-set document"
            )
        if document_id not in allow_ids:
            raise GenerateRankingsError(
                f"query '{query_id}' raw pool contains a non-allowed document"
            )


def _atomic_write_bytes(target: Path, payload: bytes) -> Path:
    tmp = target.with_suffix(target.suffix + ".tmp")
    with open(tmp, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, target)
    return target


def _rankings_jsonl_bytes(rankings: dict[str, list[dict[str, Any]]]) -> bytes:
    lines = [
        _canonical_json_bytes({"queryId": qid, "retrievedContext": items})
        for qid, items in rankings.items()
    ]
    return b"\n".join(lines) + b"\n"


def generate_rankings(
    *,
    ground_truth_path: Path,
    principal_cases_path: Path,
    approval_path: Path,
    dataset_manifest_path: Path,
    output_dir: Path,
    baseline_profile_name: str,
    candidate_profile_name: str,
    principal_case_label: str,
    evaluation_as_of: datetime,
    service_factory: Callable[[], RagService],
    manifest_extras: dict[str, Any],
    retrieval_mode: RetrievalMode = RetrievalMode.HYBRID,
    top_k: int = 5,
    run_id: str,
    experiment_id: str,
    started_at: datetime,
) -> Path:
    if evaluation_as_of.tzinfo is None:
        raise GenerateRankingsError("evaluation_as_of must be timezone-aware")
    if started_at.tzinfo is None:
        raise GenerateRankingsError("started_at must be timezone-aware")
    if baseline_profile_name == candidate_profile_name:
        raise GenerateRankingsError(
            "baseline and candidate profile names must differ"
        )
    computed_manifest_fields = {
        "experimentId", "runId", "datasetHash", "datasetComponents",
        "principalCase", "retrievalMode", "k", "evaluationAsOf",
        "candidateSetHash", "baselineRankingHash", "candidateRankingHash",
        "startedAt", "catalogSha", "catalogEtag",
    }
    collisions = computed_manifest_fields & set(manifest_extras)
    if collisions:
        raise GenerateRankingsError(
            "manifest_extras cannot overwrite computed fields: "
            + ", ".join(sorted(collisions))
        )

    dataset = _load_and_validate(
        ground_truth_path=ground_truth_path,
        principal_cases_path=principal_cases_path,
        approval_path=approval_path,
        dataset_manifest_path=dataset_manifest_path,
        principal_case_label=principal_case_label,
    )
    dataset_hash, identity_components = _validate_approval(
        dataset, as_of=started_at,
    )
    deny_ids = set(dataset.principal_case.get("expectedDenyDocumentIds") or [])
    allow_ids = set(dataset.principal_case.get("expectedAllowDocumentIds") or [])
    corpus_ids = {document["itemId"] for document in dataset.dataset_manifest["documents"]}
    case_user = dataset.principal_case["userObjectId"]
    case_groups = frozenset(dataset.principal_case.get("groupIds") or [])
    principal = Principal(case_user, "protected", case_groups)

    output_dir.mkdir(parents=True, exist_ok=True)
    baseline_path = output_dir / "baseline.jsonl"
    candidate_path = output_dir / "candidate.jsonl"
    manifest_path = output_dir / "experiment-manifest.json"

    service = service_factory()
    try:
        baseline_rankings: dict[str, list[dict[str, Any]]] = {}
        candidate_rankings: dict[str, list[dict[str, Any]]] = {}
        pool_signatures: list[dict[str, Any]] = []

        try:
            snapshot = service.capture_snapshot()
            baseline_policy = service.capture_policy(baseline_profile_name, snapshot=snapshot)
            candidate_policy = service.capture_policy(candidate_profile_name, snapshot=snapshot)
        except Exception as error:
            raise GenerateRankingsError("evaluation profile is not in the catalog") from error

        for record in dataset.ground_truth:
            query_id = record["queryId"]
            question = record["question"]

            pool: EvaluationPool = service.retrieve_evaluation_pool(
                question,
                [question],
                principal,
                mode=retrieval_mode,
                top_k=top_k,
                scoring_profile=baseline_profile_name,
                policy=baseline_policy,
            )
            candidate_pool: EvaluationPool = service.retrieve_evaluation_pool(
                question,
                [question],
                principal,
                mode=retrieval_mode,
                top_k=top_k,
                scoring_profile=candidate_profile_name,
                policy=candidate_policy,
            )
            _validate_raw_pool(
                query_id, pool,
                corpus_ids=corpus_ids, allow_ids=allow_ids, deny_ids=deny_ids,
            )
            _validate_raw_pool(
                query_id, candidate_pool,
                corpus_ids=corpus_ids, allow_ids=allow_ids, deny_ids=deny_ids,
            )
            baseline_chunks = pool.rerank(evaluation_as_of)
            candidate_chunks = candidate_pool.rerank(evaluation_as_of)

            baseline_entry = _identity_ranking(baseline_chunks)
            candidate_entry = _identity_ranking(candidate_chunks)
            _validate_deny_set(query_id, "baseline", baseline_entry, deny_ids)
            _validate_deny_set(query_id, "candidate", candidate_entry, deny_ids)
            baseline_rankings[query_id] = baseline_entry
            candidate_rankings[query_id] = candidate_entry

            pool_signatures.append({
                "queryId": query_id,
                "baselinePool": [_canonical_candidate(item) for item in pool.pool],
                "candidatePool": [
                    _canonical_candidate(item) for item in candidate_pool.pool
                ],
            })

        candidate_set_hash = _sha256_hex(_canonical_json_bytes(pool_signatures))
        baseline_bytes = _rankings_jsonl_bytes(baseline_rankings)
        candidate_bytes = _rankings_jsonl_bytes(candidate_rankings)
        baseline_hash = _sha256_hex(baseline_bytes)
        candidate_hash = _sha256_hex(candidate_bytes)

        dataset_components = {
            **identity_components,
            "approvalHash": _sha256_hex(dataset.approval_bytes),
        }
        manifest = {
            "experimentId": experiment_id,
            "runId": run_id,
            "datasetHash": dataset_hash,
            "datasetComponents": dataset_components,
            "catalogSha": snapshot.digest.removeprefix("sha256:"),
            "catalogEtag": snapshot.etag,
            "principalCase": principal_case_label,
            "retrievalMode": retrieval_mode.value,
            "k": top_k,
            "evaluationAsOf": evaluation_as_of.isoformat().replace("+00:00", "Z"),
            "candidateSetHash": candidate_set_hash,
            "baselineRankingHash": baseline_hash,
            "candidateRankingHash": candidate_hash,
            "startedAt": started_at.isoformat().replace("+00:00", "Z"),
        }
        manifest = {**manifest_extras, **manifest}
        ranking_validator = _load_validator("ranking-record.schema.json")
        try:
            for query_id, ranking in baseline_rankings.items():
                ranking_validator.validate({
                    "queryId": query_id, "retrievedContext": ranking,
                })
            for query_id, ranking in candidate_rankings.items():
                ranking_validator.validate({
                    "queryId": query_id, "retrievedContext": ranking,
                })
        except Exception as error:
            raise GenerateRankingsError(
                f"generated ranking failed schema validation: {error}"
            ) from None
        manifest_validator = _load_validator("experiment-manifest.schema.json")
        try:
            manifest_validator.validate(manifest)
        except Exception as error:
            raise GenerateRankingsError(
                f"generated manifest failed schema validation: {error}"
            ) from None

        _atomic_write_bytes(baseline_path, baseline_bytes)
        _atomic_write_bytes(candidate_path, candidate_bytes)
        _atomic_write_bytes(
            manifest_path,
            _canonical_json_bytes(manifest) + b"\n",
        )
        return manifest_path
    except BaseException:
        # Remove any partial or temporary output so a failed run leaves nothing behind.
        for path in (baseline_path, candidate_path, manifest_path):
            tmp = path.with_suffix(path.suffix + ".tmp")
            for candidate in (tmp, path):
                if candidate.exists():
                    try:
                        candidate.unlink()
                    except OSError:
                        pass
        raise
    finally:
        service.close()
