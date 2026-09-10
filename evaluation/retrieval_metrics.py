"""Compare baseline and candidate retrieval rankings against protected judgments.

The evaluator is release evidence, not a production dependency. Ground truth carries
inclusive page intervals; ranked results carry a single ``pageNumber`` per entry.
An item matches an interval when documentItemId matches and
``pageStart <= pageNumber <= pageEnd``. Every matching returned page contributes to
precision; each judged interval contributes at most once to recall; MRR uses the
first containment hit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Iterable


JudgedInterval = tuple[str, int, int]
RankingItem = tuple[str, int]


@dataclass(frozen=True)
class QueryMetrics:
    query_id: str
    precision_at_k: float
    recall_at_k: float
    reciprocal_rank: float


def _reject_bool_int(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")


def _judged_interval(value: dict[str, Any]) -> JudgedInterval:
    try:
        document_id = value["documentItemId"]
        page_start = value["pageStart"]
        page_end = value["pageEnd"]
    except KeyError as error:
        raise ValueError("judged context is missing an identity field") from error
    if not isinstance(document_id, str) or not document_id:
        raise ValueError("judged context documentItemId must be a nonempty string")
    _reject_bool_int(page_start, "judged pageStart")
    _reject_bool_int(page_end, "judged pageEnd")
    if page_start < 1 or page_end < page_start:
        raise ValueError("judged context page bounds are invalid")
    return document_id, page_start, page_end


def _ranking_item(value: dict[str, Any]) -> RankingItem:
    try:
        document_id = value["documentItemId"]
        page_number = value["pageNumber"]
    except KeyError as error:
        raise ValueError("ranking item is missing an identity field") from error
    if not isinstance(document_id, str) or not document_id:
        raise ValueError("ranking documentItemId must be a nonempty string")
    _reject_bool_int(page_number, "ranking pageNumber")
    if page_number < 1:
        raise ValueError("ranking pageNumber must be >= 1")
    return document_id, page_number


def _reject_overlapping_intervals(query_id: str, intervals: Iterable[JudgedInterval]) -> None:
    per_document: dict[str, list[tuple[int, int]]] = {}
    for doc_id, page_start, page_end in intervals:
        per_document.setdefault(doc_id, []).append((page_start, page_end))
    for doc_id, ranges in per_document.items():
        ordered = sorted(ranges)
        for (_start_a, end_a), (start_b, _end_b) in zip(ordered, ordered[1:]):
            if start_b <= end_a:
                raise ValueError(
                    f"query '{query_id}' has overlapping judged intervals for '{doc_id}'"
                )


def _matches_interval(item: RankingItem, interval: JudgedInterval) -> bool:
    doc_id, page_number = item
    judged_doc, page_start, page_end = interval
    return doc_id == judged_doc and page_start <= page_number <= page_end


def _assert_unit_metric(value: float, label: str) -> None:
    if not math.isfinite(value) or value < 0.0 or value > 1.0:
        raise ValueError(f"{label} must be in [0, 1]")


def evaluate_rankings(
    judgments: dict[str, list[Any]],
    rankings: dict[str, list[Any]],
    *,
    k: int,
    matcher: Callable[[Any, Any], bool] = _matches_interval,
) -> list[QueryMetrics]:
    if isinstance(k, bool) or not isinstance(k, int) or k < 1:
        raise ValueError("k must be a positive integer")
    judged_keys = set(judgments)
    ranked_keys = set(rankings)
    if judged_keys != ranked_keys:
        raise ValueError("judgments and rankings must cover exactly the same queryIds")
    metrics: list[QueryMetrics] = []
    for query_id in judgments:
        intervals = judgments[query_id]
        retrieved = rankings[query_id][:k]
        judged_hits: set[int] = set()
        first_hit_rank: int | None = None
        precision_hits = 0
        for rank, item in enumerate(retrieved, start=1):
            hit = False
            for index, interval in enumerate(intervals):
                if matcher(item, interval):
                    hit = True
                    judged_hits.add(index)
            if hit:
                precision_hits += 1
                if first_hit_rank is None:
                    first_hit_rank = rank
        precision = precision_hits / k
        recall = (len(judged_hits) / len(intervals)) if intervals else 0.0
        reciprocal_rank = 1.0 / first_hit_rank if first_hit_rank is not None else 0.0
        for label, value in (
            ("precision", precision), ("recall", recall), ("mrr", reciprocal_rank),
        ):
            _assert_unit_metric(value, label)
        metrics.append(QueryMetrics(query_id, precision, recall, reciprocal_rank))
    return metrics


def summarize(metrics: Iterable[QueryMetrics]) -> dict[str, float | int]:
    values = list(metrics)
    if not values:
        raise ValueError("at least one judged query is required")
    count = len(values)
    summary = {
        "queryCount": count,
        "precisionAtK": sum(item.precision_at_k for item in values) / count,
        "recallAtK": sum(item.recall_at_k for item in values) / count,
        "mrr": sum(item.reciprocal_rank for item in values) / count,
    }
    for label in ("precisionAtK", "recallAtK", "mrr"):
        _assert_unit_metric(summary[label], label)
    return summary


def compare(
    judgments: dict[str, list[Any]],
    baseline: dict[str, list[Any]],
    candidate: dict[str, list[Any]],
    *,
    k: int,
    matcher: Callable[[Any, Any], bool] = _matches_interval,
) -> dict[str, Any]:
    baseline_metrics = evaluate_rankings(judgments, baseline, k=k, matcher=matcher)
    candidate_metrics = evaluate_rankings(judgments, candidate, k=k, matcher=matcher)
    baseline_summary = summarize(baseline_metrics)
    candidate_summary = summarize(candidate_metrics)
    baseline_by_query = {item.query_id: item for item in baseline_metrics}
    regressions = []
    for item in candidate_metrics:
        previous = baseline_by_query[item.query_id]
        if (
            item.precision_at_k < previous.precision_at_k
            or item.recall_at_k < previous.recall_at_k
            or item.reciprocal_rank < previous.reciprocal_rank
        ):
            regressions.append({
                "queryId": item.query_id,
                "baseline": asdict(previous),
                "candidate": asdict(item),
            })
    return {
        "k": k,
        "baseline": baseline_summary,
        "candidate": candidate_summary,
        "delta": {
            "precisionAtK": candidate_summary["precisionAtK"] - baseline_summary["precisionAtK"],
            "recallAtK": candidate_summary["recallAtK"] - baseline_summary["recallAtK"],
            "mrr": candidate_summary["mrr"] - baseline_summary["mrr"],
        },
        "regressions": regressions,
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"{path}: line {line_number} is not valid JSON") from error
        if not isinstance(value, dict):
            raise ValueError(f"{path}: line {line_number} must be an object")
        records.append(value)
    return records


def _load_judgments(path: Path) -> dict[str, list[JudgedInterval]]:
    result: dict[str, list[JudgedInterval]] = {}
    for record in _read_jsonl(path):
        query_id = record.get("queryId")
        contexts = record.get("expectedContext")
        if not isinstance(query_id, str) or not query_id:
            raise ValueError("ground truth record queryId must be a nonempty string")
        if not isinstance(contexts, list):
            raise ValueError(f"ground truth record '{query_id}' expectedContext must be a list")
        if query_id in result:
            raise ValueError(f"duplicate queryId '{query_id}'")
        intervals: list[JudgedInterval] = []
        seen: set[JudgedInterval] = set()
        for value in contexts:
            if not isinstance(value, dict):
                raise ValueError(
                    f"ground truth record '{query_id}' expectedContext entry must be an object"
                )
            interval = _judged_interval(value)
            if interval in seen:
                raise ValueError(f"query '{query_id}' has duplicate judged intervals")
            seen.add(interval)
            intervals.append(interval)
        _reject_overlapping_intervals(query_id, intervals)
        result[query_id] = intervals
    return result


def _load_rankings(path: Path) -> dict[str, list[RankingItem]]:
    result: dict[str, list[RankingItem]] = {}
    for record in _read_jsonl(path):
        query_id = record.get("queryId")
        contexts = record.get("retrievedContext")
        if not isinstance(query_id, str) or not query_id:
            raise ValueError("ranking record queryId must be a nonempty string")
        if not isinstance(contexts, list):
            raise ValueError(f"ranking record '{query_id}' retrievedContext must be a list")
        if query_id in result:
            raise ValueError(f"duplicate queryId '{query_id}'")
        items: list[RankingItem] = []
        seen: set[RankingItem] = set()
        for value in contexts:
            if not isinstance(value, dict):
                raise ValueError(
                    f"ranking record '{query_id}' retrievedContext entry must be an object"
                )
            item = _ranking_item(value)
            if item in seen:
                raise ValueError(f"query '{query_id}' has duplicate ranked identities")
            seen.add(item)
            items.append(item)
        result[query_id] = items
    return result


def _finite_threshold(value: float, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    if not math.isfinite(float(value)):
        raise ValueError(f"{label} must be a finite number")
    return float(value)


def _verify_manifest(
    path: Path,
    *,
    ground_truth_path: Path,
    baseline_path: Path,
    candidate_path: Path,
    k: int,
) -> None:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("experiment manifest cannot be read") from error
    if not isinstance(manifest, dict):
        raise ValueError("experiment manifest must be an object")
    components = manifest.get("datasetComponents")
    if not isinstance(components, dict):
        raise ValueError("experiment manifest dataset components are missing")
    expected = {
        "ground truth": (
            components.get("groundTruthHash"), ground_truth_path
        ),
        "baseline ranking": (
            manifest.get("baselineRankingHash"), baseline_path
        ),
        "candidate ranking": (
            manifest.get("candidateRankingHash"), candidate_path
        ),
    }
    for label, (expected_hash, artifact_path) in expected.items():
        if not isinstance(expected_hash, str) or len(expected_hash) != 64:
            raise ValueError(f"experiment manifest {label} hash is invalid")
        actual_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        if actual_hash != expected_hash:
            raise ValueError(f"experiment manifest {label} hash does not match")
    if manifest.get("k") != k:
        raise ValueError("experiment manifest k does not match --k")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ground-truth", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--min-precision-delta", type=float, default=0.0)
    parser.add_argument("--min-recall-delta", type=float, default=0.0)
    parser.add_argument("--min-mrr-delta", type=float, default=0.0)
    parser.add_argument(
        "--regression-budget",
        type=int,
        default=0,
        help="Maximum tolerated per-query regressions (default 0).",
    )
    args = parser.parse_args(argv)
    try:
        if isinstance(args.k, bool) or args.k < 1:
            raise ValueError("--k must be a positive integer")
        if isinstance(args.regression_budget, bool) or args.regression_budget < 0:
            raise ValueError("--regression-budget must be a nonnegative integer")
        thresholds = {
            "precisionAtK": _finite_threshold(args.min_precision_delta, "--min-precision-delta"),
            "recallAtK": _finite_threshold(args.min_recall_delta, "--min-recall-delta"),
            "mrr": _finite_threshold(args.min_mrr_delta, "--min-mrr-delta"),
        }
        _verify_manifest(
            args.manifest,
            ground_truth_path=args.ground_truth,
            baseline_path=args.baseline,
            candidate_path=args.candidate,
            k=args.k,
        )
        report = compare(
            _load_judgments(args.ground_truth),
            _load_rankings(args.baseline),
            _load_rankings(args.candidate),
            k=args.k,
        )
    except (OSError, ValueError) as error:
        print(json.dumps({"error": str(error)}))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    threshold_failed = any(
        report["delta"][name] < minimum for name, minimum in thresholds.items()
    )
    regression_failed = len(report["regressions"]) > args.regression_budget
    return 1 if threshold_failed or regression_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
