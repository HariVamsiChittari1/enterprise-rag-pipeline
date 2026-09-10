"""Focused tests for the private profile-specific protected ranking generator."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from evaluation.generate_rankings import (
    GenerateRankingsError,
    generate_rankings,
    _canonical_json_bytes,
    _identity_ranking,
)
from retrieval.catalog import RuntimeCatalogSnapshot
from retrieval.cosmos import RetrievalLocatorKind, RetrievalMode, RetrievedChunk
from retrieval.cosmos_registry import CosmosRegistry
from retrieval.scoring import (
    FreshnessParameters,
    ScoringFunction,
    ScoringProfile,
)
from retrieval.service import RagService


SHA256 = "a" * 64


def _raw_candidate(chunk_id: str, source_modified_at: str | None) -> dict[str, Any]:
    return {
        "id": chunk_id,
        "documentId": f"doc-{chunk_id}",
        "sourceRunId": "run",
        "content": f"content {chunk_id}",
        "sourceName": f"{chunk_id}.pdf",
        "sourceUrl": f"https://sp.com/{chunk_id}.pdf",
        "pageStart": 1,
        "sourceModifiedAt": source_modified_at,
    }


def _registry(retriever) -> CosmosRegistry:
    return CosmosRegistry({"source": retriever})


def _retrieved_chunk(item: dict[str, Any]) -> RetrievedChunk:
    page_number = item["pageStart"]
    return RetrievedChunk(
        chunk_id=item["id"],
        document_id=item["documentId"],
        content=item["content"],
        source_name=item["sourceName"],
        source_url=item["sourceUrl"],
        locator_kind=RetrievalLocatorKind.PAGE,
        locator_label=f"Page {page_number}",
        locator_ordinal_start=page_number,
        locator_ordinal_end=page_number,
        source_modified_at=item.get("sourceModifiedAt"),
    )


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )


def _write_json(path: Path, record: dict[str, Any]) -> None:
    path.write_text(json.dumps(record), encoding="utf-8")


def _fabricated_ground_truth() -> list[dict[str, Any]]:
    return [
        {
            "queryId": "Q-001",
            "category": "answerable",
            "question": "A fabricated question about the policy.",
            "expectedContext": [
                {
                    "documentItemId": "doc-new",
                    "pageStart": 1,
                    "pageEnd": 1,
                    "exactText": "Fabricated.",
                }
            ],
            "expectedAnswer": "Fabricated answer.",
            "expectedCitations": ["doc-new#page=1"],
            "principalCaseLabel": "authorized-reader",
            "smeNotes": "Fabricated.",
        },
    ]


def _fabricated_principal_cases() -> dict[str, Any]:
    return {
        "version": "principals-v1",
        "cases": [
            {
                "label": "authorized-reader",
                "userObjectId": "fabricated-user",
                "groupIds": ["group-a"],
                "expectedAllowDocumentIds": ["doc-new", "doc-old"],
                "expectedDenyDocumentIds": ["doc-denied"],
            }
        ],
    }


def _fabricated_approval() -> dict[str, Any]:
    return {
        "datasetHash": SHA256,
        "approverIdentity": "fabricated-sme",
        "approvedAt": "2026-08-05T12:00:00Z",
        "status": "approved",
        "reviewNotes": "Fabricated approval.",
        "expiresAt": "2027-08-05T12:00:00Z",
    }


def _fabricated_dataset_manifest() -> dict[str, Any]:
    return {
        "datasetVersion": "dataset-v1",
        "domain": "fabricated policy corpus",
        "useCase": "Validate.",
        "source": {
            "sourceId": "fabricated-source",
            "driveId": "fabricated-drive",
            "libraryFingerprint": SHA256,
        },
        "classifications": ["internal"],
        "contentVariants": ["headings"],
        "documents": [
            {
                "itemId": "doc-new",
                "eTag": "e1",
                "contentHash": SHA256,
                "classification": "internal",
                "format": "pdf",
                "structures": ["headings"],
                "securityClass": "group-restricted",
            },
            {
                "itemId": "doc-old", "eTag": "e2", "contentHash": SHA256,
                "classification": "internal", "format": "pdf",
                "structures": ["headings"], "securityClass": "group-restricted",
            },
            {
                "itemId": "doc-denied", "eTag": "e3", "contentHash": SHA256,
                "classification": "internal", "format": "pdf",
                "structures": ["headings"], "securityClass": "group-restricted",
            },
        ],
        "coverage": {
            "documentCount": 3,
            "queryTargetCount": 100,
            "classificationCounts": {"internal": 3},
            "variantCounts": {"headings": 3},
        },
        "createdAt": "2026-08-05T12:00:00Z",
    }


def _manifest_extras() -> dict[str, Any]:
    return {
        "sourceId": "fabricated-source",
        "codeCommit": "0" * 64,
        "sourceTreeHash": SHA256,
        "submittedContextHash": SHA256,
        "imageDigest": "sha256:" + SHA256,
        "baseImageDigest": "sha256:" + SHA256,
        "dependencyLockHash": SHA256,
        "acrBuildId": "fabricated-build-id",
        "profiles": {
            name: {"version": f"{name}-v1", "parameters": {}}
            for name in (
                "extraction", "chunking", "cleaning", "enrichment",
                "embedding", "retrieval", "prompt",
            )
        },
        "modelDeployments": {
            "embedding": "text-embedding-3-large",
            "chat": "gpt-4o",
            "evaluator": "gpt-4o",
        },
        "environment": "evaluation",
        "evaluatorVersions": {"generate_rankings": "1.0.0"},
        "pythonVersion": "3.12.7",
        "repeatIndex": 1,
        "seed": 0,
    }


def _provider(profiles: dict[str, ScoringProfile]) -> SimpleNamespace:
    return SimpleNamespace(snapshot=RuntimeCatalogSnapshot(
        deployment_instance_id="test", catalog_id="runtime-catalog", etag="etag-a",
        digest="sha256:" + "a" * 64, operation_id="operation-a",
        changed_at="2026-09-08T00:00:00Z", accepted_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
        over_fetch_factor=1, hybrid_weights=(1, 1), full_text_score_scope="Global",
        default_profile=None, profiles=profiles, synonym_maps={}, synonym_expanders={},
    ))


def _service_factory(profiles: dict[str, ScoringProfile], candidates: list[dict[str, Any]]):
    """Return a factory that constructs a RagService with mocked retriever + OpenAI."""

    def factory() -> RagService:
        client = Mock()
        client.embeddings.create.return_value = SimpleNamespace(
            data=[SimpleNamespace(embedding=[0.0] * 3072)]
        )
        retriever = Mock()
        retriever.retrieve.return_value = list(candidates)
        retriever.to_chunks.side_effect = lambda pool: [
            _retrieved_chunk(item) for item in pool
        ]
        return RagService(
            client, _registry(retriever), "embedding", "chat",
            catalog_provider=_provider(profiles),
        )

    return factory


def _valid_inputs(tmp_path: Path) -> dict[str, Path]:
    ground = tmp_path / "gt.jsonl"
    principals = tmp_path / "principals.json"
    approval = tmp_path / "approval.json"
    dataset = tmp_path / "dataset.json"
    _write_jsonl(ground, _fabricated_ground_truth())
    _write_json(principals, _fabricated_principal_cases())
    _write_json(dataset, _fabricated_dataset_manifest())
    identity_components = {
        "groundTruthHash": hashlib.sha256(ground.read_bytes()).hexdigest(),
        "principalCasesHash": hashlib.sha256(principals.read_bytes()).hexdigest(),
        "datasetManifestHash": hashlib.sha256(dataset.read_bytes()).hexdigest(),
    }
    approval_record = _fabricated_approval()
    approval_record["datasetHash"] = hashlib.sha256(
        _canonical_json_bytes(identity_components)
    ).hexdigest()
    _write_json(approval, approval_record)
    return {
        "ground_truth_path": ground,
        "principal_cases_path": principals,
        "approval_path": approval,
        "dataset_manifest_path": dataset,
    }


def _fresh_profile(name: str, boost: float, agg: str) -> ScoringProfile:
    return ScoringProfile(
        name=name,
        functions=(
            ScoringFunction(
                type="freshness", field_name="source_modified_at",
                boost=boost, interpolation="linear",
                freshness=FreshnessParameters(
                    boosting_duration_seconds=10 * 365 * 86400,
                ),
            ),
        ),
        function_aggregation=agg,
    )


def _default_kwargs(
    tmp_path: Path,
    profiles: dict[str, ScoringProfile] | None = None,
) -> dict[str, Any]:
    profiles = profiles or {
        "baseline": _fresh_profile("baseline", 10.0, "sum"),
        "candidate": _fresh_profile("candidate", 10.0, "maximum"),
    }
    candidates = [
        _raw_candidate("old", "2020-01-01T00:00:00Z"),
        _raw_candidate("new", "2024-01-01T00:00:00Z"),
    ]
    return {
        **_valid_inputs(tmp_path),
        "output_dir": tmp_path / "out",
        "baseline_profile_name": "baseline",
        "candidate_profile_name": "candidate",
        "principal_case_label": "authorized-reader",
        "evaluation_as_of": datetime(2025, 1, 1, tzinfo=timezone.utc),
        "service_factory": _service_factory(profiles, candidates),
        "manifest_extras": _manifest_extras(),
        "run_id": "run-001",
        "experiment_id": "experiment-001",
        "started_at": datetime(2026, 8, 27, tzinfo=timezone.utc),
    }


def test_generate_rankings_writes_identity_only_baseline_and_candidate(tmp_path: Path) -> None:
    kwargs = _default_kwargs(tmp_path)
    manifest_path = generate_rankings(**kwargs)

    baseline_path = kwargs["output_dir"] / "baseline.jsonl"
    candidate_path = kwargs["output_dir"] / "candidate.jsonl"
    baseline_lines = [json.loads(line) for line in baseline_path.read_text("utf-8").splitlines() if line]
    candidate_lines = [json.loads(line) for line in candidate_path.read_text("utf-8").splitlines() if line]
    assert baseline_lines == [{
        "queryId": "Q-001",
        "retrievedContext": [
            {"documentItemId": "doc-new", "pageNumber": 1},
            {"documentItemId": "doc-old", "pageNumber": 1},
        ],
    }]
    # Sum vs maximum on identical two-item freshness contributions yields the same
    # ranking here; the point is that both profiles are applied to the SAME pool.
    assert candidate_lines == [{
        "queryId": "Q-001",
        "retrievedContext": [
            {"documentItemId": "doc-new", "pageNumber": 1},
            {"documentItemId": "doc-old", "pageNumber": 1},
        ],
    }]
    # No content field ever leaks into the persisted outputs.
    assert "content" not in baseline_path.read_text("utf-8")
    assert "content" not in candidate_path.read_text("utf-8")

    manifest = json.loads(manifest_path.read_text("utf-8"))
    assert manifest["principalCase"] == "authorized-reader"
    assert manifest["retrievalMode"] == "hybrid"
    assert manifest["evaluationAsOf"].endswith("Z")
    # Deterministic canonical hashes match a fresh recomputation of the outputs.
    assert manifest["baselineRankingHash"] == hashlib.sha256(baseline_path.read_bytes()).hexdigest()
    assert manifest["candidateRankingHash"] == hashlib.sha256(candidate_path.read_bytes()).hexdigest()

def test_generic_ranking_rejects_non_page_locator() -> None:
    chunk = RetrievedChunk(
        chunk_id="chunk-1",
        document_id="doc-1",
        content="Fabricated.",
        source_name="forecast.xlsx",
        source_url="https://example.invalid/forecast.xlsx",
        locator_kind=RetrievalLocatorKind.WORKSHEET,
        locator_label="Worksheet: Forecast",
        locator_ordinal_start=1,
        locator_ordinal_end=1,
    )

    with pytest.raises(GenerateRankingsError, match="page locators only"):
        _identity_ranking([chunk])


def test_generate_rankings_uses_profile_specific_retrieval_per_query(tmp_path: Path) -> None:
    profiles = {
        "baseline": _fresh_profile("baseline", 10.0, "sum"),
        "candidate": _fresh_profile("candidate", 10.0, "maximum"),
    }
    candidates = [
        _raw_candidate("old", "2020-01-01T00:00:00Z"),
        _raw_candidate("new", "2024-01-01T00:00:00Z"),
    ]
    call_counter = {"retrieve": 0}
    provider = _provider(profiles)
    observed_weights = []

    def factory() -> RagService:
        client = Mock()
        client.embeddings.create.return_value = SimpleNamespace(
            data=[SimpleNamespace(embedding=[0.0] * 3072)]
        )
        retriever = Mock()

        def _tracked_retrieve(*args, **kwargs):
            call_counter["retrieve"] += 1
            observed_weights.append(kwargs["rrf_weights"])
            provider.snapshot = replace(
                provider.snapshot, etag="etag-b", operation_id="operation-b",
                digest="sha256:" + "b" * 64, hybrid_weights=(7, 3), profiles={},
            )
            return list(candidates)

        retriever.retrieve.side_effect = _tracked_retrieve
        retriever.to_chunks.side_effect = lambda pool: [
            _retrieved_chunk(item) for item in pool
        ]
        return RagService(
            client, _registry(retriever), "embedding", "chat",
            catalog_provider=provider,
        )

    kwargs = _default_kwargs(tmp_path, profiles=profiles)
    kwargs["service_factory"] = factory
    manifest_path = generate_rankings(**kwargs)
    assert call_counter["retrieve"] == 2
    assert observed_weights == [(1, 1), (1, 1)]
    manifest = json.loads(manifest_path.read_text("utf-8"))
    assert manifest["catalogEtag"] == "etag-a"
    assert manifest["catalogSha"] == "a" * 64
    assert provider.snapshot.etag == "etag-b"


def test_generate_rankings_rejects_deny_set_hit(tmp_path: Path) -> None:
    denied = _raw_candidate("denied", "2024-01-01T00:00:00Z")
    denied["documentId"] = "doc-denied"
    profiles = {
        "baseline": _fresh_profile("baseline", 10.0, "sum"),
        "candidate": _fresh_profile("candidate", 10.0, "maximum"),
    }
    kwargs = _default_kwargs(tmp_path, profiles=profiles)
    kwargs["service_factory"] = _service_factory(profiles, [denied])
    with pytest.raises(GenerateRankingsError, match="deny-set"):
        generate_rankings(**kwargs)
    # No partial output remains.
    assert not (kwargs["output_dir"] / "baseline.jsonl").exists()
    assert not (kwargs["output_dir"] / "candidate.jsonl").exists()
    assert not (kwargs["output_dir"] / "experiment-manifest.json").exists()


def test_generate_rankings_rejects_naive_evaluation_as_of(tmp_path: Path) -> None:
    kwargs = _default_kwargs(tmp_path)
    kwargs["evaluation_as_of"] = datetime(2025, 1, 1)
    with pytest.raises(GenerateRankingsError, match="timezone-aware"):
        generate_rankings(**kwargs)


def test_generate_rankings_rejects_equal_profile_names(tmp_path: Path) -> None:
    kwargs = _default_kwargs(tmp_path)
    kwargs["candidate_profile_name"] = "baseline"
    with pytest.raises(GenerateRankingsError, match="must differ"):
        generate_rankings(**kwargs)


def test_generate_rankings_rejects_unknown_principal_case(tmp_path: Path) -> None:
    kwargs = _default_kwargs(tmp_path)
    kwargs["principal_case_label"] = "unknown-case"
    with pytest.raises(GenerateRankingsError, match="principal case"):
        generate_rankings(**kwargs)


def test_generate_rankings_manifest_is_deterministic_between_runs(tmp_path: Path) -> None:
    kwargs_a = _default_kwargs(tmp_path)
    manifest_a = generate_rankings(**kwargs_a)
    payload_a = manifest_a.read_bytes()

    # Second run into a fresh output directory using identical inputs.
    kwargs_b = _default_kwargs(tmp_path)
    kwargs_b["output_dir"] = tmp_path / "out2"
    manifest_b = generate_rankings(**kwargs_b)
    payload_b = manifest_b.read_bytes()
    assert payload_a == payload_b


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"status": "rejected"}, "status is not approved"),
        ({"expiresAt": "2026-08-26T00:00:00Z"}, "approval is expired"),
        ({"approvedAt": "2026-08-28T00:00:00Z"}, "not yet valid"),
        ({"datasetHash": "b" * 64}, "hash does not match"),
    ],
)
def test_generate_rankings_rejects_invalid_approval(
    tmp_path: Path,
    changes: dict[str, Any],
    message: str,
) -> None:
    kwargs = _default_kwargs(tmp_path)
    approval = json.loads(kwargs["approval_path"].read_text("utf-8"))
    approval.update(changes)
    _write_json(kwargs["approval_path"], approval)

    with pytest.raises(GenerateRankingsError, match=message):
        generate_rankings(**kwargs)


def test_dataset_identity_excludes_approval_bytes(tmp_path: Path) -> None:
    kwargs = _default_kwargs(tmp_path)
    first_manifest = generate_rankings(**kwargs)
    first_hash = json.loads(first_manifest.read_text("utf-8"))["datasetHash"]
    approval = json.loads(kwargs["approval_path"].read_text("utf-8"))
    approval["reviewNotes"] = "A different approved note."
    _write_json(kwargs["approval_path"], approval)

    second_manifest = generate_rankings(**kwargs)
    second_hash = json.loads(second_manifest.read_text("utf-8"))["datasetHash"]

    assert second_hash == first_hash


def test_generate_rankings_rejects_principal_label_mismatch(tmp_path: Path) -> None:
    kwargs = _default_kwargs(tmp_path)
    ground_truth = _fabricated_ground_truth()
    ground_truth[0]["principalCaseLabel"] = "different-reader"
    _write_jsonl(kwargs["ground_truth_path"], ground_truth)

    with pytest.raises(GenerateRankingsError, match="principal case label"):
        generate_rankings(**kwargs)


def test_generate_rankings_rejects_unknown_raw_pool_document(tmp_path: Path) -> None:
    profiles = {
        "baseline": _fresh_profile("baseline", 10.0, "sum"),
        "candidate": _fresh_profile("candidate", 10.0, "maximum"),
    }
    unknown = _raw_candidate("unknown", "2024-01-01T00:00:00Z")
    kwargs = _default_kwargs(tmp_path, profiles=profiles)
    kwargs["service_factory"] = _service_factory(profiles, [unknown])

    with pytest.raises(GenerateRankingsError, match="unknown document"):
        generate_rankings(**kwargs)


@pytest.mark.parametrize("field", ["datasetHash", "catalogSha", "catalogEtag"])
def test_generate_rankings_rejects_computed_manifest_collision(tmp_path: Path, field: str) -> None:
    kwargs = _default_kwargs(tmp_path)
    kwargs["manifest_extras"] = {
        **kwargs["manifest_extras"],
        field: "b" * 64,
    }

    with pytest.raises(GenerateRankingsError, match="cannot overwrite computed fields"):
        generate_rankings(**kwargs)


def test_generate_rankings_validates_generated_ranking_schema(tmp_path: Path) -> None:
    profiles = {
        "baseline": _fresh_profile("baseline", 10.0, "sum"),
        "candidate": _fresh_profile("candidate", 10.0, "maximum"),
    }
    invalid = _raw_candidate("new", "2024-01-01T00:00:00Z")
    invalid["pageStart"] = 0
    kwargs = _default_kwargs(tmp_path, profiles=profiles)
    kwargs["service_factory"] = _service_factory(profiles, [invalid])

    with pytest.raises(GenerateRankingsError, match="generated ranking failed schema"):
        generate_rankings(**kwargs)


def test_canonical_json_bytes_rejects_nan_and_infinity() -> None:
    with pytest.raises(ValueError):
        _canonical_json_bytes({"x": float("nan")})
    with pytest.raises(ValueError):
        _canonical_json_bytes({"x": float("inf")})


def test_deployed_retrieval_modules_do_not_import_evaluation(tmp_path: Path) -> None:
    """REQ-12: evaluation may import retrieval, but never the reverse."""
    forbidden = "from evaluation"
    forbidden_alt = "import evaluation"
    app_root = Path(__file__).resolve().parents[2] / "app" / "retrieval"
    for module in app_root.rglob("*.py"):
        text = module.read_text("utf-8")
        assert forbidden not in text, f"{module} imports evaluation"
        assert forbidden_alt not in text, f"{module} imports evaluation"
