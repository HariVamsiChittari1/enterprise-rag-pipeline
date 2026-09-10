from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError


PROJECT_ROOT = Path(__file__).parents[2]
SCHEMA_ROOT = PROJECT_ROOT / "evaluation" / "schemas"
SHA256 = "a" * 64


def load_validator(schema_name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMA_ROOT / schema_name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def test_dataset_manifest_accepts_complete_fabricated_record() -> None:
    record = {
        "datasetVersion": "dataset-v1",
        "domain": "fabricated policy corpus",
        "useCase": "Validate grounded answers without protected content.",
        "source": {
            "sourceId": "fabricated-source",
            "driveId": "fabricated-drive",
            "libraryFingerprint": SHA256,
        },
        "classifications": ["internal"],
        "contentVariants": ["headings", "tables"],
        "documents": [
            {
                "itemId": "fabricated-item",
                "eTag": "fabricated-etag",
                "contentHash": SHA256,
                "classification": "internal",
                "format": "pdf",
                "structures": ["headings", "tables"],
                "securityClass": "group-restricted",
            }
        ],
        "coverage": {
            "documentCount": 1,
            "queryTargetCount": 100,
            "classificationCounts": {"internal": 1},
            "variantCounts": {"headings": 1, "tables": 1},
        },
        "createdAt": "2026-08-05T12:00:00Z",
    }

    load_validator("dataset-manifest.schema.json").validate(record)


@pytest.mark.parametrize(
    ("schema_name", "record"),
    [
        (
            "ground-truth-record.schema.json",
            {
                "queryId": "Q-001",
                "category": "answerable",
                "question": "What is the fabricated retention period?",
                "expectedContext": [
                    {
                        "documentItemId": "fabricated-item",
                        "pageStart": 1,
                        "pageEnd": 1,
                        "exactText": "Fabricated records are retained for thirty days.",
                    }
                ],
                "expectedAnswer": "Thirty days.",
                "expectedCitations": ["fabricated-item#page=1"],
                "principalCaseLabel": "authorized-reader",
                "smeNotes": "Fabricated unit-test record.",
            },
        ),
        (
            "principal-cases.schema.json",
            {
                "version": "principals-v1",
                "cases": [
                    {
                        "label": "authorized-reader",
                        "userObjectId": "fabricated-user",
                        "groupIds": ["fabricated-group"],
                        "expectedAllowDocumentIds": ["fabricated-item"],
                        "expectedDenyDocumentIds": ["fabricated-denied-item"],
                    }
                ],
            },
        ),
        (
            "approval.schema.json",
            {
                "datasetHash": SHA256,
                "approverIdentity": "fabricated-sme",
                "approvedAt": "2026-08-05T12:00:00Z",
                "status": "approved",
                "reviewNotes": "Fabricated approval.",
                "expiresAt": "2027-08-05T12:00:00Z",
            },
        ),
        (
            "experiment-manifest.schema.json",
            {
                "experimentId": "experiment-v1-run-1",
                "datasetHash": SHA256,
                "datasetComponents": {
                    "groundTruthHash": SHA256,
                    "principalCasesHash": SHA256,
                    "approvalHash": SHA256,
                    "datasetManifestHash": SHA256,
                },
                "sourceId": "fabricated-source",
                "runId": "fabricated-run",
                "codeCommit": "0" * 64,
                "sourceTreeHash": SHA256,
                "submittedContextHash": SHA256,
                "imageDigest": "sha256:" + SHA256,
                "baseImageDigest": "sha256:" + SHA256,
                "dependencyLockHash": SHA256,
                "acrBuildId": "fabricated-build-id",
                "catalogSha": SHA256,
                "catalogEtag": "etag-a",
                "profiles": {
                    profile_name: {"version": f"{profile_name}-v1", "parameters": {}}
                    for profile_name in (
                        "extraction",
                        "chunking",
                        "cleaning",
                        "enrichment",
                        "embedding",
                        "retrieval",
                        "prompt",
                    )
                },
                "modelDeployments": {
                    "embedding": "fabricated-embedding",
                    "chat": "fabricated-chat",
                    "evaluator": "fabricated-evaluator",
                },
                "environment": "evaluation",
                "evaluatorVersions": {"fabricated-evaluator": "1.0.0"},
                "pythonVersion": "3.12.7",
                "principalCase": "authorized-reader",
                "retrievalMode": "hybrid",
                "k": 5,
                "repeatIndex": 1,
                "seed": 0,
                "evaluationAsOf": "2026-08-05T12:00:00Z",
                "candidateSetHash": SHA256,
                "baselineRankingHash": SHA256,
                "candidateRankingHash": SHA256,
                "startedAt": "2026-08-05T12:00:00Z",
            },
        ),
        (
            "ranking-record.schema.json",
            {
                "queryId": "Q-001",
                "retrievedContext": [
                    {"documentItemId": "fabricated-item", "pageNumber": 1},
                    {"documentItemId": "fabricated-item", "pageNumber": 2},
                ],
            },
        ),
    ],
)
def test_protected_artifact_schemas_accept_fabricated_records(
    schema_name: str,
    record: dict[str, object],
) -> None:
    load_validator(schema_name).validate(record)


def _fabricated_manifest() -> dict[str, object]:
    return {
        "experimentId": "experiment-v1-run-1",
        "datasetHash": SHA256,
        "datasetComponents": {
            "groundTruthHash": SHA256,
            "principalCasesHash": SHA256,
            "approvalHash": SHA256,
            "datasetManifestHash": SHA256,
        },
        "sourceId": "fabricated-source",
        "runId": "fabricated-run",
        "codeCommit": "0" * 64,
        "sourceTreeHash": SHA256,
        "submittedContextHash": SHA256,
        "imageDigest": "sha256:" + SHA256,
        "baseImageDigest": "sha256:" + SHA256,
        "dependencyLockHash": SHA256,
        "acrBuildId": "fabricated-build-id",
        "catalogSha": SHA256,
        "catalogEtag": "etag-a",
        "profiles": {
            profile_name: {"version": f"{profile_name}-v1", "parameters": {}}
            for profile_name in (
                "extraction",
                "chunking",
                "cleaning",
                "enrichment",
                "embedding",
                "retrieval",
                "prompt",
            )
        },
        "modelDeployments": {
            "embedding": "fabricated-embedding",
            "chat": "fabricated-chat",
            "evaluator": "fabricated-evaluator",
        },
        "environment": "evaluation",
        "evaluatorVersions": {"fabricated-evaluator": "1.0.0"},
        "pythonVersion": "3.12.7",
        "principalCase": "authorized-reader",
        "retrievalMode": "hybrid",
        "k": 5,
        "repeatIndex": 1,
        "seed": 0,
        "evaluationAsOf": "2026-08-05T12:00:00Z",
        "candidateSetHash": SHA256,
        "baselineRankingHash": SHA256,
        "candidateRankingHash": SHA256,
        "startedAt": "2026-08-05T12:00:00Z",
    }


@pytest.mark.parametrize("etag", [None, "", " "])
def test_experiment_manifest_rejects_invalid_catalog_etag(etag: object) -> None:
    record = _fabricated_manifest()
    record["catalogEtag"] = etag
    with pytest.raises(ValidationError):
        load_validator("experiment-manifest.schema.json").validate(record)


def test_experiment_manifest_rejects_missing_catalog_etag() -> None:
    record = _fabricated_manifest()
    del record["catalogEtag"]
    with pytest.raises(ValidationError):
        load_validator("experiment-manifest.schema.json").validate(record)


def test_experiment_manifest_rejects_short_commit() -> None:
    record = _fabricated_manifest()
    record["codeCommit"] = "abcdef0"
    with pytest.raises(ValidationError):
        load_validator("experiment-manifest.schema.json").validate(record)


def test_experiment_manifest_rejects_mutable_image_reference() -> None:
    record = _fabricated_manifest()
    record["imageDigest"] = "retrieval-agent:latest"
    with pytest.raises(ValidationError):
        load_validator("experiment-manifest.schema.json").validate(record)


def test_experiment_manifest_rejects_naive_evaluation_as_of() -> None:
    record = _fabricated_manifest()
    record["evaluationAsOf"] = "2026-08-05T12:00:00"
    with pytest.raises(ValidationError):
        load_validator("experiment-manifest.schema.json").validate(record)


def test_experiment_manifest_rejects_unknown_retrieval_mode() -> None:
    record = _fabricated_manifest()
    record["retrievalMode"] = "keyword"
    with pytest.raises(ValidationError):
        load_validator("experiment-manifest.schema.json").validate(record)


def test_experiment_manifest_rejects_missing_dataset_component_hash() -> None:
    record = _fabricated_manifest()
    del record["datasetComponents"]["principalCasesHash"]  # type: ignore[union-attr]
    with pytest.raises(ValidationError):
        load_validator("experiment-manifest.schema.json").validate(record)


def test_ranking_record_rejects_forbidden_fields() -> None:
    record = {
        "queryId": "Q-001",
        "retrievedContext": [
            {
                "documentItemId": "doc-a",
                "pageNumber": 1,
                "content": "PROTECTED",
            }
        ],
    }
    with pytest.raises(ValidationError):
        load_validator("ranking-record.schema.json").validate(record)


def test_ranking_record_rejects_zero_page_number() -> None:
    record = {
        "queryId": "Q-001",
        "retrievedContext": [{"documentItemId": "doc-a", "pageNumber": 0}],
    }
    with pytest.raises(ValidationError):
        load_validator("ranking-record.schema.json").validate(record)


def test_answerable_ground_truth_rejects_missing_context() -> None:
    record = {
        "queryId": "Q-002",
        "category": "answerable",
        "question": "A fabricated question?",
        "expectedContext": [],
        "expectedAnswer": "A fabricated answer.",
        "expectedCitations": [],
        "principalCaseLabel": "authorized-reader",
        "smeNotes": "",
    }

    with pytest.raises(ValidationError):
        load_validator("ground-truth-record.schema.json").validate(record)


def test_approval_rejects_non_sha256_dataset_hash() -> None:
    record = {
        "datasetHash": "not-a-hash",
        "approverIdentity": "fabricated-sme",
        "approvedAt": "2026-08-05T12:00:00Z",
        "status": "approved",
        "reviewNotes": "",
        "expiresAt": "2027-08-05T12:00:00Z",
    }

    with pytest.raises(ValidationError):
        load_validator("approval.schema.json").validate(record)


# ---------------------------------------------------------------------------
# Five-format image-rich companion schemas.
# ---------------------------------------------------------------------------


def _fabricated_image_rich_dataset_manifest() -> dict[str, object]:
    formats = ("md", "pdf", "docx", "pptx", "xlsx")
    visual_types = (
        "flow-diagram",
        "rich-image-diagram",
        "workflow-screenshot",
        "chart",
        "smartart-shape",
        "scan",
    )
    documents = []
    for index, fmt in enumerate(formats):
        document_visual_types = {
            "md": ["flow-diagram"],
            "pdf": ["scan"],
            "docx": ["rich-image-diagram", "smartart-shape"],
            "pptx": ["workflow-screenshot"],
            "xlsx": ["chart"],
        }[fmt]
        excluded_count = 1 if fmt == "pptx" else 0
        unsupported_count = 1 if fmt == "docx" else 0
        locator_kind = {
            "md": "section",
            "pdf": "page",
            "docx": "section",
            "pptx": "slide",
            "xlsx": "worksheet",
        }[fmt]
        extraction_provider, extraction_route = {
            "md": ("direct", "markdown-direct"),
            "pdf": ("document-intelligence", "di-pdf-layout-vision"),
            "docx": ("document-intelligence", "di-office-native-vision"),
            "pptx": ("content-understanding", "cu-office-rendered-pdf"),
            "xlsx": ("document-intelligence", "di-office-native-vision"),
        }[fmt]
        entries = [
            {
                "ordinal": 0,
                "visualIdHash": SHA256,
                "objectType": "image",
                "sourceLocator": {
                    "kind": locator_kind,
                    "label": "Source 1",
                    "ordinalStart": 1,
                    "ordinalEnd": 1,
                },
                "relevance": "required",
                "disposition": "described",
                "provenance": [
                    "rendered" if fmt in {"docx", "pptx", "xlsx"} else "direct"
                ],
                **(
                    {
                        "derivativeLocator": {
                            "kind": "page",
                            "label": "Rendered page 1",
                            "ordinalStart": 1,
                            "ordinalEnd": 1,
                        }
                    }
                    if fmt in {"docx", "pptx", "xlsx"}
                    else {}
                ),
            }
        ]
        if excluded_count:
            entries.append(
                {
                    "ordinal": len(entries),
                    "visualIdHash": "b" * 64,
                    "objectType": "image",
                    "sourceLocator": {
                        "kind": locator_kind,
                        "label": "Source 1",
                        "ordinalStart": 1,
                        "ordinalEnd": 1,
                    },
                    "relevance": "decorative",
                    "disposition": "excluded",
                    "provenance": [],
                }
            )
        if unsupported_count:
            entries.append(
                {
                    "ordinal": len(entries),
                    "visualIdHash": "c" * 64,
                    "objectType": "smartart",
                    "sourceLocator": {
                        "kind": locator_kind,
                        "label": "Source 1",
                        "ordinalStart": 1,
                        "ordinalEnd": 1,
                    },
                    "relevance": "unresolved",
                    "disposition": "unsupported",
                    "provenance": [],
                }
            )
        document_id = "a" * 63 + f"{index:x}"
        manifest_pages = [
            {
                "id": f"visual-manifest:{document_id}:000000",
                "pageIndex": 0,
                "pageHash": "d" * 64,
            }
        ]
        manifest_hash = hashlib.sha256(
            json.dumps(
                [
                    {"id": page["id"], "pageHash": page["pageHash"]}
                    for page in manifest_pages
                ],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        documents.append(
            {
                "sourceItemId": f"fab-item-{index}",
                "documentId": document_id,
                "sourceRunId": "fabricated-source:run-1",
                "eTag": f"fab-etag-{index}",
                "sourceContentHash": SHA256,
                "sourceSizeBytes": 1024,
                "sourceMimeType": "application/octet-stream",
                "expectedNormalizedContentHash": SHA256,
                "expectedNormalizedSizeBytes": 2048,
                "classification": "internal",
                "format": fmt,
                "extractionProvider": extraction_provider,
                "extractionRoute": extraction_route,
                "visualStructures": document_visual_types,
                "visualCoverage": {
                    "status": "complete",
                    "inventoryCount": 1 + excluded_count + unsupported_count,
                    "requiredCount": 1,
                    "describedCount": 1,
                    "excludedCount": excluded_count,
                    "unsupportedCount": unsupported_count,
                    "uncoveredCount": 0,
                },
                "visualManifestPageCount": 1,
                "visualManifestHash": manifest_hash,
                "visualManifestPages": manifest_pages,
                "visualManifestEntries": entries,
                "securityClass": "group-restricted",
            }
        )
    return {
        "datasetVersion": "image-rich-v1",
        "domain": "fabricated corpus",
        "useCase": "fabricated five-format visual coverage",
        "source": {
            "sourceId": "fabricated-source",
            "driveId": "fabricated-drive",
            "libraryFingerprint": SHA256,
        },
        "classifications": ["internal"],
        "contentVariants": ["diagrams", "text"],
        "documents": documents,
        "coverage": {
            "documentCount": len(documents),
            "queryTargetCount": len(documents),
            "classificationCounts": {"internal": len(documents)},
            "variantCounts": {"diagrams": len(documents)},
            "formatCounts": {fmt: 1 for fmt in formats},
            "visualTypeCounts": {vt: 1 for vt in visual_types},
        },
        "createdAt": "2026-09-01T12:00:00Z",
    }


def test_image_rich_dataset_manifest_accepts_complete_record() -> None:
    load_validator("image-rich-dataset-manifest.schema.json").validate(
        _fabricated_image_rich_dataset_manifest()
    )


def test_image_rich_dataset_manifest_rejects_missing_format_count() -> None:
    record = _fabricated_image_rich_dataset_manifest()
    del record["coverage"]["formatCounts"]["pdf"]  # type: ignore[union-attr]
    with pytest.raises(ValidationError):
        load_validator("image-rich-dataset-manifest.schema.json").validate(record)


def test_image_rich_dataset_manifest_rejects_missing_visual_type_count() -> None:
    record = _fabricated_image_rich_dataset_manifest()
    del record["coverage"]["visualTypeCounts"]["scan"]  # type: ignore[union-attr]
    with pytest.raises(ValidationError):
        load_validator("image-rich-dataset-manifest.schema.json").validate(record)


def test_image_rich_dataset_manifest_rejects_unknown_format() -> None:
    record = _fabricated_image_rich_dataset_manifest()
    record["documents"][0]["format"] = "rtf"  # type: ignore[index]
    with pytest.raises(ValidationError):
        load_validator("image-rich-dataset-manifest.schema.json").validate(record)


def test_image_rich_dataset_manifest_rejects_provider_route_mismatch() -> None:
    record = _fabricated_image_rich_dataset_manifest()
    record["documents"][1]["extractionRoute"] = "cu-pdf-document-search"  # type: ignore[index]
    with pytest.raises(ValidationError):
        load_validator("image-rich-dataset-manifest.schema.json").validate(record)


def test_image_rich_dataset_manifest_rejects_non_sha256_document_id() -> None:
    record = _fabricated_image_rich_dataset_manifest()
    record["documents"][0]["documentId"] = "not-a-hash"  # type: ignore[index]
    with pytest.raises(ValidationError):
        load_validator("image-rich-dataset-manifest.schema.json").validate(record)


def test_image_rich_dataset_manifest_rejects_uncovered_visual() -> None:
    record = _fabricated_image_rich_dataset_manifest()
    record["documents"][0]["visualCoverage"]["uncoveredCount"] = 1  # type: ignore[index]
    with pytest.raises(ValidationError):
        load_validator("image-rich-dataset-manifest.schema.json").validate(record)


def _fabricated_ground_truth(query_class: str) -> dict[str, object]:
    context: dict[str, object] = {
        "sourceItemId": "fab-item",
        "documentId": SHA256,
        "sourceContentHash": SHA256,
        "requiredModality": (
            "visual_description" if query_class == "visual_only" else "text"
        ),
        "locatorKind": "page",
        "locatorLabel": "Page 1",
        "locatorOrdinalStart": 1,
        "locatorOrdinalEnd": 1,
    }
    if query_class == "text_only":
        context["exactText"] = "Fabricated text."
    elif query_class == "visual_only":
        context["visualType"] = "flow-diagram"
        context["expectedFacts"] = ["Fabricated fact about a diagram."]
    elif query_class == "mixed":
        context["exactText"] = "Fabricated text."
    return {
        "queryId": "Q-IR-001",
        "queryClass": query_class,
        "question": "Fabricated question?",
        "expectedContext": [context],
        "expectedCitations": ["fab-item#page=1"],
        "principalCaseLabel": "authorized-reader",
        "smeNotes": "",
    }


@pytest.mark.parametrize(
    "query_class", ["visual_only", "text_only", "mixed", "acl"],
)
def test_image_rich_ground_truth_accepts_each_query_class(query_class: str) -> None:
    load_validator("image-rich-ground-truth-record.schema.json").validate(
        _fabricated_ground_truth(query_class)
    )


def test_image_rich_ground_truth_rejects_text_only_without_exact_text() -> None:
    record = _fabricated_ground_truth("text_only")
    del record["expectedContext"][0]["exactText"]  # type: ignore[index]
    with pytest.raises(ValidationError):
        load_validator("image-rich-ground-truth-record.schema.json").validate(record)


def test_image_rich_ground_truth_rejects_visual_only_without_expected_facts() -> None:
    record = _fabricated_ground_truth("visual_only")
    del record["expectedContext"][0]["expectedFacts"]  # type: ignore[index]
    with pytest.raises(ValidationError):
        load_validator("image-rich-ground-truth-record.schema.json").validate(record)


def test_image_rich_ground_truth_rejects_unknown_query_class() -> None:
    record = _fabricated_ground_truth("text_only")
    record["queryClass"] = "answerable"
    with pytest.raises(ValidationError):
        load_validator("image-rich-ground-truth-record.schema.json").validate(record)


def test_image_rich_ground_truth_rejects_missing_typed_locator() -> None:
    record = _fabricated_ground_truth("visual_only")
    del record["expectedContext"][0]["locatorKind"]  # type: ignore[index]
    with pytest.raises(ValidationError):
        load_validator("image-rich-ground-truth-record.schema.json").validate(record)


def _fabricated_visual_review() -> dict[str, object]:
    return {
        "queryId": "Q-IR-001",
        "arm": "di-figures-vision",
        "documentId": SHA256,
        "locatorKind": "slide",
        "locatorLabel": "Slide 2",
        "locatorOrdinalStart": 2,
        "locatorOrdinalEnd": 2,
        "factualSupport": "supported",
        "provenance": "correct",
        "criticalInvention": False,
        "reviewerIdentity": "fab-sme",
        "reviewedAt": "2026-09-01T12:00:00Z",
    }


def test_image_rich_visual_review_accepts_complete_record() -> None:
    load_validator("image-rich-visual-review-record.schema.json").validate(
        _fabricated_visual_review()
    )


def test_image_rich_visual_review_rejects_baseline_arm() -> None:
    record = _fabricated_visual_review()
    record["arm"] = "di-markdown"
    with pytest.raises(ValidationError):
        load_validator("image-rich-visual-review-record.schema.json").validate(record)


def test_image_rich_visual_review_rejects_extra_fields() -> None:
    record = _fabricated_visual_review()
    record["descriptionText"] = "PROTECTED"
    with pytest.raises(ValidationError):
        load_validator("image-rich-visual-review-record.schema.json").validate(record)