---
title: Protected RAG Evaluation
description: Protected inputs, ranking generation, metrics, and release gates
---

This directory contains versioned schemas and an offline retrieval evaluator. Real SharePoint content and per-query outputs are protected release evidence and must never be committed.

## Directory Contract

| Path | Git policy | Content |
| --- | --- | --- |
| `schemas/` | Committed | JSON Schema contracts for protected artifacts |
| `private/` | Ignored | Dataset manifest, ground truth, principal cases, SME approval |
| `results/raw/` | Ignored | Per-query retrieval, answer, ACL, safety, latency, and cost results |
| `results/summaries/` | Conditional | Sanitized aggregates only after content, PII, secret, and ACL-ID checks |

An authorized evaluator materializes protected files through environment-provided paths and credentials. The application never loads evaluation artifacts as runtime state and never activates an evaluation run against a production source. `app/retrieval/*` must not import from `evaluation/*`.

## Required Protected Files

1. `private/dataset-manifest.json`
2. `private/ground-truth.jsonl`
3. `private/principal-cases.json`
4. `private/approval.json`
5. One `experiment-manifest.json` beside each raw experiment result set

Validate every JSON document against its schema before running an experiment. Validate each nonblank JSONL line independently against `ground-truth-record.schema.json`.

## Ranking record shape

Baseline and candidate ranking files are JSONL where each line is:

```json
{
  "queryId": "Q-001",
  "retrievedContext": [
    {"documentItemId": "doc-a", "pageNumber": 1},
    {"documentItemId": "doc-a", "pageNumber": 2}
  ]
}
```

Ranking records must conform to `schemas/ranking-record.schema.json`. They are identity-only: question text, content, principal identifiers, and score fields are prohibited.

This generic contract is page-only. The generator derives `pageNumber` from a single-page typed page locator and rejects section, slide, worksheet, or multi-page locators rather than relabeling them as pages. Use the image-rich contract below for five-format evaluation.

## Interval-aware retrieval comparison

Ground truth records supply inclusive page intervals per document. Rankings supply a single `pageNumber` per returned entry. A returned entry matches a judgment when `documentItemId` matches and `pageStart <= pageNumber <= pageEnd`. Every contained returned page counts toward Precision@K; each judged interval contributes at most once to recall; MRR uses the first containment hit.

Precision@K divides by the configured `k`; recall over an empty judged set is defined as `0.0`.

```powershell
$env:PYTHONPATH = "."
python -m evaluation.retrieval_metrics `
    --ground-truth evaluation/private/ground-truth.jsonl `
    --baseline evaluation/results/raw/baseline.jsonl `
    --candidate evaluation/results/raw/candidate.jsonl `
  --manifest evaluation/results/raw/experiment-manifest.json `
    --k 5 `
    --min-precision-delta 0 `
    --min-recall-delta 0 `
    --min-mrr-delta 0 `
    --regression-budget 0
```

Ground truth and both ranking files must cover exactly the same `queryId` set. Duplicate query IDs, duplicate judged intervals, overlapping judged intervals per `(query, document)`, duplicate returned identities per query, and page numbers below `1` fail at exit code `2`. Non-finite deltas and negative regression budgets are rejected before comparison.

The evaluator prints aggregate Precision@K, Recall@K, MRR, metric deltas, and per-query regressions. Exit code `1` means a declared threshold or the regression budget failed; exit code `2` means an input was invalid. `--regression-budget` defaults to `0`, so a single per-query regression fails the run even when aggregate deltas are `>= 0`. Thresholds and budgets are release decisions and must be declared before examining candidate results.

## Protected ranking generator

`evaluation/generate_rankings.py` is the private generator for baseline and candidate rankings. It:

- Validates ground truth, principal cases, approval, and dataset manifest against their schemas.
- Rejects duplicate query IDs, unknown principal cases, and overlapping allow/deny sets.
- Retrieves separate authorized post-ACL, post-ready-manifest candidate pools for the baseline and candidate profiles through `RagService.retrieve_evaluation_pool`.
- Captures one runtime catalog snapshot for both profiles and records its ETag and normalized config digest. Caller provenance cannot override `catalogSha` or `catalogEtag`.
- Applies each profile under one caller-supplied timezone-aware `evaluationAsOf`. Profile-specific synonym behavior can therefore produce different candidate pools.
- Rejects any ranking entry whose `documentItemId` is in the principal case deny set.
- Canonical-hashes the full in-memory candidate scoring input; content is never persisted.
- Atomically writes `baseline.jsonl`, `candidate.jsonl`, and `experiment-manifest.json` and removes partial output on failure.

The manifest binds full 64-character commit, source-tree hash, submitted-context
hash, immutable image digest, base image digest, dependency lock hash, ACR build
ID, captured catalog SHA and ETag, `evaluationAsOf`, candidate-set hash, and
baseline/candidate ranking hashes. The evaluator verifies ground-truth and
ranking hashes and manifest-bound `k`. Verification of source, packaging,
dependency, catalog generation, principal cases, dataset and approval remains
an external release gate. Generation is source-equivalence evidence, not proof
of deployed image behavior or later replica adoption.

Raw rankings under `results/raw/` remain ignored and access-controlled. Summaries under `results/summaries/` may be committed only after they are sanitized of questions, content, and principal, group, and document identifiers.

## Image-Rich Five-Format Evaluation

The image-rich contracts cover Markdown, PDF, DOCX, PPTX, and XLSX without changing the generic page benchmark. Their ranking, ground-truth, dataset, and review records use typed page, section, slide, and worksheet locator identities.

### Five-format components

| Path | Purpose |
| --- | --- |
| `schemas/image-rich-dataset-manifest.schema.json` | Production dataset contract with one entry per extension, selected provider and route, and exact schema-v1 visual manifest evidence |
| `schemas/image-rich-ground-truth-record.schema.json` | Source-hash-bound and modality-bound judgments with exact typed locators |
| `schemas/image-rich-ranking-record.schema.json` | Identity-only top-five rankings with exact typed locators |
| `schemas/image-rich-visual-review-record.schema.json` | SME-authored verdict for each typed candidate-arm visual hit |

### Contract and separation

- Image-rich schemas reject unknown fields and are companions to the generic page schemas; they do not replace them.
- The protected manifest must contain all five formats, every required visual type, at least one audited hidden-content exclusion, and at least one unsupported Office object. Production fields are copied from admitted schema-v1 `source_document` and `visual_manifest_page` records; they are not reconstructed from aggregate counts or raw provider payloads.
- Each document declares a legal format, provider, and route combination. Markdown is direct. Current Content Understanding Office records use one rendered PDF derivative; `cu-office-native` remains a schema value only for historical evidence compatibility.
- Five-format release evidence must verify `sourceRunId`, contiguous object ordinals, unique hashed visual identities, typed source locators, rendered derivative provenance where required, page IDs and hashes, document-level manifest hash, and exact closure against visual coverage before rankings are accepted.
- Every document must satisfy `inventory = required + excluded + unsupported` and `required = described + uncovered`; release evidence requires `uncovered = 0`.
- Release acceptance also requires exact source-plus-locator Recall@5, no text-only or ACL regression, complete SME review, correct review provenance, no critical invention, and no unauthorized retrieval.

### Where to look next

- Production acceptance criteria: [Production readiness](../docs/PRODUCTION_READINESS.md#evaluation-gate).
