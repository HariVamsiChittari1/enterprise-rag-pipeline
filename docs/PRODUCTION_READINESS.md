---
title: Production Readiness
description: Verified evidence, release gates, unresolved risks, and readiness verdict
---

This document separates verified release evidence from production-scale work that remains unproven. It does not convert development estimates into production guarantees.

## Historical Verified Baseline

The historical 2026-09-05 release completed local validation, guarded deployment,
and non-production smoke testing. It predates the mutable runtime catalog and
does not validate that candidate. Detailed identifiers and raw evidence remain
outside maintained documentation; the historical results are summarized below.

- The complete local Python gate passed with 1,031 tests and 8 expected opt-in live-Cosmos skips. Dependency consistency, Bicep and parameter compilation, PowerShell parsing, diff hygiene, credential scanning, and editor diagnostics also passed.
- The exact reviewed candidate deployed with Document Intelligence enabled, Content Understanding disabled, ACL enforcement retained, and immutable retrieval image and catalog digests.
- A fresh full sync discovered and admitted all 31 documents with zero failures. Markdown, PDF, DOCX, PPTX, and XLSX were all represented, and every ready manifest had a matching positive chunk count.
- A disposable DOCX completed delta create, same-item update, supersession, and conditional deletion. Concurrent trigger acquisition retained one tracked instance, the 31-document baseline was restored, and lifecycle reconciliation checked 2,344 records with zero repairs or failures.
- All six standard and agentic scenarios passed across hybrid, vector, and full-text modes with citations, the reviewed catalog and scoring profile, and no degraded retrieval. Every ready manifest had a nonempty ACL group, and the unauthenticated Function boundary returned 401.
- Sanitized telemetry showed successful ingestion, Graph, Document Intelligence, vision, lifecycle, delta, cleanup, and retrieval activity. The window also contained expected concurrent-probe 499 responses, worker-exit exceptions, and error-level ingestion traces, so it is not evidence of a zero-error environment. Application cleanup paths require Document Intelligence result deletion, but telemetry alone does not prove every service-side result deletion.
- The temporary catalog job was removed through the guarded cleanup phase, leaving the reviewed serving configuration unchanged.
- Content Understanding separately passed six like-for-like retrieval scenarios. Document Intelligence remains the production default because it preserves native Office semantics and uses rendered PDF only for required visual augmentation; the reports prove contract success, not comparative ranking quality.

These results prove functional behavior for the tested corpus and target. A live non-member ACL denial and a direct private Container App probe were not completed because they required another identity or private DNS reachability from the workstation. The evidence does not prove production capacity, SLOs, disaster recovery, or multi-region behavior.

## Release Artifact Contract

A release is one reviewed tuple:

- Source-tree hash and deployment-plan hash.
- Immutable retrieval image `repository@sha256:<digest>`.
- Observed runtime catalog ETag and normalized config digest at verification time.
- Function package built from the same reviewed source.
- Exact target subscription, tenant, resource group, region, azd environment, and deployment instance.

Use `scripts/deploy.ps1`; do not run direct `azd provision`, `az containerapp update`, or `func azure functionapp publish` as production release steps.

Application rollback is a new reviewed deployment compatible with the current
singleton. Catalog recovery is a separately reviewed restoration of protected
valid content under a new ETag; no activation pointer or automatic rollback
exists. A later catalog edit invalidates affected evidence without changing the
application image.

## Required Pre-Release Gates

1. Local unit, contract, and Bicep checks pass.
2. `Authority` returns reviewed plan/source hashes.
3. `Foundation`, `Operations`, and `Final` what-if output is reviewed.
4. External Entra applications, app roles, Graph permissions, Key Vault certificate, and SharePoint site grant are verified without exposing secrets.
5. ACR returns an immutable image digest.
6. Read-only catalog verification validates the current singleton and binds the exact operations execution; seed integrity applies only to explicit initialization.
7. Function and ACA authentication configurations match the exact audience/application/principal contracts.
8. Database schemas and partition keys are compatible with the deployed code.
9. Monitoring is available for requests, dependencies, exceptions, and ACA logs.
10. The schema-v1 typed locator, modality, provenance, and visual-coverage fields are present in every candidate chunk and accepted by retrieval.
11. The selected extraction provider matches the reviewed flags: Content Understanding takes precedence when enabled; otherwise Document Intelligence is used. Markdown remains direct.
12. The SharePoint application has the approved Microsoft Graph `Files.ReadWrite.All` assignment required for Office-to-PDF Content Understanding processing and Document Intelligence visual augmentation.
13. The permission-change metadata contract is resolved: either `Sites.FullControl.All` has separate security approval, or the implementation and validation contract no longer depend on the associated Graph header. Do not grant this permission implicitly.

## Required Post-Deployment Gates

- Verify Function and ACA health, revision, traffic, image digest, catalog ETag/digest, and `ACL_ENABLED`.
- Prove private Entra-authenticated Data Explorer read/edit with keys disabled, effective editor/writer/reader denials, and separately approved removal of obsolete broad grants.
- Prove valid-edit adoption, rejected-edit last-known-good behavior, recovery and restart behavior across a stable ready replica cohort, with target telemetry and Cosmos actor/request correlation.
- Run authenticated and unauthenticated gateway tests.
- Run authorized and denied ACL retrieval tests.
- Validate full sync, delta update/delete, ACL resync/restoration, and lifecycle reconciliation as applicable to the release.
- Run standard and agentic retrieval across required modes.
- Validate all deployed scoring profiles, freshness, and synonyms.
- Verify request/audit correlation and no unexpected dependency failures.
- For Markdown, PDF, DOCX, PPTX, and XLSX, prove the selected provider and route, complete required-visual accounting, and zero unexplained uncovered visuals.
- Verify admitted schema-v1 source records bind to the exact persisted visual manifest pages and hashes before retrieval eligibility.
- For every representative visual judgment, require the expected source plus exact typed locator in the candidate top five.
- Verify hidden slides and worksheets are excluded and audited, and detected unsupported Office objects are reported rather than indexed.
- Delete all test fixtures and prove manifest/chunk/query cleanup.
- Remove the temporary catalog job after explicit approval and run a final smoke test.

## Security Gaps and Boundaries

| Area | Current state | Release impact |
| --- | --- | --- |
| Function client allowlist | Required in Bicep | Verify exact approved caller before release |
| Per-user admin authorization | Not enforced on destructive Function endpoints | Production release requires risk acceptance or implementation of app-role checks |
| Lifecycle webhook | Excluded from EasyAuth and does not validate `clientState` | Security gap requiring risk acceptance or remediation |
| Retrieval gateway | ACA and application code restrict calls to Function UAMI | Verify app-role assignment externally because Bicep does not mutate Entra |
| ACL model | Entra security groups only | Direct user shares are unsupported |
| Permission-change metadata | Connector requests a Graph header documented with `Sites.FullControl.All`, which is absent from the approved prerequisite set | Resolve through separate security approval or an implementation change; do not grant as a diagnostic shortcut |
| Rate limiting | In-memory per ACA replica | Not a hard distributed abuse control |
| Secrets | Certificate in external Key Vault; webhook client state supplied through a protected azd secret reference or approved external secret source | Verify external vault/network policy and secret rotation process; do not use plaintext `azd env set` for secrets |
| Query audit data | Stores user/tenant IDs, up to 2,000 question characters, and a 500-character answer preview for the 90-day container TTL; inspect can return these records | Apply privacy classification, least-privilege endpoint access, retention approval, and safe diagnostic handling |

## Capacity and Reliability Evidence Still Required

No current repository artifact proves the following for a production workload:

- Maximum sustainable file count, pages, chunks, or concurrent source changes.
- Cosmos RU/s, throttling envelope, partition hot spots, or query RU distribution.
- Azure OpenAI, Document Intelligence, or Language quota required for peak ingestion/query traffic.
- p50/p95/p99 latency, throughput, saturation, or error-rate SLOs.
- Scale-out behavior of the per-replica rate limiter.
- Zone-failure, regional-failure, restore, backup, or disaster-recovery objectives.
- Multi-library operation from one Function deployment.

Treat any prior 10K-file duration, RU, or concurrency number as a planning hypothesis until a representative load test records inputs, duration, throttles, costs, and recovery behavior.

## Production Capacity Plan

Before production approval:

1. Define corpus size, document-size/page distributions, change rate, query concurrency, latency SLOs, RTO/RPO, and cost ceiling.
2. Select serverless or provisioned Cosmos mode from measured RU demand; do not assume a fixed RU value is sufficient.
3. Validate Azure OpenAI and AI Services quotas in the target region.
4. Run staged load tests with representative documents and ACL distributions.
5. Measure Function/ACA scale, dependency throttling, Cosmos RU, latency percentiles, and failure recovery.
6. Run lifecycle reconciliation and restart recovery under injected partial failures.
7. Record halt criteria and the compatible immutable rollback tuple.

## Evaluation Gate

Protected evaluation requires approved ground truth, principal cases, dataset manifest, and SME approval. The generic page benchmark runs `evaluation.generate_rankings` and `evaluation.retrieval_metrics`. The five-format release gate extends the existing image-rich workflow with source-hash and modality binding, legal provider and route combinations, exact schema-v1 visual manifest bindings, and typed page, section, slide, or worksheet locator identities.

Before accepting five-format rankings, verify `sourceRunId`, manifest page identities and hashes, object-level coverage closure, typed source locators, and rendered derivative provenance where required. Ranking provenance must record the verified document, provider, route, manifest-page, and manifest-entry counts.

The five-format candidate must return every expected source-plus-locator identity in its top five, produce no unexplained uncovered required visuals, preserve ACL denial, avoid text-only regression, and have complete SME review with correct provenance and no critical invention. Declare all thresholds before examining candidate results.

The evaluator verifies ranking/ground-truth hashes bound by the manifest. Source-tree, image, dependency, catalog, approval, and submitted-context hashes remain release-gate responsibilities outside the evaluator.

## Readiness Verdict

The results below describe the 2026-09-05 release, not the current worktree.
The mutable-catalog candidate and [audio implementation](AUDIO_RAG_PROPOSAL.md#current-status)
have not established same-source deployment and end-to-end acceptance here.

- **Historical-release deployment smoke:** passed on 2026-09-05.
- **Five-format local implementation:** complete local gate passed for the current Document Intelligence candidate on 2026-09-05.
- **Five-format Azure deployment and smoke:** Document Intelligence production-default and Content Understanding rollback scenarios passed in the approved non-production environment.
- **Same-release functional E2E:** full sync, delta create/update/delete, lifecycle reconciliation, authorized retrieval, unauthenticated rejection, and six retrieval scenarios passed. A live non-member denial and direct private Container App probe remain open.
- **Current-worktree release readiness:** not established; local checks alone do not satisfy deployment and E2E gates.
- **General production readiness:** conditional on the gates in this document.
- **Blocking evidence for production scale:** workload requirements, capacity/load evidence, recovery objectives/tests, the two open E2E checks, and acceptance of or fixes for the listed security gaps.
