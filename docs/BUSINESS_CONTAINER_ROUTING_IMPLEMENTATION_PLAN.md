---
title: Business Container Routing Implementation Plan
description: Dependency-ordered implementation and validation plan for source-aware Cosmos routing
---

# Business Container Routing Implementation Plan

## Goal

Implement the approved source-aware business/common Cosmos routing design so
each SharePoint source ingests into and retrieves from its registered container
pair, applies its current ACL policy, and exposes retrieval directly through
ACA.

Done means:

- the acceptance criteria in
  [Business Container Routing Requirement and Design](BUSINESS_CONTAINER_ROUTING.md)
  pass in the development environment;
- the approved fresh-ingestion cutover completes without legacy routing
  fallback;
- Postman and PowerShell can call ACA directly with a delegated user token;
- files with at least one verified Entra security group ingest even when other
  permission entries produce non-authorizing feedback, while files without a
  verified group remain rejected;
- local Azure SDK access uses the dedicated service principal without developer
  identity fallback, while Azure-hosted workloads continue using managed
  identities;
- targeted and end-to-end checks pass; and
- agent/OBO authentication and production sizing remain outside this
  milestone.

## Status

**Implementation status for this request: PARTIALLY STARTED.**

The codebase has completed the source contract, ingestion routing, selected-pair
retrieval, and per-request ACL policy milestones. Later phases remain deferred
until their implementation gates.

This file is the execution plan. The lightweight session tracker is
`.copilot-tasks/business-container-routing-plan.md`. Update a task to
`IN PROGRESS` before changing its implementation surfaces and to `DONE` only
after its acceptance check passes. A failed or unavailable prerequisite is
`BLOCKED`, with the evidence recorded beside the task.

| Phase | Status | Exit gate |
| --- | --- | --- |
| 0. Baseline and safeguards | DONE | Baseline tests and dev-cutover inputs are recorded |
| 1. Source contract | DONE | Updated strict schema-v1 contract passes |
| 2. Ingestion routing | DONE | Every ingestion path uses the source pair |
| 3. Retrieval routing and ACL | DONE | One-source retrieval and both ACL modes pass |
| 4. Direct ACA authentication | DONE | Direct user calls work; Function gateway is absent |
| 4A. Mixed-permission ACL admission | DONE | Valid groups admit files; observations never grant access |
| 4B. Local Azure authentication | DONE | Explicit local SP and deployed managed-identity contracts validate |
| 5. Azure infrastructure | DONE | Six containers, scoped RBAC, and public ACA validate |
| 6. Business metadata contract | DONE | T33 criteria 29-34, focused tests, review, and documentation complete |
| 7. Fresh-content cutover | NOT STARTED | All dev sources complete guarded fresh ingestion |
| 8. Final validation and handoff | NOT STARTED | Full acceptance matrix and review pass |

## Evidence

- The current source parser enforces an exact schema-v1 config containing the
  source binding, container pair, and ACL policy but no `businessName`;
  `app/source_registry.py` is therefore the authoritative T33 contract owner.
- `SourcePlan` carries the complete source and catalog ETag, and ingestion
  coordinator/commit paths compare complete plans. T33 must preserve
  lifecycle/binding validation while excluding label-only changes from
  ingestion work validity.
- Retrieval point-reads one `SourceEligibility` and currently compares the
  complete ETag-bearing value before returning citations. T33 must add the
  label snapshot while revalidating lifecycle, routing, and ACL semantics
  rather than failing on a label-only ETag change.
- `source-documents` stores both `source_document` and
  `visual_manifest_page` records; both require the snapshot to make the
  container metadata contract consistent.
- Ingestion audit items are source-bound through
  `ingestion.telemetry.write_audit_record`; retrieval separately writes LLM
  usage items and a `query_request` summary. T33 must update those explicit
  source-context audit surfaces, not unrelated global catalog events.
- The external-access, VNet-integrated ACA and direct delegated-user
  authentication are complete and remain unchanged by T33. Microsoft documents
  the deployed networking model at:
  [Networking in Azure Container Apps](https://learn.microsoft.com/azure/container-apps/networking).
- ACA built-in authentication can use an existing Microsoft Entra app
  registration:
  [Enable authentication and authorization in Container Apps with Microsoft Entra ID](https://learn.microsoft.com/azure/container-apps/authentication-entra).
- Cosmos data-plane RBAC should use Microsoft Entra identities and the least
  privilege needed:
  [Connect using role-based access control and Microsoft Entra ID](https://learn.microsoft.com/azure/cosmos-db/how-to-connect-role-based-access-control).
- Azure SDK local development can use an app registration service principal
  through `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`:
  [Authenticate Python apps to Azure services during local development](https://learn.microsoft.com/azure/developer/python/sdk/authentication/local-development-service-principal).
- `DefaultAzureCredential` uses a credential chain, so local developer
  credentials must be explicitly excluded to prevent accidental Azure CLI,
  VS Code, or shared-token-cache authentication:
  [Credential chains in the Azure Identity library](https://learn.microsoft.com/azure/developer/python/sdk/authentication/credential-chains).
- Azure-hosted applications should use managed identity rather than shipping
  local client secrets:
  [Managed identities for Azure resources](https://learn.microsoft.com/entra/identity/managed-identities-azure-resources/overview).
- Microsoft Graph represents sharing links, invitations, direct identities,
  and roles as separate permission facets. A non-null `link` identifies a
  sharing-link permission but is not itself an Entra security-group grant:
  [Microsoft Graph permission resource](https://learn.microsoft.com/graph/api/resources/permission)
  and
  [Microsoft Graph sharingLink resource](https://learn.microsoft.com/graph/api/resources/sharinglink).
- The current ingestion parser raises on the first sharing link, malformed
  identity, or unsupported role. The approved rule instead requires complete
  traversal and at least one independently verified security-group grant;
  unrelated permission observations cannot invalidate that grant.

## Plan

### Phase 0: Baseline and safeguards

#### T01 - Record the executable baseline [P0]

- Status: DONE
- Depends on: none
- Surfaces: existing source-registry, ingestion, retrieval, and infrastructure
  tests
- Work:
  - run the smallest existing suites covering the affected surfaces;
  - record pre-existing failures separately; and
  - preserve the current branch's unrelated worktree changes.
- Acceptance:
  - baseline commands and results are recorded;
  - implementation failures can be distinguished from pre-existing failures.
- Evidence: existing tests under `tests/app_runtime`, `tests/ingestion`,
  `tests/retrieval`, and `tests/infra`.
- Result (2026-10-14): 1,001 passed, zero failed, one nonblocking dependency
  deprecation warning in 227.76 seconds on Python 3.12.10 and pytest 9.1.1.
- Environment: used the existing external repository virtual environment; no
  dependencies were installed. Pytest cache was written to
  `C:\DevEnvs\enterprise-rag-pipeline\pytest-cache\t01`.
- Baseline warning: FastAPI `TestClient` emitted `StarletteDeprecationWarning`
  for its httpx integration; this is not a test failure.

#### T02 - Record post-implementation cutover inputs [P1]

- Status: DONE (2026-10-18)
- Depends on: T18
- Surfaces: dev source inventory and approved six-container topology
- Work:
  - record every dev `sourceId`, SharePoint site/drive binding, `contentScope`,
    target pair, and initial `retrievalAclEnabled` value;
  - identify the authenticated ACL member and non-member test users; and
  - record current source-scoped control records that cutover must reset.
- Acceptance:
  - every source has one approved scope, mapping, and valid Boolean policy;
  - no production source or retained production record is included.
- Evidence: live development Cosmos source/container inventory, the T10
  two-source ACL matrix in `MULTI_SHAREPOINT_SOURCES.md`, and the T31 delegated
  zero-group validation recorded in this plan.
- Timing: perform this after code implementation is complete and before the
  destructive development cutover.
- Availability: the development environment is unused, so ACA recreation and
  fresh-ingestion downtime can occur at any time; no scheduling approval is
  required.
- Result (2026-10-18):
  - confirmed the default `rag-aca-e2e-20260827` azd environment matches the
    authenticated development subscription, tenant, resource group, deployment
    instance, Cosmos account, and `rag-db`;
  - confirmed all six approved business/common containers exist with the
    required `/documentKey` and `/sourceRunId` partition contracts;
  - assigned `sharepoint-drive` and `poc-auth-demo` to the Business A pair and
    `poc-pg` to the Business B pair; all three target business sources retain
    ACL filtering;
  - assigned retired `poc-auth` to the Common pair after explicit approval;
    its target record starts paused with `retrievalAclEnabled: false`;
  - recorded source-scoped run, delta, webhook, trigger, coordinator,
    operation/recovery, winner, and dispatch state for guarded reset;
  - selected source-specific member/non-member identities from prior live
    retrieval evidence: Reader-only for `sharepoint-drive`, Reader+Editor for
    `poc-pg`, and the proven zero-security-group guest where applicable;
  - selected two authenticated users for the Common source, where both must
    receive the same eligible evidence because ACL filtering is disabled; and
  - stored the detailed development-only inventory outside source control in
    the session artifact `t02-development-inventory.json`.
- Identity acceptance matrix:

  | Source | Member / user A | Non-member / user B | Expected evidence |
  |---|---|---|---|
  | `sharepoint-drive` | `f2f3ecd9-2095-4b8c-9d89-41c872cd14fc` | `4ef07a3e-8f71-4aa8-8fb1-3041ca3cb140` | Member receives source evidence; zero-group guest receives none. |
  | `poc-pg` | `62fb01d3-a4c8-41fb-b572-9c294230ce23` | `f2f3ecd9-2095-4b8c-9d89-41c872cd14fc` | Reader+Editor member receives source evidence; Reader-only user receives none. |
  | `poc-auth-demo` | `62fb01d3-a4c8-41fb-b572-9c294230ce23` | `4ef07a3e-8f71-4aa8-8fb1-3041ca3cb140` | Member receives source evidence; verified outsider receives none. |
  | `poc-auth` (Common) | `62fb01d3-a4c8-41fb-b572-9c294230ce23` | `f2f3ecd9-2095-4b8c-9d89-41c872cd14fc` | Both receive identical eligible evidence; Graph groups are not consulted. |
- Pre-cutover conditions:
  - the four live registrations do not satisfy the branch's complete strict
    source shape, so T19/T20 must recreate accepted paused records rather than
    mutate routing fields in place;
  - `sharepoint-drive` and `poc-pg` retain nonzero coordinator work counts, so
    T20 must prove drain/reconciliation before reset;
  - the retired `poc-auth` partition retains an awaiting-full-sync reprovision
    intent that T20 must reset before the Common bootstrap full sync; and
  - T21 must revalidate each selected identity because Entra group membership
    is mutable operational state, not immutable T02 configuration.

### Phase 1: Source contract

#### T03 - Update the strict schema-v1 source model [P0]

- Status: DONE
- Depends on: T01
- Surfaces: `app/source_registry.py`, source model tests and fixtures
- Work:
  - add `content_scope`, `chunks_container`, `manifests_container`, and
    `retrieval_acl_enabled`;
  - validate the scope enum, container names, strict Boolean type, and
    `common => false` invariant;
  - remove `recordType` from the ingestion-source shape and parser; and
  - include content scope and container names, but not ACL policy, in the
    binding digest.
- Acceptance:
  - the updated shape parses;
  - the old shape, unknown fields, malformed names, invalid scope/ACL
    combinations, and missing/non-Boolean persisted ACL policy fail explicitly;
  - binding changes when scope or either container changes but not when only
    ACL policy changes.
- Evidence: approved strict schema-v1 reset; no compatibility reader.
- Result (2026-10-14): 37 focused contract tests passed; Ruff and
  `git diff --check` passed.

#### T04 - Extend source registration and policy mutation [P0]

- Status: DONE
- Depends on: T03
- Surfaces: `app/ingestion/source_registry.py`, source operator routes, tests
- Work:
  - require content scope and both containers at registration;
  - populate and persist `false` when common registration omits the ACL flag,
    reject common with `true`, and require an explicit flag for business;
  - continue creating sources as paused;
  - permit only ETag-protected, invariant-valid `retrievalAclEnabled` updates;
  - reject content-scope changes and container remapping through PATCH; and
  - preserve existing audit behavior.
- Acceptance:
  - exact registration retry remains idempotent;
  - stale ETags, common `true`, and attempted scope/container changes fail;
  - ACL-only updates do not change the ingestion binding.
- Evidence: approved operator boundary and immutable container mapping.
- Review correction (2026-10-14): reopened because focused route stubs did not
  exercise repository persistence; the real repository suite exposed a
  forbidden `recordType` write and legacy/defaulted registration behavior.
- Result (2026-10-14): repaired the real route-to-repository contract, removed
  source-container `recordType` persistence and query dependencies, enforced
  explicit business policy and the common-source invariant, and preserved
  immutable routing during ETag-protected policy mutation. Focused validation
  passed with 302 tests. The broad affected suite passed with 2,407 tests,
  8 live-service skips, and the baseline dependency deprecation warning.
#### T05 - Carry immutable routing in `SourcePlan` [P0]

- Status: DONE
- Depends on: T03
- Surfaces: `app/ingestion/source_registry.py`, Durable payload tests
- Work:
  - add content scope and both container names to `SourcePlan` serialization;
  - reconstruct the updated strict source contract from Durable payloads; and
  - bind source-specific container settings in `config_for_source`.
- Acceptance:
  - round-trip payload tests preserve site, drive, content scope, target pair,
    ETag, and binding digest;
  - malformed or missing routing fields fail before activity execution.
- Evidence: existing immutable source-plan pattern.
- Result (2026-10-14): `SourcePlan` now serializes the full immutable routing
  contract, `from_payload()` rehydrates it without compatibility defaults, and
  `config_for_source()` binds the per-source chunk/manifest containers before
  activity execution.

### Phase 2: Ingestion routing

#### T06 - Key ingestion clients by container pair [P0]

- Status: DONE
- Depends on: T05
- Surfaces: `app/function_app.py`, repository/lifecycle factories and tests
- Work:
  - replace global content repository caches with caches keyed by the validated
    chunks/manifests pair;
  - retain shared clients only for shared control containers; and
  - ensure sources sharing a pair reuse clients without sharing source state.
- Acceptance:
  - same pair returns the same content clients;
  - different pairs never return the same content repositories;
  - shared control clients remain unchanged.
- Evidence: current `_client_cache` and approved pair-scoped ownership.
- Result (2026-10-14): the content repository and lifecycle repository caches are now keyed by the resolved pair, while the shared Cosmos client and control locations remain shared. Focused validation passed with 179 relevant tests.

#### T07 - Route every ingestion operation through `SourcePlan` [P0]

- Status: DONE
- Depends on: T05, T06
- Surfaces: full sync, delta, ACL refresh, deletion, reconciliation, retry,
  lifecycle, purge, and retirement paths in `app/function_app.py`
- Work:
  - resolve the accepted source plan before creating content repositories;
  - pass the immutable source-bound configuration to every child activity; and
  - remove fixed-container fallback for source content operations.
- Acceptance:
  - tests cover every listed operation;
  - an invalid/unavailable pair fails only that source;
  - no operation writes to a legacy or unrelated pair.
- Evidence: source-plan resolution is already used across Function activities.
- Review correction (2026-10-14): the initial audit missed two paths that
  validated a source without binding its content pair. Retirement built its
  lifecycle repository from deployment defaults, and operator purge opened the
  literal legacy container role before reading the registered route.
- Result (2026-10-14): active-source activities resolve their accepted
  `SourcePlan` before repository creation. Retirement now binds its validated
  retired plan before deleting documents or orphan chunks, and operator purge
  maps its requested logical role to that source's registered physical
  container. Distinct-pair, invalid-plan, cache-isolation, retry, timeout, and
  lifecycle validation passed with 283 affected tests.

#### T08 - Preserve ACL metadata for both policy values [P0]

- Status: DONE
- Depends on: T07
- Surfaces: SharePoint ACL resolution and chunk persistence tests
- Work:
  - keep existing `allowedGroupIds` ingestion independent of
    `retrievalAclEnabled`; and
  - prove common and business targets retain equivalent ACL metadata.
- Acceptance:
  - chunks contain the expected SharePoint security-group IDs for both `true`
    and `false`;
  - common sources retain ACL metadata while persisting only `false`;
  - changing ACL policy requires no re-ingestion.
- Evidence: ACL policy controls retrieval filtering, not ingestion metadata.
- Result (2026-10-14): no production change was required. Ingestion does not
  consume `retrievalAclEnabled`; it always resolves SharePoint ACLs and carries
  the verified groups through chunk persistence, document admission, and
  lifecycle activation. New regressions prove equivalent metadata for business
  `true`, business `false`, and common `false`. Existing policy-mutation tests
  prove an ACL-only change preserves the binding digest. The affected ACL,
  processing, incremental, lifecycle, source-contract, and registry suites
  passed with 381 tests.

### Phase 3: Retrieval routing and ACL

#### T09 - Point-read one active source policy [P0]

- Status: DONE
- Depends on: T03
- Surfaces: `app/retrieval/source_policy.py`, policy tests
- Work:
  - require `sourceId` at the ACA query boundary and carry it through standard
    and agentic retrieval;
  - replace registry enumeration with one `sourceId` point read;
  - capture ETag, lifecycle state, content scope, target pair, and ACL policy
    in the eligibility record;
  - fail closed on invalid common/ACL combinations and any source with a
    changed or malformed state; and
  - preserve fail-closed ETag revalidation before response.
- Acceptance:
  - unknown, paused, retired, malformed, or changed sources fail closed;
  - no request enumerates unrelated sources.
- Evidence: point-read validation method and source schema invariants in the
  approved source contract.
- Review correction (2026-10-14): the first implementation left
  `SourcePolicyReader.capture()` optional and normal requests called it without
  `sourceId`, so the retained cross-partition enumeration path was still the
  production path.
- Result (2026-10-14): `sourceId` is now required by `QueryRequest`, propagated
  through standard and agentic retrieval, and required by policy capture. The
  enumeration query and fallback were removed. A request-level regression
  proves zero registry queries and exactly one point read for the selected
  source. Focused policy/request validation passed with 146 tests. The final
  complete retrieval run passed 701 tests and skipped 8 live-Cosmos tests; one
  unchanged 20 ms runtime-catalog timing test missed its synthetic deadline and
  passed immediately when rerun alone. The baseline Starlette dependency
  warning remains.

#### T10 - Resolve one retriever by container pair [P0]

- Status: DONE
- Depends on: T09
- Surfaces: `app/retrieval/cosmos_registry.py`, retrieval startup/state, tests
- Work:
  - replace `COSMOS_REGISTRY_JSON` fan-out with a lazy pair-keyed resolver;
  - create clients only from validated source container names; and
  - bound and close cached clients using existing application lifecycle
    patterns.
- Acceptance:
  - one request reaches exactly one pair;
  - sources sharing a pair reuse the retriever;
  - different pairs cannot cross-read;
  - missing containers and RBAC denial surface explicit dependency errors.
- Evidence: approved source registry routing authority and no fallback.
- Result (2026-10-14): removed startup `COSMOS_REGISTRY_JSON` parsing and
  source fan-out. The selected `SourceEligibility` now resolves one retriever
  lazily from its validated chunks/manifests pair. Pair-keyed caching reuses
  shared pairs and isolates different pairs; the registry reuses the
  lifecycle-owned Cosmos database client and closes/evicts cached retrievers
  with the existing application shutdown path. Resolution or RBAC failures
  surface as explicit retrieval dependency errors. Broad retrieval, evaluation,
  and lifespan validation passed with 726 tests, 8 live-Cosmos skips, and the
  baseline Starlette dependency warning.

#### T11 - Make ACL filtering a request policy [P0]

- Status: DONE
- Depends on: T09, T10
- Surfaces: `app/retrieval/main.py`, `service.py`, `cosmos.py`, auth/group tests
- Work:
  - resolve Graph groups only for a source whose policy is `true`;
  - reject `contentScope: common` with `retrievalAclEnabled: true`;
  - pass ACL IDs and ACL-filter mode per request;
  - always filter chunks by requested `sourceId`; and
  - remove the service-wide `ACL_ENABLED` behavior after cutover readiness.
- Acceptance:
  - business `true` enforces authorized group intersection;
  - `false` skips Graph and the ACL predicate but retains authentication,
    lifecycle, retrievability, and source filtering;
  - common accepts only `false`;
  - business `true -> false -> true` works without re-ingestion;
  - a policy ETag change before response fails closed.
- Evidence: current global flag and approved per-source policy table.
- Result (2026-10-14): the ACA request authenticates the gateway caller before
  source-policy evaluation, resolves Graph security groups only when the
  selected active source enables retrieval ACLs, and passes the captured policy
  snapshot through both standard and agentic retrieval. Cosmos receives an
  explicit per-request ACL mode: `true` requires principal groups and adds the
  group-intersection predicate; `false` omits both while retaining requested
  source, lifecycle, schema, and retrievability checks. Defensive validation
  rejects common/true snapshots. Regression coverage proves business
  `true -> false -> true` on the same cached retriever without re-ingestion.
  Focused validation passed 401 tests; broad retrieval, evaluation, and lifespan
  validation passed 788 tests with 8 live-Cosmos skips and the baseline
  Starlette dependency warning.

#### T12 - Add `sourceId` to all retrieval execution paths [P0]

- Status: DONE
- Depends on: T09, T10, T11
- Surfaces: `QueryRequest`, standard retrieval, agentic search tool, citations,
  audit records, and tests
- Work:
  - require and validate `sourceId` at `/api/query`;
  - pass the selected policy through standard and agentic paths; and
  - preserve request ID, citations, audit, timeouts, and failure semantics.
- Acceptance:
  - requests without/with invalid source IDs fail validation;
  - standard and agentic paths cannot fan out to unrelated sources;
  - citations belong only to the requested source.
- Evidence: both query paths currently share `RagService` and source-policy
  validation.
- Result (2026-10-14): audited the standard, agentic, agent-fallback, service,
  retriever, and citation-release paths. All production requests carry the same
  captured one-source policy snapshot, resolve one pair, apply the requested
  source filter in Cosmos, and revalidate attributed chunk source IDs before
  release. Added HTTP-boundary validation using the canonical source-ID
  contract, so whitespace-only, slash/backslash, control-character, and
  over-1,023-byte IDs return `422` instead of a dependency-shaped `503`.
  Added a service-level guard and regression proving a chunk attributed to an
  unrequested source is withheld before policy revalidation or citation
  release. Focused validation passed 402 tests; broad retrieval, evaluation,
  and lifespan validation passed 794 tests with 8 live-Cosmos skips and the
  baseline Starlette dependency warning.

### Phase 4: Direct ACA authentication

#### T13 - Replace gateway principal handling with delegated-user handling [P0]

- Status: DONE
- Depends on: T01
- Surfaces: `app/retrieval/auth.py`, `main.py`, auth tests
- Work:
  - validate ACA-injected tenant, audience, delegated scope, user identity, and
    `idtyp` when present;
  - generate the request ID in ACA;
  - remove required gateway context/request-ID headers; and
  - keep Graph resolution separate and conditional on source ACL policy.
- Acceptance:
  - valid delegated users succeed;
  - anonymous, app-only, wrong-tenant, wrong-audience, and missing-scope tokens
    fail;
  - application logs do not expose tokens or sensitive claims.
- Evidence: ACA built-in Entra authentication documentation and approved
  direct-user boundary.
- Review result (2026-10-14): the root cause remains the legacy gateway contract
  in `app/retrieval/main.py` and `app/retrieval/auth.py`: request auth currently
  requires `X-MS-CLIENT-PRINCIPAL` plus custom gateway headers and validates a
  Function service principal, not the direct user identity that ACA should apply at
  the public HTTP boundary. This is the remaining compatibility break before the
  direct-ACA cutover, and the design decision is to replace the gateway contract
  with delegated-user validation while preserving the current source-aware ACL and
  policy logic.
- Result (2026-10-14): ACA retrieval now accepts exactly one platform-injected
  `X-MS-CLIENT-PRINCIPAL`, validates canonical `oid`/`tid`, exact tenant and
  audience, delegated `user_impersonation`, and `idtyp=user` when present.
  App-only, anonymous, malformed, wrong-tenant, wrong-audience, and
  missing-scope callers fail closed. ACA generates and retains one request UUID
  for success and error responses instead of trusting Function-owned headers.
  Graph expansion remains separate and runs only after the selected source
  enables retrieval ACLs. Query logs no longer emit the encoded principal or
  user object ID. Focused auth/request validation passed 175 tests. Broad
  retrieval, evaluation, lifespan, and temporary Function-gateway validation
  passed 843 tests with 8 live-Cosmos skips and the baseline Starlette warning;
  T14 subsequently removed the temporary compatibility path.
  Microsoft documents that Container Apps built-in authentication can require
  authenticated traffic and expose claims for application authorization:
  [Authentication and authorization in Azure Container Apps](https://learn.microsoft.com/azure/container-apps/authentication).

#### T14 - Remove the Function query gateway [P0]

- Status: DONE
- Depends on: T13
- Surfaces: `app/function_app.py`, Function configuration, gateway tests
- Work:
  - remove the Function `/api/query` route and service-token acquisition;
  - remove the custom gateway-header proxy mechanism and retrieval URL/scope
    plumbing;
  - retain all ingestion routes and authentication unchanged.
- Acceptance:
  - Function has no query route or retrieval service credential;
  - ingestion route tests continue to pass;
  - direct ACA tests replace gateway proxy tests.
- Evidence: approved ACA-only query boundary.
- Result (2026-10-14): removed the Function `/api/query` route, managed-identity
  service-token call, custom gateway context/request-ID contracts, proxy
  timeout/URL/scope settings, retrieval gateway identity settings, and ACA
  Function-UAMI caller restrictions. Function ingestion routes and
  `Ingestion.Operator` authorization remain. Active documentation and local
  testing now describe delegated users calling ACA directly. Bicep compiled
  without contract warnings; 409 focused auth/config/IaC tests, 1,680
  ingestion/app-runtime/infrastructure tests, and 781 retrieval/evaluation
  tests passed. Eight live-Cosmos tests were skipped because integration
  credentials were absent; the existing Starlette dependency warning remains.
  Python compilation and `git diff --check` passed.

### Phase 4A: Mixed-permission ACL admission

The ingestion boundary must accept a file when at least one permission proves
a valid Entra security-group grant. Sharing links, direct users, Unified
groups, invalid groups, malformed identities, and unsupported roles are
structured observations; they do not grant retrieval access and do not
invalidate a separate verified group. Complete permission traversal and final
ACL revalidation remain mandatory.

#### T28 - Define the strict ACL admission and feedback contract [P0]

- Status: DONE
- Depends on: T14
- Surfaces: `app/ingestion/graph.py`, `app/ingestion/models.py`, model and
  Graph permission tests
- Work:
  - define `aclStatus` as `Pending` before evaluation and `Verified` only when
    canonical `allowedGroupIds` is non-empty;
  - define sorted unique `aclFeedback` values `DirectUser`, `InvalidGroup`,
    `InvalidIdentity`, `SharedLink`, `SiteGroup`, `UnifiedGroup`, and
    `UnsupportedRole`;
  - extend `VerifiedAcl` with feedback while retaining `aclHash` as the
    authorization hash of canonical verified groups and ACL policy version;
  - keep chunk authorization limited to `allowedGroupIds`; and
  - prohibit persisted/logged sharing URLs, tokens, emails, display names, and
    raw permission payloads.
- Acceptance:
  - strict model tests accept only known status/feedback values in canonical
    order;
  - `Verified` requires at least one valid group;
  - feedback cannot alter `allowedGroupIds` or grant retrieval access; and
  - existing clean group-only ACLs map to `Verified` with empty feedback.
- Evidence: approved file ACL admission design and Microsoft Graph permission
  facet documentation.
- Result (2026-10-14): added strict `Pending`/`Verified` ACL status and the
  seven approved typed feedback values, canonical feedback validation,
  document serialization/deserialization, and immutable `VerifiedAcl`
  feedback. `aclHash` remains group-only and chunks remain unchanged. Focused
  and affected ingestion validation passed 558 tests; Ruff import checks,
  Python compilation, and `git diff --check` passed. Full permission
  collection and lifecycle propagation remain T29 and T30 respectively.

#### T29 - Replace fail-fast permission parsing with collect-and-decide [P0]

- Status: DONE
- Depends on: T28
- Surfaces: `app/ingestion/graph.py`,
  `tests/ingestion/test_graph_discovery.py`, source connector tests
- Work:
  - traverse the complete Graph permission collection before deciding the
    file outcome;
  - independently collect groups from supported `read`, `write`, and `owner`
    grants, including groups resolved through SharePoint site groups;
  - verify that every accepted group exists, is security-enabled, and is not
    `Unified`;
  - collect feedback for non-authorizing permission entries instead of
    immediately rejecting the file; and
  - reject with a safe `NoSecurityGroup` outcome only after the canonical
    verified group set is empty.
- Acceptance:
  - valid group plus sharing link, direct user, Unified/invalid group,
    malformed identity, or unsupported role succeeds;
  - sharing-link-only, user-only, Unified-only, and invalid-group-only input
    fails because no verified group remains;
  - incomplete paging and invalid top-level responses fail closed;
  - throttling and dependency failures remain retryable dependency failures,
    not `NoSecurityGroup`; and
  - permission order does not change the result.
- Evidence: Microsoft Graph permission, sharingLink, SharePointIdentitySet,
  and effective driveItem permission documentation.
- Review/result (2026-10-14): T28 passed review with no must-fix finding.
  Permission evaluation now completes traversal, collects valid group and site
  group identities independently, records canonical feedback for links,
  direct users, invalid identities/groups, Unified groups, site groups, and
  unsupported roles, and decides only after Entra group verification.
  Link-only/user-only/invalid-only inputs still fail with
  `no_verified_security_groups`; pagination failure remains fail-closed and
  group throttling remains retryable. Permission ordering does not affect the
  result, link secrets are not retained, and group-only authorization/hash
  semantics are unchanged. Focused and affected ingestion validation passed
  604 tests; final connector checks passed 105 tests. Ruff import checks,
  Python compilation, and `git diff --check` passed.

#### T30 - Persist ACL feedback through ingestion and lifecycle flows [P0]

- Status: DONE
- Depends on: T29
- Surfaces: ingestion models/repositories, document and audio processing,
  winner reuse, delta, ACL resync, recovery, and lifecycle tests
- Work:
  - persist document-level `aclStatus` and `aclFeedback` in full-sync, delta,
    document, and audio paths without duplicating feedback on chunks;
  - compare the full `VerifiedAcl` on the final pre-publication reread;
  - require winner reuse to match group authorization and feedback;
  - update only document feedback when `allowedGroupIds` is unchanged;
  - update chunks and document authorization when groups change; and
  - retire as `acl_revoked` when the last verified group disappears, restoring
    only after a verified group returns under existing source-version guards.
- Acceptance:
  - admitted documents and chunks retain identical canonical group IDs;
  - feedback-only changes do not toggle chunk retrievability or lifecycle
    generation;
  - group changes use the existing guarded chunk update path;
  - no-group resync makes prior content non-retrievable; and
  - ACL changes during processing prevent stale publication.
- Evidence: existing admission, exact-chunk verification, winner reuse, and
  ACL-resync contracts.
- Result (2026-10-14): document and audio admission now persist verified ACL
  status and canonical feedback while chunks remain group-only. Full-sync
  reuse requires the current source version, canonical groups, ACL hash,
  status, and feedback to match. Ready/lifecycle projections carry feedback,
  interrupted group refreshes persist and recover pending feedback, and full
  authorization changes retain the guarded disable/update/enable chunk path.
  Feedback-only resync patches only the document and does not change chunk
  retrievability or lifecycle generation. Last-group loss and guarded
  restoration behavior remain unchanged. Validation passed 361 focused tests,
  all 1,544 ingestion/app-runtime tests, and Python compilation.

#### T31 - Prove mixed-permission ingestion and retrieval behavior [P0]

- Status: COMPLETE (2026-10-15)
- Depends on: T30
- Surfaces: ingestion unit/contract tests, lifecycle tests, retrieval tests,
  and one approved development SharePoint fixture
- Work:
  - cover direct group; group plus anonymous/organization/specific-people
    link; direct user plus group; site group; nested/dynamic/mail-enabled
    security group; duplicate group; and mixed invalid-entry cases;
  - cover link-only, user-only, Unified-only, disabled-only, and deleted-only
    rejection;
  - prove retrieval still requires caller-group intersection and never uses
    feedback as a principal; and
  - prove retry/fail-closed behavior for pagination and Graph dependency
    failures.
- Acceptance:
  - every approved decision-matrix row has a focused test;
  - a valid-group member retrieves a `SharedLink` document;
  - a caller outside `allowedGroupIds` cannot retrieve it even when the link
    scope is anonymous or organization; and
  - no sensitive link value appears in Cosmos records, logs, or test output.
- Evidence: approved ACL decision matrix and existing Cosmos ACL predicate.
- Result (2026-10-14): focused Graph tests now cover anonymous and
  organization links, specific-people links, direct users, site groups,
  dynamic and mail-enabled security groups, duplicate grants, Unified,
  disabled, deleted, malformed, and unsupported-role observations. They also
  preserve fail-closed permission paging/response behavior and retryable Graph
  failures. Retrieval tests prove that `aclFeedback: ["SharedLink"]` is never
  referenced by the authorization query and that only caller intersection
  with `allowedGroupIds` returns the document. A read-only live-Cosmos fixture
  check was added for the same member/non-member boundary and safe persisted
  metadata. Local validation passed 226 focused tests and 1,851 affected
  ingestion/retrieval tests; the live fixture check remains skipped because
  the required `RAG_INTEGRATION_COSMOS_*` and `RAG_T31_FIXTURE_*` settings are
  not configured. T31 must not be marked done until that approved development
  fixture passes. Live execution is currently blocked: on 2026-10-14,
  `rag-aca-e2e-20260827-func-jztu542h` rejected the current source-registration
  request as `source_registration_invalid`, and its source-list response
  exposed only the legacy source fields. Dedicated `poc-auth-demo` Cosmos
  document/chunk containers and retrieval-reader assignments are provisioned,
  but source registration and ingestion require deployment of the branch's
  current source-routing Function API first.
  On 2026-10-15, the current Function API was deployed and `poc-auth-demo`
  was registered paused, activated with its ETag, and run through its
  source-specific full-sync endpoint. The exact orchestration completed with
  `discovered=0`, `failed=0`, and no new work, but this was not evidence of
  failed Graph enumeration: Application Insights correlated successful
  app-only Graph calls for the registered drive, including `/root/delta`,
  `/root/children`, nested children, item reads, permission reads, and content
  requests. The Function trace also shows the dedicated
  `poc-auth-demo-source-documents` container already contained three records
  during discovery/list processing. The zero count is therefore consistent
  with unchanged-document reuse/skip behavior, not a missing SharePoint site
  grant. The configured app registration has Microsoft Graph application-role
  grants, and adding a redundant `Sites.Selected` site grant is not justified
  by the observed evidence.

  The inspect contract was then updated and deployed so the operator supplies
  a logical container and `sourceId`; the Function resolves only that accepted
  source's registered physical container and adds a `sourceId` predicate.
  Arbitrary physical container names and missing `sourceId` requests return
  `400`. Live inspection of `03-Information-Handling-Baseline.md` resolved
  `poc-auth-demo-source-documents` and proved one `ready`/`terminal` document
  with `aclStatus=Verified`, feedback `DirectUser`, `InvalidGroup`,
  `SharedLink`, and `SiteGroup`, and one `allowedGroupIds` security-group ID.
  Its three records in `poc-auth-demo-search-chunks` contain the same group ID
  and contain neither `aclStatus` nor `aclFeedback`. This proves persistence
  and confirms `SharedLink` is non-authorizing feedback.

  The final live boundary used real delegated tokens from the secretless public
  client `fb83912d-d432-4a58-885f-abc48362b313`, with ACA Authentication allowing
  only that client. It also exposed two stale runtime assumptions: the deployed
  image still required the retired Function gateway context header, and a
  successful Graph resolution with zero security-group memberships was treated
  as a dependency failure. Revision `0000021` runs the direct Easy Auth contract,
  accepts a valid empty membership set, and short-circuits it to zero evidence
  before Cosmos while retaining fail-closed Graph dependency behavior. Validation
  passed 238 focused retrieval tests.

  Against revision `0000021`, authorized member object ID
  `62fb01d3-a4c8-41fb-b572-9c294230ce23` received HTTP 200 and citations for
  `03-Information-Handling-Baseline.md` (request
  `348a0b94-358e-4aa8-80c5-33fefe7ba78a`). Enabled guest object ID
  `4ef07a3e-8f71-4aa8-8fb1-3041ca3cb140`, verified outside the only persisted
  allowed group, received HTTP 200 with no citations (request
  `64a6200c-7094-4564-8b3a-18a2a3e84a04`). Both tokens had the required API
  audience and `user_impersonation` scope; neither request synthesized principal
  headers. The sharing-link observation therefore did not authorize retrieval,
  and T31 is complete.

#### T32 - Update ACL operations and architecture documentation [P1]

- Status: COMPLETE (2026-10-15)
- Depends on: T31
- Surfaces: architecture, API/configuration, troubleshooting, local testing,
  and operator runbooks
- Work:
  - document the verified-group admission rule, feedback meanings, and
    non-authorizing nature of sharing links;
  - document dependency failure versus `NoSecurityGroup` outcomes;
  - document feedback-only resync and group-loss retirement behavior; and
  - remove statements that all sharing-link permissions are terminal.
- Acceptance:
  - docs match tested behavior and contain no sharing URL/token examples;
  - operators can distinguish accepted mixed permissions, rejected no-group
    files, and retryable Graph failures.
- Evidence: completed T28-T31 contracts and repository documentation
  conventions.
- Result (2026-10-15): architecture, API/configuration, troubleshooting, local
  multi-source testing, source evidence, and demo/runbook documentation now use
  the collect-and-decide ACL contract. They distinguish feedback from retrieval
  authority, no-verified-group terminal outcomes from dependency failures,
  feedback-only lifecycle updates from authorization changes, and authenticated
  no-evidence from authentication failure. Retired Function-gateway query
  guidance is identified as historical or unsupported for current acceptance,
  and ACL-disabled retrieval explicitly documents its whole-authenticated-
  audience exposure.

### Phase 4B: Local Azure authentication

The local application must authenticate with a dedicated service principal,
while deployed Function and ACA workloads retain their existing managed
identities. This phase changes local credential selection only; it does not
replace deployed workload identities or ACA delegated-user authentication.

#### T23 - Inventory Azure credential construction and developer fallback [P0]

- Status: COMPLETE (2026-10-15)
- Depends on: T32
- Surfaces: `app/cosmos_connection.py`, `app/config.py`,
  `app/retrieval/config.py`, Azure SDK clients, local-development
  documentation, and tests
- Work:
  - enumerate every Azure SDK client and credential factory used by local,
    Function, and ACA execution;
  - identify where `DefaultAzureCredential` can use Azure CLI, VS Code,
    PowerShell, shared-token-cache, or interactive credentials; and
  - record the existing managed-identity client IDs and RBAC assumptions that
    must remain unchanged in Azure.
- Acceptance:
  - every Azure resource client has an identified credential path per
    environment;
  - developer-identity fallback locations are listed; and
  - no deployed identity or RBAC contract is changed by this inventory.
- Evidence: repository credential construction plus Microsoft credential-chain
  documentation.
- Result (2026-10-15): inventory completed without changing identity, RBAC, or
  deployment configuration.

  | Execution path | Azure credential path | Resources/assumption |
  | --- | --- | --- |
  | Deployed Function | `DefaultAzureCredential(managed_identity_client_id=AZURE_CLIENT_ID)` | Existing Functions UAMI `rag-aca-e2e-20260827-functions-mi` (`1d1996e3-0bbb-4885-9d16-659764156741`) remains selected for Cosmos, Key Vault, Document Intelligence/Content Understanding, Language, OpenAI, Speech, Blob Storage, and telemetry; Graph and SharePoint then use the separate certificate loaded from Key Vault. |
  | Deployed ACA retrieval | `DefaultAzureCredential(managed_identity_client_id=MANAGED_IDENTITY_CLIENT_ID)` | Existing retrieval UAMI `rag-aca-e2e-20260827-aca-retrieval-mi` (`35aa6e31-613c-4abb-b2d9-ddfca35931e5`) remains selected for scoped Cosmos reads/audit write, OpenAI, Graph group resolution, ACR pull, and telemetry. |
  | Deployed catalog job | `ManagedIdentityCredential(client_id=MANAGED_IDENTITY_CLIENT_ID)` | Existing operations UAMI `rag-aca-e2e-20260827-operations-mi` (`5282bc8b-2569-47a8-b666-16576d23e80e`) is explicit and has no developer fallback; its ACR pull and scoped `retrieval-config` Cosmos permissions remain unchanged. |
  | Local Function and diagnostic tools | Unrestricted `DefaultAzureCredential()` or a constructor with an empty managed-identity client ID | Current local settings intentionally omit `AZURE_CLIENT_ID`; Azure SDKs can therefore select environment service-principal/workload credentials, shared token cache, VS Code, Azure CLI, Azure PowerShell, Azure Developer CLI, or Windows broker credentials. Interactive browser is excluded by default. |
  | Integration catalog/Cosmos tests | Unrestricted `DefaultAzureCredential()` | Environment-gated live tests and catalog-publish/SharePoint ACL tools can use the same developer fallback chain. |

  The local guide's Azure CLI sign-in is a supported baseline, not a technical
  guarantee that it is the selected identity. T24 defines one explicit,
  secret-safe service-principal contract for local work; T25 must implement it
  and prevent accidental fallback without altering the deployed Functions, ACA
  retrieval, or operations managed-identity and resource-scoped RBAC
  contracts. Microsoft documents the chain ordering and exclusions in
  [credential chains for Python](https://learn.microsoft.com/azure/developer/python/sdk/authentication/credential-chains)
  and [DefaultAzureCredential](https://learn.microsoft.com/python/api/azure-identity/azure.identity.defaultazurecredential).

#### T24 - Define the solo-development service principal access contract [P0]

- Status: COMPLETE (2026-10-15; contract only, no Azure mutation)
- Depends on: T23
- Surfaces: Azure app registration, resource/data-plane RBAC, Microsoft Graph
  application roles, and local setup documentation
- Work:
  - define one single-tenant app registration/service principal for this
    developer's local application execution across approved non-production
    development environments;
  - keep infrastructure deployment and role-assignment administration outside
    the local application principal; and
  - assign only the capability-specific data-plane roles required by the local
    workflow being exercised.
- Acceptance:
  - the contract grants no Azure control-plane role, including Owner, User
    Access Administrator, or Contributor;
  - each required data-plane permission is explicit and resource-scoped where
    supported;
  - no client secret is committed, deployed, or written to shared repository
    configuration; and
  - the contract distinguishes local development access from Function/ACA
    managed-identity access.
- Evidence: Microsoft Cosmos RBAC guidance, Azure RBAC least-privilege
  guidance, and the existing `infra/modules/*rbac.bicep` contracts.
- Decision:
  - use one generic single-tenant app registration named
    `developer-local-azure-access` and its tenant service principal for this
    developer's local application authentication across approved development
    projects. Reuse is limited to non-production resources. Each project must
    assign only its own capability-specific roles at its own resource scopes,
    and removing one project must remove that project's assignments without
    affecting the others;
  - use a client secret because this is an attended local-workstation scenario
    without a verified OIDC workload issuer. Microsoft permits client secrets
    for local development but states that certificates or federated
    credentials are more secure and must be used for production. The secret
    must expire in less than 12 months and may be stored in the developer's
    Git-ignored `app\local.settings.json` for local development only. It must
    never be committed, deployed, placed in deployment parameters, copied to
    user/machine-wide environment variables, exposed in shell history or logs,
    or sent to Azure-hosted application settings;
  - T25 must use an explicit `AZURE_AUTH_MODE` contract:
    `service_principal` for local execution and `managed_identity` for deployed
    Azure workloads. Local mode constructs `ClientSecretCredential` directly
    from the complete `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and
    `AZURE_CLIENT_SECRET` tuple. Azure mode selects the existing workload's
    configured user-assigned managed identity and never requires or receives
    the local secret. A missing/unknown mode or incomplete mode-specific
    configuration is a clear startup error; no `DefaultAzureCredential` or
    developer-identity fallback is allowed;
  - grant no Azure control-plane role to this principal. `infra/main.bicep`
    and `scripts/deploy.ps1` deploy at resource-group scope, but the deployment
    also manages RBAC assignments. Contributor cannot grant those assignments,
    while Owner or User Access Administrator would make the local runtime
    credential privileged. An independently authenticated and approved human
    or deployment identity must provision resources, grant roles, and perform
    Microsoft Graph admin consent;
  - do not add redirect URIs, delegated permissions, an exposed API, or
    SharePoint application permissions. SharePoint ingestion continues to use
    the existing certificate application loaded from Key Vault. Local
    retrieval needs only the Microsoft Graph application roles listed below;
    and
  - role assignment does not bypass private endpoints, firewalls, VPN, or
    private DNS. The workstation must independently satisfy the approved
    network boundary.

  Assign only the rows required by the local workflow. Every Azure role uses
  its immutable role ID and the narrowest existing development resource,
  database, container, vault, account, or component scope.

  | Capability | Explicit grant | Scope and constraint |
  | --- | --- | --- |
  | Inspect or retrieve source data | Cosmos DB Built-in Data Reader (`00000000-0000-0000-0000-000000000001`) | `ingestion-sources`, `ingestion-runs`, `retrieval-config`, and only the registered document/chunk container pairs exercised locally. |
  | Write ingestion state/content | Cosmos DB Built-in Data Contributor (`00000000-0000-0000-0000-000000000002`) | Only `ingestion-runs` and the registered document/chunk pairs used by an explicitly approved local ingestion run; retain reader-only access to `ingestion-sources`. |
  | Write service audit | Existing custom audit create/read role | Only `service-audit`; do not substitute account/database-level Data Contributor. |
  | Publish retrieval catalog | Existing catalog bootstrap, editor, or guarded-writer custom role matching the operation | Only `retrieval-config`; do not grant a broader Cosmos contributor role. |
  | Generate embeddings or answers | Cognitive Services OpenAI User (`5e0bd9bd-7b93-4f28-af87-19fc36ad61bd`) | Existing development Azure OpenAI account only. |
  | Load the SharePoint certificate | Key Vault Secrets User (`4633458b-17de-408a-b874-0445c86b69e6`) | Existing SharePoint-certificate development vault only; ingestion still authenticates to Graph/SharePoint as the certificate application, not this local principal. |
  | Resolve retrieval-user groups | Microsoft Graph `User.Read.All` (`df021288-bdef-4463-88db-98f22de89214`) and `GroupMember.Read.All` (`98830695-27a2-44f7-8c18-0c3ebc9698f6`) application roles | Local service principal only, tenant admin consent required. The endpoint queries another user's transitive memberships and reads group security/type properties. |
  | Document Intelligence or Language | Cognitive Services User (`a97b65f3-24c7-4388-baec-2e87135dc908`) | Each enabled development resource separately. |
  | Content Understanding | Cognitive Services Content Understanding Contributor (`59a2dba3-6303-4fd8-9a2e-8cbb4bdda972`) | Enabled development resource only. |
  | Speech transcription | Cognitive Services Speech User (`f2dc8367-1007-4938-bd23-fe263f013447`) | Enabled development Speech resource only. |
  | Stage audio | Storage Blob Data Contributor (`ba92f5b4-2d11-453d-a403-e96b0029c9fe`) | Dedicated development audio-staging storage account only; Blob Data Owner is unnecessary. |
  | Publish application metrics | Monitoring Metrics Publisher (`3913510d-42f4-4e42-8a64-420c390055eb`) | Development Application Insights component only and only when local Azure telemetry is enabled. |

  The Key Vault and Microsoft Graph grants are high-impact conditional
  capabilities: reading the vault secret exposes the development SharePoint
  application's private certificate, and Graph application roles are
  tenant-wide rather than Azure-resource-scoped. Omit both for Cosmos-only
  inspection. Grant them only for an approved ingestion or ACL-retrieval test,
  never against production resources, and remove them when that local
  capability is no longer required.

  Reusing this generic principal increases blast radius because one credential
  can reach every development project that has assigned it a role. The
  compensating boundary is mandatory: no production assignments, no
  control-plane roles, separate project-scoped role assignments, periodic
  assignment review, and prompt removal of stale project grants. If those
  controls cannot be maintained, replace the shared principal with one app
  registration per project rather than broadening its access.

  Azure AI Search is excluded because no Search SDK client or search resource
  exists in the current runtime. Azure Function host Blob, Queue, and Table
  roles are excluded because approved local hosting uses Azurite. Broad
  subscription/resource-group Reader or Contributor roles are also excluded
  because endpoints and resource IDs are supplied as configuration and
  resource management belongs to the separate deployment boundary.

  Microsoft evidence:
  - [Azure RBAC best practices](https://learn.microsoft.com/azure/role-based-access-control/best-practices)
    requires the least privilege at the narrowest practical scope.
  - [Local Python authentication with service principals](https://learn.microsoft.com/azure/developer/python/sdk/authentication/local-development-service-principal)
    documents the three environment variables and direct
    `ClientSecretCredential` construction.
  - [Microsoft Entra application credentials](https://learn.microsoft.com/entra/identity-platform/how-to-add-credentials)
    permits secrets for local development, recommends expiration below 12
    months, and rejects secrets for production.
  - [Microsoft Graph transitive membership permissions](https://learn.microsoft.com/graph/api/user-list-transitivememberof)
    lists `User.Read.All` as the least-privileged application permission for
    another user's memberships; the existing `graph-rbac.bicep` additionally
    grants `GroupMember.Read.All` so the resolver can read the selected group
    properties rather than limited-information objects.
  - [Key Vault Azure RBAC guidance](https://learn.microsoft.com/azure/key-vault/general/rbac-guide)
    identifies Key Vault Secrets User as the role that reads secret contents
    and recommends vault-level assignment for an application/environment.
- Result (2026-10-15): the reusable local runtime identity, credential
  lifecycle, explicit local/Azure mode boundary, provisioning boundary,
  network boundary, and capability-scoped permissions are defined. Correctly
  configured local execution must use the generic service principal;
  correctly configured deployed Function and ACA workloads must continue to
  use their existing managed identities. No app registration, secret, admin
  consent, Azure role assignment, or deployment was created or changed.

#### T25 - Implement environment-safe local and Azure credential selection [P0]

- Status: COMPLETE (2026-10-15)
- Depends on: T23, T24
- Surfaces: shared Azure credential helper and all Azure SDK client factories
- Work:
  - require `AZURE_AUTH_MODE=service_principal` locally and accept
    `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`;
  - require `AZURE_AUTH_MODE=managed_identity` for Azure-hosted Function and
    ACA workloads and retain their existing user-assigned identity client IDs;
  - explicitly exclude Azure CLI, VS Code, PowerShell, shared-token-cache, and
    interactive developer credentials; and
  - fail with a clear configuration error when local service-principal
    variables are incomplete instead of silently falling back.
- Acceptance:
  - correctly configured local execution starts and selects the service
    principal;
  - correctly configured Azure-hosted execution starts and selects the
    existing managed identity;
  - missing, unknown, or incomplete mode-specific configuration fails clearly
    and never selects a developer identity; and
  - no Azure deployment receives `AZURE_CLIENT_SECRET`.
- Evidence: Microsoft Azure Identity credential-chain documentation and the
  repository's existing managed-identity deployment contract.
- Result (2026-10-15):
  - added one shared credential factory that constructs only
    `ClientSecretCredential` for `AZURE_AUTH_MODE=service_principal` or
    `ManagedIdentityCredential` for `AZURE_AUTH_MODE=managed_identity`;
  - missing, unknown, or incomplete mode-specific configuration raises a safe
    configuration error before any Azure SDK client is created;
  - wired Function ingestion clients, retrieval, Cosmos helpers, local
    SharePoint/catalog tools, and live Cosmos integration tests to the shared
    factory; the private operations job retains its already-explicit
    `ManagedIdentityCredential`;
  - made retrieval's managed-identity client ID optional in local
    service-principal mode and retained exact user-assigned identity selection
    in Azure mode;
  - set deployed Function and retrieval configuration to
    `AZURE_AUTH_MODE=managed_identity` without adding
    `AZURE_CLIENT_SECRET`; and
  - verified no executable `DefaultAzureCredential` construction remains
    under `app/`, `tools/`, or `tests/`.
- Validation (2026-10-15):
  - 230 focused credential, Function-client, inspect/purge, retrieval
    lifecycle, and configuration tests passed;
  - 70 credential/configuration/Bicep contract tests passed after adding
    explicit local and deployed-mode assertions;
  - 137 tool, operations-job, and infrastructure/deployment tests passed; and
  - 1,136 broad non-live application-runtime and retrieval tests passed with
    one pre-existing Starlette/httpx deprecation warning.
- Review (2026-10-15):
  - Azure/security review found no credential-selection or managed-identity
    continuity defect, but identified stale Azure CLI fallback instructions as
    release-blocking operational contradictions;
  - aligned the local, multi-source, catalog-operation, and configuration
    references with explicit service-principal/managed-identity modes; and
  - added tool-boundary regression tests. All 48 focused remediation tests
    passed, and no secret value was added to source, documentation, Bicep, or
    tracked settings.

#### T26 - Update local authentication documentation and secret handling [P1]

- Status: COMPLETE (2026-10-15)
- Depends on: T24, T25
- Surfaces: `docs/LOCAL_DEVELOPMENT.md`, `docs/CONFIGURATION.md`,
  `docs/AZURE_SETUP.md`, local environment examples, and troubleshooting
- Work:
  - document app registration, role assignment, environment variables, secret
    expiry, rotation, and local-only storage;
  - remove instructions that imply Azure CLI or developer identity is a valid
    application credential; and
  - document that Azure deployments use managed identity and do not receive
    the local secret.
- Acceptance:
  - a new developer can configure the local service principal without a
    developer login;
  - no secret appears in tracked files or command examples; and
  - documentation clearly separates local service access from delegated ACA
    user authentication.
- Evidence: repository documentation conventions and Microsoft local service
  principal guidance.
- Result (2026-10-15):
  - documented the environment-owner preparation and secure handoff for the
    generic single-tenant development app without requiring a developer portal
    or Azure CLI login;
  - documented the approved plaintext `app\local.settings.json` exception for
    non-production local development, Git-ignore verification, terminal
    isolation, and deterministic cleanup after local processes stop;
  - documented expiry below 12 months, owner/credential metadata, an
    overlap-test-switch-revoke rotation sequence across every recorded
    development consumer, and immediate revocation for suspected exposure;
  - aligned configuration, architecture, multi-source, catalog-operation, and
    troubleshooting references with direct local `ClientSecretCredential` and
    deployed `ManagedIdentityCredential` behavior;
  - preserved the separation from delegated ACA user authentication and
    prohibited production assignments, Azure control-plane roles, tracked
    secrets, and local-secret deployment; and
  - grounded the lifecycle in [Microsoft Entra credential guidance](https://learn.microsoft.com/entra/identity-platform/how-to-add-credentials),
    [application registration security practices](https://learn.microsoft.com/entra/identity-platform/security-best-practices-for-app-registration),
    and [Python local service-principal authentication](https://learn.microsoft.com/azure/developer/python/sdk/authentication/local-development-service-principal).
  - Evidence boundary: the application owner, credential expiry, and
    Azure/Graph assignments must be verified before live testing. This plan
    does not require an enterprise secret-manager record for the approved
    non-production `local.settings.json` exception.
- Validation (2026-10-15):
  - scoped contradiction searches found no instruction to use Azure CLI,
    cached developer credentials, or the deployed managed-identity settings as
    the local application credential;
  - secret-pattern searches found no credential value in the aligned
    documentation;
  - local setup, cleanup, catalog-operation, and multi-source PowerShell
    examples parsed successfully; and
  - Markdown whitespace and `git diff --check` validation passed for every
    aligned documentation surface.

#### T27 - Validate both credential paths and reject developer fallback [P0]

- Status: COMPLETE (2026-10-15; local.settings.json exception approved)
- Depends on: T25, T26
- Surfaces: credential unit tests, configuration tests, local smoke checks, and
  deployed Azure readiness checks
- Work:
  - test complete local service-principal configuration;
  - test missing and partial local variables fail explicitly;
  - test that CLI/VS Code/shared-token-cache credentials are excluded;
  - test Azure-hosted managed-identity selection with the existing identity
    contract; and
  - run a non-secret local Azure SDK smoke check against the approved
    development resources.
- Acceptance:
  - local success uses the service principal;
  - local failure never falls through to a developer identity;
  - deployed Function and ACA configuration retains their existing managed
    identities and excludes the local service-principal secret; live startup
    under the newly emitted explicit mode is validated with the corresponding
    infrastructure deployment rather than requiring a T27 deployment; and
  - no secret is emitted in logs, test output, or diagnostics.
- Evidence: credential helper tests plus the existing infrastructure and
  runtime authentication tests.
- Validation (2026-10-15):
  - the local process loaded the user-provided secret without printing it and
    `create_azure_credential()` returned `ClientSecretCredential`;
  - the container-scoped principal read `4` records from
    `rag-db/ingestion-sources`;
  - the same client received HTTP `403` with an RBAC authorization signal and
    no firewall signal for unassigned `rag-db/retrieval-config`;
  - the authorized temporary `106.192.217.137` firewall entry was removed in
    cleanup, restoring the original sole rule `106.192.223.144`;
  - all `10` focused credential tests passed, including missing, partial, and
    no-developer-fallback cases; and
  - live read-only inspection confirmed both deployed workloads retain
    user-assigned identities and no `AZURE_CLIENT_SECRET`; their current
    revisions predate the T25 `AZURE_AUTH_MODE` setting, so explicit-mode
    startup remains a deployment-time validation for T15/T17.
- Review (2026-10-15):
  - the user explicitly approved plaintext `app\local.settings.json` for local
    development. This is a documented security exception, not an enterprise
    secret-manager implementation; the OneDrive/plaintext exposure risk remains
    accepted for this non-production developer workflow;
  - accepted evidence: the explicit `ClientSecretCredential` path, assigned
    container read, unassigned-container RBAC denial, narrow Cosmos role
    assignment, firewall restoration, and `130` passing credential and
    infrastructure contract tests are valid;
  - accepted evidence boundary: Function and ACA Bicep emit
    `AZURE_AUTH_MODE=managed_identity`, exclude `AZURE_CLIENT_SECRET`, and
    retain their user-assigned identities. Their current deployed revisions
    predate that setting, so live explicit-mode startup remains a
    deployment-time check; and
  - exit criteria met: the developer deleted the temporary secret file, the
    local service-principal proof passed, and the Cosmos firewall was restored.
    Future local setup may use only the ignored `app\local.settings.json`;
    T15 is no longer blocked by secret storage, but production and deployed
    workloads must continue using managed identity and no client secret.

### Phase 5: Azure infrastructure

#### T15 - Provision the approved dev content topology [P0]

- Status: COMPLETE AND REVIEW APPROVED (2026-10-16)
- Depends on: T27
- Surfaces: `infra/modules/cosmos.bicep`, `infra/main.bicep`, infra tests
- Work:
  - declare the three approved chunks/manifests pairs in the existing account
    and database;
  - reproduce required partition, indexing, vector, and full-text policies;
  - keep shared control containers unchanged; and
  - expose only the container names required by RBAC and deployment wiring.
- Acceptance:
  - Bicep compiles;
  - all six content containers have the approved policies;
  - no new Cosmos account/database or dynamic app-side provisioning exists.
- Evidence: existing container definitions and approved development topology.
- Result (2026-10-16):
  - provisioned the approved Business A, Business B, and Common chunks/manifests
    pairs in the existing `rag-db` database;
  - extracted the source-specific topology into
    `cosmos-content-topology.bicep` so T15 can be previewed and deployed without
    modifying the wider foundation deployment;
  - added a guarded `ContentTopology` deployment phase with the existing plan,
    source-tree, subscription, tenant, resource-group, and azd-environment
    authority checks; and
  - preserved the shared control containers, legacy shared content containers,
    and the existing `poc-auth-demo` fixture. T16 remains responsible for
    workload RBAC on the new pairs.
- Validation (2026-10-16):
  - the topology module, parameter file, and root Bicep compiled successfully;
  - all `120` focused Bicep and deployment-controller tests passed;
  - the bounded pre-deployment what-if contained exactly six creates and no
    modifies, deletes, or replacements;
  - live inspection verified `/sourceRunId` on all three manifest containers,
    `/documentKey` plus DiskANN/vector and full-text policies on all three chunk
    containers, and the required manifest indexes;
  - the account and database were reused, local authentication remains
    disabled, and all four shared control containers remain present; and
  - the post-deployment what-if reports only service-normalized properties on
    the six new containers. Direct resource inspection confirms the declared
    application-relevant policies, so those provider-default deltas are an
    explicit Azure what-if evidence boundary rather than missing topology.
- Review (2026-10-16): APPROVED
  - no must-fix or should-fix T15 finding remains;
  - deployment operations succeeded on exactly the six intended Cosmos
    container resources, with no account, database, ACA, identity, or RBAC
    operation;
  - no application or tool path dynamically creates Cosmos containers;
  - no Cosmos SQL role assignment targets a new content-container scope, so
    T16 remains independently reviewable;
  - two owning contract tests passed again during review; and
  - consider reducing the `ContentTopology` controller's dependency on shared
    environment preparation when that controller is next changed. This is not
    a T15 correctness blocker because the guarded deployment and all target
    checks completed successfully in the approved environment.

#### T16 - Apply least-privilege data-plane RBAC per pair [P0]

- Status: COMPLETE (2026-10-16)
- Depends on: T15
- Surfaces: `cosmos-content-rbac.bicep`, `main.bicep`, bounded deployment
  controller, and infrastructure tests
- Work:
  - give the Function identity its required content read/write permissions;
  - give the retrieval identity read access to content and retain only required
    control/audit permissions; and
  - validate assignments in infrastructure tests and runtime readiness probes
    for each identity's own permissions.
- Acceptance:
  - both identities can perform only their required operations on every pair;
  - missing assignments fail deployment validation or source readiness;
  - account keys and connection strings remain disabled.
- Evidence: [Azure Cosmos DB data-plane security](https://learn.microsoft.com/azure/cosmos-db/nosql/security-data-plane)
  defines built-in Data Reader and Data Contributor roles and container-level
  scopes; existing scoped role modules provide the repository convention.
- Result (2026-10-16):
  - added one independently deployable `cosmos-content-rbac.bicep` module as
    the authoritative owner of new-content access, leaving legacy-content,
    control-container, and audit assignments unchanged;
  - made `content-container-pairs.json` the single compile-time manifest
    consumed by both topology and RBAC modules;
  - assigned the Function identity the built-in Data Contributor role and the
    retrieval identity the built-in Data Reader role on each of the six
    approved content containers; and
  - added a guarded, preview-first `ContentRbac` controller phase that resolves
    exactly one tagged Cosmos account, Function App, and retrieval Container
    App, resolves the expected workload UAMIs by deterministic deployment
    names, and fails if either workload is not attached exclusively to its
    expected identity.
- Validation (2026-10-16):
  - the content-RBAC module, its parameter file, and the root Bicep template
    compiled;
  - three focused infrastructure/deployment-controller tests passed;
  - the bounded what-if contained exactly 12 creates and 63 ignores, with no
    modifies, deletes, or replacements; and
  - deployment `cosmos-content-rbac` succeeded. Live ARM inspection confirmed
    six Function Data Contributor and six retrieval Data Reader assignments,
    all on the exact six container scopes, with no unexpected principal or
    broader content scope.
- Review (2026-10-16): APPROVED AFTER REMEDIATION
  - the remediated guarded preview reported 12 `NoChange` assignments and no
    RBAC mutation, proving idempotence with the exact expected UAMIs;
  - a temporary private ACA job used those two workload identities to test all
    six containers: Function read/create/read/delete passed, retrieval read
    passed, and retrieval create returned 403 on every scope;
  - post-delete point reads returned 404 for every temporary probe record; and
  - the temporary job and its deployment record were deleted, the live matrix
    remains six contributors plus six readers, and all 150 infrastructure
    tests pass.

#### T17 - Recreate ACA as an external-access environment [P0]

- Status: COMPLETE — IMPLEMENTED, VALIDATED, AND REVIEW-APPROVED (2026-10-16)
- Depends on: T13
- Surfaces: `aca-environment.bicep`, `aca.bicep`, `main.bicep`, focused
  environment/serving compositions, deployment controller, and infra tests
- Work completed:
  - replaced the exact internal environment with an external,
    VNet-integrated environment while preserving the dedicated subnet;
  - retained HTTPS-only ingress, direct Entra Easy Auth, the retrieval API
    audience, and the approved native/public caller restriction;
  - removed only the obsolete ACA-domain private DNS zone while preserving all
    downstream private DNS zones;
  - bound recovery and serving to the reviewed ACR, repository, and immutable
    image digest; and
  - exposed the public ACA base/query URLs without a Function, APIM, Front
    Door, or IP-allowlist proxy boundary.
- Validation (2026-10-16):
  - subscription feature `Microsoft.Network/AllowBringYourOwnPublicIpAddress`
    was registered because the live RP required it for this external,
    VNet-integrated environment;
  - environment `rag-aca-e2e-20260827-retrieval-jztu542h-env` succeeded with
    `internal: false`, public IP `20.85.52.52`, and the existing ACA subnet;
  - private catalog execution `ragacae2e20260827-catalog-job-1fpti3q`
    succeeded with the approved `7a7519…` image and returned one catalog
    digest/ETag, proving Cosmos/private-network access;
  - retrieval revision `ragacae2e20260827-retr-jztu542h--tjj1spb` is healthy
    and running at the public ACA FQDN;
  - exact Easy Auth readback passed, anonymous `POST /api/query` returned 401,
    and DNS resolved to the public environment IP;
  - the temporary catalog job and old ACA private DNS zone are absent; and
  - all 151 infrastructure tests and `git diff --check` passed.
- Review (2026-10-16): APPROVED FOR GUARDED EXECUTION
  - Azure review cleared exact-resource cleanup, resumability, immutable-image
    recovery, issuer separation, ownership checks, auth readback, dependency
    proof, and the final live deployment.
- Evidence: Microsoft Container Apps networking and authentication
  documentation plus the live ARM, job, revision, DNS, HTTP, and test results.

#### T18 - Remove legacy routing and gateway configuration [P0]

- Status: DONE (2026-10-16)
- Depends on: T07, T11, T14, T16, T17
- Surfaces: application settings, Bicep parameters/outputs, tests, deployment
  configuration
- Work:
  - remove `ACL_ENABLED`, `COSMOS_REGISTRY_JSON`, fixed content-container
    routing, Function retrieval settings, and gateway role assignment; and
  - retain shared control-container and downstream dependency settings.
- Acceptance:
  - repository search finds no runtime use of legacy settings;
  - deployment tests reject reintroduction of gateway/fallback configuration.
- Evidence: approved clean cutover with no dual authority.
- Result (2026-10-16):
  - removed the deployment-wide `ACL_ENABLED` parameter, environment variable,
    application setting, and retrieval fallback; accepted
    `SourceEligibility.retrieval_acl_enabled` is now the only request ACL-filter
    authority;
  - removed fixed chunks/manifests environment settings from Function and ACA
    configuration; ingestion receives its pair only from an accepted
    `SourcePlan`, and retrieval resolves its pair only from accepted
    `SourceEligibility`;
  - confirmed `COSMOS_REGISTRY_JSON`, the Function query gateway, and its
    service-to-service app-role assignment were already absent, and retained
    only shared control/audit container settings and direct delegated-user ACA
    authentication;
  - added deployment guards that reject reintroduction of the global ACL or
    fixed content-container settings and updated directly affected
    configuration, architecture, demo, and readiness documentation;
  - `398` focused retrieval/runtime/infra tests and `239` ingestion-binding,
    cache, source-contract, and no-query-gateway tests passed;
  - main Bicep compiled successfully outside the OneDrive workspace, fatal
    Ruff checks passed, and `git diff --check` passed.

### Phase 6: Business metadata contract

#### T33 - Add `businessName` to source, content, and audit metadata [P0]

- Status: DONE (source contract, management APIs, ingestion work-validity
  semantics, content snapshots, repository write-boundary enforcement,
  ingestion and retrieval audit propagation, architecture/data-contract
  documentation, focused regression tests, and final review complete)
- Depends on: T17 review and the completed schema-v1 source contract
- Surfaces:
  - shared `SharePointSource` parser and `SourcePlan` serialization;
  - ingestion source-plan work-validity comparisons;
  - source registration and ETag-protected PATCH API;
  - `source_document` and `visual_manifest_page` models and write paths;
  - retrieval `SourceEligibility`, selected-source capture, and semantic
    revalidation;
  - source-bound ingestion audit items, retrieval query usage items, and the
    retrieval `query_request` summary;
  - focused source-contract, ingestion-model, API, repository, retrieval-audit,
    and documentation tests.
- Requirement boundary:
  - `businessName` is required for every newly recreated source because the
    approved development reset has no compatibility reader;
  - the same non-empty, trimmed validation applies to both `business` and
    `common`; neither scope imposes or reserves a label value;
  - the label is ETag-protected mutable metadata, not source identity, routing,
    container selection, lifecycle, or authorization policy;
  - content and audit items keep accepted-context snapshots; renaming a source
    does not bulk-update historical records or mutate in-flight source plans.
- Work:
  1. Add required `businessName` to the exact schema-v1 `config` shape, source
     registration request/response, `SharePointSource`, and `SourcePlan`.
  2. Validate a trimmed, non-empty label of at most 200 characters, as approved
     for this API contract; reject missing, blank, non-string, or oversized
     values with one stable source-contract error.
  3. Extend `PATCH /api/ingestion/sources/{sourceId}` with an `If-Match`
     protected, single-field `businessName` update for either content scope.
     Write old/new label context to the existing best-effort source audit path.
  4. Exclude `businessName` from `source_binding_digest`. Replace complete-plan
     equality at ingestion work-validity gates with an explicit comparison of
     expected lifecycle state and immutable binding digest, preserving
     fail-closed behavior for state or binding changes while allowing label and
     retrieval-policy updates.
  5. Carry the accepted label through the immutable source plan and persist it
     at write time on both record types stored in the configured
     `source-documents` container: `source_document` and
     `visual_manifest_page`.
  6. Add `businessName` to retrieval `SourceEligibility` and carry it in the
     selected-source snapshot. Revalidate lifecycle, scope, container pair, and
     ACL policy before response; retain the catalog ETag as audit evidence but
     exclude both ETag and label from semantic equality.
  7. Add the accepted-context label to every source-bound ingestion audit item,
     retrieval query usage item, and `query_request` summary. Add
     `source_registered` and `source_business_name_changed` management events;
     the rename event records old and new labels. Keep `sourceId` as the stable
     correlation key and leave global catalog events unchanged.
  8. Leave existing content and audit records unchanged after a rename.
     Already accepted ingestion plans keep their captured label; plans and
     requests accepted after the update use the new label.
  9. Update the directly affected architecture, API, and data-contract
     documentation after focused tests pass.
- Acceptance:
  - registration rejects a missing, blank, non-string, or oversized
    `businessName` for either scope and accepts arbitrary valid labels for both;
  - source reads, registration responses, and PATCH responses return the
    accepted label and ETag;
  - an ETag-protected rename succeeds for `business` and `common`, while stale
    ETags and multi-field policy/binding changes fail;
  - identical bindings with different labels produce the same binding digest;
  - label-only and retrieval-policy changes do not invalidate accepted
    ingestion work, while lifecycle and immutable-binding changes remain fail
    closed;
  - label-only changes do not invalidate retrieval results, while lifecycle,
    container, scope, and ACL-policy changes retain their existing retrieval
    fail-closed behavior;
  - every new `source_document` and `visual_manifest_page` item contains the
    source label captured when written;
  - source registration/rename events, all source-bound ingestion audit items,
    retrieval query usage items, and the `query_request` summary contain
    accepted-context `sourceId` and `businessName`; historical snapshots are
    not rewritten after rename;
  - label creation or mutation does not change container resolution,
    `contentScope`, `retrievalAclEnabled`, ACL predicates, source lifecycle, or
    retrievability;
  - focused tests pass before T19 or cutover work begins.
- Evidence:
  - [BUSINESS_CONTAINER_ROUTING.md](BUSINESS_CONTAINER_ROUTING.md) defines the
    approved separation between business metadata, routing, and ACL policy.
  - `app/source_registry.py` currently enforces an exact config shape and
    computes a binding digest from routing fields, identifying the authoritative
    contract and the field that must remain excluded from identity.
  - `app/ingestion/source_registry.py` compares complete ETag-bearing plans at
    work-validity gates, so T33 must introduce a narrow semantic comparison
    rather than remove consistency validation.
  - `app/retrieval/source_policy.py` compares complete `SourceEligibility`
    values, including catalog ETag, so T33 must separate audit evidence from
    lifecycle/routing/ACL validity.
  - `app/ingestion/models.py` stores both `source_document` and
    `visual_manifest_page` records in the source-documents boundary, so both
    must carry the label to satisfy container-level metadata consistency.
  - `app/ingestion/telemetry.py` and `app/retrieval/telemetry.py` already own
    best-effort audit persistence; extending the enumerated source-context
    records keeps audit metadata in its owning boundary.
- Result (2026-10-16):
  - added required, trimmed `businessName` parsing with the approved
    200-character maximum and a stable `source_businessName_invalid` error;
  - added the normalized label to `SharePointSource` and durable `SourcePlan`
    payloads without adding it to `source_binding_digest`; and
  - added focused parsing, common-scope, payload round-trip, and digest
    independence coverage; `46` focused source-contract tests passed.
  - added required normalized registration input/response/persistence and
    exact-retry conflict detection for a changed business label;
  - added single-field ETag-protected label PATCH with explicit validation,
    missing-source/stale-ETag/error mapping, and normalized Cosmos persistence;
  - added best-effort `source_registered` and
    `source_business_name_changed` audit events, including previous/new labels
    for a rename; and
  - `64` selected contract/repository tests and all `96` source-route tests
    passed; focused lint and fatal-error lint also passed.
  - replaced whole-plan and catalog-ETag work-validity checks with one semantic
    comparison of source ID, expected lifecycle state, and immutable binding
    digest at coordinator, maintenance, full-sync commit, and retirement-latch
    gates;
  - retained exact plan equality for source acceptance, Cosmos `If-Match`
    concurrency, and catalog ETags as persisted evidence; and
  - all `103` source-registry tests passed, including label/policy continuity
    and lifecycle/binding fail-closed regressions; fatal-error lint and scoped
    diff checks passed.
  - added an optional, validation-protected `businessName` snapshot to new
    `source_document` and `visual_manifest_page` records; the field is omitted
    for pre-existing records so the compatibility reader does not rewrite or
    reject history;
  - bound the accepted source-plan label to ingestion runtime configuration and
    inherited it from each document into its derived visual-manifest pages;
  - excluded the label from the processing-profile digest because it is
    accepted audit context rather than a processing behavior; and
  - `541` affected ingestion tests passed, including document/page snapshots,
    legacy document reads, and profile-digest independence; fatal-error lint
    and scoped diff checks passed.
  - extended the centralized best-effort ingestion audit writer with explicit
    accepted-context `businessName` while preserving omission for global events
    without a selected source;
  - propagated document snapshots through extraction, enrichment, and embedding
    audits, and source-plan/config snapshots through delta, ACL, retirement,
    purge, state/policy, reprovision, and webhook audit paths;
  - retained historical audit rows unchanged and avoided mutable catalog
    re-reads during in-flight work; and
  - `389` affected ingestion and Function tests passed, including processing,
    lifecycle, governed purge, management, and global-event omission coverage;
    fatal-error lint and scoped diff checks passed.
  - added required `businessName` to retrieval `SourceEligibility`, preserving
    the accepted label and catalog ETag in the selected-source request snapshot;
  - replaced complete ETag-bearing eligibility equality with semantic
    revalidation of source identity, lifecycle, scope, container pair, and ACL
    policy, so label/ETag-only updates do not suppress otherwise valid results;
  - retained fail-closed behavior for lifecycle withdrawal, malformed or
    unreadable sources, container/scope changes, ACL-policy changes, and
    cross-source evidence; and
  - all `714` retrieval tests passed with `9` live-environment tests skipped;
    fatal-error lint and scoped diff checks passed.
  - propagated the accepted selected-source `businessName` snapshot to every
    persisted retrieval query usage item and the terminal `query_request`
    summary without altering the service's runtime usage payloads;
  - omitted the label when no source policy snapshot was accepted, avoiding
    fabricated context and any mutable catalog re-read; and
  - all `714` retrieval tests passed with `9` live-environment tests skipped,
    including standard, agentic, fallback, no-evidence, and accepted point-read
    audit paths; fatal-error lint and scoped diff checks passed.
  - enforced non-empty `businessName` at the repository create boundaries for
    new `source_document` and `visual_manifest_page` records;
  - preserved optional model deserialization and update compatibility for
    historical records that predate the field, so legacy reads are not rejected
    or rewritten; and
  - `364` focused ingestion repository/model tests passed, including labeled
    writes, unlabeled-create rejection, and legacy missing-label reads.
  - reconciled `docs/ARCHITECTURE.md` and
    `docs/MULTI_SHAREPOINT_SOURCES.md` with the implemented T33 source,
    content-snapshot, ETag mutation, and audit-context contracts;
  - documented that `businessName` is descriptive metadata rather than source
    identity, routing, lifecycle, ACL, or authorization input; and
  - removed stale statements that the source record schema is unchanged while
    preserving the documented schema-v1 identity, partition, chunk, and ACL
    behavior.
  - completed the focused final T33 implementation review against acceptance
    criteria 29-34; no implementation defect was found;
  - `1,142` consolidated focused regression tests passed with one existing
    Starlette/httpx deprecation warning; and
  - resolved the final documentation blocker by adding
    `source_registered`, `source_business_name_changed`, and
    `source_policy_changed` to the architecture audit table and documenting
    stable `sourceId` plus accepted-context `businessName` on source-bound
    ingestion audit records; and
  - closed T33 after the final documentation consistency check found no stale
    audit-contract or pending-review statements.

#### T34 - Re-verify interrupted ACL refresh recovery [P0]

- Status: DONE (2026-10-16)
- Depends on: T18 and definitive ACL failure classification
- Blocks: T19
- Surfaces: lifecycle reconciliation, ACL resync, source connector, lifecycle
  repository, focused reconciliation tests
- Work:
  - re-read the authoritative SharePoint ACL before recovering any
    `acl_refreshing` document;
  - apply the newly verified ACL instead of replaying stored pending values;
  - retire the document fail-closed when the ACL is definitively unverifiable;
    and
  - leave the transition pending when Graph fails transiently.
- Acceptance:
  - a principal revoked after ACL staging cannot be republished by lifecycle
    reconciliation;
  - an authoritative ACL change replaces the staged pending ACL;
  - terminal verification failure retires content from `acl_refreshing`;
  - throttling and transient dependency failures neither publish nor retire
    the pending transition.
- Result (2026-10-16):
  - lifecycle reconciliation now receives a lazy source-bound connector
    provider and delegates ACL recovery to the existing
    `resync_document_acl` authority without coupling non-ACL repairs to Graph;
  - reconciliation verifies the current Cosmos ETag and `acl_refreshing`
    status before reading Graph;
  - retirement now accepts `acl_refreshing`, disables affected chunks, and
    removes stale pending ACL fields; and
  - 227 lifecycle, incremental-sync, and multi-source dispatch tests passed,
    with fatal Ruff checks and `git diff --check` passing.
- Review (2026-10-16): ACCEPTED
  - verified fresh-ACL replacement, terminal retirement, transient retry,
    Cosmos ETag fencing, crash-resume cleanup, and Graph isolation for
    non-ACL reconciliation;
  - added direct ETag-fence coverage proving a stale scan cannot build a Graph
    connector or mutate lifecycle state; and
  - found no remaining T34 implementation defect.

### Phase 7: Fresh-content cutover

#### T36 - Finalize the mapping and external retrieval API contract [P0]

- Status: DONE (2026-10-18)
- Depends on: T09, T10, T11, T12, T13, T14, T18
- Surfaces: source contract, retrieval routing/ACL implementation, API and
  architecture documentation, focused contract tests
- Work:
  - confirmed `sourceId` is the only external routing key and
    `ingestion-sources` remains the single routing authority;
  - confirmed one point read resolves one registered chunks/manifests pair,
    shared pairs remain source-filtered, and no fallback exists;
  - finalized no-evidence versus dependency-failure semantics;
  - recorded the delegated-user authentication boundary and kept Agent/OBO
    authentication explicitly deferred; and
  - validated the existing executable source, registry, service, Cosmos, and
    HTTP contracts without adding another gateway or mapping layer.
- Acceptance:
  - callers cannot select containers or ACL mode;
  - one request cannot read an unrelated source or pair;
  - ACL and lifecycle changes fail closed before evidence release;
  - current external callers and deferred external-system authentication are
    unambiguous.
- Result (2026-10-18): 540 focused source-contract, source-route,
  source-policy, pair-registry, Cosmos retrieval, service, and HTTP API tests
  passed in 16.88 seconds with no failures. One unchanged FastAPI
  `StarletteDeprecationWarning` remains nonblocking.
- Evidence: repository contract tests plus Microsoft Web API, ACA Entra
  authentication, Cosmos partitioning, and Cosmos RBAC guidance cited in the
  approved design.

#### T19 - Validate provisioning before source activation [P0]

- Status: COMPLETE; DEPLOYED/LIVE FUNCTION PROOF PASSED (2026-10-18)
- Depends on: T04, T15, T16, T33, T34, T36
- Surfaces: source activation path, readiness checks, operator tests
- Work:
  - validate content scope, the common-source ACL invariant, configured
    container existence, and policy compatibility;
  - validate the Function identity's required operations in its boundary;
  - rely on IaC contract/runtime readiness checks for the retrieval identity;
  - keep the source paused when any validation fails.
- Acceptance:
  - a source cannot activate against a missing, incompatible, or unauthorized
    pair;
  - validation errors identify the source and failed dependency without
    exposing secrets.
- Evidence: split infrastructure/application ownership in the approved design.
- Preparation result (2026-10-18):
  - live read-only inspection confirmed the Function App is running, the six
    target containers retain their approved partition contracts, and T16's
    container-scoped Function/retrieval role assignments remain the intended
    authorization boundary;
  - the strict branch parser rejected all four live source records:
    `sharepoint-drive`, `poc-pg`, and `poc-auth` are legacy-shaped, while
    `poc-auth-demo` lacks `businessName`;
  - the deployed operator list API therefore returned HTTP 200 with zero
    accepted sources, but omitted its already-computed rejection diagnostics;
  - the operator response now additively exposes rejected source IDs,
    unattributed parse errors, and duplicate-drive source IDs without making
    rejected records eligible; and
  - the exact target request bodies remain the T02 inventory authority. No
    second runtime registry or speculative staging store was introduced.
- Preparation validation:
  - 97 focused source-route tests passed;
  - fatal Ruff checks passed for the changed Python surfaces;
  - the preparation artifact matched all four live coordinators and all three
    unresolved permits; and
  - JSON parsing and `git diff --check` passed.
- Implementation result (2026-10-18):
  - added operator-only `POST /api/ingestion/sources/preflight` using the same
    strict proposal parser as registration;
  - validates the Graph site/drive relationship, both partition contracts,
    required manifest/chunk indexing, vector and full-text policies, and
    bounded Function metadata/query access;
  - returns `writesPerformed: false`, creates no audit record, and does not
    touch source, coordinator, content, or orchestration state;
  - reports Function write and retrieval read permissions as
    deployment-evidence requirements because a read-only Function request
    cannot prove writes or impersonate the retrieval managed identity;
  - exact live ARM inspection confirmed Built-in Data Contributor for the
    Function and Built-in Data Reader for retrieval on all six target
    containers; and
  - all four exact T02 request bodies passed strict parsing and live
    container-policy/query checks under the developer's read-only validation
    identity.
- Implementation validation:
  - 185 focused preflight, source-route, and operator-authentication tests
    passed, including the four exact T02 bodies and explicit no-write
    assertions;
  - two focused content-topology and pair-scoped RBAC infrastructure contracts
    passed;
  - new-file Ruff and fatal modified-file Ruff checks passed;
  - existing content topology/RBAC tests remain the deployment contract.
- Live proof result (2026-10-18):
  - strengthened the deployment controller to require a reviewed ZIP and to
    verify the exact live ARM ID, deployment/service tags, and sole expected
    Function UAMI before preview and again before execution;
  - 1,850 Function/runtime/ingestion/deployment tests and 110 focused deployment
    controller tests passed before publication;
  - validated and deployed the 56-entry package with SHA-256
    `56f206f8a4b884046700703ccba1e2bb24ccf5d1b2c0a829b29fa299f62e0124`
    to `rag-aca-e2e-20260827-func-jztu542h`;
  - all four exact T02 bodies returned HTTP 200, `preflightPassed: true`, and
    `writesPerformed: false`; Graph binding, content policy, Function metadata
    read, and Function query checks passed for every approved pair;
  - the authenticated source-list response hash was identical before and after
    all four calls, and all twelve exact container-scoped Function Contributor
    and retrieval Reader assignments remain present; and
  - no portal role was added for `admin@mngenvmcap600995.onmicrosoft.com`:
    live ARM evidence confirms two inherited Owner assignments and no direct
    resource-group Reader assignment. Owner already includes portal resource
    visibility, so Reader would be redundant rather than least privilege.

#### T20 - Execute guarded source-by-source cutover [P0]

- Status: COMPLETE; CUTOVER, INGESTION, AND DELEGATED RETRIEVAL VALIDATED
  (2026-10-18)
- Depends on: T07, T08, T12, T18, T19
- Surfaces: existing coordinator maintenance, purge, reprovision/reset, full
  sync, and operator runbook
- Work, repeated for each source from T02:
  1. pause/quiesce source work;
  2. purge that source's legacy manifests/chunks through the audited boundary;
  3. reset old-corpus pointers, delta/work, and recovery state;
  4. recreate and accept the updated paused source registration;
  5. run a guarded fresh full sync into its approved pair;
  6. validate completeness, isolation, ACL metadata, and retrieval; and
  7. activate recurring ingestion only after all checks pass.
- Acceptance:
  - each source has fresh content only in its approved pair;
  - source identity and audit history are preserved;
  - failed sources remain paused without blocking unrelated sources;
  - no legacy indexed content or stale operational pointer remains.
- Evidence: existing source-filtered purge and approved dev-only reset.
- Preparation evidence and decision (2026-10-18):
  - `sharepoint-drive` is draining with two running `acl-resync` permits;
    `poc-pg` is draining with one claimed `acl-resync` permit;
    `poc-auth-demo` and `poc-auth` are settled with zero active work;
  - the Function is still running. Existing recovery correctly forbids permit
    abandonment without exact operation identity plus affirmative evidence
    that workers are stopped, remote audio is reconciled, and indexed state is
    reconciled. Age alone is not quiescence evidence;
  - the current recovery route resolves only strict accepted source plans, so
    it cannot reach these legacy source partitions. Direct Cosmos mutation
    would bypass the authorized audit boundary and is rejected;
  - ordinary registration cannot replace an existing source ID, routing is
    part of the binding digest, and reprovision deletes operational records.
    Consequently, recreating a strict paused registration is itself T20
    cutover, not non-destructive preparation; and
  - no source, coordinator, permit, registration, content, or orchestration was
    mutated during preparation.
- Development reset decision (2026-10-18):
  - the environment owner explicitly approved deleting and recreating the four
    T02 sources. A production-grade legacy recovery route and generic in-place
    binding replacement would add a permanent mutation surface solely to
    migrate rejected development records, so they are not needed for T20;
  - stop the exact Function and delete the one development Durable Task hub
    before Cosmos mutation. This prevents legacy orchestration history from
    resuming writes after restart;
  - bind execution to a SHA-256-approved preview containing exact registration
    and reservation ETags, counts, and per-record-set digests. Revalidate the
    Azure CLI tenant, subscription, operator object, Function stopped state,
    and task-hub absence immediately before mutation;
  - delete only the four approved source partitions, their exact drive
    reservations and registrations, and source-filtered records in the legacy
    and approved content pairs. Preserve `service-audit`, `retrieval-config`,
    shared database infrastructure, and unrelated source state;
  - create an immutable in-progress audit manifest before the first delete,
    checkpoint each source with optimistic concurrency, verify zero remaining
    scoped state, and then mark the manifest complete. Failure remains
    fail-closed while the Function is stopped; and
  - recreate the four strict registrations through the operator API, perform
    one fresh source-specific sync at a time, validate isolation and ACL
    behavior after each source, and leave `poc-auth` paused.
- Execution evidence (2026-10-18):
  - the approved reset preview SHA-256 was
    `6c301dc738017c7d312d0b54b634281a0f2a5e392fee1b54f5dd2a89d6b977a5`;
    the reset deleted 59,667 source-partition operational records, four exact
    registrations and reservations, 57 legacy manifests, and 2,443 legacy
    chunks. Its durable manifest is `completed`, and post-reset scoped counts
    were zero;
  - the task hub was deleted while the Function was stopped, recreated through
    the focused Bicep module, and verified with one task-hub-scoped Function
    `Durable Task Data Contributor` assignment. The Function returned to
    `Running` with four accepted sources and no rejected, duplicate, or
    unattributed records;
  - all registrations use their exact T02 pair. Final ready content is 36/2,318
    documents/chunks for `sharepoint-drive`, 3/11 for `poc-auth-demo`, 11/113
    for `poc-pg`, and 5/6 for `poc-auth`; source documents have verified ACL
    metadata, no source record exists in a legacy or wrong pair, and every
    coordinator is settled with zero active work;
  - `sharepoint-drive` initially hit Azure OpenAI call-rate quota. The old
    Durable hub was removed, five exact claimed permits were recovered only
    after proving their manifests were still `discovered` with zero chunks and
    no audio metadata, and the source completed with temporary `WAVE_SIZE=1`.
    The override was then removed; and
  - the ACA revision is Running and its UAMI retains Data Reader on all six
    target containers. The approved public client acquired a delegated
    `user_impersonation` token for the expected tenant, audience, caller, and
    user. Direct ACA queries returned five source-isolated citations for each
    active business source, the paused Common source returned the canonical
    no-evidence response, and an anonymous query returned `401`.

### Focused runtime catalog control plane

#### T35 - Implement the Function-managed retrieval catalog API [P0]

- Status: API AND LIVE CUTOVER VALIDATED; TEMPORARY-JOB CLEANUP PENDING (2026-10-18)
- Depends on: validated environment-local `/id` singleton contract
- Surfaces: `app/function_app.py`, catalog guarded writer, Function Cosmos RBAC,
  API/architecture/infrastructure docs, focused runtime and Bicep tests
- Work:
  - added `GET` and conditional `PUT /api/management/retrieval-config`;
  - reused `Ingestion.Operator`, strict catalog validation, operation replay,
    ETag replacement, immutable history, and outcome records;
  - assigned the Function identity the guarded-writer role scoped only to
    `retrieval-config`; and
  - enforced independent 1,572,864-byte raw-request and canonical-item budgets
    before Cosmos access, returning a stable 413 error; and
  - retained empty-container baseline serving and rejected caller-supplied
    deployment identity.
- Acceptance:
  - bootstrap requires `If-None-Match: *`;
  - update requires the current `If-Match` ETag;
  - stale writes, malformed catalogs, changed operation replays, oversize
    requests, and uncertain outcomes have explicit non-success responses;
  - focused Function, catalog, request-size, polling, and Bicep contracts pass.
- Live deployment evidence:
  - after explicit no-backup approval, only the local-development
    `retrieval-config` container was recreated with `/id`; exact ARM readback
    confirmed the immutable partition contract;
  - the Function API passed authenticated bootstrap, exact replay, stale-ETag
    rejection, guarded candidate update, oversized-request rejection, and
    anonymous `401`;
  - immutable retrieval image
    `sha256:a1807e4a9e61e98bf246fd96e7d9148558137a7e0e99e46e4e4a428cc5a540ed`
    deployed through guarded Bicep and its only traffic-serving revision became
    healthy;
  - candidate operation `64e48dbf-1d35-4ab5-9f1b-8a91991d6d8c` and digest
    `sha256:146a4a031c933771a5bca152a7945287982288b967dba7b88502fa300ab92169`
    converged on the complete container-bound stable 1/1 replica cohort;
  - rollback operation `cbd1c35d-afa4-497e-aa8c-992748ed46f8` created ETag
    `"2209d5db-0000-0200-0000-6ad4477d0000"` and baseline digest
    `sha256:b6346292a4b2133f9bc40f9282c12531b6fa8e3bf34fa023fe66c0c3e77d72aa`,
    which also converged on the complete stable 1/1 cohort; and
  - final GET confirmed over-fetch factor `3`, neutral `1:1` weights, no
    profiles, and no synonym maps.
- Repository validation: 332 focused Function, catalog, polling/observation,
  publisher, image-package, deployment-controller, and Bicep tests passed in
  228.71 seconds; Ruff fatal/undefined-name checks and PowerShell parsing passed.
- Remaining gate: `main.bicep` still conditionally defines the temporary
  catalog job. Remove its IaC, identity/RBAC, and parameters only as the next
  separately reviewed cleanup; this live gate did not perform that removal.

### Phase 8: Final validation and handoff

#### T21 - Run the full acceptance matrix [P0]

- Status: COMPLETE WITH EXPLICIT GUEST REAUTHENTICATION LIMITATION (2026-10-18)
- Depends on: T20
- Surfaces: targeted unit/contract tests, integration tests, deployed dev smoke
  tests
- Work:
  - run source schema and scope/ACL invariant tests, ingestion routing,
    mixed-permission ACL admission and feedback, retrieval isolation, business
    ACL transition, auth, Bicep, and cutover tests;
  - run the four-source development matrix;
  - call the ACA FQDN from local Postman or PowerShell; and
  - confirm the request does not traverse Azure Functions.
- Acceptance:
  - all 34 design acceptance criteria pass with captured evidence;
  - any skipped check is an explicit blocker, not an assumed pass.
- Evidence: implementation validation matrix in the approved design.
- Runtime evidence (2026-10-18):
  - Microsoft Graph revalidation confirmed all three approved T02 identities
    remain enabled. The administrator and member each currently resolve to two
    transitive security groups; the approved guest resolves to zero;
  - a fresh administrator delegated token passed exact tenant, audience,
    approved-client, and `user_impersonation` checks;
  - direct ACA queries returned `200` with five citations from only
    `POC-Auth-Docs` for `sharepoint-drive`, only `PoCAuthDemo` for
    `poc-auth-demo`, and only `POC-PG` for `poc-pg`;
  - `poc-auth`, which remains paused, returned `200` with the canonical
    no-evidence answer and zero citations; anonymous ACA access returned `401`;
  - current guest authorization was revalidated through localhost-only trusted
    principal emulation against the live Graph, Cosmos, runtime catalog, and
    retrieval implementation. All four sources returned `200`, the canonical
    no-evidence answer, and zero citations;
  - a fresh deployed guest token could not be acquired because the operator no
    longer has access to that external account. No credential bypass, app-only
    substitution, or authentication weakening was used. The earlier deployed
    guest proof remains the end-to-end Easy Auth composition evidence, while
    the current Graph and live-dependency proof establishes current no-authority
    behavior; and
  - the calls used the ACA FQDN directly. The focused gateway-removal tests
    confirm no Function query route remains.
- Repository validation (2026-10-18):
  - 1,406 focused source, routing, ACL, retrieval, auth, Function-gateway,
    infrastructure, and deployment-contract tests passed;
  - the complete suite passed 2,705 tests with nine explicitly reported
    live-environment skips and one dependency deprecation warning;
  - Ruff fatal/error checks passed; the full optional Ruff ruleset reports
    existing formatting and modernization debt and was not mass-fixed in T21;
  - `main.bicep` compiled to external build storage and parsed successfully;
    and
  - `git diff --check` passed.

#### T22 - Review and hand off [P0]

- Status: NOT STARTED
- Depends on: T21
- Surfaces: implementation diff, deployment evidence, operational docs
- Work:
  - review for source/container leakage, hidden fallback, broad RBAC, stale
    gateway behavior, and unsafe cutover paths;
  - update directly affected API, architecture, deployment, and runbook docs;
  - record rollback steps and residual risks; and
  - leave production capacity and agent/OBO work on hold.
- Acceptance:
  - review has no unresolved must-fix findings;
  - docs describe the deployed behavior;
  - implementation status tracker is complete and archived.
- Evidence: approved milestone exclusions and repository documentation
  conventions.

## Dependency path

The critical path is:

`T01 -> T03 -> T05 -> T06 -> T07 -> T08 -> T11 -> T12 -> T18 -> T33 -> T34 -> T36 -> T19 -> T20 -> T21 -> T22`

The approved ad hoc ACL work is the next implementation gate:

`T14 -> T28 -> T29 -> T30 -> T31 -> T32`

Local authentication and infrastructure continue only after that gate:

`T32 -> T23 -> T24 -> T25 -> T27 -> T15 -> T16 -> T33 -> T34 -> T19`

Direct ACA work can proceed in parallel with routing:

`T01 -> T13 -> T14` and `T13 -> T17`

No cutover starts until application routing, authentication, infrastructure,
RBAC, business metadata, and source activation validation converge at T20.

## Verification commands

Use the repository's existing test runner and redirect generated Python/test
artifacts to `C:\DevEnvs\enterprise-rag-pipeline\`. Select the smallest tests
for the active task first, then run these grouped gates:

```powershell
$env:PYTHONPYCACHEPREFIX = 'C:\DevEnvs\enterprise-rag-pipeline\pycache'
$env:RUFF_CACHE_DIR = 'C:\DevEnvs\enterprise-rag-pipeline\ruff-cache'
$pytestCache = 'C:\DevEnvs\enterprise-rag-pipeline\pytest-cache'
python -m pytest -o "cache_dir=$pytestCache" tests\ingestion\test_source_registry.py tests\app_runtime\test_source_routes.py
python -m pytest -o "cache_dir=$pytestCache" tests\ingestion\test_graph_discovery.py tests\ingestion\test_source_connector.py tests\ingestion\test_models.py
python -m pytest -o "cache_dir=$pytestCache" tests\app_runtime\test_multi_source_dispatch.py tests\ingestion\test_repository.py tests\ingestion\test_lifecycle_repository.py
python -m pytest -o "cache_dir=$pytestCache" tests\retrieval\test_auth.py tests\retrieval\test_source_policy.py tests\retrieval\test_cosmos_registry.py tests\retrieval\test_cosmos.py tests\retrieval\test_service.py tests\retrieval\test_main.py
python -m pytest -o "cache_dir=$pytestCache" tests\app_runtime\test_query_gateway.py tests\infra\test_bicep_contracts.py tests\infra\test_deployment_contract.py
ruff check app tests
```

The implementation must also compile Bicep through the repository's existing
infrastructure validation path and complete deployed smoke tests because unit
tests cannot prove Azure ingress, identity, RBAC, DNS, or private dependency
connectivity.

## Scope

Included:

- strict schema-v1 source contract reset in dev;
- required, scope-independent, mutable `businessName` metadata with
  source-document and source-context audit snapshots;
- business/common container-pair provisioning and routing;
- immutable source `contentScope` with the common-source ACL invariant;
- source-scoped ingestion and retrieval;
- per-source ACL policy;
- verified-group ACL admission with structured, non-authorizing permission
  feedback;
- direct delegated-user ACA authentication;
- Function query-gateway removal;
- guarded fresh ingestion and dev validation.

Excluded:

- agent-specific client registrations and OAuth OBO;
- schema-v2 or backward-compatible migration;
- runtime container remapping;
- dynamic container creation by the application;
- API Management, Front Door, or another query gateway;
- production SKU approval without measurements;
- changes to existing SharePoint sites.

Deferred post-implementation follow-ups:

- Confirm the four development `sourceId` values and SharePoint site/drive
  bindings at T02.
- Confirm each source's target container pair and initial ACL policy at T02.
- Identify one ACL-member and one non-member test user at T02.
- Confirm the native/public dev client registration for Postman and PowerShell
  at T02.
- ACA environment recreation and fresh-ingestion downtime can be scheduled at
  any time after T18 because no one is using the development environment.

Evidence gaps:

- The exact source IDs, bindings, target pairs, test users, and native/public
  client registration are intentionally not collected yet.
- Production capacity remains blocked on measured workload evidence and is not
  an implementation prerequisite for this dev milestone.
