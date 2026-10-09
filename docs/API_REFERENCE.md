---
title: API Reference
description: HTTP routes, response contracts, and errors for the Enterprise RAG Pipeline
---

Complete HTTP API for the Enterprise RAG Pipeline. All routes exposed by the Function App and the retrieval service, with headers, request bodies, responses, and error codes.

For the complete deployment and runtime environment-variable inventory, see [CONFIGURATION.md](CONFIGURATION.md).

## Contents

- [Authentication](#authentication)
- [Common Headers](#common-headers)
- [Ingestion Endpoints](#ingestion-endpoints) (Function App)
- [Query Endpoint](#query-endpoint) (Function App proxy → retrieval)
- [Webhook Endpoints](#webhook-endpoints) (Function App, unauthenticated)
- [Retrieval Service Endpoints](#retrieval-service-endpoints) (Azure Container Apps, internal service contract)
- [Retrieval Configuration](#retrieval-configuration) (scoring profiles, startup logs)
- [Error Responses](#error-responses)

---

## Authentication

The Function App uses **App Service Authentication (EasyAuth)** with Microsoft Entra ID. Every non-webhook endpoint requires an authenticated Bearer token with the exact configured audience from an allowed client application. All Function routes use `AuthLevel.ANONYMOUS`; Function keys do not replace EasyAuth authentication. The `/api/query` gateway requires delegated user claims with `user_impersonation`. Purge and termination/recovery additionally require the `Ingestion.Operator` app role claim (override with `INGESTION_ADMIN_ROLE`); other ingestion routes continue to rely on the configured EasyAuth client allowlist.

### Bearer Token

```powershell
$clientId = "<ADMIN_API_CLIENT_ID>"   # Function API application client ID
$token = az account get-access-token --resource "api://$clientId" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token" }
```

For query automation, obtain a delegated token through an approved client application. App-only callers are rejected by the Function query gateway. Assign the `Ingestion.Operator` app role only to approved operator principals for purge and terminate/recovery. Treat other ingestion and retry endpoints as privileged because EasyAuth client allowlisting remains their application-level authorization boundary.

---

## Common Headers

| Header | Required | Value | Notes |
|---|---|---|---|
| `Authorization` | Yes (operator endpoints) | `Bearer <token>` | EasyAuth-validated Entra token |
| `Content-Type` | Yes (POST/DELETE with body) | `application/json` | UTF-8 |
| `X-MS-CLIENT-PRINCIPAL` | Platform-generated | Base64 JSON | Consumed by the Function; never accepted from the public request as a gateway substitute |
| `X-MS-CLIENT-PRINCIPAL-NAME` | Auto-set by EasyAuth | Caller's UPN/email | Used by purge audit trail |

---

## Ingestion Endpoints

Base URL: `https://<function-app-name>.azurewebsites.net`

### `POST /api/ingestion/full-sync`

Start a parent orchestration that validates the complete source registry and fans out one full-sync child per active registered SharePoint drive. Paused/retired source IDs are returned in the parent output without dispatch. The source registry must be present; legacy environment values are not used as a fallback.

#### Request

```http
POST /api/ingestion/full-sync HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json

(empty body)
```

#### Response — 202 Accepted

```json
{
  "id": "3f9c1a7b2e4d4f6a9c8b1d2e3f4a5b6c",
  "statusQueryGetUri": "https://<func>.azurewebsites.net/runtime/webhooks/durabletask/instances/3f9c1a7b2e4d4f6a9c8b1d2e3f4a5b6c?...",
  "sendEventPostUri": "...",
  "terminatePostUri": "..."
}
```

Each run gets a fresh, randomly generated instance ID — Durable Functions instance-ID
reuse is documented as best-effort/racy at the storage layer
([Azure/azure-functions-durable-python#410](https://github.com/Azure/azure-functions-durable-python/issues/410)),
so the app never reuses one. The current instance ID for a run is available from the
response above, or by omitting `instanceId` on `GET /api/ingestion/status` (see below).

#### Error responses

| Status | Body | Reason |
|---|---|---|
| 503 | `{"error":"source_registry_validation_failed"}` | Registry enumeration, binding, or site/drive validation failed before dispatch |

The parent output contains per-source child outcomes (`completed`, `failed`, or
`pending`) plus paused/retired source IDs. Delta document create/update work is
claimed against the existing source-partition permit ledger; pending audio and
unresolved permits do not advance the delta cursor. Delta deletion and ACL changes
and other mutation writers remain outside this accounting, so it does not prove
that all cross-trigger writer races are drained; see the multi-source design's
implementation status before deployment.

For delta batch-audio updates, the poller completes superseded-version cleanup
before releasing the matching document permit. If cleanup cannot be verified,
the permit remains unresolved and the delta cursor is not advanced.

---

### `GET /api/ingestion/status`

Query the runtime status of an orchestration instance (multi-source parent, per-drive full-sync child, delta-sync, or ACL-resync).

#### Request

```http
GET /api/ingestion/status?instanceId=<id>&showHistory=true HTTP/1.1
Authorization: Bearer <token>
```

#### Query parameters

| Name | Required | Default | Description |
|---|---|---|---|
| `instanceId` | Yes | None | Any orchestration instance ID |
| `showHistory` | No | `false` | If `true`, adds a `history` field with the orchestration's replay history (diagnostic use) |
| `includeSourceOutcomes` | No | `false` | If `true`, include one bounded page of persisted per-drive outcomes for a multi-source full-sync parent |
| `sourceContinuationToken` | No | None | Continue paging persisted per-drive outcomes; use only with the same `instanceId` |

Instance IDs are randomly generated per run, not derived from `INGESTION_SOURCE_ID` or the
orchestration kind. The caller must retain the ID returned when the orchestration starts.
To check a periodic tick, read `currentInstanceId` from the `delta-sync-trigger`,
`acl-resync-trigger`, or `lifecycle-reconcile-trigger` control record in `ingestion-runs`
through an approved Cosmos read path, then pass that value as `instanceId`.

The deployed Durable Task Scheduler uses its default 30-day retention for
terminal orchestration data. Status and replay history are diagnostic data, not
permanent release evidence. This endpoint can return `404` after scheduler
purge, and the terminate endpoint may purge its target sooner.

#### Response — 200 OK

```json
{
  "instanceId": "3f9c1a7b2e4d4f6a9c8b1d2e3f4a5b6c",
  "runtimeStatus": "OrchestrationRuntimeStatus.Completed",
  "output": {
    "status": "completed",
    "runId": "run:sharepoint-drive:2026-08-19T...",
    "discovered": 22,
    "succeeded": 22,
    "failed": 0,
    "runStatus": "completed"
  },
  "createdTime": "2026-08-19T17:25:05+00:00",
  "lastUpdatedTime": "2026-08-19T17:32:11+00:00"
}
```

#### Error responses

| Status | Body | Reason |
|---|---|---|
| 400 | `{"error":"instance_id_required"}` | `instanceId` was omitted |
| 404 | `{"error":"not_found"}` | No orchestration with that instance ID |
| 409 | `{"error":"source_outcomes_unavailable"}` | The requested source-outcome page could not be read |

---

### `POST /api/ingestion/terminate`

Request termination, then finalize the run as `TERMINATED` only after the
orchestration is terminal and the source coordinator reports no unresolved
registered mutation permits. The endpoint marks an active coordinator as
draining before requesting termination. Durable termination does not cancel
already-running activities, so the coordinator remains draining while those
registered activities settle.

An ambiguous permit is never released by timeout or lease expiry. Separately
authorized recovery may abandon one exact permit only after the Durable instance
is terminal and a named operator attests that old workers are stopped, remote
audio work is settled or canceled, and indexed state is reconciled. Each
abandonment is audited; any remaining permit keeps the source draining. The
attestation is an operational assertion, not an automated proof.

For batch audio submitted by this document-processing path, the job retains its
work and attempt IDs. The poller verifies the matching terminal ready/failed
document, settles the permit idempotently, and then clears those IDs. Pending or
unverified finalization remains unresolved.

#### Request

```http
POST /api/ingestion/terminate?instanceId=<id> HTTP/1.1
Authorization: Bearer <token>
```

#### Query parameters

| Name | Required | Default | Description |
|---|---|---|---|
| `instanceId` | No | Current full-sync instance, resolved via Cosmos | Instance to terminate |
| `abandonWork` | No | `false` | Set `true` only for separately authorized operator recovery of one exact permit |
| `workId`, `attemptId`, `sourceRunId`, `documentId` | Required with `abandonWork=true` | None | Exact permit identity to abandon |
| `workersStopped` | Required with `abandonWork=true` | None | Must equal `yes` after the shared Function workers are stopped |
| `remoteAudioReconciled` | Required with `abandonWork=true` | None | Must equal `yes` after remote transcription jobs are settled or canceled |
| `indexedStateReconciled` | Required with `abandonWork=true` | None | Must equal `yes` after manifests, chunks, and document lifecycle are reconciled |
| `quiescenceEvidenceId` | Required with `abandonWork=true` | None | External maintenance/change record ID persisted in the recovery receipt |
| `recoveryReason` | Required with `abandonWork=true` | None | Audited operator explanation |

#### Response — 200 OK

```json
{
  "status": "terminated",
  "runId": "run:sharepoint-drive:...",
  "docsForceFailed": 3,
  "counters": {"discovered": 22, "ready": 19, "failed": 3},
  "orchestrationId": "3f9c1a7b2e4d4f6a9c8b1d2e3f4a5b6c"
}
```

#### Response — 202 Accepted

The orchestration has not become terminal yet, or registered document work
remains unresolved. The run is not finalized, and Durable history is retained.
Retry the termination request after the registered work settles. There is no
permit-expiry takeover; an abandoned permit remains blocked until a separately
authorized recovery can establish that its prior writer cannot continue.

```json
{
  "status": "draining",
  "orchestrationId": "<id>",
  "unresolvedDocumentWork": 1
}
```

Coverage is limited to audio jobs carrying work/attempt IDs from the registered
document-processing path. The poller settles those permits only after verifying
the matching terminal document, then clears its persisted pointer for replay
safety. While draining, paused or retired sources may run a settlement-only poll
under the stored coordinator owner; that pass admits no work. Pending or
unverified jobs remain unresolved. Delta-submitted audio and other mutation paths
are outside this accounting and remain deployment blockers.

#### Alternative responses (all 200 OK)

| `status` value | Meaning |
|---|---|
| `terminated` | Orchestration was running; force-failed non-terminal docs and finalized as TERMINATED |
| `no_active_run` | No source-control record or current run — nothing to terminate |
| `already_terminal` | Run has already reached a terminal state (completed/failed/terminated); returns the existing `runStatus` |

---

### `POST /api/ingestion/retry-failed`

Reprocess only the failed documents from the current run. Skips re-scanning the entire corpus.

#### Request

```http
POST /api/ingestion/retry-failed HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json

(empty body)
```

#### Response — 202 Accepted

```json
{
  "status": "retrying",
  "count": 3,
  "orchestrationId": "retry-failed-3f9c1a7b2e4d4f6a9c8b1d2e3f4a5b6c"
}
```

#### Alternative responses

| Status | Body | Reason |
|---|---|---|
| 200 | `{"status":"nothing_to_retry","failed":0}` | No failed documents in current run |
| 409 | `{"error":"sync_in_progress"}` | Full-sync currently running (must complete first) |
| 500 | `{"status":"reset_failed","failed":3}` | Could not reset document status to discovered |

---

### `POST /api/ingestion/sources`

Register one SharePoint document-library drive. The route validates that the
site owns the drive through Microsoft Graph, creates the source as `paused`,
and records its immutable source/drive acceptance. Requires the
`Ingestion.Operator` role.

#### Request

```http
POST /api/ingestion/sources HTTP/1.1
Authorization: ******
Content-Type: application/json

{
  "sourceId": "poc-pg",
  "siteUrl": "https://tenant.sharepoint.com/sites/POC-PG",
  "driveId": "b!example"
}
```

#### Response — 201 Created

```json
{
  "sourceId": "poc-pg",
  "siteId": "tenant.sharepoint.com,site-guid,web-guid",
  "siteUrl": "https://tenant.sharepoint.com/sites/POC-PG",
  "driveId": "b!example",
  "desiredState": "paused",
  "etag": "\"00000000-0000-0000-0000-000000000000\""
}
```

An exact retry is idempotent. The same `sourceId` with different immutable
values, or another source using the same drive, returns `409`.

| Status | Error | Reason |
|---|---|---|
| 400 | `source_registration_invalid` or field-specific validation error | Body, source ID, or site URL is invalid |
| 403 | `operator_role_required` | Caller lacks the operator role |
| 409 | `source_id_conflict` or `sharepoint_drive_reservation_conflict` | Source ID or drive conflicts |
| 422 | `source_validation_failed` | Graph rejected the site/drive relationship |
| 503 | `graph_unavailable` or `source_registry_unavailable` | Graph or Cosmos is unavailable |

---

### `GET /api/ingestion/sources`

Return one bounded page of registered, accepted sources. Requires the
`Ingestion.Operator` role.

```http
GET /api/ingestion/sources?continuationToken=<token> HTTP/1.1
Authorization: ******
```

```json
{
  "sources": [
    {
      "sourceId": "poc-pg",
      "siteUrl": "https://tenant.sharepoint.com/sites/POC-PG",
      "driveId": "b!example",
      "desiredState": "active",
      "etag": "\"00000000-0000-0000-0000-000000000000\""
    }
  ],
  "continuationToken": null
}
```

Pass the returned token unchanged to read the next page. A registry or invalid
continuation-token failure returns `503 source_registry_unavailable`.

---

### `PATCH /api/ingestion/sources/{sourceId}`

Change only `desiredState`. Allowed transitions are `paused` ↔ `active`, and
`active` or `paused` → `retired`. Retirement is final. The current ETag is
required to prevent lost updates. Activation does not start ingestion; use
`POST /api/ingestion/sources/{sourceId}/full-sync` for that source, the all-source
full-sync route, or wait for a trigger.

Retirement immediately excludes the source from retrieval and new ingestion.
The lifecycle timer drains an existing writer and waits for its coordinator to
settle before starting `source_retirement_orchestrator`. This asynchronously
changes document manifests to `deleting`, disables their chunks, then physically
deletes the manifests, visual manifest pages, and chunks. Leftover source chunks
are also deleted. Matching document winners are cleared. Registration, bindings,
retirement latch, coordination history, and service-audit records are retained.
Deletion writes `document_deleted` audit events with `reason=source_retired`
and `method=source_retirement` through the same best-effort audit helper used by
SharePoint delta deletion; audit-write failures are logged, not guaranteed delivery.
Every successful (`200`) PATCH also writes one best-effort `source_state_changed`
audit event (`runId=source-state`) with the resulting `desiredState`, the new
`catalogEtag`, `recordedAt`, `operatorObjectId` (the caller's immutable Entra
`oid` claim from `X-MS-CLIENT-PRINCIPAL`; `null` if absent or invalid) and
`operator` from the App Service `X-MS-CLIENT-PRINCIPAL-NAME` header (`unknown`
if absent). Use `operatorObjectId` to identify who made the change; Microsoft
identity guidance says names, UPNs and emails are mutable and can be reused, so
`operator` is only a readable label. Rejected PATCH requests are not audited.
The PATCH `200` confirms the
catalog/latch change, not completion of this background work. Use the
`lifecycle-reconcile-trigger` instance ID with the status endpoint to inspect
completion or failure. Failed pages can retry without reactivating content.
Completed output reports `deletedDocuments` (document versions) and
`deletedOrphanChunks` (remaining chunks; excludes chunks deleted with a manifest).

```http
PATCH /api/ingestion/sources/poc-pg HTTP/1.1
Authorization: ******
Content-Type: application/json
If-Match: "<current-etag>"

{"desiredState":"active"}
```

The `200` response has the same source fields as registration, excluding
`siteId`, and includes the new `etag`.

| Status | Error | Reason |
|---|---|---|
| 400 | `source_state_invalid` or source-ID validation error | Path or body is invalid |
| 403 | `operator_role_required` | Caller lacks the operator role |
| 404 | `source_registration_missing` | Source does not exist |
| 409 | Registry conflict | Binding is incomplete or retirement prevents the transition |
| 412 | `source_etag_mismatch` | `If-Match` is stale |
| 428 | `if_match_required` | `If-Match` is missing |
| 503 | `source_lifecycle_pending` or `source_registry_unavailable` | Lifecycle recording or Cosmos failed |

---

### `POST /api/ingestion/sources/{sourceId}/reprovision`

Rebuild the **same** fully retired source identity and immutable SharePoint
site/drive binding. This is a separate destructive recovery operation; it does
not weaken the normal rule that `retired` is terminal. It requires the operator
role, the current source ETag, and an explicit confirmation body.

The preflight requires the retirement latch to be set, the retirement
coordinator to be settled with no active work, the immutable source/drive
acceptance records to match, and Microsoft Graph to confirm the stored site and
drive relationship. These checks run before the reprovision orchestration starts.

```http
POST /api/ingestion/sources/poc-auth/reprovision HTTP/1.1
Authorization: ******
Content-Type: application/json
If-Match: "<current-etag>"

{"confirm":"reprovision"}
```

The endpoint returns a Durable Functions `202 Accepted` check-status response.
The bounded orchestration deletes source-scoped
operational records such as delta cursors, document operations, winners, and
old dispatch items. Cleanup uses an explicit allowlist and stops on an unknown
record. It preserves the immutable source binding, global drive reservation,
durable reprovision intent/receipt, and service-audit history. During
finalization the source-serving latch remains retired until the catalog and
control records can be updated safely. Cleanup progress and operator object ID
are persisted transactionally so activity retries and ambiguous responses are
reconciled from Cosmos state.

Successful output reports `desiredState: "paused"`,
`deletedOperationalRecords`, and `fullSyncRequired: true`. Reprovision does not
start ingestion. While this latch is set, `PATCH` cannot activate the source,
and automated triggers continue to exclude the paused source. Explicitly call
`POST /api/ingestion/sources/{sourceId}/full-sync` while it is paused. A
successful full sync clears the latch; then use `PATCH` to activate the source.
This full sync is mandatory because retirement deleted the indexed content and
reprovision removed the previous incremental state.

| Status | Error | Reason |
|---|---|---|
| 202 | — | Reprovision orchestration started |
| 400 | `source_reprovision_confirmation_required` or source-ID validation error | Confirmation or path is invalid |
| 403 | `operator_role_required` or `operator_object_id_required` | Caller lacks the role or immutable Entra `oid` required for destructive attribution |
| 404 | `source_registration_missing` | Source does not exist |
| 409 | `source_not_retired`, `source_retirement_incomplete`, or `drive_coordinator_not_quiescent` | Retirement is incomplete or source work is unresolved |
| 412 | `source_etag_mismatch` | `If-Match` is stale |
| 422 | `source_validation_failed` | Graph no longer confirms the stored site/drive binding |
| 428 | `if_match_required` | `If-Match` is missing |
| 503 | `graph_unavailable`, registry error, or `source_reprovision_start_failed` | A required dependency or orchestration start failed |

---

### `POST /api/ingestion/sources/{sourceId}/full-sync`

Start one full-sync orchestration for one active source. Other sources are not
scanned. Use it after registering and activating a new source; later changes
arrive through webhooks and the delta timer. The response is the same Durable
`202 Accepted` check-status payload as `POST /api/ingestion/full-sync`; poll
`GET /api/ingestion/status?instanceId=<id>`. The busy check is best-effort; if
another run takes the source first, the orchestration fails in the `activate`
phase with `drive_writer_already_active` and the running job continues.

```http
POST /api/ingestion/sources/poc-pg/full-sync HTTP/1.1
Authorization: ******
```

| Status | Error | Reason |
|---|---|---|
| 202 | — | Full sync started for this source |
| 400 | Source-ID validation error | Path is invalid |
| 403 | `operator_role_required` | Caller lacks the operator role |
| 404 | `source_registration_missing` | Source does not exist |
| 409 | `source_not_active` | Source is paused or retired |
| 409 | `source_busy` (with `operation`) | Another run holds or is draining the source; wait and retry |
| 409 | Registry conflict | Binding is not accepted or does not match |
| 503 | Registry error or `source_registry_unavailable` | Cosmos failed |

---

### `GET /api/ingestion/inspect`

Read rows from any Cosmos container for debugging. Sanitizes system fields (`_ts`, `_etag`, etc).

#### Request

```http
GET /api/ingestion/inspect?container=<name>&limit=<n> HTTP/1.1
Authorization: Bearer <token>
```

#### Query parameters

| Name | Required | Default | Description |
|---|---|---|---|
| `container` | Yes | — | One of: `ingestion-runs`, `source-documents`, `search-chunks`, `service-audit`, `retrieval-config` |
| `limit` | No | `10` | Number of rows (max `200`) |
| `runId` | No | — | Supported only with `container=source-documents`; filters the `/sourceRunId` partition as `<source_id>:<runId>` |
| `deploymentInstanceId` | Conditional | — | Required with `container=retrieval-config` (its partition key); `runId` is rejected for that container |

For `ingestion-runs`, `search-chunks`, and `service-audit`, omit `runId`; their partition keys are not `/sourceRunId`, and supplying it can return an empty result that does not prove absence. Without `runId`, `service-audit` is queried cross-partition and ordered by `recordedAt DESC`, so the result is the most recent rows. `search-chunks` has no equivalent ordering.

Inspect removes Cosmos `_` system properties only. Source documents, chunk content, audit user/tenant IDs, and historical questions and answer previews can remain in the response. Restrict endpoint access and handle diagnostic output according to its data classification.

Query summaries written by the current implementation omit question text, answer previews,
and their truncation flags. Identity and operational metadata remain; existing records
are not modified. This change does not establish content-free logs or traces.

#### Response — 200 OK

```json
{
  "container": "source-documents",
  "count": 3,
  "rows": [
    {
      "id": "doc:sharepoint-drive:...",
      "sourceRunId": "sharepoint-drive:run:...",
      "sourceName": "Policy.pdf",
      "status": "ready",
      "stage": "terminal",
      "allowedGroupIds": ["<security-group-id-1>", "<security-group-id-2>"],
      "aclHash": "sha256:...",
      "eTag": "..."
    }
  ]
}
```

#### Error responses

| Status | Body | Reason |
|---|---|---|
| 400 | `{"error":"invalid_container","allowed":["..."]}` | Container name not in allowlist |
| 503 | `{"error":"cosmos_query_failed"}` | Cosmos SDK exception |

---

### `DELETE /api/ingestion/purge`

Delete items from a Cosmos container with an audit record. Refuses to purge `service-audit`.
Ingestion data purge is scoped to one registered `sourceId`, requires explicit confirmation,
and acquires an exclusive operator-maintenance coordinator before deleting. It is rejected
while the drive coordinator is active, draining, or has unresolved work. After the operation,
the coordinator is settled.
The `ingestion-runs` container is not purgeable because it contains coordinator,
dispatch, work, and recovery authorities.

#### Request

```http
DELETE /api/ingestion/purge HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json

{
  "container": "search-chunks",
  "sourceId": "<registered-source-id>",
  "ids": ["chunk1", "chunk2"],
  "confirm": "yes"
}
```

#### Body schema

| Field | Type | Required | Description |
|---|---|---|---|
| `container` | string | Yes | One of: `source-documents`, `search-chunks`, `retrieval-config`; `ingestion-runs` is rejected |
| `sourceId` | string | For ingestion data | Registered source ID; all item queries are filtered to this source |
| `ids` | `array<string>` | Conditional | List of item IDs (max 100). Required if `purgeAll` is not set. |
| `purgeAll` | boolean | Conditional | If `true`, delete all items in the selected source scope |
| `confirm` | string | Required for ingestion data | Must equal `"yes"` — safety guard |

#### Response — 200 OK

```json
{
  "deleted": 2,
  "failed": 0,
  "auditId": "5ff5c1a0-...-uuid"
}
```

#### Error responses

| Status | Body | Reason |
|---|---|---|
| 400 | `{"error":"invalid_json"}` | Body is not valid JSON |
| 400 | `{"error":"invalid_container","allowed":["..."]}` | Container name not allowed |
| 400 | `{"error":"provide 'ids' (list) or 'purgeAll':true"}` | Missing target |
| 400 | `{"error":"purgeAll requires 'confirm':'yes'"}` | Safety guard failed |
| 400 | `{"error":"ids must be a list with max 100 items"}` | Bulk limit exceeded |
| 503 | `{"error":"purge_failed"}` | Cosmos error during delete |

**Audit trail**: every purge writes a record to the `service-audit` container with `operatorObjectId` (the caller's immutable Entra `oid`; `null` if absent or invalid), the operator's display name (`X-MS-CLIENT-PRINCIPAL-NAME` header, a mutable readable label), `deletedIds` (first 100), `deletedCount`, `failedCount`, `purgeAll`, and `recordedAt`.

---

## Query Endpoint

### `POST /api/query`

RAG query. The Function validates the EasyAuth user claims (`tid`, exact `aud`, `oid`, `user_impersonation`, and optional `idtyp=user`), creates a bounded gateway context, obtains a Function-UAMI service token, and calls Azure Container Apps with a Function-owned request ID.

#### Request

```http
POST /api/query HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json

{
  "question": "What is the password policy?",
  "mode": "hybrid",
  "history": [],
  "top_k": 5,
  "scoring_profile": null,
  "expand_synonyms": null
}
```

#### Body schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `question` | string | Yes | — | 1–4000 characters |
| `mode` | string | No | `"hybrid"` | One of: `hybrid`, `vector`, `full_text` |
| `history` | `array<object>` | No | `[]` | Conversation history for multi-turn queries. Each item: `{role: "user"\|"assistant", content: "..."}`. Bounded to last 10 messages. |
| `top_k` | integer | No | `MAX_EVIDENCE_CHUNKS` env var (default 5) | Chunks to retrieve (1–20) |
| `scoring_profile` | string \| null | No | Captured catalog's optional `defaultProfile` | Omitted or `null` selects the default, or no profile if absent. Unknown names are rejected. |
| `expand_synonyms` | boolean \| null | No | `null` | `false` disables expansion. Otherwise only the selected profile's map applies. See [Synonym enablement truth table](#synonym-enablement-truth-table). |

Freshness has no request field. The selected profile automatically applies any configured freshness functions to each candidate's `sourceModifiedAt`.

The deployed example catalog defines:

| Profile | Function aggregation | Synonym map |
|---|---|---|
| `hr-relevance` | `sum` | `hr-en` |
| `hr-relevance-average` | `average` | `hr-en` |
| `hr-relevance-minimum` | `minimum` | none |
| `hr-relevance-maximum` | `maximum` | none |

**Path selection is automatic** based on LLM query planner output:

- 1 planned query → **Standard RAG path**
- 2–3 planned queries → **Agentic RAG path**, with automatic fallback to standard on timeout or ordinary agent failure, but not citation-validation failure or a recognized safety block

#### Response — 200 OK

```json
{
  "answer": "The policy requires MFA [S1] and periodic password reviews [S2] every 90 days.",
  "citations": [
    {
      "ref": "[S1]",
      "source_name": "04_Password_and_Authentication_Policy.pdf",
      "url": "https://<tenant>.sharepoint.com/.../04_Password_and_Authentication_Policy.pdf#page=3"
    },
    {
      "ref": "[S2]",
      "source_name": "04_Password_and_Authentication_Policy.pdf",
      "url": "https://<tenant>.sharepoint.com/.../04_Password_and_Authentication_Policy.pdf#page=8"
    }
  ],
  "request_id": "3f8c9e5a-...-uuid"
}
```

#### Response schema

| Field | Type | Description |
|---|---|---|
| `answer` | string | Accepted answer or canonical refusal. Validated `[S#]` markers are removed when `INCLUDE_CITATIONS=false`. |
| `citations` | `array<object>` | Ordered evidence metadata, including sources not referenced in the answer. Empty for refusal or when `INCLUDE_CITATIONS=false`. |
| `citations[].ref` | string | Stable source label available to `[S#]` references in `answer` |
| `citations[].source_name` | string | Original file name |
| `citations[].location` | string | Human-readable source locator, such as a page, slide, section, worksheet, or audio time range. |
| `citations[].url` | string | Source URL; only PDF page locators append `#page=N`. Audio URLs do not promise time seeking or historical-version playback. |
| `citations[].start_ms` | integer, optional | Audio evidence start offset in milliseconds; absent for document citations. |
| `citations[].end_ms` | integer, optional | Audio evidence end offset in milliseconds; absent for document citations. |
| `citations[].evidence_version` | string, optional | Source version bound to audio evidence; absent for document citations. |
| `request_id` | string | UUID for log correlation |

Temporal citation fields are reader-first contracts. Audio ingestion is implemented via
asynchronous Azure Speech batch transcription (see [ADR 0001](adr/0001-audio-batch-transcription.md));
the resulting transcript chunks use the same retrieval ACL and source-lifecycle policy
as other extracted text.

Both generation paths validate references before applying `INCLUDE_CITATIONS`.
With evidence, a non-refusal answer must contain at least one canonical reference
such as `[S1]`; every citation candidate must resolve to that request's evidence.
Candidates begin with `[S` followed by an ASCII digit, whitespace, `+`, `-`, or `#`,
or are the exact placeholder `[Sx]`. Malformed, unclosed, and unknown candidates
fail validation. Ordinary bracketed text such as `[Summary]` is unaffected, and
repeated valid references are allowed. Source metadata is not filtered or relabeled.

No evidence returns `I could not find authorized evidence for this question.`
with no citations. The exact same refusal is also accepted when evidence exists.
Invalid references return the fixed error below without answer repair or fallback
generation. These checks establish reference syntax and membership, not factual
support or a citation for every claim.

Recognized provider safety blocks terminate query planning or generation before
returned text is accepted, including empty or partial filtered completions and
intermediate agent responses. A recognized tool-originated block also terminates
the agent run. Blocks are not retried, repaired, or routed to fallback generation;
ordinary errors retain existing retry and fallback behavior. This handling does
not establish the model deployment's effective filter policy.

#### Error responses

| Status | Body | Reason |
|---|---|---|
| 400 | `{"error":{"code":"question_required","message":"A question is required."},"request_id":"..."}` | Missing or empty `question` |
| 400/422/429 | `retrieval_request_failed` Function envelope | Retrieval rejected the body, profile, or rate limit |
| 422 | `content_filtered` retrieval and Function envelope | Recognized safety block; message: `The request could not be completed under the content safety policy.` No answer or provider detail is returned. |
| 401 | `unauthorized` Function envelope | Delegated user claims are missing or invalid |
| 502 | `retrieval_auth_failed` Function envelope | ACA or retrieval rejected Function service authentication |
| 502 | `answer_citation_invalid` retrieval and Function envelope | Generated references failed validation; message: `The generated answer could not be validated.` |
| 502 | `retrieval_unavailable` Function envelope | Retrieval returned a server error |
| 502 | `invalid_retrieval_response` Function envelope | Retrieval returned malformed, oversized, or contract-incompatible JSON |
| 503 | `gateway_not_configured` or `service_unavailable` Function envelope | Gateway settings are invalid or the proxy failed |
| 504 | `retrieval_timeout` Function envelope | Function-to-retrieval call exceeded `QUERY_PROXY_TIMEOUT_SECONDS` |

#### Notes

- The Function does not forward the user's `X-MS-CLIENT-PRINCIPAL`. It sends a managed-identity token, `X-RAG-GATEWAY-CONTEXT`, and `X-RAG-REQUEST-ID`.
- Answer generation timeout is controlled by `GENERATION_TIMEOUT_SECONDS` (retrieval service).
- Proxy timeout is controlled by `QUERY_PROXY_TIMEOUT_SECONDS` (Function App, default 30s).

---

## Webhook Endpoints

Webhook endpoints are excluded from EasyAuth so Microsoft Graph can deliver notifications. `/api/webhook/sharepoint` validates `clientState` against `WEBHOOK_CLIENT_STATE`. `/api/webhook/lifecycle` currently parses and logs lifecycle events without validating `clientState`; this is a documented security gap, not a protected endpoint contract.

### `POST /api/webhook/sharepoint`

Receive Microsoft Graph change notifications for the subscribed drive. Triggers a delta-sync orchestration when content changes are detected.

**Two modes**:

**A. Subscription validation handshake** (Graph sends this when creating/renewing the subscription):

Return the decoded token as plain text with HTTP 200 within 10 seconds.

```http
POST /api/webhook/sharepoint?validationToken=<opaque-token> HTTP/1.1
```

Response — echo the token as `text/plain`:

```text
200 OK
Content-Type: text/plain

<opaque-token>
```

**B. Change notification** (Graph sends this when a drive item changes):

```http
POST /api/webhook/sharepoint HTTP/1.1
Content-Type: application/json

{
  "value": [
    {
      "subscriptionId": "abc123-...-def456",
      "clientState": "<matches WEBHOOK_CLIENT_STATE>",
      "changeType": "updated",
      "resource": "drives/{driveId}/root",
      "resourceData": {...},
      "tenantId": "..."
    }
  ]
}
```

**Response — 200 OK** (empty body). Return a 2xx response within 3 seconds;
Microsoft Graph retries failed deliveries, while slow endpoints can delay or
drop later notifications. The Function dispatches orchestration work before
returning and does not process the changed item inline.

#### Error responses

| Status | Body | Reason |
|---|---|---|
| 400 | (empty) | Body is not valid JSON |
| 403 | (empty) | `clientState` mismatch |
| 500 | (empty) | `WEBHOOK_CLIENT_STATE` env var not configured |

**Side effects**: on valid notification, starts `delta_sync_orchestrator` (unless full-sync is running or another delta-sync is in-flight). Writes an audit record with `operation=webhook_received`.

---

### `POST /api/webhook/lifecycle`

Receive Microsoft Graph subscription lifecycle notifications. For the subscribed
`driveItem` resource, Microsoft Graph supports only
`reauthorizationRequired`; it does not send `missed` or
`subscriptionRemoved` lifecycle events.

Same validation handshake as `/api/webhook/sharepoint` (returns `validationToken` as `text/plain` when queried with `?validationToken=`).

**Change notification**:

```http
POST /api/webhook/lifecycle HTTP/1.1
Content-Type: application/json

{
  "value": [
    {
      "subscriptionId": "abc123-...",
      "lifecycleEvent": "reauthorizationRequired",
      "subscriptionExpirationDateTime": "..."
    }
  ]
}
```

**Response — 200 OK** (empty body)

**Behavior**: logs the event as a warning. It does not reauthorize, renew, or
reconcile immediately, and it does not validate `clientState`. The next renewal
tick recreates the subscription only when renewal reports the persisted
subscription missing.

---

## Retrieval Service Endpoints

The retrieval service runs in an internal ACA managed environment and is reached through its private DNS name. ACA Authentication accepts only the configured Function UAMI application and principal. Application code then validates the service token, `Retrieval.Gateway` role, gateway context, and request ID.

Base URL: `RETRIEVAL_SERVICE_URL` (from Bicep output)

### `POST /api/query` (retrieval service)

Same request schema and success response as the Function endpoint, but this is an internal service-to-service contract. Delegated callers and manually supplied user headers are not supported.

Request headers required:

| Header | Required | Description |
|---|---|---|
| `Authorization` | Yes | Function UAMI application token accepted by ACA Authentication |
| `X-MS-CLIENT-PRINCIPAL` | Platform-generated | ACA claim header for the authenticated Function application |
| `X-RAG-GATEWAY-CONTEXT` | Yes | Function-generated canonical user `oid`/`tid` context |
| `X-RAG-REQUEST-ID` | Yes | Canonical UUID generated by the Function |
| `Content-Type` | Yes | `application/json` |

**Direct-call error responses** (in addition to the query errors documented above):

| Status | Body | Reason |
|---|---|---|
| 400 | `unknown_scoring_profile` application envelope | Requested profile is not in the captured catalog |
| 401 | platform or application response | Missing/invalid service identity, gateway role, context, or request ID |
| 422 | `invalid_request` application envelope | FastAPI body validation failed |
| 429 | `rate_limit_exceeded` application envelope | Per-user, per-replica rate limit exceeded |
| 503 | `retrieval_dependency_unavailable` application envelope | Every submitted retrieval task failed or timed out |
| 504 | `operation_timeout` application envelope | ACA operation deadline exceeded |

The Function normalizes downstream failures, so direct retrieval error bodies are not identical to public Function error bodies.

---

### `GET /health/live`

Liveness probe. Always returns 200 if the process is alive.

#### Response — 200 OK

```json
{"status": "alive"}
```

---

### `GET /health/ready`

Readiness probe. Executes `SELECT TOP 1 c.id` against the first configured chunks container.

#### Response — 200 OK

```json
{"status": "ready"}
```

#### Response — 503 Service Unavailable

```json
{"detail": "cosmos_unavailable"}
```

ACA uses this to route traffic only to ready replicas.

---

## Retrieval Configuration

Scoring profiles and synonym maps load at startup and refresh together through
periodic whole-catalog validation. Each request retains its captured snapshot;
see [refresh timing](CONFIGURATION.md#direct-catalog-editing).
They define application-side secondary reranking after Cosmos BM25/vector/RRF,
not native Cosmos index scoring profiles or exact Azure AI Search scoring parity.
See [architecture](ARCHITECTURE.md#scoring-profiles-freshness-and-client-side-rerank).

### Cosmos catalog source

Retrieval requires these startup settings:

| Setting | Purpose |
|---|---|
| `RETRIEVAL_CONFIG_CONTAINER` | Cosmos container containing the singleton; default `retrieval-config` |
| `DEPLOYMENT_INSTANCE_ID` | Catalog partition key |
| `RETRIEVAL_CATALOG_POLL_SECONDS` | Bounded refresh interval; see [configuration](CONFIGURATION.md#direct-catalog-editing) |

Retrieval point-reads `runtime-catalog` from the `/deploymentInstanceId`
partition at startup and on refresh. It validates the envelope, ETag, strict
schema, unique names, finite values, functions, optional default profile, and
map references. When the item is absent, retrieval starts on a built-in baseline
(neutral `1:1` hybrid RRF, `overFetchFactor` 3, `Global` scope, no profiles or
synonym maps); a present item that is invalid fails closed, and later failures
retain last-known-good configuration. A request captures one snapshot before
planning, including agentic execution and standard fallback. No activation
pointer is used.

The authoritative authoring example is [app/retrieval/catalog.example.json](../app/retrieval/catalog.example.json). Its deployed profiles are listed in the query request section above.

### Catalog property reference

Bootstrap source files contain only `config`, based on
[the example](../app/retrieval/catalog.example.json). For routine changes, edit
only the existing singleton's `config` using the
[direct-edit procedure](CONFIGURATION.md#direct-catalog-editing). The normalized,
validated config digest plus Cosmos ETag identifies a generation; optional
writer metadata cannot attribute subsequent direct edits.

#### Top-level and retrieval properties

| Property | Required | Allowed value or default | Runtime effect and configuration guidance |
| --- | --- | --- | --- |
| `config` | Yes | Object; no unknown properties | Contains all runtime retrieval configuration. |
| `config.retrieval` | Yes | Object | Candidate retrieval settings shared by all profiles. |
| `config.retrieval.overFetchFactor` | Yes | Integer `1`–`50`; example uses `3` | With a scoring profile, requests up to `min(top_k × overFetchFactor, 50)` candidates across planned queries and registered instances before reranking. Higher values can improve reranking recall but increase retrieval work and latency. Start with the validated example value and change it only with relevance/latency evidence. |
| `config.retrieval.hybridWeights` | Yes | Object containing exactly `vector` and `text` | Supplies both positive positional weights for Cosmos hybrid RRF. |
| `config.retrieval.hybridWeights.vector` | Yes | Finite number greater than `0`; example `2.0` | Positional weight for `VectorDistance` inside Cosmos hybrid RRF. The ratio to `text` matters; larger relative values favor vector ranking. Used only in hybrid mode. |
| `config.retrieval.hybridWeights.text` | Yes | Finite number greater than `0`; example `1.0` | Positional weight for `FullTextScore` inside Cosmos hybrid RRF. Larger relative values favor lexical ranking. Used only in hybrid mode. |
| `config.retrieval.fullTextScoreScope` | Yes | `"Local"` or `"Global"`; example `"Global"` | Passed to Cosmos full-text queries. `Global` uses statistics across physical partitions for more consistent cross-partition ranking; `Local` uses partition-local statistics. Use `Global` unless measured latency/cost evidence justifies `Local`. |
| `config.defaultProfile` | No | Nonempty profile name; omit for no default | Must match a profile when supplied. Explicit `null` is invalid. |
| `config.profiles` | Yes | Array, maximum 100 entries; may be empty | Unique definitions make profiles available; selection applies one profile. |
| `config.synonymMaps` | Yes | Array | Named synonym maps. Names must be unique. Every profile `synonymMap` reference must resolve or startup fails closed. |

#### Scoring profile properties

| Property | Required | Allowed value or default | Runtime effect and configuration guidance |
| --- | --- | --- | --- |
| `name` | Yes | Nonempty unique string | Public value accepted in request `scoring_profile`. Treat it as a client contract; renaming requires callers to change. |
| `textWeights` | No | Object; default `{}` | Adds a flat bonus when any normalized query term matches the configured candidate field. This is an application approximation, not a multiplication of Cosmos BM25/vector scores. |
| `textWeights.<field>` | No | Finite number `>= 0` | Supported fields: `content`, `sourceName`, `sectionPath`, and `keyPhrases`; snake-case aliases are accepted. `0` disables that field bonus. Do not configure both aliases for the same canonical field. Establish values through protected relevance evaluation. |
| `functionAggregation` | No | `"sum"`, `"average"`, `"minimum"`, or `"maximum"`; default `"sum"` | Combines the profile's function contributions. `average` divides by the number of declared functions. With no functions, the function bonus is `0`. |
| `synonymMap` | No | Nonempty map name | Associates this profile with one entry in `config.synonymMaps`. Omit it when the profile must never expand synonyms. A missing referenced map fails startup. |
| `functions` | No | Array, maximum 8; default `[]` | Application-side scoring functions evaluated after the ACL-filtered candidate pool is returned. Only freshness is supported. |

The final application score is `1 / (originalRank + 1)` plus matching text-weight bonuses plus the aggregated function contribution, where `originalRank` is zero-based. Original rank breaks score ties.

#### Freshness function properties

| Property | Required | Allowed value | Runtime effect and configuration guidance |
| --- | --- | --- | --- |
| `type` | Yes | `"freshness"` only | Selects the only currently supported function type. |
| `fieldName` | Yes | `"sourceModifiedAt"` or `"source_modified_at"` | Reads the service-level Microsoft Graph modification timestamp denormalized onto each chunk. Prefer the canonical camel-case name used by the example. |
| `boost` | Yes | Any finite number | Multiplied by the interpolation factor. Positive values promote recent content, `0` has no effect, and negative values penalize recent content. Use negative values only with explicit evaluation evidence. |
| `interpolation` | Yes | `"constant"`, `"linear"`, `"quadratic"`, or `"logarithmic"` | Controls decay from the candidate timestamp to the end of `boostingDuration`. `constant` keeps the full contribution until the boundary; `linear` decays evenly; `quadratic` retains more contribution early; `logarithmic` decays more quickly early. |
| `freshness` | Yes | Object containing only `boostingDuration` | Parameters for the freshness calculation. |
| `freshness.boostingDuration` | Yes | Positive ISO-8601 day/time duration such as `"P30D"`, `"P180D"`, or `"PT12H"` | Defines the freshness window. At or beyond the window, contribution is `0`. Missing, malformed, timezone-naive, or future candidate timestamps also contribute `0`. Choose the window from the business meaning of “recent,” then verify ranking effects. |

For elapsed fraction `f` in `[0,1)`, the contribution is `boost × factor`: constant uses `1`, linear uses `1-f`, quadratic uses `1-f²`, and logarithmic uses `1 - log(1 + f × (e - 1))`. At `f >= 1`, the factor is `0`.

#### Synonym map properties

| Property | Required | Allowed value or default | Runtime effect and configuration guidance |
| --- | --- | --- | --- |
| `name` | Yes | Nonempty unique string | Referenced by profile `synonymMap`. |
| `format` | Yes | `"solr"` only | Selects the supported rule grammar. |
| `language` | No | Nonempty string | Accepted as catalog metadata but not currently consumed by expansion logic; it does not enable language-specific tokenization. Omit it unless a client needs informational metadata. |
| `rules` | Yes | Array of nonempty strings, maximum 20,000 | Parsed at startup. Invalid rules fail startup. Duplicate identical rules are deduplicated. Keep rules reviewed by the domain/relevance owner. |

Supported rule forms:

- Equivalence: `annual leave, vacation, paid time off`. The original query is retained and matching phrases are replaced with other terms.
- Explicit mapping: `Washington, Wash., WA => WA`. Matching left-side phrases are replaced by right-side values.
- Matching and deduplication are case-insensitive and phrase-boundary aware.
- Escape a literal comma or backslash with `\`.
- A rule can add at most five variants per matched input term, and a query is capped at eight total variants.
- Expanded terms are bound as Cosmos query parameters; values are not concatenated into SQL.

#### Fixed Cosmos envelope and optional metadata

Bootstrap creates the envelope. Keep it unchanged during routine direct editing:

| Property | Generated value | Purpose |
| --- | --- | --- |
| `id` | `runtime-catalog` | Fixed singleton ID. |
| `deploymentInstanceId` | `--deployment-instance-id` value | `/deploymentInstanceId` partition key; maximum 100 characters. |
| `type` | `retrieval-runtime-catalog` | Fixed discriminator. |
| `change` | Optional object | When present, requires exactly `operationId` (lowercase UUID), `changedAt` (valid UTC timestamp ending in `Z`), and `reason` (nonblank, at most 500 characters). |

There is no schema-version field, digest field, activation pointer, or catalog
feature switch. Cosmos `_rid`, `_self`, `_etag`, `_attachments`, and `_ts` are
service-generated. A valid new ETag is adopted even when optional `change`
metadata is unchanged. Direct saves do not guarantee writer history or rollback.

Supported text fields are `content`, `sourceName`, `sectionPath`, and `keyPhrases`. Text weights apply when normalized query terms match a configured field. The only supported scoring function is freshness over `sourceModifiedAt`. Supported interpolation modes are `constant`, `linear`, `quadratic`, and `logarithmic`; supported aggregation modes are `sum`, `average`, `minimum`, and `maximum`. Magnitude, tag, distance, arbitrary signals, and the legacy `max` alias are rejected.

Hard guardrails include 100 profiles per catalog, eight functions per profile, a 50-candidate global pool, eight full-text query variants, five additions per matched input term, parameter-bound SQL, and a 1.5-MiB serialized catalog limit. Solr equivalence retains the original variant and adds replacements; explicit mappings replace matching left-hand phrases. Prefix reserved comma and backslash characters with `\`.

Validate authoring JSON without changing Azure:

```powershell
python tools/publish_retrieval_catalog.py validate `
  --file app/retrieval/catalog.example.json `
  --deployment-instance-id <deployment-instance-id>
```

The command prints `catalogId` and `catalogDigest`. It does not change Azure.
Use [Azure setup](AZURE_SETUP.md) for create-only initialization, read-only
redeployment verification, and optional guarded writer commands. Catalog
recovery restores a protected valid body under a new ETag, not an old image pin.

### Synonym enablement truth table

Definitions and references govern expansion; there is no global enable switch.

| Selected profile | Map reference | Request `expand_synonyms` | Effective behavior |
| --- | --- | --- | --- |
| None | None | Any | No expansion |
| Present | Absent | Any | No expansion |
| Present | Loaded map | `false` | No expansion |
| Present | Loaded map | `true` or `null` | Expand using the profile's map |
| Any | Unresolved reference | Any | Reject catalog; fail startup or retain running last-known-good snapshot |

### Startup logs (redacted)

On successful load, the service logs:

```json
{
  "event": "retrieval_service_started",
  "catalog_version": "sha256:...",
  "catalog_etag": "<cosmos-etag>"
}
```

Rule bodies, weights, boosts, and synonym strings are intentionally omitted from logs.

---

## Error Responses

### Standard Envelope

The Function query gateway uses this shape:

```json
{
  "error": {"code": "<error_code>", "message": "<safe message>"},
  "request_id": "<uuid>"
}
```

Registered retrieval handlers use the same shape. EasyAuth and ACA Authentication can reject a request before application code and may return platform-defined bodies. Some ingestion endpoints retain their older compact `{"error":"<code>"}` response.

### HTTP Status Meanings

| Status | Meaning |
|---|---|
| 200 | Success |
| 202 | Async operation started; poll status endpoint |
| 400 | Client error — invalid input, wrong content type, missing required field |
| 401 | Missing or invalid Bearer token (EasyAuth rejected the request) |
| 403 | Token valid but caller lacks permission (webhook clientState mismatch, or EasyAuth policy) |
| 404 | Resource not found (orchestration instance, document, container) |
| 409 | Conflict — already running, or state prevents this operation |
| 429 | Rate limit exceeded (per-user **per-replica** sliding window on `/api/query`) |
| 500 | Unhandled server error or endpoint-specific server failure |
| 502 | Function gateway rejected downstream auth, server error, or malformed response |
| 503 | Gateway not configured or a required dependency is unavailable |
| 504 | Function proxy or ACA operation deadline exceeded |

### Common Error Codes

| Code | Endpoint | Meaning |
|---|---|---|
| `missing_ingestion_source_id` | `POST /full-sync` | `INGESTION_SOURCE_ID` env var not set |
| `already_running` | `POST /full-sync` | Full-sync in progress; wait for completion |
| `sync_in_progress` | `POST /retry-failed` | Full-sync currently running |
| `nothing_to_retry` | `POST /retry-failed` | No failed documents |
| `not_found` | `GET /status` | Orchestration instance does not exist |
| `invalid_container` | `GET /inspect`, `DELETE /purge` | Container name not in allowlist |
| `invalid_json` | `DELETE /purge` | Body is not valid JSON |
| `question_required` | Function `POST /query` | Missing or empty `question` field |
| `unauthorized` | Function `POST /query` | Delegated user claims failed gateway validation |
| `gateway_not_configured` | Function `POST /query` | Retrieval URL or service scope is missing/invalid |
| `retrieval_auth_failed` | Function `POST /query` | ACA/retrieval rejected the Function service identity |
| `answer_citation_invalid` | Function and internal retrieval `POST /query` | See [query response validation](#response-schema) |
| `retrieval_unavailable` | Function `POST /query` | Retrieval returned a server error |
| `invalid_retrieval_response` | Function `POST /query` | Retrieval returned malformed, oversized, or incompatible JSON |
| `retrieval_request_failed` | Function `POST /query` | Retrieval returned a non-authentication 4xx response |
| `retrieval_timeout` | Function `POST /query` | Function proxy timeout expired |
| `rate_limit_exceeded` | `POST /query` | Per-user **per-replica** RPM exceeded |
| `unknown_scoring_profile` | Internal retrieval `POST /query` | Requested profile is not in the captured catalog |
| `invalid_request` | Internal retrieval `POST /query` | FastAPI request validation failed |
| `retrieval_dependency_unavailable` | Internal retrieval `POST /query` | Every submitted retrieval task failed or timed out |
| `operation_timeout` | Internal retrieval `POST /query` | ACA operation deadline expired |
| `cosmos_query_failed` | `GET /inspect` | Cosmos SDK exception |
| `purge_failed` | `DELETE /purge` | Cosmos delete error |
| `service_unavailable` | Function `POST /query` | Unexpected proxy failure |

---

## PowerShell Quick Reference

```powershell
# Setup once per session
$funcApp = "<function-app-name>"
$rg = "<resource-group>"
$clientId = "<ADMIN_API_CLIENT_ID>"
$base = "https://$funcApp.azurewebsites.net"
$token = az account get-access-token --resource "api://$clientId" --query accessToken -o tsv
$h = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

# Start full-sync
Invoke-RestMethod -Method POST -Uri "$base/api/ingestion/full-sync" -Headers $h

# Start full-sync for one source only
Invoke-RestMethod -Method POST -Uri "$base/api/ingestion/sources/poc-pg/full-sync" -Headers $h

# Check status
Invoke-RestMethod -Uri "$base/api/ingestion/status" -Headers $h

# Query
$body = @{ question = "What is the password policy?"; mode = "hybrid"; top_k = 5 } | ConvertTo-Json
Invoke-RestMethod -Method POST -Uri "$base/api/query" -Headers $h -Body $body

# Inspect a container
Invoke-RestMethod -Uri "$base/api/ingestion/inspect?container=source-documents&limit=10" -Headers $h

# Purge specific IDs
$body = @{ container = "search-chunks"; ids = @("chunk1", "chunk2") } | ConvertTo-Json
Invoke-RestMethod -Method DELETE -Uri "$base/api/ingestion/purge" -Headers $h -Body $body

# Terminate a stuck orchestration
Invoke-RestMethod -Method POST -Uri "$base/api/ingestion/terminate" -Headers $h

# Retry failed documents
Invoke-RestMethod -Method POST -Uri "$base/api/ingestion/retry-failed" -Headers $h
```

---

## Related Documentation

- [README.md](../README.md) — Project overview and env var reference
- [ARCHITECTURE.md](ARCHITECTURE.md) — System design, flows, and data model
- [AZURE_SETUP.md](AZURE_SETUP.md) — Deployment guide and operations
- [DEMO_RUNBOOK.md](DEMO_RUNBOOK.md) — Current demo and end-to-end validation procedures
