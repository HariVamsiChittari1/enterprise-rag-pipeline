# Troubleshooting

These procedures follow the current Function, ACA, Cosmos lifecycle, and guarded deployment contracts. They describe failures observed in prior deployed environments; they are not all production incidents.

## Establish the Target

Before diagnosis, capture:

- Subscription, tenant, resource group, azd environment, and deployment instance.
- Function name and ACA latest ready revision.
- Full image and catalog digests.
- Exact orchestration ID or query request ID.

Do not mutate resources until the target and artifact identity are unambiguous.

## Authentication Failures

### Local Azure SDK authentication fails

The local application accepts only `AZURE_AUTH_MODE=service_principal`. In the
same PowerShell process that starts Python or Functions, confirm the tenant and
client IDs and whether a secret exists without printing its value:

```powershell
[pscustomobject]@{
  mode = $env:AZURE_AUTH_MODE
  tenant = $env:AZURE_TENANT_ID
  client = $env:AZURE_CLIENT_ID
  secretPresent = -not [string]::IsNullOrWhiteSpace($env:AZURE_CLIENT_SECRET)
}
```

- `AzureCredentialConfigurationError` means the mode is missing/unknown or a
  required variable is empty. Reload all four values using the
  [secret-safe procedure](LOCAL_DEVELOPMENT.md#stage-5-load-the-approved-local-application-identity).
- An Entra authentication failure with complete variables usually means the
  secret expired/was revoked, the value is wrong, or the tenant/client pair
  does not identify the same app. Check the owner's non-secret credential ID
  and expiry record; re-enter the secret rather than printing it.
- HTTP 403 after token acquisition means authentication succeeded but the
  service principal lacks the required data-plane role at the exact
  development resource/container scope. Do not add Contributor, Owner, account
  keys, or production access.
- DNS, timeout, firewall, or private-endpoint failures are network failures;
  changing credentials cannot bypass them.

Azure CLI, VS Code, Azure PowerShell, cached user tokens, and broker identities
are intentionally ignored. Do not use them as a workaround. When finished,
stop child processes and clear the four variables using
[Stage 10](LOCAL_DEVELOPMENT.md#stage-10-stop-the-local-environment).

### Deployed Function or ACA managed identity fails

Deployed workloads must have `AZURE_AUTH_MODE=managed_identity`, their existing
user-assigned managed-identity client ID, and no `AZURE_CLIENT_SECRET`. Verify
the identity remains assigned to the workload and retains the resource-scoped
RBAC/Graph roles before changing code. A local client secret is never a
deployment recovery mechanism.

### Function returns 401

Check that the token:

- Uses exactly `FUNCTION_API_AUDIENCE`.
- Comes from an application in `FUNCTION_ALLOWED_CALLER_CLIENT_ID`.
- Uses the expected tenant.

A Function master key does not bypass EasyAuth.

### ACA query returns 401

Verify:

- The delegated token uses the retrieval API audience and expected tenant.
- The token contains `user_impersonation` and represents a user.
- ACA built-in authentication is enabled and unauthenticated requests return 401.
- The client does not synthesize `X-MS-CLIENT-PRINCIPAL`; ACA injects it.

## Full Sync Has Failed or Stuck Documents

Inspect the current run and source documents:

```powershell
$sourceId = '<registered-source-id>'
$rows = (Invoke-RestMethod `
  -Uri "$baseUrl/api/ingestion/inspect?container=source-documents&sourceId=$sourceId&limit=200" `
  -Headers $headers).rows
$rows | Where-Object status -ne 'ready' |
  Select-Object sourceName, status, stage, attemptCount, error
```

The inspect endpoint is capped at 200 rows. Registered document/chunk container
inspection requires `sourceId`. `runId` partition filtering applies only to
`source-documents`; omit it for other containers.

If the orchestration must be terminated:

```powershell
Invoke-RestMethod -Uri "$baseUrl/api/ingestion/terminate" -Method Post -Headers $headers
```

Durable termination is asynchronous. It stops future orchestration scheduling but does not cancel an activity or suborchestration that is already running. The endpoint waits for terminal state for up to 30 seconds and purges orchestration history only after terminal state is observed; continue checking the exact instance before assuming processing or history removal is complete.

The deployed scheduler retains terminal orchestration history for 30 days by default. A missing older instance can therefore indicate scheduler retention or an explicit purge rather than a never-started request.

Retry failed documents and capture the returned random orchestration ID:

```powershell
$retry = Invoke-RestMethod `
  -Uri "$baseUrl/api/ingestion/retry-failed" `
  -Method Post `
  -Headers $headers

Invoke-RestMethod `
  -Uri "$baseUrl/api/ingestion/status?instanceId=$($retry.orchestrationId)" `
  -Headers $headers
```

Do not construct `retry-failed-<full-sync-id>`; retry IDs use a new UUID.

## Extraction Rejection or Incomplete Visuals

`document_intelligence_rejected` is terminal for that document attempt. Separate application limits from provider limits:

- The application accepts source files up to 100 MB, rendered Office-to-PDF derivatives up to 200 MB, and at most 300 document units.
- Document Intelligence S0 supports up to 500 MB and 2,000 PDF/TIFF pages.
- Document Intelligence F0 supports up to 4 MB and processes only the first two pages.

The lowest applicable limit controls. Verify the selected provider and tier, source format and size, rendered derivative size, document-unit count, and source-connector readability. Native Office analysis does not extract embedded images; the deployed Office path renders a PDF derivative for visual extraction. Missing Office visuals therefore require checking Graph conversion and rendered-PDF processing, not only native provider output.

For Content Understanding, verify that the configured account, models, and `prebuilt-documentSearch` analyzer are available in the target region. A prebuilt analyzer ID is not an immutable definition; if output changes without a configuration change, compare the active analyzer definition and consider a governed copied analyzer.

Tier changes or AI-service replacements are infrastructure changes. Update reviewed deployment inputs and use `scripts/deploy.ps1`; do not deploy `ai-services.bicep` directly or delete private endpoints to work around access.

## SharePoint ACL or Site-Group Failures

Verify:

- `SHAREPOINT_SITE_URL` is present, HTTPS, and resolves to the configured drive.
- The ingestion app has the required Graph and SharePoint application permissions and site grant.
- The certificate secret is readable by the Function UAMI.
- At least one direct Entra group or SharePoint site-group member resolves to a
  verified Entra security group.
- Entra groups return `securityEnabled=true` and `groupTypes` does not contain `Unified`.

Interpret the outcome before changing permissions:

- `aclStatus=Verified` with nonempty `allowedGroupIds` means admission succeeded.
  `aclFeedback` values such as `SharedLink`, `DirectUser`, `SiteGroup`,
  `InvalidGroup`, `InvalidIdentity`, `UnifiedGroup`, or `UnsupportedRole` are
  observations only and do not grant access.
- `unsafe_acl:no_verified_security_groups` means complete evaluation found no
  authorizing group. Link-only, user-only, disabled-group, deleted-group, and
  `Unified`-only files are intentionally rejected or ACL-retired.
- Paging limits, invalid top-level Graph responses, throttling, timeouts, and
  permission-service failures are dependency failures. They must retry or fail
  closed; do not misclassify them as a no-group business outcome.
- When `allowedGroupIds` are unchanged, ACL resync can update only document
  feedback. Chunks deliberately omit `aclStatus` and `aclFeedback`.

Never grant a broader Graph or SharePoint permission merely to suppress
`aclFeedback`, and never copy a sharing URL or token into logs or diagnostics.

An ACL-revoked document can be restored only when its current source eTag matches, no active version exists, and it is the authoritative matching historical version.

## Delta Sync Does Not Advance

- Resolve the random instance ID from the `delta-sync-trigger` control record.
- Inspect the exact orchestration output.
- Retryable, internal, and unclassified delta failures retain the previous cursor;
  a later tick replays the round.
- A terminal source-version failure is quarantined. Matching prior ready versions
  are retired before that item settles; retirement or identity conflicts retain
  the previous cursor.
- A `410 Gone` cursor causes reset and re-bootstrap handling. The replacement cursor is persisted only after the reset round succeeds; a failed round intentionally replays from the prior durable state.
- A full sync or prior delta run can intentionally suppress a new trigger.

Do not manually replace the cursor unless a separately approved recovery procedure requires it.

## Graph Webhook Subscription Does Not Recover

- Confirm the daily renewal timer and 10-minute lifecycle reconciliation are running.
- Confirm the stored subscription belongs to the configured drive and notification URLs.
- The connector requests a 41,000-minute lifetime, below Graph's 42,300-minute `driveItem` maximum.
- A renewal `404 Not Found` is treated as a missing subscription and the reconciliation path creates a replacement; do not reuse the stale subscription ID manually.
- For `driveItem`, only `reauthorizationRequired` is supported as a lifecycle event. Do not wait for `missed` or `subscriptionRemoved` lifecycle events.
- Graph validation tokens must be returned as plain text within 10 seconds. Change notifications should be acknowledged within 3 seconds; processing remains asynchronous.
- Notifications can be duplicated or replayed. Diagnose by subscription, resource, and change state rather than by notification count alone.

If ordinary notifications arrive but permission-change behavior does not, check the documented permission-header contract separately. The current connector requests metadata that Microsoft documents with `Sites.FullControl.All`, while the approved deployment prerequisite set intentionally omits that permission. Do not add it as a troubleshooting shortcut; resolve the security or implementation decision first.

## Lifecycle Transition Stuck

The 10-minute lifecycle reconciliation handles:

- `admitting` documents.
- `acl_refreshing` documents.
- `retiring` documents.
- `deleting` documents.
- Orphan chunks.
- Older duplicate ready versions when the current full-sync run supplies the ready winner.

Inspect the manifest status, pending fields, lifecycle generation, expected chunk count, and exact document key before intervening.

## Retrieval Startup or Readiness Fails

Readiness executes a small query against the first configured chunks container. Check:

- Cosmos private DNS/network reachability.
- Retrieval UAMI data-reader assignments on `search-chunks`, `source-documents`, and `retrieval-config`.
- Exact `DEPLOYMENT_INSTANCE_ID` and valid `RETRIEVAL_CATALOG_POLL_SECONDS`.
- Validity of `runtime-catalog` in that partition (including its ETag) when the item is present; an absent item starts on a built-in baseline rather than failing startup.
- Optional default profile and all profile-to-synonym-map references.

There is no active pointer. After startup, invalid edits or Cosmos failures
retain last-known-good policy and emit degraded catalog telemetry; they do not
by themselves turn readiness into HTTP 503. Restarting can remove that fallback
and fail startup while persisted configuration remains invalid.

## Catalog Edit Not Adopted

Inspect the current body and compare ETag plus validated config digest with
replica observations using the [operator procedure](AZURE_SETUP.md#catalog-observation-and-optional-writer).
Do not infer adoption from a portal save or one successful query. Check the
configured poll interval, read timeout, telemetry ingestion, active revisions,
ready replicas, process restarts, and observer permissions. Missing evidence
means unassured, not completed.

Coordinate concurrent editors. Portal conflict handling is target-unverified;
reread before and after saving and stop on unexpected changes. Recover an
invalid direct edit by restoring the protected prior valid body, preserving
the fixed envelope. This produces a new ETag, not a historical generation.
Direct saves do not guarantee writer history or automatic rollback.

For an interrupted guarded write, preserve the exact manifest and source and
run `reconcile`. Do not regenerate an operation or retry a stale write blindly.
A successful replacement without complete semantic, actor/request, adoption,
and final readback evidence remains `applied-but-unassured`.

## Managed Identity or Private Endpoint Access Fails

Diagnose identity and network state independently:

- Confirm the expected UAMI is attached and that the client ID is selected explicitly where the SDK supports multiple identities.
- Confirm the target-resource RBAC or data-plane role assignment; attaching a managed identity does not grant access.
- Allow for Entra and managed-identity permission caching after a new assignment. Restarting a revision can refresh application state but does not replace a missing role.
- Resolve the target private FQDN from the calling subnet and verify that it maps to the expected private endpoint.
- Confirm route, NSG, private DNS zone link, and service public-access state before changing credentials.

Do not enable public access, add account keys, or replace managed identity with a secret merely to make a network diagnostic pass.

## Query Failures

| ACA/retrieval outcome | Meaning |
| --- | --- |
| Platform 401/403 | Token, tenant, audience, client application, or ACA policy rejected the request before application code |
| `invalid_authenticated_principal_header` | ACA principal header is missing or ambiguous; callers must not synthesize it |
| `security_groups_unresolved` | Graph group resolution failed or returned an invalid contract; this is not the valid zero-membership case |
| HTTP 200 with no citations | Authenticated caller has no authorized evidence, the selected source is paused/ineligible, or the question has no grounded evidence |
| `retrieval_dependency_unavailable` | Every submitted retrieval task failed or timed out |
| `answer_citation_invalid` | Generated references failed the application integrity check |
| `operation_timeout` | Retrieval exceeded the ACA operation deadline |

Use `request_id` to correlate ACA logs and `service-audit`. The audit container
is best-effort and has a 90-day TTL. Do not treat the canonical HTTP 200
no-evidence response as an authentication or Graph dependency failure.

## Cosmos Vector Results or Throttling

The repositories retry supported 429 paths. Long-running ingestion can therefore be slow without being incorrect. Check dependency telemetry and orchestration progress before terminating.

For vector-query behavior, account for these service boundaries:

- DiskANN is approximate, so equally valid result order can vary across replicas.
- A container with fewer than 1,000 vectors uses a full scan instead of the DiskANN index; small test corpora do not prove production RU or latency behavior.
- The deployed dedicated `search-chunks` throughput boundary is required because this vector path does not support a shared-throughput database.
- Treat vector policy or index changes as a container migration. Microsoft guidance differs on exact mutation mechanics, so validate the deployed API behavior before choosing a migration procedure.

Do not disable ACL checks, open public Cosmos access, or use account keys to make a diagnostic pass.

## Safe Data Cleanup

`DELETE /api/ingestion/purge` supports exact item IDs and guarded purge-all for `ingestion-runs`, `source-documents`, and `search-chunks`. It refuses `service-audit`.

Prefer exact IDs. A complete cleanup of one document must account for its manifest partition and all chunk IDs in its `documentKey` partition. The inspect endpoint's 200-row cap cannot prove complete large-container cleanup.

## Deployment Failures

Use the controller's preview first:

```powershell
.\scripts\deploy.ps1 -Phase <phase> @target
```

Common guards:

- Plan or source hash changed: rerun `Authority`, review changes, and obtain approval again.
- Target mismatch: align Azure CLI and azd subscription/location with the reviewed target.
- Mutable image/tag: run `Build` and set the returned digest reference.
- Catalog initialization conflict: inspect and resolve the existing singleton under explicit approval; never overwrite it by rerunning bootstrap.
- Read-only catalog verification failure: check the exact execution, target, identity, image and result logs; a seed digest does not select runtime configuration.
- Temporary job remains: run `OperationsCleanup` only after E2E gates and explicit approval.

Do not use direct `az containerapp update`, direct Bicep deployment, or direct Function publishing as recovery shortcuts.
