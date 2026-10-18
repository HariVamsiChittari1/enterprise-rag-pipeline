# Local multi-SharePoint ingestion and retrieval test runbook

Use this runbook to validate two SharePoint sources from a developer
workstation before an Azure deployment. Run the checks in order and stop at
the first failed checkpoint.

## Goal and boundaries

The local milestone is complete when:

- source registration, activation, and source-specific full sync pass for two
  approved development SharePoint drives;
- each source-specific run changes only its target source;
- the parent full sync reports one independent successful outcome per active
  source;
- authorization and concurrency failure cases return their documented status
  codes; and
- the focused retrieval gateway and source-policy tests pass.

The local Functions host uses approved Azure development SharePoint, Key
Vault, Azure OpenAI, and Cosmos DB resources. Azurite holds local Durable
Functions state. Do not use production resources.

Direct retrieval can run on localhost against approved Azure development
Cosmos DB, Azure OpenAI, Microsoft Graph, and the runtime catalog. This uses
the explicit non-production service principal configured by
`AZURE_AUTH_MODE=service_principal` and localhost-only emulation of the trusted
Function-to-retrieval headers. It validates retrieval, ACL, lifecycle, and
citations, but it does not validate EasyAuth or managed-identity token exchange;
that identity chain remains an Azure deployment test.

## Safety rules

- Use only approved non-sensitive development SharePoint sites and drives.
- Confirm the Azure tenant and subscription before starting the host.
- Never print, copy, or commit secrets or `app\local.settings.json`.
- Use the synthetic operator header only with `localhost`.
- Do not load timers, webhooks, purge, or retirement functions for this test.
- Do not retire a source during testing; retirement is final.
- Record run IDs, counts, status codes, and errors, not document content.

## Prerequisites

Complete the Azure identity, local environment, Azurite, settings, network,
and RBAC setup in [Azure-backed local development](LOCAL_DEVELOPMENT.md).

You need:

- PowerShell 7;
- Python 3.12 in `C:\DevEnvs\enterprise-rag-pipeline\.venv`;
- Azure Functions Core Tools;
- Docker Desktop with Azurite running;
- the explicit service-principal variables loaded through
  [local Stage 5](LOCAL_DEVELOPMENT.md#stage-5-load-the-approved-local-application-identity);
- optionally, Azure CLI signed into the approved development target only for
  deployed-configuration discovery; it is not an application credential;
- an ignored `app\local.settings.json` with approved development settings;
- two SharePoint site URLs and document-library drive IDs; and
- permission to read the SharePoint drives and required Azure resources.

## Inspect one file's Graph and SharePoint ACL

For a single file, use the standalone diagnostic instead of manually
requesting separate Graph and SharePoint tokens. It reads the certificate from
the ignored local settings and Key Vault, then prints direct Entra groups,
SharePoint site groups, their members, and nested security-group IDs. It never
prints tokens, the PFX, or private-key material. The output also lists every
read-only API call, HTTP status, and URL; JSON output stores these under
`requests`.

Use the project virtual environment and replace only the three SharePoint
inputs:

```powershell
$siteUrl = 'https://<tenant>.sharepoint.com/sites/<site-name>'
$driveId = '<drive-id>'
$itemId = '<drive-item-id>'

& $python .\tools\inspect_sharepoint_acl.py `
  --site-url $siteUrl `
  --drive-id $driveId `
  --item-id $itemId
```

For machine-readable output, use `--format json`; to save it outside the
repository, use `--output 'C:\DevEnvs\enterprise-rag-pipeline\acl.json'`.
The command uses `app\local.settings.json` by default. Override it with
`--settings-path` only when testing a separate approved settings file.

## Test evidence

Record evidence in the workspace tracker:
`.copilot-tasks\multi-sharepoint-manual-validation.md`.

For each live run, record:

| Field | Required evidence |
| --- | --- |
| Test date | Local date and time |
| Git state | Branch and commit, plus whether the worktree was dirty |
| Source | `sourceId` only; do not record sensitive content |
| Run | Durable instance ID |
| Result | Terminal runtime status and failed-document count |
| Isolation | Other source's run/document state before and after |
| Failure | Error code and relevant sanitized host log |

## Stage 1: Open the repository

Open PowerShell in the repository root:

```powershell
Set-Location 'C:\Users\hchittari\OneDrive - Microsoft\Work\Project\BFL\enterprise-rag-pipeline'
.\scripts\initialize-dev-environment.ps1
$devRoot = $env:DEV_ARTIFACTS_ROOT
$python = "$devRoot\.venv\Scripts\python.exe"
```

Confirm the tools and Azure context:

```powershell
& $python --version
func --version
docker version
az account show --query '{tenant:tenantId,subscription:id,name:name}' -o table
git --no-pager branch --show-current
git --no-pager rev-parse --short HEAD
```

**Checkpoint 1:** stop if Python, Functions Core Tools, or Docker is
unavailable, or if the tenant/subscription is not the approved development
environment.

## Stage 2: Run the focused automated gate

```powershell
& $python -m pytest `
  tests\app_runtime\test_source_routes.py `
  tests\app_runtime\test_source_full_sync_route.py `
  tests\app_runtime\test_multi_source_dispatch.py `
  tests\app_runtime\test_operator_routes.py `
  tests\app_runtime\test_query_gateway.py `
  tests\retrieval\test_source_policy.py `
  -q
```

This gate covers source registration and state changes, source-specific sync,
parent dispatch, operator authorization, direct ACA query authentication, and retrieval
source policy.

**Checkpoint 2:** all tests must pass before starting an Azure-connected local
host.

## Stage 3: Verify settings without exposing values

```powershell
Test-Path .\app\local.settings.json
git status --short -- .\app\local.settings.json
```

The first command must print `True`; the second must print nothing.

Check only the presence of required setting names:

```powershell
$settings = Get-Content .\app\local.settings.json -Raw | ConvertFrom-Json
$required = @(
  'FUNCTIONS_WORKER_RUNTIME',
  'AzureWebJobsStorage',
  'SHAREPOINT_TENANT_ID',
  'SHAREPOINT_APP_CLIENT_ID',
  'KEY_VAULT_URI',
  'COSMOS_ENDPOINT',
  'COSMOS_DATABASE_NAME',
  'OPENAI_ENDPOINT'
)

$missing = foreach ($name in $required) {
  $property = $settings.Values.PSObject.Properties[$name]
  if ($null -eq $property -or
      [string]::IsNullOrWhiteSpace([string]$property.Value) -or
      [string]$property.Value -match '^<.*>$') {
    $name
  }
}

if ($missing) {
  throw "Missing or placeholder settings: $($missing -join ', ')"
}
```

**Checkpoint 3:** stop if the settings file is tracked, a required setting is
missing, or a placeholder remains.

## Stage 4: Verify Azurite and Azure connectivity

Confirm Azurite is running and all three service ports respond:

```powershell
docker ps --filter "name=^bfl-rag-local-azurite$" `
  --format "{{.Names}} {{.Status}}"

10000..10002 | ForEach-Object {
  Test-NetConnection 127.0.0.1 -Port $_ -InformationLevel Quiet
}
```

Verify Cosmos network access without printing the endpoint:

```powershell
$cosmosEndpoint = [Uri]$settings.Values.COSMOS_ENDPOINT
Resolve-DnsName $cosmosEndpoint.Host
Test-NetConnection $cosmosEndpoint.Host -Port 443
```

**Checkpoint 4:** Azurite must be `Up`, all three local port checks must be
`True`, and the Cosmos port check must succeed.

## Stage 5: Start the controlled Functions host

In terminal 1, from the repository root:

```powershell
$functions = @(
  'register_source', 'change_source_state', 'list_sources',
  'start_source_full_sync', 'start_full_sync', 'get_status',
  'terminate_ingestion', 'inspect_data',
  'multi_source_full_sync_orchestrator', 'full_sync_orchestrator',
  'full_sync_absence_reconcile_orchestrator',
  'prepare_source_inventory_page_activity',
  'complete_source_dispatch_activity',
  'list_source_dispatch_page_activity',
  'record_source_dispatch_outcome_activity',
  'list_source_dispatch_outcomes_page_activity',
  'activate_run_activity', 'discover_all_activity',
  'list_discovered_documents_page_activity',
  'claim_document_work_activity', 'process_document_activity',
  'timeout_wave_documents_activity', 'fail_wave_documents_activity',
  'finalize_run_activity',
  'full_sync_absence_reconcile_page_activity',
  'commit_delta_cursor_activity', 'settle_drive_coordinator_activity',
  'mark_drive_coordinator_draining_activity',
  'acl_resync_page_activity', 'lifecycle_reconcile_page_activity',
  'duplicate_version_reconcile_page_activity',
  'orphan_chunk_reconcile_page_activity', 'delta_sync_activity'
)

$env:PATH = "$devRoot\.venv\Scripts;$env:PATH"
$env:VIRTUAL_ENV = "$devRoot\.venv"
$env:PYTHONPYCACHEPREFIX = "$devRoot\pycache"
$env:WAVE_TIMEOUT_MINUTES = '60'

Set-Location .\app
func start --port 7071 --functions $functions
```

Keep this terminal open.

**Checkpoint 5:** Core Tools must list the source routes, status route, two
full-sync starters, required orchestrators, and activities. Stop the host if it
loads timers, webhooks, purge, or retirement functions.

## Stage 6: Create the localhost operator header

Open terminal 2 in the repository root:

```powershell
$base = 'http://localhost:7071'
$settings = Get-Content .\app\local.settings.json -Raw | ConvertFrom-Json
$tenantId = $settings.Values.SHAREPOINT_TENANT_ID
$operatorObjectId = '<approved-local-operator-object-id>'

$claims = @{ claims = @(
  @{ typ = 'tid'; val = $tenantId },
  @{ typ = 'roles'; val = 'Ingestion.Operator' },
  @{ typ = 'http://schemas.microsoft.com/identity/claims/objectidentifier'; val = $operatorObjectId }
) } | ConvertTo-Json -Compress -Depth 3

$principal = [Convert]::ToBase64String(
  [Text.Encoding]::UTF8.GetBytes($claims)
)
$operator = @{ 'X-MS-CLIENT-PRINCIPAL' = $principal }
```

Core Tools does not run App Service Authentication. This header emulates the
already-authenticated claims that Azure supplies to the Function. It is not a
general authentication bypass. Reprovision requires the immutable Entra object
ID claim so its destructive audit receipt identifies the operator.

Verify the boundary:

```powershell
curl.exe -s -o NUL -w "%{http_code}`n" `
  "$base/api/ingestion/sources"

curl.exe -s -o NUL -w "%{http_code}`n" `
  -H "X-MS-CLIENT-PRINCIPAL: $principal" `
  "$base/api/ingestion/sources"
```

**Checkpoint 6:** the request without the header must return `403`; the valid
operator request must return `200`.

## Stage 7: List existing sources

Always inspect the registry before creating anything:

```powershell
$inventory = Invoke-RestMethod -Method Get `
  -Uri "$base/api/ingestion/sources" `
  -Headers $operator

$inventory.sources |
  Format-Table sourceId, siteUrl, driveId, desiredState, etag
```

If both intended sources already exist, keep their stable `sourceId` values
and current ETags. Do not register duplicate records.

**Checkpoint 7:** the response must contain only accepted source records.
Resolve registry errors before continuing.

## Stage 8: Register missing sources

Replace the placeholders with approved development values:

```powershell
$sourceARequest = @{
  sourceId = 'source-a'
  siteUrl  = 'https://<tenant>.sharepoint.com/sites/<site-a>'
  driveId  = '<drive-a-id>'
}

$sourceBRequest = @{
  sourceId = 'source-b'
  siteUrl  = 'https://<tenant>.sharepoint.com/sites/<site-b>'
  driveId  = '<drive-b-id>'
}
```

Register source A:

```powershell
$sourceA = Invoke-RestMethod -Method Post `
  -Uri "$base/api/ingestion/sources" `
  -Headers $operator `
  -ContentType 'application/json' `
  -Body ($sourceARequest | ConvertTo-Json)
$sourceA | Format-List
```

Register source B:

```powershell
$sourceB = Invoke-RestMethod -Method Post `
  -Uri "$base/api/ingestion/sources" `
  -Headers $operator `
  -ContentType 'application/json' `
  -Body ($sourceBRequest | ConvertTo-Json)
$sourceB | Format-List
```

Expected for each new source:

- HTTP `201`;
- correct `sourceId`, `siteUrl`, and `driveId`;
- `desiredState` equals `paused`; and
- a non-empty `etag`.

Important errors:

| Status | Meaning |
| --- | --- |
| `409` | The source ID or drive is already reserved |
| `422 source_validation_failed` | The drive does not belong to the supplied site |
| `503 graph_unavailable` | Graph, identity, certificate, or network access failed |
| `503 source_registry_unavailable` | Cosmos access failed |

**Checkpoint 8:** both intended sources must exist as independent paused
records before activation.

## Stage 9: Activate each source

Activation requires the current ETag and does not start ingestion.

```powershell
$sourceA = Invoke-RestMethod -Method Patch `
  -Uri "$base/api/ingestion/sources/$($sourceA.sourceId)" `
  -Headers ($operator + @{ 'If-Match' = $sourceA.etag }) `
  -SkipHeaderValidation `
  -ContentType 'application/json' `
  -Body '{"desiredState":"active"}'

$sourceB = Invoke-RestMethod -Method Patch `
  -Uri "$base/api/ingestion/sources/$($sourceB.sourceId)" `
  -Headers ($operator + @{ 'If-Match' = $sourceB.etag }) `
  -SkipHeaderValidation `
  -ContentType 'application/json' `
  -Body '{"desiredState":"active"}'
```

`-SkipHeaderValidation` is needed because PowerShell 7 otherwise rejects some
raw Cosmos ETag values.

**Checkpoint 9:** both calls must return `200`, `desiredState=active`, and a
new ETag. For `412 source_etag_mismatch`, list the sources again and make a
deliberate retry with the current ETag.

## Stage 10: Create the status polling helper

```powershell
function Wait-Ingestion {
  param([Parameter(Mandatory)][string]$InstanceId)

  do {
    $status = Invoke-RestMethod -Method Get `
      -Uri "$base/api/ingestion/status?instanceId=$InstanceId&includeSourceOutcomes=true" `
      -Headers $operator

    $status |
      Select-Object instanceId, runtimeStatus, output, sourceOutcomes

    if ($status.runtimeStatus -notin @(
      'Completed', 'Failed', 'Terminated', 'Canceled'
    )) {
      Start-Sleep -Seconds 5
    }
  } while ($status.runtimeStatus -notin @(
    'Completed', 'Failed', 'Terminated', 'Canceled'
  ))

  return $status
}
```

## Stage 11: Capture the pre-run baseline

Capture bounded document counts without copying content:

```powershell
$beforeA = Invoke-RestMethod -Method Get `
  -Uri "$base/api/ingestion/inspect?container=source-documents&sourceId=$($sourceA.sourceId)&limit=200" `
  -Headers $operator
$beforeB = Invoke-RestMethod -Method Get `
  -Uri "$base/api/ingestion/inspect?container=source-documents&sourceId=$($sourceB.sourceId)&limit=200" `
  -Headers $operator

@($beforeA.rows) + @($beforeB.rows) |
  Group-Object sourceId |
  Select-Object Name, Count
```

If a source has more than 200 records, the inspection endpoint is insufficient
for a complete count. Treat it as a bounded sample and use approved Cosmos
diagnostics for full evidence.

## Stage 12: Run source A only

```powershell
$runA = Invoke-RestMethod -Method Post `
  -Uri "$base/api/ingestion/sources/$($sourceA.sourceId)/full-sync" `
  -Headers $operator

$statusA = Wait-Ingestion -InstanceId $runA.id
$statusA | ConvertTo-Json -Depth 10
```

Pass criteria:

- the start request returns HTTP `202` and an instance ID;
- the terminal status is `Completed`;
- the output reports zero failed documents; and
- source B has no new run or document changes.

Record the source A instance ID and outcome in the tracker.

## Stage 13: Run source B only

```powershell
$runB = Invoke-RestMethod -Method Post `
  -Uri "$base/api/ingestion/sources/$($sourceB.sourceId)/full-sync" `
  -Headers $operator

$statusB = Wait-Ingestion -InstanceId $runB.id
$statusB | ConvertTo-Json -Depth 10
```

Apply the same pass criteria with source A unchanged. Record the source B
instance ID and outcome.

## Stage 14: Verify source isolation

```powershell
$afterA = Invoke-RestMethod -Method Get `
  -Uri "$base/api/ingestion/inspect?container=source-documents&sourceId=$($sourceA.sourceId)&limit=200" `
  -Headers $operator
$afterB = Invoke-RestMethod -Method Get `
  -Uri "$base/api/ingestion/inspect?container=source-documents&sourceId=$($sourceB.sourceId)&limit=200" `
  -Headers $operator

@($afterA.rows) + @($afterB.rows) |
  Group-Object sourceId |
  Select-Object Name, Count
```

Verify:

- records retain the correct `sourceId`;
- one source's run does not rewrite the other source's records;
- document IDs remain stable across an unchanged rerun; and
- both source coordinators settle after successful runs.

## Stage 15: Run the parent multi-source sync

```powershell
$parentRun = Invoke-RestMethod -Method Post `
  -Uri "$base/api/ingestion/full-sync" `
  -Headers $operator

$parentStatus = Wait-Ingestion -InstanceId $parentRun.id
$parentStatus.sourceOutcomes |
  Format-Table sourceId, status, instanceId
```

Pass criteria:

- the parent returns one outcome per active source;
- both child runs complete with zero failures;
- child instance IDs are distinct; and
- one source cannot block the other source from starting.

## Stage 16: Run negative tests

Run these cases before declaring local ingestion complete:

| Test | Expected result |
| --- | --- |
| Omit the operator header | `403 operator_role_required` |
| Use a malformed or wrong-tenant principal | `403 operator_role_required` |
| Register the same drive under another source ID | `409` |
| PATCH without `If-Match` | `428 if_match_required` |
| PATCH with a stale ETag | `412 source_etag_mismatch` |
| Full-sync a paused source | `409 source_not_active` |
| Full-sync an unknown source | `404 source_registration_missing` |
| Start another run while a source is busy | `409 source_busy` |

Do not retire a source merely to test a negative transition. Retirement is
irreversible and belongs in a separately approved destructive test.

### Approved retired-source rebuild

Use this only after retirement cleanup completed and only when rebuilding the
same source ID, site, and drive is explicitly approved. Reprovision preserves
the immutable binding and audit history, clears old source operational state,
and returns the source to `paused`.

```powershell
$retired = (Invoke-RestMethod -Method Get `
  -Uri "$base/api/ingestion/sources" `
  -Headers $operator).sources |
  Where-Object sourceId -eq "poc-auth"

$body = @{ confirm = "reprovision" } | ConvertTo-Json -Compress

# Validate the retired source and start bounded cleanup.
$reset = Invoke-RestMethod -Method Post `
  -Uri "$base/api/ingestion/sources/poc-auth/reprovision" `
  -Headers ($operator + @{ "If-Match" = $retired.etag }) `
  -ContentType "application/json" `
  -Body $body

$resetStatus = Wait-Ingestion -InstanceId $reset.id
```

Require `status=completed`, `desiredState=paused`, and
`fullSyncRequired=true`. Call the source-specific `full-sync` endpoint while
the source remains paused. After that orchestration completes successfully,
get the current ETag and activate the source with `PATCH`. Activation is blocked
until the bootstrap full sync clears the latch. Do not reuse an old delta
cursor; a full ingestion is mandatory after retirement deleted content.

## Postman equivalent

Create a Postman environment:

| Variable | Value |
| --- | --- |
| `baseUrl` | `http://localhost:7071` |
| `principal` | The Base64 `$principal` value from Stage 6 |
| `sourceA` | Stable source A ID |
| `sourceB` | Stable source B ID |

Add these headers to local ingestion requests:

```text
X-MS-CLIENT-PRINCIPAL: {{principal}}
Content-Type: application/json
```

For PATCH requests also add:

```text
If-Match: <current-etag>
```

Run the same order as the PowerShell workflow:

1. `GET {{baseUrl}}/api/ingestion/sources`
2. `POST {{baseUrl}}/api/ingestion/sources` for each missing source
3. `PATCH {{baseUrl}}/api/ingestion/sources/{{sourceA}}`
4. `PATCH {{baseUrl}}/api/ingestion/sources/{{sourceB}}`
5. `POST {{baseUrl}}/api/ingestion/sources/{{sourceA}}/full-sync`
6. Poll `GET {{baseUrl}}/api/ingestion/status?instanceId=<id>`
7. Repeat for source B
8. `POST {{baseUrl}}/api/ingestion/full-sync`
9. Poll status with `includeSourceOutcomes=true`

## Retrieval validation

### What is validated locally

The focused automated gate validates:

- delegated-user ACA query claims;
- source lifecycle eligibility;
- cited-source ETag revalidation; and
- source isolation policy.

The Cosmos DB Linux emulator does not provide the vector and full-text query
parity needed by this retrieval implementation. Do not treat an emulator-only
query result as cloud retrieval evidence.

### Run retrieval locally against Azure resources

Use the deployed retrieval configuration as the source of truth rather than
copying environment values by hand. Complete local Stage 5 in this same
terminal first. Azure CLI reads non-secret deployed configuration only; the
loop must not import Azure workload identity settings into the local process.

```powershell
$resourceGroup = 'rg-rag-aca-e2e-20260827'
$containerApp = 'ragacae2e20260827-retr-jztu542h'
$python = 'C:\DevEnvs\enterprise-rag-pipeline\.venv\Scripts\python.exe'

$settings = az containerapp show `
  --resource-group $resourceGroup `
  --name $containerApp `
  --query 'properties.template.containers[0].env' `
  --output json | ConvertFrom-Json

foreach ($setting in $settings) {
  $azureOnlyIdentitySettings = @(
    'AZURE_AUTH_MODE',
    'AZURE_CLIENT_ID',
    'AZURE_CLIENT_SECRET',
    'AZURE_TENANT_ID',
    'MANAGED_IDENTITY_CLIENT_ID'
  )
  if ($null -ne $setting.value -and
      $setting.name -notin $azureOnlyIdentitySettings) {
    Set-Item -Path "Env:$($setting.name)" -Value $setting.value
  }
}

$localIdentity = (
  Get-Content .\app\local.settings.json -Raw |
  ConvertFrom-Json
).Values

foreach ($name in @(
  'AZURE_AUTH_MODE',
  'AZURE_TENANT_ID',
  'AZURE_CLIENT_ID',
  'AZURE_CLIENT_SECRET'
)) {
  $value = [string]$localIdentity.$name
  if ([string]::IsNullOrWhiteSpace($value) -or $value -match '^<.*>$') {
    throw "Missing or placeholder local identity setting: $name"
  }
  Set-Item -Path "Env:$name" -Value $value
}

Remove-Item Env:MANAGED_IDENTITY_CLIENT_ID -ErrorAction SilentlyContinue
$env:APPLICATIONINSIGHTS_CONNECTION_STRING = ''
$env:PYTHONPYCACHEPREFIX = 'C:\DevEnvs\enterprise-rag-pipeline\pycache'

# Local network and developer-token latency is higher than in the VNet.
$env:RETRIEVAL_TIMEOUT_SECONDS = '30'
$env:GENERATION_TIMEOUT_SECONDS = '60'
$env:AGENT_TIMEOUT_SECONDS = '60'
$env:GRAPH_GROUP_TIMEOUT_SECONDS = '30'
$env:RETRIEVAL_OPERATION_TIMEOUT_SECONDS = '90'

Set-Location .\app
& $python -m uvicorn retrieval.main:app --host 127.0.0.1 --port 8080
```

Keep that terminal open. Startup must report `retrieval_service_started`.
Verify both health endpoints:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/health/live
Invoke-RestMethod http://127.0.0.1:8080/health/ready
```

In a second terminal, build a localhost-only ACA principal header for the
signed-in developer and execute a query. Never send this synthesized header to
a deployed endpoint; ACA injects it after validating the delegated token:

```powershell
$tenantId = 'e140e1f9-c7cb-4d87-a943-e16669bcb80a'
$retrievalAudience = '1fe3217b-c48f-4953-bec2-f56ace50a9d8'

$userId = '<approved-test-user-object-id>'

$claims = @{ claims = @(
  @{ typ = 'oid';   val = $userId },
  @{ typ = 'tid';   val = $tenantId },
  @{ typ = 'aud';   val = $retrievalAudience },
  @{ typ = 'idtyp'; val = 'user' },
  @{ typ = 'scp';   val = 'user_impersonation' }
) } | ConvertTo-Json -Compress -Depth 4

$userPrincipal = [Convert]::ToBase64String(
  [Text.Encoding]::UTF8.GetBytes($claims)
)

$headers = @{
  'X-MS-CLIENT-PRINCIPAL' = $userPrincipal
}

$body = @{
  sourceId = 'source-a'
  question = 'What are the Northstar access requirements?'
  mode = 'hybrid'
  top_k = 5
} | ConvertTo-Json

Invoke-RestMethod `
  -Method POST `
  -Uri 'http://127.0.0.1:8080/api/query' `
  -Headers $headers `
  -ContentType 'application/json' `
  -Body $body `
  -TimeoutSec 120 |
  ConvertTo-Json -Depth 10
```

Use this emulation only with `127.0.0.1` or `localhost`. It does not prove that
an external caller can forge these headers in Azure; Container Apps
authentication and the retrieval application's boundary protect the deployed
service.

### What waits for Azure

End-to-end identity testing waits for an approved deployment because ACA
built-in authentication must validate a real delegated-user token and inject
the principal consumed by the retrieval application. Retrieval then resolves
that user's transitive security groups only when the selected source enables
ACL filtering. The Function App is ingestion-only and is not a query gateway.

Do not use the localhost header emulation as a substitute for this end-to-end
identity validation.

After deployment, call the ACA retrieval FQDN with a delegated token whose
audience is `RETRIEVAL_API_AUDIENCE` and whose scope contains
`user_impersonation`. Do not send `X-MS-CLIENT-PRINCIPAL`; ACA owns that header.

```powershell
$token = '<delegated retrieval API token acquired without printing it>'
$headers = @{ Authorization = "******"; 'Content-Type' = 'application/json' }
$body = @{
  sourceId = 'source-a'
  question = '<question answerable only from source A>'
  mode = 'hybrid'
  top_k = 5
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'https://<retrieval-fqdn>/api/query' `
  -Headers $headers -Body $body
```

Repeat with:

- a user allowed only for source A;
- a user allowed only for source B;
- a user allowed for both;
- a user allowed for neither; and
- one source paused while the other remains active.

Verify citation source, URL, lifecycle exclusion, ACL exclusion, and the
grounded no-evidence response.

The final T31 development validation used two real delegated identities against
the same source and document. The authorized member received the target
citation; the verified outside-group guest received HTTP 200 with the canonical
no-evidence answer and zero citations. A successful Graph resolution with zero
security-group memberships is a valid no-authority result, not an authentication
or dependency failure. Restore any temporary membership changes before closing
the test.

## Stop and recovery

Stop the Functions and retrieval hosts with `Ctrl+C`. In every terminal that
loaded the local application credential, clear it after the child process
stops:

```powershell
'AZURE_AUTH_MODE', 'AZURE_TENANT_ID', 'AZURE_CLIENT_ID',
  'AZURE_CLIENT_SECRET' | ForEach-Object {
    Remove-Item "Env:$_" -ErrorAction SilentlyContinue
  }
```

Do not automatically abandon unresolved work. If a run leaves a source
coordinator draining, inspect the status and host logs first. Use the
documented terminate/recovery route only after confirming the target instance,
source, and unresolved operation identities.

Keep Azurite data if another local run is planned:

```powershell
docker stop bfl-rag-local-azurite
```

Do not remove approved Azure firewall or RBAC changes unless they were
temporary changes created specifically for this test and their original state
was recorded.

## Completion checklist

- [x] Focused automated gate passed.
- [x] Azure context, settings, network, identity, and RBAC passed.
- [x] Source registry inventory and intended lifecycle states verified.
- [x] Both active sources validated with current ETags.
- [x] Source A full sync completed with source B unchanged.
- [x] Source B full sync completed with source A unchanged.
- [x] Parent sync dispatched both active sources; child content finalized successfully.
- [x] Negative authorization, lifecycle, and busy cases passed.
- [x] Run IDs and sanitized evidence were recorded in the tracker.
- [x] Retrieval unit/integration gate passed.
- [x] Local Azure-backed retrieval and ACL-on/ACL-off matrix passed.
- [x] Azure delegated retrieval, modes, citations, audio time range, and pause exclusion passed.
- [ ] Azure four-principal ACL allow/deny matrix repeated with real delegated identities.
- [ ] Controlled Azure delta change and irreversible retirement decision completed.

## References

- [Project API reference](API_REFERENCE.md)
- [Multi-SharePoint requirements and status](MULTI_SHAREPOINT_SOURCES.md)
- [Azure-backed local development](LOCAL_DEVELOPMENT.md)
- [Microsoft Learn: Access user claims in application code](https://learn.microsoft.com/azure/app-service/configure-authentication-user-identities)
- [Microsoft Learn: Durable Task fan-out/fan-in](https://learn.microsoft.com/azure/durable-task/common/durable-task-fan-in-fan-out)
- [Microsoft Learn: Azure Functions Python developer reference](https://learn.microsoft.com/azure/azure-functions/functions-reference-python)
