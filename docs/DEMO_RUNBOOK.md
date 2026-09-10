# Enterprise RAG Pipeline Demo and E2E Runbook

This runbook validates the deployed Function gateway, ACA retrieval service, ingestion lifecycle, ACL trimming, scoring profiles, freshness, synonyms, telemetry, and cleanup. Use only an approved non-production test site and disposable fixtures.

## Preconditions

- The deployed Function, ACA revision, image digest, and current catalog ETag/digest are known.
- The caller can obtain a delegated token for `FUNCTION_API_AUDIENCE` with `user_impersonation`.
- Disposable SharePoint fixtures inherit the target site's permissions.
- The image is immutable; coordinate catalog editors so the expected generation remains stable during each evidence window.

```powershell
$funcApp = '<function-app-name>'
$clientId = '<function-api-client-id>'
$baseUrl = "https://$funcApp.azurewebsites.net"
$token = az account get-access-token --resource "api://$clientId" --query accessToken -o tsv
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }
```

Never print the token or persist it in reports.

## Verify Deployment Identity

Capture:

- Function App name and gateway target.
- ACA latest ready revision and 100% traffic assignment.
- Full `repository@sha256:<digest>` image reference.
- `DEPLOYMENT_INSTANCE_ID` and the inspected current catalog ETag/digest.
- `ACL_ENABLED=true`.

Any relevant Function deployment, ACA revision/traffic, image, catalog, profile, synonym, or timeout change invalidates affected live evidence.

## Audited Query Helper

```powershell
function Invoke-AuditedRagQuery([hashtable] $payload) {
  $response = Invoke-RestMethod `
    -Uri "$baseUrl/api/query" `
    -Method Post `
    -Headers $headers `
    -Body ($payload | ConvertTo-Json -Depth 5 -Compress) `
    -TimeoutSec 90

  $audit = $null
  1..15 | ForEach-Object {
    if (-not $audit) {
      $rows = (Invoke-RestMethod `
        -Uri "$baseUrl/api/ingestion/inspect?container=service-audit&limit=200" `
        -Headers $headers).rows
      $audit = $rows |
        Where-Object { $_.requestId -eq $response.request_id -and $_.operation -eq 'query_request' } |
        Select-Object -First 1
      if (-not $audit) { Start-Sleep -Seconds 2 }
    }
  }

  [pscustomobject]@{ Response = $response; Audit = $audit }
}
```

The inspect endpoint is capped at 200 rows. If an exact audit is absent from that sample, use an approved private-network, read-only Cosmos query filtered by `requestId`; do not weaken networking or expose keys.

## Retrieval Matrix

Run the repository tool against the Function gateway:

```powershell
python tools/script_query_retrieval.py `
  --function-app $funcApp `
  --client-id $clientId `
  --scenario-matrix `
  --expected-catalog-sha 'sha256:<catalog-digest>' `
  --expected-catalog-etag '<exact-cosmos-etag>' `
  --report demo-output/retrieval-scenario-matrix.json
```

Required coverage:

- Standard and agentic paths.
- Hybrid, vector, and full-text modes.
- Correct effective mode in audit.
- At least one citation for corpus-supported questions.
- `retrieval_degraded=false`.
- Exact captured catalog digest and ETag in request audit evidence.

## Direct-Edit Validation

Use [the operator procedure](AZURE_SETUP.md#catalog-observation-and-optional-writer)
with an approved editor and protected previous valid body. Prove private keyless
browser read/save, a valid config-only edit without an ACA deployment, and
stable-cohort adoption. Verify malformed persisted edits retain last-known-good
policy with degraded telemetry, then restore the protected body and prove
recovery under a new ETag. Coordinate restart testing separately: an invalid
persisted catalog must fail startup. Re-run affected retrieval/ACL scenarios
and preserve protected generation-bound evidence after restoration.

## Scoring Profiles

Use the existing `tools/script_query_retrieval.py` helper; no separate scoring demo script is required. For a quick interactive query, pass `--scoring-profile` without `--scenario-matrix`. When `--question` is omitted, the helper prompts for a question; press Enter to use the displayed verified default. It writes no report and prints the question, request parameters, answer, and citations; add `--json` only when the raw response is needed. For auditable evidence, use the matrix command below so the helper verifies path, mode, degradation, citations, selected profile, and catalog digest.

```powershell
python tools/script_query_retrieval.py `
  --function-app $funcApp `
  --client-id $clientId `
  --mode hybrid `
  --scoring-profile 'hr-relevance'
```

Run the matrix separately for each deployed profile:

| Profile | Aggregation | Synonym map |
| --- | --- | --- |
| `hr-relevance` | `sum` | `hr-en` |
| `hr-relevance-average` | `average` | `hr-en` |
| `hr-relevance-minimum` | `minimum` | none |
| `hr-relevance-maximum` | `maximum` | none |

For each run pass both `--scoring-profile` and `--expected-scoring-profile`. Require six passing scenarios, at least one citation per scenario, the exact catalog digest, and no degraded retrieval. HTTP 200 alone does not prove profile selection.

## Freshness

Freshness has no request flag; the selected profile applies it automatically from chunk `sourceModifiedAt`.

Use two authorized documents with equivalent ranking-relevant content and different service-level Microsoft Graph `driveItem.lastModifiedDateTime` values. Graph documents this service timestamp as read-only; `fileSystemInfo.lastModifiedDateTime` is a separate client facet and is not the field ingested by this pipeline.

```powershell
$result = Invoke-AuditedRagQuery @{
  question = '<query shared by the controlled documents>'
  mode = 'hybrid'
  top_k = 20
  scoring_profile = 'hr-relevance'
  expand_synonyms = $false
}
```

Require both exact source URLs in the candidate citations and record their first ranks. A newer-first result is an observational live smoke because reciprocal candidate rank also contributes to the application score. Deterministic scoring tests provide the causal freshness-function evidence. Do not tune the deployed profile or patch Cosmos merely to force the ordering.

## Synonyms

Use a controlled document containing a mapped term but none of the original query terms. For `hr-en`, a document containing `vacation` but neither `annual` nor `leave` can test the query `annual leave`.

```powershell
$on = Invoke-AuditedRagQuery @{
  question = 'annual leave'
  mode = 'full_text'
  top_k = 20
  scoring_profile = 'hr-relevance'
  expand_synonyms = $true
}

$off = Invoke-AuditedRagQuery @{
  question = 'annual leave'
  mode = 'full_text'
  top_k = 20
  scoring_profile = 'hr-relevance'
  expand_synonyms = $false
}
```

Require:

- Enabled audit: `synonym_map=hr-en`, exact profile/catalog, standard path, full-text mode, and non-degraded retrieval.
- Disabled audit: `synonym_map=null` with the same profile/catalog/path/mode.
- Enabled response cites the exact controlled URL.
- Disabled response does not cite that URL.

## Ingestion and Lifecycle

Start full sync and use the returned status URL:

```powershell
$start = Invoke-RestMethod -Uri "$baseUrl/api/ingestion/full-sync" -Method Post -Headers $headers
$status = Invoke-RestMethod -Uri $start.statusQueryGetUri -Headers $headers
$status.output | Select-Object status, runStatus, discovered, succeeded, failed
```

Validate:

- Source manifests become `ready` only after expected and written chunk counts match.
- Chunks have `isRetrievable=true` and the same `lifecycleGeneration` as the ready manifest.
- Representative Markdown, PDF, DOCX, PPTX, and XLSX fixtures use the expected provider route and preserve the original source identity.
- Every required visual has a persisted manifest entry and description, with zero unexplained uncovered visuals before admission.
- Representative visual queries return the expected source and typed page, section, slide, or worksheet locator in the top five; Office citations do not use PDF page fragments.
- Hidden slides and worksheets remain excluded, and detected unsupported Office objects appear in audit evidence rather than retrievable chunks.
- Delta cursor advances only after all items succeed.
- ACL revocation removes retrieval eligibility; a valid unchanged source version can be restored.
- Source deletion and supersession hard-delete document/chunk versions.

`GET /api/ingestion/inspect` returns at most 200 rows and removes only Cosmos `_` system properties. It is a diagnostic sample, not a complete export API.

## Security Checks

- Unauthenticated Function requests return 401 from EasyAuth.
- Authorized group membership returns evidence.
- A denied principal returns no citations.
- Public callers cannot call ACA directly; ACA Authentication accepts only the Function UAMI application/principal.
- SharePoint notifications with an invalid `clientState` return 403.
- The lifecycle webhook is excluded from EasyAuth and currently does not validate `clientState`; record this known gap rather than treating it as a passing security control.

## Telemetry Checks

For each query, verify `query_request` audit fields:

- `requestId`, `path`, `mode`, `planned_queries`.
- `catalog_version`, `scoring_profile`, `synonym_map`.
- `citations_count`, `e2e_latency_ms`, `retrieval_degraded`.

Review Application Insights requests, dependencies, and exceptions for the test window. Report the observed window and counts; do not infer production SLOs from a functional demo.

## Fixture Cleanup

Record every temporary Graph item ID, document ID, document key, and source URL at creation.

1. Delete fixture files by exact Graph item ID and ETag.
2. Wait for the resulting delta orchestration to complete with the expected deletion count.
3. Verify no manifest remains for each item ID.
4. Verify zero chunks in each exact `documentKey` partition through an approved private read path.
5. Verify queries return no temporary source URL.
6. Delete the empty temporary folder and verify Graph returns 404.

If an invalid fixture creates a failed manifest without chunks, remove only that exact manifest through the authenticated targeted purge endpoint and retain the purge audit ID.

## Temporary Catalog Job Cleanup

After all E2E and fixture-cleanup gates pass:

1. Verify the exact job name/resource ID, `DeploymentInstance`, `Temporary=true`, immutable image, publisher command, and absence of running executions.
2. Preview `scripts/deploy.ps1 -Phase OperationsCleanup` with current reviewed hashes and target arguments.
3. Obtain explicit approval.
4. Execute with `-Execute`.
5. Verify no tagged job remains.
6. Run a final authenticated retrieval smoke test.

## Evidence Report

Write a token-free report under `demo-output/` containing deployment identity, report paths, request IDs, aggregate results, cleanup proof, telemetry counts, limitations, and residual risks. Exclude tokens, questions, answers, document content, and principal/group identifiers.
