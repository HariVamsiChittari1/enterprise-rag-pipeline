---
title: Environment Variable Reference
description: Deployment inputs and runtime configuration for ingestion and retrieval
---

This is the single inventory for deployment inputs and runtime environment variables used by ingestion, ACA retrieval, and the temporary catalog publisher.

## Ownership Rules

| Classification | Who sets it | Guidance |
| --- | --- | --- |
| Client deployment input | Operator through the selected azd environment or guarded phase arguments | Set and review before running `scripts/deploy.ps1`. |
| Generated runtime setting | Bicep or the deployment controller | Do not hand-edit in the deployed Function or Container App; change the owning Bicep input and redeploy. |
| Runtime fallback | Application code | Used when Bicep does not emit an override. Add a deployment parameter before relying on a client-specific override. |
| Catalog-owned setting | Mutable runtime catalog | Edit the singleton's `config` in Cosmos Data Explorer after runtime-catalog deployment; see the direct-edit procedure below. Do not use ACA environment overrides. |
| Secret | Protected azd secret reference or external Key Vault | Set an azd-managed secret with `azd env set-secret <name>`; never put the value in source, reports, command output, documentation, or a plaintext azd environment value. |

### Direct catalog editing

The current source supports configuration-only editing and create-only bootstrap.
Local validation does not establish deployed behavior. Use this procedure only
after deploying and verifying the current runtime contract; never reset existing
Cosmos data to bypass a bootstrap conflict.

After that deployment, use Cosmos Data Explorer with approved data-plane access
and private-network reachability to the configured retrieval-config container:

Open the item with `id` equal to `runtime-catalog`; `/id` is the container
partition key, so `runtime-catalog` is both the item ID and partition-key value.
Keep `id` and `type: retrieval-runtime-catalog` unchanged. Do not edit Cosmos
system fields.

Retain a protected previous valid item outside the repository before editing.
Edit only `config`: `retrieval`, `profiles`, `synonymMaps`, and optional
`defaultProfile`. Use the [source example](../app/retrieval/catalog.example.json)
for the config structure. Definitions make profiles and maps available;
profile selection and map/function references apply their behavior. There are
no feature switches.

Save the whole item. No operation UUID or timestamp is required. Optional
`change` metadata, when present, must remain structurally valid; it does not
identify a later direct edit. Cosmos `_etag` and the validated configuration
digest identify the serving generation.

Allow the configured polling interval before checking adoption. The source
accepts `RETRIEVAL_CATALOG_POLL_SECONDS` from 60 through 86,400 seconds and
defaults to 7,200 only when unset. Changing relevance values needs no
application deployment; changing the polling setting is deployment-owned.

Check each replica's observed ETag hash and digest, not only the stored item or
optional writer metadata. The read-only `observe` command in the
[setup guide](AZURE_SETUP.md#catalog-observation-and-optional-writer) requires an
unchanged, ready cohort bracketing observations for at least 30 seconds. Allow
the poll interval, a five-second read deadline, and telemetry ingestion and
inventory time. The observer defaults to a 300-second deadline, so set a longer
deadline than the poll interval when needed, up to 86,460 seconds. Missing logs,
unready revisions, restarts, membership changes, or degraded health cannot prove
adoption. Export latency and process identity must be verified in the target.

The loader validates the entire item before atomically replacing its immutable
snapshot. Invalid edits or read failures retain the running process's
last-known-good configuration with degraded health. A new process starts on a
built-in baseline when the item is absent, but a present item must be valid or
startup fails closed. Restore the protected previous valid body if an edit
is rejected; it is accepted under its new Cosmos ETag, without generating a new
operation UUID.

Direct edits do not guarantee guarded-writer history, automatic rollback, or
semantic audit attribution. Coordinate concurrent editors; this procedure does
not claim portal conflict detection. Data-plane authorization is not restricted
to this one item. The guarded writer remains optional, with its own validation,
history, and ETag checks; its audit/replica assurance must not be
reported as complete. Never delete the container or unrelated ingestion, ACL,
document, or chunk data to change relevance configuration.

`azd env set` writes a plaintext value to the local azd environment. Do not use it for `WEBHOOK_CLIENT_STATE` or another secret. Use `azd env set-secret WEBHOOK_CLIENT_STATE` interactively, or supply the value from an organization-approved external secret source used by the guarded deployment controller.

## Client Deployment Inputs

These values configure `infra/main.parameters.bicepparam` and `scripts/deploy.ps1`.

### Target and authority

| Variable or argument | Required | Default/example | Purpose |
| --- | --- | --- | --- |
| `AZURE_SUBSCRIPTION_ID` | Yes | Subscription GUID | azd target subscription. Must match the guarded `-SubscriptionId` argument and Azure CLI context. |
| `AZURE_LOCATION` | Yes | Azure region such as `eastus2` | Resource deployment region. Must match guarded `-Location`. |
| `DEPLOYMENT_INSTANCE_ID` | Yes | Stable value such as `client-prod` | Resource tag/name discriminator and guarded deployment target. The controller sets this process variable from `-DeploymentInstanceId`; it is not stored in the retrieval catalog. |
| `COST_CENTER` | Yes | Organization-defined tag | Required resource tag. |
| `CLEANUP_DATE` | Yes | `YYYY-MM-DD` | Required governance tag. It does not automatically delete resources. |

The guarded controller also requires reviewed `ExpectedPlanHash`, `ExpectedSourceTreeHash`, tenant, resource group, azd environment, and deployment instance arguments. These are command parameters, not application environment variables.

### Existing Azure OpenAI

| Variable | Required | Default/example | Purpose |
| --- | --- | --- | --- |
| `AZURE_OPENAI_ACCOUNT_NAME` | Yes | Existing account name | Existing Azure OpenAI account consumed by Function and retrieval. |
| `AZURE_OPENAI_RESOURCE_GROUP` | Yes | Existing resource group | Resource group in the deployment subscription containing the account. |
| `OPENAI_EMBEDDING_DEPLOYMENT_NAME` | No | `text-embedding-3-large` | Existing 3,072-dimensional embedding deployment. |
| `OPENAI_CHAT_DEPLOYMENT_NAME` | Yes | Existing chat deployment | Chat model used for bounded visual descriptions, planning, standard generation, and agent generation. |

The deployment does not create the Azure OpenAI account or model deployments and does not configure their capacity.

### Content Understanding

| Variable | Required | Default/example | Purpose |
| --- | --- | --- | --- |
| `DOCUMENT_INTELLIGENCE_ENABLED` | No | `true` | Enables Document Intelligence as an extraction provider. |
| `CONTENT_UNDERSTANDING_ENABLED` | No | `false` | Provisions Content Understanding and enables it as an extraction provider. |
| `CONTENT_UNDERSTANDING_ANALYZER_ID` | When Content Understanding is enabled | `prebuilt-documentSearch` | Analyzer ID verified by the guarded Final phase and injected into the Function App. |

When extraction is enabled, at least one provider must be enabled. Content Understanding takes precedence when both providers are enabled; the Function constructs only the selected provider client. Markdown extraction remains direct and provider-independent. Document Intelligence uses `prebuilt-layout` plus Azure OpenAI vision for required visual descriptions. The Document Intelligence `2024-11-30` API is generally available. The Content Understanding production API is `2025-11-01`; `2026-06-01-preview` is a separate preview API without an SLA. These API versions are service status facts, not configurable environment variables in this deployment; the pinned Python client packages own data-plane compatibility.

Content Understanding uses `prebuilt-documentSearch`. Microsoft can update a prebuilt analyzer definition even when its ID remains unchanged, so production environments that require invariant analyzer behavior should copy and govern an analyzer definition instead of treating this ID as a version pin. Confirm regional account, model, and analyzer availability before enabling the provider.

### SharePoint and certificate

| Variable | Required | Default/example | Purpose |
| --- | --- | --- | --- |
| `SHAREPOINT_TENANT_ID` | Yes | Tenant GUID | Tenant for SharePoint certificate auth and Function/retrieval identity validation. |
| `SHAREPOINT_APP_CLIENT_ID` | Yes | Application client GUID | Certificate-authenticated SharePoint ingestion application. |
| `SHAREPOINT_ASSIGNED_DRIVE_ID` | Legacy deployment input | Existing Graph drive ID | Still emitted by Bicep for compatibility; multi-source dispatch does not use it as a catalog fallback. |
| `SHAREPOINT_SITE_URL` | Legacy deployment input | `https://tenant.sharepoint.com/sites/site` | Still emitted by Bicep for compatibility; each runtime source uses its validated registry value. |
| `INGESTION_SOURCE_ID` | Legacy deployment input | Stable identifier such as `sharepoint-drive` | Still used to derive `TASKHUB_NAME` and as an audit fallback; multi-source dispatch reads source IDs from Cosmos. |
| `SHAREPOINT_KEY_VAULT_NAME` | Yes | Existing vault name | Existing vault holding the certificate PFX secret. Must be in the deployment subscription. |
| `SHAREPOINT_KEY_VAULT_RESOURCE_GROUP` | Yes | Existing resource group | Resource group containing that vault in the deployment subscription. |
| `SHAREPOINT_CERTIFICATE_SECRET_NAME` | No | `sharepoint-app-cert` | Key Vault secret containing the exportable PFX bytes. |

### Function and retrieval API identities

| Variable | Required | Default/example | Purpose |
| --- | --- | --- | --- |
| `ADMIN_API_CLIENT_ID` | Yes | Function API application client GUID | EasyAuth provider registration protecting Function routes. |
| `FUNCTION_API_AUDIENCE` | Yes | Usually `api://<function-api-client-id>` | Exact audience accepted by Function EasyAuth. |
| `FUNCTION_ALLOWED_CALLER_CLIENT_ID` | Yes | Approved client application GUID | Sole caller application inserted into the Function EasyAuth allowlist. |
| `RETRIEVAL_API_CLIENT_ID` | Yes | Retrieval API client GUID | ACA Authentication application registration. |
| `RETRIEVAL_ALLOWED_CALLER_CLIENT_ID` | Yes | Approved native/public client GUID | Sole delegated caller application inserted into the retrieval ACA Authentication allowlist. |
| `RETRIEVAL_API_SERVICE_PRINCIPAL_ID` | Yes | Service-principal object GUID | External validation/evidence input for the retrieval API service principal. Active Bicep re-outputs it but does not create app-role assignments. |
| `WEBHOOK_CLIENT_STATE` | Yes, secret | Cryptographically random value | Shared secret checked by `/api/webhook/sharepoint`. The lifecycle webhook currently does not validate it. |

The retrieval UAMI requires its Graph application permissions. This is an external Entra assignment.

### Capacity and reliability

| Variable | Required | Default | Allowed values/effect |
| --- | --- | --- | --- |
| `COSMOS_DB_MODE` | No | `serverless` | `serverless` or `provisioned`. |
| `COSMOS_METADATA_AUTOSCALE_MAX_RUS` | No | `1000` | Autoscale maximum for metadata containers in provisioned mode; minimum 1,000. |
| `COSMOS_SEARCH_AUTOSCALE_MAX_RUS` | No | `1000` | Dedicated `search-chunks` autoscale maximum in provisioned mode; minimum 1,000. |
| `STORAGE_REDUNDANCY` | No | `ZRS` | `LRS`, `ZRS`, or `GRS`. |
| `APPLICATION_INSIGHTS_DAILY_CAP_GB` | No | `5` in parameter file | Positive value configures a daily cap; Bicep module supports `-1` for unlimited. |
| `RETRIEVAL_MIN_REPLICAS` | No | `1` | ACA minimum replicas; minimum 1. |
| `RETRIEVAL_MAX_REPLICAS` | No | `5` | ACA maximum replicas; minimum 1 and must not be lower than the minimum. |
| `RETRIEVAL_ZONE_REDUNDANT` | No | `false` | ACA managed-environment creation-time zone-redundancy setting. |

### Build and immutable artifacts

| Variable | When required | Source | Purpose |
| --- | --- | --- | --- |
| `ACR_NAME` | Build, operations | Foundation output/operator selection | Registry used by the guarded build and operations phases. |
| `RELEASE_BUILD_ID` | Build | Reviewed release identifier | Temporary image tag used during ACR build; must match the controller's restricted pattern. |
| `RETRIEVAL_IMAGE_REFERENCE` | Operations and Final | Output from executed `Build` | Immutable `registry/repository@sha256:<64 lowercase hex>`. |
| `RETRIEVAL_CATALOG_DIGEST` | Explicit initialization only | Output from seed validation | Reviewed seed digest; never injected as a runtime selection pin. Ordinary redeployment verifies the current singleton. |
| `RETRIEVAL_CATALOG_POLL_SECONDS` | Optional | Default `7200` only when absent | Integer `60` through `86400`; malformed or blank values fail. Requires guarded serving redeployment. |
| `CATALOG_EDITOR_PRINCIPAL_ID` | Optional | Reviewed human principal object ID | Container-scoped metadata/read/replace/query/change-feed access. No create, delete, or upsert. |
| `CATALOG_WRITER_PRINCIPAL_ID` | Optional | Reviewed optional writer object ID | Editor actions plus create for guarded history/outcome records. No delete or upsert. |
| `CATALOG_OBSERVER_PRINCIPAL_ID` | Optional | Reviewed observer object ID | App-scoped Reader and workspace Log Analytics Reader. No catalog writes. |

`DEPLOY_SERVING` and `DEPLOY_OPERATIONS` are internal phase switches set by `scripts/deploy.ps1`; clients should not set them manually.

## Function Runtime: Ingestion and Gateway

The active Function Bicep module injects the following settings. Values shown as “generated” come from resources or reviewed deployment inputs.

### Identity, host, and platform settings

| Variable | Value/default | Purpose |
| --- | --- | --- |
| `FUNCTIONS_EXTENSION_VERSION` | `~4` | Azure Functions runtime major version. |
| `AzureWebJobsStorage__blobServiceUri` | Generated private blob endpoint | Functions host/deployment storage using identity-based connections. |
| `AzureWebJobsStorage__queueServiceUri` | Generated private queue endpoint | Durable/Functions queue storage. |
| `AzureWebJobsStorage__tableServiceUri` | Generated private table endpoint | Durable/Functions table storage. |
| `AzureWebJobsStorage__credential` | `managedidentity` | Selects managed-identity storage authentication. |
| `AzureWebJobsStorage__clientId` | Function UAMI client ID | Identity used for host storage. |
| `AZURE_AUTH_MODE` | `managed_identity` | Selects the explicit Azure-hosted credential path. |
| `AZURE_CLIENT_ID` | Function UAMI client ID | User-assigned identity selected by the shared credential factory. |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | Generated, secret | Application Insights telemetry connection. Do not print. |
| `APPLICATIONINSIGHTS_AUTHENTICATION_STRING` | `ClientId=<Function-UAMI>;Authorization=AAD` | Entra authentication for Application Insights ingestion. |
| `INSTANCE_MEMORY_MB` | `2048` from current Bicep default | Informational Function instance-memory setting emitted by Bicep. Allowed Bicep values: 512, 1024, 2048, 4096. |

### Durable orchestration

| Variable | Value/default | Purpose |
| --- | --- | --- |
| `DURABLE_TASK_SCHEDULER_CONNECTION_STRING` | Generated endpoint plus Function UAMI | Managed-identity Durable Task Scheduler connection. |
| `TASKHUB_NAME` | Generated from legacy `INGESTION_SOURCE_ID`, maximum 45 characters | Deployment-wide Durable task hub used by all registered sources. |
| `WAVE_SIZE` | Runtime fallback `4` | Documents dispatched concurrently per full-sync wave. Current Bicep does not emit an override. |
| `WAVE_TIMEOUT_MINUTES` | Runtime fallback `20` | Deadline for one full-sync/retry wave. Current Bicep does not emit an override. |
| `PROCESS_DOCUMENT_MAX_ATTEMPTS` | Runtime fallback `5` | Maximum retries inside `process_document_activity`. Current Bicep does not emit an override. |
| `PROCESS_DOCUMENT_RETRY_DELAY_SECONDS` | Runtime fallback `60` | Base retry delay; deterministic document jitter adds 0–29 seconds. Current Bicep does not emit an override. |

`app/host.json` sets `durableTask.maxConcurrentActivityFunctions` to `1`. This matches the Python
worker's one-function-at-a-time model ([Durable Functions concurrency](https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-perf-and-scale#language-runtime-considerations)):
each document activity is long-running and memory-heavy (extraction, figure rendering, vision,
embeddings), so a higher per-worker limit would pin idle activities in one worker's memory instead
of letting the scale controller distribute them across workers. Flex Consumption does not support
`FUNCTIONS_WORKER_PROCESS_COUNT`; increase throughput through platform scale-out rather than
raising the activity throttle or worker-process count
([Flex Consumption](https://learn.microsoft.com/azure/azure-functions/flex-consumption-plan)).

The deployed Durable Task Scheduler retains terminal orchestration history for 30 days by default. This is scheduler-owned state, not an application environment variable. Confirm the scheduler configuration against organizational retention requirements before relying on that duration; explicit purge operations can remove terminal history earlier.

### SharePoint and ingestion services

| Variable | Value/default | Purpose |
| --- | --- | --- |
| `INGESTION_SOURCE_ID` | Legacy deployment input | Task-hub seed and audit fallback; not the runtime source inventory. |
| `SHAREPOINT_ASSIGNED_DRIVE_ID` | Legacy deployment input | Retained for compatibility; runtime sources use catalog drive IDs. |
| `SHAREPOINT_SITE_URL` | Legacy deployment input | Retained for compatibility; runtime sources use catalog site URLs. |
| `SHAREPOINT_TENANT_ID` | Client input | Certificate-auth tenant. |
| `SHAREPOINT_APP_CLIENT_ID` | Client input | Certificate-auth application. |
| `SHAREPOINT_CERTIFICATE_SECRET_NAME` | `sharepoint-app-cert` unless overridden | Certificate PFX secret name. |
| `KEY_VAULT_URI` | Generated from existing vault | Certificate vault URI. |
| `DOCUMENT_INTELLIGENCE_ENDPOINT` | Generated | Document Intelligence endpoint used when that provider is selected. |
| `DOCUMENT_INTELLIGENCE_ENABLED` | Client input/default `true` | Enables Document Intelligence selection when Content Understanding is disabled. |
| `CONTENT_UNDERSTANDING_ENDPOINT` | Generated when enabled; otherwise empty | Content Understanding endpoint used when that provider is selected. |
| `CONTENT_UNDERSTANDING_ENABLED` | Client input/default `false` | Enables Content Understanding and gives it precedence over Document Intelligence. |
| `CONTENT_UNDERSTANDING_ANALYZER_ID` | Client input/default `prebuilt-documentSearch` | Selected Content Understanding analyzer ID. Empty when Content Understanding is disabled; the prebuilt definition can change independently of the ID. |
| `AZURE_LANGUAGE_ENDPOINT` | Generated | Enrichment endpoint; code requires it when any enrichment module is enabled. |
| `OPENAI_ENDPOINT` | Generated | Azure OpenAI endpoint used by ingestion embeddings. |
| `OPENAI_EMBEDDING_DEPLOYMENT_NAME` | Required runtime setting; client input defaults to `text-embedding-3-large` | Existing 3,072-dimensional deployment passed explicitly to every ingestion embedding request. |
| `OPENAI_CHAT_DEPLOYMENT_NAME` | Client input | Chat deployment used for bounded visual descriptions and recorded in Function settings. |

### Cosmos DB

| Variable | Value/default | Purpose |
| --- | --- | --- |
| `COSMOS_ENDPOINT` | Generated private endpoint | Cosmos account endpoint. |
| `COSMOS_DATABASE_NAME` | Generated; active database is `rag-db` | Ingestion database name. Retrieval intentionally uses `COSMOS_DATABASE` instead. |
| `COSMOS_INGESTION_RUNS_CONTAINER_NAME` | `ingestion-runs` | Runs/control container. |
| `COSMOS_INGESTION_SOURCES_CONTAINER_NAME` | `ingestion-sources` | Common explicitly configured source registry. |

Content container names are not Function application settings. Each accepted
source plan supplies its validated chunks/manifests pair; unbound ingestion
configuration has no content-container fallback.

Retrieval additionally reads `COSMOS_INGESTION_SOURCES_CONTAINER` and
`COSMOS_INGESTION_RUNS_CONTAINER` through its managed identity for source-policy
eligibility. The reader uses Strong consistency and denies all content when complete
registry enumeration fails; verify actual account settings and container RBAC before
deployment.

### Ingestion behavior

| Variable | Value/default | Accepted values and effect |
| --- | --- | --- |
| `EXTRACTION_ENABLED` | Runtime fallback `true` | Boolean parser treats `true`, `1`, and `yes` as true; other nonempty values are false. Disabling causes documents to fail because no extraction alternative exists. Current Bicep does not emit an override. |
| `AUDIO_WRITER_ENABLED` | `false` | Ingestion (writer) audio gate: strict boolean, trimmed and case-normalized. Enabling requires `EXTRACTION_ENABLED=true` and the Speech settings below; DI/CU selection is unchanged. Independent of the retrieval gate. When enabled, discovered `.wav`, `.mp3`, and `.flac` items (Bicep appends these extensions to `ALLOWED_FILE_EXTENSIONS` automatically) are transcribed by Azure Speech — **batch** transcription when the provider is `speech_batch` (the deployed default while the writer is enabled), which stages the file to the audio-staging account, submits an asynchronous job, and finalizes it from the audio poll timer — and are published as time-located chunks. Speech validates and decodes the format server-side. Audio is admitted by file extension; the Graph `file.mimeType` is [server-determined](https://learn.microsoft.com/en-us/graph/api/resources/file) and not used to gate audio. No client-side audio decoder is used; source channel count is not recorded (the default transcribe response does not report it). Requires a live Speech resource, private endpoint, and the `Cognitive Services Speech User` role before real use. |
| `AUDIO_TRANSCRIPTION_PROVIDER` | `speech_fast` (code default); Bicep emits `speech_batch` when the writer is enabled | Accepts `speech_fast` or `speech_batch` after trimming and case normalization; any other value fails configuration loading even when audio is disabled. `speech_batch` (asynchronous submit → poll → finalize) is the deployed audio path and requires the `AUDIO_STAGING_*` settings below; `speech_fast` is the legacy synchronous path. |
| `SPEECH_ENDPOINT` | Empty; required when the writer is enabled | HTTPS custom-domain base URL with one lowercase DNS label before `.cognitiveservices.azure.com`. Optional root slash; no credentials, port, other path, query, fragment, or embedded whitespace. Independent of DI/CU endpoints. |
| `SPEECH_REGION` | Empty; required when the writer is enabled | Initial project allowlist: `eastus2`, `centralindia`, `southeastasia`; trimmed and lowercased. |
| `AUDIO_DEPLOYMENT_REGION` | Empty; required when the writer is enabled | Declared ingestion deployment region; same initial allowlist and normalization. Must equal `SPEECH_REGION` when both are supplied. No inferred region or cross-region fallback. |
| `AUDIO_LOCALE` | Empty; required when the writer is enabled | Initial project allowlist: `en-US`, `en-GB`, `en-IN`; trimmed, case-sensitive. No automatic locale selection. This does not detect or reject non-English speech. |
| `SPEECH_REQUEST_TIMEOUT_SECONDS` | `120` | Positive number; per-request HTTP timeout for Speech transcription calls (batch submit, poll, and result download). |
| `AUDIO_STAGING_BLOB_ENDPOINT` | Empty; required when the writer is enabled with `speech_batch` | HTTPS blob endpoint of the dedicated audio-staging account. Validated for scheme and absence of control characters. Speech reads staged audio from here; no SAS is minted. |
| `AUDIO_STAGING_CONTAINER` | Empty; required when the writer is enabled with `speech_batch` | Blob container that holds staged audio pending transcription. |
| `AUDIO_BATCH_TTL_HOURS` | `48` | Integer `6` through `744`. Retention bound for the staged blob and batch job before cleanup. |
| `AUDIO_POLL_SCHEDULE` | `0 */5 * * * *` | NCRONTAB for the audio-transcription poll timer that finalizes completed batch jobs (default every 5 minutes). |
| `AUDIO_POLL_PAGE_SIZE` | `20` | Maximum `transcribing` documents processed per poll tick. |
| `KEY_PHRASES_ENABLED` | Runtime fallback `true` | Enables key-phrase enrichment. Same boolean parsing. |
| `ENTITIES_ENABLED` | Runtime fallback `true` | Enables entity enrichment. Same boolean parsing. |
| `SUMMARY_ENABLED` | Runtime fallback `false` | Enables summary enrichment. Same boolean parsing. |
| `ALLOWED_FILE_EXTENSIONS` | Bicep value `.md,.pdf,.docx,.pptx,.xlsx`, plus `.wav,.mp3,.flac` appended when `AUDIO_WRITER_ENABLED=true`; runtime fallback `.pdf` | Comma-separated, case-normalized suffixes. Bicep emits the five-format document contract and appends the three audio extensions when the audio writer is enabled. |
| `CHUNK_MAX_TOKENS` | `800` | Maximum chunk token count. |
| `CHUNK_OVERLAP_TOKENS` | `100` | Token overlap between adjacent chunks. |
| `ACL_MAX_PAGES` | `10` | Maximum permission/group paging calls per ACL read. |
| `DOWNLOAD_TIMEOUT_SECONDS` | `120` | Source download HTTP timeout in seconds. |
| `DELTA_MAX_PAGES` | `200` | Maximum Graph delta pages per activity window. A sealed `nextLink` continues in the same Durable round; the committed cursor advances only after the terminal `deltaLink` settles. |
| `EMBEDDING_BATCH_SIZE` | `100` | Ingestion embedding batch size. |
| `MAX_ELIGIBLE_SOURCES` | `10000` | Maximum eligible source documents admitted in one discovery run. |
| `MAX_DRIVE_ITEMS` | `50000` | Maximum source items scanned in one discovery run. |
| `MAX_FOLDERS` | `10000` | Maximum folders traversed in one discovery run. |
| `MAX_FOLDER_DEPTH` | `32` | Maximum source folder depth traversed. |
| `MAX_GRAPH_PAGES` | `20000` | Maximum Graph pages traversed during full discovery. |
| `MAX_SOURCE_BYTES` | `524288000` | Maximum bytes accepted for one source file. |
| `MAX_RENDERED_DERIVATIVE_BYTES` | `524288000` | Maximum bytes accepted for one rendered derivative. |
| `MAX_PAGES_PER_SOURCE` | `2000` | Maximum physical pages accepted for one page-based source. This is application admission, not a declaration of provider capacity. |
| `MAX_CONTENT_UNITS_PER_SOURCE` | `2000` | Maximum logical content units accepted for one non-page source processor. |
| `MAX_OFFICE_CHARACTERS` | `8000000` | Maximum extracted Office characters accepted for one source. |
| `MAX_VISUAL_ASSET_BYTES` | `2097152` | Maximum bytes accepted for one linked or extracted visual asset. |
| `MAX_VISUAL_ASSETS_PER_SOURCE` | `60` | Maximum visual assets described for one source. |
| `MAX_AUDIO_PHRASES_PER_SOURCE` | `20000` | Maximum transcript phrases accepted for one audio source. |
| `MAX_AUDIO_WORDS_PER_SOURCE` | `400000` | Maximum transcript words accepted for one audio source. |
| `MAX_CHUNKS_PER_SOURCE` | `2000` | Maximum chunks emitted for one source. |
| `VISION_MAX_OUTPUT_TOKENS` | `400` | Maximum tokens returned for one bounded visual description. |

`SourceAdmissionConfig` owns the generic `MAX_*` source limits, while
`ExecutionBatchConfig` owns ACL paging, download timeouts, delta paging,
embedding batches, Speech request timeouts, audio batch TTL, orchestration wave
size and timeout, processing retry policy, and reconciliation, audio-poll, and
source-dispatch page sizes. The Function worker loads this execution policy
once at startup so Durable replays use a stable snapshot. Timer expressions
remain trigger-owned schedule strings. Algorithm settings such as chunk token
windows and vision output tokens remain separate.
Document Intelligence S0 currently supports up to 500 MB and 2,000 PDF/TIFF
pages; F0 supports 4 MB and processes only the first two pages. Microsoft
documentation and the deployed service remain authoritative. The effective
limit is the lowest applicable application admission, provider-tier,
format-specific, and regional limit.

Audio is default-disabled and enabled per environment via `AUDIO_WRITER_ENABLED`.
When disabled, the ingestion configuration validator is only a preparation step:
supplied Speech and staging settings are still validated, but missing ones are
permitted. When enabled with `speech_batch`, the Speech resource, the audio-staging
account, and the `AUDIO_STAGING_*` settings are required.
These are initial project allowlists, not the complete Azure region or English
locale lists. Their service eligibility is grounded in [Speech regions](https://learn.microsoft.com/en-us/azure/ai-services/speech-service/regions),
[language support](https://learn.microsoft.com/en-us/azure/ai-services/speech-service/language-support?tabs=stt),
the [batch transcription guide](https://learn.microsoft.com/en-us/azure/ai-services/speech-service/batch-transcription), and the [fast transcription endpoint guide](https://learn.microsoft.com/en-us/azure/ai-services/speech-service/fast-transcription-create).

URL validation and matching declared regions do not verify account ownership,
actual region/residency, worker identity, private routing, or permission to process
recordings. Decoder integrity/containment, policy approvals, and live verification
remain open; see [ADR 0001](adr/0001-audio-batch-transcription.md).
No Speech client is created by configuration loading. These ingestion audio/Speech
settings are not emitted by Bicep or the deployment controller.

### Timers and webhooks

| Variable | Value/default | Purpose |
| --- | --- | --- |
| `DELTA_SYNC_SCHEDULE` | `0 0 4 * * *` | Daily 04:00 UTC safety-net delta timer (NCRONTAB). |
| `ACL_RESYNC_SCHEDULE` | `0 0 3 * * 0` | Weekly Sunday 03:00 UTC ACL reconciliation. |
| `ACL_RESYNC_PAGE_SIZE` | `50`; Bicep allows 1–100 | Documents per ACL-resync activity. |
| `LIFECYCLE_RECONCILE_SCHEDULE` | `0 */10 * * * *` | Lifecycle reconciliation every 10 minutes. |
| `LIFECYCLE_RECONCILE_PAGE_SIZE` | `50`; Bicep allows 1–100 | Items per lifecycle reconciliation activity. |
| `SUBSCRIPTION_RENEW_SCHEDULE` | `0 0 2 * * *` | Daily 02:00 UTC webhook subscription renewal. |
| `FUNCTION_PUBLIC_BASE_URL` | Generated Function HTTPS URL | Builds Graph notification/lifecycle URLs. |
| `WEBHOOK_CLIENT_STATE` | Client secret | Validates SharePoint change notifications. Empty value disables processing with server error. |
| `TENANT_ID` | SharePoint tenant input | Expected tenant in EasyAuth user claims. |
| `FUNCTION_API_AUDIENCE` | Client input | Exact expected audience in Function operator claims. Retrieval queries use `RETRIEVAL_API_AUDIENCE`. |

## ACA Retrieval Runtime

The retrieval Bicep module owns connectivity and operational settings. Relevance
values come exclusively from the validated runtime catalog snapshot.

### Required connectivity and identity

| Variable | Required | Value/default | Purpose |
| --- | --- | --- | --- |
| `COSMOS_ENDPOINT` | Yes | Generated | Cosmos endpoint. |
| `COSMOS_DATABASE` | Yes | Generated; `rag-db` | Retrieval database name. |
| `COSMOS_AUDIT_CONTAINER` | No | `service-audit` | Best-effort query audit container. |
| `AZURE_OPENAI_ENDPOINT` | Yes | Generated | Retrieval OpenAI endpoint. |
| `EMBEDDING_DEPLOYMENT` | No | `text-embedding-3-large` | Query embedding deployment. |
| `CHAT_DEPLOYMENT` | Yes | Client input | Planning/generation/agent model deployment. |
| `TENANT_ID` | Yes | Client input | Expected gateway tenant and Graph tenant. |
| `AZURE_AUTH_MODE` | Yes | Generated; `managed_identity` | Selects the explicit Azure-hosted credential path. |
| `MANAGED_IDENTITY_CLIENT_ID` | Yes | Retrieval UAMI client ID | Identity for Cosmos, OpenAI, Graph, and ACR. |
| `RETRIEVAL_API_AUDIENCE` | Yes | Retrieval API client ID in active Bicep | Exact delegated-user token audience expected by retrieval. |
| `RETRIEVAL_CATALOG_POLL_SECONDS` | No | `7200` | Bounded runtime refresh interval; see direct-edit timing above. |
| `RETRIEVAL_CONFIG_CONTAINER` | No | `retrieval-config` | Catalog container. |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | No | Generated; empty skips Azure Monitor setup | Sensitive telemetry connection string. Do not print. GenAI suppression is independent of this value; see [observability](ARCHITECTURE.md#observability). |

Local application execution instead requires
`AZURE_AUTH_MODE=service_principal` with process-scoped `AZURE_TENANT_ID`,
`AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`. The secret is local-only and
must never appear in tracked settings or an Azure deployment. Missing,
unknown, or incomplete mode-specific settings fail at startup; developer
credential fallback is not supported.

### Local application authentication

| Variable | Required | Classification | Purpose |
| --- | --- | --- | --- |
| `AZURE_AUTH_MODE` | Yes | Non-secret; exact value `service_principal` | Selects direct local `ClientSecretCredential`. |
| `AZURE_TENANT_ID` | Yes | Non-secret GUID | Tenant containing the generic development app. |
| `AZURE_CLIENT_ID` | Yes | Non-secret GUID | Application/client ID for `developer-local-azure-access`; not its service-principal object ID. |
| `AZURE_CLIENT_SECRET` | Yes | Secret | Current local credential, stored only in the Git-ignored local `app\local.settings.json` for non-production development. |

Do not place these values in tracked configuration. Never place
`AZURE_CLIENT_SECRET` in `.env`, PowerShell profiles, user/machine environment
variables, shell history, deployment parameters, logs, or Azure application
settings. The local exception is `app\local.settings.json` only; it must remain
Git-ignored and must never be copied into a deployment. The secret expires in
less than 12 months and follows the overlap-then-revoke procedure in
[local development](LOCAL_DEVELOPMENT.md#rotate-or-revoke-the-local-credential).

Azure deployments use `AZURE_AUTH_MODE=managed_identity` plus their existing
user-assigned managed-identity client ID. They do not receive
`AZURE_TENANT_ID` or `AZURE_CLIENT_SECRET`. Delegated users calling ACA are a
separate Easy Auth/API contract and do not use this application credential.

### Retrieval behavior and limits

The guarded controller's `-IncludeCitations` boolean argument sets
`INCLUDE_CITATIONS` through Bicep and defaults to `$true`. Deploy with
`-IncludeCitations:$false` to return an empty citation array; this does not
remove reference markers from generated answer text.

| Variable | Required | Value/default | Accepted values and effect |
| --- | --- | --- | --- |
| `INCLUDE_CITATIONS` | No | `true` | Only literal `false` disables citations. |
| `MAX_EVIDENCE_CHUNKS` | No | `5` | Default top-K when omitted by request; public request allows 1–20. |
| `MAX_PLANNED_QUERIES` | No | `3` | Maximum planner queries used by retrieval. |
| `RETRIEVAL_TIMEOUT_SECONDS` | No | `5.0` | Retrieval fan-out wait bound and query embedding timeout; not a Cosmos SDK query timeout. |
| `GENERATION_TIMEOUT_SECONDS` | No | `15.0` | Standard answer-generation OpenAI timeout. |
| `AGENT_TIMEOUT_SECONDS` | No | `20.0` | Agent path and tool deadline before standard fallback. |
| `AGENT_MAX_ITERATIONS` | No | `5` | Positive integer limiting Agent Framework model/tool roundtrips before one final no-tools response. |
| `AGENT_OPENAI_API_VERSION` | No | `preview` | API-version value passed to the Agent Framework OpenAI client. |
| `GRAPH_GROUP_TIMEOUT_SECONDS` | No | `10.0` | HTTP timeout for transitive security-group resolution. |
| `OPENAI_API_VERSION` | No | `2024-10-21` | Azure OpenAI API version for standard embeddings/chat. |
| `RETRIEVAL_OPERATION_TIMEOUT_SECONDS` | No | `27.0` | Wall-clock middleware deadline for `/api/query`. |
| `RATE_LIMIT_RPM` | No | Runtime fallback `30` | Per-user requests per minute per ACA replica. Current Bicep does not emit an override. |

A selected source with `retrievalAclEnabled=false` does not make the API
anonymous. ACA still requires an approved delegated-user token, but retrieval
omits the `allowedGroupIds` filter for that source, so every authenticated
caller admitted by the API can see its eligible content. Ingestion still
requires at least one verified Entra security group and persists
`allowedGroupIds`; the source policy changes query filtering, not document
admission. Use `false` only for content approved for the API's entire
authenticated audience.

The audio settings above are not yet emitted by the deployment controller or Bicep.
They do not enable a Speech client or grant permission to process recordings.

### Catalog-owned relevance settings

Do not use relevance environment overrides. Edit the singleton's `config`;
each request captures one immutable snapshot before planning and retrieval.

| Concern | Catalog property | Application |
| --- | --- | --- |
| Over-fetch | `config.retrieval.overFetchFactor` | Shared candidate limit tuning |
| Full-text scope | `config.retrieval.fullTextScoreScope` | Shared score statistics scope |
| Hybrid weights | `config.retrieval.hybridWeights` | Positive vector/text weights |
| Default profile | Optional `config.defaultProfile` | Absent means no default scoring profile |
| Synonyms | Selected profile's `synonymMap` | Referenced map applies unless the request opts out |

### Optional multi-instance registry

| Variable | Default | Purpose |
| --- | --- | --- |
| `INGESTION_SOURCE_ID` | `default` in retrieval registry construction | Source label for the default retrieval instance. Active retrieval Bicep does not currently emit it. |

## Temporary Catalog Publisher Runtime

The deployment controller sets the following process variables to parameterize the temporary Container App Job. They are not injected into the job container unless listed in the next table.

| Variable | Value/source | Purpose |
| --- | --- | --- |
| `OPERATIONS_JOB_NAME` | Derived from deployment instance | Temporary job resource name used by the controller. |
| `AZURE_LOCATION` | Guarded target | Job region. |
| `MANAGED_ENVIRONMENT_ID` | Discovered tagged ACA environment | Private execution environment. |
| `ACR_LOGIN_SERVER` | Discovered tagged registry | Operations image registry. |
| `RETRIEVAL_IMAGE_REFERENCE` | Immutable build output | Image containing `retrieval.operations`. |
| `OPERATIONS_MANAGED_IDENTITY_ID` | Operations UAMI resource ID | Job identity assignment. |
| `OPERATIONS_MANAGED_IDENTITY_CLIENT_ID` | Operations UAMI client ID | Identity selected inside the job. |
| `COSMOS_ENDPOINT` | Discovered deployment Cosmos endpoint | Catalog publication endpoint. |
| `DEPLOYMENT_INSTANCE_ID` | Guarded target | Selects and names the environment's temporary job resource; it is not injected into the catalog document. |

The job container itself receives this runtime contract:

| Variable | Value/source | Purpose |
| --- | --- | --- |
| `COSMOS_ENDPOINT` | Passed through from the controller | Catalog publication endpoint. |
| `COSMOS_DATABASE` | `rag-db` | Catalog database. |
| `RETRIEVAL_CONFIG_CONTAINER` | `retrieval-config` | Catalog container. |
| `EXPECTED_CATALOG_DIGEST` | `RETRIEVAL_CATALOG_DIGEST`, publish only | Checks seed integrity on create-only initialization; absent for read-only verification. |
| `MANAGED_IDENTITY_CLIENT_ID` | Operations UAMI client ID | Selects the job identity for Cosmos access. |
| `CATALOG_PATH` | Runtime fallback `/app/retrieval/catalog.example.json`, publish only | Seed path inside the reviewed image. Verification does not load a seed file. |

`-CatalogOperation publish-catalog` selects explicit initialization. The default
`verify-catalog` operation only reads the current singleton. `Final` requires a
verified read-only execution against the reviewed image and target; retain the
job until Final and E2E checks finish. See [Azure setup](AZURE_SETUP.md).

## Validation Sources

- Function runtime settings: `infra/modules/functions.bicep`, `app/config.py`, and `app/function_app.py`.
- Retrieval runtime settings: `infra/modules/retrieval-config.bicep`, `app/retrieval/config.py`, `app/retrieval/main.py`, and `app/retrieval/cosmos_registry.py`.
- Operations settings: `scripts/deploy.ps1`, `infra/operations.parameters.bicepparam`, and `app/retrieval/operations.py`.
- Deployment inputs: `infra/main.parameters.bicepparam` and `scripts/deploy.ps1`.
- Relevance properties: [API reference catalog property table](API_REFERENCE.md#catalog-property-reference).
