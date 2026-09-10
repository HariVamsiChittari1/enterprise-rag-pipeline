---
title: Environment Variable Reference
description: Deployment inputs and runtime configuration for ingestion and retrieval
---

This is the single inventory for deployment inputs and runtime environment variables used by ingestion, the Function query gateway, ACA retrieval, and the temporary catalog publisher.

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

Open the item with `id` equal to `runtime-catalog` in the deployment's
`deploymentInstanceId` partition. Keep both fields and
`type: retrieval-runtime-catalog` unchanged. Do not edit Cosmos system fields.

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
| `DEPLOYMENT_INSTANCE_ID` | Yes | Stable value such as `client-prod` | Resource tag/name discriminator and retrieval catalog partition key. The controller sets this process variable from `-DeploymentInstanceId`. |
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
| `SHAREPOINT_ASSIGNED_DRIVE_ID` | Yes | Graph drive ID | Single SharePoint document-library drive ingested by this deployment. |
| `SHAREPOINT_SITE_URL` | Yes | `https://tenant.sharepoint.com/sites/site` | HTTPS site used to verify drive ownership and expand SharePoint site groups. No query string, fragment, credentials, or nonstandard port. |
| `INGESTION_SOURCE_ID` | Yes | Stable identifier such as `sharepoint-drive` | Source namespace for runs, document keys, task hub, and controls. Keep stable across redeployments. |
| `SHAREPOINT_KEY_VAULT_NAME` | Yes | Existing vault name | Existing vault holding the certificate PFX secret. Must be in the deployment subscription. |
| `SHAREPOINT_KEY_VAULT_RESOURCE_GROUP` | Yes | Existing resource group | Resource group containing that vault in the deployment subscription. |
| `SHAREPOINT_CERTIFICATE_SECRET_NAME` | No | `sharepoint-app-cert` | Key Vault secret containing the exportable PFX bytes. |

### Function and retrieval API identities

| Variable | Required | Default/example | Purpose |
| --- | --- | --- | --- |
| `ADMIN_API_CLIENT_ID` | Yes | Function API application client GUID | EasyAuth provider registration protecting Function routes. |
| `FUNCTION_API_AUDIENCE` | Yes | Usually `api://<function-api-client-id>` | Exact audience accepted by EasyAuth and the query gateway. |
| `FUNCTION_ALLOWED_CALLER_CLIENT_ID` | Yes | Approved client application GUID | Sole caller application inserted into the Function EasyAuth allowlist. |
| `RETRIEVAL_API_CLIENT_ID` | Yes | Retrieval API client GUID | ACA Authentication application registration. |
| `RETRIEVAL_API_AUDIENCE` | Yes | Usually `api://<retrieval-api-client-id>` | Scope base used to generate the Function setting `<value>/.default`. Active Bicep separately validates ACA tokens against `RETRIEVAL_API_CLIENT_ID`; keep both values consistent with the same app registration. |
| `RETRIEVAL_API_SERVICE_PRINCIPAL_ID` | Yes | Service-principal object GUID | External validation/evidence input for the retrieval API service principal. Active Bicep re-outputs it but does not create app-role assignments. |
| `WEBHOOK_CLIENT_STATE` | Yes, secret | Cryptographically random value | Shared secret checked by `/api/webhook/sharepoint`. The lifecycle webhook currently does not validate it. |

The Function UAMI must receive `Retrieval.Gateway` on the retrieval API application, and the retrieval UAMI requires its Graph application permissions. These are external Entra assignments.

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
| `AZURE_CLIENT_ID` | Function UAMI client ID | User-assigned identity selected by `DefaultAzureCredential`. |
| `MANAGED_IDENTITY_CLIENT_ID` | Function UAMI client ID | Identity used to obtain the retrieval service token. |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | Generated, secret | Application Insights telemetry connection. Do not print. |
| `APPLICATIONINSIGHTS_AUTHENTICATION_STRING` | `ClientId=<Function-UAMI>;Authorization=AAD` | Entra authentication for Application Insights ingestion. |
| `INSTANCE_MEMORY_MB` | `2048` from current Bicep default | Informational Function instance-memory setting emitted by Bicep. Allowed Bicep values: 512, 1024, 2048, 4096. |

### Durable orchestration

| Variable | Value/default | Purpose |
| --- | --- | --- |
| `DURABLE_TASK_SCHEDULER_CONNECTION_STRING` | Generated endpoint plus Function UAMI | Managed-identity Durable Task Scheduler connection. |
| `TASKHUB_NAME` | Generated from `INGESTION_SOURCE_ID`, maximum 45 characters | Source-specific Durable task hub. |
| `WAVE_SIZE` | Runtime fallback `4` | Documents dispatched concurrently per full-sync wave. Current Bicep does not emit an override. |
| `WAVE_TIMEOUT_MINUTES` | Runtime fallback `20` | Deadline for one full-sync/retry wave. Current Bicep does not emit an override. |
| `PROCESS_DOCUMENT_MAX_ATTEMPTS` | Runtime fallback `5` | Maximum retries inside `process_document_activity`. Current Bicep does not emit an override. |
| `PROCESS_DOCUMENT_RETRY_DELAY_SECONDS` | Runtime fallback `60` | Base retry delay; deterministic document jitter adds 0–29 seconds. Current Bicep does not emit an override. |

`app/host.json` sets `durableTask.maxConcurrentActivityFunctions` to `1`. This matches the Python
worker's one-function-at-a-time model ([Durable Functions concurrency](https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-perf-and-scale#language-runtime-considerations)):
each document activity is long-running and memory-heavy (extraction, figure rendering, vision,
embeddings), so a higher per-worker limit would pin idle activities in one worker's memory instead
of letting the scale controller distribute them across workers. Increase ingestion throughput by
scaling out to more workers (and/or raising `FUNCTIONS_WORKER_PROCESS_COUNT` where per-worker memory
allows), not by raising the activity throttle.

The deployed Durable Task Scheduler retains terminal orchestration history for 30 days by default. This is scheduler-owned state, not an application environment variable. Confirm the scheduler configuration against organizational retention requirements before relying on that duration; explicit purge operations can remove terminal history earlier.

### SharePoint and ingestion services

| Variable | Value/default | Purpose |
| --- | --- | --- |
| `INGESTION_SOURCE_ID` | Client input | Stable source namespace. |
| `SHAREPOINT_ASSIGNED_DRIVE_ID` | Client input | Configured document-library drive. |
| `SHAREPOINT_SITE_URL` | Client input | Required site URL for drive binding and site-group expansion. |
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
| `OPENAI_EMBEDDING_DEPLOYMENT_NAME` | Client input/default `text-embedding-3-large` | Embedding deployment recorded in Function settings. |
| `OPENAI_CHAT_DEPLOYMENT_NAME` | Client input | Chat deployment used for bounded visual descriptions and recorded in Function settings. |

### Cosmos DB

| Variable | Value/default | Purpose |
| --- | --- | --- |
| `COSMOS_ENDPOINT` | Generated private endpoint | Cosmos account endpoint. |
| `COSMOS_DATABASE_NAME` | Generated; active database is `rag-db` | Ingestion database name. Retrieval intentionally uses `COSMOS_DATABASE` instead. |
| `COSMOS_INGESTION_RUNS_CONTAINER_NAME` | `ingestion-runs` | Runs/control container. |
| `COSMOS_SOURCE_DOCUMENTS_CONTAINER_NAME` | `source-documents` | Source manifest container. |
| `COSMOS_SEARCH_CHUNKS_CONTAINER_NAME` | `search-chunks` | Chunk container. |

### Ingestion behavior

| Variable | Value/default | Accepted values and effect |
| --- | --- | --- |
| `EXTRACTION_ENABLED` | Runtime fallback `true` | Boolean parser treats `true`, `1`, and `yes` as true; other nonempty values are false. Disabling causes documents to fail because no extraction alternative exists. Current Bicep does not emit an override. |
| `KEY_PHRASES_ENABLED` | Runtime fallback `true` | Enables key-phrase enrichment. Same boolean parsing. |
| `ENTITIES_ENABLED` | Runtime fallback `true` | Enables entity enrichment. Same boolean parsing. |
| `SUMMARY_ENABLED` | Runtime fallback `false` | Enables summary enrichment. Same boolean parsing. |
| `ALLOWED_FILE_EXTENSIONS` | Bicep value `.md,.pdf,.docx,.pptx,.xlsx`; runtime fallback `.pdf` | Comma-separated, case-normalized suffixes. The deployed five-format contract is emitted by Bicep. |
| `CHUNK_MAX_TOKENS` | `800` | Maximum chunk token count. |
| `CHUNK_OVERLAP_TOKENS` | `100` | Token overlap between adjacent chunks. |
| `ACL_MAX_PAGES` | `10` | Maximum permission/group paging calls per ACL read. |
| `DOWNLOAD_TIMEOUT_SECONDS` | `120` | Source download HTTP timeout in seconds. |
| `DELTA_MAX_PAGES` | `200` | Maximum Graph delta pages per tick. |
| `EMBEDDING_BATCH_SIZE` | `100` | Ingestion embedding batch size. |
| `MAX_PDF_PAGES` | Runtime setting `500`; effective application cap `300` | Configurable PDF page ceiling, bounded by the shared 300-document-unit application limit. |
| `VISION_MAX_OUTPUT_TOKENS` | `400` | Maximum tokens returned for one bounded visual description. |
| `VISION_MAX_IMAGE_BYTES` | `2097152` | Maximum bytes accepted for one linked Markdown image or extracted figure. |
| `VISION_MAX_FIGURES` | `60` | Maximum figures described for one source document. |

Shared application limits also reject source files over 100 MB and rendered Office-to-PDF derivatives over 200 MB. These limits are stricter than some provider-tier limits and apply before or around provider analysis. Document Intelligence S0 supports up to 500 MB and 2,000 PDF/TIFF pages; F0 supports 4 MB and processes only the first two pages. The effective limit is always the lowest applicable application, provider-tier, and format-specific limit.

### Timers, webhooks, and query gateway

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
| `FUNCTION_API_AUDIENCE` | Client input | Exact expected audience in query user claims. |
| `RETRIEVAL_SERVICE_URL` | Generated ACA private URL | Function proxy target. Must be a bare HTTPS `azurecontainerapps.io` URL accepted by gateway validation. |
| `RETRIEVAL_SERVICE_SCOPE` | Generated as `<RETRIEVAL_API_AUDIENCE>/.default` | Scope requested for the Function UAMI service token. |
| `QUERY_PROXY_TIMEOUT_SECONDS` | `30` | Function-to-ACA HTTP timeout in seconds. |

## ACA Retrieval Runtime

The retrieval Bicep module owns connectivity and operational settings. Relevance
values come exclusively from the validated runtime catalog snapshot.

### Required connectivity and identity

| Variable | Required | Value/default | Purpose |
| --- | --- | --- | --- |
| `COSMOS_ENDPOINT` | Yes | Generated | Cosmos endpoint. |
| `COSMOS_DATABASE` | Yes | Generated; `rag-db` | Retrieval database name. |
| `COSMOS_CHUNKS_CONTAINER` | No | `search-chunks` | Chunk container. |
| `COSMOS_MANIFESTS_CONTAINER` | No | `source-documents` | Ready-manifest container. |
| `COSMOS_AUDIT_CONTAINER` | No | `service-audit` | Best-effort query audit container. |
| `AZURE_OPENAI_ENDPOINT` | Yes | Generated | Retrieval OpenAI endpoint. |
| `EMBEDDING_DEPLOYMENT` | No | `text-embedding-3-large` | Query embedding deployment. |
| `CHAT_DEPLOYMENT` | Yes | Client input | Planning/generation/agent model deployment. |
| `TENANT_ID` | Yes | Client input | Expected gateway tenant and Graph tenant. |
| `MANAGED_IDENTITY_CLIENT_ID` | Yes | Retrieval UAMI client ID | Identity for Cosmos, OpenAI, Graph, and ACR. |
| `RETRIEVAL_API_AUDIENCE` | Yes | Retrieval API client ID in active Bicep | Exact app-token audience expected by retrieval. |
| `RETRIEVAL_GATEWAY_CLIENT_ID` | Yes | Function UAMI client ID | Expected service token `azp`. |
| `RETRIEVAL_GATEWAY_PRINCIPAL_ID` | Yes | Function UAMI principal ID | Expected service token `oid`. |
| `DEPLOYMENT_INSTANCE_ID` | Yes | Client target | Catalog partition key. |
| `RETRIEVAL_CATALOG_POLL_SECONDS` | No | `7200` | Bounded runtime refresh interval; see direct-edit timing above. |
| `RETRIEVAL_CONFIG_CONTAINER` | No | `retrieval-config` | Catalog container. |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | No | Generated; empty disables tracing setup | Sensitive telemetry connection string. Do not print. |

### Retrieval behavior and limits

The guarded controller's `-IncludeCitations` boolean argument sets
`INCLUDE_CITATIONS` through Bicep and defaults to `$true`. Deploy with
`-IncludeCitations:$false` to return an empty citation array; this does not
remove reference markers from generated answer text.

| Variable | Required | Value/default | Accepted values and effect |
| --- | --- | --- | --- |
| `ACL_ENABLED` | No | `true` | `false`, `0`, or `no` disable ACL filtering; any other value enables it. Never disable in a shared secure environment merely to make a test pass. |
| `INCLUDE_CITATIONS` | No | `true` | Only literal `false` disables citations. |
| `MAX_EVIDENCE_CHUNKS` | No | `5` | Default top-K when omitted by request; public request allows 1–20. |
| `MAX_PLANNED_QUERIES` | No | `3` | Maximum planner queries used by retrieval. |
| `RETRIEVAL_TIMEOUT_SECONDS` | No | `5.0` | Retrieval fan-out wait bound and query embedding timeout; not a Cosmos SDK query timeout. |
| `GENERATION_TIMEOUT_SECONDS` | No | `15.0` | Standard answer-generation OpenAI timeout. |
| `AGENT_TIMEOUT_SECONDS` | No | Runtime fallback `8.0`; active Bicep emits `20.0` | Agent path and tool deadline before standard fallback. |
| `AGENT_MAX_ITERATIONS` | No | `5` | Loaded and emitted, but currently not passed to agent construction. Changing it has no runtime effect in current code. |
| `AGENT_OPENAI_API_VERSION` | No | `preview` | API-version value passed to the Agent Framework OpenAI client. |
| `GRAPH_GROUP_TIMEOUT_SECONDS` | No | `10.0` | HTTP timeout for transitive security-group resolution. |
| `OPENAI_API_VERSION` | No | `2024-10-21` | Azure OpenAI API version for standard embeddings/chat. |
| `RETRIEVAL_OPERATION_TIMEOUT_SECONDS` | No | `27.0` | Wall-clock middleware deadline for `/api/query`. |
| `RATE_LIMIT_RPM` | No | Runtime fallback `30` | Per-user requests per minute per ACA replica. Current Bicep does not emit an override. |

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
| `COSMOS_REGISTRY_JSON` | Empty | Optional nonempty JSON array of retrieval instance definitions. When empty, one instance is built from the default Cosmos settings. Invalid entries fail startup. This does not make ingestion multi-library. |
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
| `DEPLOYMENT_INSTANCE_ID` | Guarded target | Catalog partition key. |

The job container itself receives this runtime contract:

| Variable | Value/source | Purpose |
| --- | --- | --- |
| `COSMOS_ENDPOINT` | Passed through from the controller | Catalog publication endpoint. |
| `COSMOS_DATABASE` | `rag-db` | Catalog database. |
| `RETRIEVAL_CONFIG_CONTAINER` | `retrieval-config` | Catalog container. |
| `DEPLOYMENT_INSTANCE_ID` | Passed through from the controller | Catalog partition key. |
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
