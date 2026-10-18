---
title: Azure Resource, Identity, and Network Inventory
description: Active Azure resources, identities, RBAC, networking, and outputs
---

This document describes the resources created or consumed by the active Bicep deployment. It does not claim that a particular Azure environment currently matches the template; use Azure inventory tools for live-state verification.

## Deployment Boundary

- Active serving topology: Azure Functions Flex Consumption plus Azure Container Apps.
- AKS modules and manifests remain in the repository but are not referenced by `infra/main.bicep`.
- The resource group, Azure OpenAI account, SharePoint certificate Key Vault, and three Entra application registrations are external prerequisites.
- `scripts/deploy.ps1` is the deployment authority. Serving resources are conditional on `deployServing`; the temporary catalog publisher job is conditional on `deployOperations`.

## Resources

| Resource | Active module | Purpose and material settings |
| --- | --- | --- |
| Application Insights and Log Analytics | `monitoring.bicep` | Function/retrieval telemetry and ACA logs; optional daily cap |
| Storage account | `storage.bicep` | Functions deployment and runtime storage; ZRS default, shared-key access disabled, public access disabled |
| Function UAMI | `identity.bicep` | Function ingestion runtime identity |
| Cosmos DB NoSQL account | `cosmos.bicep` | Strong consistency, single region, local auth disabled, public access disabled, vector/full-text capabilities |
| Durable Task Scheduler | `durable-task.bicep` | Durable Functions orchestration backend and source-derived task hub |
| Document Intelligence | `ai-services.bicep` | PDF and native Office extraction when selected; public access disabled |
| Azure AI Language | `ai-services.bicep` | Key phrases, entities, and optional summaries; public access disabled |
| Microsoft Foundry and Content Understanding | `content-understanding.bicep` | Optional extraction provider with pinned completion and embedding deployments, system-assigned identity, local auth disabled, and public access disabled |
| Azure AI Speech | `speech.bicep` | Optional audio batch transcription; keyless (system-assigned identity), custom subdomain, network default-deny with private endpoint. Created only when the audio writer is enabled |
| Audio-staging storage account | `audio-staging.bicep` | Optional dedicated staging for audio batch transcription; shared-key access disabled, public endpoint network default-deny allowlisted to the Speech resource instance, blob private endpoint. Created only when the audio writer is enabled |
| Virtual network, private DNS, private endpoints | `networking.bicep` | Function integration, ACA infrastructure, and private endpoint subnets |
| ACA managed environment | `aca-environment.bicep` | Internal VNet-integrated environment with Log Analytics |
| Azure Container Registry | `acr.bicep` | Retrieval image storage; serving uses an immutable digest |
| Retrieval UAMI | `identity.bicep` | ACA access to Cosmos, Azure OpenAI, Graph, and ACR |
| Operations UAMI | `identity.bicep` | Temporary private catalog-publisher job |
| Function App | `functions.bicep` | Python 3.12 Flex Consumption API and Durable activities; created in `Final` |
| Retrieval Container App | `aca.bicep` | FastAPI service, single active revision, authenticated Function-only ingress; created in `Final` |
| Temporary catalog job | `aca-operations-job.bicep` | Creates a reviewed seed only on explicit initialization; otherwise verifies the current singleton. Retained through Final/E2E, then removed by approved cleanup. |

## Cosmos DB Containers

| Container | Partition key | Purpose |
| --- | --- | --- |
| `ingestion-sources` | `/sourceId` | Registered SharePoint source binding and active/paused/retired desired state |
| `ingestion-runs` | `/sourceId` | Full-sync runs, source controls, delta cursor, trigger IDs, webhook subscription ID |
| `source-documents` | `/sourceRunId` | Schema-v1 `source_document` records and paged `visual_manifest_page` evidence, discriminated by `recordType` |
| `search-chunks` | `/documentKey` | Content, 3,072-dimensional embeddings, full-text fields, ACL and retrieval eligibility |
| `retrieval-config` | `/id` | Environment-local mutable `runtime-catalog` singleton; optional guarded-writer history and outcome items |
| `service-audit` | `/id` | Best-effort service/lifecycle audit records with a 90-day TTL |

`search-chunks` has DiskANN on `/embedding`, full-text indexes on `/content` and `/searchableText`, and indexes for ACL, source timestamp, retrieval eligibility, and lifecycle generation.

## Managed Identities and RBAC

### Function UAMI

Active Bicep creates the core assignments below plus cross-module assignments. The Content Understanding assignment exists only when that provider is enabled:

- Cosmos DB Built-in Data Contributor at account scope.
- Storage Blob Data Owner.
- Storage Queue Data Contributor.
- Storage Table Data Contributor.
- Cognitive Services User on Document Intelligence.
- Cognitive Services User on Azure AI Language.
- Cognitive Services Content Understanding Contributor on the Content Understanding account.
- Monitoring Metrics Publisher on Application Insights.
- Key Vault Secrets User on the existing SharePoint certificate vault.
- Cognitive Services OpenAI User on the existing Azure OpenAI account.
- Durable Task Data Contributor on the scheduler/task hub.

### Retrieval UAMI

- Cosmos DB Built-in Data Reader scoped separately to `search-chunks`, `source-documents`, and `retrieval-config`.
- Custom metadata/read/create role scoped to `service-audit`; no update or delete.
- Monitoring Metrics Publisher scoped to Application Insights for Entra-authenticated export.
- Cognitive Services OpenAI User on the existing Azure OpenAI account.
- ACR pull access.
- Microsoft Graph application permissions required for transitive group resolution are external Entra prerequisites.

### Operations UAMI

- ACR pull access.
- Custom metadata/read/create role scoped to `retrieval-config`; no replace or delete.
- Used only by the temporary manual catalog-publisher job.

### Function UAMI retrieval-config access

- Custom guarded-writer role scoped only to `retrieval-config`.
- Metadata, point reads, conditional replace, query/change-feed, and create are
  allowed for singleton bootstrap, immutable history, and operation outcomes.
- Delete, upsert, wildcard container actions, and account-wide contributor
  access are not granted.

### Optional Catalog Principals

- Editor: container-scoped metadata, item read/replace, query and change-feed actions.
- Guarded writer: editor actions plus create for history and semantic outcomes.
- Observer: Container App Reader and workspace Log Analytics Reader; no catalog writes.

Neither editor nor writer receives delete, upsert, or wildcard actions from
these custom roles. Container scope is not an item or partition restriction.
The Function's existing account-level grant remains unchanged. Incremental
deployment does not remove prior broad grants: inspect inherited/effective
assignments and obtain separate approval for each exact removal. Do not claim
least privilege from desired Bicep alone.

### Audio transcription (optional)

Created by `audio-staging-rbac.bicep` only when the audio writer is enabled:

- Speech resource system-assigned identity: Storage Blob Data Reader on the audio-staging account (reads staged audio for batch jobs).
- Function UAMI: Storage Blob Data Contributor on the audio-staging account (stages audio blobs and cleans them up after transcription).

### Diagnostics and Cost

The template exports Cosmos account-wide `DataPlaneRequests` to the existing
workspace using resource-specific tables. It is not limited to catalog traffic.
The workspace defaults to 90-day retention; review retention and billable log
volume before deployment. Caps, filtering, export failure, and ingestion delay
can remove required assurance evidence. Target checks must verify AppTraces
custom fields and Cosmos activity, actor, resource, operation, and status fields.

## Resource Reuse Contract

| Dependency | Supported ownership | Arbitrary adoption |
| --- | --- | --- |
| Resource group, OpenAI account/deployments, certificate vault, Entra applications/grants | External prerequisites with reviewed identifiers and access | Only the documented external contract is supported |
| Cosmos, Storage, scheduler, AI services, ACR, monitoring | Repository-created; compatible same-instance incremental redeployment | Not supported through arbitrary resource IDs |
| VNet, subnets, private endpoints/DNS, identities, ACA environment | Repository-created; controller preserves discovered subnet NSG associations | Not general network or identity adoption inputs |
| Function, retrieval app, operations job | Guarded deployment phases own lifecycle | No direct portal replacement or image override |

Redeployment requires compatible schemas, partition keys, naming, region, and
capacity mode. Do not interpret Bicep `existing` references as complete access
or compatibility preflight. Reuse does not authorize destructive recreation.

## External Identity Prerequisites

| Registration | Required contract |
| --- | --- |
| SharePoint ingestion application | Certificate credential; Microsoft Graph application permissions `Sites.Selected`, `Sites.Read.All`, `Files.ReadWrite.All`, `GroupMember.Read.All`, and `User.Read.All`; SharePoint application permission `Sites.Read.All`; required target-site grant. `Files.ReadWrite.All` supports Office-to-PDF conversion. |
| Function API application | Exposes delegated `user_impersonation`; exact API audience is configured in EasyAuth |
| Retrieval API application | Exposes delegated `user_impersonation`; approved native/public clients request that scope |

`FUNCTION_ALLOWED_CALLER_CLIENT_ID` is required. Function EasyAuth accepts exactly `FUNCTION_API_AUDIENCE` and the configured caller application. ACA Authentication accepts delegated users for the retrieval API audience.

Permission-change metadata is a separate unresolved contract. The connector requests Microsoft Graph permission-change headers, for which Microsoft documents `Sites.FullControl.All`; that permission is intentionally absent from the approved prerequisite set. Do not add it through this deployment. A security decision must either approve the additional directory permission or require an implementation and validation change before this metadata is treated as reliable.

Content Understanding is selected when enabled, including when Document Intelligence is also enabled; otherwise Document Intelligence is selected. Markdown remains direct. The guarded controller configures the selected Content Understanding account and verifies `prebuilt-documentSearch`; the Function runtime does not mutate model defaults.

## Network Topology

The VNet contains three subnets:

- Function integration subnet.
- Private endpoint subnet.
- ACA environment infrastructure subnet.

Seven private endpoints are always created for Storage blob, queue, and table; Cosmos SQL; the existing Key Vault; Document Intelligence; and Azure AI Language. Enabling Content Understanding adds its private endpoint and `privatelink.services.ai.azure.com` DNS zone. Document Intelligence and Language share the Cognitive Services zone.

Cosmos, Storage, Document Intelligence, Language, and Content Understanding disable public access. The Key Vault is externally supplied: this deployment creates its private endpoint and role assignment but does not change the vault's existing public-access policy. ACR, monitoring endpoints, Microsoft Graph, and the externally supplied Azure OpenAI network policy are outside that private-endpoint claim.

## Deployment Outputs

`infra/main.bicep` returns:

- Function name and URL.
- Function, retrieval, and operations UAMI client/principal IDs.
- Operations job name when deployed.
- Retrieval API service-principal ID passed as an external validation input.
- Existing Key Vault name.
- Cosmos endpoint and database name.
- Document Intelligence and Azure OpenAI endpoints.
- Content Understanding endpoint, account ID, and completion and embedding deployment names when enabled; otherwise empty outputs.
- ACR login server.
- Retrieval URL and Container App name when serving is deployed.
- Retrieval configuration map.

## Production Sizing Decision

The approved workload fact is:

```text
40-70 SharePoint sites per business x at least 1 GB per site
= at least 40-70 GB of raw source files per business
```

This is a storage lower bound, not a production-capacity model. It does not
provide the number of businesses, actual file count or size distribution,
pages, extracted tokens, chunks, vector/index expansion, initial-ingestion
window, change rate, retrieval concurrency, OpenAI token rate, availability
target, retention, or growth. Microsoft sizes the services below using request
units, requests/pages/characters/tokens, executions, concurrency, and measured
resource utilization rather than SharePoint site count alone.

The previous numeric production estimates for a 10,000-file/5,000-user
scenario were not measurements for this workload and have been removed.

### Evidence-supported SKU status

| Resource | Current dev family/tier | Production decision supported now | Capacity still requiring evidence |
| --- | --- | --- | --- |
| Cosmos DB NoSQL | Serverless by template default | Retain NoSQL vector/full-text architecture; serverless versus provisioned autoscale is undecided | Mode, RU/s per business container, storage, partition behavior, regions and zones require measured item sizes, RU charges, ingest/query rates, hot partitions, growth, RPO and RTO. Use the [capacity planner](https://learn.microsoft.com/azure/cosmos-db/nosql/estimate-ru-with-capacity-planner) and measured [request units](https://learn.microsoft.com/azure/cosmos-db/request-units). |
| Functions | Flex Consumption, 2,048 MB, maximum 40 by template default | Retain the Flex Consumption plan family | Memory, always-ready instances, and maximum instances require per-activity CPU/memory/duration, concurrent files/runs, ingestion window, dependency throttling and latency targets. See [Flex Consumption](https://learn.microsoft.com/azure/azure-functions/flex-consumption-plan). |
| Durable Task Scheduler | Consumption | Retain until measured orchestration demand proves otherwise | Starts/second, concurrent orchestrations, activity rate, timers, retries, payloads and scheduling latency. See [Durable Task Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler). |
| Retrieval Container App | Consumption workload profile; 0.5 vCPU/1 GiB, 1-5 replicas by template default | Retain Container Apps; numeric allocation is undecided | Peak QPS/concurrency, request duration, CPU/memory percentiles, agentic fan-out, p95/p99 latency, minimum warm replicas, scale rule and zone requirement. See [Container Apps scaling](https://learn.microsoft.com/azure/container-apps/scale-app). |
| ACA managed environment | Consumption, non-zone-redundant by template default | Zone choice remains tied to the production availability target | Minimum replicas, zones and subnet headroom require availability and scale targets. See [Container Apps reliability](https://learn.microsoft.com/azure/reliability/reliability-azure-container-apps). |
| Azure Container Registry | Basic | Basic remains sufficient unless private ACR access, geo-replication, or higher throughput is required; Private Link requires Premium | Image count/size, push/pull rates, retention, regions and private-access requirement. See [ACR tiers](https://learn.microsoft.com/azure/container-registry/container-registry-skus). |
| Document Intelligence | S0 | S0 paid tier | Quota requires files/pages by type, ingestion window, parallelism and regional quota. See [service limits](https://learn.microsoft.com/azure/ai-services/document-intelligence/service-limits). |
| Azure AI Language | S | S paid tier | Characters, records, feature mix, batches and calls per ingestion window. See [data limits](https://learn.microsoft.com/azure/ai-services/language-service/concepts/data-limits). |
| Azure AI Speech, optional | S0 when enabled | S0 only when audio ingestion is required | Audio hours, duration distribution, codecs/locales, concurrency and turnaround target. See [Speech quotas](https://learn.microsoft.com/azure/ai-services/speech-service/speech-services-quotas-and-limits). |
| Content Understanding / Foundry, optional | AIServices S0 when enabled | S0 only when the selected extraction path requires it | Routed documents, pages/minute, analyzer behavior, completion/embedding tokens and concurrency. See [service limits](https://learn.microsoft.com/azure/ai-services/content-understanding/service-limits). |
| Functions/Durable Storage | Standard StorageV2; ZRS template default | Retain Standard StorageV2 | LRS/ZRS/GRS selection requires availability, RPO/RTO and regional design. See [storage redundancy](https://learn.microsoft.com/azure/storage/common/storage-redundancy). |
| Audio-staging Storage, optional | Standard StorageV2; ZRS template default | Provision only when audio ingestion is enabled | Audio volume, concurrent jobs, dwell time and recovery objective. |
| Log Analytics/Application Insights | PerGB2018 workspace, 90-day template retention, configurable cap | Retain workspace-based monitoring | Commitment tier, sampling, cap and retention require measured GB/day, compliance retention and budget. A daily cap stops collection after the limit; see [log costs](https://learn.microsoft.com/azure/azure-monitor/logs/cost-logs) and [daily caps](https://learn.microsoft.com/azure/azure-monitor/logs/daily-cap). |
| Azure OpenAI chat | External deployment | Deployment type and capacity are undecided | Model/version, peak TPM/RPM, calls per query, input/output tokens, concurrency, latency, residency and availability. See [deployment types](https://learn.microsoft.com/azure/ai-foundry/openai/how-to/deployment-types) and [quotas](https://learn.microsoft.com/azure/ai-foundry/openai/quotas-limits). |
| Azure OpenAI embeddings | External `text-embedding-3-large`; schema expects 3,072 dimensions | Retain model/dimension contract unless a separately approved schema migration changes it | Initial and change tokens, query tokens, batch behavior, ingestion window, retries and regional quota. |
| Key Vault | External Standard in the current dev inventory | Standard is sufficient for the current secret/PFX use; Premium is required only for HSM-backed keys | Requests/second, object count, rotation and availability. See [Key Vault tiers](https://learn.microsoft.com/azure/key-vault/general/overview#service-tiers). |
| VNet, private DNS, private endpoints | No SKU tier | Retain the private production topology | Address space and endpoint count require maximum scale, optional resources, regions and headroom. |
| Managed identities and Entra app registrations | No SKU | Retain | No capacity SKU; validate identities, roles and directory limits separately. |

### Required workload evidence

Final production capacity requires, at minimum:

1. Number of businesses and actual bytes, libraries and file count per site.
2. File-type mix and average, P95, P99 and maximum file sizes.
3. Pages, slides, worksheets/cells, audio duration, extracted characters and
   tokens by percentile.
4. Initial-ingestion completion window and concurrent businesses/sites/files.
5. Peak new, changed, ACL-changed and deleted documents per hour.
6. Observed chunks per document, Cosmos item/index sizes and per-operation RU.
7. Peak retrieval QPS, concurrency, mode mix, top-K and latency SLOs.
8. Planner/agentic calls and prompt, context and completion tokens per query.
9. Availability target, zones/regions, RPO, RTO and data residency.
10. Telemetry GB/day, retention, cost ceiling and growth horizon.

Until those inputs and measurements exist, exact Cosmos RU/s, Function
instances/memory, ACA CPU/memory/replicas, OpenAI TPM/PTU, AI-service quota,
monitoring cap, and regional topology are **not approved production SKUs**.

## Sources of Truth

- Resource graph: `infra/main.bicep` and `infra/modules/*.bicep`.
- Deployment sequencing and mutation controls: `scripts/deploy.ps1`.
- Parameters: `infra/main.parameters.bicepparam` and `infra/operations.parameters.bicepparam`.
- Contract tests: `tests/infra/test_bicep_contracts.py` and `tests/infra/test_deployment_contract.py`.
- Runtime boundaries: [ARCHITECTURE.md](ARCHITECTURE.md).
