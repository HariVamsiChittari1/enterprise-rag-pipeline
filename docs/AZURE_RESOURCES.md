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
| Function UAMI | `identity.bicep` | Function runtime identity and retrieval gateway identity |
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
| `ingestion-runs` | `/sourceId` | Full-sync runs, source controls, delta cursor, trigger IDs, webhook subscription ID |
| `source-documents` | `/sourceRunId` | Schema-v1 `source_document` records and paged `visual_manifest_page` evidence, discriminated by `recordType` |
| `search-chunks` | `/documentKey` | Content, 3,072-dimensional embeddings, full-text fields, ACL and retrieval eligibility |
| `retrieval-config` | `/deploymentInstanceId` | Mutable `runtime-catalog` singleton; optional guarded-writer history and outcome items |
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

The Function UAMI is also the application identity allowed to call retrieval. Assignment of the external retrieval API's `Retrieval.Gateway` app role is an Entra prerequisite; active Bicep does not mutate directory objects.

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
| Retrieval API application | Exposes application role `Retrieval.Gateway`; Function UAMI receives that role |

`FUNCTION_ALLOWED_CALLER_CLIENT_ID` is required. Function EasyAuth accepts exactly `FUNCTION_API_AUDIENCE` and the configured caller application. ACA Authentication accepts only the Function UAMI application and principal.

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

## Sources of Truth

- Resource graph: `infra/main.bicep` and `infra/modules/*.bicep`.
- Deployment sequencing and mutation controls: `scripts/deploy.ps1`.
- Parameters: `infra/main.parameters.bicepparam` and `infra/operations.parameters.bicepparam`.
- Contract tests: `tests/infra/test_bicep_contracts.py` and `tests/infra/test_deployment_contract.py`.
- Runtime boundaries: [ARCHITECTURE.md](ARCHITECTURE.md).
