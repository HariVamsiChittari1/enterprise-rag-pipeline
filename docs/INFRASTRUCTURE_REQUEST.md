# Cloud Operations Request

Use this document to request the external prerequisites and permissions required by the active ACA deployment. The repository deployment creates project resources inside an existing resource group; it does not create Entra applications, grant tenant-wide admin consent, create the SharePoint site, create the Azure OpenAI account, or create the SharePoint certificate Key Vault.

## Required Target

Provide:

- Existing Azure subscription, tenant, resource group, and region.
- Unique `DEPLOYMENT_INSTANCE_ID`.
- Cost center and cleanup date tags.
- Existing Azure OpenAI account name/resource group and deployed chat and embedding model names. The account/resource group must be in the deployment subscription.
- Existing SharePoint site URL, document-library drive ID, stable ingestion source ID, and tenant ID.
- Existing Key Vault name/resource group and certificate secret name. The vault/resource group must be in the deployment subscription.

## Entra Applications

### SharePoint Ingestion Application

- Certificate credential whose exportable PFX is stored as a Key Vault secret.
- Graph application permissions required by the deployed five-format connector: `Sites.Selected`, `Sites.Read.All`, `Files.ReadWrite.All`, `GroupMember.Read.All`, and `User.Read.All`. `Files.ReadWrite.All` is required for Office-to-PDF conversion.
- SharePoint application permission `Sites.Read.All` for `/_api/web/sitegroups(...)/users` expansion.
- Required SharePoint site grant and tenant admin consent.

The current connector also requests permission-change metadata whose documented Microsoft Graph permission is `Sites.FullControl.All`. That broader permission is not approved by this request and must not be granted implicitly. Return a security decision to either approve it as a separately reviewed exception or require the connector and validation contract to stop depending on that metadata.

### Function API Application

- Single-tenant API application.
- Application ID URI supplied as `FUNCTION_API_AUDIENCE`.
- Delegated scope `user_impersonation`.
- Approved caller application client ID supplied as `FUNCTION_ALLOWED_CALLER_CLIENT_ID`.

### Retrieval API Application

- Single-tenant API application.
- Client ID/audience supplied as `RETRIEVAL_API_CLIENT_ID` and `RETRIEVAL_API_AUDIENCE`.
- Application role `Retrieval.Gateway`.
- Function UAMI service principal assigned to that role.
- Retrieval API service-principal object ID supplied as `RETRIEVAL_API_SERVICE_PRINCIPAL_ID` for external validation and deployment evidence.

Active Bicep does not create or consent these directory objects.

## Existing Key Vault

The Key Vault is an external prerequisite. Cloud operations must provide a private-connected administrative path for initial certificate upload and renewal. Do not delete its private endpoint or temporarily enable public access as part of this repository workflow.

The deployment creates a private endpoint/private DNS integration for the existing vault and assigns Key Vault Secrets User to the Function UAMI. It does not change the vault's public-access policy.

## Resources Created by Bicep

- Application Insights and Log Analytics.
- Storage account and Functions deployment container.
- Cosmos DB NoSQL account, database, and five containers.
- Durable Task Scheduler and source-derived task hub.
- Document Intelligence and Azure AI Language accounts.
- VNet, three subnets, seven private endpoints, and six unique private DNS zones.
- Azure Container Registry.
- Function, retrieval, and operations UAMIs.
- Internal ACA managed environment.
- Function App and retrieval Container App during the `Final` phase.
- Temporary catalog-publisher job during `Operations`; removed by `OperationsCleanup`.

Default storage redundancy is ZRS. Cosmos defaults to serverless but supports provisioned autoscale through reviewed environment inputs. Azure OpenAI is consumed as an existing resource; this template does not set model capacity.

## Cosmos Containers

| Container | Partition key |
| --- | --- |
| `ingestion-runs` | `/sourceId` |
| `source-documents` | `/sourceRunId` |
| `search-chunks` | `/documentKey` |
| `retrieval-config` | `/deploymentInstanceId` |
| `service-audit` | `/id` |

## Deployment Inputs

The guarded controller requires these azd environment values before resource mutation:

- `AZURE_OPENAI_ACCOUNT_NAME`, `AZURE_OPENAI_RESOURCE_GROUP`, `OPENAI_CHAT_DEPLOYMENT_NAME`.
- `SHAREPOINT_TENANT_ID`, `SHAREPOINT_APP_CLIENT_ID`, `SHAREPOINT_ASSIGNED_DRIVE_ID`, `SHAREPOINT_SITE_URL`.
- `SHAREPOINT_KEY_VAULT_NAME`, `SHAREPOINT_KEY_VAULT_RESOURCE_GROUP`, and optionally `SHAREPOINT_CERTIFICATE_SECRET_NAME`.
- `INGESTION_SOURCE_ID`.
- `ADMIN_API_CLIENT_ID`, `FUNCTION_API_AUDIENCE`, `FUNCTION_ALLOWED_CALLER_CLIENT_ID`.
- `RETRIEVAL_API_CLIENT_ID`, `RETRIEVAL_API_AUDIENCE`, `RETRIEVAL_API_SERVICE_PRINCIPAL_ID`.
- `WEBHOOK_CLIENT_STATE`, supplied through `azd env set-secret WEBHOOK_CLIENT_STATE` or an organization-approved external secret source, never `azd env set`.
- `COST_CENTER`, `CLEANUP_DATE`.

Capacity and reliability inputs include Cosmos mode/RUs, storage redundancy, Application Insights daily cap, ACA replica bounds, and ACA zone redundancy.

Artifact and catalog inputs have distinct lifecycles:

- `RETRIEVAL_IMAGE_REFERENCE=repository@sha256:<digest>`.
- `RETRIEVAL_CATALOG_DIGEST=sha256:<digest>` verifies the reviewed seed only during explicit initialization; it is not a serving pin.
- Current catalog evidence records its ETag and validated config digest.

Optional editor, guarded-writer, and observer principals and polling inputs are
listed in [configuration](CONFIGURATION.md). The
[resource reuse contract](AZURE_RESOURCES.md#resource-reuse-contract) distinguishes
external prerequisites from same-instance redeployment and unsupported adoption.

## Deployment and Acceptance

Cloud operations must use `scripts/deploy.ps1`, which defaults to preview and requires reviewed plan/source hashes plus exact subscription, tenant, resource group, region, azd environment, and deployment instance. The ordered phases are:

1. `Authority`
2. `Foundation`
3. `Build`
4. `Operations`
5. `Catalog`
6. `CatalogVerify`
7. `Final`
8. `Function`
9. End-to-end validation and fixture cleanup.
10. `OperationsCleanup` after explicit approval.

Acceptance evidence must include successful Bicep what-if/deployment, immutable
image identity, current catalog ETag/digest and read-only verification, Function
and ACA health, exact authentication, private endpoint/DNS and managed identity,
effective role denials, private keyless Data Explorer editing, replica adoption,
end-to-end retrieval/ACL tests, and approved temporary job cleanup. Review
account-wide diagnostic volume and retention before enabling export. See
[Azure setup](AZURE_SETUP.md) for the initialization and verification sequence.

## Required Outputs

Return the outputs declared by `infra/main.bicep`: Function name/URL, all three UAMI identifiers, Cosmos endpoint/database, AI endpoints, ACR login server, retrieval URL/Container App name, and retrieval configuration map. The Key Vault name is returned as an existing input; no AKS output is part of the active deployment.
