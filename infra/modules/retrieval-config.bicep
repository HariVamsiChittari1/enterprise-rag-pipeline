// Canonical host-neutral retrieval-service configuration consumed by ACA.

@description('Cosmos DB endpoint')
param cosmosEndpoint string

@description('Cosmos DB database name')
param cosmosDatabaseName string

@description('Azure OpenAI endpoint')
param openAiEndpoint string

@description('Chat model deployment name')
param chatDeploymentName string

@description('Embedding model deployment name')
param embeddingDeploymentName string = 'text-embedding-3-large'

@description('Entra tenant ID for Graph group resolution')
param tenantId string

@description('Managed identity client ID (target-specific: differs between ACA and AKS)')
param managedIdentityClientId string

@description('Retrieval API client ID validated as the v2 access-token audience')
@minLength(1)
param retrievalApiAudience string

@description('Function managed identity application/client ID allowed to call retrieval')
param gatewayClientId string

@description('Function managed identity service-principal object ID allowed to call retrieval')
param gatewayPrincipalId string

@description('Unique deployment instance and Cosmos retrieval-config partition key')
@minLength(1)
param deploymentInstanceId string

@description('Runtime catalog poll interval in seconds')
@minValue(60)
@maxValue(86400)
param catalogPollSeconds int = 7200

@description('Enable ACL filtering at retrieval time')
param aclEnabled bool = true

@description('Application Insights connection string (empty to disable tracing)')
param appInsightsConnectionString string = ''

// --- Tuning knobs (sensible defaults, override per environment) ---

@description('Cosmos container name for search chunks')
param cosmosChunksContainer string = 'search-chunks'

@description('Cosmos container name for source document manifests')
param cosmosManifestsContainer string = 'source-documents'

@description('Cosmos container name for audit records')
param cosmosAuditContainer string = 'service-audit'

@description('Timeout (seconds) for Cosmos vector/hybrid retrieval calls')
param retrievalTimeoutSeconds string = '5.0'

@description('Timeout (seconds) for OpenAI chat completion (answer generation)')
param generationTimeoutSeconds string = '15.0'

@description('Timeout (seconds) for the full agentic reasoning loop')
param agentTimeoutSeconds string = '20.0'

@description('Max tool-call iterations the agent may perform')
param agentMaxIterations string = '5'

@description('OpenAI API version used for agent/tool-call requests')
// Azure OpenAI v1 Responses API only supports "preview" today; "latest" (GA) isn't released yet.
param agentOpenAiApiVersion string = 'preview'

@description('Maximum evidence chunks returned per retrieval call')
param maxEvidenceChunks string = '5'

@description('Maximum planned sub-queries the planner may emit')
param maxPlannedQueries string = '3'

@description('Timeout (seconds) for Microsoft Graph group membership lookups')
param graphGroupTimeoutSeconds string = '10.0'

@description('OpenAI API version for embeddings and standard chat')
param openAiApiVersion string = '2024-10-21'

@description('Include source citations in query responses')
param includeCitations bool = true

@description('Cosmos container containing the mutable runtime catalog.')
param retrievalConfigContainer string = 'retrieval-config'

@description('Wall-clock operation deadline at ACA ingress')
param operationTimeoutSeconds string = '27.0'

// --- Output: complete env var array consumable by ACA container or AKS configmap ---

var envVars = [
  { name: 'COSMOS_ENDPOINT', value: cosmosEndpoint }
  { name: 'COSMOS_DATABASE', value: cosmosDatabaseName }
  { name: 'COSMOS_CHUNKS_CONTAINER', value: cosmosChunksContainer }
  { name: 'COSMOS_MANIFESTS_CONTAINER', value: cosmosManifestsContainer }
  { name: 'COSMOS_AUDIT_CONTAINER', value: cosmosAuditContainer }
  { name: 'AZURE_OPENAI_ENDPOINT', value: openAiEndpoint }
  { name: 'EMBEDDING_DEPLOYMENT', value: embeddingDeploymentName }
  { name: 'CHAT_DEPLOYMENT', value: chatDeploymentName }
  { name: 'TENANT_ID', value: tenantId }
  { name: 'MANAGED_IDENTITY_CLIENT_ID', value: managedIdentityClientId }
  { name: 'RETRIEVAL_API_AUDIENCE', value: retrievalApiAudience }
  { name: 'RETRIEVAL_GATEWAY_CLIENT_ID', value: gatewayClientId }
  { name: 'RETRIEVAL_GATEWAY_PRINCIPAL_ID', value: gatewayPrincipalId }
  { name: 'DEPLOYMENT_INSTANCE_ID', value: deploymentInstanceId }
  { name: 'RETRIEVAL_CATALOG_POLL_SECONDS', value: string(catalogPollSeconds) }
  { name: 'ACL_ENABLED', value: string(aclEnabled) }
  { name: 'INCLUDE_CITATIONS', value: string(includeCitations) }
  { name: 'MAX_EVIDENCE_CHUNKS', value: maxEvidenceChunks }
  { name: 'MAX_PLANNED_QUERIES', value: maxPlannedQueries }
  { name: 'RETRIEVAL_TIMEOUT_SECONDS', value: retrievalTimeoutSeconds }
  { name: 'GENERATION_TIMEOUT_SECONDS', value: generationTimeoutSeconds }
  { name: 'AGENT_TIMEOUT_SECONDS', value: agentTimeoutSeconds }
  { name: 'AGENT_MAX_ITERATIONS', value: agentMaxIterations }
  { name: 'AGENT_OPENAI_API_VERSION', value: agentOpenAiApiVersion }
  { name: 'GRAPH_GROUP_TIMEOUT_SECONDS', value: graphGroupTimeoutSeconds }
  { name: 'OPENAI_API_VERSION', value: openAiApiVersion }
  { name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: appInsightsConnectionString }
  { name: 'RETRIEVAL_CONFIG_CONTAINER', value: retrievalConfigContainer }
  { name: 'RETRIEVAL_OPERATION_TIMEOUT_SECONDS', value: operationTimeoutSeconds }
]

@description('Complete env var array for the retrieval service container')
output envVars array = envVars

@description('Flat key-value map for K8s configmap generation or deployment outputs')
output configMap object = reduce(envVars, {}, (cur, item) => union(cur, { '${item.name}': item.value }))
