using './modules/external-aca-serving.bicep'

param containerAppName = readEnvironmentVariable('RETRIEVAL_APP_NAME')
param location = readEnvironmentVariable('AZURE_LOCATION')
param managedEnvironmentId = readEnvironmentVariable('MANAGED_ENVIRONMENT_ID')
param acrLoginServer = readEnvironmentVariable('ACR_LOGIN_SERVER')
param imageDigest = readEnvironmentVariable('RETRIEVAL_IMAGE_DIGEST')
param managedIdentityId = readEnvironmentVariable('RETRIEVAL_MANAGED_IDENTITY_ID')
param managedIdentityClientId = readEnvironmentVariable('RETRIEVAL_MANAGED_IDENTITY_CLIENT_ID')
param cosmosEndpoint = readEnvironmentVariable('COSMOS_ENDPOINT')
param openAiEndpoint = readEnvironmentVariable('AZURE_OPENAI_ENDPOINT')
param chatDeploymentName = readEnvironmentVariable('OPENAI_CHAT_DEPLOYMENT_NAME')
param embeddingDeploymentName = readEnvironmentVariable('OPENAI_EMBEDDING_DEPLOYMENT_NAME', 'text-embedding-3-large')
param sharePointTenantId = readEnvironmentVariable('SHAREPOINT_TENANT_ID')
param entraApiTenantId = readEnvironmentVariable('ENTRA_API_TENANT_ID')
param retrievalApiClientId = readEnvironmentVariable('RETRIEVAL_API_CLIENT_ID')
param retrievalAllowedCallerClientId = readEnvironmentVariable('RETRIEVAL_ALLOWED_CALLER_CLIENT_ID')
param appInsightsConnectionString = readEnvironmentVariable('APPLICATIONINSIGHTS_CONNECTION_STRING')
param catalogPollSeconds = int(readEnvironmentVariable('RETRIEVAL_CATALOG_POLL_SECONDS', '7200'))
param includeCitations = readEnvironmentVariable('INCLUDE_CITATIONS', 'true') == 'true'
param minReplicas = int(readEnvironmentVariable('RETRIEVAL_MIN_REPLICAS', '1'))
param maxReplicas = int(readEnvironmentVariable('RETRIEVAL_MAX_REPLICAS', '5'))
param tags = {
  DeploymentInstance: readEnvironmentVariable('DEPLOYMENT_INSTANCE_ID')
  Project: 'RAG-SharePoint'
  ManagedBy: 'azd'
  CostCenter: readEnvironmentVariable('COST_CENTER')
  CleanupDate: readEnvironmentVariable('CLEANUP_DATE')
}
