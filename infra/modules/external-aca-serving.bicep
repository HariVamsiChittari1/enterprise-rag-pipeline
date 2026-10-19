@description('Reviewed retrieval Container App resource name')
param containerAppName string

@description('Azure region')
param location string

@description('Existing external ACA environment resource ID')
param managedEnvironmentId string

@description('ACR login server')
param acrLoginServer string

@description('Immutable SHA-256 digest for the approved rag-retrieval repository')
param imageDigest string

@description('Retrieval UAMI resource ID')
param managedIdentityId string

@description('Retrieval UAMI client ID')
param managedIdentityClientId string

@description('Cosmos DB endpoint')
param cosmosEndpoint string

@description('Azure OpenAI endpoint')
param openAiEndpoint string

@description('Chat model deployment name')
param chatDeploymentName string

@description('Embedding model deployment name')
param embeddingDeploymentName string

@description('SharePoint tenant ID used for Microsoft Graph group resolution')
param sharePointTenantId string

@description('Verified Microsoft Entra tenant ID protecting the retrieval API')
param entraApiTenantId string

@description('Retrieval API application/client ID')
param retrievalApiClientId string

@description('Approved native/public caller client ID')
param retrievalAllowedCallerClientId string

@description('Application Insights connection string')
param appInsightsConnectionString string

@description('Runtime catalog poll interval in seconds')
param catalogPollSeconds int = 7200

@description('Include citations in retrieval responses')
param includeCitations bool = true

@description('Minimum ACA replicas')
param minReplicas int = 1

@description('Maximum ACA replicas')
param maxReplicas int = 5

@description('Resource tags')
param tags object

module retrievalConfig './retrieval-config.bicep' = {
  name: 'external-retrieval-config'
  params: {
    cosmosEndpoint: cosmosEndpoint
    cosmosDatabaseName: 'rag-db'
    openAiEndpoint: openAiEndpoint
    chatDeploymentName: chatDeploymentName
    embeddingDeploymentName: embeddingDeploymentName
    tenantId: sharePointTenantId
    managedIdentityClientId: managedIdentityClientId
    retrievalApiAudience: retrievalApiClientId
    catalogPollSeconds: catalogPollSeconds
    includeCitations: includeCitations
    appInsightsConnectionString: appInsightsConnectionString
    retrievalConfigContainer: 'retrieval-config'
    ingestionSourcesContainer: 'ingestion-sources'
    ingestionRunsContainer: 'ingestion-runs'
  }
}

module retrievalApp './aca.bicep' = {
  name: 'external-retrieval-app'
  params: {
    containerAppName: containerAppName
    location: location
    acrLoginServer: acrLoginServer
    imageDigest: imageDigest
    managedIdentityId: managedIdentityId
    managedEnvironmentId: managedEnvironmentId
    retrievalEnvVars: retrievalConfig.outputs.envVars
    retrievalApiClientId: retrievalApiClientId
    retrievalApiAudience: retrievalApiClientId
    allowedApplicationClientIds: [retrievalAllowedCallerClientId]
    entraIssuer: '${environment().authentication.loginEndpoint}${entraApiTenantId}/v2.0'
    minReplicas: minReplicas
    maxReplicas: maxReplicas
    tags: tags
  }
}

output retrievalFqdn string = retrievalApp.outputs.fqdn
output retrievalPublicUrl string = retrievalApp.outputs.publicUrl
output retrievalQueryUrl string = retrievalApp.outputs.queryUrl
