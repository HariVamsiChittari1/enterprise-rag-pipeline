targetScope = 'resourceGroup'

@description('Unique deployment instance identifier')
@minLength(1)
param deploymentInstanceId string

@description('Azure region')
param location string = resourceGroup().location

@description('Existing Azure OpenAI account name')
@minLength(1)
param openAiAccountName string

@description('Resource group containing the existing Azure OpenAI account')
@minLength(1)
param openAiResourceGroupName string = resourceGroup().name

@description('Existing text-embedding-3-large deployment name')
param embeddingDeploymentName string = 'text-embedding-3-large'

@description('Existing chat deployment name')
@minLength(1)
param chatDeploymentName string

@description('Enable Document Intelligence extraction')
param documentIntelligenceEnabled bool = true

@description('Enable Content Understanding extraction and infrastructure')
param contentUnderstandingEnabled bool = false

@description('API-version-pinned Content Understanding analyzer ID')
@minLength(1)
param contentUnderstandingAnalyzerId string = 'prebuilt-documentSearch'

@description('Temporary public IPv4 address allowed during guarded Content Understanding setup; empty keeps private-only access')
param contentUnderstandingAllowedIpAddress string = ''

@description('SharePoint tenant ID')
param sharePointTenantId string

@description('SharePoint application client ID')
param sharePointAppClientId string

@description('Assigned SharePoint document-library drive ID')
param sharePointDriveId string

@description('SharePoint site URL for site group ACL resolution via REST API')
@minLength(1)
param sharePointSiteUrl string

@description('Stable source registration ID')
param ingestionSourceId string

@description('Key Vault secret containing the exportable SharePoint PFX')
param sharePointCertificateSecretName string = 'sharepoint-app-cert'

@description('Existing Key Vault containing the SharePoint certificate')
param sharePointKeyVaultName string

@description('Resource group containing the existing SharePoint certificate Key Vault')
param sharePointKeyVaultResourceGroupName string

@description('Microsoft Entra application client ID protecting operator and query endpoints')
param adminApiClientId string

@description('Function API audience required in delegated user tokens')
param functionApiAudience string

@description('Retrieval API application/client ID')
param retrievalApiClientId string

@description('Retrieval API audience')
param retrievalApiAudience string

@description('Retrieval API service-principal object ID for external role-assignment validation')
param retrievalApiServicePrincipalId string

@secure()
@description('Shared secret for Microsoft Graph webhook clientState validation')
param webhookClientState string

@description('Entra app client IDs allowed to call the Function App API')
@minLength(1)
param allowedApplicationClientIds array

@description('Cosmos DB mode')
@allowed(['serverless', 'provisioned'])
param cosmosDbMode string = 'serverless'

@description('Cosmos DB shared metadata autoscale maximum RU/s in provisioned mode')
@minValue(1000)
param cosmosMetadataAutoscaleMaxRUs int = 1000

@description('Cosmos DB dedicated search-chunks autoscale maximum RU/s in provisioned mode')
@minValue(1000)
param cosmosSearchChunksAutoscaleMaxRUs int = 1000

@description('Storage redundancy')
@allowed(['LRS', 'ZRS', 'GRS'])
param storageRedundancy string = 'ZRS'

@description('Use Document Intelligence F0 for development')
param useDocumentIntelligenceFreeTier bool = false

@description('Use Azure AI Language F0 for development')
param useLanguageFreeTier bool = false

@description('Create serving ACA and Function resources after immutable artifacts exist')
param deployServing bool = false

@description('Create the temporary private catalog publication job')
param deployOperations bool = false

@description('Application Insights daily cap in GB; -1 means unlimited')
param applicationInsightsDailyCapGb int = -1

@description('Enable ACL filtering in the retrieval service')
param aclEnabled bool = true

@description('Include source citations in retrieval query responses')
param includeCitations bool = true

@description('Immutable retrieval image reference repository@sha256:<digest>; required when deployServing=true')
param retrievalImageReference string = ''

@description('Reviewed seed sha256:<digest>; required only for explicit catalog initialization')
param retrievalCatalogDigest string = ''

@description('Human catalog editor principal object ID; empty leaves assignment disabled')
param catalogEditorPrincipalId string = ''

@description('Optional guarded catalog writer principal object ID')
param catalogWriterPrincipalId string = ''

@description('Read-only catalog observer principal object ID')
param catalogObserverPrincipalId string = ''

@description('Private catalog operation; ordinary redeployment verifies without writing')
@allowed(['publish-catalog', 'verify-catalog'])
param catalogOperation string = 'verify-catalog'

@description('Runtime catalog poll interval in seconds')
@minValue(60)
@maxValue(86400)
param retrievalCatalogPollSeconds int = 7200

@description('ACA minimum replicas')
@minValue(1)
param retrievalMinReplicas int = 1

@description('ACA maximum replicas')
@minValue(1)
param retrievalMaxReplicas int = 5

@description('ACA zone redundancy creation-time setting')
param retrievalZoneRedundant bool = false

@description('Existing Function integration subnet network security group resource ID')
param functionIntegrationSubnetNsgId string = ''

@description('Existing private endpoint subnet network security group resource ID')
param privateEndpointSubnetNsgId string = ''

@description('Existing ACA environment subnet network security group resource ID')
param acaEnvironmentSubnetNsgId string = ''

@description('Resource tags')
param tags object = {
  DeploymentInstance: deploymentInstanceId
  Project: 'RAG-SharePoint'
  ManagedBy: 'azd'
}

var suffix = take(uniqueString(resourceGroup().id, deploymentInstanceId), 8)
var prefix = 'rag-${deploymentInstanceId}'
var storageName = take('st${replace(prefix, '-', '')}${suffix}', 24)
var openAiEndpoint = 'https://${openAiAccountName}.openai.azure.com/'
var sharePointKeyVaultId = resourceId(
  subscription().subscriptionId,
  sharePointKeyVaultResourceGroupName,
  'Microsoft.KeyVault/vaults',
  sharePointKeyVaultName
)
var sharePointKeyVaultUri = 'https://${sharePointKeyVaultName}${environment().suffixes.keyvaultDns}'

module monitoring './modules/monitoring.bicep' = {
  name: 'monitoring'
  params: {
    logAnalyticsName: '${prefix}-logs'
    applicationInsightsName: '${prefix}-ai'
    location: location
    dailyQuotaGb: applicationInsightsDailyCapGb
    retrievalPrincipalId: acaRetrievalIdentity.outputs.identityPrincipalId
    observerPrincipalId: catalogObserverPrincipalId
    tags: tags
  }
}

module storage './modules/storage.bicep' = {
  name: 'storage'
  params: {
    storageAccountName: storageName
    location: location
    redundancy: storageRedundancy
    tags: tags
  }
}

module identity './modules/identity.bicep' = {
  name: 'identity'
  params: {
    identityName: '${prefix}-functions-mi'
    location: location
    tags: tags
  }
}

module cosmos './modules/cosmos.bicep' = {
  name: 'cosmos'
  params: {
    cosmosAccountName: take('${prefix}-cosmos-${suffix}', 44)
    logAnalyticsWorkspaceId: monitoring.outputs.logAnalyticsId
    location: location
    mode: cosmosDbMode
    metadataAutoscaleMaxThroughput: cosmosMetadataAutoscaleMaxRUs
    searchChunksAutoscaleMaxThroughput: cosmosSearchChunksAutoscaleMaxRUs
    tags: tags
  }
}

module durableTask './modules/durable-task.bicep' = {
  params: {
    schedulerName: take('${prefix}-dts-${suffix}', 45)
    location: location
    functionAppPrincipalId: identity.outputs.identityPrincipalId
    // Independent scaling per source: each source's azd deployment provisions its own
    // scheduler, so the task hub is named from the source rather than a hardcoded
    // 'full-sync' literal.
    taskHubName: take('${ingestionSourceId}-sync', 45)
    tags: tags
  }
}

module documentIntelligence './modules/ai-services.bicep' = {
  name: 'document-intelligence'
  params: {
    documentIntelligenceName: '${prefix}-doc-intel-${suffix}'
    languageServiceName: '${prefix}-lang-${suffix}'
    location: location
    useFreeF0: useDocumentIntelligenceFreeTier
    useLanguageFreeTier: useLanguageFreeTier
    tags: tags
  }
}

module contentUnderstanding './modules/content-understanding.bicep' = if (contentUnderstandingEnabled) {
  name: 'content-understanding'
  params: {
    accountName: take('${prefix}-cu-${suffix}', 64)
    location: location
    allowedIpAddress: contentUnderstandingAllowedIpAddress
    tags: tags
  }
}

module networking './modules/networking.bicep' = {
  name: 'networking'
  params: {
    virtualNetworkName: '${prefix}-vnet'
    location: location
    functionIntegrationSubnetNsgId: functionIntegrationSubnetNsgId
    privateEndpointSubnetNsgId: privateEndpointSubnetNsgId
    acaEnvironmentSubnetNsgId: acaEnvironmentSubnetNsgId
    privateEndpointTargets: concat(
      [
        {
          name: 'storage-blob'
          resourceId: storage.outputs.storageAccountId
          groupId: 'blob'
          dnsZoneName: 'privatelink.blob.${environment().suffixes.storage}'
        }
        {
          name: 'storage-queue'
          resourceId: storage.outputs.storageAccountId
          groupId: 'queue'
          dnsZoneName: 'privatelink.queue.${environment().suffixes.storage}'
        }
        {
          name: 'storage-table'
          resourceId: storage.outputs.storageAccountId
          groupId: 'table'
          dnsZoneName: 'privatelink.table.${environment().suffixes.storage}'
        }
        {
          name: 'cosmos-sql'
          resourceId: cosmos.outputs.cosmosAccountId
          groupId: 'Sql'
          dnsZoneName: 'privatelink.documents.azure.com'
        }
        {
          name: 'key-vault'
          resourceId: sharePointKeyVaultId
          groupId: 'vault'
          dnsZoneName: 'privatelink.vaultcore.azure.net'
        }
        {
          name: 'document-intelligence'
          resourceId: documentIntelligence.outputs.documentIntelligenceId
          groupId: 'account'
          dnsZoneName: 'privatelink.cognitiveservices.azure.com'
        }
        {
          name: 'language-service'
          resourceId: documentIntelligence.outputs.languageServiceId
          groupId: 'account'
          dnsZoneName: 'privatelink.cognitiveservices.azure.com'
        }
      ],
      contentUnderstandingEnabled
        ? [
            {
              name: 'content-understanding'
              resourceId: contentUnderstanding.?outputs.?accountId ?? ''
              groupId: 'account'
              dnsZoneName: 'privatelink.services.ai.azure.com'
            }
          ]
        : []
    )
    tags: tags
  }
}

module acaEnvironment './modules/aca-environment.bicep' = {
  params: {
    environmentName: '${prefix}-retrieval-${suffix}-env'
    location: location
    infrastructureSubnetId: networking.outputs.acaSubnetId
    logAnalyticsWorkspaceId: monitoring.outputs.logAnalyticsId
    zoneRedundant: retrievalZoneRedundant
    tags: tags
  }
}

module functions './modules/functions.bicep' = if (deployServing) {
  name: 'functions'
  params: {
    functionAppName: '${prefix}-func-${suffix}'
    location: location
    managedIdentityId: identity.outputs.identityId
    managedIdentityClientId: identity.outputs.identityClientId
    storageAccountName: storage.outputs.storageAccountName
    deploymentStorageContainerUrl: storage.outputs.deploymentContainerUrl
    integrationSubnetId: networking.outputs.integrationSubnetId
    appInsightsConnectionString: monitoring.outputs.connectionString
    cosmosEndpoint: cosmos.outputs.endpoint
    cosmosDatabaseName: cosmos.outputs.databaseName
    cosmosContainerNames: {
      ingestionRuns: cosmos.outputs.ingestionRunsContainerName
      sourceDocuments: cosmos.outputs.sourceDocumentsContainerName
      searchChunks: cosmos.outputs.searchChunksContainerName
    }
    durableTaskSchedulerEndpoint: durableTask.outputs.endpoint
    durableTaskHubName: durableTask.outputs.taskHubName
    openAiEndpoint: openAiEndpoint
    embeddingDeploymentName: embeddingDeploymentName
    chatDeploymentName: chatDeploymentName
    documentIntelligenceEndpoint: documentIntelligence.outputs.documentIntelligenceEndpoint
    documentIntelligenceEnabled: documentIntelligenceEnabled
    contentUnderstandingEndpoint: contentUnderstanding.?outputs.?endpoint ?? ''
    contentUnderstandingAnalyzerId: contentUnderstandingEnabled ? contentUnderstandingAnalyzerId : ''
    contentUnderstandingEnabled: contentUnderstandingEnabled
    languageEndpoint: documentIntelligence.outputs.languageServiceEndpoint
    keyVaultUri: sharePointKeyVaultUri
    entraTenantId: sharePointTenantId
    sharePointAppClientId: sharePointAppClientId
    ingestionSourceId: ingestionSourceId
    sharePointDriveId: sharePointDriveId
    sharePointSiteUrl: sharePointSiteUrl
    sharePointCertificateSecretName: sharePointCertificateSecretName
    adminApiClientId: adminApiClientId
    functionApiAudience: functionApiAudience
    retrievalServiceScope: '${retrievalApiAudience}/.default'
    webhookClientState: webhookClientState
    allowedApplicationClientIds: allowedApplicationClientIds
    retrievalServiceUrl: aca.?outputs.?internalUrl ?? ''
    tags: tags
  }
}

module rbac './modules/rbac.bicep' = {
  name: 'rbac'
  params: {
    principalId: identity.outputs.identityPrincipalId
    cosmosAccountId: cosmos.outputs.cosmosAccountId
    storageAccountId: storage.outputs.storageAccountId
    documentIntelligenceId: documentIntelligence.outputs.documentIntelligenceId
    contentUnderstandingId: contentUnderstanding.?outputs.?accountId ?? ''
    languageServiceId: documentIntelligence.outputs.languageServiceId
    applicationInsightsId: monitoring.outputs.applicationInsightsId
  }
}

module sharePointKeyVaultRbac './modules/keyvault-secrets-user-rbac.bicep' = {
  scope: resourceGroup(sharePointKeyVaultResourceGroupName)
  params: {
    principalId: identity.outputs.identityPrincipalId
    keyVaultName: sharePointKeyVaultName
  }
}

module openAiRbac './modules/openai-rbac.bicep' = {
  name: 'openai-rbac'
  scope: resourceGroup(openAiResourceGroupName)
  params: {
    principalId: identity.outputs.identityPrincipalId
    openAiAccountName: openAiAccountName
  }
}

module acr './modules/acr.bicep' = {
  name: 'acr'
  params: {
    registryName: take('${replace(prefix, '-', '')}acr${suffix}', 50)
    location: location
    kubeletPrincipalId: ''
    appIdentityPrincipalIds: [
      acaRetrievalIdentity.outputs.identityPrincipalId
      operationsIdentity.outputs.identityPrincipalId
    ]
    tags: tags
  }
}

module acaRetrievalIdentity './modules/identity.bicep' = {
  name: 'aca-retrieval-identity'
  params: {
    identityName: '${prefix}-aca-retrieval-mi'
    location: location
    tags: tags
  }
}

module operationsIdentity './modules/identity.bicep' = {
  params: {
    identityName: '${prefix}-operations-mi'
    location: location
    tags: tags
  }
}

module acaRetrievalCosmosRbac './modules/retrieval-cosmos-rbac.bicep' = {
  name: 'aca-retrieval-cosmos-rbac'
  params: {
    principalId: acaRetrievalIdentity.outputs.identityPrincipalId
    cosmosAccountId: cosmos.outputs.cosmosAccountId
    cosmosDatabaseName: cosmos.outputs.databaseName
    serviceAuditContainerName: cosmos.outputs.serviceAuditContainerName
    searchChunksContainerName: cosmos.outputs.searchChunksContainerName
    sourceDocumentsContainerName: cosmos.outputs.sourceDocumentsContainerName
    retrievalConfigContainerName: cosmos.outputs.retrievalConfigContainerName
  }
}

module acaRetrievalOpenAiRbac './modules/openai-rbac.bicep' = {
  name: 'aca-retrieval-openai-rbac'
  scope: resourceGroup(openAiResourceGroupName)
  params: {
    principalId: acaRetrievalIdentity.outputs.identityPrincipalId
    openAiAccountName: openAiAccountName
  }
}

module retrievalConfigPublisherRbac './modules/retrieval-config-publisher-rbac.bicep' = {
  name: 'retrieval-config-publisher-rbac'
  params: {
    publisherPrincipalId: operationsIdentity.outputs.identityPrincipalId
    editorPrincipalId: catalogEditorPrincipalId
    writerPrincipalId: catalogWriterPrincipalId
    cosmosAccountId: cosmos.outputs.cosmosAccountId
    cosmosDatabaseName: cosmos.outputs.databaseName
    retrievalConfigContainerName: cosmos.outputs.retrievalConfigContainerName
  }
}

module operationsJob './modules/aca-operations-job.bicep' = if (deployOperations) {
  params: {
    jobName: '${take(replace(prefix, '-', ''), 19)}-catalog-job'
    location: location
    managedEnvironmentId: acaEnvironment.outputs.environmentId
    acrLoginServer: acr.outputs.loginServer
    imageReference: retrievalImageReference
    managedIdentityId: operationsIdentity.outputs.identityId
    managedIdentityClientId: operationsIdentity.outputs.identityClientId
    cosmosEndpoint: cosmos.outputs.endpoint
    cosmosDatabaseName: cosmos.outputs.databaseName
    retrievalConfigContainerName: cosmos.outputs.retrievalConfigContainerName
    deploymentInstanceId: deploymentInstanceId
    catalogDigest: retrievalCatalogDigest
    catalogOperation: catalogOperation
    tags: tags
  }
}

module retrievalConfig './modules/retrieval-config.bicep' = if (deployServing) {
  name: 'retrieval-config'
  params: {
    cosmosEndpoint: cosmos.outputs.endpoint
    cosmosDatabaseName: cosmos.outputs.databaseName
    openAiEndpoint: openAiEndpoint
    chatDeploymentName: chatDeploymentName
    embeddingDeploymentName: embeddingDeploymentName
    tenantId: sharePointTenantId
    managedIdentityClientId: acaRetrievalIdentity.outputs.identityClientId
    retrievalApiAudience: retrievalApiClientId
    gatewayClientId: identity.outputs.identityClientId
    gatewayPrincipalId: identity.outputs.identityPrincipalId
    deploymentInstanceId: deploymentInstanceId
    catalogPollSeconds: retrievalCatalogPollSeconds
    aclEnabled: aclEnabled
    includeCitations: includeCitations
    appInsightsConnectionString: monitoring.outputs.connectionString
    retrievalConfigContainer: cosmos.outputs.retrievalConfigContainerName
  }
}

module aca './modules/aca.bicep' = if (deployServing) {
  name: 'aca'
  params: {
    containerAppName: '${take(replace(prefix, '-', ''), 18)}-retr-${suffix}'
    observerPrincipalId: catalogObserverPrincipalId
    location: location
    acrLoginServer: acr.outputs.loginServer
    imageName: retrievalImageReference
    managedIdentityId: acaRetrievalIdentity.outputs.identityId
    managedEnvironmentId: acaEnvironment.outputs.environmentId
    retrievalEnvVars: retrievalConfig.?outputs.?envVars ?? []
    retrievalApiClientId: retrievalApiClientId
    retrievalApiAudience: retrievalApiClientId
    entraIssuer: '${environment().authentication.loginEndpoint}${sharePointTenantId}/v2.0'
    gatewayClientId: identity.outputs.identityClientId
    gatewayPrincipalId: identity.outputs.identityPrincipalId
    minReplicas: retrievalMinReplicas
    maxReplicas: retrievalMaxReplicas
    tags: tags
  }
}

module acaDns './modules/aca-dns.bicep' = if (deployServing) {
  name: 'aca-dns'
  params: {
    domainName: acaEnvironment.outputs.defaultDomain
    staticIp: acaEnvironment.outputs.staticIp
    virtualNetworkId: networking.outputs.virtualNetworkId
    tags: tags
  }
}

output functionAppName string = functions.?outputs.?functionAppName ?? ''
output functionAppUrl string = functions.?outputs.?functionAppUrl ?? ''
output managedIdentityClientId string = identity.outputs.identityClientId
output managedIdentityPrincipalId string = identity.outputs.identityPrincipalId
output retrievalIdentityClientId string = acaRetrievalIdentity.outputs.identityClientId
output retrievalIdentityPrincipalId string = acaRetrievalIdentity.outputs.identityPrincipalId
output operationsIdentityClientId string = operationsIdentity.outputs.identityClientId
output operationsIdentityPrincipalId string = operationsIdentity.outputs.identityPrincipalId
output operationsJobName string = operationsJob.?outputs.?jobName ?? ''
output retrievalApiServicePrincipalId string = retrievalApiServicePrincipalId
output keyVaultName string = sharePointKeyVaultName
output cosmosDbEndpoint string = cosmos.outputs.endpoint
output cosmosDbDatabaseName string = cosmos.outputs.databaseName
output documentIntelligenceEndpoint string = documentIntelligence.outputs.documentIntelligenceEndpoint
output contentUnderstandingAccountId string = contentUnderstanding.?outputs.?accountId ?? ''
output contentUnderstandingEndpoint string = contentUnderstanding.?outputs.?endpoint ?? ''
output contentUnderstandingCompletionDeploymentName string = contentUnderstanding.?outputs.?completionDeploymentName ?? ''
output contentUnderstandingEmbeddingDeploymentName string = contentUnderstanding.?outputs.?embeddingDeploymentName ?? ''
output openAiEndpoint string = openAiEndpoint
output acrLoginServer string = acr.outputs.loginServer
output retrievalServiceUrl string = aca.?outputs.?internalUrl ?? ''
output retrievalContainerAppName string = aca.?outputs.?containerAppName ?? ''
output catalogObservationWorkspaceId string = monitoring.outputs.logAnalyticsWorkspaceId
output retrievalConfigMap object = retrievalConfig.?outputs.?configMap ?? {}
