@description('Function App managed identity principal ID')
param principalId string

@description('Cosmos DB account resource ID')
param cosmosAccountId string

@description('Cosmos DB database name')
param cosmosDatabaseName string

@description('Ingestion runs container name')
param ingestionRunsContainerName string

@description('Ingestion source registry container name')
param ingestionSourcesContainerName string

@description('Source documents container name')
param sourceDocumentsContainerName string

@description('Search chunks container name')
param searchChunksContainerName string

@description('Service audit container name')
param serviceAuditContainerName string

@description('Storage account resource ID')
param storageAccountId string

@description('Document Intelligence resource ID')
param documentIntelligenceId string

@description('Content Understanding resource ID')
param contentUnderstandingId string

@description('Speech resource ID')
param speechId string = ''

@description('Azure AI Language resource ID')
param languageServiceId string

@description('Application Insights resource ID')
param applicationInsightsId string

resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' existing = {
  name: split(cosmosAccountId, '/')[8]
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: split(storageAccountId, '/')[8]
}

resource documentIntelligence 'Microsoft.CognitiveServices/accounts@2024-10-01' existing = {
  name: split(documentIntelligenceId, '/')[8]
}

resource contentUnderstanding 'Microsoft.CognitiveServices/accounts@2025-06-01' existing = if (!empty(contentUnderstandingId)) {
  name: split(contentUnderstandingId, '/')[8]
}

resource speech 'Microsoft.CognitiveServices/accounts@2025-06-01' existing = if (!empty(speechId)) {
  name: split(speechId, '/')[8]
}

resource languageService 'Microsoft.CognitiveServices/accounts@2024-10-01' existing = {
  name: split(languageServiceId, '/')[8]
}

resource applicationInsights 'Microsoft.Insights/components@2020-02-02' existing = {
  name: split(applicationInsightsId, '/')[8]
}

var roles = {
  CosmosDBDataContributor: '00000000-0000-0000-0000-000000000002'
  StorageBlobDataOwner: 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b'
  StorageQueueDataContributor: '974c5e8b-45b9-4653-ba55-5f855dd0fb88'
  StorageTableDataContributor: '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'
  CognitiveServicesUser: 'a97b65f3-24c7-4388-baec-2e87135dc908'
  ContentUnderstandingContributor: '59a2dba3-6303-4fd8-9a2e-8cbb4bdda972'
  CognitiveServicesSpeechUser: 'f2dc8367-1007-4938-bd23-fe263f013447'
  MonitoringMetricsPublisher: '3913510d-42f4-4e42-8a64-420c390055eb'
}

var writableCosmosContainerNames = [
  ingestionRunsContainerName
  sourceDocumentsContainerName
  searchChunksContainerName
]

resource cosmosDataContributors 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = [
  for containerName in writableCosmosContainerNames: {
  parent: cosmosAccount
  name: guid(cosmosAccountId, principalId, 'function-data-contributor', containerName)
  properties: {
    roleDefinitionId: '${cosmosAccountId}/sqlRoleDefinitions/${roles.CosmosDBDataContributor}'
    principalId: principalId
    scope: '${cosmosAccountId}/dbs/${cosmosDatabaseName}/colls/${containerName}'
  }
}]

resource cosmosSourceRegistryReader 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = {
  parent: cosmosAccount
  name: guid(cosmosAccountId, principalId, 'function-source-registry-reader')
  properties: {
    roleDefinitionId: '${cosmosAccountId}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000001'
    principalId: principalId
    scope: '${cosmosAccountId}/dbs/${cosmosDatabaseName}/colls/${ingestionSourcesContainerName}'
  }
}

resource functionAuditWriterRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions@2024-05-15' = {
  parent: cosmosAccount
  name: guid(cosmosAccountId, principalId, 'function-audit-create-read-role')
  properties: {
    roleName: 'Function audit create and read'
    type: 'CustomRole'
    assignableScopes: [cosmosAccountId]
    permissions: [
      {
        dataActions: [
          'Microsoft.DocumentDB/databaseAccounts/readMetadata'
          'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/create'
          'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/read'
        ]
      }
    ]
  }
}

resource functionAuditWriter 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = {
  parent: cosmosAccount
  name: guid(cosmosAccountId, principalId, 'function-audit-create-read')
  properties: {
    roleDefinitionId: functionAuditWriterRole.id
    principalId: principalId
    scope: '${cosmosAccountId}/dbs/${cosmosDatabaseName}/colls/${serviceAuditContainerName}'
  }
}

resource storageBlobDataOwner 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, principalId, roles.StorageBlobDataOwner)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.StorageBlobDataOwner)
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

resource storageQueueDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, principalId, roles.StorageQueueDataContributor)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      roles.StorageQueueDataContributor
    )
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

resource storageTableDataContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, principalId, roles.StorageTableDataContributor)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      roles.StorageTableDataContributor
    )
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

resource documentIntelligenceUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(documentIntelligence.id, principalId, roles.CognitiveServicesUser)
  scope: documentIntelligence
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.CognitiveServicesUser)
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

resource contentUnderstandingContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(contentUnderstandingId)) {
  name: guid(contentUnderstanding.id, principalId, roles.ContentUnderstandingContributor)
  scope: contentUnderstanding
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      roles.ContentUnderstandingContributor
    )
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

resource languageServiceUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(languageService.id, principalId, roles.CognitiveServicesUser)
  scope: languageService
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.CognitiveServicesUser)
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

resource speechUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(speechId)) {
  name: guid(speech.id, principalId, roles.CognitiveServicesSpeechUser)
  scope: speech
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      roles.CognitiveServicesSpeechUser
    )
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

resource monitoringPublisher 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(applicationInsights.id, principalId, roles.MonitoringMetricsPublisher)
  scope: applicationInsights
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      roles.MonitoringMetricsPublisher
    )
    principalId: principalId
    principalType: 'ServicePrincipal'
  }
}

output roleAssignmentsCreated int = 7 + (empty(contentUnderstandingId) ? 0 : 1) + (empty(speechId) ? 0 : 1)
