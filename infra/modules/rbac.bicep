@description('Function App managed identity principal ID')
param principalId string

@description('Cosmos DB account resource ID')
param cosmosAccountId string

@description('Storage account resource ID')
param storageAccountId string

@description('Document Intelligence resource ID')
param documentIntelligenceId string

@description('Content Understanding resource ID')
param contentUnderstandingId string

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
  MonitoringMetricsPublisher: '3913510d-42f4-4e42-8a64-420c390055eb'
}

resource cosmosDataContributor 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = {
  parent: cosmosAccount
  name: guid(cosmosAccountId, principalId, 'CosmosDBDataContributor')
  properties: {
    roleDefinitionId: '${cosmosAccountId}/sqlRoleDefinitions/${roles.CosmosDBDataContributor}'
    principalId: principalId
    scope: cosmosAccountId
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

output roleAssignmentsCreated int = empty(contentUnderstandingId) ? 7 : 8
