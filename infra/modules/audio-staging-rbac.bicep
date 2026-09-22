// =========================================
// Audio Staging Storage RBAC Module
// =========================================
// Grants least-privilege data-plane access to the dedicated audio-staging account:
// - Function managed identity: Storage Blob Data Contributor (upload + delete source audio).
// - Speech system-assigned identity: Storage Blob Data Reader (batch transcription reads source).

@description('Function App managed identity principal ID')
@minLength(1)
param functionPrincipalId string

@description('Speech resource system-assigned identity principal ID')
@minLength(1)
param speechPrincipalId string

@description('Audio staging storage account resource ID')
@minLength(1)
param storageAccountId string

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: split(storageAccountId, '/')[8]
}

var roles = {
  StorageBlobDataContributor: 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
  StorageBlobDataReader: '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'
}

resource functionBlobContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, functionPrincipalId, roles.StorageBlobDataContributor)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      roles.StorageBlobDataContributor
    )
    principalId: functionPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource speechBlobReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, speechPrincipalId, roles.StorageBlobDataReader)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roles.StorageBlobDataReader)
    principalId: speechPrincipalId
    principalType: 'ServicePrincipal'
  }
}

output roleAssignmentsCreated int = 2
