@description('Existing Cosmos DB account name')
param cosmosAccountName string

@description('Existing Cosmos DB database name')
param cosmosDatabaseName string

@description('Function App managed identity principal ID')
param functionPrincipalId string

@description('Retrieval managed identity principal ID')
param retrievalPrincipalId string

var dataReaderRoleId = '00000000-0000-0000-0000-000000000001'
var dataContributorRoleId = '00000000-0000-0000-0000-000000000002'
var contentContainerPairs = loadJsonContent('../content-container-pairs.json')
var contentContainerNamesByPair = [
  for pair in contentContainerPairs: [
    pair.chunksContainer
    pair.manifestsContainer
  ]
]
var contentContainerNames = flatten(contentContainerNamesByPair)

resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' existing = {
  name: cosmosAccountName
}

resource functionContentContributors 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = [
  for containerName in contentContainerNames: {
    parent: cosmosAccount
    name: guid(cosmosAccount.id, functionPrincipalId, 'content-contributor', containerName)
    properties: {
      roleDefinitionId: '${cosmosAccount.id}/sqlRoleDefinitions/${dataContributorRoleId}'
      principalId: functionPrincipalId
      scope: '${cosmosAccount.id}/dbs/${cosmosDatabaseName}/colls/${containerName}'
    }
  }
]

resource retrievalContentReaders 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = [
  for containerName in contentContainerNames: {
    parent: cosmosAccount
    name: guid(cosmosAccount.id, retrievalPrincipalId, 'content-reader', containerName)
    properties: {
      roleDefinitionId: '${cosmosAccount.id}/sqlRoleDefinitions/${dataReaderRoleId}'
      principalId: retrievalPrincipalId
      scope: '${cosmosAccount.id}/dbs/${cosmosDatabaseName}/colls/${containerName}'
    }
  }
]

output contentContainerNames array = contentContainerNames
