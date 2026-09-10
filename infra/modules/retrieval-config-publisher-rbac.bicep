@description('Deployment publisher principal ID; empty disables assignment')
param publisherPrincipalId string = ''

@description('Human catalog editor principal; no create, upsert or delete permission')
param editorPrincipalId string = ''

@description('Optional guarded writer principal; separate from bootstrap and human editor')
param writerPrincipalId string = ''

@description('Cosmos account resource ID')
param cosmosAccountId string

@description('Cosmos database name')
param cosmosDatabaseName string

@description('Retrieval configuration container name')
param retrievalConfigContainerName string

var accountName = last(split(cosmosAccountId, '/'))
var containerScope = '${cosmosAccountId}/dbs/${cosmosDatabaseName}/colls/${retrievalConfigContainerName}'
var readActions = [
  'Microsoft.DocumentDB/databaseAccounts/readMetadata'
  'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/read'
]
var editActions = concat(readActions, [
  'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/replace'
  'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/executeQuery'
  'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/readChangeFeed'
])

resource bootstrapRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions@2024-05-15' = {
  name: '${accountName}/${guid(cosmosAccountId, 'catalog-bootstrap-create-read')}'
  properties: {
    roleName: 'Catalog bootstrap create and read'
    type: 'CustomRole'
    assignableScopes: [cosmosAccountId]
    permissions: [
      {
        dataActions: concat(readActions, [
          'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/create'
        ])
      }
    ]
  }
}

resource editorRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions@2024-05-15' = {
  name: '${accountName}/${guid(cosmosAccountId, 'catalog-editor-read-replace')}'
  properties: {
    roleName: 'Catalog editor read and replace'
    type: 'CustomRole'
    assignableScopes: [cosmosAccountId]
    permissions: [{ dataActions: editActions }]
  }
}

resource writerRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions@2024-05-15' = {
  name: '${accountName}/${guid(cosmosAccountId, 'catalog-guarded-writer')}'
  properties: {
    roleName: 'Catalog guarded writer'
    type: 'CustomRole'
    assignableScopes: [cosmosAccountId]
    permissions: [
      {
        dataActions: concat(editActions, [
          'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/create'
        ])
      }
    ]
  }
}

resource publisherWriter 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = if (!empty(publisherPrincipalId)) {
  name: '${accountName}/${guid(publisherPrincipalId, cosmosAccountId, retrievalConfigContainerName, 'create-read')}'
  properties: {
    roleDefinitionId: bootstrapRole.id
    principalId: publisherPrincipalId
    scope: containerScope
  }
}

resource editor 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = if (!empty(editorPrincipalId)) {
  name: '${accountName}/${guid(editorPrincipalId, cosmosAccountId, retrievalConfigContainerName, 'editor')}'
  properties: {
    roleDefinitionId: editorRole.id
    principalId: editorPrincipalId
    scope: containerScope
  }
}

resource writer 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = if (!empty(writerPrincipalId)) {
  name: '${accountName}/${guid(writerPrincipalId, cosmosAccountId, retrievalConfigContainerName, 'writer')}'
  properties: {
    roleDefinitionId: writerRole.id
    principalId: writerPrincipalId
    scope: containerScope
  }
}
