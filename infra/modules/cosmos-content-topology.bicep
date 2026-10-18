@description('Existing Cosmos DB account name')
param cosmosAccountName string

@description('Existing database name')
param databaseName string = 'rag-db'

@description('Cosmos DB capacity mode')
@allowed(['serverless', 'provisioned'])
param mode string = 'serverless'

@description('Dedicated autoscale maximum RU/s for source-specific chunk containers')
@minValue(1000)
param searchChunksAutoscaleMaxThroughput int = 1000

var contentContainerPairs = loadJsonContent('../content-container-pairs.json')

resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2026-03-15' existing = {
  name: cosmosAccountName
}

resource database 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2026-03-15' existing = {
  parent: cosmosAccount
  name: databaseName
}

resource manifestsContainers 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2026-03-15' = [
  for pair in contentContainerPairs: {
    parent: database
    name: pair.manifestsContainer
    properties: {
      resource: {
        id: pair.manifestsContainer
        partitionKey: {
          paths: ['/sourceRunId']
          kind: 'Hash'
          version: 2
        }
        indexingPolicy: {
          indexingMode: 'consistent'
          automatic: true
          includedPaths: [
            { path: '/sourceId/?' }
            { path: '/driveId/?' }
            { path: '/recordType/?' }
            { path: '/runId/?' }
            { path: '/status/?' }
            { path: '/stage/?' }
            { path: '/documentId/?' }
            { path: '/discoveryOrdinal/?' }
            { path: '/itemId/?' }
            { path: '/discoveredAt/?' }
            { path: '/processingStartedAt/?' }
            { path: '/readyAt/?' }
            { path: '/failedAt/?' }
            { path: '/updatedAt/?' }
            { path: '/sourceModifiedAt/?' }
            { path: '/lifecycleGeneration/?' }
          ]
          excludedPaths: [
            { path: '/*' }
          ]
          compositeIndexes: [
            [
              {
                path: '/status'
                order: 'ascending'
              }
              {
                path: '/discoveryOrdinal'
                order: 'ascending'
              }
            ]
          ]
        }
      }
      options: {}
    }
  }
]

resource chunksContainers 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2026-03-15' = [
  for pair in contentContainerPairs: {
    parent: database
    name: pair.chunksContainer
    properties: {
      resource: {
        id: pair.chunksContainer
        partitionKey: {
          paths: ['/documentKey']
          kind: 'Hash'
          version: 2
        }
        indexingPolicy: {
          indexingMode: 'consistent'
          automatic: true
          includedPaths: [
            { path: '/sourceId/?' }
            { path: '/runId/?' }
            { path: '/sourceRunId/?' }
            { path: '/documentId/?' }
            { path: '/documentKey/?' }
            { path: '/allowedGroupIds/[]/?' }
            { path: '/pageStart/?' }
            { path: '/pageEnd/?' }
            { path: '/chunkIndex/?' }
            { path: '/createdAt/?' }
            { path: '/sourceModifiedAt/?' }
            { path: '/isRetrievable/?' }
            { path: '/lifecycleGeneration/?' }
          ]
          excludedPaths: [
            { path: '/*' }
            { path: '/embedding/*' }
          ]
          #disable-next-line BCP037
          fullTextIndexes: [
            { path: '/content' }
            { path: '/searchableText' }
          ]
          vectorIndexes: [
            {
              path: '/embedding'
              type: 'diskANN'
            }
          ]
        }
        fullTextPolicy: {
          defaultLanguage: 'en-US'
          fullTextPaths: [
            {
              path: '/content'
              language: 'en-US'
            }
            {
              path: '/searchableText'
              language: 'en-US'
            }
          ]
        }
        vectorEmbeddingPolicy: {
          vectorEmbeddings: [
            {
              path: '/embedding'
              dataType: 'float32'
              dimensions: 3072
              distanceFunction: 'cosine'
            }
          ]
        }
      }
      options: mode == 'serverless'
        ? {}
        : {
            autoscaleSettings: {
              maxThroughput: searchChunksAutoscaleMaxThroughput
            }
          }
    }
  }
]

@description('Approved source-specific content container pairs')
output contentContainerPairs array = contentContainerPairs
