@description('Private catalog publication job name')
@minLength(2)
@maxLength(31)
param jobName string

@description('Azure region')
param location string = resourceGroup().location

@description('Existing Container Apps managed environment resource ID')
param managedEnvironmentId string

@description('ACR login server')
param acrLoginServer string

@description('Immutable operations image reference')
@minLength(1)
param imageReference string

@description('Operations runner managed identity resource ID')
param managedIdentityId string

@description('Operations runner managed identity client ID')
param managedIdentityClientId string

@description('Cosmos DB endpoint')
param cosmosEndpoint string

@description('Cosmos database name')
param cosmosDatabaseName string

@description('Retrieval configuration container name')
param retrievalConfigContainerName string

@description('Deployment instance partition key')
param deploymentInstanceId string

@description('Reviewed seed digest; used only for explicit initialization')
@maxLength(71)
param catalogDigest string = ''

@description('Private catalog operation')
@allowed(['publish-catalog', 'verify-catalog'])
param catalogOperation string = 'verify-catalog'

@description('Resource tags')
param tags object = {}

resource job 'Microsoft.App/jobs@2024-03-01' = {
  name: jobName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentityId}': {}
    }
  }
  properties: {
    environmentId: managedEnvironmentId
    workloadProfileName: 'Consumption'
    configuration: {
      triggerType: 'Manual'
      replicaTimeout: 600
      replicaRetryLimit: 1
      manualTriggerConfig: {
        parallelism: 1
        replicaCompletionCount: 1
      }
      registries: [
        {
          server: acrLoginServer
          identity: managedIdentityId
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'catalog-publisher'
          image: imageReference
          command: ['python']
          args: ['-m', 'retrieval.operations', catalogOperation]
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
          env: [
            { name: 'COSMOS_ENDPOINT', value: cosmosEndpoint }
            { name: 'COSMOS_DATABASE', value: cosmosDatabaseName }
            { name: 'RETRIEVAL_CONFIG_CONTAINER', value: retrievalConfigContainerName }
            { name: 'DEPLOYMENT_INSTANCE_ID', value: deploymentInstanceId }
            { name: 'EXPECTED_CATALOG_DIGEST', value: catalogOperation == 'publish-catalog' ? catalogDigest : '' }
            { name: 'MANAGED_IDENTITY_CLIENT_ID', value: managedIdentityClientId }
          ]
        }
      ]
    }
  }
}

output jobName string = job.name
output jobId string = job.id
