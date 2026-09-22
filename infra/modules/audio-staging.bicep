// =========================================
// Audio Staging Storage Module (batch transcription source)
// =========================================
// Dedicated, single-purpose account that holds only transient source audio for Azure Speech
// batch transcription. Its public endpoint is Enabled but denies everything except the Speech
// resource instance (Trusted Azure services / managed identity), because storage firewall rules
// govern the public endpoint and Speech reads over it via its system-assigned MI. The VNet
// Function writes/deletes via a private endpoint (PE traffic bypasses the firewall). The shared
// Functions/Durable account stays fully private (publicNetworkAccess Disabled).

@description('Audio staging storage account name (3-24 lowercase alphanumeric)')
@minLength(3)
@maxLength(24)
param storageAccountName string

@description('Azure region')
param location string = resourceGroup().location

@description('Storage redundancy: LRS, ZRS, GRS')
@allowed(['LRS', 'ZRS', 'GRS'])
param redundancy string = 'ZRS'

@description('Speech resource ID granted resource-instance access to the account public endpoint')
@minLength(1)
param speechAccountId string

@description('Blob container holding transient source audio')
param containerName string = 'audio-staging'

@description('Resource tags')
param tags object = {}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_${redundancy}'
  }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    supportsHttpsTrafficOnly: true
    encryption: {
      services: {
        blob: { enabled: true }
        file: { enabled: true }
        table: { enabled: true }
        queue: { enabled: true }
      }
      keySource: 'Microsoft.Storage'
    }
    // Public endpoint stays on so the Speech resource instance can reach it; everything except
    // that instance is denied. The Function reaches it privately via a blob private endpoint.
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
      virtualNetworkRules: []
      ipRules: []
      resourceAccessRules: [
        {
          tenantId: subscription().tenantId
          resourceId: speechAccountId
        }
      ]
    }
    publicNetworkAccess: 'Enabled'
  }
  tags: tags
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 7
    }
  }
}

resource stagingContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: containerName
  properties: {
    publicAccess: 'None'
  }
}

@description('Resource ID of the audio staging storage account')
output storageAccountId string = storageAccount.id

@description('Audio staging storage account name')
output storageAccountName string = storageAccount.name

@description('Blob service endpoint')
output blobEndpoint string = storageAccount.properties.primaryEndpoints.blob

@description('Source-audio container name')
output containerName string = stagingContainer.name
