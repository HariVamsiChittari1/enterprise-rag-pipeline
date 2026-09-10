// =========================================
// Azure AI Content Understanding Module
// =========================================
// Creates a private Foundry resource. Pinned generative model deployments remain
// available for rollback to prebuilt-documentSearch; prebuilt-layout does
// not require them.

@description('Microsoft Foundry resource name')
param accountName string

@description('Azure region')
param location string = resourceGroup().location

@description('Resource tags')
param tags object = {}

@description('Temporary public IPv4 address allowed during guarded administrative setup; empty keeps the account private-only')
param allowedIpAddress string = ''

var completionDeploymentName = 'cu-gpt-5-2'
var embeddingDeploymentName = 'cu-text-embedding-3-large'

// =========================================
// Resources
// =========================================

resource account 'Microsoft.CognitiveServices/accounts@2025-06-01' = {
  name: accountName
  location: location
  kind: 'AIServices'
  sku: {
    name: 'S0'
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    customSubDomainName: accountName
    networkAcls: {
      defaultAction: 'Deny'
      virtualNetworkRules: []
      ipRules: empty(allowedIpAddress)
        ? []
        : [
            {
              value: allowedIpAddress
            }
          ]
    }
    publicNetworkAccess: empty(allowedIpAddress) ? 'Disabled' : 'Enabled'
    disableLocalAuth: true
  }
  tags: tags
}

resource completionDeployment 'Microsoft.CognitiveServices/accounts/deployments@2025-06-01' = {
  parent: account
  name: completionDeploymentName
  sku: {
    name: 'GlobalStandard'
    capacity: 30
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: 'gpt-5.2'
      version: '2025-12-11'
    }
    versionUpgradeOption: 'NoAutoUpgrade'
  }
}

resource embeddingDeployment 'Microsoft.CognitiveServices/accounts/deployments@2025-06-01' = {
  parent: account
  name: embeddingDeploymentName
  dependsOn: [
    completionDeployment
  ]
  sku: {
    name: 'GlobalStandard'
    capacity: 120
  }
  properties: {
    model: {
      format: 'OpenAI'
      name: 'text-embedding-3-large'
      version: '1'
    }
    versionUpgradeOption: 'NoAutoUpgrade'
  }
}

// =========================================
// Outputs
// =========================================

@description('Microsoft Foundry resource ID')
output accountId string = account.id

@description('Content Understanding endpoint')
output endpoint string = 'https://${accountName}.services.ai.azure.com/'

@description('Content Understanding completion model deployment name')
output completionDeploymentName string = completionDeployment.name

@description('Content Understanding embedding model deployment name')
output embeddingDeploymentName string = embeddingDeployment.name
