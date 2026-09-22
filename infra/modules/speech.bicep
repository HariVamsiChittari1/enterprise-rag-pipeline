// =========================================
// Azure AI Speech Module
// =========================================
// Dedicated Speech resource for audio fast transcription. Keyless (managed identity
// only); a custom subdomain is required for Microsoft Entra token authentication.
// Private by default; a temporary public IP may be allowed during a guarded spike.

@description('Speech resource name')
@minLength(2)
@maxLength(64)
param accountName string

@description('Azure region')
param location string = resourceGroup().location

@description('Resource tags')
param tags object = {}

@description('Temporary public IPv4 address allowed during a guarded transcription spike; empty keeps the account private-only')
param allowedIpAddress string = ''

resource account 'Microsoft.CognitiveServices/accounts@2025-06-01' = {
  name: accountName
  location: location
  kind: 'SpeechServices'
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

@description('Speech resource ID')
output accountId string = account.id

@description('Speech system-assigned managed identity principal ID (for storage resource-instance access)')
output principalId string = account.identity.principalId

@description('Speech fast-transcription custom-domain endpoint')
output endpoint string = 'https://${accountName}.cognitiveservices.azure.com/'
