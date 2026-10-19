@description('Container Apps managed environment name')
param environmentName string

@description('Azure region')
param location string = resourceGroup().location

@description('VNet infrastructure subnet resource ID')
param infrastructureSubnetId string

@description('Log Analytics workspace resource ID')
param logAnalyticsWorkspaceId string

@description('Enable zone redundancy at environment creation time')
param zoneRedundant bool = false

@description('Resource tags')
param tags object = {}

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: environmentName
  location: location
  tags: tags
  properties: {
    vnetConfiguration: {
      infrastructureSubnetId: infrastructureSubnetId
      internal: false
    }
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: reference(logAnalyticsWorkspaceId, '2023-09-01').customerId
        sharedKey: listKeys(logAnalyticsWorkspaceId, '2023-09-01').primarySharedKey
      }
    }
    zoneRedundant: zoneRedundant
  }
}

output environmentId string = environment.id
output defaultDomain string = environment.properties.defaultDomain
output staticIp string = environment.properties.staticIp
