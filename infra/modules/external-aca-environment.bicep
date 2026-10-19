@description('Reviewed ACA environment resource name')
param environmentName string

@description('Azure region')
param location string

@description('Existing dedicated ACA infrastructure subnet resource ID')
param infrastructureSubnetId string

@description('Existing Log Analytics workspace resource ID')
param logAnalyticsWorkspaceId string

@description('Enable zone redundancy at environment creation time')
param zoneRedundant bool = false

@description('Resource tags')
param tags object

module environment './aca-environment.bicep' = {
  name: 'external-aca-environment-resource'
  params: {
    environmentName: environmentName
    location: location
    infrastructureSubnetId: infrastructureSubnetId
    logAnalyticsWorkspaceId: logAnalyticsWorkspaceId
    zoneRedundant: zoneRedundant
    tags: tags
  }
}

output environmentId string = environment.outputs.environmentId
output environmentName string = last(split(environment.outputs.environmentId, '/'))
