@description('Virtual network name')
param virtualNetworkName string

@description('Azure region')
param location string = resourceGroup().location

@description('Private Link targets and DNS zones')
param privateEndpointTargets array

@description('Existing Function integration subnet network security group resource ID')
param functionIntegrationSubnetNsgId string = ''

@description('Existing private endpoint subnet network security group resource ID')
param privateEndpointSubnetNsgId string = ''

@description('Existing ACA environment subnet network security group resource ID')
param acaEnvironmentSubnetNsgId string = ''

@description('Resource tags')
param tags object = {}

var integrationSubnetName = 'function-integration'
var privateEndpointSubnetName = 'private-endpoints'

// Deduplicate DNS zone names — multiple PE targets can share one zone (e.g. cognitiveservices)
var uniqueDnsZoneNames = union(map(privateEndpointTargets, target => target.dnsZoneName), [])

module virtualNetwork 'br/public:avm/res/network/virtual-network:0.10.0' = {
  params: {
    name: virtualNetworkName
    location: location
    addressPrefixes: [
      '10.20.0.0/22'
    ]
    subnets: [
      {
        name: integrationSubnetName
        addressPrefix: '10.20.0.0/27'
        delegation: 'Microsoft.App/environments'
        networkSecurityGroupResourceId: functionIntegrationSubnetNsgId
        privateEndpointNetworkPolicies: 'Disabled'
      }
      {
        name: privateEndpointSubnetName
        addressPrefix: '10.20.0.32/27'
        networkSecurityGroupResourceId: privateEndpointSubnetNsgId
        privateEndpointNetworkPolicies: 'Disabled'
      }
      {
        name: 'aca-environment'
        addressPrefix: '10.20.2.0/23'
        delegation: 'Microsoft.App/environments'
        networkSecurityGroupResourceId: acaEnvironmentSubnetNsgId
        privateEndpointNetworkPolicies: 'Disabled'
      }
    ]
    tags: tags
    enableTelemetry: false
  }
}

module privateDnsZones 'br/public:avm/res/network/private-dns-zone:0.8.0' = [
  for zoneName in uniqueDnsZoneNames: {
    name: 'dnsZone-${replace(zoneName, '.', '-')}'
    params: {
      name: zoneName
      virtualNetworkLinks: [
        {
          name: '${virtualNetworkName}-link'
          registrationEnabled: false
          virtualNetworkResourceId: virtualNetwork.outputs.resourceId
        }
      ]
      tags: tags
      enableTelemetry: false
    }
  }
]

module privateEndpoints 'br/public:avm/res/network/private-endpoint:0.12.0' = [
  for (target, index) in privateEndpointTargets: {
    name: 'pe-${target.name}'
    params: {
      name: '${virtualNetworkName}-${target.name}-pe'
      location: location
      subnetResourceId: resourceId(
        'Microsoft.Network/virtualNetworks/subnets',
        virtualNetworkName,
        privateEndpointSubnetName
      )
      privateLinkServiceConnections: [
        {
          name: '${target.name}-connection'
          properties: {
            groupIds: [
              target.groupId
            ]
            privateLinkServiceId: target.resourceId
          }
        }
      ]
      privateDnsZoneGroup: {
        name: 'default'
        privateDnsZoneGroupConfigs: [
          {
            name: target.name
            privateDnsZoneResourceId: privateDnsZones[indexOf(uniqueDnsZoneNames, target.dnsZoneName)].outputs.resourceId
          }
        ]
      }
      tags: tags
      enableTelemetry: false
    }
  }
]

output integrationSubnetId string = resourceId(
  'Microsoft.Network/virtualNetworks/subnets',
  virtualNetworkName,
  integrationSubnetName
)

output privateEndpointSubnetId string = resourceId(
  'Microsoft.Network/virtualNetworks/subnets',
  virtualNetworkName,
  privateEndpointSubnetName
)

output virtualNetworkId string = virtualNetwork.outputs.resourceId

output acaSubnetId string = resourceId(
  'Microsoft.Network/virtualNetworks/subnets',
  virtualNetworkName,
  'aca-environment'
)
