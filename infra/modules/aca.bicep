@description('Container App name')
@minLength(2)
@maxLength(32)
param containerAppName string

@description('Azure region')
param location string = resourceGroup().location

@description('ACR login server (e.g. myacr.azurecr.io)')
param acrLoginServer string

@description('Immutable container image name/tag or fully qualified registry reference')
@minLength(1)
param imageName string

@description('User-assigned managed identity resource ID for the container app')
param managedIdentityId string

@description('Existing Container Apps managed environment resource ID')
param managedEnvironmentId string

@description('Retrieval service env vars (from retrieval-config module)')
param retrievalEnvVars array

@description('Retrieval API application/client ID')
param retrievalApiClientId string

@description('Retrieval API client ID used as the v2 access-token audience')
@minLength(1)
param retrievalApiAudience string

@description('Single-tenant Entra issuer URL')
@minLength(1)
param entraIssuer string

@description('Function UAMI application/client ID allowed by ACA auth')
param gatewayClientId string

@description('Function UAMI service-principal object ID allowed by ACA auth')
param gatewayPrincipalId string

@description('Container CPU cores')
param containerCpu string = '0.5'

@description('Container memory')
param containerMemory string = '1Gi'

@description('Minimum replicas')
@minValue(1)
param minReplicas int = 1

@description('Maximum replicas')
@minValue(1)
param maxReplicas int = 5

@description('Resource tags')
param tags object = {}

@description('Observer principal authorized to read revision and replica inventories')
param observerPrincipalId string = ''

resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: containerAppName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentityId}': {}
    }
  }
  properties: {
    managedEnvironmentId: managedEnvironmentId
    configuration: {
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
      }
      registries: [
        {
          server: acrLoginServer
          identity: managedIdentityId
        }
      ]
      activeRevisionsMode: 'Single'
    }
    template: {
      containers: [
        {
          name: 'retrieval-agent'
          image: contains(imageName, '.azurecr.io/') ? imageName : '${acrLoginServer}/${imageName}'
          resources: {
            cpu: json(containerCpu)
            memory: containerMemory
          }
          env: retrievalEnvVars
          probes: [
            {
              type: 'Liveness'
              httpGet: { path: '/health/live', port: 8080 }
              initialDelaySeconds: 5
              periodSeconds: 10
            }
            {
              type: 'Readiness'
              httpGet: { path: '/health/ready', port: 8080 }
              initialDelaySeconds: 10
              periodSeconds: 15
            }
          ]
        }
      ]
      scale: {
        minReplicas: minReplicas
        maxReplicas: maxReplicas
      }
    }
  }
}

resource observerReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(observerPrincipalId)) {
  name: guid(containerApp.id, observerPrincipalId, 'acdd72a7-3385-48ef-bd42-f606fba81ae7')
  scope: containerApp
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      'acdd72a7-3385-48ef-bd42-f606fba81ae7'
    )
    principalId: observerPrincipalId
  }
}

resource authConfig 'Microsoft.App/containerApps/authConfigs@2024-03-01' = {
  parent: containerApp
  name: 'current'
  properties: {
    platform: {
      enabled: true
    }
    globalValidation: {
      unauthenticatedClientAction: 'Return401'
    }
    identityProviders: {
      azureActiveDirectory: {
        enabled: true
        registration: {
          clientId: retrievalApiClientId
          openIdIssuer: entraIssuer
        }
        validation: {
          allowedAudiences: [retrievalApiAudience]
          defaultAuthorizationPolicy: {
            allowedApplications: [gatewayClientId]
            allowedPrincipals: {
              identities: [gatewayPrincipalId]
            }
          }
        }
      }
    }
    httpSettings: {
      requireHttps: true
    }
  }
}

@description('Container App FQDN (internal)')
output fqdn string = containerApp.properties.configuration.ingress.fqdn

@description('Container App resource name')
output containerAppName string = containerApp.name

@description('Container App internal URL')
output internalUrl string = 'https://${containerApp.properties.configuration.ingress.fqdn}'
