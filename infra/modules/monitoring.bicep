// =========================================
// Monitoring Module
// =========================================
// Creates Log Analytics Workspace and Application Insights for telemetry

@description('Log Analytics workspace name')
param logAnalyticsName string

@description('Application Insights instance name')
param applicationInsightsName string

@description('Azure region')
param location string = resourceGroup().location

@description('Resource tags')
param tags object = {}

@description('Data retention period in days')
@minValue(30)
@maxValue(730)
param retentionInDays int = 90

@description('Daily ingestion cap in GB (-1 for unlimited)')
param dailyQuotaGb int = -1

@description('Retrieval identity authorized to publish telemetry with Entra authentication')
param retrievalPrincipalId string = ''

@description('Observer principal authorized to query the existing workspace')
param observerPrincipalId string = ''

// =========================================
// Resources
// =========================================

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: retentionInDays
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
  tags: tags
}

resource applicationInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: applicationInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
    IngestionMode: 'LogAnalytics'
    DisableLocalAuth: true
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
  tags: tags
}

resource retrievalPublisher 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(retrievalPrincipalId)) {
  name: guid(applicationInsights.id, retrievalPrincipalId, '3913510d-42f4-4e42-8a64-420c390055eb')
  scope: applicationInsights
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      '3913510d-42f4-4e42-8a64-420c390055eb'
    )
    principalId: retrievalPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource observerReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(observerPrincipalId)) {
  name: guid(logAnalytics.id, observerPrincipalId, '73c42c96-874c-492b-b04d-ab87d138a893')
  scope: logAnalytics
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      '73c42c96-874c-492b-b04d-ab87d138a893'
    )
    principalId: observerPrincipalId
  }
}

// Daily cap configuration (if specified)
#disable-next-line BCP081
resource dailyCap 'Microsoft.Insights/components/currentbillingfeatures@2015-05-01' = if (dailyQuotaGb > 0) {
  parent: applicationInsights
  name: 'currentbillingfeatures'
  properties: {
    CurrentBillingFeatures: 'Basic'
    DataVolumeCap: {
      Cap: dailyQuotaGb
      WarningThreshold: dailyQuotaGb > 1 ? (dailyQuotaGb - 1) : 0
    }
  }
}

// =========================================
// Outputs
// =========================================

@description('Log Analytics workspace ID')
output logAnalyticsId string = logAnalytics.id

@description('Log Analytics workspace Customer ID (for queries)')
output logAnalyticsWorkspaceId string = logAnalytics.properties.customerId

@description('Application Insights ID')
output applicationInsightsId string = applicationInsights.id

@description('Application Insights connection string')
output connectionString string = applicationInsights.properties.ConnectionString

@description('Application Insights app ID')
output appInsightsAppId string = applicationInsights.properties.AppId
