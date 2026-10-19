using './modules/external-aca-environment.bicep'

param environmentName = readEnvironmentVariable('ACA_ENVIRONMENT_NAME')
param location = readEnvironmentVariable('AZURE_LOCATION')
param infrastructureSubnetId = readEnvironmentVariable('ACA_ENVIRONMENT_SUBNET_ID')
param logAnalyticsWorkspaceId = readEnvironmentVariable('LOG_ANALYTICS_WORKSPACE_ID')
param zoneRedundant = readEnvironmentVariable('RETRIEVAL_ZONE_REDUNDANT', 'false') == 'true'
param tags = {
  DeploymentInstance: readEnvironmentVariable('DEPLOYMENT_INSTANCE_ID')
  Project: 'RAG-SharePoint'
  ManagedBy: 'azd'
  CostCenter: readEnvironmentVariable('COST_CENTER')
  CleanupDate: readEnvironmentVariable('CLEANUP_DATE')
}
