using './modules/content-understanding.bicep'

param accountName = readEnvironmentVariable('CONTENT_UNDERSTANDING_ACCOUNT_NAME')
param location = readEnvironmentVariable('AZURE_LOCATION')
param allowedIpAddress = readEnvironmentVariable('CONTENT_UNDERSTANDING_ALLOWED_IP_ADDRESS', '')
param tags = {
  DeploymentInstance: readEnvironmentVariable('DEPLOYMENT_INSTANCE_ID')
  Project: 'RAG-SharePoint'
  ManagedBy: 'azd'
  CostCenter: readEnvironmentVariable('COST_CENTER')
  CleanupDate: readEnvironmentVariable('CLEANUP_DATE')
}
