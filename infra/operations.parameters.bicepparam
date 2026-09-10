using './modules/aca-operations-job.bicep'

param jobName = readEnvironmentVariable('OPERATIONS_JOB_NAME')
param location = readEnvironmentVariable('AZURE_LOCATION')
param managedEnvironmentId = readEnvironmentVariable('MANAGED_ENVIRONMENT_ID')
param acrLoginServer = readEnvironmentVariable('ACR_LOGIN_SERVER')
param imageReference = readEnvironmentVariable('RETRIEVAL_IMAGE_REFERENCE')
param managedIdentityId = readEnvironmentVariable('OPERATIONS_MANAGED_IDENTITY_ID')
param managedIdentityClientId = readEnvironmentVariable('OPERATIONS_MANAGED_IDENTITY_CLIENT_ID')
param cosmosEndpoint = readEnvironmentVariable('COSMOS_ENDPOINT')
param cosmosDatabaseName = 'rag-db'
param retrievalConfigContainerName = 'retrieval-config'
param deploymentInstanceId = readEnvironmentVariable('DEPLOYMENT_INSTANCE_ID')
param catalogDigest = readEnvironmentVariable('RETRIEVAL_CATALOG_DIGEST', '')
param catalogOperation = readEnvironmentVariable('CATALOG_OPERATION', 'verify-catalog')
param tags = {
  DeploymentInstance: readEnvironmentVariable('DEPLOYMENT_INSTANCE_ID')
  Project: 'RAG-SharePoint'
  ManagedBy: 'azd'
  CostCenter: readEnvironmentVariable('COST_CENTER')
  CleanupDate: readEnvironmentVariable('CLEANUP_DATE')
  Temporary: 'true'
}
