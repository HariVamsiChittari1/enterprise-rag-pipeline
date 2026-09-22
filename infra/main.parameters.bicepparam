using './main.bicep'

param deploymentInstanceId = readEnvironmentVariable('DEPLOYMENT_INSTANCE_ID')
param location = readEnvironmentVariable('AZURE_LOCATION')
param openAiAccountName = readEnvironmentVariable('AZURE_OPENAI_ACCOUNT_NAME')
param openAiResourceGroupName = readEnvironmentVariable('AZURE_OPENAI_RESOURCE_GROUP')
param embeddingDeploymentName = readEnvironmentVariable('OPENAI_EMBEDDING_DEPLOYMENT_NAME', 'text-embedding-3-large')
param chatDeploymentName = readEnvironmentVariable('OPENAI_CHAT_DEPLOYMENT_NAME')
param documentIntelligenceEnabled = readEnvironmentVariable('DOCUMENT_INTELLIGENCE_ENABLED', 'true') == 'true'
param contentUnderstandingEnabled = readEnvironmentVariable('CONTENT_UNDERSTANDING_ENABLED', 'false') == 'true'
param aclEnabled = readEnvironmentVariable('ACL_ENABLED', 'true') == 'true'
param includeCitations = readEnvironmentVariable('INCLUDE_CITATIONS', 'true') == 'true'
param contentUnderstandingAnalyzerId = readEnvironmentVariable(
  'CONTENT_UNDERSTANDING_ANALYZER_ID',
  'prebuilt-documentSearch'
)
param contentUnderstandingAllowedIpAddress = readEnvironmentVariable('CONTENT_UNDERSTANDING_ALLOWED_IP_ADDRESS', '')
param audioWriterEnabled = readEnvironmentVariable('AUDIO_WRITER_ENABLED', 'false') == 'true'
param audioRetrievalEnabled = readEnvironmentVariable('AUDIO_RETRIEVAL_ENABLED', 'false') == 'true'
param audioLocale = readEnvironmentVariable('AUDIO_LOCALE', 'en-US')
param speechAllowedIpAddress = readEnvironmentVariable('SPEECH_ALLOWED_IP_ADDRESS', '')
param sharePointTenantId = readEnvironmentVariable('SHAREPOINT_TENANT_ID')
param sharePointAppClientId = readEnvironmentVariable('SHAREPOINT_APP_CLIENT_ID')
param sharePointDriveId = readEnvironmentVariable('SHAREPOINT_ASSIGNED_DRIVE_ID')
param sharePointSiteUrl = readEnvironmentVariable('SHAREPOINT_SITE_URL')
param ingestionSourceId = readEnvironmentVariable('INGESTION_SOURCE_ID')
param sharePointCertificateSecretName = readEnvironmentVariable(
  'SHAREPOINT_CERTIFICATE_SECRET_NAME',
  'sharepoint-app-cert'
)
param sharePointKeyVaultName = readEnvironmentVariable('SHAREPOINT_KEY_VAULT_NAME')
param sharePointKeyVaultResourceGroupName = readEnvironmentVariable('SHAREPOINT_KEY_VAULT_RESOURCE_GROUP')
param adminApiClientId = readEnvironmentVariable('ADMIN_API_CLIENT_ID')
param functionApiAudience = readEnvironmentVariable('FUNCTION_API_AUDIENCE')
param retrievalApiClientId = readEnvironmentVariable('RETRIEVAL_API_CLIENT_ID')
param retrievalApiAudience = readEnvironmentVariable('RETRIEVAL_API_AUDIENCE')
param retrievalApiServicePrincipalId = readEnvironmentVariable('RETRIEVAL_API_SERVICE_PRINCIPAL_ID')
param allowedApplicationClientIds = [
  readEnvironmentVariable('FUNCTION_ALLOWED_CALLER_CLIENT_ID')
]
param webhookClientState = readEnvironmentVariable('WEBHOOK_CLIENT_STATE')
param cosmosDbMode = readEnvironmentVariable('COSMOS_DB_MODE', 'serverless')
param cosmosMetadataAutoscaleMaxRUs = int(readEnvironmentVariable('COSMOS_METADATA_AUTOSCALE_MAX_RUS', '1000'))
param cosmosSearchChunksAutoscaleMaxRUs = int(readEnvironmentVariable('COSMOS_SEARCH_AUTOSCALE_MAX_RUS', '1000'))
param storageRedundancy = readEnvironmentVariable('STORAGE_REDUNDANCY', 'ZRS')
param useDocumentIntelligenceFreeTier = false
param useLanguageFreeTier = false
param applicationInsightsDailyCapGb = int(readEnvironmentVariable('APPLICATION_INSIGHTS_DAILY_CAP_GB', '5'))
param deployServing = readEnvironmentVariable('DEPLOY_SERVING', 'false') == 'true'
param deployOperations = readEnvironmentVariable('DEPLOY_OPERATIONS', 'false') == 'true'
param retrievalImageReference = readEnvironmentVariable('RETRIEVAL_IMAGE_REFERENCE', '')
param retrievalCatalogDigest = readEnvironmentVariable('RETRIEVAL_CATALOG_DIGEST', '')
param catalogEditorPrincipalId = readEnvironmentVariable('CATALOG_EDITOR_PRINCIPAL_ID', '')
param catalogWriterPrincipalId = readEnvironmentVariable('CATALOG_WRITER_PRINCIPAL_ID', '')
param catalogObserverPrincipalId = readEnvironmentVariable('CATALOG_OBSERVER_PRINCIPAL_ID', '')
param catalogOperation = readEnvironmentVariable('CATALOG_OPERATION', 'verify-catalog')
param retrievalCatalogPollSeconds = int(readEnvironmentVariable('RETRIEVAL_CATALOG_POLL_SECONDS', '7200'))
param retrievalMinReplicas = int(readEnvironmentVariable('RETRIEVAL_MIN_REPLICAS', '1'))
param retrievalMaxReplicas = int(readEnvironmentVariable('RETRIEVAL_MAX_REPLICAS', '5'))
param retrievalZoneRedundant = readEnvironmentVariable('RETRIEVAL_ZONE_REDUNDANT', 'false') == 'true'
param functionIntegrationSubnetNsgId = readEnvironmentVariable('SUBNET_FUNCTION_INTEGRATION_NSG_ID', '')
param privateEndpointSubnetNsgId = readEnvironmentVariable('SUBNET_PRIVATE_ENDPOINTS_NSG_ID', '')
param acaEnvironmentSubnetNsgId = readEnvironmentVariable('SUBNET_ACA_ENVIRONMENT_NSG_ID', '')
param tags = {
  DeploymentInstance: readEnvironmentVariable('DEPLOYMENT_INSTANCE_ID')
  Project: 'RAG-SharePoint'
  ManagedBy: 'azd'
  CostCenter: readEnvironmentVariable('COST_CENTER')
  CleanupDate: readEnvironmentVariable('CLEANUP_DATE')
}
