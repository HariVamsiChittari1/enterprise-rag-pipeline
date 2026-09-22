// =========================================
// Azure Functions Module
// =========================================
// Creates Azure Functions Flex Consumption plan + Function App for ingestion pipeline

@description('Function App name')
param functionAppName string

@description('Azure region')
param location string = resourceGroup().location

@description('Instance memory in MB: 512, 1024, 2048, 4096')
@allowed([512, 1024, 2048, 4096])
param instanceMemoryMB int = 2048

@description('Maximum Flex Consumption instance count')
@minValue(40)
@maxValue(1000)
param maximumInstanceCount int = 40

@description('Managed identity resource ID')
param managedIdentityId string

@description('Managed identity client ID')
param managedIdentityClientId string

@description('Storage account name for Functions host')
param storageAccountName string

@description('Blob container URL used for Flex Consumption package deployment')
param deploymentStorageContainerUrl string

@description('Resource ID of the Flex Consumption VNet integration subnet')
param integrationSubnetId string

@description('Application Insights connection string')
@secure()
param appInsightsConnectionString string

@description('Cosmos DB endpoint')
param cosmosEndpoint string

@description('Cosmos DB database name')
param cosmosDatabaseName string

@description('Cosmos DB ingestion container names')
param cosmosContainerNames object

@description('Durable Task Scheduler endpoint')
param durableTaskSchedulerEndpoint string

@description('Durable task hub name')
param durableTaskHubName string

@description('Azure OpenAI endpoint')
param openAiEndpoint string

@description('Azure OpenAI embedding deployment name')
param embeddingDeploymentName string

@description('Azure OpenAI chat deployment name')
param chatDeploymentName string

@description('Document Intelligence endpoint')
param documentIntelligenceEndpoint string

@description('Enable Document Intelligence extraction')
param documentIntelligenceEnabled bool

@description('Content Understanding endpoint')
param contentUnderstandingEndpoint string

@description('Stable copied Content Understanding analyzer ID')
param contentUnderstandingAnalyzerId string

@description('Enable Content Understanding extraction')
param contentUnderstandingEnabled bool

@description('Azure AI Language endpoint')
param languageEndpoint string

@description('Speech fast-transcription custom-domain endpoint; empty disables the audio writer')
param speechEndpoint string = ''

@description('Speech resource region')
param speechRegion string = ''

@description('Enable the audio ingestion writer')
param audioWriterEnabled bool = false

@description('Audio transcription locale')
param audioLocale string = 'en-US'

@description('Blob service endpoint of the dedicated audio-staging account; empty disables batch staging')
param audioStagingBlobEndpoint string = ''

@description('Source-audio container in the audio-staging account')
param audioStagingContainer string = ''

@description('Batch transcription result time-to-live in hours (6-744)')
@minValue(6)
@maxValue(744)
param audioBatchTtlHours int = 48

@description('Comma-separated allowed source file extensions')
@minLength(1)
param allowedFileExtensions string = '.md,.pdf,.docx,.pptx,.xlsx'

@description('Key Vault URI')
param keyVaultUri string

@description('Microsoft Entra tenant ID used by SharePoint and Function authentication')
param entraTenantId string

@description('SharePoint application client ID')
param sharePointAppClientId string

@description('Stable source registration ID')
param ingestionSourceId string

@description('Assigned SharePoint document-library drive ID')
param sharePointDriveId string

@description('Key Vault secret name containing the exportable SharePoint PFX')
param sharePointCertificateSecretName string

@description('Microsoft Entra application client ID protecting operator endpoints')
param adminApiClientId string

@description('Function API audience required in delegated user tokens')
@minLength(1)
param functionApiAudience string

@description('Managed-identity scope for the retrieval API')
@minLength(1)
param retrievalServiceScope string

@description('NCRONTAB schedule for the reconciliation timer (daily safety-net delta query; webhooks are the primary trigger)')
param deltaSyncSchedule string = '0 0 4 * * *'

@description('NCRONTAB schedule for the ACL-resync timer (weekly Sunday 03:00 UTC)')
param aclResyncSchedule string = '0 0 3 * * 0'

@description('Page size for each ACL-resync activity call')
@minValue(1)
@maxValue(100)
param aclResyncPageSize int = 50

@description('NCRONTAB schedule for persisted lifecycle reconciliation')
param lifecycleReconcileSchedule string = '0 */10 * * * *'

@description('Page size for each lifecycle reconciliation activity')
@minValue(1)
@maxValue(100)
param lifecycleReconcilePageSize int = 50

@description('Internal URL of the retrieval service for query proxy')
param retrievalServiceUrl string = ''

@secure()
@description('Shared secret for Microsoft Graph webhook clientState validation')
param webhookClientState string

@description('NCRONTAB schedule for Graph subscription renewal')
param subscriptionRenewSchedule string = '0 0 2 * * *'

@description('SharePoint site URL for site group ACL resolution via REST API')
@minLength(1)
param sharePointSiteUrl string

@description('Entra app client IDs allowed to call the Function API')
@minLength(1)
param allowedApplicationClientIds array

@description('Resource tags')
param tags object = {}

// =========================================
// Resources
// =========================================

// Flex Consumption hosting plan
#disable-next-line BCP081
resource hostingPlan 'Microsoft.Web/serverfarms@2025-03-01' = {
  name: '${functionAppName}-plan'
  location: location
  kind: 'functionapp'
  sku: {
    name: 'FC1' // Flex Consumption
    tier: 'FlexConsumption'
  }
  properties: {
    reserved: true // Linux
  }
  tags: tags
}

// Function App
#disable-next-line BCP081
resource functionApp 'Microsoft.Web/sites@2025-03-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp,linux'
  tags: union(tags, {
    'azd-service-name': 'rag-functions'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentityId}': {}
    }
  }
  properties: {
    serverFarmId: hostingPlan.id
    siteConfig: {
      appSettings: [
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'AzureWebJobsStorage__blobServiceUri'
          value: 'https://${storageAccountName}.blob.${environment().suffixes.storage}'
        }
        {
          name: 'AzureWebJobsStorage__queueServiceUri'
          value: 'https://${storageAccountName}.queue.${environment().suffixes.storage}'
        }
        {
          name: 'AzureWebJobsStorage__tableServiceUri'
          value: 'https://${storageAccountName}.table.${environment().suffixes.storage}'
        }
        {
          name: 'AzureWebJobsStorage__credential'
          value: 'managedidentity'
        }
        {
          name: 'AzureWebJobsStorage__clientId'
          value: managedIdentityClientId
        }
        {
          name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
          value: appInsightsConnectionString
        }
        {
          name: 'APPLICATIONINSIGHTS_AUTHENTICATION_STRING'
          value: 'ClientId=${managedIdentityClientId};Authorization=AAD'
        }
        {
          name: 'AZURE_CLIENT_ID'
          value: managedIdentityClientId
        }
        {
          name: 'MANAGED_IDENTITY_CLIENT_ID'
          value: managedIdentityClientId
        }
        {
          name: 'TENANT_ID'
          value: entraTenantId
        }
        {
          name: 'FUNCTION_API_AUDIENCE'
          value: functionApiAudience
        }
        {
          name: 'RETRIEVAL_SERVICE_SCOPE'
          value: retrievalServiceScope
        }
        {
          name: 'COSMOS_ENDPOINT'
          value: cosmosEndpoint
        }
        {
          name: 'COSMOS_DATABASE_NAME'
          value: cosmosDatabaseName
        }
        {
          name: 'COSMOS_INGESTION_RUNS_CONTAINER_NAME'
          value: cosmosContainerNames.ingestionRuns
        }
        {
          name: 'COSMOS_SOURCE_DOCUMENTS_CONTAINER_NAME'
          value: cosmosContainerNames.sourceDocuments
        }
        {
          name: 'COSMOS_SEARCH_CHUNKS_CONTAINER_NAME'
          value: cosmosContainerNames.searchChunks
        }
        {
          name: 'DURABLE_TASK_SCHEDULER_CONNECTION_STRING'
          value: 'Endpoint=${durableTaskSchedulerEndpoint};Authentication=ManagedIdentity;ClientID=${managedIdentityClientId}'
        }
        {
          name: 'TASKHUB_NAME'
          value: durableTaskHubName
        }
        {
          name: 'OPENAI_ENDPOINT'
          value: openAiEndpoint
        }
        {
          name: 'OPENAI_EMBEDDING_DEPLOYMENT_NAME'
          value: embeddingDeploymentName
        }
        {
          name: 'OPENAI_CHAT_DEPLOYMENT_NAME'
          value: chatDeploymentName
        }
        {
          name: 'VISION_MAX_OUTPUT_TOKENS'
          value: '400'
        }
        {
          name: 'VISION_MAX_IMAGE_BYTES'
          value: '2097152'
        }
        {
          name: 'VISION_MAX_FIGURES'
          value: '60'
        }
        {
          name: 'DOCUMENT_INTELLIGENCE_ENDPOINT'
          value: documentIntelligenceEndpoint
        }
        {
          name: 'DOCUMENT_INTELLIGENCE_ENABLED'
          value: string(documentIntelligenceEnabled)
        }
        {
          name: 'CONTENT_UNDERSTANDING_ENDPOINT'
          value: contentUnderstandingEndpoint
        }
        {
          name: 'CONTENT_UNDERSTANDING_ANALYZER_ID'
          value: contentUnderstandingAnalyzerId
        }
        {
          name: 'CONTENT_UNDERSTANDING_ENABLED'
          value: string(contentUnderstandingEnabled)
        }
        {
          name: 'AZURE_LANGUAGE_ENDPOINT'
          value: languageEndpoint
        }
        {
          name: 'SPEECH_ENDPOINT'
          value: speechEndpoint
        }
        {
          name: 'SPEECH_REGION'
          value: speechRegion
        }
        {
          name: 'AUDIO_DEPLOYMENT_REGION'
          value: speechRegion
        }
        {
          name: 'AUDIO_LOCALE'
          value: audioWriterEnabled ? audioLocale : ''
        }
        {
          name: 'AUDIO_WRITER_ENABLED'
          value: string(audioWriterEnabled)
        }
        {
          name: 'AUDIO_TRANSCRIPTION_PROVIDER'
          value: audioWriterEnabled ? 'speech_batch' : 'speech_fast'
        }
        {
          name: 'AUDIO_STAGING_BLOB_ENDPOINT'
          value: audioStagingBlobEndpoint
        }
        {
          name: 'AUDIO_STAGING_CONTAINER'
          value: audioStagingContainer
        }
        {
          name: 'AUDIO_BATCH_TTL_HOURS'
          value: string(audioBatchTtlHours)
        }
        {
          name: 'KEY_VAULT_URI'
          value: keyVaultUri
        }
        {
          name: 'SHAREPOINT_TENANT_ID'
          value: entraTenantId
        }
        {
          name: 'SHAREPOINT_APP_CLIENT_ID'
          value: sharePointAppClientId
        }
        {
          name: 'INGESTION_SOURCE_ID'
          value: ingestionSourceId
        }
        {
          name: 'ALLOWED_FILE_EXTENSIONS'
          value: allowedFileExtensions
        }
        {
          name: 'SHAREPOINT_ASSIGNED_DRIVE_ID'
          value: sharePointDriveId
        }
        {
          name: 'SHAREPOINT_CERTIFICATE_SECRET_NAME'
          value: sharePointCertificateSecretName
        }
        {
          name: 'FUNCTION_PUBLIC_BASE_URL'
          value: 'https://${functionAppName}.azurewebsites.net'
        }
        {
          name: 'DELTA_SYNC_SCHEDULE'
          value: deltaSyncSchedule
        }
        {
          name: 'ACL_RESYNC_SCHEDULE'
          value: aclResyncSchedule
        }
        {
          name: 'ACL_RESYNC_PAGE_SIZE'
          value: string(aclResyncPageSize)
        }
        {
          name: 'LIFECYCLE_RECONCILE_SCHEDULE'
          value: lifecycleReconcileSchedule
        }
        {
          name: 'LIFECYCLE_RECONCILE_PAGE_SIZE'
          value: string(lifecycleReconcilePageSize)
        }
        {
          name: 'CHUNK_MAX_TOKENS'
          value: '800'
        }
        {
          name: 'CHUNK_OVERLAP_TOKENS'
          value: '100'
        }
        {
          name: 'ACL_MAX_PAGES'
          value: '10'
        }
        {
          name: 'DOWNLOAD_TIMEOUT_SECONDS'
          value: '120'
        }
        {
          name: 'DELTA_MAX_PAGES'
          value: '200'
        }
        {
          name: 'EMBEDDING_BATCH_SIZE'
          value: '100'
        }
        {
          name: 'MAX_PDF_PAGES'
          value: '500'
        }
        {
          name: 'QUERY_PROXY_TIMEOUT_SECONDS'
          value: '30'
        }
        {
          name: 'RETRIEVAL_SERVICE_URL'
          value: retrievalServiceUrl
        }
        {
          name: 'WEBHOOK_CLIENT_STATE'
          value: webhookClientState
        }
        {
          name: 'SUBSCRIPTION_RENEW_SCHEDULE'
          value: subscriptionRenewSchedule
        }
        {
          name: 'SHAREPOINT_SITE_URL'
          value: sharePointSiteUrl
        }
        {
          name: 'INSTANCE_MEMORY_MB'
          value: string(instanceMemoryMB)
        }
      ]
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      use32BitWorkerProcess: false
      cors: {
        allowedOrigins: []
      }
    }
    functionAppConfig: {
      deployment: {
        storage: {
          type: 'blobContainer'
          value: deploymentStorageContainerUrl
          authentication: {
            type: 'UserAssignedIdentity'
            userAssignedIdentityResourceId: managedIdentityId
          }
        }
      }
      runtime: {
        name: 'python'
        version: '3.12'
      }
      scaleAndConcurrency: {
        maximumInstanceCount: maximumInstanceCount
        instanceMemoryMB: instanceMemoryMB
        alwaysReady: [
          {
            name: 'http'
            instanceCount: 1
          }
        ]
      }
    }
    httpsOnly: true
    keyVaultReferenceIdentity: managedIdentityId
    publicNetworkAccess: 'Enabled'
    virtualNetworkSubnetId: integrationSubnetId
  }
}

#disable-next-line BCP081
resource authSettings 'Microsoft.Web/sites/config@2025-03-01' = {
  parent: functionApp
  name: 'authsettingsV2'
  properties: {
    platform: {
      enabled: true
      runtimeVersion: '~1'
    }
    globalValidation: {
      requireAuthentication: true
      unauthenticatedClientAction: 'Return401'
      excludedPaths: [
        '/api/webhook/*'
      ]
    }
    identityProviders: {
      azureActiveDirectory: {
        enabled: true
        registration: {
          clientId: adminApiClientId
          openIdIssuer: '${environment().authentication.loginEndpoint}${entraTenantId}/v2.0'
        }
        validation: {
          allowedAudiences: [
            functionApiAudience
          ]
          defaultAuthorizationPolicy: {
            allowedApplications: allowedApplicationClientIds
          }
        }
      }
    }
    httpSettings: {
      requireHttps: true
    }
  }
}

// =========================================
// Outputs
// =========================================

@description('Function App resource ID')
output functionAppId string = functionApp.id

@description('Function App name')
output functionAppName string = functionApp.name

@description('Function App URL')
output functionAppUrl string = 'https://${functionApp.properties.defaultHostName}'
