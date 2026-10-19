using './modules/cosmos-content-rbac.bicep'

param cosmosAccountName = readEnvironmentVariable('COSMOS_ACCOUNT_NAME')
param cosmosDatabaseName = 'rag-db'
param functionPrincipalId = readEnvironmentVariable('FUNCTION_PRINCIPAL_ID')
param retrievalPrincipalId = readEnvironmentVariable('RETRIEVAL_PRINCIPAL_ID')
