using './modules/cosmos-content-topology.bicep'

param cosmosAccountName = readEnvironmentVariable('COSMOS_ACCOUNT_NAME')
param databaseName = 'rag-db'
param mode = readEnvironmentVariable('COSMOS_DB_MODE', 'serverless')
param searchChunksAutoscaleMaxThroughput = int(readEnvironmentVariable('COSMOS_SEARCH_AUTOSCALE_MAX_RUS', '1000'))
