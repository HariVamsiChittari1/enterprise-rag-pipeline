<#
.SYNOPSIS
    Preview or execute one authorized phase of the greenfield ACA deployment.

.DESCRIPTION
    Preview is the default. Azure mutations require -Execute, exact plan/source
    hashes, and a target matching the current Azure CLI and azd context. The
    script never creates or deletes a resource group, mutates Entra directory
    objects, invents secrets, or preserves mutable image tags. Only the explicit
    OperationsCleanup phase deletes the uniquely tagged temporary catalog job.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet(
        'Authority', 'Foundation', 'Build', 'Operations', 'Catalog', 'CatalogVerify',
        'OperationsCleanup', 'Final', 'Function', 'FunctionPackage'
    )]
    [string]$Phase,

    [string]$PlanId = 'aca-greenfield-retrieval-v1',
    [string]$ExpectedPlanHash,
    [string]$ExpectedSourceTreeHash,
    [string]$FunctionPackagePath,
    [string]$ExpectedFunctionPackageHash,
    [string]$SubscriptionId,
    [string]$TenantId,
    [string]$ResourceGroup,
    [string]$FunctionAppName,
    [string]$Location,
    [string]$AzdEnvironment,
    [string]$DeploymentInstanceId,
    [bool]$DocumentIntelligenceEnabled = $true,
    [bool]$ContentUnderstandingEnabled = $false,
    [bool]$AudioWriterEnabled = $false,
    [bool]$AudioRetrievalEnabled = $false,
    [ValidateSet('en-US', 'en-GB', 'en-IN')]
    [string]$AudioLocale = 'en-US',
    [bool]$AclEnabled = $true,
    [bool]$IncludeCitations = $true,
    [string]$ContentUnderstandingAnalyzerId = 'prebuilt-documentSearch',
    [string]$TemporaryContentUnderstandingClientIp,
    [string]$CatalogFile = 'app/retrieval/catalog.example.json',
    [ValidateSet('publish-catalog', 'verify-catalog')]
    [string]$CatalogOperation = 'verify-catalog',
    [string]$JobExecutionName,
    [switch]$Execute
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PlanPath = Join-Path $ProjectRoot '.azure/deployment-plan.md'
$TemplatePath = Join-Path $ProjectRoot 'infra/main.bicep'
$ParameterPath = Join-Path $ProjectRoot 'infra/main.parameters.bicepparam'
$ContentUnderstandingAccessParameterPath = Join-Path $ProjectRoot 'infra/content-understanding-access.parameters.bicepparam'
$OperationsTemplatePath = Join-Path $ProjectRoot 'infra/modules/aca-operations-job.bicep'
$OperationsParameterPath = Join-Path $ProjectRoot 'infra/operations.parameters.bicepparam'
$ProtectedPathPatterns = @(
    '^\.git/', '^\.venv/', '^app/\.venv', '^data/', '^demo-output/',
    '/__pycache__/', '^\.azure/[^/]+/\.env$'
)

function Get-RequiredValue {
    param([string]$Name, [string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw "$Name is required."
    }
    return $Value.Trim()
}

function Get-SourceTreeHash {
    $paths = git -C $ProjectRoot ls-files --cached --others --exclude-standard
    if ($LASTEXITCODE -ne 0) { throw 'Unable to enumerate the source tree.' }
    $included = @($paths | Sort-Object -Unique | Where-Object {
            $normalized = $_.Replace('\', '/')
            -not ($ProtectedPathPatterns | Where-Object { $normalized -match $_ }) -and
            (Test-Path -LiteralPath (Join-Path $ProjectRoot $_) -PathType Leaf)
        })
    if ($included.Count -eq 0) { throw 'Source inventory is empty.' }
    $hashes = @($included | git -C $ProjectRoot hash-object --stdin-paths)
    if ($LASTEXITCODE -ne 0 -or $hashes.Count -ne $included.Count) {
        throw 'Unable to hash the source tree.'
    }
    $entries = for ($index = 0; $index -lt $included.Count; $index++) {
        $relativePath = $included[$index]
        $normalized = $relativePath.Replace('\', '/')
        "$normalized`n$($hashes[$index])"
    }
    $payload = [Text.Encoding]::UTF8.GetBytes(($entries -join "`n"))
    $stream = [IO.MemoryStream]::new($payload)
    try { return (Get-FileHash -InputStream $stream -Algorithm SHA256).Hash.ToLowerInvariant() }
    finally { $stream.Dispose() }
}

function Get-PlanAuthority {
    param([string]$ReviewedPlanPath, [string]$ReviewedPlanId)
    if (-not (Test-Path -LiteralPath $ReviewedPlanPath -PathType Leaf)) {
        throw 'Deployment plan is missing.'
    }
    $planText = Get-Content -LiteralPath $ReviewedPlanPath -Raw
    if ($planText -notmatch [regex]::Escape("Plan ID: ``$ReviewedPlanId``")) {
        throw 'Deployment plan ID does not match this controller.'
    }
    if ($planText -match 'existing development environment') {
        throw 'Stale deployment target authority is present in the plan.'
    }
    return [ordered]@{
        planId         = $ReviewedPlanId
        planHash       = (Get-FileHash -LiteralPath $ReviewedPlanPath -Algorithm SHA256).Hash.ToLowerInvariant()
        sourceTreeHash = Get-SourceTreeHash
    }
}

function Get-Authority {
    return Get-PlanAuthority -ReviewedPlanPath $PlanPath -ReviewedPlanId $PlanId
}

function Assert-Authority {
    param($Authority)
    $authority = $Authority
    if ($authority.planHash -ne (Get-RequiredValue 'ExpectedPlanHash' $ExpectedPlanHash).ToLowerInvariant()) {
        throw 'Deployment plan hash changed after review.'
    }
    if ($authority.sourceTreeHash -ne (Get-RequiredValue 'ExpectedSourceTreeHash' $ExpectedSourceTreeHash).ToLowerInvariant()) {
        throw 'Source tree hash changed after review.'
    }
    return $authority
}

function Test-FunctionPackage {
    $packagePath = Get-RequiredValue 'FunctionPackagePath' $FunctionPackagePath
    $expectedHash = Get-RequiredValue 'ExpectedFunctionPackageHash' $ExpectedFunctionPackageHash
    if ($expectedHash -cnotmatch '^[a-f0-9]{64}$') {
        throw 'ExpectedFunctionPackageHash must be a lowercase SHA-256 digest.'
    }
    if ([IO.Path]::GetExtension($packagePath) -ine '.zip' -or
        -not (Test-Path -LiteralPath $packagePath -PathType Leaf)) {
        throw 'FunctionPackagePath must reference an existing ZIP file.'
    }
    $stream = [IO.File]::OpenRead((Resolve-Path -LiteralPath $packagePath).Path)
    try {
        if ($stream.Length -gt 256MB) { throw 'Function source ZIP exceeds the 256 MiB preflight limit.' }
        $actualHash = (Get-FileHash -InputStream $stream -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actualHash -cne $expectedHash) { throw 'Function package hash does not match review.' }
        $stream.Position = 0
        $archive = [IO.Compression.ZipArchive]::new($stream, [IO.Compression.ZipArchiveMode]::Read, $true)
        try {
            if ($archive.Entries.Count -gt 4096) { throw 'Function source ZIP exceeds the 4096-entry preflight limit.' }
            $paths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
            $files = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
            $filePaths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
            $buffer = [byte[]]::new(65536)
            $totalBytes = 0L
            foreach ($entry in $archive.Entries) {
                $path = $entry.FullName
                if ([string]::IsNullOrWhiteSpace($path) -or
                    $path -match '[\\:\x00-\x1f\x7f]' -or $path.StartsWith('/') -or
                    $path -match '(^|/)\.{1,2}(/|$)' -or $path.Contains('//') -or
                    -not $paths.Add($path.TrimEnd('/'))) {
                    throw 'Function package contains unsafe or duplicate paths.'
                }
                if ($path -match '(^|/)(local\.settings\.json|\.env[^/]*|\.azure|\.git|\.venv[^/]*|venv|__pycache__|\.pytest_cache)(/|$)' -or
                    $path -match '\.(pem|pfx|p12|key|pyc|pyo)$') {
                    throw 'Function package contains excluded local or credential artifacts.'
                }
                if ($path -match '^operations(/|$)') {
                    throw 'Function package contains standalone operations tooling.'
                }
                $unixType = ($entry.ExternalAttributes -shr 16) -band 0xF000
                if ($unixType -notin @(0, 0x8000, 0x4000) -or
                    ($unixType -eq 0x4000 -and -not $path.EndsWith('/')) -or
                    ($unixType -eq 0x8000 -and $path.EndsWith('/'))) {
                    throw 'Function package contains unsupported filesystem entry types.'
                }
                if ($entry.Length -gt 256MB -or $totalBytes + $entry.Length -gt 256MB) {
                    throw 'Function source ZIP exceeds the 256 MiB expanded preflight limit.'
                }
                $entryStream = $null
                $entryBytes = 0L
                try {
                    $entryStream = $entry.Open()
                    while (($bytesRead = $entryStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                        $entryBytes += $bytesRead
                        $totalBytes += $bytesRead
                        if ($totalBytes -gt 256MB -or $entryBytes -gt $entry.Length) {
                            throw 'Expanded entry exceeds its validated bounds.'
                        }
                    }
                    if ($entryBytes -ne $entry.Length -or ($path.EndsWith('/') -and $entryBytes -ne 0)) {
                        throw 'Expanded entry does not match its declared length.'
                    }
                }
                catch { throw 'Function package contains unreadable or inconsistent entry data.' }
                finally { if ($null -ne $entryStream) { $entryStream.Dispose() } }
                if (-not $path.EndsWith('/')) {
                    $filePaths.Add($path) | Out-Null
                    if ($entryBytes -gt 0) { $files.Add($path) | Out-Null }
                }
            }
            foreach ($path in $paths) {
                $ancestor = $path
                while ($ancestor.Contains('/')) {
                    $ancestor = $ancestor.Substring(0, $ancestor.LastIndexOf('/'))
                    if ($filePaths.Contains($ancestor)) {
                        throw 'Function package contains conflicting file and directory paths.'
                    }
                }
            }
            foreach ($required in @('host.json', 'function_app.py', 'requirements.txt')) {
                if (-not $files.Contains($required)) {
                    throw 'Function package requires nonempty host.json, function_app.py and requirements.txt at its root.'
                }
            }
            return [ordered]@{
                action      = 'validated-package-only'
                packageHash = $actualHash
                entryCount  = $archive.Entries.Count
            }
        }
        finally { $archive.Dispose() }
    }
    finally { $stream.Dispose() }
}

function Assert-FunctionTarget {
    $appName = Get-RequiredValue 'FunctionAppName' $FunctionAppName
    if ($appName -cnotmatch '^[a-zA-Z0-9][a-zA-Z0-9-]{0,58}[a-zA-Z0-9]$' -or
        $FunctionAppName -cne $appName) {
        throw 'FunctionAppName must be an explicit valid main-site name.'
    }
    $version = azd version 2>$null
    if ($LASTEXITCODE -ne 0 -or ($version -join "`n") -notmatch '^azd version 1\.34\.[01](?: |$)') {
        throw 'Function target resolution requires reviewed azd version 1.34.0 or 1.34.1.'
    }
    $bindings = [ordered]@{
        AZURE_SUBSCRIPTION_ID          = $SubscriptionId
        FUNCTION_DEPLOY_RESOURCE_GROUP = $ResourceGroup
        FUNCTION_DEPLOY_APP_NAME       = $appName
    }
    foreach ($key in $bindings.Keys) {
        $stored = @(azd env get-value $key --environment $AzdEnvironment 2>$null)
        if ($LASTEXITCODE -ne 0 -or $stored.Count -ne 1 -or
            [string]::IsNullOrWhiteSpace($stored[0]) -or
            $stored[0] -cne $stored[0].Trim() -or $stored[0] -ine $bindings[$key]) {
            throw "Stored azd $key does not match the reviewed Function target."
        }
    }
}

function Invoke-FunctionDeployment {
    $packageStream = $null
    Push-Location $ProjectRoot
    try {
        Assert-FunctionTarget
        $package = $null
        $packagePath = $null
        if (-not [string]::IsNullOrEmpty($FunctionPackagePath) -or
            -not [string]::IsNullOrEmpty($ExpectedFunctionPackageHash)) {
            $package = Test-FunctionPackage
            $packagePath = (Resolve-Path -LiteralPath $FunctionPackagePath).Path
            $packageStream = [IO.File]::Open($packagePath, [IO.FileMode]::Open,
                [IO.FileAccess]::Read, [IO.FileShare]::Read)
            $lockedHash = (Get-FileHash -InputStream $packageStream -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($lockedHash -cne $package.packageHash) { throw 'Function package changed after validation.' }
        }
        if (-not $Execute) {
            $preview = [ordered]@{ action = 'preview'; service = 'rag-functions'; environment = $AzdEnvironment }
            if ($null -ne $package) { $preview.packageHash = $package.packageHash }
            $preview | ConvertTo-Json -Compress
            return
        }
        Assert-Authority -Authority (Get-Authority) | Out-Null
        Assert-FunctionTarget
        $arguments = @('deploy', 'rag-functions', '--environment', $AzdEnvironment, '--no-prompt')
        if ($null -ne $packagePath) { $arguments += @('--from-package', $packagePath) }
        azd @arguments
        if ($LASTEXITCODE -ne 0) { throw 'Function deployment failed.' }
    }
    finally {
        if ($null -ne $packageStream) { $packageStream.Dispose() }
        Pop-Location
    }
}

function Assert-Target {
    $script:SubscriptionId = Get-RequiredValue 'SubscriptionId' $SubscriptionId
    $script:TenantId = Get-RequiredValue 'TenantId' $TenantId
    $script:ResourceGroup = Get-RequiredValue 'ResourceGroup' $ResourceGroup
    $script:Location = Get-RequiredValue 'Location' $Location
    $script:AzdEnvironment = Get-RequiredValue 'AzdEnvironment' $AzdEnvironment
    $script:DeploymentInstanceId = Get-RequiredValue 'DeploymentInstanceId' $DeploymentInstanceId

    $account = az account show --query '{subscription:id,tenant:tenantId}' --output json --only-show-errors | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or -not $account.subscription) { throw 'Azure CLI authentication is required.' }
    if ($account.subscription -ne $SubscriptionId -or $account.tenant -ne $TenantId) {
        throw 'Azure CLI subscription or tenant does not match the reviewed target.'
    }
    $azdSubscription = azd env get-value AZURE_SUBSCRIPTION_ID --environment $AzdEnvironment 2>$null
    $azdLocation = azd env get-value AZURE_LOCATION --environment $AzdEnvironment 2>$null
    if ($LASTEXITCODE -ne 0 -or $azdSubscription.Trim() -ne $SubscriptionId -or $azdLocation.Trim() -ne $Location) {
        throw 'azd environment does not match the reviewed subscription and location.'
    }
    $exists = az group exists --name $ResourceGroup --subscription $SubscriptionId --only-show-errors
    if ($LASTEXITCODE -ne 0 -or $exists -ne 'true') {
        throw 'The reviewed resource group must already exist; this script will not create it.'
    }
}

function Assert-RequiredEnvironment {
    $required = @(
        'AZURE_OPENAI_ACCOUNT_NAME', 'AZURE_OPENAI_RESOURCE_GROUP',
        'OPENAI_CHAT_DEPLOYMENT_NAME', 'SHAREPOINT_TENANT_ID',
        'SHAREPOINT_APP_CLIENT_ID', 'SHAREPOINT_ASSIGNED_DRIVE_ID', 'SHAREPOINT_SITE_URL',
        'SHAREPOINT_KEY_VAULT_NAME', 'SHAREPOINT_KEY_VAULT_RESOURCE_GROUP',
        'INGESTION_SOURCE_ID', 'ADMIN_API_CLIENT_ID', 'FUNCTION_API_AUDIENCE',
        'RETRIEVAL_API_CLIENT_ID', 'RETRIEVAL_API_AUDIENCE',
        'RETRIEVAL_API_SERVICE_PRINCIPAL_ID',
        'FUNCTION_ALLOWED_CALLER_CLIENT_ID', 'WEBHOOK_CLIENT_STATE',
        'COST_CENTER', 'CLEANUP_DATE'
    )
    $missing = @($required | Where-Object { [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($_)) })
    if ($missing.Count -gt 0) { throw "Required deployment settings are missing: $($missing -join ', ')" }
}

function Import-AzdEnvironment {
    $names = @(
        'AZURE_OPENAI_ACCOUNT_NAME', 'AZURE_OPENAI_RESOURCE_GROUP',
        'OPENAI_EMBEDDING_DEPLOYMENT_NAME', 'OPENAI_CHAT_DEPLOYMENT_NAME',
        'SHAREPOINT_TENANT_ID', 'SHAREPOINT_APP_CLIENT_ID',
        'SHAREPOINT_ASSIGNED_DRIVE_ID', 'SHAREPOINT_SITE_URL',
        'SHAREPOINT_CERTIFICATE_SECRET_NAME', 'SHAREPOINT_KEY_VAULT_NAME',
        'SHAREPOINT_KEY_VAULT_RESOURCE_GROUP', 'INGESTION_SOURCE_ID',
        'ADMIN_API_CLIENT_ID', 'FUNCTION_API_AUDIENCE',
        'RETRIEVAL_API_CLIENT_ID', 'RETRIEVAL_API_AUDIENCE',
        'RETRIEVAL_API_SERVICE_PRINCIPAL_ID',
        'FUNCTION_ALLOWED_CALLER_CLIENT_ID', 'WEBHOOK_CLIENT_STATE',
        'COSMOS_DB_MODE', 'COSMOS_METADATA_AUTOSCALE_MAX_RUS',
        'COSMOS_SEARCH_AUTOSCALE_MAX_RUS', 'STORAGE_REDUNDANCY',
        'APPLICATION_INSIGHTS_DAILY_CAP_GB', 'RETRIEVAL_MIN_REPLICAS',
        'RETRIEVAL_MAX_REPLICAS', 'RETRIEVAL_ZONE_REDUNDANT',
        'COST_CENTER', 'CLEANUP_DATE', 'ACR_NAME', 'RELEASE_BUILD_ID',
        'RETRIEVAL_IMAGE_REFERENCE', 'RETRIEVAL_CATALOG_DIGEST',
        'RETRIEVAL_CATALOG_POLL_SECONDS', 'CATALOG_EDITOR_PRINCIPAL_ID',
        'CATALOG_WRITER_PRINCIPAL_ID', 'CATALOG_OBSERVER_PRINCIPAL_ID'
    )
    foreach ($name in $names) {
        $value = azd env get-value $name --environment $AzdEnvironment 2>$null
        if ($name -eq 'RETRIEVAL_CATALOG_POLL_SECONDS') {
            if ($LASTEXITCODE -eq 0 -and [string]::IsNullOrWhiteSpace($value)) {
                throw 'RETRIEVAL_CATALOG_POLL_SECONDS must not be blank when configured.'
            }
            if ($LASTEXITCODE -eq 0) {
                [Environment]::SetEnvironmentVariable($name, [string]$value, 'Process')
            }
            continue
        }
        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($value)) {
            [Environment]::SetEnvironmentVariable($name, $value.Trim(), 'Process')
        }
    }
}

function Set-CatalogEnvironment {
    $pollValue = [Environment]::GetEnvironmentVariable('RETRIEVAL_CATALOG_POLL_SECONDS')
    if ($null -eq $pollValue) { $pollValue = '7200' }
    $pollSeconds = 0
    if ($pollValue -notmatch '^[0-9]+$' -or
        -not [int]::TryParse($pollValue, [ref]$pollSeconds) -or
        $pollSeconds -lt 60 -or $pollSeconds -gt 86400) {
        throw 'RETRIEVAL_CATALOG_POLL_SECONDS must be an integer from 60 through 86400.'
    }
    $env:RETRIEVAL_CATALOG_POLL_SECONDS = [string]$pollSeconds
    $env:CATALOG_OPERATION = $CatalogOperation
}

function Assert-CatalogInitialization {
    if ($CatalogOperation -eq 'verify-catalog') { return }
    if ($env:RETRIEVAL_CATALOG_DIGEST -notmatch '^sha256:[a-f0-9]{64}$') {
        throw 'RETRIEVAL_CATALOG_DIGEST must be sha256:<64 lowercase hex> for initialization.'
    }
    if ($env:RETRIEVAL_CATALOG_DIGEST -ne (Get-ReviewedCatalogDigest)) {
        throw 'RETRIEVAL_CATALOG_DIGEST differs from the reviewed catalog file.'
    }
}

function Set-ParameterEnvironment {
    Set-CatalogEnvironment
    if (-not $DocumentIntelligenceEnabled -and -not $ContentUnderstandingEnabled) {
        throw 'At least one extraction provider must be enabled.'
    }
    $env:AZURE_LOCATION = $Location
    $env:DEPLOYMENT_INSTANCE_ID = $DeploymentInstanceId
    $env:DOCUMENT_INTELLIGENCE_ENABLED = $DocumentIntelligenceEnabled.ToString().ToLowerInvariant()
    $env:CONTENT_UNDERSTANDING_ENABLED = $ContentUnderstandingEnabled.ToString().ToLowerInvariant()
    $env:AUDIO_WRITER_ENABLED = $AudioWriterEnabled.ToString().ToLowerInvariant()
    $env:AUDIO_RETRIEVAL_ENABLED = $AudioRetrievalEnabled.ToString().ToLowerInvariant()
    $env:AUDIO_LOCALE = $AudioLocale
    $env:ACL_ENABLED = $AclEnabled.ToString().ToLowerInvariant()
    $env:INCLUDE_CITATIONS = $IncludeCitations.ToString().ToLowerInvariant()
    $env:CONTENT_UNDERSTANDING_ANALYZER_ID = Get-RequiredValue `
        'ContentUnderstandingAnalyzerId' $ContentUnderstandingAnalyzerId

    $virtualNetworkName = "rag-$DeploymentInstanceId-vnet"
    $subnetMappings = @(
        @{ Name = 'function-integration'; Environment = 'SUBNET_FUNCTION_INTEGRATION_NSG_ID' }
        @{ Name = 'private-endpoints'; Environment = 'SUBNET_PRIVATE_ENDPOINTS_NSG_ID' }
        @{ Name = 'aca-environment'; Environment = 'SUBNET_ACA_ENVIRONMENT_NSG_ID' }
    )
    $virtualNetworkIds = @(az network vnet list `
            --subscription $SubscriptionId `
            --resource-group $ResourceGroup `
            --query "[?name=='$virtualNetworkName'].id" `
            --output tsv `
            --only-show-errors)
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to verify reviewed virtual network state.'
    }
    if ($virtualNetworkIds.Count -gt 1) {
        throw 'Expected at most one reviewed virtual network.'
    }
    if ($virtualNetworkIds.Count -eq 0) {
        foreach ($mapping in $subnetMappings) {
            [Environment]::SetEnvironmentVariable($mapping.Environment, '', 'Process')
        }
        return
    }
    foreach ($mapping in $subnetMappings) {
        $nsgId = az network vnet subnet show `
            --subscription $SubscriptionId `
            --resource-group $ResourceGroup `
            --vnet-name $virtualNetworkName `
            --name $mapping.Name `
            --query networkSecurityGroup.id `
            --output tsv `
            --only-show-errors 2>$null
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($nsgId)) {
            throw "Unable to preserve network security group association for subnet '$($mapping.Name)'."
        }
        [Environment]::SetEnvironmentVariable($mapping.Environment, $nsgId.Trim(), 'Process')
    }
}

function Get-SingleDeploymentResource {
    param([string]$ResourceType, [string]$Kind)
    $resources = @(az resource list `
            --subscription $SubscriptionId `
            --resource-group $ResourceGroup `
            --resource-type $ResourceType `
            --query "[?tags.DeploymentInstance=='$DeploymentInstanceId']" `
            --output json `
            --only-show-errors | ConvertFrom-Json)
    if (-not [string]::IsNullOrWhiteSpace($Kind)) {
        $resources = @($resources | Where-Object { $_.kind -eq $Kind })
    }
    if ($LASTEXITCODE -ne 0 -or $resources.Count -ne 1) {
        $expected = if ([string]::IsNullOrWhiteSpace($Kind)) { $ResourceType } else { "$Kind $ResourceType" }
        throw "Expected exactly one $expected resource for this deployment instance; found $($resources.Count)."
    }
    return $resources[0]
}

function Get-TemporaryContentUnderstandingClientIp {
    if ([string]::IsNullOrWhiteSpace($TemporaryContentUnderstandingClientIp)) {
        return ''
    }

    $parsedAddress = $null
    if (
        -not [Net.IPAddress]::TryParse($TemporaryContentUnderstandingClientIp.Trim(), [ref]$parsedAddress) -or
        $parsedAddress.AddressFamily -ne [Net.Sockets.AddressFamily]::InterNetwork
    ) {
        throw 'TemporaryContentUnderstandingClientIp must be one public IPv4 address without CIDR notation.'
    }
    if (
        [Net.IPAddress]::IsLoopback($parsedAddress) -or
        $parsedAddress.GetAddressBytes()[0] -in @(0, 10, 127) -or
        ($parsedAddress.GetAddressBytes()[0] -eq 169 -and $parsedAddress.GetAddressBytes()[1] -eq 254) -or
        ($parsedAddress.GetAddressBytes()[0] -eq 172 -and $parsedAddress.GetAddressBytes()[1] -in 16..31) -or
        ($parsedAddress.GetAddressBytes()[0] -eq 192 -and $parsedAddress.GetAddressBytes()[1] -eq 168)
    ) {
        throw 'TemporaryContentUnderstandingClientIp must be a public IPv4 address.'
    }
    return $parsedAddress.ToString()
}

function Assert-ContentUnderstandingNetworkState {
    param([string]$AllowedIpAddress)

    $account = Get-SingleDeploymentResource `
        -ResourceType 'Microsoft.CognitiveServices/accounts' `
        -Kind 'AIServices'
    $configured = az resource show `
        --ids $account.id `
        --api-version 2025-06-01 `
        --output json `
        --only-show-errors | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to verify the Content Understanding network state.'
    }

    $expectedPublicNetworkAccess = if ([string]::IsNullOrEmpty($AllowedIpAddress)) { 'Disabled' } else { 'Enabled' }
    $configuredIpRules = @($configured.properties.networkAcls.ipRules)
    $expectedIpRules = @()
    if (-not [string]::IsNullOrEmpty($AllowedIpAddress)) {
        $expectedIpRules = @($AllowedIpAddress)
    }
    $actualIpRules = @($configuredIpRules | ForEach-Object { $_.value })
    $ipRulesMatch = $actualIpRules.Count -eq $expectedIpRules.Count -and (
        $actualIpRules.Count -eq 0 -or
        -not (Compare-Object -ReferenceObject $actualIpRules -DifferenceObject $expectedIpRules)
    )
    if (
        $configured.properties.publicNetworkAccess -ne $expectedPublicNetworkAccess -or
        $configured.properties.disableLocalAuth -ne $true -or
        $configured.properties.networkAcls.defaultAction -ne 'Deny' -or
        -not $ipRulesMatch
    ) {
        throw 'Content Understanding network state does not match the guarded access contract.'
    }
}

function Set-ContentUnderstandingNetworkAccess {
    param([string]$AllowedIpAddress)

    $account = Get-SingleDeploymentResource `
        -ResourceType 'Microsoft.CognitiveServices/accounts' `
        -Kind 'AIServices'
    $env:CONTENT_UNDERSTANDING_ACCOUNT_NAME = $account.name
    $env:CONTENT_UNDERSTANDING_ALLOWED_IP_ADDRESS = $AllowedIpAddress

    az deployment group what-if `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --template-file (Join-Path $ProjectRoot 'infra/modules/content-understanding.bicep') `
        --parameters $ContentUnderstandingAccessParameterPath `
        --result-format FullResourcePayloads `
        --no-pretty-print `
        --only-show-errors
    if ($LASTEXITCODE -ne 0) { throw 'Content Understanding network access preview failed.' }

    az deployment group create `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --template-file (Join-Path $ProjectRoot 'infra/modules/content-understanding.bicep') `
        --parameters $ContentUnderstandingAccessParameterPath `
        --mode Incremental `
        --only-show-errors
    if ($LASTEXITCODE -ne 0) { throw 'Content Understanding network access deployment failed.' }
    Assert-ContentUnderstandingNetworkState -AllowedIpAddress $AllowedIpAddress
}

function Set-ContentUnderstandingDefaults {
    $account = Get-SingleDeploymentResource `
        -ResourceType 'Microsoft.CognitiveServices/accounts' `
        -Kind 'AIServices'
    $endpoint = "https://$($account.name).services.ai.azure.com"
    $defaultsUrl = "$endpoint/contentunderstanding/defaults?api-version=2025-11-01"
    $defaults = [ordered]@{
        modelDeployments = [ordered]@{
            'gpt-5.2'                           = 'cu-gpt-5-2'
            'text-embedding-3-large'            = 'cu-text-embedding-3-large'
            'prebuilt-analyzer-completion'      = 'cu-gpt-5-2'
            'prebuilt-analyzer-completion-mini' = 'cu-gpt-5-2'
            'prebuilt-analyzer-embedding'       = 'cu-text-embedding-3-large'
        }
    }
    $body = ($defaults | ConvertTo-Json -Depth 3 -Compress).Replace('"', '\"')

    $patchExitCode = 1
    $patchError = ''
    for ($attempt = 0; $attempt -lt 12 -and $patchExitCode -ne 0; $attempt++) {
        $patchOutput = az rest `
            --method patch `
            --url $defaultsUrl `
            --resource 'https://cognitiveservices.azure.com/' `
            --headers 'Content-Type=application/json' `
            --body $body `
            --only-show-errors 2>&1
        $patchExitCode = $LASTEXITCODE
        $patchError = $patchOutput -join "`n"
        if ($patchExitCode -ne 0 -and $attempt -lt 11) {
            Start-Sleep -Seconds 5
        }
    }
    if ($patchExitCode -ne 0) {
        throw "Content Understanding defaults configuration failed after guarded access propagation: $patchError"
    }

    $configured = az rest `
        --method get `
        --url $defaultsUrl `
        --resource 'https://cognitiveservices.azure.com/' `
        --only-show-errors | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) {
        throw 'Content Understanding defaults verification failed.'
    }
    foreach ($mapping in $defaults.modelDeployments.GetEnumerator()) {
        if ($configured.modelDeployments.($mapping.Key) -ne $mapping.Value) {
            throw 'Content Understanding defaults do not match the reviewed model contract.'
        }
    }
}

function Assert-ContentUnderstandingAnalyzer {
    $account = Get-SingleDeploymentResource `
        -ResourceType 'Microsoft.CognitiveServices/accounts' `
        -Kind 'AIServices'
    $endpoint = "https://$($account.name).services.ai.azure.com"
    $analyzerId = Get-RequiredValue `
        'ContentUnderstandingAnalyzerId' $ContentUnderstandingAnalyzerId
    $analyzerUrl = "$endpoint/contentunderstanding/analyzers/$analyzerId`?api-version=2025-11-01"
    $configured = $null
    $getError = ''
    for ($attempt = 0; $attempt -lt 12 -and $null -eq $configured; $attempt++) {
        $getOutput = az rest `
            --method get `
            --url $analyzerUrl `
            --resource 'https://cognitiveservices.azure.com/' `
            --only-show-errors 2>&1
        $getExitCode = $LASTEXITCODE
        if ($getExitCode -eq 0) {
            $configured = $getOutput | ConvertFrom-Json
        }
        else {
            $getError = $getOutput -join "`n"
            if ($attempt -lt 11) { Start-Sleep -Seconds 5 }
        }
    }

    if ($null -eq $configured) {
        throw "Content Understanding analyzer verification failed after guarded access propagation: $getError"
    }

    if ($null -eq $configured -or $configured.analyzerId -ne $analyzerId) {
        throw 'Content Understanding analyzer identity does not match the reviewed analyzer.'
    }
}

function Set-OperationsEnvironment {
    $identityName = "rag-$DeploymentInstanceId-operations-mi"
    $identity = az identity show `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --name $identityName `
        --output json `
        --only-show-errors | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $identity.tags.DeploymentInstance -ne $DeploymentInstanceId) {
        throw 'The operations identity does not match the reviewed deployment instance.'
    }
    $managedEnvironment = Get-SingleDeploymentResource 'Microsoft.App/managedEnvironments'
    $cosmosAccount = Get-SingleDeploymentResource 'Microsoft.DocumentDB/databaseAccounts'
    $cosmosEndpoint = az cosmosdb show `
        --subscription $SubscriptionId `
        --ids $cosmosAccount.id `
        --query documentEndpoint `
        --output tsv `
        --only-show-errors
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($cosmosEndpoint)) {
        throw 'The Cosmos DB endpoint could not be resolved.'
    }
    $registryName = Get-RequiredValue 'ACR_NAME' $env:ACR_NAME
    $registry = az acr show `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --name $registryName `
        --output json `
        --only-show-errors | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $registry.tags.DeploymentInstance -ne $DeploymentInstanceId) {
        throw 'The container registry does not match the reviewed deployment instance.'
    }
    $jobStem = ("rag-$DeploymentInstanceId" -replace '-', '')
    $jobStem = $jobStem.Substring(0, [Math]::Min(19, $jobStem.Length))
    $env:OPERATIONS_JOB_NAME = "$jobStem-catalog-job"
    $env:MANAGED_ENVIRONMENT_ID = $managedEnvironment.id
    $env:ACR_LOGIN_SERVER = $registry.loginServer
    $env:OPERATIONS_MANAGED_IDENTITY_ID = $identity.id
    $env:OPERATIONS_MANAGED_IDENTITY_CLIENT_ID = $identity.clientId
    $env:COSMOS_ENDPOINT = $cosmosEndpoint.Trim()
}

function Invoke-InfrastructurePhase {
    param([bool]$Serving, [bool]$Operations)
    Assert-RequiredEnvironment
    Set-ParameterEnvironment
    $env:DEPLOY_SERVING = if ($Serving) { 'true' } else { 'false' }
    $env:DEPLOY_OPERATIONS = if ($Operations) { 'true' } else { 'false' }
    if ($Serving -or $Operations) {
        if ($env:RETRIEVAL_IMAGE_REFERENCE -notmatch '^[a-z0-9.-]+/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$') {
            throw 'RETRIEVAL_IMAGE_REFERENCE must be repository@sha256:<64 lowercase hex>.'
        }
        if ($Operations) { Assert-CatalogInitialization }
    }
    else {
        $env:RETRIEVAL_IMAGE_REFERENCE = ''
        $env:RETRIEVAL_CATALOG_DIGEST = ''
    }
    if ($Serving) {
        if ($CatalogOperation -ne 'verify-catalog') {
            throw 'Serving deployment requires a read-only verify-catalog execution.'
        }
        Test-CatalogJob | Out-Null
    }

    az deployment group what-if `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --template-file $TemplatePath `
        --parameters $ParameterPath `
        --result-format FullResourcePayloads `
        --no-pretty-print `
        --only-show-errors
    if ($LASTEXITCODE -ne 0) { throw 'Infrastructure preview failed.' }
    if (-not $Execute) {
        if ($Serving -and $ContentUnderstandingEnabled) {
            [ordered]@{
                action           = 'preview'
                dataPlaneActions = @(
                    'configure-content-understanding-defaults'
                    'verify-content-understanding-analyzer'
                )
            } | ConvertTo-Json -Compress
        }
        return
    }
    if ($Serving -and $ContentUnderstandingEnabled) {
        $temporaryClientIp = Get-TemporaryContentUnderstandingClientIp
        if ([string]::IsNullOrEmpty($temporaryClientIp)) {
            Set-ContentUnderstandingDefaults
            Assert-ContentUnderstandingAnalyzer
        }
        else {
            try {
                Set-ContentUnderstandingNetworkAccess -AllowedIpAddress $temporaryClientIp
                Set-ContentUnderstandingDefaults
                Assert-ContentUnderstandingAnalyzer
            }
            finally {
                Set-ContentUnderstandingNetworkAccess -AllowedIpAddress ''
            }
        }
    }

    az deployment group create `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --template-file $TemplatePath `
        --parameters $ParameterPath `
        --mode Incremental `
        --only-show-errors
    if ($LASTEXITCODE -ne 0) { throw 'Infrastructure deployment failed.' }
}

function Invoke-OperationsInfrastructure {
    Assert-RequiredEnvironment
    Set-ParameterEnvironment
    if ($env:RETRIEVAL_IMAGE_REFERENCE -notmatch '^[a-z0-9.-]+/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$') {
        throw 'RETRIEVAL_IMAGE_REFERENCE must be repository@sha256:<64 lowercase hex>.'
    }
    Assert-CatalogInitialization
    Set-OperationsEnvironment
    az deployment group what-if `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --template-file $OperationsTemplatePath `
        --parameters $OperationsParameterPath `
        --result-format FullResourcePayloads `
        --no-pretty-print `
        --only-show-errors
    if ($LASTEXITCODE -ne 0) { throw 'Operations job preview failed.' }
    if (-not $Execute) { return }

    az deployment group create `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --template-file $OperationsTemplatePath `
        --parameters $OperationsParameterPath `
        --mode Incremental `
        --only-show-errors
    if ($LASTEXITCODE -ne 0) { throw 'Operations job deployment failed.' }
}

function Invoke-ImageBuild {
    $registryName = Get-RequiredValue 'ACR_NAME' $env:ACR_NAME
    $buildId = Get-RequiredValue 'RELEASE_BUILD_ID' $env:RELEASE_BUILD_ID
    if ($buildId -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$') { throw 'RELEASE_BUILD_ID is invalid.' }
    if (-not $Execute) {
        [ordered]@{ action = 'preview'; image = "rag-retrieval:$buildId" } | ConvertTo-Json -Compress
        return
    }
    az acr build --registry $registryName --image "rag-retrieval:$buildId" --file app/retrieval/Dockerfile app --only-show-errors
    if ($LASTEXITCODE -ne 0) { throw 'ACR build failed.' }
    $digest = az acr repository show --name $registryName --image "rag-retrieval:$buildId" --query digest --output tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or $digest -notmatch '^sha256:[a-f0-9]{64}$') { throw 'ACR did not return an immutable digest.' }
    $loginServer = az acr show --name $registryName --query loginServer --output tsv --only-show-errors
    [ordered]@{ imageReference = "$loginServer/rag-retrieval@$digest"; buildId = $buildId } | ConvertTo-Json -Compress
}

function Get-ReviewedCatalogDigest {
    $catalogPath = Join-Path $ProjectRoot $CatalogFile
    $output = & python @(
        (Join-Path $ProjectRoot 'tools/publish_retrieval_catalog.py'),
        'validate',
        '--file', $catalogPath,
        '--deployment-instance-id', $DeploymentInstanceId
    )
    if ($LASTEXITCODE -ne 0) { throw 'Catalog validation failed.' }
    $catalog = $output | ConvertFrom-Json
    if ($catalog.catalogDigest -notmatch '^sha256:[a-f0-9]{64}$') {
        throw 'Catalog validation did not return an immutable digest.'
    }
    return $catalog.catalogDigest
}

function Get-OperationsJobName {
    $names = @(az containerapp job list `
            --subscription $SubscriptionId `
            --resource-group $ResourceGroup `
            --query "[?tags.DeploymentInstance=='$DeploymentInstanceId'].name" `
            --output tsv `
            --only-show-errors)
    if ($LASTEXITCODE -ne 0 -or $names.Count -ne 1 -or [string]::IsNullOrWhiteSpace($names[0])) {
        throw "Expected exactly one private operations job for this deployment instance; found $($names.Count)."
    }
    return $names[0].Trim()
}

function Assert-CatalogContainer {
    param($Containers)
    if ($env:RETRIEVAL_IMAGE_REFERENCE -notmatch '^[a-z0-9.-]+/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$') {
        throw 'RETRIEVAL_IMAGE_REFERENCE must be repository@sha256:<64 lowercase hex>.'
    }
    $containers = @($Containers)
    if ($containers.Count -ne 1) { throw 'Expected one private catalog container.' }
    $container = $containers[0]
    if ($container.name -ne 'catalog-publisher' -or
        $container.image -cne $env:RETRIEVAL_IMAGE_REFERENCE -or
        ($container.command -join '|') -cne 'python' -or
        ($container.args -join '|') -cne "-m|retrieval.operations|$CatalogOperation") {
        throw 'Private catalog image or operation does not match the reviewed candidate.'
    }
    $partitions = @($container.env | Where-Object { $_.name -eq 'DEPLOYMENT_INSTANCE_ID' })
    if ($partitions.Count -ne 1 -or $partitions[0].value -cne $DeploymentInstanceId) {
        throw 'Private catalog partition does not match the reviewed deployment instance.'
    }
    $expectedSettings = @{
        COSMOS_ENDPOINT            = $env:COSMOS_ENDPOINT
        COSMOS_DATABASE            = 'rag-db'
        RETRIEVAL_CONFIG_CONTAINER = 'retrieval-config'
        MANAGED_IDENTITY_CLIENT_ID = $env:OPERATIONS_MANAGED_IDENTITY_CLIENT_ID
    }
    foreach ($name in $expectedSettings.Keys) {
        $values = @($container.env | Where-Object { $_.name -ceq $name })
        if ([string]::IsNullOrWhiteSpace($expectedSettings[$name]) -or
            $values.Count -ne 1 -or $values[0].value -cne $expectedSettings[$name]) {
            throw "Private catalog $name does not match the reviewed target."
        }
    }
    if ($CatalogOperation -eq 'publish-catalog') {
        Assert-CatalogInitialization
        $digests = @($container.env | Where-Object { $_.name -eq 'EXPECTED_CATALOG_DIGEST' })
        if ($digests.Count -ne 1 -or $digests[0].value -cne $env:RETRIEVAL_CATALOG_DIGEST) {
            throw 'Private catalog seed does not match the reviewed artifact.'
        }
    }
}

function Get-VerifiedCatalogJob {
    Set-OperationsEnvironment
    $jobName = Get-OperationsJobName
    $job = az containerapp job show `
        --subscription $SubscriptionId --resource-group $ResourceGroup --name $jobName `
        --query '{name:name,environmentId:properties.environmentId,identities:identity.userAssignedIdentities,containers:properties.template.containers}' `
        --output json --only-show-errors | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'Private catalog job definition could not be read.' }
    $identityIds = @($job.identities.PSObject.Properties.Name)
    if ($job.name -cne $jobName -or $job.environmentId -ine $env:MANAGED_ENVIRONMENT_ID -or
        $identityIds.Count -ne 1 -or $identityIds[0] -ine $env:OPERATIONS_MANAGED_IDENTITY_ID) {
        throw 'Private catalog job environment or identity does not match the reviewed target.'
    }
    Assert-CatalogContainer $job.containers
    return $jobName
}

function Invoke-CatalogJob {
    Assert-CatalogInitialization
    $jobName = Get-VerifiedCatalogJob
    if (-not $Execute) {
        [ordered]@{ action = 'preview'; jobName = $jobName; operation = $CatalogOperation } | ConvertTo-Json -Compress
        return
    }
    $executionName = az containerapp job start `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --name $jobName `
        --query name `
        --output tsv `
        --only-show-errors
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($executionName)) {
        throw 'Private catalog job failed to start.'
    }
    [ordered]@{ jobName = $jobName; executionName = $executionName.Trim(); operation = $CatalogOperation } | ConvertTo-Json -Compress
}

function Test-CatalogJob {
    $executionName = Get-RequiredValue 'JobExecutionName' $JobExecutionName
    if ($executionName -notmatch '^[a-z0-9-]+$') { throw 'JobExecutionName is invalid.' }
    $jobName = Get-VerifiedCatalogJob
    $execution = az containerapp job execution show `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --name $jobName `
        --job-execution-name $executionName `
        --query '{id:id,name:name,status:properties.status,startTime:properties.startTime,endTime:properties.endTime,containers:properties.template.containers}' `
        --output json `
        --only-show-errors | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'Private catalog job execution could not be read.' }
    $expectedExecutionId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.App/jobs/$jobName/executions/$executionName"
    if ($execution.id -ine $expectedExecutionId) {
        throw 'Private catalog execution resource does not match the reviewed target job.'
    }
    if ($execution.name -cne $executionName -or $execution.status -ne 'Succeeded') {
        throw "Private catalog job has not succeeded; current status: $($execution.status)."
    }
    Assert-CatalogContainer $execution.containers
    $startTime = ([DateTimeOffset]::Parse($execution.startTime)).ToUniversalTime()
    $endTime = ([DateTimeOffset]::Parse($execution.endTime)).ToUniversalTime()
    if ($endTime -lt $startTime) { throw 'Private catalog execution timestamps are inconsistent.' }
    $managedEnvironment = Get-SingleDeploymentResource 'Microsoft.App/managedEnvironments'
    $logConfiguration = az containerapp env show `
        --subscription $SubscriptionId --resource-group $ResourceGroup --name $managedEnvironment.name `
        --query '{defaultDomain:properties.defaultDomain,workspaceId:properties.appLogsConfiguration.logAnalyticsConfiguration.customerId}' `
        --output json --only-show-errors | ConvertFrom-Json
    $workspaceGuid = [guid]::Empty
    if ($LASTEXITCODE -ne 0 -or -not [guid]::TryParse([string]$logConfiguration.workspaceId, [ref]$workspaceGuid)) {
        throw 'Private catalog log workspace could not be resolved.'
    }
    if ($logConfiguration.defaultDomain -cnotmatch '^[a-z0-9-]+\.[a-z0-9.-]+$') {
        throw 'Private catalog log environment could not be resolved.'
    }
    $environmentName = $logConfiguration.defaultDomain.Split('.')[0]
    $query = @"
ContainerAppConsoleLogs_CL
| where EnvironmentName_s == '$environmentName'
| where ContainerGroupName_s startswith '$executionName-'
| where ContainerName_s == 'catalog-publisher'
| where ContainerImage_s == '$env:RETRIEVAL_IMAGE_REFERENCE'
| where TimeGenerated between (datetime($($startTime.AddMinutes(-1).ToString('o'))) .. datetime($($endTime.AddMinutes(1).ToString('o'))))
| extend result = parse_json(Log_s)
| where result.status == 'succeeded' and result.operation == '$CatalogOperation'
| where result.catalogId == 'runtime-catalog'
| project catalogDigest = tostring(result.catalogDigest), catalogEtag = tostring(result.catalogEtag)
| distinct catalogDigest, catalogEtag
| take 2
"@
    $observations = @(az monitor log-analytics query --workspace $workspaceGuid.ToString() `
            --analytics-query ($query -replace '\r?\n', ' ') `
            --timespan "$($startTime.AddMinutes(-1).ToString('o'))/$($endTime.AddMinutes(1).ToString('o'))" `
            --output json --only-show-errors | ConvertFrom-Json)
    if ($LASTEXITCODE -ne 0 -or $observations.Count -ne 1 -or
        $observations[0].catalogDigest -notmatch '^sha256:[a-f0-9]{64}$' -or
        [string]::IsNullOrWhiteSpace($observations[0].catalogEtag)) {
        throw 'Private catalog result is missing, invalid or conflicting; retry verification after log ingestion.'
    }
    [ordered]@{
        executionName  = $executionName
        operation      = $CatalogOperation
        imageReference = $env:RETRIEVAL_IMAGE_REFERENCE
        catalogDigest  = $observations[0].catalogDigest
        catalogEtag    = $observations[0].catalogEtag
    } | ConvertTo-Json -Compress
}

function Remove-OperationsJob {
    $jobName = Get-OperationsJobName
    if (-not $Execute) {
        [ordered]@{ action = 'preview-delete'; jobName = $jobName; deploymentInstanceId = $DeploymentInstanceId } | ConvertTo-Json -Compress
        return
    }
    az containerapp job delete `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --name $jobName `
        --yes `
        --only-show-errors
    if ($LASTEXITCODE -ne 0) { throw 'Temporary operations job cleanup failed.' }
    $remaining = @(az containerapp job list `
            --subscription $SubscriptionId `
            --resource-group $ResourceGroup `
            --query "[?tags.DeploymentInstance=='$DeploymentInstanceId'].name" `
            --output tsv `
            --only-show-errors)
    if ($LASTEXITCODE -ne 0 -or $remaining.Count -ne 0) {
        throw 'Temporary operations job still exists after cleanup.'
    }
    [ordered]@{ action = 'deleted'; jobName = $jobName; deploymentInstanceId = $DeploymentInstanceId } | ConvertTo-Json -Compress
}

if ($Phase -notin @('FunctionPackage', 'Function') -and
    ($PSBoundParameters.ContainsKey('FunctionPackagePath') -or
    $PSBoundParameters.ContainsKey('ExpectedFunctionPackageHash'))) {
    throw 'Package arguments are supported only by FunctionPackage and Function phases.'
}
if ($Phase -eq 'FunctionPackage' -and $Execute) {
    throw 'FunctionPackage is local validation only; Execute is not supported.'
}
$authority = Get-Authority
if ($Phase -eq 'Authority') {
    $authority | ConvertTo-Json -Compress
    return
}

Assert-Authority -Authority $authority | Out-Null
if ($Phase -eq 'FunctionPackage') {
    Test-FunctionPackage | ConvertTo-Json -Compress
    return
}
if (
    -not [string]::IsNullOrWhiteSpace($TemporaryContentUnderstandingClientIp) -and
    $Phase -ne 'Final'
) {
    throw 'TemporaryContentUnderstandingClientIp is supported only for the Final phase.'
}
Assert-Target
if ($Phase -eq 'Function') {
    Invoke-FunctionDeployment
    return
}
Import-AzdEnvironment
Set-ParameterEnvironment

switch ($Phase) {
    'Foundation' { Invoke-InfrastructurePhase -Serving $false -Operations $false }
    'Build' { Invoke-ImageBuild }
    'Operations' { Invoke-OperationsInfrastructure }
    'Catalog' { Invoke-CatalogJob }
    'CatalogVerify' { Test-CatalogJob }
    'OperationsCleanup' { Remove-OperationsJob }
    'Final' { Invoke-InfrastructurePhase -Serving $true -Operations $false }
}
