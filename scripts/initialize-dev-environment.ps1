[CmdletBinding()]
param(
    [switch]$CreateVenv
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$devRoot = 'C:\DevEnvs\enterprise-rag-pipeline'
$venvRoot = Join-Path $devRoot '.venv'
$venvScripts = Join-Path $venvRoot 'Scripts'
$python = Join-Path $venvScripts 'python.exe'
$pytestCache = Join-Path $devRoot 'pytest-cache'

foreach ($path in @(
    $devRoot,
    (Join-Path $devRoot 'pip-cache'),
    (Join-Path $devRoot 'pycache'),
    $pytestCache,
    (Join-Path $devRoot 'ruff-cache'),
    (Join-Path $devRoot 'bicep')
)) {
    New-Item -ItemType Directory -Force -Path $path | Out-Null
}

if ($CreateVenv -and -not (Test-Path -LiteralPath $python)) {
    & py -3.12 -m venv $venvRoot
    if ($LASTEXITCODE -ne 0) {
        throw 'Python virtual environment creation failed.'
    }
}
if (-not (Test-Path -LiteralPath $python)) {
    throw "External Python environment is missing. Re-run with -CreateVenv: $venvRoot"
}

$env:DEV_ARTIFACTS_ROOT = $devRoot
$env:PIP_CACHE_DIR = Join-Path $devRoot 'pip-cache'
$env:PYTHONPYCACHEPREFIX = Join-Path $devRoot 'pycache'
$env:RUFF_CACHE_DIR = Join-Path $devRoot 'ruff-cache'
$env:BICEP_OUTPUT_DIR = Join-Path $devRoot 'bicep'
$env:VIRTUAL_ENV = $venvRoot

$pytestCacheOption = "cache_dir=$($pytestCache.Replace('\', '/'))"
$currentPytestOptions = [string]$env:PYTEST_ADDOPTS
if ($currentPytestOptions -notmatch [regex]::Escape($pytestCacheOption)) {
    $env:PYTEST_ADDOPTS = "$currentPytestOptions -o $pytestCacheOption".Trim()
}

$pathEntries = $env:PATH -split ';'
if ($venvScripts -notin $pathEntries) {
    $env:PATH = "$venvScripts;$env:PATH"
}

[pscustomobject]@{
    DevArtifactsRoot = $env:DEV_ARTIFACTS_ROOT
    Python = $python
    PytestCache = $pytestCache
    RuffCache = $env:RUFF_CACHE_DIR
    BicepOutput = $env:BICEP_OUTPUT_DIR
}
