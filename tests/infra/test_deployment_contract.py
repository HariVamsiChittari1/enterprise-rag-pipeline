from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import zipfile

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = PROJECT_ROOT / "scripts" / "deploy.ps1"
MAIN_PARAMETERS = PROJECT_ROOT / "infra" / "main.parameters.bicepparam"
OPERATIONS_PARAMETERS = PROJECT_ROOT / "infra" / "operations.parameters.bicepparam"
FUNCTION_IGNORE = PROJECT_ROOT / "app" / ".funcignore"
CONTENT_UNDERSTANDING_MODULE = PROJECT_ROOT / "infra" / "modules" / "content-understanding.bicep"
CONTENT_UNDERSTANDING_ACCESS_PARAMETERS = (
    PROJECT_ROOT / "infra" / "content-understanding-access.parameters.bicepparam"
)


@pytest.mark.parametrize("parameters", [MAIN_PARAMETERS, OPERATIONS_PARAMETERS])
@pytest.mark.parametrize("include_citations", [None, "false"])
def test_parameter_sources_compile_with_isolated_synthetic_inputs(
    parameters: Path, include_citations: str | None,
) -> None:
    azure_cli = shutil.which("az")
    assert azure_cli is not None, "Azure CLI with Bicep is required for parameter compilation"
    source = parameters.read_text(encoding="utf-8")
    environment = os.environ.copy()
    settings = re.findall(r"readEnvironmentVariable\(\s*'([^']+)'\s*([,)])", source)
    for name, separator in settings:
        environment.pop(name, None)
        if separator == ")":
            environment[name] = "synthetic-input"
    environment.update({
        "DEPLOYMENT_INSTANCE_ID": "synthetic",
        "AZURE_LOCATION": "eastus2",
        "RETRIEVAL_IMAGE_REFERENCE": "registry.example/app@sha256:" + "a" * 64,
    })
    if include_citations is not None:
        environment["INCLUDE_CITATIONS"] = include_citations
    result = subprocess.run(
        [azure_cli, "bicep", "build-params", "--file", str(parameters), "--stdout"],
        env=environment, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    compiled = json.loads(result.stdout)
    values = json.loads(compiled["parametersJson"])["parameters"]
    assert values["catalogOperation"]["value"] == "verify-catalog"
    if parameters == MAIN_PARAMETERS:
        assert values["retrievalCatalogPollSeconds"]["value"] == 7200
        assert values["includeCitations"]["value"] is (include_citations != "false")


def test_deployment_controller_has_no_implicit_or_destructive_target() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    parameters = MAIN_PARAMETERS.read_text(encoding="utf-8")
    required_environment = source[
        source.index("function Assert-RequiredEnvironment"):source.index("function Import-AzdEnvironment")
    ]

    assert "[Parameter(Mandatory)]" in source
    assert "[switch]$Execute" in source
    assert "az deployment group what-if" in source
    assert source.index("az deployment group what-if") < source.index("az deployment group create")
    assert "--mode Incremental" in source
    assert "az group create" not in source
    assert "azd down" not in source
    assert "az ad " not in source
    assert "az containerapp update" not in source
    assert "RETRIEVAL_IMAGE_NAME" not in source
    assert "rag-dev-webhook-secret" not in source
    assert "[string]$ResourceGroup," in source
    assert "[string]$ResourceGroup =" not in source
    assert "existing development environment" in source
    assert "repository@sha256:<64 lowercase hex>" in source
    assert "sha256:<64 lowercase hex>" in source
    assert "'Operations'" in source
    assert "'CatalogVerify'" in source
    assert "'OperationsCleanup'" in source
    assert "az containerapp job start" in source
    assert "az containerapp job execution show" in source
    assert "az containerapp job delete" in source
    assert "Invoke-OperationsInfrastructure" in source
    assert "$OperationsTemplatePath" in source
    assert "$OperationsParameterPath" in source
    assert "Get-SingleDeploymentResource" in source
    assert "az network vnet list" in source
    assert "az network vnet subnet show" in source
    assert "networkSecurityGroup.id" in source
    assert "Unable to verify reviewed virtual network state" in source
    assert "Unable to preserve network security group association" in source
    assert "SUBNET_FUNCTION_INTEGRATION_NSG_ID" in source
    assert "SUBNET_PRIVATE_ENDPOINTS_NSG_ID" in source
    assert "SUBNET_ACA_ENVIRONMENT_NSG_ID" in source
    assert "[bool]$AclEnabled = $true" in source
    assert "$env:ACL_ENABLED = $AclEnabled.ToString().ToLowerInvariant()" in source
    assert "param aclEnabled = readEnvironmentVariable('ACL_ENABLED', 'true') == 'true'" in parameters
    assert "[bool]$IncludeCitations = $true" in source
    assert "$env:INCLUDE_CITATIONS = $IncludeCitations.ToString().ToLowerInvariant()" in source
    assert "includeCitations: includeCitations" in (PROJECT_ROOT / "infra" / "main.bicep").read_text(encoding="utf-8")
    assert "'Final' { Invoke-InfrastructurePhase -Serving $true -Operations $false }" in source
    assert "Expected exactly one private operations job" in source
    assert "publish_retrieval_catalog.py" in source
    assert "'SHAREPOINT_SITE_URL'" in required_environment


def test_final_phase_guards_and_verifies_content_understanding_defaults() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    defaults = source[
        source.index("function Set-ContentUnderstandingDefaults"):
        source.index("function Assert-ContentUnderstandingAnalyzer")
    ]
    infrastructure = source[
        source.index("function Invoke-InfrastructurePhase"):
        source.index("function Invoke-OperationsInfrastructure")
    ]

    assert "-Kind 'AIServices'" in defaults
    assert "'gpt-5.2'                           = 'cu-gpt-5-2'" in defaults
    assert "'text-embedding-3-large'            = 'cu-text-embedding-3-large'" in defaults
    assert "'prebuilt-analyzer-completion'      = 'cu-gpt-5-2'" in defaults
    assert "prebuilt-analyzer-completion-mini' = 'cu-gpt-5-2'" in defaults
    assert "prebuilt-analyzer-embedding'       = 'cu-text-embedding-3-large'" in defaults
    assert ".Replace('\"', '\\\"')" in defaults
    assert "for ($attempt = 0; $attempt -lt 12 -and $patchExitCode -ne 0; $attempt++)" in defaults
    assert "Start-Sleep -Seconds 5" in defaults
    assert "after guarded access propagation: $patchError" in defaults
    assert "contentunderstanding/defaults?api-version=2025-11-01" in defaults
    assert defaults.index("--method patch") < defaults.index("--method get")
    assert defaults.count("--resource 'https://cognitiveservices.azure.com/'") == 2
    assert "foreach ($mapping in $defaults.modelDeployments.GetEnumerator())" in defaults
    assert "Content Understanding defaults do not match" in defaults
    assert "listKeys" not in defaults
    assert infrastructure.index("if (-not $Execute)") < infrastructure.index(
        "az deployment group create"
    )
    assert infrastructure.index("Set-ContentUnderstandingDefaults") < infrastructure.index(
        "az deployment group create"
    )
    assert "configure-content-understanding-defaults" in infrastructure
    assert "verify-content-understanding-analyzer" in infrastructure
    assert "if ($Serving -and $ContentUnderstandingEnabled)" in infrastructure


def test_final_phase_verifies_prebuilt_content_understanding_analyzer() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    analyzer = source[
        source.index("function Assert-ContentUnderstandingAnalyzer"):
        source.index("function Set-OperationsEnvironment")
    ]
    infrastructure = source[
        source.index("function Invoke-InfrastructurePhase"):
        source.index("function Invoke-OperationsInfrastructure")
    ]

    assert "[bool]$ContentUnderstandingEnabled = $false" in source
    assert "[string]$ContentUnderstandingAnalyzerId = 'prebuilt-documentSearch'" in source
    parameter_environment = source[
        source.index("function Set-ParameterEnvironment"):
        source.index("function Get-SingleDeploymentResource")
    ]
    assert (
        "$env:CONTENT_UNDERSTANDING_ANALYZER_ID = Get-RequiredValue"
        in parameter_environment
    )
    assert "if ($ContentUnderstandingEnabled)" not in parameter_environment
    assert "analyzers/$analyzerId`?api-version=2025-11-01" in analyzer
    assert "for ($attempt = 0; $attempt -lt 12 -and $null -eq $configured; $attempt++)" in analyzer
    assert "if ($attempt -lt 11) { Start-Sleep -Seconds 5 }" in analyzer
    assert "after guarded access propagation: $getError" in analyzer
    assert "--method post" not in analyzer
    assert analyzer.count("--resource 'https://cognitiveservices.azure.com/'") == 1
    assert "configured.analyzerId -ne $analyzerId" in analyzer
    assert "listKeys" not in analyzer
    assert infrastructure.index("if (-not $Execute)") < infrastructure.index(
        "Assert-ContentUnderstandingAnalyzer"
    )
    assert infrastructure.index("Assert-ContentUnderstandingAnalyzer") < infrastructure.index(
        "az deployment group create"
    )


def test_final_phase_guards_and_restores_temporary_content_understanding_access() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    module = CONTENT_UNDERSTANDING_MODULE.read_text(encoding="utf-8")
    parameters = CONTENT_UNDERSTANDING_ACCESS_PARAMETERS.read_text(encoding="utf-8")
    network_access = source[
        source.index("function Get-TemporaryContentUnderstandingClientIp"):
        source.index("function Set-ContentUnderstandingDefaults")
    ]
    infrastructure = source[
        source.index("function Invoke-InfrastructurePhase"):
        source.index("function Invoke-OperationsInfrastructure")
    ]

    assert "[string]$TemporaryContentUnderstandingClientIp" in source
    assert "[Net.IPAddress]::TryParse" in network_access
    assert "[Net.Sockets.AddressFamily]::InterNetwork" in network_access
    assert "must be a public IPv4 address" in network_access
    assert network_access.index("az deployment group what-if") < network_access.index(
        "az deployment group create"
    )
    assert "Assert-ContentUnderstandingNetworkState -AllowedIpAddress $AllowedIpAddress" in network_access
    assert "$configured.properties.disableLocalAuth -ne $true" in network_access
    assert "$configured.properties.networkAcls.defaultAction -ne 'Deny'" in network_access
    assert "$expectedIpRules = @()" in network_access
    assert "if (-not [string]::IsNullOrEmpty($AllowedIpAddress))" in network_access
    assert "$actualIpRules.Count -eq 0 -or" in network_access
    assert "-not $ipRulesMatch" in network_access
    assert "finally" in infrastructure
    assert "Set-ContentUnderstandingNetworkAccess -AllowedIpAddress $temporaryClientIp" in infrastructure
    assert "Set-ContentUnderstandingNetworkAccess -AllowedIpAddress ''" in infrastructure
    assert infrastructure.index("Set-ContentUnderstandingNetworkAccess -AllowedIpAddress ''") < (
        infrastructure.index("az deployment group create")
    )
    assert "TemporaryContentUnderstandingClientIp is supported only for the Final phase" in source
    assert "param allowedIpAddress string = ''" in module
    assert "defaultAction: 'Deny'" in module
    assert "empty(allowedIpAddress) ? 'Disabled' : 'Enabled'" in module
    assert "ipRules: empty(allowedIpAddress)" in module
    assert "? []" in module
    assert "using './modules/content-understanding.bicep'" in parameters
    assert "CONTENT_UNDERSTANDING_ALLOWED_IP_ADDRESS" in parameters


def test_operations_job_parameters_use_only_reviewed_existing_inputs() -> None:
    source = OPERATIONS_PARAMETERS.read_text(encoding="utf-8")

    assert "using './modules/aca-operations-job.bicep'" in source
    assert "readEnvironmentVariable('RETRIEVAL_IMAGE_REFERENCE')" in source
    assert "readEnvironmentVariable('RETRIEVAL_CATALOG_DIGEST', '')" in source
    assert "readEnvironmentVariable('CATALOG_OPERATION', 'verify-catalog')" in source
    assert "readEnvironmentVariable('MANAGED_ENVIRONMENT_ID')" in source
    assert "readEnvironmentVariable('OPERATIONS_MANAGED_IDENTITY_ID')" in source
    assert "readEnvironmentVariable('COSMOS_ENDPOINT')" in source
    assert "Temporary: 'true'" in source


def test_function_package_excludes_local_python_environments_and_settings() -> None:
    patterns = FUNCTION_IGNORE.read_text(encoding="utf-8").splitlines()

    assert ".venv/" in patterns
    assert ".venv-*/" in patterns
    assert "local.settings.json" in patterns
    assert "operations/" in patterns


@pytest.fixture
def authority_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for name in tuple(os.environ):
        if name.upper().startswith("GIT_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_TERMINAL_PROMPT", "0")
    subprocess.run(
        ["git", "init", "-q", "--object-format=sha1", str(tmp_path)],
        check=True, capture_output=True,
    )
    for name, value in (("core.autocrlf", "false"), ("core.excludesFile", os.devnull)):
        subprocess.run(
            ["git", "config", name, value], cwd=tmp_path, check=True, capture_output=True,
        )
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/deploy.ps1").write_bytes(SCRIPT.read_bytes())
    (tmp_path / "sample.txt").write_bytes(b"synthetic source\n")
    (tmp_path / ".gitignore").write_bytes(b".azure/\n")
    (tmp_path / ".azure").mkdir()
    (tmp_path / ".azure/deployment-plan.md").write_bytes(
        b"Plan ID: `aca-greenfield-retrieval-v1`\n\nSynthetic unit-test plan.\n"
    )
    subprocess.run(
        ["git", "add", "--", ".gitignore", "sample.txt", "scripts/deploy.ps1"],
        cwd=tmp_path, check=True, capture_output=True,
    )
    return tmp_path


def _run_authority(project: Path) -> subprocess.CompletedProcess[str]:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is required for deployment controller tests")
    return subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command",
         "function az { throw 'Unexpected Azure call' }; "
         "function azd { throw 'Unexpected azd call' }; "
         "& './scripts/deploy.ps1' -Phase Authority"],
        cwd=project, capture_output=True, text=True, check=False,
    )


@pytest.mark.parametrize(
    ("case", "error"),
    [
        ("valid", None),
        ("digest", "hash does not match"),
        ("missing_hash", "ExpectedFunctionPackageHash is required"),
        ("missing_root", "at its root"),
        ("traversal", "unsafe or duplicate paths"),
        ("duplicate", "unsafe or duplicate paths"),
        ("local_settings", "excluded local or credential artifacts"),
        ("environment", "excluded local or credential artifacts"),
        ("certificate", "excluded local or credential artifacts"),
        ("operations", "standalone operations tooling"),
        ("operations_case", "standalone operations tooling"),
        ("execute", "Execute is not supported"),
        ("wrong_phase", "supported only by FunctionPackage and Function"),
        ("source_hash", "Source tree hash changed"),
        ("plan_hash", "Deployment plan hash changed"),
        ("ancestor_first", "conflicting file and directory paths"),
        ("ancestor_last", "conflicting file and directory paths"),
        ("unreadable", "unreadable or inconsistent entry data"),
        ("empty_root", "at its root"),
        ("symlink", "unsupported filesystem entry types"),
        ("expanded_size", "expanded preflight limit"),
        ("entry_count", "4096-entry preflight limit"),
    ],
)
def test_function_package_preflight_is_local_and_fail_closed(
    authority_project: Path, case: str, error: str | None,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is required for deployment controller tests")
    package = authority_project / ".azure" / "candidate.zip"
    entries = {"host.json": b"{}", "function_app.py": b"pass", "requirements.txt": b"soundfile==0.14.0"}
    if case == "missing_root":
        entries = {f"app/{name}": value for name, value in entries.items()}
    additions = {
        "traversal": "../escape.py", "duplicate": "HOST.JSON",
        "local_settings": "local.settings.json", "environment": "nested/.env.test",
        "certificate": "nested/private.pfx",
        "operations": "operations/storage_probe.py",
        "operations_case": "OPERATIONS/Dockerfile",
    }
    if case in additions:
        entries[additions[case]] = b"synthetic"
    if case == "ancestor_first":
        entries["host.json/child.py"] = b"pass"
    if case == "ancestor_last":
        entries = {"host.json/child.py": b"pass", **entries}
    if case == "empty_root":
        entries["host.json"] = b""
    if case == "entry_count":
        entries.update({f"extra/{index}.py": b"pass" for index in range(4094)})
    with zipfile.ZipFile(package, "w") as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
        if case == "symlink":
            symlink = zipfile.ZipInfo("linked.py")
            symlink.create_system = 3
            symlink.external_attr = 0o120777 << 16
            archive.writestr(symlink, "function_app.py")
    if case == "unreadable":
        payload = bytearray(package.read_bytes())
        payload[:4] = b"BAD!"
        package.write_bytes(payload)
    if case == "expanded_size":
        payload = bytearray(package.read_bytes())
        directory_offset = payload.index(b"PK\x01\x02")
        struct.pack_into("<I", payload, directory_offset + 24, 256 * 1024 * 1024 + 1)
        package.write_bytes(payload)
    authority_result = _run_authority(authority_project)
    assert authority_result.returncode == 0, authority_result.stderr
    authority = json.loads(authority_result.stdout)
    digest = hashlib.sha256(package.read_bytes()).hexdigest()
    command = (
        "function az { throw 'Unexpected Azure call' }; "
        "function azd { throw 'Unexpected azd call' }; "
        "& './scripts/deploy.ps1' "
        f"-Phase {'Build' if case == 'wrong_phase' else 'FunctionPackage'} "
        f"-ExpectedPlanHash {'0' * 64 if case == 'plan_hash' else authority['planHash']} "
        f"-ExpectedSourceTreeHash {'0' * 64 if case == 'source_hash' else authority['sourceTreeHash']} "
        "-FunctionPackagePath './.azure/candidate.zip' "
    )
    if case != "missing_hash":
        command += f"-ExpectedFunctionPackageHash {'0' * 64 if case == 'digest' else digest} "
    if case == "execute":
        command += "-Execute"
    result = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
        cwd=authority_project, capture_output=True, text=True, check=False,
    )
    if error is None:
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {
            "action": "validated-package-only", "packageHash": digest, "entryCount": 3,
        }
    else:
        assert result.returncode != 0
        assert error in result.stderr
    assert "Unexpected Azure call" not in result.stderr
    assert "Unexpected azd call" not in result.stderr


def test_authority_mode_is_local_and_machine_readable(authority_project: Path) -> None:
    result = _run_authority(authority_project)

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    entries = []
    for relative in (".gitignore", "sample.txt", "scripts/deploy.ps1"):
        content = (authority_project / relative).read_bytes()
        blob = f"blob {len(content)}\0".encode() + content
        entries.append(f"{relative}\n{hashlib.sha1(blob).hexdigest()}")
    assert payload == {
        "planId": "aca-greenfield-retrieval-v1",
        "planHash": hashlib.sha256(
            (authority_project / ".azure/deployment-plan.md").read_bytes()
        ).hexdigest(),
        "sourceTreeHash": hashlib.sha256("\n".join(entries).encode()).hexdigest(),
    }


def test_authority_hash_changes_with_source(authority_project: Path) -> None:
    before = _run_authority(authority_project)
    (authority_project / "sample.txt").write_bytes(b"changed synthetic source\n")
    after = _run_authority(authority_project)

    assert before.returncode == after.returncode == 0
    first = json.loads(before.stdout)
    second = json.loads(after.stdout)
    assert first["sourceTreeHash"] != second["sourceTreeHash"]
    assert first["planHash"] == second["planHash"]


@pytest.mark.parametrize(
    ("plan_content", "error"),
    [
        (None, "Deployment plan is missing."),
        ("Plan ID: `different-plan`", "Deployment plan ID does not match"),
        ("Plan ID: `aca-greenfield-retrieval-v1`\nexisting development environment",
         "Stale deployment target authority"),
    ],
    ids=["missing", "mismatched", "stale"],
)
def test_authority_rejects_invalid_plan(
    authority_project: Path, plan_content: str | None, error: str,
) -> None:
    plan = authority_project / ".azure/deployment-plan.md"
    if plan_content is None:
        plan.unlink()
    else:
        plan.write_text(plan_content, encoding="utf-8")

    result = _run_authority(authority_project)

    assert result.returncode != 0
    assert error in result.stderr


@pytest.mark.parametrize("protected_only", [False, True], ids=["empty", "excluded-only"])
def test_authority_rejects_empty_source_inventory(
    authority_project: Path, protected_only: bool,
) -> None:
    subprocess.run(
        ["git", "read-tree", "--empty"], cwd=authority_project,
        check=True, capture_output=True,
    )
    (authority_project / ".gitignore").write_bytes(b"*\n")
    if protected_only:
        (authority_project / "data").mkdir()
        (authority_project / "data/synthetic.txt").write_bytes(b"synthetic excluded input\n")
        subprocess.run(
            ["git", "add", "-f", "--", "data/synthetic.txt"],
            cwd=authority_project, check=True, capture_output=True,
        )

    result = _run_authority(authority_project)

    assert result.returncode != 0
    assert "Source inventory is empty." in result.stderr


def _run_controller_functions(body: str) -> subprocess.CompletedProcess[str]:
    powershell = shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is required for deployment controller tests")
    setup = r"""
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Join-Path (Get-Location) 'scripts/deploy.ps1'), [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw ($errors | Out-String) }
foreach ($definition in $ast.FindAll({ param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst]
}, $false)) {
    . ([scriptblock]::Create($definition.Extent.Text))
}
function az { throw 'Unexpected Azure call' }
function azd { throw 'Unexpected azd call' }
$CatalogOperation = 'verify-catalog'
$env:RETRIEVAL_CATALOG_DIGEST = ''
$env:COSMOS_ENDPOINT = 'https://synthetic.documents.azure.com:443/'
$env:OPERATIONS_MANAGED_IDENTITY_CLIENT_ID = 'synthetic-client'
"""
    return subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", setup + body],
        cwd=PROJECT_ROOT, capture_output=True, text=True, check=False,
    )


def test_function_target_uses_explicit_yaml_binding() -> None:
    import yaml

    configuration = yaml.safe_load((PROJECT_ROOT / "azure.yaml").read_text(encoding="utf-8"))
    service = configuration["services"]["rag-functions"]
    assert service["resourceGroup"] == "${FUNCTION_DEPLOY_RESOURCE_GROUP}"
    assert service["resourceName"] == "${FUNCTION_DEPLOY_APP_NAME}"
    assert service["host"] == "function"
    assert service["project"] == "./app"


@pytest.mark.parametrize(
    "case",
    ["valid", "valid_1340", "missing_app", "invalid_app", "version", "missing", "blank", "whitespace",
     "multiple", "subscription", "group", "app"],
)
def test_function_target_fails_closed_without_cloud_calls(case: str) -> None:
    body = r"""
$FunctionAppName = 'synthetic-app'
$SubscriptionId = '11111111-1111-4111-8111-111111111111'
$ResourceGroup = 'synthetic-group'
$AzdEnvironment = 'synthetic-env'
$case = 'CASE'
if ($case -eq 'missing_app') { $FunctionAppName = '' }
if ($case -eq 'invalid_app') { $FunctionAppName = 'app/slots/staging' }
$script:reads = 0
function azd {
    $global:LASTEXITCODE = 0
    if ($args[0] -eq 'version') {
        if ($case -eq 'version') { return 'azd version 0.0.0' }
        if ($case -eq 'valid_1340') { return 'azd version 1.34.0 (commit synthetic) (stable)' }
        return 'azd version 1.34.1 (commit synthetic)'
    }
    if (($args[0..1] -join ' ') -ne 'env get-value' -or
        ($args[3..4] -join ' ') -ne '--environment synthetic-env') { throw 'Unexpected azd call' }
    $script:reads++
    if ($case -eq 'missing') { $global:LASTEXITCODE = 1; return '' }
    if ($case -eq 'blank') { return '' }
    if ($case -eq 'multiple') { return @('first', 'second') }
    switch ($args[2]) {
        'AZURE_SUBSCRIPTION_ID' {
            if ($case -eq 'subscription') { return 'other' }
            return $SubscriptionId
        }
        'FUNCTION_DEPLOY_RESOURCE_GROUP' {
            if ($case -eq 'group') { return 'other' }
            return $ResourceGroup
        }
        'FUNCTION_DEPLOY_APP_NAME' {
            if ($case -eq 'app') { return 'other' }
            if ($case -eq 'whitespace') { return ' synthetic-app ' }
            return 'synthetic-app'
        }
        default { throw 'Unexpected setting' }
    }
}
Assert-FunctionTarget
if ($script:reads -ne 3) { throw 'Bindings were not all checked' }
Write-Output 'verified'
""".replace("CASE", case)
    result = _run_controller_functions(body)
    if case in {"valid", "valid_1340"}:
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "verified"
    else:
        assert result.returncode != 0
        assert any(message in result.stderr for message in (
            "FunctionAppName", "reviewed azd version", "does not match the reviewed Function target",
        ))
    assert "Unexpected Azure call" not in result.stderr
    assert "Unexpected azd call" not in result.stderr


@pytest.mark.parametrize(
    "case", ["preview", "execute", "mismatch", "target", "target_recheck", "replaced",
             "authority", "deploy_failure"],
)
def test_function_candidate_deployment_is_guarded(tmp_path: Path, case: str) -> None:
    package = tmp_path / "candidate.zip"
    with zipfile.ZipFile(package, "w") as archive:
        for name in ("host.json", "function_app.py", "requirements.txt"):
            archive.writestr(name, "synthetic")
    digest = hashlib.sha256(package.read_bytes()).hexdigest()
    body = r"""
$ProjectRoot = (Get-Location).Path
$FunctionPackagePath = 'PACKAGE'
$ExpectedFunctionPackageHash = 'HASH'
$AzdEnvironment = 'synthetic-env'
$case = 'CASE'
$Execute = $case -ne 'preview'
$script:checks = 0
function Assert-FunctionTarget {
    $script:checks++
    if ($case -eq 'target' -or ($case -eq 'target_recheck' -and $script:checks -eq 2)) {
        throw 'target rejected'
    }
}
if ($case -eq 'replaced') {
    $script:originalValidator = ${function:Test-FunctionPackage}
    function Test-FunctionPackage {
        $validated = & $script:originalValidator
        [IO.File]::AppendAllText($FunctionPackagePath, 'synthetic replacement')
        return $validated
    }
}
function Get-Authority { return @{} }
function Assert-Authority {
    param($Authority)
    if ($case -eq 'authority') { throw 'authority rejected' }
}
function azd {
    if ($script:checks -ne 2) { throw 'Missing target recheck' }
    if (($args[0..4] -join '|') -cne 'deploy|rag-functions|--environment|synthetic-env|--no-prompt' -or
        $args.Count -ne 7 -or $args[5] -ne '--from-package' -or $args[6] -ne $FunctionPackagePath) {
        throw 'Unexpected deployment arguments'
    }
    if ((Get-Location).Path -ne $ProjectRoot) { throw 'Wrong project root' }
    if ($null -eq $packageStream -or -not $packageStream.CanRead) { throw 'Missing package read handle' }
    $global:LASTEXITCODE = 0
    if ($case -eq 'deploy_failure') { $global:LASTEXITCODE = 1 }
    Write-Output 'mock-deployed'
}
try { Invoke-FunctionDeployment }
finally {
    $releaseCheck = [IO.File]::Open($FunctionPackagePath, 'Open', 'ReadWrite', 'None')
    $releaseCheck.Dispose()
}
""".replace("PACKAGE", str(package).replace("'", "''")).replace(
        "HASH", "0" * 64 if case == "mismatch" else digest,
    ).replace("CASE", case)
    result = _run_controller_functions(body)
    if case == "preview":
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["packageHash"] == digest
        assert "mock-deployed" not in result.stdout
    elif case == "execute":
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "mock-deployed"
    else:
        assert result.returncode != 0
        expected = {
            "mismatch": "hash does not match", "target": "target rejected",
            "target_recheck": "target rejected", "replaced": "changed after validation",
            "authority": "authority rejected", "deploy_failure": "Function deployment failed",
        }[case]
        assert expected in result.stderr
        if case != "deploy_failure":
            assert "mock-deployed" not in result.stdout


@pytest.mark.parametrize("poll", ["60", "7200", "86400"])
def test_catalog_poll_accepts_inclusive_integer_bounds(poll: str) -> None:
    result = _run_controller_functions(
        f"$env:RETRIEVAL_CATALOG_POLL_SECONDS = '{poll}'; "
        "Set-CatalogEnvironment; Write-Output $env:RETRIEVAL_CATALOG_POLL_SECONDS"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == poll


@pytest.mark.parametrize("poll", ["59", "86401", "60.0", "true", " ", "999999999999"])
def test_catalog_poll_rejects_invalid_explicit_values(poll: str) -> None:
    result = _run_controller_functions(
        f"$env:RETRIEVAL_CATALOG_POLL_SECONDS = '{poll}'; Set-CatalogEnvironment"
    )
    assert result.returncode != 0
    assert "must be an integer" in result.stderr


@pytest.mark.parametrize("phase", ["Function"])
def test_function_dispatch_does_not_import_unrelated_infrastructure(phase: str) -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    dispatch = source[source.index("Assert-Target\nif ($Phase") :]
    result = _run_controller_functions(r"""
$Phase = 'Function'
function Assert-Target { Write-Output 'target-checked' }
function Invoke-FunctionDeployment { Write-Output 'function-dispatched' }
function Import-AzdEnvironment { throw 'Unrelated environment import' }
function Set-ParameterEnvironment { throw 'Unrelated infrastructure setup' }
""".replace("$Phase = 'Function'", f"$Phase = '{phase}'") + dispatch)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["target-checked", "function-dispatched"]


def test_catalog_poll_defaults_only_when_absent() -> None:
    result = _run_controller_functions(
        "Remove-Item Env:RETRIEVAL_CATALOG_POLL_SECONDS -ErrorAction SilentlyContinue; "
        "Set-CatalogEnvironment; Write-Output $env:RETRIEVAL_CATALOG_POLL_SECONDS"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "7200"


@pytest.mark.parametrize("configured", ["absent", "blank"])
def test_azd_poll_import_preserves_explicit_process_value_or_rejects_blank(configured: str) -> None:
    body = r"""
$AzdEnvironment = 'synthetic-environment'
$env:RETRIEVAL_CATALOG_POLL_SECONDS = '90'
function azd {
    $global:LASTEXITCODE = EXIT_CODE
    return ''
}
Import-AzdEnvironment
Set-CatalogEnvironment
Write-Output $env:RETRIEVAL_CATALOG_POLL_SECONDS
""".replace("EXIT_CODE", "1" if configured == "absent" else "0")
    result = _run_controller_functions(body)
    if configured == "absent":
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "90"
    else:
        assert result.returncode != 0
        assert "must not be blank" in result.stderr


@pytest.mark.parametrize("operation", ["verify-catalog", "publish-catalog"])
def test_final_preview_requires_read_only_verification_without_mutation(operation: str) -> None:
    body = r"""
$CatalogOperation = 'OPERATION'
$Execute = $false
$ContentUnderstandingEnabled = $false
$SubscriptionId = 'synthetic-subscription'
$ResourceGroup = 'synthetic-group'
$TemplatePath = 'synthetic-template'
$ParameterPath = 'synthetic-parameters'
$env:RETRIEVAL_IMAGE_REFERENCE = 'registry.example/app@sha256:' + ('a' * 64)
$script:verified = $false
function Assert-RequiredEnvironment {}
function Set-ParameterEnvironment {}
function Test-CatalogJob { $script:verified = $true }
function az {
    if (($args[0..2] -join ' ') -ne 'deployment group what-if' -or -not $script:verified) {
        throw 'Mutation or preview without verification'
    }
    $global:LASTEXITCODE = 0
    Write-Output 'preview-only'
}
Invoke-InfrastructurePhase -Serving $true -Operations $false
""".replace("OPERATION", operation)
    result = _run_controller_functions(body)
    if operation == "verify-catalog":
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "preview-only"
    else:
        assert result.returncode != 0
        assert "requires a read-only" in result.stderr


def test_redeployment_catalog_preview_does_not_read_seed_or_start_job() -> None:
    result = _run_controller_functions(r"""
$Execute = $false
$SubscriptionId = 'synthetic-subscription'
$ResourceGroup = 'synthetic-group'
function Get-ReviewedCatalogDigest { throw 'Seed must not be read' }
function Get-VerifiedCatalogJob { return 'synthetic-job' }
Invoke-CatalogJob
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "action": "preview", "jobName": "synthetic-job", "operation": "verify-catalog",
    }


def test_initialization_rejects_seed_mismatch_before_job_lookup() -> None:
    result = _run_controller_functions(r"""
$CatalogOperation = 'publish-catalog'
$env:RETRIEVAL_CATALOG_DIGEST = 'sha256:' + ('a' * 64)
function Get-ReviewedCatalogDigest { return ('sha256:' + ('b' * 64)) }
function Get-OperationsJobName { throw 'Job must not be looked up' }
Invoke-CatalogJob
""")
    assert result.returncode != 0
    assert "differs from the reviewed catalog file" in result.stderr


@pytest.mark.parametrize("mismatch", ["none", "image", "operation", "partition", "endpoint", "database", "container", "identity"])
def test_catalog_container_checks_actual_execution_identity(mismatch: str) -> None:
    body = r"""
$env:RETRIEVAL_IMAGE_REFERENCE = 'registry.example/app@sha256:' + ('a' * 64)
$DeploymentInstanceId = 'synthetic-instance'
$container = [pscustomobject]@{
    name = 'catalog-publisher'; image = $env:RETRIEVAL_IMAGE_REFERENCE
    command = @('python'); args = @('-m', 'retrieval.operations', 'verify-catalog')
    env = @(
        [pscustomobject]@{ name = 'DEPLOYMENT_INSTANCE_ID'; value = $DeploymentInstanceId }
        [pscustomobject]@{ name = 'COSMOS_ENDPOINT'; value = $env:COSMOS_ENDPOINT }
        [pscustomobject]@{ name = 'COSMOS_DATABASE'; value = 'rag-db' }
        [pscustomobject]@{ name = 'RETRIEVAL_CONFIG_CONTAINER'; value = 'retrieval-config' }
        [pscustomobject]@{ name = 'MANAGED_IDENTITY_CLIENT_ID'; value = $env:OPERATIONS_MANAGED_IDENTITY_CLIENT_ID }
    )
}
"""
    mutations = {
        "none": "",
        "image": "$container.image = 'registry.example/app@sha256:' + ('b' * 64)",
        "operation": "$container.args[2] = 'publish-catalog'",
        "partition": "$container.env[0].value = 'other-instance'",
        "endpoint": "$container.env[1].value = 'https://other.documents.azure.com:443/'",
        "database": "$container.env[2].value = 'other-db'",
        "container": "$container.env[3].value = 'other-container'",
        "identity": "$container.env[4].value = 'other-client'",
    }
    result = _run_controller_functions(body + mutations[mismatch] + "\nAssert-CatalogContainer @($container)")
    if mismatch == "none":
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0
        assert "does not match" in result.stderr


@pytest.mark.parametrize("evidence", ["matching", "missing", "conflicting", "wrong-image", "wrong-job", "wrong-execution", "failed", "missing-domain", "invalid-domain", "invalid-workspace"])
def test_catalog_verification_requires_correlated_result(evidence: str) -> None:
    body = r"""
$SubscriptionId = 'synthetic-subscription'
$ResourceGroup = 'synthetic-group'
$DeploymentInstanceId = 'synthetic-instance'
$JobExecutionName = 'synthetic-job-abc1234'
$env:RETRIEVAL_IMAGE_REFERENCE = 'registry.example/app@sha256:' + ('a' * 64)
$script:evidence = 'EVIDENCE'
function Get-VerifiedCatalogJob { return 'synthetic-job' }
function Get-SingleDeploymentResource { return [pscustomobject]@{ name = 'synthetic-environment' } }
function az {
    $global:LASTEXITCODE = 0
    $command = $args[0..2] -join ' '
    if ($command -eq 'containerapp job execution') {
        $image = $env:RETRIEVAL_IMAGE_REFERENCE
        if ($script:evidence -eq 'wrong-image') { $image = 'registry.example/other@sha256:' + ('b' * 64) }
        $resourceId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.App/jobs/synthetic-job/executions/$JobExecutionName"
        if ($script:evidence -eq 'wrong-job') { $resourceId = $resourceId.Replace('/jobs/synthetic-job/', '/jobs/other-job/') }
        $name = if ($script:evidence -eq 'wrong-execution') { 'other-execution' } else { $JobExecutionName }
        $status = if ($script:evidence -eq 'failed') { 'Failed' } else { 'Succeeded' }
        return (@{
            id = $resourceId; name = $name; status = $status
            startTime = '2026-01-01T00:00:00Z'; endTime = '2026-01-01T00:01:00Z'
            containers = @(@{
                name = 'catalog-publisher'; image = $image; command = @('python')
                args = @('-m', 'retrieval.operations', 'verify-catalog')
                env = @(
                    @{ name = 'DEPLOYMENT_INSTANCE_ID'; value = $DeploymentInstanceId }
                    @{ name = 'COSMOS_ENDPOINT'; value = $env:COSMOS_ENDPOINT }
                    @{ name = 'COSMOS_DATABASE'; value = 'rag-db' }
                    @{ name = 'RETRIEVAL_CONFIG_CONTAINER'; value = 'retrieval-config' }
                    @{ name = 'MANAGED_IDENTITY_CLIENT_ID'; value = $env:OPERATIONS_MANAGED_IDENTITY_CLIENT_ID }
                )
            })
        } | ConvertTo-Json -Depth 8 -Compress)
    }
    if ($command -eq 'containerapp env show') {
        $domain = 'synthetic-log-label.region.azurecontainerapps.io'
        if ($script:evidence -eq 'missing-domain') { $domain = '' }
        if ($script:evidence -eq 'invalid-domain') { $domain = "invalid'log-label.example" }
        $workspace = '11111111-1111-1111-1111-111111111111'
        if ($script:evidence -eq 'invalid-workspace') { $workspace = 'invalid' }
        return (@{ defaultDomain = $domain; workspaceId = $workspace } | ConvertTo-Json -Compress)
    }
    if ($command -eq 'monitor log-analytics query') {
        $query = $args[[array]::IndexOf($args, '--analytics-query') + 1]
        if ($query.Contains("`n") -or $query.Contains("`r")) { throw 'Multiline native query argument' }
        foreach ($expected in @(($JobExecutionName + '-'), $env:RETRIEVAL_IMAGE_REFERENCE, "result.operation == 'verify-catalog'", "EnvironmentName_s == 'synthetic-log-label'")) {
            if (-not $query.Contains($expected)) { throw 'Unscoped log query' }
        }
        if ($script:evidence -eq 'missing') { return '[]' }
        $rows = @(@{ catalogDigest = 'sha256:' + ('c' * 64); catalogEtag = 'observed-etag' })
        if ($script:evidence -eq 'conflicting') { $rows += @{ catalogDigest = 'sha256:' + ('d' * 64); catalogEtag = 'other-etag' } }
        return (ConvertTo-Json -InputObject $rows -Compress)
    }
    throw 'Unexpected Azure operation'
}
Test-CatalogJob
""".replace("EVIDENCE", evidence)
    result = _run_controller_functions(body)
    if evidence == "matching":
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["catalogEtag"] == "observed-etag"
    else:
        assert result.returncode != 0
        assert any(message in result.stderr for message in (
            "does not match", "missing, invalid or conflicting", "has not succeeded",
            "log environment could not be resolved", "log workspace could not be resolved",
        ))


@pytest.mark.parametrize("mismatch", ["none", "environment", "identity", "job"])
def test_catalog_job_checks_discovered_environment_and_attached_identity(mismatch: str) -> None:
    body = r"""
$SubscriptionId = 'synthetic-subscription'
$ResourceGroup = 'synthetic-group'
$env:MANAGED_ENVIRONMENT_ID = '/synthetic/environment'
$env:OPERATIONS_MANAGED_IDENTITY_ID = '/synthetic/identity'
$script:mismatch = 'MISMATCH'
function Set-OperationsEnvironment {}
function Get-OperationsJobName { return 'synthetic-job' }
function Assert-CatalogContainer { param($Containers) }
function az {
    if (($args[0..2] -join ' ') -ne 'containerapp job show') { throw 'Unexpected mutation' }
    $global:LASTEXITCODE = 0
    $environment = if ($script:mismatch -eq 'environment') { '/other/environment' } else { $env:MANAGED_ENVIRONMENT_ID }
    $identity = if ($script:mismatch -eq 'identity') { '/other/identity' } else { $env:OPERATIONS_MANAGED_IDENTITY_ID }
    $name = if ($script:mismatch -eq 'job') { 'other-job' } else { 'synthetic-job' }
    return (@{ name = $name; environmentId = $environment; identities = @{ $identity = @{} }; containers = @() } | ConvertTo-Json -Depth 5)
}
Get-VerifiedCatalogJob
""".replace("MISMATCH", mismatch)
    result = _run_controller_functions(body)
    if mismatch == "none":
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "synthetic-job"
    else:
        assert result.returncode != 0
        assert "does not match the reviewed target" in result.stderr
