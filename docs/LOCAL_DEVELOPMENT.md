# Azure-backed local development setup

This guide is for a developer who is new to the repository and needs the
Azure-backed hybrid development mode. It starts with a clean Windows
workstation and ends with:

1. the Python test suite running locally; and
2. the read-only `inspect_data` Azure Function running on `localhost` while it
   reads the approved Azure development Cosmos DB used by the multi-source
   implementation.

Follow the stages in order. Do not continue when a checkpoint fails.

> [!IMPORTANT]
> This guide deliberately loads only `inspect_data`. It does not load ingestion,
> purge, query, webhook, timer, orchestrator, or activity functions. Those
> functions can change shared Azure development data. Starting or triggering
> them requires a separate, explicit approval.

Azure development Cosmos DB is required because local validation must exercise
the vector and full-text query behavior used by retrieval.

## How the local setup works

| Component | Runs where | Why |
| --- | --- | --- |
| Python application and Functions host | Developer workstation | Runs the code being edited without deployment. |
| Functions host storage | Local Docker Azurite | Keeps local host state separate from the deployed Function App. |
| Cosmos DB application data | Approved Azure development account | Exercises the real Cosmos data plane and indexing behavior. |
| Application authentication | Explicit local service principal | `AZURE_AUTH_MODE=service_principal` selects only the reviewed non-production service principal. Azure CLI is not an application credential. |
| Multi-source registry and application data | Approved Azure development Cosmos DB | Uses the real `ingestion-sources` registry and application containers. |
| SharePoint, Graph, Key Vault, and AI services | Not contacted by this safe startup | The loaded inspection function needs only configuration validation and Cosmos DB. |

Microsoft documents that Azure Functions Core Tools can run Functions locally
and connect them to live Azure services. Azurite is Microsoft's local emulator
for Blob, Queue, and Table storage:

- [Develop and run Azure Functions locally](https://learn.microsoft.com/azure/azure-functions/functions-develop-local)
- [Use the Azurite emulator for local Azure Storage development](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
- [Authenticate Python apps with a local service principal](https://learn.microsoft.com/azure/developer/python/sdk/authentication/local-development-service-principal)

## Before you start

### Workstation prerequisites

Install these tools before cloning the repository.

| Tool | Required version | Installation |
| --- | --- | --- |
| PowerShell | 7 or later | [Install PowerShell](https://learn.microsoft.com/powershell/scripting/install/installing-powershell-on-windows) |
| Git | Current supported version | [Git for Windows](https://git-scm.com/download/win) |
| Python | 3.12 | [Python for Windows](https://www.python.org/downloads/windows/) |
| Azure Functions Core Tools | Version 4 | [Install Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local) |
| Docker Desktop | Current supported version, Linux containers | [Install Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/) |
| Postman | Optional | Only needed if you prefer Postman over PowerShell or `curl`. |

Open a new **PowerShell 7** terminal and run:

```powershell
$PSVersionTable.PSVersion
git --version
py -3.12 --version
func --version
docker --version
```

**Expected outcome:** every command prints a version. Python prints `3.12.x`
and Functions Core Tools prints `4.x`.

**Checkpoint 0:** stop here if a command is missing. Install or repair that tool
before continuing.

### Azure access prerequisites

Ask the development-environment owner for these **non-secret** values:

| Value | Example shape |
| --- | --- |
| Azure tenant ID | GUID |
| Azure subscription ID | GUID |
| SharePoint application client ID | GUID |
| Key Vault URI | `https://<vault>.vault.azure.net/` |
| Cosmos endpoint | `https://<account>.documents.azure.com:443/` |
| Cosmos database name | Commonly `rag-db`; use the supplied value |
| Azure OpenAI endpoint | `https://<account>.openai.azure.com/` |

The local service principal also needs:

- Cosmos DB data-plane read access to the selected database and containers;
- network access to the Cosmos endpoint, through the approved VPN/private DNS
  path or an approved firewall rule; and
- permission to use the selected development environment.

An Azure subscription `Reader` role alone does **not** grant Cosmos data-plane
access. Do not request account keys and do not add broad public network access.
Microsoft documents that an allowed network source and valid authorization are
both required when a Cosmos firewall is enabled:
[Configure an IP firewall for Azure Cosmos DB](https://learn.microsoft.com/azure/cosmos-db/how-to-configure-firewall).

### Environment-owner preparation and handoff

An Entra application owner and Azure resource owner must prepare access before
the developer starts. The developer does not need an Azure CLI or portal login.

1. Create or reuse the single-tenant `developer-local-azure-access` app
   registration and its tenant service principal. It must have no redirect
   URI, exposed API, delegated permission, production assignment, or Azure
   control-plane role.
2. Assign only this project's required non-production data-plane roles from
   the [T24 access contract](BUSINESS_CONTAINER_ROUTING_IMPLEMENTATION_PLAN.md#t24---define-the-solo-development-service-principal-access-contract-p0).
   Omit Graph and Key Vault access unless the approved local test needs them.
3. Create one client secret that expires in less than 12 months. The developer
   may place the value in the Git-ignored `app\local.settings.json` for this
   non-production local workflow. Entra displays the value only once. Do not
   send it by email, chat, ticket, or tracked repository file.
4. Give the developer the tenant ID, application/client ID, and secret-expiry
   date. Keep the service-principal object ID and secret credential/key ID in
   the owner record for role review and revocation; neither is a substitute for
   the client ID.
5. Record which development projects use this generic principal. Its credential
   rotation affects every listed project, while each project's role assignments
   remain independently scoped and removable.

Microsoft recommends a separate app registration per developer and per
application. The generic principal is a user-approved exception for one
developer's non-production projects; if project-scoped assignment review and
coordinated rotation cannot be maintained, stop sharing it and create separate
registrations. T27 requires the application owner, expiry, and scoped role
assignments to be identified. This local-settings exception is not approved
for production or deployed workloads.

Microsoft permits client secrets for local development but considers
certificates or federated credentials more secure and rejects secrets for
production workloads. It limits client secrets to 24 months and recommends
less than 12 months:
[Add and manage app credentials](https://learn.microsoft.com/entra/identity-platform/how-to-add-credentials).
This repository deliberately retains managed identities for Azure-hosted
Function and ACA workloads.

## Stage 1: Clone and open the repository

Clone the repository into an approved source-code folder, then enter it:

```powershell
git clone https://github.com/HariVamsiChittari1/enterprise-rag-pipeline.git
Set-Location .\enterprise-rag-pipeline
```

If the repository is already cloned, open PowerShell 7 in its root folder.

Verify that you are in the correct folder:

```powershell
git status --short
Test-Path .\app\function_app.py
Test-Path .\requirements-dev.txt
```

**Expected outcome:** both `Test-Path` commands print `True`. `git status`
either prints nothing or lists only changes you already know about.

**Checkpoint 1:** do not continue if either path is missing.

## Stage 2: Create the Python environment outside OneDrive

This repository can live in a OneDrive-synchronized folder, but generated
environments and caches must not. The commands below use
`C:\DevEnvs\enterprise-rag-pipeline`.

```powershell
$repo = (Get-Location).Path
.\scripts\initialize-dev-environment.ps1 -CreateVenv
$devRoot = $env:DEV_ARTIFACTS_ROOT
$python = "$devRoot\.venv\Scripts\python.exe"

& $python -m pip install -r .\requirements-dev.txt -r .\app\retrieval\requirements.txt
```

Use `& $python` in later commands; activation is not required.

Verify the runtime imports:

```powershell
& $python -c "import azure.functions, azure.durable_functions, azure.cosmos; print('imports ok')"
```

**Expected outcome:** the final command prints `imports ok`.

**Checkpoint 2:** if installation or import fails, use
[Troubleshooting](#troubleshooting) before continuing.

## Stage 3: Run the automated tests

From the repository root:

```powershell
& $python -m pytest -q
```

**Expected outcome:** pytest ends with no `failed` or `error` entries. The exact
number of tests can change as the repository evolves.

**Checkpoint 3:** a failing suite means the local code or environment is not in
a known-good state. Do not start Azure-connected Functions until it passes.

## Stage 4: Start local Azure Storage with Azurite

Start Docker Desktop and wait until it reports that the engine is running:

```powershell
docker info
```

Create the external data folder:

```powershell
$devRoot = 'C:\DevEnvs\enterprise-rag-pipeline'
New-Item -ItemType Directory -Force "$devRoot\azurite" | Out-Null
```

Check whether this repository's Azurite container already exists:

```powershell
docker ps -a --filter "name=^bfl-rag-local-azurite$" `
  --format "{{.Names}} {{.Status}}"
```

- If nothing prints, create it:

  ```powershell
  docker run -d --name bfl-rag-local-azurite `
    -p 127.0.0.1:10000:10000 `
    -p 127.0.0.1:10001:10001 `
    -p 127.0.0.1:10002:10002 `
    --mount "type=bind,source=$devRoot\azurite,target=/data" `
    mcr.microsoft.com/azure-storage/azurite:latest `
    azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 `
      --tableHost 0.0.0.0 --location /data
  ```

- If it prints `Exited`, start it:

  ```powershell
  docker start bfl-rag-local-azurite
  ```

- If it prints `Up`, leave it running.

Verify all three ports:

```powershell
10000..10002 | ForEach-Object {
  Test-NetConnection 127.0.0.1 -Port $_ -InformationLevel Quiet
}
```

**Expected outcome:** Docker reports the container as `Up`, and the port check
prints `True` three times.

**Checkpoint 4:** do not start the Functions host until all three ports respond.

## Stage 5: Load the approved local application identity

For this approved non-production exception, put the current local-development
secret in `app\local.settings.json`. The file is intentionally plaintext;
OneDrive synchronization and local access can expose it. Do not reuse this
file for production or deployment. The Functions host reads the values and
passes them to the explicit local service-principal credential path.

Never print the secret, dump the process environment, or run the host from a
terminal shared with unrelated work. Verify only that the ignored settings file
exists and that the application starts with `AZURE_AUTH_MODE=service_principal`.

**Expected outcome:** the settings file exists, remains ignored, and the local
application starts in `service_principal` mode.

**Checkpoint 5:** stop if any identifier differs, the secret is absent, or its
recorded expiry has passed. Do not try Azure CLI, VS Code, or another developer
credential as a fallback.

## Stage 6: Create local Function settings

The local host reads `app\local.settings.json`. The file is ignored by Git.
Create it only on your workstation. The service identifiers below are
non-secret, but they must still refer to the approved development environment.
The current centralized configuration validator requires the SharePoint
application, Key Vault, and Azure OpenAI identifiers even though the
read-only route does not contact those services.

```powershell
@'
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "AzureFunctionsJobHost__extensions__durableTask__hubName": "BflRagLocalMultiSourceReadOnly",
    "AZURE_AUTH_MODE": "service_principal",
    "AZURE_TENANT_ID": "<approved-development-tenant-id>",
    "AZURE_CLIENT_ID": "<developer-local-azure-access-client-id>",
    "AZURE_CLIENT_SECRET": "<local-development-client-secret>",
    "SHAREPOINT_TENANT_ID": "<approved-development-tenant-id>",
    "SHAREPOINT_APP_CLIENT_ID": "<approved-sharepoint-application-client-id>",
    "KEY_VAULT_URI": "<approved-development-key-vault-uri>",
    "COSMOS_ENDPOINT": "<approved-development-cosmos-endpoint>",
    "COSMOS_DATABASE_NAME": "<approved-development-database-name>",
    "OPENAI_ENDPOINT": "<approved-development-openai-endpoint>",
    "EXTRACTION_ENABLED": "false",
    "SUMMARY_ENABLED": "false",
    "KEY_PHRASES_ENABLED": "false",
    "ENTITIES_ENABLED": "false",
    "AUDIO_WRITER_ENABLED": "false"
  }
}
'@ | Set-Content -LiteralPath .\app\local.settings.json -Encoding utf8
```

Replace every `<approved-...>` placeholder with the supplied value.

Container names, extraction-provider switches, and ingestion tuning values use
the defaults in `app\config.py` unless an environment-specific override is
required. Do not add those defaults to `local.settings.json`.

Do not add the legacy `INGESTION_SOURCE_ID`,
`SHAREPOINT_ASSIGNED_DRIVE_ID`, or `SHAREPOINT_SITE_URL` values. Multi-source
runtime inventory comes from the `ingestion-sources` Cosmos container.

Do **not** add:

- a Cosmos account key;
- certificate contents;
- `AZURE_CLIENT_ID` or `MANAGED_IDENTITY_CLIENT_ID` copied from the deployed
  Function App; or
- the deployed Function App's Azure Storage or Durable Task Scheduler settings.

T25 constructs `ClientSecretCredential` directly and does not fall back to
Azure CLI, VS Code, Azure PowerShell, Azure Developer CLI, shared token cache,
or broker credentials. The local principal has no Azure control-plane role;
it needs only the project-specific data-plane assignment for the capability
being tested. The local Azurite task hub keeps host state separate from Azure.

Check that the file exists and remains untracked:

```powershell
Test-Path .\app\local.settings.json
git status --short -- .\app\local.settings.json
```

**Expected outcome:** the first command prints `True`; the second prints
nothing.

**Checkpoint 6:** if Git lists the file, stop and confirm that
`local.settings.json` is ignored before continuing.

## Stage 7: Verify Cosmos network and identity access

First confirm that the endpoint host resolves:

```powershell
$settings = Get-Content .\app\local.settings.json -Raw | ConvertFrom-Json
$cosmosEndpoint = [Uri]$settings.Values.COSMOS_ENDPOINT

Resolve-DnsName $cosmosEndpoint.Host
Test-NetConnection $cosmosEndpoint.Host -Port 443
```

**Expected outcome:** DNS returns an address and `TcpTestSucceeded` is `True`.
If the address is private, connect to the approved VPN before retrying.

The first Function request in Stage 9 is the data-plane authorization check. A
`503 cosmos_query_failed` response usually means network or RBAC access is
still missing; see [Troubleshooting](#troubleshooting).

**Checkpoint 7:** do not continue if DNS or TCP connectivity fails.

## Stage 8: Start only the read-only Function

Check that port 7071 is free:

```powershell
Get-NetTCPConnection -State Listen -LocalPort 7071 -ErrorAction SilentlyContinue
```

No output means the port is available. Start the Functions host:

```powershell
$devRoot = 'C:\DevEnvs\enterprise-rag-pipeline'
$env:PATH = "$devRoot\.venv\Scripts;$env:PATH"
$env:VIRTUAL_ENV = "$devRoot\.venv"
$env:PYTHONPYCACHEPREFIX = "$devRoot\pycache"

Set-Location .\app
func start --port 7071 --functions inspect_data
```

Keep this terminal open. Core Tools should list exactly:

```text
Functions:

        inspect_data: [GET] http://localhost:7071/api/ingestion/inspect
```

**Expected outcome:** the host remains running and only `inspect_data` is
listed. Extension-bundle download can make the first start slower.

**Checkpoint 8:** stop the host with `Ctrl+C` if any write function, timer,
webhook, orchestrator, or activity is listed.

## Stage 9: Trigger the Function

Open a second PowerShell 7 terminal. The examples request at most one row
because the response can contain development data. Do not copy response rows
outside the approved team.

Create the local operator header. Core Tools does not run App Service
authentication, so this header emulates only the already-authenticated claims
that Azure supplies to the Function:

```powershell
$tenantId = '<approved-development-tenant-id>'
$claims = @{ claims = @(
  @{ typ = 'tid'; val = $tenantId },
  @{ typ = 'roles'; val = 'Ingestion.Operator' }
) } | ConvertTo-Json -Compress -Depth 3
$principal = [Convert]::ToBase64String(
  [Text.Encoding]::UTF8.GetBytes($claims)
)
$operator = @{ 'X-MS-CLIENT-PRINCIPAL' = $principal }
```

Use this synthetic header only with `localhost`. Never send it to a deployed
endpoint.

### PowerShell

```powershell
$sourceId = '<registered-source-id>'
$uri = "http://localhost:7071/api/ingestion/inspect?container=source-documents&sourceId=$sourceId&limit=1"
$result = Invoke-RestMethod -Method Get -Uri $uri -Headers $operator
$result | Select-Object container, resolvedContainer, count
```

### Command line with curl

```powershell
curl.exe -i -H "X-MS-CLIENT-PRINCIPAL: $principal" `
  "http://localhost:7071/api/ingestion/inspect?container=source-documents&sourceId=$sourceId&limit=1"
```

### Postman

1. Create a new HTTP request.
2. Select `GET`.
3. Enter
   `http://localhost:7071/api/ingestion/inspect?container=source-documents&sourceId=<registered-source-id>&limit=1`.
4. Add header `X-MS-CLIENT-PRINCIPAL` with the `$principal` value created
   above.
5. Select **Send**. Use this header only with `localhost`.

**Expected outcome:** each client receives HTTP `200` and JSON containing:

```json
{
  "container": "source-documents",
  "resolvedContainer": "<registered-source-documents-container>",
  "count": 0,
  "rows": []
}
```

`count` can be greater than zero and `rows` can contain one approved
development record.

Verify input validation:

```powershell
curl.exe -s -o NUL -w "%{http_code}`n" `
  -H "X-MS-CLIENT-PRINCIPAL: $principal" `
  "http://localhost:7071/api/ingestion/inspect?container=not-a-container"
```

**Expected outcome:** the invalid-container request prints `400`.

**Checkpoint 9:** a valid request must return `200`, and the invalid request
must return `400`. The local hybrid read-only setup is complete.

Also omit the header once to verify the authorization boundary:

```powershell
curl.exe -s -o NUL -w "%{http_code}`n" `
  "http://localhost:7071/api/ingestion/inspect?container=source-documents&sourceId=$sourceId"
```

**Expected outcome:** the request without the operator header prints `403`.

## Stage 10: Stop the local environment

In the Functions terminal, press `Ctrl+C`.

Stop Azurite when it is no longer needed:

```powershell
docker stop bfl-rag-local-azurite

'AZURE_AUTH_MODE', 'AZURE_TENANT_ID', 'AZURE_CLIENT_ID',
  'AZURE_CLIENT_SECRET' | ForEach-Object {
    Remove-Item "Env:$_" -ErrorAction SilentlyContinue
  }
```

Do not commit or share `app\local.settings.json`. Delete it when local
development ends or when the credential is rotated. Close the dedicated
terminal after stopping the local host:

```powershell
Test-Path .\app\local.settings.json
```

**Expected outcome:** the Functions process exits and Docker reports
`bfl-rag-local-azurite`; the final command prints `False` when cleanup is
complete.

## Rotate or revoke the local credential

The application owner must rotate before the recorded expiry. Because the app
registration is reused across approved development projects, first inventory
every project that retrieves this credential.

1. Add a new client secret with an expiry below 12 months; do not delete the
   current credential yet.
2. Replace the value in the ignored `app\local.settings.json` without logging
   or sharing either value.
3. Run the T27 smoke check and confirm every local consumer succeeds.
4. Remove the old Entra credential after the replacement succeeds and update
   the expiry reminder.

If a value might have leaked, stop all local processes, clear the four
environment variables, revoke that Entra credential immediately, review the
generic principal's development-resource access, and issue a new credential.
Do not wait for overlap validation when compromise is suspected.

## Loading other Functions

Core Tools accepts HTTP requests from Postman, PowerShell, `curl`, browser-based
clients, or automated tests. Only functions named after `--functions` are
loaded.

This guide intentionally stops at read-only inspection. Before loading another
function:

1. read its contract in [API reference](API_REFERENCE.md);
2. obtain approval for any Azure write or external-service call;
3. add all settings required by [Configuration reference](CONFIGURATION.md);
4. confirm the deployed Function App is stopped so timers and source writers do
   not compete;
5. load only the requested route and its required Durable orchestrators and
   activities; and
6. verify the host's printed function list before sending a request.

Do not run `func start` without an explicit `--functions` list in a shared Azure
development environment. That would load timers, webhooks, purge, and ingestion
functions.

## Troubleshooting

### PowerShell blocks a script

This guide does not require activating the virtual environment. Use the
documented `& $python` commands instead of changing execution policy.

### `py -3.12` is not found

Re-run the Python installer and select the Python launcher option. Open a new
PowerShell terminal and retry `py -3.12 --version`.

### Package installation fails with a long-path or OneDrive error

Confirm the environment and caches are under
`C:\DevEnvs\enterprise-rag-pipeline`, not inside the repository. Remove only
the incomplete `C:\DevEnvs\enterprise-rag-pipeline\.venv` folder, recreate it,
and repeat Stage 2.

### Docker commands cannot connect to the engine

Start Docker Desktop, wait for **Engine running**, confirm Linux-container mode,
and retry `docker info`.

### An Azurite port is already in use

Find the listener:

```powershell
Get-NetTCPConnection -State Listen -LocalPort 10000,10001,10002
```

If the process is an existing approved Azurite instance, use it. Otherwise stop
only the process or container you own, or ask the workstation owner. Do not
terminate processes by name.

### Port 7071 is already in use

Start the host on another port:

```powershell
func start --port 7072 --functions inspect_data
```

Use `http://localhost:7072` in Stage 9.

### Explicit Azure credential configuration fails

Confirm `AZURE_AUTH_MODE` is exactly `service_principal` and the current
process contains nonempty `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and
`AZURE_CLIENT_SECRET`. Re-enter the secret from the approved secret manager if
it expired; do not print it. Azure CLI, VS Code, PowerShell, and cached user
sessions are deliberately ignored.

### The Function returns `503` with `cosmos_query_failed`

The endpoint intentionally hides the underlying Azure error. Check the
Functions terminal, then verify in this order:

1. `COSMOS_ENDPOINT` and `COSMOS_DATABASE_NAME` are correct.
2. The local service principal belongs to the approved tenant and has
   container-scoped Cosmos DB data-plane read access.
3. DNS and TCP checks in Stage 7 pass.
4. The current public IP is approved when the account uses an IP firewall, or
   the approved VPN/private DNS path is connected.

Do not replace identity authentication with a Cosmos key.

### The host lists unexpected Functions

Press `Ctrl+C`. Restart with the exact command:

```powershell
func start --port 7071 --functions inspect_data
```

Do not send requests until the printed list contains only `inspect_data`.

### Postman works differently from the deployed API

The local Core Tools host does not run Azure App Service authentication
(EasyAuth). The local request therefore uses a synthetic
`X-MS-CLIENT-PRINCIPAL` header, while the deployed API requires its documented
Azure authentication and receives that header from the platform. Never expose
the local host to another machine or treat a successful local call as proof
that deployed authentication works.

For production/deployed failures, use the separate
[operations troubleshooting guide](TROUBLESHOOTING.md).
