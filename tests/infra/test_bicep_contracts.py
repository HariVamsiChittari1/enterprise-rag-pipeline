from __future__ import annotations

import json
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
AZURE_CLI = shutil.which("az")


@lru_cache(maxsize=None)
def _compile_bicep(relative_path: str) -> dict[str, Any]:
    if AZURE_CLI is None:
        pytest.skip("Azure CLI is required to compile Bicep contract tests")

    result = subprocess.run(
        [AZURE_CLI, "bicep", "build", "--file", relative_path, "--stdout"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _resources(template: dict[str, Any], resource_type: str) -> list[dict[str, Any]]:
    return [
        resource
        for resource in template["resources"]
        if resource["type"] == resource_type
    ]


def test_cosmos_container_topology_and_partition_keys() -> None:
    content_container_pairs = json.loads(
        (PROJECT_ROOT / "infra/content-container-pairs.json").read_text(
            encoding="utf-8"
        )
    )
    template = _compile_bicep("infra/modules/cosmos.bicep")
    containers = _resources(
        template,
        "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers",
    )
    containers_by_id = {
        container["properties"]["resource"]["id"]: container
        for container in containers
        if not container["properties"]["resource"]["id"].startswith("[")
    }

    assert set(containers_by_id) == {
        "ingestion-runs",
        "source-documents",
        "search-chunks",
        "retrieval-config",
        "service-audit",
        "ingestion-sources",
    }
    assert containers_by_id["ingestion-runs"]["properties"]["resource"][
        "partitionKey"
    ]["paths"] == ["/sourceId"]
    assert containers_by_id["source-documents"]["properties"]["resource"][
        "partitionKey"
    ]["paths"] == ["/sourceRunId"]
    assert containers_by_id["search-chunks"]["properties"]["resource"][
        "partitionKey"
    ]["paths"] == ["/documentKey"]
    assert containers_by_id["retrieval-config"]["properties"]["resource"][
        "partitionKey"
    ]["paths"] == ["/id"]
    assert containers_by_id["ingestion-sources"]["properties"]["resource"][
        "partitionKey"
    ]["paths"] == ["/sourceId"]
    assert containers_by_id["service-audit"]["properties"]["resource"][
        "partitionKey"
    ]["paths"] == ["/id"]

    topology = _compile_bicep("infra/modules/cosmos-content-topology.bicep")
    assert content_container_pairs == [
        {
            "chunksContainer": "dev-business-a-search-chunks",
            "manifestsContainer": "dev-business-a-source-documents",
        },
        {
            "chunksContainer": "dev-business-b-search-chunks",
            "manifestsContainer": "dev-business-b-source-documents",
        },
        {
            "chunksContainer": "dev-common-search-chunks",
            "manifestsContainer": "dev-common-source-documents",
        },
    ]
    topology_source = (
        PROJECT_ROOT / "infra/modules/cosmos-content-topology.bicep"
    ).read_text(encoding="utf-8")
    assert (
        "var contentContainerPairs = "
        "loadJsonContent('../content-container-pairs.json')"
    ) in topology_source
    copied_containers = {
        container["copy"]["name"]: container
        for container in _resources(
            topology,
            "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers",
        )
        if "copy" in container
    }
    manifests = copied_containers["manifestsContainers"]
    chunks = copied_containers["chunksContainers"]
    assert manifests["copy"]["count"] == (
        "[length(variables('contentContainerPairs'))]"
    )
    assert manifests["properties"]["resource"]["partitionKey"]["paths"] == [
        "/sourceRunId"
    ]
    assert chunks["copy"]["count"] == (
        "[length(variables('contentContainerPairs'))]"
    )
    assert chunks["properties"]["resource"]["partitionKey"]["paths"] == [
        "/documentKey"
    ]
    assert chunks["properties"]["resource"]["indexingPolicy"]["vectorIndexes"] == [
        {"path": "/embedding", "type": "diskANN"}
    ]
    assert chunks["properties"]["resource"]["indexingPolicy"]["fullTextIndexes"] == [
        {"path": "/content"},
        {"path": "/searchableText"},
    ]
    assert chunks["properties"]["resource"]["vectorEmbeddingPolicy"][
        "vectorEmbeddings"
    ][0] == {
        "path": "/embedding",
        "dataType": "float32",
        "dimensions": 3072,
        "distanceFunction": "cosine",
    }
    assert chunks["properties"]["resource"]["fullTextPolicy"]["fullTextPaths"] == [
        {"path": "/content", "language": "en-US"},
        {"path": "/searchableText", "language": "en-US"},
    ]
    main_template = _compile_bicep("infra/main.bicep")
    assert "cosmosContentContainerPairs" in main_template["outputs"]


def test_content_cosmos_roles_are_pair_scoped_and_least_privileged() -> None:
    content_container_pairs = json.loads(
        (PROJECT_ROOT / "infra/content-container-pairs.json").read_text(
            encoding="utf-8"
        )
    )
    content_rbac = _compile_bicep("infra/modules/cosmos-content-rbac.bicep")
    assignments = _resources(
        content_rbac,
        "Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments",
    )

    assert content_rbac["variables"]["copy"] == [
        {
            "name": "contentContainerNamesByPair",
            "count": "[length(variables('contentContainerPairs'))]",
            "input": (
                "[createArray(variables('contentContainerPairs')[copyIndex("
                "'contentContainerNamesByPair')].chunksContainer, "
                "variables('contentContainerPairs')[copyIndex("
                "'contentContainerNamesByPair')].manifestsContainer)]"
            ),
        }
    ]
    assert content_rbac["variables"]["contentContainerNames"] == (
        "[flatten(variables('contentContainerNamesByPair'))]"
    )
    assert len(content_container_pairs) == 3
    assert len(assignments) == 2

    contributors = [
        assignment
        for assignment in assignments
        if "content-contributor" in assignment["name"]
    ]
    readers = [
        assignment
        for assignment in assignments
        if "content-reader" in assignment["name"]
    ]
    assert len(contributors) == 1
    assert len(readers) == 1

    for assignment, role_variable, expected_role_id, principal_name in (
        (
            contributors[0],
            "dataContributorRoleId",
            "00000000-0000-0000-0000-000000000002",
            "functionPrincipalId",
        ),
        (
            readers[0],
            "dataReaderRoleId",
            "00000000-0000-0000-0000-000000000001",
            "retrievalPrincipalId",
        ),
    ):
        assert assignment["copy"]["count"] == (
            "[length(variables('contentContainerNames'))]"
        )
        assert content_rbac["variables"][role_variable] == expected_role_id
        assert f"variables('{role_variable}')" in assignment["properties"][
            "roleDefinitionId"
        ]
        assert assignment["properties"]["principalId"] == (
            f"[parameters('{principal_name}')]"
        )
        assert "contentContainerNames" in assignment["properties"]["scope"]
        assert "colls/{2}" in assignment["properties"]["scope"]

    main_source = (PROJECT_ROOT / "infra/main.bicep").read_text(encoding="utf-8")
    assert "module contentRbac './modules/cosmos-content-rbac.bicep'" in main_source
    parameters_source = (
        PROJECT_ROOT / "infra/content-rbac.parameters.bicepparam"
    ).read_text(encoding="utf-8")
    assert "contentContainerPairs" not in parameters_source
    assert main_source.count("contentContainerPairs:") == 0
    assert (
        PROJECT_ROOT / "infra/modules/cosmos-content-topology.bicep"
    ).read_text(encoding="utf-8").count(
        "loadJsonContent('../content-container-pairs.json')"
    ) == 1
    assert (
        PROJECT_ROOT / "infra/modules/cosmos-content-rbac.bicep"
    ).read_text(encoding="utf-8").count(
        "loadJsonContent('../content-container-pairs.json')"
    ) == 1


def test_ingestion_runs_indexes_order_by_paths() -> None:
    # Real Cosmos rejects ORDER BY on an excluded path; the emulator does not enforce this.
    template = _compile_bicep("infra/modules/cosmos.bicep")
    containers = _resources(
        template,
        "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers",
    )
    ingestion_runs = next(
        container for container in containers
        if container["properties"]["resource"]["id"] == "ingestion-runs"
    )
    included = {
        entry["path"]
        for entry in ingestion_runs["properties"]["resource"]["indexingPolicy"]["includedPaths"]
    }

    assert {"/sourceId/?", "/itemId/?", "/startedAt/?"} <= included


def test_cosmos_search_policy_and_throughput_ownership() -> None:
    template = _compile_bicep("infra/modules/cosmos.bicep")
    containers = _resources(
        template,
        "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers",
    )
    containers_by_id = {
        container["properties"]["resource"]["id"]: container
        for container in containers
    }
    search_chunks = containers_by_id["search-chunks"]
    search_resource = search_chunks["properties"]["resource"]

    assert search_resource["vectorEmbeddingPolicy"] == {
        "vectorEmbeddings": [
            {
                "path": "/embedding",
                "dataType": "float32",
                "dimensions": 3072,
                "distanceFunction": "cosine",
            }
        ]
    }
    assert search_resource["fullTextPolicy"] == {
        "defaultLanguage": "en-US",
        "fullTextPaths": [
            {"path": "/content", "language": "en-US"},
            {"path": "/searchableText", "language": "en-US"},
        ],
    }
    assert search_resource["indexingPolicy"]["vectorIndexes"] == [
        {"path": "/embedding", "type": "diskANN"}
    ]
    assert search_resource["indexingPolicy"]["fullTextIndexes"] == [
        {"path": "/content"},
        {"path": "/searchableText"},
    ]
    included_paths = {
        path["path"] for path in search_resource["indexingPolicy"]["includedPaths"]
    }
    assert "/isRetrievable/?" in included_paths
    assert "/lifecycleGeneration/?" in included_paths
    account = _resources(template, "Microsoft.DocumentDB/databaseAccounts")[0]
    assert account["apiVersion"] == "2026-03-15"
    assert account["properties"]["consistencyPolicy"]["defaultConsistencyLevel"] == "Strong"
    assert containers_by_id["source-documents"]["properties"]["resource"][
        "indexingPolicy"
    ]["compositeIndexes"] == [
        [
            {"path": "/status", "order": "ascending"},
            {"path": "/discoveryOrdinal", "order": "ascending"},
        ]
    ]
    source_document_paths = {
        path["path"]
        for path in containers_by_id["source-documents"]["properties"][
            "resource"
        ]["indexingPolicy"]["includedPaths"]
    }
    assert "/recordType/?" in source_document_paths

    database = _resources(
        template, "Microsoft.DocumentDB/databaseAccounts/sqlDatabases"
    )[0]
    assert "metadataAutoscaleMaxThroughput" in database["properties"]
    assert "searchChunksAutoscaleMaxThroughput" not in database["properties"]
    assert "searchChunksAutoscaleMaxThroughput" in search_chunks["properties"][
        "options"
    ]
    assert containers_by_id["ingestion-runs"]["properties"]["options"] == {}
    assert containers_by_id["source-documents"]["properties"]["options"] == {}
    assert containers_by_id["retrieval-config"]["properties"]["options"] == {}


def test_function_environment_names_are_exactly_locked_per_surface() -> None:
    template = _compile_bicep("infra/modules/functions.bicep")
    function_app = _resources(template, "Microsoft.Web/sites")[0]
    env_names = {
        setting["name"]
        for setting in function_app["properties"]["siteConfig"]["appSettings"]
    }
    assert env_names == {
        "FUNCTIONS_EXTENSION_VERSION",
        "APPLICATIONINSIGHTS_CONNECTION_STRING",
        "APPLICATIONINSIGHTS_AUTHENTICATION_STRING",
        "AzureWebJobsStorage__blobServiceUri",
        "AzureWebJobsStorage__queueServiceUri",
        "AzureWebJobsStorage__tableServiceUri",
        "AzureWebJobsStorage__credential",
        "AzureWebJobsStorage__clientId",
        "AZURE_AUTH_MODE",
        "AZURE_CLIENT_ID",
        "COSMOS_ENDPOINT",
        "COSMOS_DATABASE_NAME",
        "COSMOS_INGESTION_RUNS_CONTAINER_NAME",
        "COSMOS_INGESTION_SOURCES_CONTAINER_NAME",
        "DURABLE_TASK_SCHEDULER_CONNECTION_STRING",
        "TASKHUB_NAME",
        "OPENAI_ENDPOINT",
        "OPENAI_EMBEDDING_DEPLOYMENT_NAME",
        "OPENAI_CHAT_DEPLOYMENT_NAME",
        "VISION_MAX_OUTPUT_TOKENS",
        "DOCUMENT_INTELLIGENCE_ENDPOINT",
        "DOCUMENT_INTELLIGENCE_ENABLED",
        "CONTENT_UNDERSTANDING_ENDPOINT",
        "CONTENT_UNDERSTANDING_ANALYZER_ID",
        "CONTENT_UNDERSTANDING_ENABLED",
        "AZURE_LANGUAGE_ENDPOINT",
        "SPEECH_ENDPOINT",
        "SPEECH_REGION",
        "AUDIO_DEPLOYMENT_REGION",
        "AUDIO_LOCALE",
        "AUDIO_WRITER_ENABLED",
        "AUDIO_TRANSCRIPTION_PROVIDER",
        "AUDIO_STAGING_BLOB_ENDPOINT",
        "AUDIO_STAGING_CONTAINER",
        "AUDIO_BATCH_TTL_HOURS",
        "KEY_VAULT_URI",
        "SHAREPOINT_TENANT_ID",
        "SHAREPOINT_APP_CLIENT_ID",
        "INGESTION_SOURCE_ID",
        "ALLOWED_FILE_EXTENSIONS",
        "SHAREPOINT_ASSIGNED_DRIVE_ID",
        "SHAREPOINT_CERTIFICATE_SECRET_NAME",
        "FUNCTION_PUBLIC_BASE_URL",
        "DELTA_SYNC_SCHEDULE",
        "ACL_RESYNC_SCHEDULE",
        "ACL_RESYNC_PAGE_SIZE",
        "LIFECYCLE_RECONCILE_SCHEDULE",
        "LIFECYCLE_RECONCILE_PAGE_SIZE",
        "CHUNK_MAX_TOKENS",
        "CHUNK_OVERLAP_TOKENS",
        "ACL_MAX_PAGES",
        "DOWNLOAD_TIMEOUT_SECONDS",
        "DELTA_MAX_PAGES",
        "EMBEDDING_BATCH_SIZE",
        "MAX_PAGES_PER_SOURCE",
        "WEBHOOK_CLIENT_STATE",
        "SUBSCRIPTION_RENEW_SCHEDULE",
        "SHAREPOINT_SITE_URL",
    }


def test_retrieval_environment_names_are_exactly_locked_per_surface() -> None:
    template = _compile_bicep("infra/modules/retrieval-config.bicep")
    env_names = {item["name"] for item in template["variables"]["envVars"]}
    assert template["parameters"]["agentTimeoutSeconds"]["defaultValue"] == "20.0"
    assert env_names == {
        "COSMOS_ENDPOINT",
        "COSMOS_DATABASE",
        "COSMOS_AUDIT_CONTAINER",
        "COSMOS_INGESTION_SOURCES_CONTAINER",
        "COSMOS_INGESTION_RUNS_CONTAINER",
        "AZURE_OPENAI_ENDPOINT",
        "EMBEDDING_DEPLOYMENT",
        "CHAT_DEPLOYMENT",
        "TENANT_ID",
        "AZURE_AUTH_MODE",
        "MANAGED_IDENTITY_CLIENT_ID",
        "RETRIEVAL_API_AUDIENCE",
        "RETRIEVAL_CATALOG_POLL_SECONDS",
        "INCLUDE_CITATIONS",
        "MAX_EVIDENCE_CHUNKS",
        "MAX_PLANNED_QUERIES",
        "RETRIEVAL_TIMEOUT_SECONDS",
        "GENERATION_TIMEOUT_SECONDS",
        "AGENT_TIMEOUT_SECONDS",
        "AGENT_MAX_ITERATIONS",
        "AGENT_OPENAI_API_VERSION",
        "GRAPH_GROUP_TIMEOUT_SECONDS",
        "OPENAI_API_VERSION",
        "APPLICATIONINSIGHTS_CONNECTION_STRING",
        "RETRIEVAL_CONFIG_CONTAINER",
        "RETRIEVAL_OPERATION_TIMEOUT_SECONDS",
    }


def test_retrieval_catalog_and_direct_auth_env_contracts() -> None:
    config_source = (PROJECT_ROOT / "infra/modules/retrieval-config.bicep").read_text(
        encoding="utf-8"
    )
    for name in (
        "RETRIEVAL_CATALOG_POLL_SECONDS",
        "RETRIEVAL_CONFIG_CONTAINER",
        "RETRIEVAL_API_AUDIENCE",
        "RETRIEVAL_OPERATION_TIMEOUT_SECONDS",
        "COSMOS_INGESTION_SOURCES_CONTAINER",
        "COSMOS_INGESTION_RUNS_CONTAINER",
    ):
        assert name in config_source
    assert "RETRIEVAL_GATEWAY_CLIENT_ID" not in config_source
    assert "RETRIEVAL_GATEWAY_PRINCIPAL_ID" not in config_source

    assert "RETRIEVAL_CATALOG_DIGEST" not in config_source
    template = _compile_bicep("infra/modules/retrieval-config.bicep")
    retrieval_env = {
        item["name"]: item["value"]
        for item in template["variables"]["envVars"]
    }
    assert retrieval_env["AZURE_AUTH_MODE"] == "managed_identity"
    assert "AZURE_CLIENT_SECRET" not in retrieval_env
    assert template["parameters"]["catalogPollSeconds"] == {
        "type": "int", "defaultValue": 7200, "minValue": 60, "maxValue": 86400,
        "metadata": {"description": "Runtime catalog poll interval in seconds"},
    }

    retrieval_rbac = _compile_bicep("infra/modules/retrieval-cosmos-rbac.bicep")
    retrieval_roles = _resources(
        retrieval_rbac, "Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments"
    )
    assert len(retrieval_roles) == 2
    reader_roles = [
        role for role in retrieval_roles
        if role["properties"]["roleDefinitionId"].endswith(
            "000000000001', parameters('cosmosAccountId'))]"
        )
    ]
    assert len(reader_roles) == 1
    reader_role = reader_roles[0]
    assert reader_role["copy"]["count"] == "[length(variables('readableContainerNames'))]"
    assert retrieval_rbac["variables"]["readableContainerNames"] == [
        "[parameters('searchChunksContainerName')]",
        "[parameters('sourceDocumentsContainerName')]",
        "[parameters('ingestionSourcesContainerName')]",
        "[parameters('ingestionRunsContainerName')]",
        "[parameters('retrievalConfigContainerName')]",
    ]
    assert "colls/{2}" in reader_role["properties"]["scope"]
    assert "readableContainerNames" in reader_role["properties"]["scope"]
    assert any(
        "serviceAuditContainerName" in role["properties"]["scope"]
        and "audit-create-read" in role["properties"]["roleDefinitionId"]
        for role in retrieval_roles
    )

    publisher = _compile_bicep("infra/modules/retrieval-config-publisher-rbac.bicep")
    publisher_roles = _resources(
        publisher, "Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments"
    )
    publisher_role = next(
        role for role in publisher_roles
        if role["name"].endswith("create-read'))]")
    )
    assert publisher_role["condition"] == "[not(empty(parameters('publisherPrincipalId')))]"
    assert publisher_role["properties"]["scope"] == "[variables('containerScope')]"
    assert "catalog-bootstrap-create-read" in publisher_role["properties"]["roleDefinitionId"]
    function_writer = next(
        role for role in publisher_roles
        if "function-writer" in role["name"]
    )
    assert function_writer["condition"] == (
        "[not(empty(parameters('functionWriterPrincipalId')))]"
    )
    assert function_writer["properties"]["scope"] == "[variables('containerScope')]"
    assert "catalog-guarded-writer" in function_writer["properties"]["roleDefinitionId"]


def test_catalog_roles_have_no_delete_upsert_or_wildcard_actions():
    publisher = _compile_bicep("infra/modules/retrieval-config-publisher-rbac.bicep")
    roles = _resources(publisher, "Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions")
    assert len(roles) == 3
    source = (PROJECT_ROOT / "infra/modules/retrieval-config-publisher-rbac.bicep").read_text(encoding="utf-8")
    for forbidden in ("items/delete", "items/upsert", "items/*", "containers/*", "000000000002"):
        assert forbidden not in source
    assert publisher["variables"]["readActions"] == [
        "Microsoft.DocumentDB/databaseAccounts/readMetadata",
        "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/read",
    ]
    assert publisher["variables"]["editActions"] == (
        "[concat(variables('readActions'), createArray("
        "'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/replace', "
        "'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/executeQuery'))]"
    )
    assert "items/create" in json.dumps(roles[0]["properties"]["permissions"])
    assert roles[1]["properties"]["permissions"] == [{"dataActions": "[variables('editActions')]"}]
    audit = _compile_bicep("infra/modules/retrieval-cosmos-rbac.bicep")
    actions = _resources(audit, "Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions")[0]["properties"]["permissions"][0]["dataActions"]
    assert set(actions) == {
        "Microsoft.DocumentDB/databaseAccounts/readMetadata",
        "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/create",
        "Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers/items/read",
    }


def test_function_cosmos_data_roles_are_container_scoped() -> None:
    template = _compile_bicep("infra/modules/rbac.bicep")
    assignments = _resources(
        template,
        "Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments",
    )
    scopes = [assignment["properties"]["scope"] for assignment in assignments]

    assert len([scope for scope in scopes if "colls/{" in scope]) == 3
    assert any("function-source-registry-reader" in assignment["name"] for assignment in assignments)
    assert any("function-audit-create-read" in assignment["name"] for assignment in assignments)
    assert not any(
        scope == "[parameters('cosmosAccountId')]"
        for scope in scopes
    )
    assert template["variables"]["writableCosmosContainerNames"] == [
        "[parameters('ingestionRunsContainerName')]",
        "[parameters('sourceDocumentsContainerName')]",
        "[parameters('searchChunksContainerName')]",
    ]


def test_catalog_diagnostics_and_observer_wiring_compile():
    template = _compile_bicep("infra/main.bicep")
    for name in ("catalogEditorPrincipalId", "catalogWriterPrincipalId", "catalogObserverPrincipalId"):
        assert template["parameters"][name]["defaultValue"] == ""
    cosmos = _compile_bicep("infra/modules/cosmos.bicep")
    diagnostic = _resources(cosmos, "Microsoft.Insights/diagnosticSettings")[0]
    assert diagnostic["properties"]["logs"] == [{"category": "DataPlaneRequests", "enabled": True}]
    assert diagnostic["properties"]["logAnalyticsDestinationType"] == "Dedicated"
    monitoring = _compile_bicep("infra/modules/monitoring.bicep")
    assignments = _resources(monitoring, "Microsoft.Authorization/roleAssignments")
    assert len(assignments) == 2
    assert "3913510d-42f4-4e42-8a64-420c390055eb" in assignments[0]["properties"]["roleDefinitionId"]
    assert "73c42c96-874c-492b-b04d-ab87d138a893" in assignments[1]["properties"]["roleDefinitionId"]
    aca = _compile_bicep("infra/modules/aca.bicep")
    reader = _resources(aca, "Microsoft.Authorization/roleAssignments")[0]
    assert "acdd72a7-3385-48ef-bd42-f606fba81ae7" in reader["properties"]["roleDefinitionId"]
    assert "Microsoft.App/containerApps" in reader["scope"]

def test_main_has_no_active_aks_or_directory_mutation_path() -> None:
    main_source = (PROJECT_ROOT / "infra/main.bicep").read_text(encoding="utf-8")

    assert "deployAks" not in main_source
    assert "modules/aks.bicep" not in main_source
    assert "modules/aks-identity.bicep" not in main_source
    assert "modules/graph-rbac.bicep" not in main_source
    assert "graphServicePrincipalId" not in main_source
    assert "param deployServing bool = false" in main_source
    assert "module aca './modules/aca.bicep' = if (deployServing)" in main_source
    assert "module functions './modules/functions.bicep' = if (deployServing)" in main_source


def test_retrieval_configuration_has_no_legacy_routing_authority() -> None:
    template = _compile_bicep("infra/modules/retrieval-config.bicep")
    config_map = template["outputs"]["configMap"]["value"]
    serialized = json.dumps(config_map)

    assert "ACL_ENABLED" not in serialized
    assert "COSMOS_CHUNKS_CONTAINER" not in serialized
    assert "COSMOS_MANIFESTS_CONTAINER" not in serialized
    assert "COSMOS_REGISTRY_JSON" not in serialized


def test_content_understanding_account_and_model_contract() -> None:
    template = _compile_bicep("infra/modules/content-understanding.bicep")
    account = _resources(template, "Microsoft.CognitiveServices/accounts")[0]
    deployments = _resources(
        template,
        "Microsoft.CognitiveServices/accounts/deployments",
    )
    deployments_by_model = {
        deployment["properties"]["model"]["name"]: deployment
        for deployment in deployments
    }

    assert account["apiVersion"] == "2025-06-01"
    assert account["kind"] == "AIServices"
    assert account["sku"] == {"name": "S0"}
    assert account["identity"] == {"type": "SystemAssigned"}
    assert account["properties"]["disableLocalAuth"] is True
    assert template["parameters"]["allowedIpAddress"]["defaultValue"] == ""
    assert account["properties"]["publicNetworkAccess"] == (
        "[if(empty(parameters('allowedIpAddress')), 'Disabled', 'Enabled')]"
    )
    network_acls = account["properties"]["networkAcls"]
    assert network_acls["defaultAction"] == "Deny"
    assert network_acls["virtualNetworkRules"] == []
    assert network_acls["ipRules"] == (
        "[if(empty(parameters('allowedIpAddress')), createArray(), "
        "createArray(createObject('value', parameters('allowedIpAddress'))))]"
    )
    assert set(deployments_by_model) == {
        "gpt-5.2",
        "text-embedding-3-large",
    }
    assert deployments_by_model["gpt-5.2"]["properties"] == {
        "model": {
            "format": "OpenAI",
            "name": "gpt-5.2",
            "version": "2025-12-11",
        },
        "versionUpgradeOption": "NoAutoUpgrade",
    }
    assert deployments_by_model["gpt-5.2"]["sku"] == {
        "name": "GlobalStandard",
        "capacity": 30,
    }
    assert deployments_by_model["text-embedding-3-large"]["properties"] == {
        "model": {
            "format": "OpenAI",
            "name": "text-embedding-3-large",
            "version": "1",
        },
        "versionUpgradeOption": "NoAutoUpgrade",
    }
    assert deployments_by_model["text-embedding-3-large"]["sku"] == {
        "name": "GlobalStandard",
        "capacity": 120,
    }
    assert deployments_by_model["text-embedding-3-large"]["dependsOn"] == [
        "[resourceId('Microsoft.CognitiveServices/accounts', "
        "parameters('accountName'))]",
        "[resourceId('Microsoft.CognitiveServices/accounts/deployments', "
        "parameters('accountName'), variables('completionDeploymentName'))]"
    ]
    assert all(deployment["apiVersion"] == "2025-06-01" for deployment in deployments)


def test_content_understanding_composition_is_private_and_identity_scoped() -> None:
    main_source = (PROJECT_ROOT / "infra/main.bicep").read_text(encoding="utf-8")
    rbac = _compile_bicep("infra/modules/rbac.bicep")
    roles = _resources(rbac, "Microsoft.Authorization/roleAssignments")
    content_understanding_roles = [
        role
        for role in roles
        if "contentUnderstandingId" in role["scope"]
    ]

    assert (
        "module contentUnderstanding './modules/content-understanding.bicep' = "
        "if (contentUnderstandingEnabled)"
    ) in main_source
    assert "param contentUnderstandingEnabled bool = false" in main_source
    assert "name: 'content-understanding'" in main_source
    assert "resourceId: contentUnderstanding.?outputs.?accountId ?? ''" in main_source
    assert "groupId: 'account'" in main_source
    assert "dnsZoneName: 'privatelink.services.ai.azure.com'" in main_source
    assert "contentUnderstanding.?outputs.?endpoint ?? ''" in main_source
    assert "contentUnderstanding.?outputs.?accountId ?? ''" in main_source
    assert rbac["variables"]["roles"]["ContentUnderstandingContributor"] == (
        "59a2dba3-6303-4fd8-9a2e-8cbb4bdda972"
    )
    assert len(content_understanding_roles) == 1
    assert content_understanding_roles[0]["properties"]["roleDefinitionId"] == (
        "[subscriptionResourceId('Microsoft.Authorization/roleDefinitions', "
        "variables('roles').ContentUnderstandingContributor)]"
    )
    assert content_understanding_roles[0]["properties"]["principalId"] == (
        "[parameters('principalId')]"
    )
    assert content_understanding_roles[0]["properties"]["principalType"] == (
        "ServicePrincipal"
    )
    assert content_understanding_roles[0]["condition"] == (
        "[not(empty(parameters('contentUnderstandingId')))]"
    )


def test_aca_requires_immutable_inputs_and_direct_user_auth() -> None:
    main_source = (PROJECT_ROOT / "infra/main.bicep").read_text(encoding="utf-8")
    environment_template = _compile_bicep("infra/modules/aca-environment.bicep")
    aca_path = PROJECT_ROOT / "infra/modules/aca.bicep"
    aca_source = aca_path.read_text(encoding="utf-8")
    aca_template = _compile_bicep("infra/modules/aca.bicep")

    assert "param retrievalImageReference string = ''" in main_source
    assert "param retrievalCatalogDigest string = ''" in main_source
    assert "param entraApiTenantId string" in main_source
    assert (
        "entraIssuer: "
        "'${environment().authentication.loginEndpoint}${entraApiTenantId}/v2.0'"
    ) in main_source
    assert (
        "entraIssuer: "
        "'${environment().authentication.loginEndpoint}${sharePointTenantId}/v2.0'"
    ) not in main_source
    assert "take(replace(prefix, '-', ''), 18)" in main_source
    assert main_source.count("retrievalApiAudience: retrievalApiClientId") == 2
    assert "param retrievalApiAudience" not in main_source
    assert aca_template["parameters"]["containerAppName"]["maxLength"] == 32
    assert "imageDigest: last(split(retrievalImageReference, '@sha256:'))" in main_source
    assert "param imageDigest string" in aca_source
    assert "'${acrLoginServer}/rag-retrieval@sha256:${imageDigest}'" in aca_source
    assert "quickstart" not in aca_source
    assert "@sha256:" in main_source
    assert "gatewayClientId" not in aca_source
    assert "gatewayPrincipalId" not in aca_source
    assert "param allowedApplicationClientIds array" in aca_source
    assert "allowedApplications: allowedApplicationClientIds" in aca_source
    assert (
        "allowedApplicationClientIds: retrievalAllowedApplicationClientIds"
        in main_source
    )
    assert "allowedPrincipals" not in aca_source
    assert "excludedPaths" not in aca_source
    assert "unauthenticatedClientAction: 'Return401'" in aca_source
    assert "allowInsecure: false" in aca_source
    assert "clientSecretSettingName" not in aca_source
    assert "retrieval-auth-client-secret" not in aca_source
    assert "output retrievalContainerAppName string" in main_source
    managed_environment = _resources(
        environment_template,
        "Microsoft.App/managedEnvironments",
    )[0]
    assert managed_environment["properties"]["vnetConfiguration"]["internal"] is False
    assert "publicNetworkAccess" not in managed_environment["properties"]
    assert "module acaDns " not in main_source
    assert not (PROJECT_ROOT / "infra/modules/aca-dns.bicep").exists()
    assert "output publicUrl string" in aca_source
    assert "output queryUrl string" in aca_source
    assert "aca.?outputs.?publicUrl" in main_source
    assert "aca.?outputs.?queryUrl" in main_source

    external_environment_path = (
        PROJECT_ROOT / "infra/modules/external-aca-environment.bicep"
    )
    external_environment_source = external_environment_path.read_text(encoding="utf-8")
    external_environment = _compile_bicep(
        "infra/modules/external-aca-environment.bicep"
    )
    assert "name: 'external-aca-environment-resource'" in external_environment_source
    assert len(
        _resources(external_environment, "Microsoft.Resources/deployments")
    ) == 1
    external_serving = _compile_bicep("infra/modules/external-aca-serving.bicep")
    deployments = _resources(external_serving, "Microsoft.Resources/deployments")
    assert len(deployments) == 2
    assert set(external_serving["outputs"]) == {
        "retrievalFqdn",
        "retrievalPublicUrl",
        "retrievalQueryUrl",
    }
    serving_source = (
        PROJECT_ROOT / "infra/modules/external-aca-serving.bicep"
    ).read_text(encoding="utf-8")
    assert "tenantId: sharePointTenantId" in serving_source
    assert "${entraApiTenantId}/v2.0" in serving_source


def test_audio_staging_account_is_deny_gated_to_speech_resource_instance() -> None:
    template = _compile_bicep("infra/modules/audio-staging.bicep")
    account = _resources(template, "Microsoft.Storage/storageAccounts")[0]
    properties = account["properties"]

    # Public endpoint on (required for Speech resource-instance access) but locked down.
    assert properties["publicNetworkAccess"] == "Enabled"
    assert properties["allowSharedKeyAccess"] is False
    assert properties["allowBlobPublicAccess"] is False
    assert properties["minimumTlsVersion"] == "TLS1_2"
    network_acls = properties["networkAcls"]
    assert network_acls["defaultAction"] == "Deny"
    assert network_acls["virtualNetworkRules"] == []
    assert network_acls["ipRules"] == []
    assert network_acls["resourceAccessRules"] == [
        {
            "tenantId": "[subscription().tenantId]",
            "resourceId": "[parameters('speechAccountId')]",
        }
    ]
    containers = _resources(
        template, "Microsoft.Storage/storageAccounts/blobServices/containers"
    )
    assert len(containers) == 1
    assert template["parameters"]["containerName"]["defaultValue"] == "audio-staging"
    assert containers[0]["properties"]["publicAccess"] == "None"


def test_audio_staging_rbac_is_least_privilege_split() -> None:
    template = _compile_bicep("infra/modules/audio-staging-rbac.bicep")
    roles = _resources(template, "Microsoft.Authorization/roleAssignments")
    roles_by_principal = {
        role["properties"]["principalId"]: role["properties"]["roleDefinitionId"]
        for role in roles
    }
    assert len(roles) == 2
    assert template["variables"]["roles"]["StorageBlobDataContributor"] == (
        "ba92f5b4-2d11-453d-a403-e96b0029c9fe"
    )
    assert template["variables"]["roles"]["StorageBlobDataReader"] == (
        "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"
    )
    # Function MI writes/deletes source audio; Speech MI only reads it.
    assert roles_by_principal["[parameters('functionPrincipalId')]"].endswith(
        "variables('roles').StorageBlobDataContributor)]"
    )
    assert roles_by_principal["[parameters('speechPrincipalId')]"].endswith(
        "variables('roles').StorageBlobDataReader)]"
    )
    assert all(
        role["properties"]["principalType"] == "ServicePrincipal" for role in roles
    )


def test_audio_staging_composition_is_gated_and_privately_reachable() -> None:
    main_source = (PROJECT_ROOT / "infra/main.bicep").read_text(encoding="utf-8")

    assert (
        "module audioStaging './modules/audio-staging.bicep' = if (audioWriterEnabled)"
    ) in main_source
    assert (
        "module audioStagingRbac './modules/audio-staging-rbac.bicep' = if (audioWriterEnabled)"
    ) in main_source
    assert "speechAccountId: speech.?outputs.?accountId ?? ''" in main_source
    assert "speechPrincipalId: speech.?outputs.?principalId ?? ''" in main_source
    # Function reaches the Deny-gated account only via a blob private endpoint.
    assert "name: 'storage-blob-staging'" in main_source
    assert "resourceId: audioStaging.?outputs.?storageAccountId ?? ''" in main_source
    assert "audioStaging.?outputs.?blobEndpoint ?? ''" in main_source
    assert "audioStaging.?outputs.?containerName ?? ''" in main_source


def test_key_vault_uri_uses_the_complete_environment_suffix() -> None:
    main_source = (PROJECT_ROOT / "infra/main.bicep").read_text(encoding="utf-8")

    assert (
        "'https://${sharePointKeyVaultName}${environment().suffixes.keyvaultDns}'"
        in main_source
    )
    assert ".vault.${environment().suffixes.keyvaultDns}" not in main_source


def test_private_catalog_job_is_manual_identity_only_and_secretless() -> None:
    template = _compile_bicep("infra/modules/aca-operations-job.bicep")
    assert template["parameters"]["jobName"]["minLength"] == 2
    assert template["parameters"]["jobName"]["maxLength"] == 31
    job = _resources(template, "Microsoft.App/jobs")[0]
    properties = job["properties"]
    configuration = properties["configuration"]
    container = properties["template"]["containers"][0]
    env_names = {entry["name"] for entry in container["env"]}

    assert job["identity"]["type"] == "UserAssigned"
    assert properties["workloadProfileName"] == "Consumption"
    assert configuration["triggerType"] == "Manual"
    assert configuration["manualTriggerConfig"] == {
        "parallelism": 1,
        "replicaCompletionCount": 1,
    }
    assert container["command"] == ["python"]
    assert container["args"] == [
        "-m", "retrieval.operations", "[parameters('catalogOperation')]",
    ]
    assert template["parameters"]["catalogOperation"]["defaultValue"] == "verify-catalog"
    assert template["parameters"]["catalogOperation"]["allowedValues"] == [
        "publish-catalog", "verify-catalog",
    ]
    assert template["parameters"]["catalogDigest"]["defaultValue"] == ""
    assert env_names == {
        "COSMOS_ENDPOINT",
        "COSMOS_DATABASE",
        "RETRIEVAL_CONFIG_CONTAINER",
        "EXPECTED_CATALOG_DIGEST",
        "MANAGED_IDENTITY_CLIENT_ID",
    }
    assert "secrets" not in configuration

    main_source = (PROJECT_ROOT / "infra/main.bicep").read_text(encoding="utf-8")
    assert "param deployOperations bool = false" in main_source
    assert "module operationsIdentity" in main_source
    assert "module operationsJob" in main_source
    assert "if (deployOperations)" in main_source
    assert "take(replace(prefix, '-', ''), 19)" in main_source
    assert "publisherPrincipalId: operationsIdentity.outputs.identityPrincipalId" in main_source


def test_durable_scheduler_task_hub_and_rbac_contract() -> None:
    template = _compile_bicep("infra/modules/durable-task.bicep")
    scheduler = _resources(template, "Microsoft.DurableTask/schedulers")[0]
    task_hub = _resources(
        template, "Microsoft.DurableTask/schedulers/taskHubs"
    )[0]
    role = _resources(template, "Microsoft.Authorization/roleAssignments")[0]

    assert scheduler["apiVersion"] == "2025-11-01"
    assert scheduler["properties"]["sku"] == {"name": "Consumption"}
    assert task_hub["apiVersion"] == "2025-11-01"
    assert task_hub["properties"] == {}
    assert template["variables"]["durableTaskDataContributorRoleId"] == (
        "0ad04412-c4d5-4796-b79c-f76d14c8d402"
    )
    assert "Microsoft.DurableTask/schedulers" in role["scope"]
    assert "taskHubs" in role["scope"]
    assert role["properties"]["principalId"] == (
        "[parameters('functionAppPrincipalId')]"
    )
    assert role["properties"]["principalType"] == "ServicePrincipal"


def test_function_settings_match_full_sync_contract() -> None:
    template = _compile_bicep("infra/modules/functions.bicep")
    function_app = _resources(template, "Microsoft.Web/sites")[0]
    settings = {
        setting["name"]: setting["value"]
        for setting in function_app["properties"]["siteConfig"]["appSettings"]
    }

    assert settings["AZURE_AUTH_MODE"] == "managed_identity"
    assert settings["AZURE_CLIENT_ID"] == "[parameters('managedIdentityClientId')]"
    assert "AZURE_CLIENT_SECRET" not in settings
    for redundant_name in (
        "MANAGED_IDENTITY_CLIENT_ID",
        "TENANT_ID",
        "FUNCTION_API_AUDIENCE",
        "VISION_MAX_IMAGE_BYTES",
        "VISION_MAX_FIGURES",
        "INSTANCE_MEMORY_MB",
    ):
        assert redundant_name not in settings
    assert settings["COSMOS_INGESTION_RUNS_CONTAINER_NAME"] == (
        "[parameters('cosmosContainerNames').ingestionRuns]"
    )
    assert "COSMOS_SOURCE_DOCUMENTS_CONTAINER_NAME" not in settings
    assert "COSMOS_SEARCH_CHUNKS_CONTAINER_NAME" not in settings
    assert settings["TASKHUB_NAME"] == "[parameters('durableTaskHubName')]"
    assert settings["CONTENT_UNDERSTANDING_ENDPOINT"] == (
        "[parameters('contentUnderstandingEndpoint')]"
    )
    assert settings["CONTENT_UNDERSTANDING_ANALYZER_ID"] == (
        "[parameters('contentUnderstandingAnalyzerId')]"
    )
    assert settings["DOCUMENT_INTELLIGENCE_ENABLED"] == (
        "[string(parameters('documentIntelligenceEnabled'))]"
    )
    assert settings["CONTENT_UNDERSTANDING_ENABLED"] == (
        "[string(parameters('contentUnderstandingEnabled'))]"
    )
    assert settings["MAX_PAGES_PER_SOURCE"] == "2000"
    assert "MAX_PDF_PAGES" not in settings
    assert settings["DURABLE_TASK_SCHEDULER_CONNECTION_STRING"] == (
        "[format('Endpoint={0};Authentication=ManagedIdentity;ClientID={1}', "
        "parameters('durableTaskSchedulerEndpoint'), "
        "parameters('managedIdentityClientId'))]"
    )
    assert function_app["properties"]["functionAppConfig"][
        "scaleAndConcurrency"
    ]["instanceMemoryMB"] == "[parameters('instanceMemoryMB')]"
    site_url_parameter = template["parameters"]["sharePointSiteUrl"]
    assert site_url_parameter["minLength"] == 1
    assert "defaultValue" not in site_url_parameter


def test_one_generic_parameter_contract_has_no_target_defaults() -> None:
    parameters = (PROJECT_ROOT / "infra/main.parameters.bicepparam").read_text(
        encoding="utf-8"
    )
    assert not (PROJECT_ROOT / "infra/main.parameters.dev.bicepparam").exists()
    assert not (PROJECT_ROOT / "infra/main.parameters.prod.bicepparam").exists()
    assert "readEnvironmentVariable('DEPLOYMENT_INSTANCE_ID')" in parameters
    assert "readEnvironmentVariable('AZURE_LOCATION')" in parameters
    assert "readEnvironmentVariable('SHAREPOINT_ASSIGNED_DRIVE_ID')" in parameters
    assert "readEnvironmentVariable('SHAREPOINT_SITE_URL')" in parameters
    assert "readEnvironmentVariable('SHAREPOINT_SITE_URL', '')" not in parameters
    assert "readEnvironmentVariable('SUBNET_FUNCTION_INTEGRATION_NSG_ID', '')" in parameters
    assert "readEnvironmentVariable('SUBNET_PRIVATE_ENDPOINTS_NSG_ID', '')" in parameters
    assert "readEnvironmentVariable('SUBNET_ACA_ENVIRONMENT_NSG_ID', '')" in parameters
    assert "readEnvironmentVariable('DOCUMENT_INTELLIGENCE_ENABLED', 'true') == 'true'" in parameters
    assert "readEnvironmentVariable('CONTENT_UNDERSTANDING_ENABLED', 'false') == 'true'" in parameters
    assert "'prebuilt-documentSearch'" in parameters
    assert "param resourceGroup" not in parameters
    assert "dev-webhook-secret" not in parameters


def test_networking_preserves_subnet_security_associations() -> None:
    template = _compile_bicep("infra/modules/networking.bicep")
    subnet_inputs = template["resources"]["virtualNetwork"]["properties"][
        "parameters"
    ]["subnets"]["value"]

    assert len(subnet_inputs) == 3
    for subnet in subnet_inputs:
        assert subnet["privateEndpointNetworkPolicies"] == "Disabled"
        assert subnet["networkSecurityGroupResourceId"].startswith("[parameters('")


def test_function_easyauth_requires_exact_api_audience_and_callers() -> None:
    template = _compile_bicep("infra/modules/functions.bicep")
    auth_settings = _resources(template, "Microsoft.Web/sites/config")[0]
    validation = auth_settings["properties"]["identityProviders"][
        "azureActiveDirectory"
    ]["validation"]
    audiences = validation["allowedAudiences"]

    assert audiences == ["[parameters('functionApiAudience')]"]
    assert validation["defaultAuthorizationPolicy"] == {
        "allowedApplications": "[parameters('allowedApplicationClientIds')]"
    }

    parameters = (PROJECT_ROOT / "infra/main.parameters.bicepparam").read_text(
        encoding="utf-8"
    )
    assert "FUNCTION_ALLOWED_CALLER_CLIENT_ID" in parameters
    assert "04b07795-8ddb-461a-bbee-02f9e1bf7b46" not in parameters
