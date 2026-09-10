"""Tests for function_app._client_cache: _build_* factories must construct their
underlying SDK client once per process and reuse it across calls (Azure Functions
Python guidance: create clients at module level, don't rebuild per invocation).

Monkeypatches the SDK classes each factory imports locally (azure.cosmos.CosmosClient,
azure.identity.DefaultAzureCredential, openai.AzureOpenAI), matching the pattern used in
test_retire_prior_version.py: a local `from x import Y` re-resolves the module attribute
on every call, so patching the module attribute is sufficient. Production code is never
modified by these tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import azure.cosmos
import azure.ai.contentunderstanding
import azure.identity
import httpx
import openai
import pytest

import function_app
from config import ExtractionProvider


@pytest.fixture(autouse=True)
def clear_client_cache():
    """_client_cache is module-level shared state; isolate each test from it."""
    function_app._client_cache.clear()
    yield
    function_app._client_cache.clear()


class FakeContainer:
    pass


class FakeDatabase:
    def get_container_client(self, name: str) -> FakeContainer:
        return FakeContainer()


class FakeCosmosClient:
    instances: list["FakeCosmosClient"] = []

    def __init__(self, endpoint: str, credential: Any) -> None:
        FakeCosmosClient.instances.append(self)

    def get_database_client(self, name: str) -> FakeDatabase:
        return FakeDatabase()


class FakeAzureOpenAI:
    instances: list["FakeAzureOpenAI"] = []

    def __init__(self, **kwargs: Any) -> None:
        FakeAzureOpenAI.instances.append(self)


class FakeContentUnderstandingClient:
    instances: list["FakeContentUnderstandingClient"] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        FakeContentUnderstandingClient.instances.append(self)


def _fake_config(**overrides: Any) -> SimpleNamespace:
    base = dict(
        managed_identity_client_id="mi-client-id",
        cosmos_endpoint="https://cosmos.example",
        cosmos_database="db",
        cosmos_ingestion_runs_container="ingestion-runs",
        cosmos_source_documents_container="source-documents",
        cosmos_search_chunks_container="search-chunks",
        openai_endpoint="https://openai.example",
        content_understanding_endpoint="https://cu.example",
        key_vault_uri="https://vault.example",
        certificate_secret_name="sharepoint-cert",
        tenant_id="tenant-id",
        app_client_id="app-client-id",
        drive_id="drive-id",
        acl_max_pages=10,
        sharepoint_site_url="",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_build_repository_constructs_cosmos_client_once(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeCosmosClient.instances.clear()
    monkeypatch.setattr(azure.cosmos, "CosmosClient", FakeCosmosClient)
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", lambda **kwargs: object())
    config = _fake_config()

    first = function_app._build_repository(config)
    second = function_app._build_repository(config)

    assert first is second
    assert len(FakeCosmosClient.instances) == 1


def test_build_openai_client_constructs_sdk_client_once(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeAzureOpenAI.instances.clear()
    monkeypatch.setattr(openai, "AzureOpenAI", FakeAzureOpenAI)
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", lambda **kwargs: object())
    monkeypatch.setattr(azure.identity, "get_bearer_token_provider", lambda credential, scope: lambda: "token")
    config = _fake_config()

    first = function_app._build_openai_client(config)
    second = function_app._build_openai_client(config)

    assert first is second
    assert len(FakeAzureOpenAI.instances) == 1


def test_build_cu_client_constructs_sdk_client_once(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeContentUnderstandingClient.instances.clear()
    credential = object()
    monkeypatch.setattr(
        azure.ai.contentunderstanding,
        "ContentUnderstandingClient",
        FakeContentUnderstandingClient,
    )
    monkeypatch.setattr(
        azure.identity,
        "DefaultAzureCredential",
        lambda **kwargs: credential,
    )
    config = _fake_config(content_understanding_endpoint="https://cu.example/")

    first = function_app._build_cu_client(config)
    second = function_app._build_cu_client(config)

    assert first is second
    assert len(FakeContentUnderstandingClient.instances) == 1
    assert first.kwargs == {
        "endpoint": "https://cu.example",
        "credential": credential,
    }


@pytest.mark.parametrize(
    ("extraction_enabled", "provider", "expected"),
    [
        (False, None, (None, None)),
        (True, ExtractionProvider.DOCUMENT_INTELLIGENCE, ("di", None)),
        (True, ExtractionProvider.CONTENT_UNDERSTANDING, (None, "cu")),
    ],
)
def test_build_extraction_clients_constructs_only_selected_provider(
    monkeypatch: pytest.MonkeyPatch,
    extraction_enabled: bool,
    provider: ExtractionProvider | None,
    expected: tuple[str | None, str | None],
) -> None:
    config = _fake_config(
        extraction_enabled=extraction_enabled,
        extraction_provider=provider,
    )
    monkeypatch.setattr(function_app, "_build_di_client", lambda _config: "di")
    monkeypatch.setattr(function_app, "_build_cu_client", lambda _config: "cu")

    assert function_app._build_extraction_clients(config) == expected


def test_build_sharepoint_client_rejects_missing_site_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Site-group resolution must not be silently disabled by missing configuration."""

    def _fail_if_called(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("SecretClient should not be constructed when sharepoint_site_url is empty")

    monkeypatch.setattr("azure.keyvault.secrets.SecretClient", _fail_if_called)
    config = _fake_config(sharepoint_site_url="")

    with pytest.raises(EnvironmentError, match="SharePoint site URL"):
        function_app._build_sharepoint_client(config)

    assert "sharepoint_client" not in function_app._client_cache


def test_build_sharepoint_client_constructs_client_once(monkeypatch: pytest.MonkeyPatch) -> None:
    secret_clients: list[Any] = []
    binding_calls: list[tuple[Any, str, str, int]] = []

    class FakeSecretClient:
        def __init__(self, vault_url: str, credential: Any) -> None:
            secret_clients.append(self)

        def get_secret(self, name: str) -> SimpleNamespace:
            return SimpleNamespace(value="Y2VydA==")

    class FakeHttpxClient:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs

    monkeypatch.setattr("azure.keyvault.secrets.SecretClient", FakeSecretClient)
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", lambda **kwargs: object())
    monkeypatch.setattr(azure.identity, "CertificateCredential", lambda **kwargs: object())
    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kwargs: object())
    monkeypatch.setattr(httpx, "Client", FakeHttpxClient)
    graph_client = object()
    monkeypatch.setattr(function_app, "_build_graph_client", lambda config: graph_client)
    monkeypatch.setattr(
        "ingestion.graph.validate_sharepoint_drive_site",
        lambda client, drive_id, site_url, max_pages: binding_calls.append(
            (client, drive_id, site_url, max_pages)
        ),
    )
    config = _fake_config(sharepoint_site_url="https://tenant.sharepoint.com/sites/site")

    first = function_app._build_sharepoint_client(config)
    second = function_app._build_sharepoint_client(config)

    assert first is second
    assert len(secret_clients) == 1
    assert binding_calls == [(
        graph_client,
        config.drive_id,
        "https://tenant.sharepoint.com/sites/site",
        config.acl_max_pages,
    )]
