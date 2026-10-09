from unittest.mock import Mock

import azure.cosmos
import azure.identity
import pytest

import cosmos_connection


def test_default_authentication_preserves_entra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential = Mock()
    factory = Mock(return_value=credential)
    client = Mock()
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", factory)
    monkeypatch.setattr(azure.cosmos, "CosmosClient", client)
    cosmos_connection.create_cosmos_client(
        "https://account.documents.azure.com", "identity"
    )
    factory.assert_called_once_with(managed_identity_client_id="identity")
    client.assert_called_once_with(
        "https://account.documents.azure.com", credential=credential
    )


def test_supplied_credential_is_used_instead_of_default_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supplied = Mock()
    factory = Mock()
    client = Mock()
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", factory)
    monkeypatch.setattr(azure.cosmos, "CosmosClient", client)
    cosmos_connection.create_cosmos_client(
        "https://account.documents.azure.com", credential=supplied
    )
    factory.assert_not_called()
    client.assert_called_once_with(
        "https://account.documents.azure.com", credential=supplied
    )
