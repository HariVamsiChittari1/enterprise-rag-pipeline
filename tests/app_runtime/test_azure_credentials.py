from unittest.mock import Mock

import azure_credentials
import pytest


def test_service_principal_mode_uses_only_client_secret_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential = Mock()
    client_secret_factory = Mock(return_value=credential)
    managed_identity_factory = Mock()
    monkeypatch.setattr(
        azure_credentials,
        "ClientSecretCredential",
        client_secret_factory,
    )
    monkeypatch.setattr(
        azure_credentials,
        "ManagedIdentityCredential",
        managed_identity_factory,
    )

    result = azure_credentials.create_azure_credential(
        environ={
            "AZURE_AUTH_MODE": "service_principal",
            "AZURE_TENANT_ID": "tenant",
            "AZURE_CLIENT_ID": "client",
            "AZURE_CLIENT_SECRET": "secret",
        }
    )

    assert result is credential
    client_secret_factory.assert_called_once_with("tenant", "client", "secret")
    managed_identity_factory.assert_not_called()


def test_managed_identity_mode_uses_only_selected_user_assigned_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential = Mock()
    client_secret_factory = Mock()
    managed_identity_factory = Mock(return_value=credential)
    monkeypatch.setattr(
        azure_credentials,
        "ClientSecretCredential",
        client_secret_factory,
    )
    monkeypatch.setattr(
        azure_credentials,
        "ManagedIdentityCredential",
        managed_identity_factory,
    )

    result = azure_credentials.create_azure_credential(
        "workload-client",
        environ={"AZURE_AUTH_MODE": "managed_identity"},
    )

    assert result is credential
    managed_identity_factory.assert_called_once_with(client_id="workload-client")
    client_secret_factory.assert_not_called()


@pytest.mark.parametrize(
    ("environ", "expected_client_id"),
    [
        (
            {
                "AZURE_AUTH_MODE": "managed_identity",
                "MANAGED_IDENTITY_CLIENT_ID": "retrieval-client",
                "AZURE_CLIENT_ID": "function-client",
            },
            "retrieval-client",
        ),
        (
            {
                "AZURE_AUTH_MODE": "managed_identity",
                "AZURE_CLIENT_ID": "function-client",
            },
            "function-client",
        ),
    ],
)
def test_managed_identity_mode_resolves_existing_workload_settings(
    monkeypatch: pytest.MonkeyPatch,
    environ: dict[str, str],
    expected_client_id: str,
) -> None:
    factory = Mock()
    monkeypatch.setattr(azure_credentials, "ManagedIdentityCredential", factory)

    azure_credentials.create_azure_credential(environ=environ)

    factory.assert_called_once_with(client_id=expected_client_id)


@pytest.mark.parametrize(
    ("environ", "message"),
    [
        ({}, "AZURE_AUTH_MODE"),
        ({"AZURE_AUTH_MODE": "cli"}, "AZURE_AUTH_MODE"),
        (
            {
                "AZURE_AUTH_MODE": "service_principal",
                "AZURE_CLIENT_ID": "client",
                "AZURE_CLIENT_SECRET": "secret",
            },
            "AZURE_TENANT_ID",
        ),
        (
            {
                "AZURE_AUTH_MODE": "service_principal",
                "AZURE_TENANT_ID": "tenant",
                "AZURE_CLIENT_SECRET": "secret",
            },
            "AZURE_CLIENT_ID",
        ),
        (
            {
                "AZURE_AUTH_MODE": "service_principal",
                "AZURE_TENANT_ID": "tenant",
                "AZURE_CLIENT_ID": "client",
            },
            "AZURE_CLIENT_SECRET",
        ),
        (
            {"AZURE_AUTH_MODE": "managed_identity"},
            "MANAGED_IDENTITY_CLIENT_ID",
        ),
    ],
)
def test_invalid_authentication_configuration_fails_without_fallback(
    environ: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(
        azure_credentials.AzureCredentialConfigurationError,
        match=message,
    ):
        azure_credentials.create_azure_credential(environ=environ)
