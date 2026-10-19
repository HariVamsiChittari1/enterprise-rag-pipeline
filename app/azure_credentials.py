"""Explicit Azure credential selection without developer-identity fallback."""

from __future__ import annotations

import os
from collections.abc import Mapping

from azure.core.credentials import TokenCredential
from azure.identity import ClientSecretCredential, ManagedIdentityCredential

SERVICE_PRINCIPAL_MODE = "service_principal"
MANAGED_IDENTITY_MODE = "managed_identity"


class AzureCredentialConfigurationError(ValueError):
    """Azure authentication configuration is missing or inconsistent."""


def create_azure_credential(
    managed_identity_client_id: str = "",
    *,
    environ: Mapping[str, str] | None = None,
) -> TokenCredential:
    """Create only the credential selected by ``AZURE_AUTH_MODE``."""
    values = os.environ if environ is None else environ
    auth_mode = values.get("AZURE_AUTH_MODE", "").strip().lower()
    if auth_mode == SERVICE_PRINCIPAL_MODE:
        tenant_id = _required(values, "AZURE_TENANT_ID", auth_mode)
        client_id = _required(values, "AZURE_CLIENT_ID", auth_mode)
        client_secret = _required(values, "AZURE_CLIENT_SECRET", auth_mode)
        return ClientSecretCredential(tenant_id, client_id, client_secret)
    if auth_mode == MANAGED_IDENTITY_MODE:
        client_id = managed_identity_client_id.strip()
        if not client_id:
            client_id = values.get("MANAGED_IDENTITY_CLIENT_ID", "").strip()
        if not client_id:
            client_id = values.get("AZURE_CLIENT_ID", "").strip()
        if not client_id:
            raise AzureCredentialConfigurationError(
                "managed_identity mode requires MANAGED_IDENTITY_CLIENT_ID "
                "or AZURE_CLIENT_ID"
            )
        return ManagedIdentityCredential(client_id=client_id)
    if not auth_mode:
        raise AzureCredentialConfigurationError(
            "AZURE_AUTH_MODE must be set to service_principal or managed_identity"
        )
    raise AzureCredentialConfigurationError(
        "AZURE_AUTH_MODE must be service_principal or managed_identity"
    )


def _required(
    values: Mapping[str, str],
    name: str,
    auth_mode: str,
) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise AzureCredentialConfigurationError(
            f"{auth_mode} mode requires {name}"
        )
    return value
