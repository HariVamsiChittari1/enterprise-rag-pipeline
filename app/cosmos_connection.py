"""Azure Cosmos DB client construction using Entra authentication."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    from azure.cosmos import CosmosClient

from azure_credentials import create_azure_credential


def create_cosmos_client(
    endpoint: str,
    managed_identity_client_id: str = "",
    *,
    credential: TokenCredential | None = None,
) -> CosmosClient:
    """Create an Azure Cosmos DB client with supplied or explicit credentials."""
    from azure.cosmos import CosmosClient

    if credential is None:
        credential = create_azure_credential(managed_identity_client_id)
    return CosmosClient(endpoint, credential=credential)
