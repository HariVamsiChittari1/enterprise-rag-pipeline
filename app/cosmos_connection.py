"""Azure Cosmos DB client construction using Entra authentication."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    from azure.cosmos import CosmosClient


def create_cosmos_client(
    endpoint: str,
    managed_identity_client_id: str = "",
    *,
    credential: TokenCredential | None = None,
) -> CosmosClient:
    """Create an Azure Cosmos DB client with supplied or default Entra credentials."""
    from azure.cosmos import CosmosClient
    from azure.identity import DefaultAzureCredential

    if credential is None:
        credential = DefaultAzureCredential(
            managed_identity_client_id=managed_identity_client_id
        )
    return CosmosClient(endpoint, credential=credential)
