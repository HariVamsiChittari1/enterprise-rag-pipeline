"""Private one-shot deployment operations executed inside the ACA environment."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
from typing import Any

from azure.cosmos import CosmosClient
from azure.identity import ManagedIdentityCredential

from retrieval.catalog import (
    CatalogError,
    RuntimeCatalogLoader,
    build_catalog_item,
    load_catalog_item,
    publish_catalog,
)


class OperationsError(RuntimeError):
    """A private deployment operation failed without exposing sensitive details."""


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise OperationsError(f"{name} is required")
    return value


def publish_bootstrap_catalog() -> dict[str, str]:
    deployment_instance_id = _required("DEPLOYMENT_INSTANCE_ID")
    expected_digest = _required("EXPECTED_CATALOG_DIGEST")
    source_path = Path(
        os.getenv("CATALOG_PATH", "/app/retrieval/catalog.example.json")
    )
    try:
        source = json.loads(source_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise OperationsError("catalog source cannot be read") from error
    if not isinstance(source, dict):
        raise OperationsError("catalog source must be an object")

    item = build_catalog_item(source, deployment_instance_id)
    catalog = load_catalog_item(item)
    if catalog.digest != expected_digest:
        raise OperationsError("catalog digest does not match the reviewed artifact")

    return _run_catalog_operation(item)


def verify_current_catalog() -> dict[str, str]:
    return _run_catalog_operation()


def _run_catalog_operation(item: dict[str, Any] | None = None) -> dict[str, str]:
    endpoint = _required("COSMOS_ENDPOINT")
    database_name = _required("COSMOS_DATABASE")
    container_name = _required("RETRIEVAL_CONFIG_CONTAINER")
    deployment_instance_id = _required("DEPLOYMENT_INSTANCE_ID")
    managed_identity_client_id = _required("MANAGED_IDENTITY_CLIENT_ID")
    try:
        with ExitStack() as resources:
            credential = ManagedIdentityCredential(client_id=managed_identity_client_id)
            resources.callback(credential.close)
            cosmos = CosmosClient(endpoint, credential=credential)
            resources.callback(cosmos.close)
            container = cosmos.get_database_client(database_name).get_container_client(
                container_name
            )
            if item is not None:
                publish_catalog(container, item)
            observed = RuntimeCatalogLoader(container, deployment_instance_id).load()
            if observed.etag is None:
                raise OperationsError("runtime catalog has no ETag")
        return {
            "catalogId": observed.catalog_id,
            "catalogDigest": observed.digest,
            "catalogEtag": observed.etag,
        }
    except CatalogError:
        raise
    except OperationsError:
        raise
    except Exception as error:
        raise OperationsError("private catalog operation failed") from error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("publish-catalog", "verify-catalog"))
    args = parser.parse_args(argv)
    try:
        if args.operation == "publish-catalog":
            result = publish_bootstrap_catalog()
        else:
            result = verify_current_catalog()
    except (CatalogError, OperationsError) as error:
        print(json.dumps({"status": "failed", "error": str(error)}))
        return 2
    print(json.dumps({"status": "succeeded", "operation": args.operation, **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
