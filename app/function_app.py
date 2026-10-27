"""Azure Functions Durable App — SharePoint RAG Ingestion Pipeline.

Endpoints:
  POST /api/ingestion/full-sync    Start durable full-sync orchestrator (returns 202)
  GET  /api/ingestion/status       Query orchestration instance status
  GET  /api/ingestion/inspect      Read Cosmos data for debugging
  POST /api/webhook/sharepoint     Microsoft Graph change notification receiver
  POST /api/webhook/lifecycle      Microsoft Graph lifecycle notification receiver

      reconciliation_timer     Daily safety-net delta query (replaces 15-min polling)
  acl_resync_timer         Re-verify ACLs on already-ingested documents
  subscription_renew_timer Renew Microsoft Graph webhook subscription daily
"""

from __future__ import annotations

import json
import hashlib
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import azure.functions as func
import azure.durable_functions as df

from azure_credentials import create_azure_credential
from config import load_execution_batch_config

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
logger = logging.getLogger("rag-ingestion")

# Keep one worker-start snapshot so Durable replays use stable execution policy.
EXECUTION_CONFIG = load_execution_batch_config()
DELTA_SYNC_SCHEDULE = os.getenv("DELTA_SYNC_SCHEDULE", "0 0 4 * * *")  # daily 04:00 UTC
ACL_RESYNC_SCHEDULE = os.getenv("ACL_RESYNC_SCHEDULE", "0 0 3 * * 0")  # weekly Sunday 03:00 UTC (safety net)
LIFECYCLE_RECONCILE_SCHEDULE = os.getenv(
    "LIFECYCLE_RECONCILE_SCHEDULE", "0 */10 * * * *"
)
AUDIO_POLL_SCHEDULE = os.getenv("AUDIO_POLL_SCHEDULE", "0 */5 * * * *")  # every 5 minutes
SUBSCRIPTION_RENEW_SCHEDULE = os.getenv("SUBSCRIPTION_RENEW_SCHEDULE", "0 0 2 * * *")
WEBHOOK_CLIENT_STATE = os.getenv("WEBHOOK_CLIENT_STATE", "")
INGESTION_ADMIN_ROLE = os.getenv("INGESTION_ADMIN_ROLE", "Ingestion.Operator")
MAX_RETRIEVAL_CONFIG_REQUEST_BYTES = 1_572_864


def _require_ingestion_operator(req: func.HttpRequest) -> func.HttpResponse | None:
    from config import load_config
    from retrieval.auth import AuthorizationError, require_easy_auth_role

    try:
        config = load_config()
        require_easy_auth_role(
            req.headers.get("X-MS-CLIENT-PRINCIPAL"),
            config.tenant_id,
            INGESTION_ADMIN_ROLE,
        )
    except AuthorizationError:
        return _json_response({"error": "operator_role_required"}, 403)
    except Exception:
        logger.exception("ingestion operator authorization unavailable")
        return _json_response({"error": "operator_authorization_unavailable"}, 503)
    return None


@app.route(route="ingestion/full-sync", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@app.durable_client_input(client_name="client")
async def start_full_sync(req: func.HttpRequest, client) -> func.HttpResponse:
    """Start the durable full-sync orchestrator with a fresh, never-reused instance ID.

    Durable instance-ID reuse is documented by Microsoft as best-effort and racy at the
    storage layer (Azure/azure-functions-durable-python#410), so "is a full-sync already
    running" is tracked via the app's own Cosmos source-control/run records instead of a
    fixed, guessable Durable instance ID.
    """
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    from config import load_config
    load_config()
    instance_id = await client.start_new("multi_source_full_sync_orchestrator")
    return client.create_check_status_response(req, instance_id)


@app.route(route="ingestion/status", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
@app.durable_client_input(client_name="client")
async def get_status(req: func.HttpRequest, client) -> func.HttpResponse:
    """Query orchestration status. ?showHistory=true adds the replay history (diagnostic)."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    instance_id = req.params.get("instanceId")
    if not instance_id:
        return _json_response({"error": "instance_id_required"}, 400)
    show_history = (req.params.get("showHistory") or "").lower() == "true"
    status = await client.get_status(instance_id, show_history=show_history, show_history_output=show_history)
    if not status:
        return _json_response({"error": "not_found"}, 404)
    payload = {
        "instanceId": instance_id,
        "runtimeStatus": str(status.runtime_status),
        "output": status.output,
        "createdTime": str(status.created_time) if status.created_time else None,
        "lastUpdatedTime": str(status.last_updated_time) if status.last_updated_time else None,
    }
    if req.params.get("includeSourceOutcomes") == "true" or req.params.get("sourceContinuationToken"):
        try:
            from config import load_config

            registry = _build_source_registry_repository(load_config())
            page = registry.list_source_dispatch_outcomes_page(
                instance_id,
                page_size=EXECUTION_CONFIG.source_dispatch_page_size,
                continuation_token=req.params.get("sourceContinuationToken"),
            )
            payload["sourceOutcomes"] = list(page.items)
            payload["sourceContinuationToken"] = page.continuation_token
        except Exception:
            logger.exception("source dispatch outcome page read failed")
            return _json_response({"error": "source_outcomes_unavailable"}, 409)
    if show_history:
        payload["history"] = status.history
    return _json_response(payload, 200)


@app.route(route="ingestion/terminate", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@app.durable_client_input(client_name="client")
async def terminate_ingestion(req: func.HttpRequest, client) -> func.HttpResponse:
    """Start explicit per-source recovery by draining and terminating one child run."""
    from config import load_config
    from ingestion.source_registry import config_for_source
    from ingestion.services import terminate_run
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    source_id = (req.params.get("sourceId") or "").strip()
    if not source_id:
        return _json_response({"error": "source_id_required"}, 400)
    config = load_config()
    plan = next(
        (item for item in _load_validated_source_plans(config) if item.source_id == source_id),
        None,
    )
    if plan is None:
        return _json_response({"error": "registered_source_not_found"}, 404)
    config = config_for_source(config, plan)
    repository = _build_repository(config)
    source_control = repository.get_source_control(source_id)
    instance_id = req.params.get("instanceId") or (
        source_control.record.current_orchestration_instance_id
        if source_control is not None else None
    )
    if not instance_id:
        return _json_response({"error": "not_found"}, 404)
    source_registry_repository = _build_source_registry_repository(config)
    coordinator = source_registry_repository.get_drive_coordinator(source_id)
    if coordinator is not None and coordinator.get("state") in {"active", "draining"}:
        if coordinator.get("ownerInstanceId") != instance_id:
            return _json_response({"error": "coordinator_owner_mismatch"}, 409)
        if coordinator.get("state") == "active":
            source_registry_repository.mark_drive_coordinator_draining(source_id, instance_id)
    if instance_id:
        try:
            await client.terminate(instance_id, "operator_terminated")
        except Exception:
            logger.exception("durable termination request was inconclusive instance=%s", instance_id)
        terminal_observed = await _wait_for_terminal_status(client, instance_id)
        if not terminal_observed and req.params.get("abandonWork") != "true":
            return _json_response({
                "status": "draining",
                "orchestrationId": instance_id,
                "unresolvedDocumentWork": (
                    source_registry_repository.get_unresolved_document_operation_count(source_id)
                ),
            }, 202)
    if req.params.get("abandonWork") == "true":
        from ingestion.source_registry import SourceRegistryConflictError

        try:
            _operator_abandon_document_work(
                source_registry_repository,
                source_id=source_id,
                owner_instance_id=instance_id,
                work_id=req.params.get("workId", ""),
                attempt_id=req.params.get("attemptId", ""),
                source_run_id=req.params.get("sourceRunId", ""),
                document_id=req.params.get("documentId", ""),
                operator_id=req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME", "unknown"),
                reason=req.params.get("recoveryReason", ""),
                quiescence_evidence={
                    "evidenceId": req.params.get("quiescenceEvidenceId", ""),
                    "workersStopped": req.params.get("workersStopped"),
                    "remoteAudioReconciled": req.params.get("remoteAudioReconciled"),
                    "indexedStateReconciled": req.params.get("indexedStateReconciled"),
                },
            )
        except ValueError as error:
            return _json_response({"error": str(error)}, 400)
        except SourceRegistryConflictError:
            return _json_response({"error": "operator_recovery_conflict"}, 409)
    result = _terminate_run_if_document_work_settled(
        source_registry_repository,
        source_id,
        lambda: terminate_run(config, repository),
    )
    if result["status"] == "draining":
        result["orchestrationId"] = instance_id
        return _json_response(result, 202)
    if instance_id:
        # Purge requires a terminal orchestration; retain history while work is unresolved.
        await client.purge_instance_history(instance_id)
    coordinator = source_registry_repository.get_drive_coordinator(source_id)
    if (
        coordinator is not None
        and coordinator.get("state") == "draining"
        and coordinator.get("ownerInstanceId") == instance_id
    ):
        source_registry_repository.settle_drive_coordinator(source_id, instance_id)
    result["orchestrationId"] = instance_id
    return _json_response(result, 200)


def _terminate_run_if_document_work_settled(
    source_registry_repository: Any,
    source_id: str,
    finalize: Any,
) -> dict[str, Any]:
    unresolved = source_registry_repository.get_unresolved_document_operation_count(source_id)
    if unresolved > 0:
        return {"status": "draining", "unresolvedDocumentWork": unresolved}
    return finalize()


def _operator_abandon_document_work(
    registry: Any,
    *,
    source_id: str,
    owner_instance_id: str,
    work_id: str,
    attempt_id: str,
    source_run_id: str,
    document_id: str,
    operator_id: str,
    reason: str,
    quiescence_evidence: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(operator_id, str) or not operator_id or operator_id == "unknown":
        raise ValueError("operator_identity_required")
    if not all(isinstance(value, str) and value for value in (
        owner_instance_id, work_id, attempt_id, source_run_id, document_id, reason,
    )):
        raise ValueError("operator_recovery_identity_required")
    if work_id != attempt_id:
        raise ValueError("operator_recovery_identity_mismatch")
    if not isinstance(quiescence_evidence, dict) or any(
        quiescence_evidence.get(key) != "yes"
        for key in ("workersStopped", "remoteAudioReconciled", "indexedStateReconciled")
    ) or not quiescence_evidence.get("evidenceId"):
        raise ValueError("operator_quiescence_evidence_required")
    operation_kind = registry.get_document_operation_kind(
        source_id,
        work_id,
        source_run_id,
        document_id,
    )
    return registry.abandon_document_operation_for_operator(
        source_id,
        owner_instance_id,
        work_id,
        source_run_id,
        document_id,
        operation_kind,
        operator_id=operator_id,
        reason=reason,
        quiescence_evidence=quiescence_evidence,
    )


async def _wait_for_terminal_status(client, instance_id: str, timeout_seconds: int = 30) -> bool:
    """Poll until the instance reaches a status purge_instance_history requires, or timeout.

    terminate()/purge_instance_history() are client-function calls, not orchestrator code,
    so ordinary async/await and asyncio.sleep are safe here (Durable's determinism
    constraints only apply inside orchestrator functions).
    """
    import asyncio
    terminal = (
        df.OrchestrationRuntimeStatus.Completed,
        df.OrchestrationRuntimeStatus.Failed,
        df.OrchestrationRuntimeStatus.Terminated,
    )
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        status = await client.get_status(instance_id)
        if status is None:
            return False
        if status.runtime_status in terminal:
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(1)


@app.route(route="ingestion/retry-failed", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@app.durable_client_input(client_name="client")
async def retry_failed(req: func.HttpRequest, client) -> func.HttpResponse:
    """Retry failed documents for one explicitly selected registered source."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    import uuid
    from config import load_config
    from ingestion.source_registry import config_for_source
    source_id = (req.params.get("sourceId") or "").strip()
    if not source_id:
        return _json_response({"error": "source_id_required"}, 400)
    config = load_config()
    plan = next(
        (item for item in _load_validated_source_plans(config) if item.source_id == source_id),
        None,
    )
    if plan is None or plan.source.desired_state != "active":
        return _json_response({"error": "active_registered_source_not_found"}, 404)
    config = config_for_source(config, plan)
    repository = _build_repository(config)
    if _full_sync_is_running(repository, source_id):
        return _json_response({"error": "sync_in_progress"}, 409)
    source_control = repository.get_source_control(source_id)
    if source_control is None or not source_control.record.current_run_id:
        return _json_response({"status": "nothing_to_retry", "failed": 0}, 200)
    run_id = source_control.record.current_run_id
    failed_page = repository.list_failed_documents_page(source_id, run_id, page_size=1)
    if not failed_page.items:
        return _json_response({"status": "nothing_to_retry", "failed": 0}, 200)
    registry = _build_source_registry_repository(config)
    coordinator = registry.get_drive_coordinator(source_id)
    if coordinator is not None and coordinator.get("state") in {"active", "draining"}:
        return _json_response({"error": "drive_sync_in_progress"}, 409)
    retry_id = f"retry-failed-{uuid.uuid4().hex}"
    registry.acquire_drive_coordinator(
        plan,
        owner_instance_id=retry_id,
        operation="retry-failed",
    )
    retry_operation_kind = f"retry-failed:{retry_id}"
    await client.start_new(
        "retry_failed_orchestrator",
        instance_id=retry_id,
        client_input={
            "sourcePlan": plan.to_payload(),
            "coordinatorOwnerId": retry_id,
            "retryPaged": True,
            "runId": run_id,
            "operationKind": retry_operation_kind,
            "continuationToken": None,
        },
    )
    return _json_response({"status": "retrying", "orchestrationId": retry_id}, 202)


@app.route(route="ingestion/sources", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def register_source(req: func.HttpRequest) -> func.HttpResponse:
    """Register one SharePoint drive as a paused source after Graph validation."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    import httpx
    from config import load_config
    from ingestion import graph
    from ingestion.source_preflight import ProposedSourceRegistration
    from ingestion.source_registry import SourceRegistryConflictError
    from source_registry import SourceRegistryError

    try:
        body = req.get_json()
    except ValueError:
        body = None
    try:
        proposal = ProposedSourceRegistration.from_body(body)
    except SourceRegistryError as error:
        return _json_response({"error": str(error)}, 400)

    try:
        config = load_config()
        site_id = graph.validate_sharepoint_drive_site(
            _build_graph_client(config),
            proposal.drive_id,
            proposal.site_url,
            config.execution.acl_max_pages,
        )
    except httpx.HTTPStatusError as error:
        status_code = error.response.status_code
        if status_code == 429 or status_code >= 500:
            logger.warning("source registration graph unavailable status=%s", status_code)
            return _json_response({"error": "graph_unavailable"}, 503)
        return _json_response({"error": "source_validation_failed"}, 422)
    except ValueError:
        return _json_response({"error": "source_validation_failed"}, 422)
    except Exception:
        logger.exception("source registration graph validation unavailable")
        return _json_response({"error": "graph_unavailable"}, 503)

    try:
        plan = _build_source_registry_repository(config).register_sharepoint_source(
            source_id=proposal.source_id,
            site_id=site_id,
            site_url=proposal.site_url,
            drive_id=proposal.drive_id,
            business_name=proposal.business_name,
            content_scope=proposal.content_scope,
            chunks_container=proposal.chunks_container,
            manifests_container=proposal.manifests_container,
            retrieval_acl_enabled=proposal.retrieval_acl_enabled,
        )
    except SourceRegistryConflictError as error:
        return _json_response({"error": str(error)}, 409)
    except SourceRegistryError as error:
        code = str(error)
        if code in {
            "source_contentScope_invalid",
            "source_businessName_invalid",
            "source_chunksContainer_invalid",
            "source_manifestsContainer_invalid",
            "source_retrievalAclEnabled_invalid",
        }:
            return _json_response({"error": code}, 400)
        logger.exception(
            "source registration incomplete source=%s",
            proposal.source_id,
        )
        return _json_response({"error": code}, 503)
    except Exception:
        logger.exception(
            "source registration unavailable source=%s",
            proposal.source_id,
        )
        return _json_response({"error": "source_registry_unavailable"}, 503)
    try:
        from ingestion.telemetry import write_audit_record
        from retrieval.auth import easy_auth_object_id
        write_audit_record(_build_audit_container(config), plan.source_id, "source-registration", {
            "operation": "source_registered",
            "businessName": plan.source.business_name,
            "catalogEtag": plan.catalog_etag,
            "operatorObjectId": easy_auth_object_id(req.headers.get("X-MS-CLIENT-PRINCIPAL")),
            "operator": req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME", "unknown"),
        })
    except Exception:
        logger.exception("source registration audit unavailable source=%s", plan.source_id)
    return _json_response({
        "sourceId": plan.source_id,
        "siteId": plan.source.site_id,
        "siteUrl": plan.source.site_url,
        "driveId": plan.source.drive_id,
        "businessName": plan.source.business_name,
        "contentScope": plan.source.content_scope,
        "chunksContainer": plan.source.chunks_container,
        "manifestsContainer": plan.source.manifests_container,
        "retrievalAclEnabled": plan.source.retrieval_acl_enabled,
        "desiredState": plan.source.desired_state,
        "etag": plan.catalog_etag,
    }, 201)


@app.route(
    route="ingestion/sources/preflight",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def preflight_source(req: func.HttpRequest) -> func.HttpResponse:
    """Validate one proposed source and target pair without persisting state."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    import httpx
    from config import load_config
    from ingestion import graph
    from ingestion.source_preflight import (
        ProposedSourceRegistration,
        SourcePreflightError,
        inspect_content_pair,
    )
    from source_registry import SourceRegistryError

    try:
        body = req.get_json()
    except ValueError:
        body = None
    try:
        proposal = ProposedSourceRegistration.from_body(body)
    except SourceRegistryError as error:
        return _json_response({"error": str(error)}, 400)

    try:
        config = load_config()
        site_id = graph.validate_sharepoint_drive_site(
            _build_graph_client(config),
            proposal.drive_id,
            proposal.site_url,
            config.execution.acl_max_pages,
        )
    except httpx.HTTPStatusError as error:
        status_code = error.response.status_code
        code = (
            "source_preflight_graph_unavailable"
            if status_code == 429 or status_code >= 500
            else "source_preflight_graph_binding_invalid"
        )
        return _source_preflight_error(
            proposal.source_id,
            code,
            "microsoft-graph",
            503 if code.endswith("unavailable") else 422,
        )
    except ValueError:
        return _source_preflight_error(
            proposal.source_id,
            "source_preflight_graph_binding_invalid",
            "microsoft-graph",
            422,
        )
    except Exception:
        logger.exception(
            "source preflight Graph validation unavailable source=%s",
            proposal.source_id,
        )
        return _source_preflight_error(
            proposal.source_id,
            "source_preflight_graph_unavailable",
            "microsoft-graph",
            503,
        )

    try:
        database = _build_cosmos_client(config).get_database_client(
            config.cosmos_database,
        )
        pair = inspect_content_pair(database, proposal)
    except SourcePreflightError as error:
        status_code = (
            422
            if error.code in {
                "source_preflight_container_missing",
                "source_preflight_container_incompatible",
            }
            else 503
        )
        return _source_preflight_error(
            proposal.source_id,
            error.code,
            error.dependency,
            status_code,
        )
    except Exception:
        logger.exception(
            "source preflight Cosmos validation unavailable source=%s",
            proposal.source_id,
        )
        return _source_preflight_error(
            proposal.source_id,
            "source_preflight_container_unavailable",
            "cosmos-content-pair",
            503,
        )

    return _json_response({
        "sourceId": proposal.source_id,
        "siteId": site_id,
        "businessName": proposal.business_name,
        "contentScope": proposal.content_scope,
        "retrievalAclEnabled": proposal.retrieval_acl_enabled,
        "preflightPassed": True,
        "writesPerformed": False,
        "checks": {
            "graphBinding": "passed",
            "contentPairPolicy": "passed",
            "functionDataPlane": {
                "metadataRead": pair["metadataRead"],
                "query": pair["query"],
                "writePermission": pair["functionWritePermission"],
            },
            "retrievalReadiness": {
                "containerPolicy": "passed",
                "readPermission": pair["retrievalReadPermission"],
            },
        },
        "targetPair": {
            "chunksContainer": pair["chunksContainer"],
            "chunksPartitionKey": pair["chunksPartitionKey"],
            "manifestsContainer": pair["manifestsContainer"],
            "manifestsPartitionKey": pair["manifestsPartitionKey"],
        },
    }, 200)


def _source_preflight_error(
    source_id: str,
    code: str,
    dependency: str,
    status_code: int,
) -> func.HttpResponse:
    return _json_response({
        "error": code,
        "sourceId": source_id,
        "dependency": dependency,
        "writesPerformed": False,
    }, status_code)


@app.route(route="ingestion/sources/{sourceId}", methods=["PATCH"], auth_level=func.AuthLevel.ANONYMOUS)
def change_source_state(req: func.HttpRequest) -> func.HttpResponse:
    """Change a source lifecycle state or its retrieval ACL policy under If-Match."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    from config import load_config
    from ingestion.source_registry import SourceRegistryConflictError
    from source_registry import (
        SourceRegistryError,
        validate_business_name,
        validate_source_id,
    )

    try:
        source_id = validate_source_id(req.route_params.get("sourceId"))
    except SourceRegistryError as error:
        return _json_response({"error": str(error)}, 400)
    try:
        body = req.get_json()
    except ValueError:
        body = None
    if_match = req.headers.get("If-Match")
    if not if_match:
        return _json_response({"error": "if_match_required"}, 428)
    if not isinstance(body, dict):
        return _json_response({"error": "source_state_invalid"}, 400)
    if set(body) == {"businessName"}:
        try:
            business_name = validate_business_name(body["businessName"])
        except SourceRegistryError as error:
            return _json_response({"error": str(error)}, 400)
        try:
            plan, previous_business_name = (
                _build_source_registry_repository(load_config())
                .change_source_business_name(
                    source_id=source_id,
                    business_name=business_name,
                    if_match=if_match,
                )
            )
        except SourceRegistryConflictError as error:
            code = str(error)
            return _json_response(
                {"error": code},
                412 if code == "source_etag_mismatch" else 409,
            )
        except SourceRegistryError as error:
            code = str(error)
            if code == "source_registration_missing":
                return _json_response({"error": code}, 404)
            if code == "source_businessName_invalid":
                return _json_response({"error": code}, 400)
            logger.exception("source business name change incomplete source=%s", source_id)
            return _json_response({"error": code}, 503)
        except Exception:
            logger.exception("source business name change unavailable source=%s", source_id)
            return _json_response({"error": "source_registry_unavailable"}, 503)
        try:
            from ingestion.telemetry import write_audit_record
            from retrieval.auth import easy_auth_object_id
            write_audit_record(
                _build_audit_container(load_config()),
                plan.source_id,
                "source-business-name",
                {
                    "operation": "source_business_name_changed",
                    "previousBusinessName": previous_business_name,
                    "businessName": plan.source.business_name,
                    "catalogEtag": plan.catalog_etag,
                    "operatorObjectId": easy_auth_object_id(
                        req.headers.get("X-MS-CLIENT-PRINCIPAL")
                    ),
                    "operator": req.headers.get(
                        "X-MS-CLIENT-PRINCIPAL-NAME",
                        "unknown",
                    ),
                },
            )
        except Exception:
            logger.exception("source business name audit unavailable source=%s", plan.source_id)
        return _json_response({
            "sourceId": plan.source_id,
            "businessName": plan.source.business_name,
            "etag": plan.catalog_etag,
        }, 200)
    if set(body) == {"desiredState"}:
        if not isinstance(body["desiredState"], str) or body["desiredState"] not in {"active", "paused", "retired"}:
            return _json_response({"error": "source_state_invalid"}, 400)
        try:
            plan = _build_source_registry_repository(load_config()).change_source_state(
                source_id=source_id, desired_state=body["desiredState"], if_match=if_match,
            )
        except SourceRegistryConflictError as error:
            code = str(error)
            return _json_response({"error": code}, 412 if code == "source_etag_mismatch" else 409)
        except SourceRegistryError as error:
            code = str(error)
            if code == "source_registration_missing":
                return _json_response({"error": code}, 404)
            if code == "source_binding_not_accepted":
                return _json_response({"error": code}, 409)
            logger.exception("source state change incomplete source=%s", source_id)
            return _json_response({"error": code}, 503)
        except Exception:
            logger.exception("source state change unavailable source=%s", source_id)
            return _json_response({"error": "source_registry_unavailable"}, 503)
        try:
            from ingestion.telemetry import write_audit_record
            from retrieval.auth import easy_auth_object_id
            write_audit_record(_build_audit_container(load_config()), plan.source_id, "source-state", {
                "operation": "source_state_changed",
                "businessName": plan.source.business_name,
                "desiredState": plan.source.desired_state,
                "catalogEtag": plan.catalog_etag,
                "operatorObjectId": easy_auth_object_id(req.headers.get("X-MS-CLIENT-PRINCIPAL")),
                "operator": req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME", "unknown"),
            })
        except Exception:
            logger.exception("source state audit unavailable source=%s", plan.source_id)
        return _json_response({
            "sourceId": plan.source_id,
            "siteUrl": plan.source.site_url,
            "driveId": plan.source.drive_id,
            "desiredState": plan.source.desired_state,
            "etag": plan.catalog_etag,
        }, 200)
    if set(body) == {"retrievalAclEnabled"}:
        value = body["retrievalAclEnabled"]
        if type(value) is not bool:
            return _json_response({"error": "source_retrievalAclEnabled_invalid"}, 400)
        try:
            plan = _build_source_registry_repository(load_config()).change_source_policy(
                source_id=source_id,
                retrieval_acl_enabled=value,
                if_match=if_match,
            )
        except SourceRegistryConflictError as error:
            code = str(error)
            return _json_response({"error": code}, 412 if code == "source_etag_mismatch" else 409)
        except SourceRegistryError as error:
            code = str(error)
            if code == "source_registration_missing":
                return _json_response({"error": code}, 404)
            if code == "source_retrievalAclEnabled_invalid":
                return _json_response({"error": code}, 400)
            logger.exception("source policy change incomplete source=%s", source_id)
            return _json_response({"error": code}, 503)
        except Exception:
            logger.exception("source policy change unavailable source=%s", source_id)
            return _json_response({"error": "source_registry_unavailable"}, 503)
        try:
            from ingestion.telemetry import write_audit_record
            from retrieval.auth import easy_auth_object_id
            write_audit_record(_build_audit_container(load_config()), plan.source_id, "source-policy", {
                "operation": "source_policy_changed",
                "businessName": plan.source.business_name,
                "retrievalAclEnabled": plan.source.retrieval_acl_enabled,
                "catalogEtag": plan.catalog_etag,
                "operatorObjectId": easy_auth_object_id(req.headers.get("X-MS-CLIENT-PRINCIPAL")),
                "operator": req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME", "unknown"),
            })
        except Exception:
            logger.exception("source policy audit unavailable source=%s", plan.source_id)
        return _json_response({
            "sourceId": plan.source_id,
            "siteUrl": plan.source.site_url,
            "driveId": plan.source.drive_id,
            "desiredState": plan.source.desired_state,
            "retrievalAclEnabled": plan.source.retrieval_acl_enabled,
            "etag": plan.catalog_etag,
        }, 200)
    return _json_response({"error": "source_state_invalid"}, 400)


@app.route(
    route="ingestion/sources/{sourceId}/full-sync", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS,
)
@app.durable_client_input(client_name="client")
async def start_source_full_sync(req: func.HttpRequest, client) -> func.HttpResponse:
    """Start a full sync for one active source without rescanning the other sources."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    from config import load_config
    from ingestion.source_registry import SourceRegistryConflictError
    from source_registry import SourceRegistryError, validate_source_id

    try:
        source_id = validate_source_id(req.route_params.get("sourceId"))
    except SourceRegistryError as error:
        return _json_response({"error": str(error)}, 400)
    try:
        registry = _build_source_registry_repository(load_config())
        plan = registry.read_source_plan(source_id)
        coordinator = registry.get_drive_coordinator(source_id)
    except SourceRegistryConflictError as error:
        return _json_response({"error": str(error)}, 409)
    except SourceRegistryError as error:
        code = str(error)
        if code == "source_registration_missing":
            return _json_response({"error": code}, 404)
        if code == "source_binding_not_accepted":
            return _json_response({"error": code}, 409)
        logger.exception("source full-sync preflight incomplete source=%s", source_id)
        return _json_response({"error": code}, 503)
    except Exception:
        logger.exception("source full-sync preflight unavailable source=%s", source_id)
        return _json_response({"error": "source_registry_unavailable"}, 503)
    reprovision_bootstrap = (
        plan.source.desired_state == "paused"
        and registry.is_reprovision_full_sync_required(source_id)
    )
    if plan.source.desired_state != "active" and not reprovision_bootstrap:
        return _json_response({"error": "source_not_active"}, 409)
    # Best-effort immediate feedback; acquire_drive_coordinator remains the race-safe guard.
    if coordinator is not None and coordinator.get("state") in {"active", "draining", "maintenance"}:
        return _json_response({"error": "source_busy", "operation": coordinator.get("operation")}, 409)
    source_payload = plan.to_payload()
    if reprovision_bootstrap:
        source_payload["reprovisionBootstrap"] = True
    instance_id = await client.start_new("full_sync_orchestrator", client_input=source_payload)
    return client.create_check_status_response(req, instance_id)


@app.route(
    route="ingestion/sources/{sourceId}/reprovision",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
@app.durable_client_input(client_name="client")
async def reprovision_source(req: func.HttpRequest, client) -> func.HttpResponse:
    """Rebuild the same fully retired source identity as paused after bounded cleanup."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    import httpx
    import uuid
    from config import load_config
    from ingestion import graph
    from ingestion.source_registry import SourceRegistryConflictError
    from retrieval.auth import easy_auth_object_id
    from source_registry import SourceRegistryError, validate_source_id

    try:
        source_id = validate_source_id(req.route_params.get("sourceId"))
    except SourceRegistryError as error:
        return _json_response({"error": str(error)}, 400)
    try:
        body = req.get_json()
    except ValueError:
        body = None
    if body != {"confirm": "reprovision"}:
        return _json_response({"error": "source_reprovision_confirmation_required"}, 400)
    if_match = req.headers.get("If-Match")
    if not if_match:
        return _json_response({"error": "if_match_required"}, 428)

    try:
        config = load_config()
        registry = _build_source_registry_repository(config)
        await _release_coordinator_of_ended_owner(client, registry, source_id)
        plan = registry.inspect_source_reprovision(
            source_id=source_id,
            if_match=if_match,
        )
        validated_site_id = graph.validate_sharepoint_drive_site(
            _build_graph_client(config),
            plan.source.drive_id,
            plan.source.site_url,
            config.execution.acl_max_pages,
        )
        if validated_site_id != plan.source.site_id:
            raise ValueError("source_site_binding_changed")
    except SourceRegistryConflictError as error:
        code = str(error)
        return _json_response({"error": code}, 412 if code == "source_etag_mismatch" else 409)
    except SourceRegistryError as error:
        code = str(error)
        if code == "source_registration_missing":
            return _json_response({"error": code}, 404)
        logger.exception("source reprovision preflight incomplete source=%s", source_id)
        return _json_response({"error": code}, 503)
    except httpx.HTTPStatusError as error:
        status_code = error.response.status_code
        if status_code == 429 or status_code >= 500:
            logger.warning("source reprovision graph unavailable status=%s", status_code)
            return _json_response({"error": "graph_unavailable"}, 503)
        return _json_response({"error": "source_validation_failed"}, 422)
    except ValueError:
        return _json_response({"error": "source_validation_failed"}, 422)
    except Exception:
        logger.exception("source reprovision preflight unavailable source=%s", source_id)
        return _json_response({"error": "source_registry_unavailable"}, 503)

    operator_object_id = easy_auth_object_id(req.headers.get("X-MS-CLIENT-PRINCIPAL"))
    if operator_object_id is None:
        return _json_response({"error": "operator_object_id_required"}, 403)
    instance_id = f"source-reprovision-{uuid.uuid4()}"
    try:
        await client.start_new(
            "source_reprovision_orchestrator",
            instance_id=instance_id,
            client_input={
                "sourcePlan": plan.to_payload(),
                "operatorObjectId": operator_object_id,
                "operator": req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME", "unknown"),
            },
        )
    except Exception:
        logger.exception("source reprovision orchestration start failed source=%s", source_id)
        return _json_response({"error": "source_reprovision_start_failed"}, 503)
    return client.create_check_status_response(req, instance_id)


@app.route(route="ingestion/sources", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def list_sources(req: func.HttpRequest) -> func.HttpResponse:
    """Return one bounded page of registered sources; pass continuationToken for the next."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    from config import load_config

    try:
        page = _build_source_registry_repository(load_config()).list_source_inventory_page(
            page_size=EXECUTION_CONFIG.source_dispatch_page_size,
            continuation_token=req.params.get("continuationToken") or None,
        )
    except Exception:
        logger.exception("source list unavailable")
        return _json_response({"error": "source_registry_unavailable"}, 503)
    return _json_response({
        "sources": [
            {
                "sourceId": plan.source_id,
                "siteUrl": plan.source.site_url,
                "driveId": plan.source.drive_id,
                "desiredState": plan.source.desired_state,
                "etag": plan.catalog_etag,
            }
            for plan in page.plans
        ],
        "rejectedSourceIds": list(page.rejected_source_ids),
        "unattributedErrors": page.unattributed_errors,
        "duplicateDriveSourceIds": list(page.duplicate_drive_source_ids),
        "continuationToken": page.continuation_token,
    }, 200)


@app.route(route="ingestion/inspect", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def inspect_data(req: func.HttpRequest) -> func.HttpResponse:
    """Read Cosmos containers using the configured authentication for debugging."""
    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization
    from config import load_config
    from cosmos_connection import create_cosmos_client
    from ingestion.source_registry import SourceRegistryConflictError
    from source_registry import SourceRegistryError, validate_source_id

    container_name = (req.params.get("container") or "").strip()
    allowed = {"ingestion-runs", "source-documents", "search-chunks", "service-audit", "retrieval-config"}
    if container_name not in allowed:
        return _json_response({"error": "invalid_container", "allowed": sorted(allowed)}, 400)

    limit_value = req.params.get("limit")
    try:
        limit = int(limit_value) if limit_value else 10
    except (TypeError, ValueError):
        return _json_response({"error": "invalid_limit"}, 400)
    if limit < 1:
        return _json_response({"error": "invalid_limit"}, 400)
    limit = min(limit, 200)
    run_id = (req.params.get("runId") or "").strip()
    source_id = req.params.get("sourceId") or ""
    owner_instance_id = (req.params.get("ownerInstanceId") or "").strip()
    unresolved_only = (req.params.get("unresolvedOnly") or "").strip().lower()
    document_key = (req.params.get("documentKey") or "").strip()
    recovery_filter_requested = bool(owner_instance_id or unresolved_only)
    source_container_requested = container_name in {"source-documents", "search-chunks"}

    if recovery_filter_requested and (
        container_name != "ingestion-runs"
        or not source_id
        or not owner_instance_id
        or unresolved_only != "true"
        or bool(run_id)
    ):
        return _json_response({
            "error": (
                "sourceId, ownerInstanceId, and unresolvedOnly=true are required "
                "only with ingestion-runs"
            ),
        }, 400)
    if source_container_requested and not source_id:
        return _json_response({
            "error": "sourceId is required with source-documents or search-chunks",
        }, 400)
    if run_id and (container_name != "source-documents" or recovery_filter_requested):
        return _json_response({
            "error": "runId is supported only with sourceId and source-documents",
        }, 400)
    if document_key and (container_name != "search-chunks" or recovery_filter_requested):
        return _json_response({
            "error": "documentKey is supported only with sourceId and search-chunks",
        }, 400)
    if (
        not recovery_filter_requested
        and source_id
        and not source_container_requested
    ):
        return _json_response({
            "error": "sourceId is supported only with registered source containers",
        }, 400)

    # retrieval-config is environment-local and partitioned by item id.
    if container_name == "retrieval-config":
        if "deploymentInstanceId" in req.params:
            return _json_response({"error": "deploymentInstanceId is not valid for retrieval-config"}, 400)
        if run_id:
            return _json_response({"error": "runId is not valid for retrieval-config"}, 400)

    resolved_container = container_name
    if source_container_requested:
        try:
            source_id = validate_source_id(source_id)
        except SourceRegistryError as error:
            return _json_response({"error": str(error)}, 400)
        try:
            plan = _build_source_registry_repository(load_config()).read_source_plan(source_id)
        except SourceRegistryConflictError as error:
            return _json_response({"error": str(error)}, 409)
        except SourceRegistryError as error:
            code = str(error)
            if code == "source_registration_missing":
                return _json_response({"error": code}, 404)
            logger.exception("source inspection preflight incomplete source=%s", source_id)
            return _json_response({"error": code}, 503)
        except Exception:
            logger.exception("source inspection preflight unavailable source=%s", source_id)
            return _json_response({"error": "source_registry_unavailable"}, 503)
        resolved_container = (
            plan.source.manifests_container
            if container_name == "source-documents"
            else plan.source.chunks_container
        )

    try:
        cosmos = create_cosmos_client(os.getenv("COSMOS_ENDPOINT", ""), os.getenv("AZURE_CLIENT_ID", ""))
        db = cosmos.get_database_client(os.getenv("COSMOS_DATABASE_NAME", ""))
        container = db.get_container_client(resolved_container)

        if source_container_requested:
            parameters = [
                {"name": "@limit", "value": limit},
                {"name": "@sourceId", "value": source_id},
            ]
            query_kwargs = {}
            if run_id:
                query_kwargs["partition_key"] = f"{source_id}:{run_id}"
            elif document_key:
                query_kwargs["partition_key"] = document_key
            else:
                query_kwargs["enable_cross_partition_query"] = True
            rows = list(container.query_items(
                query="SELECT TOP @limit * FROM c WHERE c.sourceId = @sourceId",
                parameters=parameters,
                **query_kwargs,
            ))
        elif recovery_filter_requested:
            rows = list(container.query_items(
                query=(
                    "SELECT TOP @limit * FROM c WHERE c.recordType = @recordType "
                    "AND c.ownerInstanceId = @owner "
                    "AND ARRAY_CONTAINS(@states, c.state)"
                ),
                parameters=[
                    {"name": "@limit", "value": limit},
                    {"name": "@recordType", "value": "document-operation"},
                    {"name": "@owner", "value": owner_instance_id},
                    {"name": "@states", "value": ["claimed", "running", "unresolved"]},
                ],
                partition_key=source_id,
            ))
        elif container_name == "retrieval-config":
            rows = list(container.query_items(
                query="SELECT TOP @limit * FROM c",
                parameters=[{"name": "@limit", "value": limit}],
                enable_cross_partition_query=True,
            ))
        elif run_id:
            rows = list(container.query_items(
                query="SELECT TOP @limit * FROM c",
                parameters=[{"name": "@limit", "value": limit}],
                partition_key=f"{source_id}:{run_id}",
            ))
        else:
            # service-audit is self-partitioned (partition key = each record's own id), so
            # order by recordedAt to avoid an arbitrary cross-partition sample.
            order_by = " ORDER BY c.recordedAt DESC" if container_name == "service-audit" else ""
            rows = list(container.query_items(
                query=f"SELECT TOP @limit * FROM c{order_by}",
                parameters=[{"name": "@limit", "value": limit}],
                enable_cross_partition_query=True,
            ))
    except Exception:
        logger.exception("inspect_data failed")
        return _json_response({"error": "cosmos_query_failed"}, 503)

    sanitized = [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows]
    return _json_response({
        "container": container_name,
        "resolvedContainer": resolved_container,
        "count": len(sanitized),
        "rows": sanitized,
    }, 200)


def _build_retrieval_config_container() -> Any:
    from config import load_config
    from cosmos_connection import create_cosmos_client

    config = load_config()
    cosmos = create_cosmos_client(
        config.cosmos_endpoint,
        config.managed_identity_client_id,
    )
    database = cosmos.get_database_client(config.cosmos_database)
    return database.get_container_client(
        os.getenv("RETRIEVAL_CONFIG_CONTAINER", "retrieval-config").strip()
        or "retrieval-config"
    )


def _catalog_response(item: dict[str, Any], status: str, status_code: int) -> func.HttpResponse:
    from retrieval.catalog import catalog_body, load_catalog_item

    snapshot = load_catalog_item(item)
    return _json_response(
        {
            "status": status,
            "catalog": catalog_body(item),
            "etag": snapshot.etag,
            "catalogDigest": snapshot.digest,
        },
        status_code,
        headers={"ETag": snapshot.etag} if snapshot.etag is not None else None,
    )


@app.route(
    route="management/retrieval-config",
    methods=["GET", "PUT"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def manage_retrieval_config(req: func.HttpRequest) -> func.HttpResponse:
    """Read or conditionally publish the environment-local runtime catalog."""
    from retrieval.catalog import (
        CATALOG_ID,
        CatalogConflictError,
        CatalogError,
        CatalogSizeError,
        apply_catalog_operation,
        build_catalog_item,
        catalog_body,
        load_catalog_item,
        prepare_catalog_operation,
    )

    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization

    body: Any = None
    proposed: dict[str, Any] | None = None
    if req.method == "PUT":
        declared_length = (req.headers.get("Content-Length") or "").strip()
        if (
            declared_length.isdecimal()
            and int(declared_length) > MAX_RETRIEVAL_CONFIG_REQUEST_BYTES
        ):
            return _json_response({"error": "catalog_request_too_large"}, 413)
        raw_body = req.get_body()
        if len(raw_body) > MAX_RETRIEVAL_CONFIG_REQUEST_BYTES:
            return _json_response({"error": "catalog_request_too_large"}, 413)
        try:
            body = json.loads(raw_body)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError):
            return _json_response({"error": "invalid_json"}, 400)
        if not isinstance(body, dict) or set(body) != {"config", "operationId", "reason"}:
            return _json_response({"error": "invalid_catalog_request"}, 400)

        if_match = (req.headers.get("If-Match") or "").strip()
        if_none_match = (req.headers.get("If-None-Match") or "").strip()
        bootstrap = if_none_match == "*" and not if_match
        update = bool(if_match) and not if_none_match and if_match != "*"
        if not bootstrap and not update:
            return _json_response({"error": "catalog_precondition_required"}, 428)

        source = {"config": body["config"]}
        operation_id = body["operationId"]
        reason = body["reason"]
        try:
            proposed = build_catalog_item(
                source,
                operation_id=operation_id,
                changed_at=datetime.now(timezone.utc),
                reason=reason,
            )
        except CatalogSizeError:
            return _json_response({"error": "catalog_request_too_large"}, 413)
        except CatalogError:
            return _json_response({"error": "invalid_catalog"}, 400)

    try:
        container = _build_retrieval_config_container()
    except Exception:
        logger.exception("retrieval catalog container unavailable")
        return _json_response({"error": "retrieval_config_unavailable"}, 503)

    if req.method == "GET":
        try:
            item = container.read_item(item=CATALOG_ID, partition_key=CATALOG_ID)
            return _catalog_response(item, "current", 200)
        except Exception as error:
            if getattr(error, "status_code", None) == 404:
                return _json_response({"error": "catalog_not_found"}, 404)
            logger.exception("retrieval catalog read failed")
            return _json_response({"error": "retrieval_config_unavailable"}, 503)

    assert isinstance(body, dict)
    assert proposed is not None

    try:
        current = container.read_item(item=CATALOG_ID, partition_key=CATALOG_ID)
    except Exception as error:
        if getattr(error, "status_code", None) == 404:
            current = None
        else:
            logger.exception("retrieval catalog precondition read failed")
            return _json_response({"error": "retrieval_config_unavailable"}, 503)

    if current is not None:
        try:
            current_snapshot = load_catalog_item(current)
            if current_snapshot.operation_id == operation_id:
                replay = build_catalog_item(
                    source,
                    operation_id=operation_id,
                    changed_at=datetime.fromisoformat(
                        current_snapshot.changed_at.replace("Z", "+00:00")
                    ),
                    reason=reason,
                )
                if catalog_body(current) != replay:
                    return _json_response({"error": "operation_id_reused"}, 409)
                return _catalog_response(current, "replayed", 200)
        except CatalogError:
            logger.exception("persisted retrieval catalog is invalid")
            return _json_response({"error": "retrieval_config_unavailable"}, 503)

    if bootstrap:
        if current is not None:
            return _json_response({"error": "catalog_already_exists"}, 412)
        try:
            created = container.create_item(body=proposed)
            persisted = container.read_item(item=CATALOG_ID, partition_key=CATALOG_ID)
            if catalog_body(created) != catalog_body(persisted):
                raise CatalogConflictError("created catalog verification failed")
            return _catalog_response(persisted, "created", 201)
        except CatalogConflictError:
            return _json_response({"error": "catalog_create_conflict"}, 409)
        except Exception as error:
            if getattr(error, "status_code", None) == 409:
                return _json_response({"error": "catalog_already_exists"}, 412)
            logger.exception("retrieval catalog create failed")
            return _json_response({"error": "retrieval_config_unavailable"}, 503)

    if current is None:
        return _json_response({"error": "catalog_not_found"}, 412)
    try:
        snapshot = load_catalog_item(current)
    except CatalogError:
        logger.exception("persisted retrieval catalog is invalid")
        return _json_response({"error": "retrieval_config_unavailable"}, 503)
    if snapshot.etag != if_match:
        return _json_response({"error": "catalog_etag_mismatch"}, 412)

    try:
        candidate, manifest = prepare_catalog_operation(
            source,
            operation_id=operation_id,
            changed_at=datetime.now(timezone.utc),
            expected_etag=if_match,
            reason=reason,
        )
    except CatalogError:
        return _json_response({"error": "invalid_catalog"}, 400)
    try:
        outcome = apply_catalog_operation(container, candidate, manifest)
    except CatalogConflictError as error:
        code = "catalog_etag_mismatch" if "ETag" in str(error) or "stale" in str(error) else "catalog_conflict"
        return _json_response({"error": code}, 412 if code == "catalog_etag_mismatch" else 409)
    except CatalogError:
        logger.exception("retrieval catalog guarded write failed")
        return _json_response({"error": "retrieval_config_unavailable"}, 503)
    if outcome["status"] == "outcome-unknown":
        return _json_response(
            {"error": "catalog_outcome_unknown", "operationId": operation_id},
            503,
        )
    try:
        persisted = container.read_item(item=CATALOG_ID, partition_key=CATALOG_ID)
    except Exception:
        logger.exception("retrieval catalog verification read failed")
        return _json_response(
            {"error": "catalog_outcome_unknown", "operationId": operation_id},
            503,
        )
    return _catalog_response(persisted, "updated", 200)


_PARTITION_KEY_FIELD = {
    "ingestion-runs": "sourceId",
    "source-documents": "sourceRunId",
    "search-chunks": "documentKey",
    "service-audit": "id",
    "retrieval-config": "id",
}


@app.route(route="ingestion/purge", methods=["DELETE"], auth_level=func.AuthLevel.ANONYMOUS)
def purge_data(req: func.HttpRequest) -> func.HttpResponse:
    """Delete items from a Cosmos container. Writes an audit record for every purge."""
    from cosmos_connection import create_cosmos_client
    from ingestion.source_registry import SourceRegistryConflictError
    import uuid

    try:
        body = req.get_json()
    except ValueError:
        return _json_response({"error": "invalid_json"}, 400)

    container_name = (body.get("container") or "").strip()
    purgeable = {"ingestion-runs", "source-documents", "search-chunks", "retrieval-config"}
    if container_name not in purgeable:
        return _json_response({"error": "invalid_container", "allowed": sorted(purgeable)}, 400)
    if container_name == "ingestion-runs":
        return _json_response({"error": "source_control_purge_not_supported"}, 409)

    ids = body.get("ids")
    purge_all = body.get("purgeAll", False)
    confirm = body.get("confirm", "")
    source_id = (body.get("sourceId") or "").strip()

    if not ids and not purge_all:
        return _json_response({"error": "provide 'ids' (list) or 'purgeAll':true"}, 400)
    if purge_all and confirm != "yes":
        return _json_response({"error": "purgeAll requires 'confirm':'yes'"}, 400)
    if ids and (not isinstance(ids, list) or len(ids) > 100):
        return _json_response({"error": "ids must be a list with max 100 items"}, 400)

    governed_containers = {"source-documents", "search-chunks"}
    if container_name in governed_containers and (not source_id or confirm != "yes"):
        return _json_response({"error": "sourceId and confirm:'yes' required for ingestion data purge"}, 400)

    # retrieval-config is environment-local; require exact ids and confirmation.
    if container_name == "retrieval-config":
        if purge_all:
            return _json_response({"error": "purgeAll is not permitted for retrieval-config"}, 400)
        if "deploymentInstanceId" in body:
            return _json_response({"error": "deploymentInstanceId is not valid for retrieval-config"}, 400)
        if confirm != "yes":
            return _json_response({"error": "retrieval-config purge requires 'confirm':'yes'"}, 400)

    authorization = _require_ingestion_operator(req)
    if authorization is not None:
        return authorization

    operator = req.headers.get("X-MS-CLIENT-PRINCIPAL-NAME", "unknown")
    pk_field = _PARTITION_KEY_FIELD[container_name]
    purge_registry = None
    purge_owner_id = None
    purge_maintenance_acquired = False
    audit_business_name = None

    try:
        cosmos = create_cosmos_client(os.getenv("COSMOS_ENDPOINT", ""), os.getenv("AZURE_CLIENT_ID", ""))
        db = cosmos.get_database_client(os.getenv("COSMOS_DATABASE_NAME", ""))
        resolved_container_name = container_name
        if container_name in governed_containers:
            from config import load_config
            from ingestion.source_registry import config_for_source

            purge_config = load_config()
            purge_registry = _build_source_registry_repository(purge_config)
            purge_plan = purge_registry.read_source_plan(source_id)
            purge_owner_id = f"operator-purge-{uuid.uuid4().hex}"
            purge_registry.begin_operator_maintenance(
                purge_plan,
                owner_instance_id=purge_owner_id,
                operation="operator-purge",
            )
            purge_maintenance_acquired = True
            purge_config = config_for_source(purge_config, purge_plan)
            audit_business_name = purge_config.business_name
            resolved_container_name = (
                purge_config.cosmos_source_documents_container
                if container_name == "source-documents"
                else purge_config.cosmos_search_chunks_container
            )
        container = db.get_container_client(resolved_container_name)
        audit_container = db.get_container_client("service-audit")
        deleted_ids = []
        failed = 0

        if ids:
            # Read each item to get its partition key, then delete
            for item_id in ids:
                try:
                    if container_name == "retrieval-config":
                        container.delete_item(item=item_id, partition_key=item_id)
                        deleted_ids.append(item_id)
                        continue
                    items = list(container.query_items(
                        query=(
                            f"SELECT c.id, c.{pk_field} as pk FROM c WHERE c.id = @id "
                            "AND c.sourceId = @sourceId"
                        ),
                        parameters=[
                            {"name": "@id", "value": item_id},
                            {"name": "@sourceId", "value": source_id},
                        ],
                        enable_cross_partition_query=True,
                    ))
                    if not items:
                        failed += 1
                        continue
                    pk_value = items[0].get("pk", items[0].get(pk_field, ""))
                    container.delete_item(item=item_id, partition_key=pk_value)
                    deleted_ids.append(item_id)
                except Exception:
                    failed += 1
        else:
            continuation_token = None
            while True:
                iterator = container.query_items(
                    query=f"SELECT c.id, c.{pk_field} FROM c WHERE c.sourceId = @sourceId",
                    parameters=[{"name": "@sourceId", "value": source_id}],
                    enable_cross_partition_query=True,
                    max_item_count=100,
                )
                by_page = getattr(iterator, "by_page", None)
                if callable(by_page):
                    pager = by_page(continuation_token)
                    batch = list(next(pager, []))
                    continuation_token = pager.continuation_token
                else:
                    batch = list(iterator)
                    continuation_token = None
                if not batch:
                    break
                failed_before_page = failed
                for item in batch:
                    try:
                        container.delete_item(item=item["id"], partition_key=item[pk_field])
                        deleted_ids.append(item["id"])
                    except Exception:
                        failed += 1
                if failed > failed_before_page or continuation_token is None:
                    break

        if purge_maintenance_acquired and purge_registry is not None and purge_owner_id is not None:
            purge_registry.settle_drive_coordinator(source_id, purge_owner_id)
            purge_maintenance_acquired = False

        # write_audit_record never raises, so it can't mask a completed purge as a failure.
        from ingestion.telemetry import write_audit_record
        from retrieval.auth import easy_auth_object_id
        audit_id = str(uuid.uuid4())
        audit_source_id = source_id or os.getenv("INGESTION_SOURCE_ID", "").strip()
        write_audit_record(audit_container, audit_source_id, "purge", {
            "id": audit_id,
            "operation": "purge",
            "container": container_name,
            "operatorObjectId": easy_auth_object_id(req.headers.get("X-MS-CLIENT-PRINCIPAL")),
            "operator": operator,
            "deletedIds": deleted_ids[:100],
            "deletedCount": len(deleted_ids),
            "failedCount": failed,
            "purgeAll": purge_all,
        }, business_name=audit_business_name)
        logger.warning("purge_executed container=%s deleted=%d failed=%d operator=%s", container_name, len(deleted_ids), failed, operator)

        return _json_response({"deleted": len(deleted_ids), "failed": failed, "auditId": audit_id}, 200)
    except SourceRegistryConflictError as error:
        logger.warning("purge refused by source coordinator: %s", type(error).__name__)
        if purge_maintenance_acquired and purge_registry is not None and purge_owner_id is not None:
            try:
                purge_registry.settle_drive_coordinator(source_id, purge_owner_id)
            except Exception:
                logger.exception("operator purge maintenance release failed")
        return _json_response({"error": "source_maintenance_not_quiescent"}, 409)
    except Exception:
        logger.exception("purge_data failed")
        if purge_maintenance_acquired and purge_registry is not None and purge_owner_id is not None:
            try:
                purge_registry.settle_drive_coordinator(source_id, purge_owner_id)
            except Exception:
                logger.exception("operator purge maintenance release failed")
        return _json_response({"error": "purge_failed"}, 503)


def _current_full_sync_instance_id(repository, source_id: str) -> str | None:
    """The Durable instance ID of the most recent full-sync run, per Cosmos, or None."""
    control = repository.get_source_control(source_id)
    return control.record.current_orchestration_instance_id if control else None


def _mark_coordinator_draining_safely(registry: Any, source_id: str, owner_instance_id: str) -> None:
    try:
        coordinator = registry.get_drive_coordinator(source_id)
        if (
            coordinator is not None
            and coordinator.get("ownerInstanceId") == owner_instance_id
            and coordinator.get("state") == "active"
        ):
            registry.mark_drive_coordinator_draining(source_id, owner_instance_id)
    except Exception:
        logger.exception("coordinator drain transition failed source=%s", source_id)


# Matches host.json functionTimeout: activities of a terminated owner can keep running
# until then (terminate does not stop in-flight activities).
_ENDED_OWNER_QUIET_PERIOD = timedelta(minutes=30)


async def _release_coordinator_of_ended_owner(client, registry: Any, source_id: str) -> None:
    """Settle a drive lock whose owner run has ended, so no operator terminate is needed."""
    try:
        coordinator = registry.get_drive_coordinator(source_id)
        if coordinator is None or (
            coordinator.get("state") not in {"active", "draining"}
            and not (
                coordinator.get("state") == "maintenance"
                and coordinator.get("operation") in {
                    "source-retirement", "source-reprovision",
                }
            )
        ):
            return
        if coordinator.get("activeWorkCount", 0) != 0:
            return
        updated_at = datetime.fromisoformat(str(coordinator.get("updatedAt")).replace("Z", "+00:00"))
        if datetime.now(timezone.utc) - updated_at < _ENDED_OWNER_QUIET_PERIOD:
            return
        owner_instance_id = coordinator.get("ownerInstanceId")
        status = await client.get_status(owner_instance_id)
        if status is None or status.runtime_status not in (
            df.OrchestrationRuntimeStatus.Completed,
            df.OrchestrationRuntimeStatus.Failed,
            df.OrchestrationRuntimeStatus.Terminated,
            df.OrchestrationRuntimeStatus.Canceled,
        ):
            return
        registry.settle_drive_coordinator(source_id, owner_instance_id)
        logger.info(
            "released drive lock of ended owner source=%s owner=%s status=%s",
            source_id, owner_instance_id, status.runtime_status,
        )
    except Exception:
        logger.exception("ended-owner lock release failed source=%s", source_id)


def _source_dispatch_child_instance_id(dispatch_id: str, source_id: str) -> str:
    digest = hashlib.sha256(f"{dispatch_id}:{source_id}".encode("utf-8")).hexdigest()
    return f"sharepoint-drive-{digest[:32]}"


def _source_dispatch_outcome_summary(outcome: Any) -> dict[str, Any]:
    if not isinstance(outcome, dict):
        return {"status": "failed", "error": "source_orchestration_failed"}
    keys = ("status", "phase", "runId", "discovered", "succeeded", "failed", "pending", "error")
    return {key: outcome[key] for key in keys if key in outcome and isinstance(outcome[key], (str, int))}


def _full_sync_is_running(repository, source_id: str) -> bool:
    """True if full_sync_orchestrator is currently active for this source.

    delta-sync/acl-resync skip their tick while this is true: full-sync's
    discover_all() has no knowledge of delta-sync's cursor (and vice versa), so
    running them concurrently on the same changed file could create two
    simultaneously-"ready" versions with neither retiring the other.

    Checked via Cosmos rather than Durable's per-instance-ID status: this app never
    reuses instance IDs, so there is no fixed ID to poll (see start_full_sync).
    """
    from ingestion.models import RunStatus
    control = repository.get_source_control(source_id)
    if control is None:
        return False
    run = repository.get_run(source_id, control.record.current_run_id)
    return run is not None and run.record.status == RunStatus.RUNNING


async def _start_if_not_running(
    client,
    lifecycle_repository,
    source_id: str,
    control_id: str,
    orchestrator_name: str,
    *,
    client_input: Any = None,
    source_registry: Any = None,
) -> str | None:
    """Start `orchestrator_name` with a fresh, never-reused instance ID, unless the tick
    tracked under `control_id` is still Running/Pending. Returns the new instance ID, or
    None if a previous tick is still active.

    Mirrors start_full_sync's fix: Durable instance-ID reuse is best-effort/racy at the
    storage layer (Azure/azure-functions-durable-python#410), so we never poll or start a
    fixed ID — the last-dispatched ID is tracked in Cosmos purely so we know what to poll.
    """
    import uuid
    existing_id = lifecycle_repository.get_trigger_instance_id(source_id, control_id)
    if existing_id:
        existing = await client.get_status(existing_id)
        if existing and existing.runtime_status in (
            df.OrchestrationRuntimeStatus.Running,
            df.OrchestrationRuntimeStatus.Pending,
        ):
            return None
    if source_registry is not None:
        await _release_coordinator_of_ended_owner(client, source_registry, source_id)
    instance_id = f"{control_id}-{uuid.uuid4().hex}"
    claimed = lifecycle_repository.try_claim_trigger_instance_id(
        source_id,
        control_id,
        existing_id,
        instance_id,
    )
    if not claimed:
        return None
    if client_input is None:
        await client.start_new(orchestrator_name, instance_id=instance_id)
    else:
        await client.start_new(
            orchestrator_name,
            instance_id=instance_id,
            client_input=client_input,
        )
    return instance_id


def _load_validated_source_plans(config):
    from ingestion.source_registry import SourceRegistryConflictError
    from source_registry import SourceRegistryError

    registry = _build_source_registry_repository(config)
    continuation_token = None
    while True:
        page = registry.list_source_inventory_page(
            page_size=EXECUTION_CONFIG.source_dispatch_page_size,
            continuation_token=continuation_token,
        )
        for source_id in page.duplicate_drive_source_ids:
            logger.warning("registered source rejected source=%s error=sharepoint_drive_already_registered", source_id)
        for plan in page.plans:
            try:
                accepted = registry.read_source_plan(plan.source_id)
            except (SourceRegistryError, SourceRegistryConflictError) as error:
                logger.warning(
                    "registered source binding rejected source=%s error=%s",
                    plan.source_id,
                    type(error).__name__,
                )
                continue
            yield accepted
        continuation_token = page.continuation_token
        if continuation_token is None:
            return


def _match_webhook_source_plans(notifications: list[Any], plans, lifecycle_repository):
    requested_subscriptions: set[str] = set()
    for notification in notifications:
        if not isinstance(notification, dict):
            raise ValueError("webhook_notification_invalid")
        subscription_id = notification.get("subscriptionId")
        if not isinstance(subscription_id, str) or not subscription_id:
            raise ValueError("webhook_notification_invalid")
        requested_subscriptions.add(subscription_id)
    subscriptions = {}
    for plan in plans:
        subscription_id = lifecycle_repository.get_webhook_subscription_id(plan.source_id)
        if subscription_id in requested_subscriptions:
            subscriptions[subscription_id] = plan
    matched = {}
    for notification in notifications:
        subscription_id = notification.get("subscriptionId")
        plan = subscriptions.get(subscription_id)
        if plan is None:
            logger.warning("webhook subscription not matched to an accepted source; ignored")
            continue
        if plan.source.desired_state == "active":
            matched[plan.source_id] = plan
    return tuple(matched.values())


def _lifecycle_subscription_action(plan, event: object) -> str:
    if plan.source.desired_state == "retired":
        return "delete"
    if plan.source.desired_state == "paused":
        return "ignore"
    return "reauthorize" if event == "reauthorizationRequired" else "unsupported"


@app.timer_trigger(schedule=DELTA_SYNC_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
@app.durable_client_input(client_name="client")
async def reconciliation_timer(timer: func.TimerRequest, client) -> None:
    """Daily safety-net delta-sync — catches changes missed by webhooks."""
    from config import load_config
    from ingestion.lifecycle_repository import DELTA_SYNC_TRIGGER_ID
    from ingestion.source_registry import config_for_source
    config = load_config()
    repository = _build_repository(config)
    lifecycle_repository = _build_lifecycle_repository(config)
    source_registry = _build_source_registry_repository(config)
    for plan in _load_validated_source_plans(config):
        if plan.source.desired_state != "active":
            continue
        try:
            source_config = config_for_source(config, plan)
            if _full_sync_is_running(repository, source_config.source_id):
                continue
            await _start_if_not_running(
                client, lifecycle_repository, source_config.source_id,
                DELTA_SYNC_TRIGGER_ID, "delta_sync_orchestrator",
                client_input=plan.to_payload(),
                source_registry=source_registry,
            )
        except Exception:
            logger.exception("delta timer source dispatch failed source=%s", plan.source_id)


@app.timer_trigger(schedule=ACL_RESYNC_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
@app.durable_client_input(client_name="client")
async def acl_resync_timer(timer: func.TimerRequest, client) -> None:
    """Kick off one ACL-resync pass unless the previous pass, or a full-sync,
    is still running."""
    from config import load_config
    from ingestion.lifecycle_repository import ACL_RESYNC_TRIGGER_ID
    from ingestion.source_registry import config_for_source
    config = load_config()
    repository = _build_repository(config)
    lifecycle_repository = _build_lifecycle_repository(config)
    source_registry = _build_source_registry_repository(config)
    for plan in _load_validated_source_plans(config):
        if plan.source.desired_state != "active":
            continue
        try:
            source_config = config_for_source(config, plan)
            if _full_sync_is_running(repository, source_config.source_id):
                continue
            await _start_if_not_running(
                client, lifecycle_repository, source_config.source_id,
                ACL_RESYNC_TRIGGER_ID, "acl_resync_orchestrator",
                client_input=plan.to_payload(),
                source_registry=source_registry,
            )
        except Exception:
            logger.exception("ACL timer source dispatch failed source=%s", plan.source_id)


@app.timer_trigger(
    schedule=LIFECYCLE_RECONCILE_SCHEDULE,
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
@app.durable_client_input(client_name="client")
async def lifecycle_reconcile_timer(timer: func.TimerRequest, client) -> None:
    """Repair interrupted persisted lifecycle transitions without external AI calls."""
    from config import load_config
    from ingestion.lifecycle_repository import LIFECYCLE_RECONCILE_TRIGGER_ID
    config = load_config()
    lifecycle_repository = _build_lifecycle_repository(config)
    source_registry = _build_source_registry_repository(config)
    for plan in _load_validated_source_plans(config):
        if plan.source.desired_state == "paused":
            continue
        try:
            orchestrator_name = "lifecycle_reconcile_orchestrator"
            if plan.source.desired_state == "retired":
                await _release_coordinator_of_ended_owner(client, source_registry, plan.source_id)
                coordinator = source_registry.get_drive_coordinator(plan.source_id)
                if coordinator is not None and coordinator.get("state") != "settled":
                    if coordinator.get("state") == "active":
                        source_registry.mark_drive_coordinator_draining(
                            plan.source_id, coordinator["ownerInstanceId"],
                        )
                    logger.info(
                        "source retirement waiting for coordinator source=%s state=%s",
                        plan.source_id, coordinator.get("state"),
                    )
                    continue
                orchestrator_name = "source_retirement_orchestrator"
            await _start_if_not_running(
                client,
                lifecycle_repository,
                plan.source_id,
                LIFECYCLE_RECONCILE_TRIGGER_ID,
                orchestrator_name,
                client_input=plan.to_payload(),
                source_registry=source_registry,
            )
        except Exception:
            logger.exception(
                "lifecycle timer source dispatch failed source=%s", plan.source_id,
            )



@app.route(route="webhook/sharepoint", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@app.durable_client_input(client_name="client")
async def webhook_sharepoint(req: func.HttpRequest, client) -> func.HttpResponse:
    """Receive Microsoft Graph change notifications for the subscribed drive."""
    validation_token = req.params.get("validationToken")
    if validation_token:
        return func.HttpResponse(validation_token, status_code=200, mimetype="text/plain")

    if not WEBHOOK_CLIENT_STATE:
        logger.error("webhook_sharepoint: WEBHOOK_CLIENT_STATE not configured")
        return func.HttpResponse(status_code=500)

    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(status_code=400)

    from ingestion.subscription import validate_webhook_notification
    if not validate_webhook_notification(payload, WEBHOOK_CLIENT_STATE):
        return func.HttpResponse(status_code=403)

    from config import load_config
    from ingestion.lifecycle_repository import DELTA_SYNC_TRIGGER_ID
    try:
        config = load_config()
        plans = _load_validated_source_plans(config)
        lifecycle_repository = _build_lifecycle_repository(config)
        repository = _build_repository(config)
        notifications = payload.get("value")
        if not isinstance(notifications, list):
            return func.HttpResponse(status_code=400)
        try:
            matched_plans = _match_webhook_source_plans(
                notifications, plans, lifecycle_repository,
            )
        except ValueError:
            return func.HttpResponse(status_code=403)
        from ingestion.source_registry import config_for_source
        source_registry = _build_source_registry_repository(config)
        routing_failed = False
        for plan in matched_plans:
            try:
                source_config = config_for_source(config, plan)
                if _full_sync_is_running(repository, source_config.source_id):
                    continue
                await _start_if_not_running(
                    client,
                    lifecycle_repository,
                    plan.source_id,
                    DELTA_SYNC_TRIGGER_ID,
                    "delta_sync_orchestrator",
                    client_input=plan.to_payload(),
                    source_registry=source_registry,
                )
                _write_webhook_audit(
                    plan.source_id,
                    plan.source.business_name,
                    "webhook_received",
                    {"action": "delta_sync_triggered"},
                )
            except Exception:
                routing_failed = True
                logger.exception(
                    "webhook source dispatch failed source=%s", plan.source_id,
                )
        if routing_failed:
            return func.HttpResponse(status_code=503)
    except Exception as error:
        logger.error("webhook source routing failed: %s", type(error).__name__)
        return func.HttpResponse(status_code=503)
    return func.HttpResponse(status_code=200)


@app.route(route="webhook/lifecycle", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
async def webhook_lifecycle(req: func.HttpRequest) -> func.HttpResponse:
    """Validate and handle supported lifecycle events for registered drive subscriptions."""
    validation_token = req.params.get("validationToken")
    if validation_token:
        return func.HttpResponse(validation_token, status_code=200, mimetype="text/plain")

    if not WEBHOOK_CLIENT_STATE:
        return func.HttpResponse(status_code=500)

    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(status_code=400)

    from config import load_config
    from ingestion.source_registry import config_for_source
    from ingestion.subscription import (
        SubscriptionNotFoundError,
        create_subscription,
        delete_subscription,
        renew_subscription,
        validate_webhook_notification,
    )
    if not validate_webhook_notification(payload, WEBHOOK_CLIENT_STATE):
        return func.HttpResponse(status_code=403)
    notifications = payload.get("value")
    if not isinstance(notifications, list) or any(
        not isinstance(item, dict)
        or not isinstance(item.get("subscriptionId"), str)
        or not item.get("subscriptionId")
        for item in notifications
    ):
        return func.HttpResponse(status_code=403)
    requested_subscriptions = {item["subscriptionId"] for item in notifications}
    try:
        config = load_config()
        plans = _load_validated_source_plans(config)
        lifecycle_repository = _build_lifecycle_repository(config)
        subscription_plans = {
            subscription_id: plan
            for plan in plans
            if (subscription_id := lifecycle_repository.get_webhook_subscription_id(plan.source_id))
            in requested_subscriptions
        }
        base_url = os.getenv("FUNCTION_PUBLIC_BASE_URL", "").strip()
        if not base_url:
            return func.HttpResponse(status_code=503)
        notification_url = f"{base_url}/api/webhook/sharepoint"
        lifecycle_url = f"{base_url}/api/webhook/lifecycle"
        graph_client = _build_graph_client(config)
        for notification in notifications:
            if not isinstance(notification, dict):
                return func.HttpResponse(status_code=403)
            subscription_id = notification.get("subscriptionId")
            plan = subscription_plans.get(subscription_id)
            if plan is None:
                logger.warning("webhook_lifecycle subscription not matched to an accepted source; ignored")
                continue
            event = notification.get("lifecycleEvent")
            action = _lifecycle_subscription_action(plan, event)
            if action == "delete":
                delete_subscription(graph_client, subscription_id)
                lifecycle_repository.clear_webhook_subscription_id(plan.source_id)
                continue
            if action == "ignore":
                continue
            if action == "unsupported":
                logger.warning(
                    "webhook_lifecycle_unsupported source=%s event=%s",
                    plan.source_id,
                    event if isinstance(event, str) else "invalid",
                )
                continue
            source_config = config_for_source(config, plan)
            try:
                renew_subscription(graph_client, subscription_id)
            except SubscriptionNotFoundError:
                info = create_subscription(
                    graph_client,
                    source_config.drive_id,
                    notification_url,
                    lifecycle_url,
                    WEBHOOK_CLIENT_STATE,
                )
                lifecycle_repository.save_webhook_subscription_id(
                    plan.source_id,
                    info.subscription_id,
                )
    except Exception as error:
        logger.warning("webhook lifecycle processing failed: %s", type(error).__name__)
        return func.HttpResponse(status_code=503)
    return func.HttpResponse(status_code=200)


@app.timer_trigger(schedule=SUBSCRIPTION_RENEW_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
async def subscription_renew_timer(timer: func.TimerRequest) -> None:
    """Create or renew the Microsoft Graph webhook subscription for drive changes."""
    from config import load_config
    from ingestion.subscription import (
        delete_subscription,
        find_drive_subscriptions,
        reconcile_drive_subscription,
    )

    if not WEBHOOK_CLIENT_STATE:
        logger.warning("subscription_renew_timer skipped: missing config")
        return

    from ingestion.source_registry import config_for_source
    config = load_config()
    graph_client = _build_graph_client(config)
    base_url = os.getenv("FUNCTION_PUBLIC_BASE_URL", "").strip()
    if not base_url:
        logger.warning("subscription_renew_timer skipped: FUNCTION_PUBLIC_BASE_URL not set")
        return

    notification_url = f"{base_url}/api/webhook/sharepoint"
    lifecycle_url = f"{base_url}/api/webhook/lifecycle"

    lifecycle_repository = _build_lifecycle_repository(config)
    for plan in _load_validated_source_plans(config):
        try:
            existing_id = lifecycle_repository.get_webhook_subscription_id(plan.source_id)
            if plan.source.desired_state == "retired":
                subscription_ids = {item.subscription_id for item in find_drive_subscriptions(
                    graph_client, plan.source.drive_id, WEBHOOK_CLIENT_STATE,
                )}
                if existing_id:
                    subscription_ids.add(existing_id)
                for subscription_id in subscription_ids:
                    delete_subscription(graph_client, subscription_id)
                lifecycle_repository.clear_webhook_subscription_id(plan.source_id)
                continue
            if plan.source.desired_state != "active":
                continue
            source_config = config_for_source(config, plan)
            info = reconcile_drive_subscription(
                graph_client, source_config.drive_id, notification_url, lifecycle_url,
                WEBHOOK_CLIENT_STATE, existing_id,
            )
            lifecycle_repository.save_webhook_subscription_id(plan.source_id, info.subscription_id)
            logger.info("subscription_reconciled source=%s expires=%s", plan.source_id, info.expiration)
        except Exception:
            logger.exception("subscription_reconciliation_failed source=%s", plan.source_id)


@app.orchestration_trigger(context_name="context")
def multi_source_full_sync_orchestrator(context: df.DurableOrchestrationContext):
    """Preflight the full catalog, then dispatch drives in bounded continuations."""
    state = context.get_input() or {}
    if not isinstance(state, dict):
        return {"status": "failed", "phase": "input", "error": "source_dispatch_input_invalid"}
    dispatch_id = context.instance_id
    phase = state.get("phase", "preflight")
    continuation_token = state.get("continuationToken")
    inventory_counts = state.get("inventoryCounts", {
        "enumerated": 0, "active": 0, "paused": 0, "retired": 0,
        "failed": 0, "unattributed": 0,
    })
    dispatch_counts = state.get("dispatchCounts", {"completed": 0, "failed": 0, "pending": 0})

    if phase == "preflight":
        page = yield context.call_activity(
            "prepare_source_inventory_page_activity",
            {"dispatchId": dispatch_id, "continuationToken": continuation_token},
        )
        if page.get("error"):
            return {"status": "failed", "phase": "source_registry", "error": page["error"]}
        for key in inventory_counts:
            inventory_counts[key] += page.get("counts", {}).get(key, 0)
        continuation_token = page.get("continuationToken")
        if continuation_token is not None:
            context.continue_as_new({
                "phase": "preflight",
                "continuationToken": continuation_token,
                "inventoryCounts": inventory_counts,
            })
            return
        completed = yield context.call_activity(
            "complete_source_dispatch_activity",
            {"dispatchId": dispatch_id, "counts": inventory_counts},
        )
        if completed.get("error"):
            return {"status": "failed", "phase": "source_registry", "error": completed["error"]}
        if inventory_counts["enumerated"] == 0:
            return {"status": "failed", "phase": "source_registry", "error": "source_registry_empty"}
        context.continue_as_new({
            "phase": "dispatch",
            "inventoryCounts": inventory_counts,
            "dispatchCounts": dispatch_counts,
            "continuationToken": None,
        })
        return

    if phase != "dispatch":
        return {"status": "failed", "phase": "input", "error": "source_dispatch_phase_invalid"}

    # Fan out per-source children in a bounded window (Durable fan-out/fan-in) so one slow or
    # failed source never delays another, including sources on later pages. History is not
    # compacted here: continue-as-new would drop the in-flight child tasks.
    queue: list[tuple[str, dict]] = []
    in_flight: list[tuple[Any, str, str]] = []
    pages_done = False
    fatal_error = None
    while True:
        while (
            queue
            and fatal_error is None
            and len(in_flight) < EXECUTION_CONFIG.source_dispatch_page_size
        ):
            source_id, source = queue.pop(0)
            child_id = _source_dispatch_child_instance_id(dispatch_id, source_id)
            task = context.call_sub_orchestrator("full_sync_orchestrator", source, child_id)
            in_flight.append((task, source_id, child_id))
        if (
            not queue
            and not pages_done
            and fatal_error is None
            and len(in_flight) < EXECUTION_CONFIG.source_dispatch_page_size
        ):
            page = yield context.call_activity(
                "list_source_dispatch_page_activity",
                {"dispatchId": dispatch_id, "continuationToken": continuation_token},
            )
            if page.get("error"):
                fatal_error = page["error"]
                continue
            for item in page.get("items", []):
                if item.get("status") != "prepared":
                    continue
                source = item.get("sourcePlan")
                source_id = item.get("sourceId")
                if not isinstance(source, dict) or not isinstance(source_id, str) or not source_id:
                    fatal_error = "dispatch_item_invalid"
                    break
                queue.append((source_id, source))
            continuation_token = page.get("continuationToken")
            pages_done = continuation_token is None
            continue
        if not in_flight:
            if pages_done or fatal_error is not None:
                break
            continue
        winner = yield context.task_any([entry[0] for entry in in_flight])
        index = next(i for i, entry in enumerate(in_flight) if entry[0] is winner)
        _, source_id, child_id = in_flight.pop(index)
        outcome = winner.result
        if not isinstance(outcome, dict):
            # A failed child task carries its exception as the result.
            outcome = {"status": "failed", "error": "source_orchestration_failed"}
        status = outcome.get("status")
        if status not in {"completed", "failed", "pending"}:
            status = "failed"
        recorded = yield context.call_activity(
            "record_source_dispatch_outcome_activity",
            {
                "dispatchId": dispatch_id,
                "sourceId": source_id,
                "status": status,
                "childInstanceId": child_id,
                "outcome": _source_dispatch_outcome_summary(outcome),
            },
        )
        if recorded.get("error") and fatal_error is None:
            fatal_error = recorded["error"]
        dispatch_counts[status] += 1
    if fatal_error is not None:
        return {"status": "failed", "phase": "source_dispatch", "error": fatal_error}

    failed_total = inventory_counts["failed"] + dispatch_counts["failed"] + inventory_counts["unattributed"]
    pending_total = dispatch_counts["pending"]
    return {
        "status": "failed" if failed_total and dispatch_counts["completed"] == 0
        else "partial" if failed_total or pending_total
        else "completed",
        "dispatchId": dispatch_id,
        "inventory": inventory_counts,
        "drives": dispatch_counts,
        "outcomePageSize": EXECUTION_CONFIG.source_dispatch_page_size,
    }


@app.activity_trigger(input_name="payload")
def prepare_source_inventory_page_activity(payload: dict) -> dict:
    from config import load_config

    dispatch_id = payload.get("dispatchId")
    if not isinstance(dispatch_id, str) or not dispatch_id:
        return {"error": "source_dispatch_id_invalid"}
    registry = None
    try:
        config = load_config()
        registry = _build_source_registry_repository(config)
        registry.begin_source_dispatch(dispatch_id)
        page = registry.list_source_inventory_page(
            page_size=EXECUTION_CONFIG.source_dispatch_page_size,
            continuation_token=payload.get("continuationToken"),
        )
        counts = {
            "enumerated": (
                len(page.plans) + len(page.rejected_source_ids)
                + len(page.duplicate_drive_source_ids) + page.unattributed_errors
            ),
            "active": 0,
            "paused": 0,
            "retired": 0,
            "failed": len(page.rejected_source_ids) + len(page.duplicate_drive_source_ids),
            "unattributed": page.unattributed_errors,
        }
        for page_plan in page.plans:
            try:
                plan = registry.read_source_plan(page_plan.source_id)
            except Exception as error:
                logger.warning(
                    "registered source binding rejected source=%s error=%s",
                    page_plan.source_id,
                    type(error).__name__,
                )
                registry.stage_source_dispatch_item(
                    dispatch_id,
                    page_plan.source_id,
                    status="failed",
                    outcome={"status": "failed", "error": "source_binding_validation_failed"},
                )
                counts["failed"] += 1
                continue
            state = plan.source.desired_state
            if state == "active":
                registry.stage_source_dispatch_item(
                    dispatch_id,
                    plan.source_id,
                    status="prepared",
                    source_payload=plan.to_payload(),
                )
                counts["active"] += 1
            else:
                registry.stage_source_dispatch_item(
                    dispatch_id,
                    plan.source_id,
                    status=state,
                    source_payload=plan.to_payload(),
                    outcome={"status": state, "processed": False},
                )
                counts[state] += 1
        for source_id in page.rejected_source_ids:
            registry.stage_source_dispatch_item(
                dispatch_id,
                source_id,
                status="failed",
                outcome={"status": "failed", "error": "source_record_invalid"},
            )
        for source_id in page.duplicate_drive_source_ids:
            registry.stage_source_dispatch_item(
                dispatch_id,
                source_id,
                status="failed",
                outcome={"status": "failed", "error": "sharepoint_drive_already_registered"},
            )
        return {"counts": counts, "continuationToken": page.continuation_token}
    except Exception:
        logger.exception("source inventory preflight page failed")
        if registry is not None:
            try:
                registry.fail_source_dispatch(dispatch_id, "source_registry_validation_failed")
            except Exception:
                logger.exception("source dispatch manifest could not be failed")
        return {"error": "source_registry_validation_failed"}


@app.activity_trigger(input_name="payload")
def complete_source_dispatch_activity(payload: dict) -> dict:
    try:
        config = __import__("config").load_config()
        registry = _build_source_registry_repository(config)
        registry.complete_source_dispatch(payload["dispatchId"], payload["counts"])
        return {"status": "ready"}
    except Exception:
        logger.exception("source dispatch inventory completion failed")
        return {"error": "source_dispatch_completion_failed"}


@app.activity_trigger(input_name="payload")
def list_source_dispatch_page_activity(payload: dict) -> dict:
    try:
        config = __import__("config").load_config()
        registry = _build_source_registry_repository(config)
        page = registry.list_source_dispatch_page(
            payload["dispatchId"],
            page_size=EXECUTION_CONFIG.source_dispatch_page_size,
            continuation_token=payload.get("continuationToken"),
        )
        return {"items": list(page.items), "continuationToken": page.continuation_token}
    except Exception:
        logger.exception("source dispatch page read failed")
        return {"error": "source_dispatch_page_failed"}


@app.activity_trigger(input_name="payload")
def record_source_dispatch_outcome_activity(payload: dict) -> dict:
    try:
        config = __import__("config").load_config()
        registry = _build_source_registry_repository(config)
        registry.record_source_dispatch_outcome(
            payload["dispatchId"],
            payload["sourceId"],
            status=payload["status"],
            child_instance_id=payload["childInstanceId"],
            outcome=payload["outcome"],
        )
        return {"status": "recorded"}
    except Exception:
        logger.exception("source dispatch outcome write failed")
        return {"error": "source_dispatch_outcome_failed"}


@app.activity_trigger(input_name="payload")
def list_source_dispatch_outcomes_page_activity(payload: dict) -> dict:
    try:
        config = __import__("config").load_config()
        registry = _build_source_registry_repository(config)
        page = registry.list_source_dispatch_outcomes_page(
            payload["dispatchId"],
            page_size=EXECUTION_CONFIG.source_dispatch_page_size,
            continuation_token=payload.get("continuationToken"),
        )
        return {"items": list(page.items), "continuationToken": page.continuation_token}
    except Exception:
        logger.exception("source dispatch outcomes page read failed")
        return {"error": "source_dispatch_outcomes_page_failed"}


@app.orchestration_trigger(context_name="context")
def full_sync_orchestrator(context: df.DurableOrchestrationContext):
    """Orchestrate: activate → discover → fan-out process → finalize."""
    from datetime import timedelta

    orchestration_input = context.get_input()
    if not isinstance(orchestration_input, dict):
        return {"status": "failed", "phase": "input", "error": "source_plan_required"}
    orchestration_phase = orchestration_input.get("orchestrationPhase")
    is_continuation = orchestration_phase == "process"
    is_discovery_continuation = orchestration_phase == "discover"
    is_catchup_continuation = orchestration_phase == "catchup"
    source_plan = (
        orchestration_input.get("sourcePlan")
        if is_continuation or is_discovery_continuation or is_catchup_continuation
        else orchestration_input
    )
    if not isinstance(source_plan, dict):
        return {"status": "failed", "phase": "input", "error": "source_plan_required"}

    if is_continuation or is_discovery_continuation or is_catchup_continuation:
        run_id = orchestration_input.get("runId")
        run_etag = orchestration_input.get("runEtag")
        discovered_count = orchestration_input.get("discoveredCount")
        items_scanned = orchestration_input.get("itemsScanned")
        rejected_count = orchestration_input.get("rejectedCount", 0)
        baseline_candidate = orchestration_input.get("baselineCandidate")
        if is_continuation:
            continuation_token = orchestration_input.get("continuationToken")
            discovery_state = None
            catchup_phase = "items"
            catchup_token = None
            catchup_candidate = None
            cursor_committed = False
            total_succeeded = orchestration_input.get("totalSucceeded", 0)
            total_failed = orchestration_input.get("totalFailed", 0)
            total_pending = orchestration_input.get("totalPending", 0)
        elif is_catchup_continuation:
            continuation_token = None
            discovery_state = None
            catchup_phase = orchestration_input.get("deltaPhase", "items")
            catchup_token = orchestration_input.get("phaseContinuationToken")
            catchup_candidate = catchup_token if catchup_phase == "commit" else None
            cursor_committed = False
            total_succeeded = orchestration_input.get("totalSucceeded", 0)
            total_failed = orchestration_input.get("totalFailed", 0)
            total_pending = orchestration_input.get("totalPending", 0)
        else:
            continuation_token = None
            discovery_state = orchestration_input.get("discoveryState")
            catchup_phase = "items"
            catchup_token = None
            catchup_candidate = None
            cursor_committed = False
            total_succeeded = total_failed = total_pending = 0
        if (
            not isinstance(run_id, str) or not isinstance(run_etag, str)
            or not isinstance(discovered_count, int) or not isinstance(items_scanned, int)
            or not isinstance(rejected_count, int)
            or not isinstance(baseline_candidate, str) or not baseline_candidate
            or (is_continuation and (
                not isinstance(total_succeeded, int)
                or not isinstance(total_failed, int)
                or not isinstance(total_pending, int)
            ))
            or (continuation_token is not None and not isinstance(continuation_token, str))
            or (catchup_token is not None and not isinstance(catchup_token, str))
            or (discovery_state is not None and not isinstance(discovery_state, dict))
        ):
            return {"status": "failed", "phase": "continuation_input", "error": "invalid_state"}
    else:
        activated = yield context.call_activity(
            "activate_run_activity",
            {"instanceId": context.instance_id, "sourcePlan": source_plan},
        )
        if activated.get("error"):
            return {
                "sourceId": source_plan.get("sourceId", ""),
                "status": "failed", "phase": "activate", "error": activated["error"],
            }

        run_id = activated["runId"]
        run_etag = activated["runEtag"]
        discovered_count = 0
        items_scanned = 0
        rejected_count = 0
        baseline_candidate = None
        discovery_state = None
        catchup_phase = "items"
        continuation_token = None
        catchup_token = None
        catchup_candidate = None
        cursor_committed = False
        total_succeeded = total_failed = total_pending = 0
    draining = False

    if not is_continuation and not is_discovery_continuation and not is_catchup_continuation:
        baseline = yield context.call_activity(
            "delta_sync_activity",
            {
                "sourcePlan": source_plan,
                "instanceId": context.instance_id,
                "coordinatorOwnerId": context.instance_id,
                "deltaExecutionId": f"fullsync-baseline-{context.instance_id}",
                "fullSyncBaseline": True,
                "enumerateOnly": True,
                "deferCursorCommit": True,
            },
        )
        if (
            baseline.get("error")
            or baseline.get("continuationPhase") != "baseline_complete"
            or not isinstance(baseline.get("phaseContinuationToken"), str)
        ):
            return {
                "sourceId": source_plan.get("sourceId", ""),
                "status": "failed",
                "phase": "baseline",
                "error": baseline.get("error", "baseline_candidate_missing"),
            }
        context.continue_as_new({
            "orchestrationPhase": "discover",
            "sourcePlan": source_plan,
            "runId": run_id,
            "runEtag": run_etag,
            "discoveredCount": discovered_count,
            "itemsScanned": items_scanned,
            "rejectedCount": rejected_count,
            "baselineCandidate": baseline["phaseContinuationToken"],
            "discoveryState": None,
        })
        return

    if not is_continuation and not is_catchup_continuation:
        discovered = yield context.call_activity(
            "discover_all_activity",
            {
                "runId": run_id,
                "sourcePlan": source_plan,
                "coordinatorOwnerId": context.instance_id,
                "discoveryState": discovery_state,
            },
        )
        if discovered.get("error"):
            return {
                "sourceId": source_plan.get("sourceId", ""),
                "status": "failed", "phase": "discover", "runId": run_id,
                "error": discovered["error"],
            }

        discovered_count += discovered["discoveredCount"]
        items_scanned += discovered["itemsScanned"]
        rejected_count += discovered.get("rejectedCount", 0)
        if not discovered.get("complete"):
            context.continue_as_new({
                "orchestrationPhase": "discover",
                "sourcePlan": source_plan,
                "runId": run_id,
                "runEtag": run_etag,
                "discoveredCount": discovered_count,
                "itemsScanned": items_scanned,
                "rejectedCount": rejected_count,
                "baselineCandidate": baseline_candidate,
                "discoveryState": discovered["discoveryState"],
            })
            return
        total_succeeded = total_failed = total_pending = 0

    def continue_after_page(next_token: str) -> None:
        context.continue_as_new({
            "orchestrationPhase": "process",
            "sourcePlan": source_plan,
            "runId": run_id,
            "runEtag": run_etag,
            "discoveredCount": discovered_count,
            "itemsScanned": items_scanned,
            "rejectedCount": rejected_count,
            "baselineCandidate": baseline_candidate,
            "continuationToken": next_token,
            "totalSucceeded": total_succeeded,
            "totalFailed": total_failed,
            "totalPending": total_pending,
        })
    while not is_catchup_continuation:
        page = yield context.call_activity(
            "list_discovered_documents_page_activity",
            {
                "runId": run_id,
                "sourcePlan": source_plan,
                "coordinatorOwnerId": context.instance_id,
                "continuationToken": continuation_token,
            },
        )
        if page.get("error"):
            total_failed += 1
            break
        wave = page["documents"]
        continuation_token = page.get("continuationToken")
        if not wave:
            if continuation_token is not None:
                continue_after_page(continuation_token)
                return
            break
        claim_tasks = [
            context.call_activity(
                "claim_document_work_activity",
                {
                    "runId": run_id,
                    "document": doc,
                    "operationKind": "full-sync",
                    "sourcePlan": source_plan,
                    "coordinatorOwnerId": context.instance_id,
                },
            )
            for doc in wave
        ]
        claims = yield context.task_all(claim_tasks)
        process_wave: list[dict[str, str]] = []
        tasks = []
        claim_failed = False
        for document, claim in zip(wave, claims):
            if claim.get("status") == "acquired":
                process_wave.append({
                    **document,
                    "operationId": claim["operationId"],
                    "workId": claim["operationId"],
                    "attemptId": claim["operationId"],
                    "operationKind": "full-sync",
                })
                tasks.append(context.call_activity(
                    "process_document_activity",
                    {
                        "runId": run_id,
                        "document": document,
                        "operationId": claim["operationId"],
                        "workId": claim["operationId"],
                        "attemptId": claim["operationId"],
                        "operationKind": "full-sync",
                        "sourcePlan": source_plan,
                        "coordinatorOwnerId": context.instance_id,
                    },
                ))
            elif claim.get("status") == "completed":
                outcome = claim.get("outcome") or {}
                if outcome.get("status") in {"succeeded", "skipped"}:
                    total_succeeded += 1
                elif outcome.get("status") == "pending":
                    total_pending += 1
                else:
                    total_failed += 1
            elif claim.get("status") in {"pending", "unresolved", "duplicate_active"}:
                total_pending += 1
            else:
                total_failed += 1
                claim_failed = True

        if tasks:
            deadline = context.current_utc_datetime + timedelta(
                minutes=EXECUTION_CONFIG.wave_timeout_minutes
            )
            timer = context.create_timer(deadline)
            wave_task = context.task_all(tasks)
            try:
                winner = yield context.task_any([wave_task, timer])
            except Exception:
                timer.cancel()
                interrupted = yield context.call_activity(
                    "timeout_wave_documents_activity",
                    {
                        "documents": process_wave, "errorCode": "wave_retry_exhausted",
                        "sourcePlan": source_plan, "coordinatorOwnerId": context.instance_id,
                    },
                )
                if interrupted.get("error"):
                    return {
                        "status": "failed",
                        "phase": "timeout_fence",
                        "sourceId": source_plan.get("sourceId", ""),
                        "runId": run_id,
                        "error": interrupted["error"],
                    }
                total_failed += len(process_wave)
                draining = True
            if not draining and winner == timer:
                timed_out = yield context.call_activity(
                    "timeout_wave_documents_activity",
                    {
                        "documents": process_wave, "errorCode": "wave_timeout",
                        "sourcePlan": source_plan, "coordinatorOwnerId": context.instance_id,
                    },
                )
                if timed_out.get("error"):
                    return {
                        "status": "failed",
                        "phase": "timeout_fence",
                        "sourceId": source_plan.get("sourceId", ""),
                        "runId": run_id,
                        "error": timed_out["error"],
                    }
                total_failed += len(process_wave)
                draining = True
            elif not draining:
                timer.cancel()
                for result in wave_task.result:
                    if result.get("status") == "succeeded":
                        total_succeeded += 1
                    elif result.get("status") == "pending":
                        total_pending += 1
                    else:
                        total_failed += 1
        if claim_failed:
            draining = True
        if draining:
            break
        if continuation_token is None:
            break
        continue_after_page(continuation_token)
        return

    if draining:
        yield context.call_activity(
            "mark_drive_coordinator_draining_activity",
            {"sourcePlan": source_plan, "coordinatorOwnerId": context.instance_id},
        )
        return {
            "status": "draining",
            "sourceId": source_plan.get("sourceId", ""),
            "runId": run_id,
            "failed": total_failed,
        }

    catchup_round_id = f"fullsync-catchup-{context.instance_id}"
    if total_failed == 0 and catchup_candidate is None:
        catch_up = yield context.call_activity(
            "delta_sync_activity",
            {
                "sourcePlan": source_plan,
                "instanceId": context.instance_id,
                "coordinatorOwnerId": context.instance_id,
                "settleCoordinator": False,
                "fullSyncCatchup": True,
                "deltaExecutionId": catchup_round_id,
                "roundStartLink": baseline_candidate,
                "deferCursorCommit": True,
                "reducedItemsContinuationToken": catchup_token if catchup_phase == "items" else None,
                "processingPhase": catchup_phase,
                "phaseContinuationToken": catchup_token if catchup_phase != "items" else None,
            },
        )
        if catch_up.get("error"):
            total_failed += 1
        else:
            total_failed += catch_up.get("failed", 0)
            total_pending += catch_up.get("pending", 0)
            catchup_token = catch_up.get("phaseContinuationToken")
            if catchup_token is not None and total_failed == 0 and total_pending == 0:
                next_phase = catch_up.get("continuationPhase")
                if next_phase == "commit":
                    catchup_candidate = catchup_token
                else:
                    context.continue_as_new({
                        "orchestrationPhase": "catchup",
                        "sourcePlan": source_plan,
                        "runId": run_id,
                        "runEtag": run_etag,
                        "discoveredCount": discovered_count,
                        "itemsScanned": items_scanned,
                        "rejectedCount": rejected_count,
                        "baselineCandidate": baseline_candidate,
                        "deltaPhase": next_phase,
                        "phaseContinuationToken": catchup_token,
                        "totalSucceeded": total_succeeded,
                        "totalFailed": total_failed,
                        "totalPending": total_pending,
                    })
                    return

    if total_failed == 0 and total_pending == 0:
        absence = yield context.call_sub_orchestrator(
            "full_sync_absence_reconcile_orchestrator",
            {
                "sourcePlan": source_plan,
                "coordinatorOwnerId": context.instance_id,
                "currentRunId": run_id,
            },
            f"{context.instance_id}-absence-reconcile",
        )
        if absence.get("status") != "completed":
            total_failed += absence.get("failed", 1)

    finalized = yield context.call_activity(
        "finalize_run_activity",
        {
            "runId": run_id,
            "runEtag": run_etag,
            "itemsScanned": items_scanned,
            "sourcePlan": source_plan,
            "coordinatorOwnerId": context.instance_id,
        },
    )
    finalization_failed = finalized.get("runStatus") != "completed"
    if finalization_failed:
        total_failed += 1

    if catchup_candidate is not None and total_failed == 0 and total_pending == 0:
        committed = yield context.call_activity(
            "commit_delta_cursor_activity",
            {
                "sourcePlan": source_plan,
                "coordinatorOwnerId": context.instance_id,
                "deltaExecutionId": catchup_round_id,
                "candidateDeltaLink": catchup_candidate,
                "parentRunId": run_id,
            },
        )
        if committed.get("error"):
            total_failed += 1
        else:
            cursor_committed = True
    elif catchup_candidate is not None:
        yield context.call_activity(
            "mark_drive_coordinator_draining_activity",
            {"sourcePlan": source_plan, "coordinatorOwnerId": context.instance_id},
        )

    if total_failed == 0 and total_pending == 0 and not cursor_committed:
        total_failed += 1
    if (
        total_failed == 0
        and total_pending == 0
        and source_plan.get("reprovisionBootstrap") is True
    ):
        bootstrap = yield context.call_activity(
            "complete_reprovision_full_sync_activity",
            {
                "sourcePlan": source_plan,
                "coordinatorOwnerId": context.instance_id,
            },
        )
        if bootstrap.get("error"):
            total_failed += 1
    if total_failed > 0 and total_pending == 0:
        yield context.call_activity(
            "mark_drive_coordinator_draining_activity",
            {"sourcePlan": source_plan, "coordinatorOwnerId": context.instance_id},
        )

    return {
        "status": "failed" if total_failed else "pending" if total_pending else "completed",
        "sourceId": source_plan.get("sourceId", ""),
        "runId": run_id,
        "discovered": discovered_count,
        "rejected": rejected_count,
        "succeeded": total_succeeded,
        "failed": total_failed,
        "pending": total_pending,
        "runStatus": finalized.get("runStatus", "unknown"),
    }


@app.orchestration_trigger(context_name="context")
def delta_sync_orchestrator(context: df.DurableOrchestrationContext):
    """Process one bounded reduced-item page per Durable execution."""
    orchestration_input = context.get_input()
    is_continuation = (
        isinstance(orchestration_input, dict)
        and orchestration_input.get("orchestrationPhase") == "delta"
    )
    source_plan = orchestration_input.get("sourcePlan") if is_continuation else orchestration_input
    processing_phase = orchestration_input.get("deltaPhase", "items") if is_continuation else "items"
    continuation_token = orchestration_input.get("phaseContinuationToken") if is_continuation else None
    totals = orchestration_input.get("totals", {}) if is_continuation else {}
    if not isinstance(source_plan, dict) or not isinstance(totals, dict):
        return {"status": "failed", "error": "delta_orchestration_input_invalid"}
    result = yield context.call_activity(
        "delta_sync_activity",
        {
            "sourcePlan": source_plan,
            "instanceId": context.instance_id,
            "coordinatorOwnerId": context.instance_id,
            "deltaExecutionId": context.instance_id,
            "reducedItemsContinuationToken": continuation_token if processing_phase == "items" else None,
            "processingPhase": processing_phase,
            "phaseContinuationToken": continuation_token if processing_phase != "items" else None,
        },
    )
    if result.get("error"):
        return {"status": "failed", "error": result["error"]}
    for key in (
        "createdOrUpdated",
        "deleted",
        "aclResynced",
        "quarantined",
        "failed",
        "pending",
    ):
        totals[key] = totals.get(key, 0) + result.get(key, 0)
    totals["bootstrapped"] = result.get("bootstrapped", totals.get("bootstrapped", False))
    totals["itemsSeen"] = max(totals.get("itemsSeen", 0), result.get("itemsSeen", 0))
    if result.get("failed", 0):
        return {"status": "failed", **totals}
    if result.get("pending", 0):
        return {"status": "pending", **totals}
    next_token = result.get("phaseContinuationToken")
    next_phase = result.get("continuationPhase")
    if next_token is not None:
        context.continue_as_new({
            "orchestrationPhase": "delta",
            "sourcePlan": source_plan,
            "deltaPhase": next_phase,
            "phaseContinuationToken": next_token,
            "totals": totals,
        })
        return
    return {"status": "completed", **totals}


@app.orchestration_trigger(context_name="context")
def acl_resync_orchestrator(context: df.DurableOrchestrationContext):
    """Page through all ready documents re-verifying ACLs."""
    orchestration_input = context.get_input()
    is_continuation = orchestration_input.get("orchestrationPhase") == "acl-resync"
    source_plan = (
        orchestration_input.get("sourcePlan") if is_continuation else orchestration_input
    )
    owner_instance_id = context.instance_id
    continuation_token = orchestration_input.get("continuationToken") if is_continuation else None
    total_checked = orchestration_input.get("totalChecked", 0) if is_continuation else 0
    total_updated = orchestration_input.get("totalUpdated", 0) if is_continuation else 0
    total_retired = orchestration_input.get("totalRetired", 0) if is_continuation else 0
    while True:
        result = yield context.call_activity(
            "acl_resync_page_activity",
            {
                "continuationToken": continuation_token,
                "sourcePlan": source_plan,
                "coordinatorOwnerId": owner_instance_id,
            },
        )
        if result.get("error"):
            return {"status": "failed", "error": result["error"]}
        total_checked += result["checked"]
        total_updated += result["updated"]
        total_retired += result["retired"]
        continuation_token = result.get("continuationToken")
        if continuation_token is None:
            break
        context.continue_as_new({
            "orchestrationPhase": "acl-resync",
            "sourcePlan": source_plan,
            "continuationToken": continuation_token,
            "totalChecked": total_checked,
            "totalUpdated": total_updated,
            "totalRetired": total_retired,
        })
        return
    settled = yield context.call_activity(
        "settle_drive_coordinator_activity",
        {"sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id},
    )
    if settled.get("error"):
        return {"status": "failed", "error": settled["error"]}
    return {
        "status": "completed",
        "checked": total_checked,
        "updated": total_updated,
        "retired": total_retired,
    }


@app.orchestration_trigger(context_name="context")
def source_retirement_orchestrator(context: df.DurableOrchestrationContext):
    """Delete retired-source content in bounded pages while preserving audit history."""
    payload = context.get_input()
    source_plan = payload.get("sourcePlan", payload)
    deleted_documents = payload.get("deletedDocuments", 0)
    deleted_chunks = payload.get("deletedOrphanChunks", 0)
    activity_payload = {
        "sourcePlan": source_plan,
        "coordinatorOwnerId": context.instance_id,
    }
    result = yield context.call_activity("source_retirement_page_activity", activity_payload)
    if not result.get("error"):
        deleted_documents += result["deletedDocuments"]
        deleted_chunks += result["deletedOrphanChunks"]
        if result["hasMore"]:
            context.continue_as_new({
                "sourcePlan": source_plan,
                "deletedDocuments": deleted_documents,
                "deletedOrphanChunks": deleted_chunks,
            })
            return
    settled = yield context.call_activity("settle_source_retirement_activity", activity_payload)
    if result.get("error") or settled.get("error"):
        return {"status": "failed", "error": result.get("error") or settled["error"]}
    return {
        "status": "completed",
        "deletedDocuments": deleted_documents,
        "deletedOrphanChunks": deleted_chunks,
    }


@app.orchestration_trigger(context_name="context")
def source_reprovision_orchestrator(context: df.DurableOrchestrationContext):
    """Clear retired-source operational state and return the same binding as paused."""
    payload = context.get_input()
    deleted_records = payload.get("deletedRecords", 0)
    activity_payload = {
        "sourcePlan": payload.get("sourcePlan"),
        "coordinatorOwnerId": context.instance_id,
        "operatorObjectId": payload.get("operatorObjectId"),
        "operator": payload.get("operator"),
    }
    result = yield context.call_activity("source_reprovision_page_activity", activity_payload)
    if result.get("error"):
        yield context.call_activity(
            "fail_source_reprovision_activity",
            activity_payload | {"error": result["error"]},
        )
        return {"status": "failed", "error": result["error"]}
    deleted_records = result["deletedRecords"]
    if result["hasMore"]:
        context.continue_as_new({
            "sourcePlan": payload.get("sourcePlan"),
            "operatorObjectId": payload.get("operatorObjectId"),
            "operator": payload.get("operator"),
            "deletedRecords": deleted_records,
        })
        return
    completed = yield context.call_activity(
        "complete_source_reprovision_activity",
        activity_payload | {"deletedRecords": deleted_records},
    )
    if completed.get("error"):
        return {"status": "failed", "error": completed["error"]}
    return completed


@app.orchestration_trigger(context_name="context")
def lifecycle_reconcile_orchestrator(context: df.DurableOrchestrationContext):
    """Page through and repair all persisted non-ready lifecycle transitions."""
    orchestration_input = context.get_input()
    is_continuation = orchestration_input.get("orchestrationPhase") == "lifecycle-reconcile"
    source_plan = (
        orchestration_input.get("sourcePlan") if is_continuation else orchestration_input
    )
    owner_instance_id = context.instance_id
    phase = orchestration_input.get("phase", "lifecycle") if is_continuation else "lifecycle"
    continuation_token = orchestration_input.get("continuationToken") if is_continuation else None
    total_checked = orchestration_input.get("totalChecked", 0) if is_continuation else 0
    total_repaired = orchestration_input.get("totalRepaired", 0) if is_continuation else 0
    total_failed = orchestration_input.get("totalFailed", 0) if is_continuation else 0
    while True:
        if phase == "lifecycle":
            activity_name = "lifecycle_reconcile_page_activity"
        elif phase == "duplicate":
            activity_name = "duplicate_version_reconcile_page_activity"
        elif phase == "orphan":
            activity_name = "orphan_chunk_reconcile_page_activity"
        else:
            return {"status": "failed", "error": "lifecycle_reconcile_phase_invalid"}
        result = yield context.call_activity(activity_name, {
            "continuationToken": continuation_token,
            "sourcePlan": source_plan,
            "coordinatorOwnerId": owner_instance_id,
        })
        if result.get("error"):
            return {"status": "failed", "error": result["error"]}
        total_checked += result["checked"]
        total_repaired += result["repaired"]
        total_failed += result["failed"]
        continuation_token = result.get("continuationToken")
        if continuation_token is not None:
            context.continue_as_new({
                "orchestrationPhase": "lifecycle-reconcile",
                "sourcePlan": source_plan,
                "phase": phase,
                "continuationToken": continuation_token,
                "totalChecked": total_checked,
                "totalRepaired": total_repaired,
                "totalFailed": total_failed,
            })
            return
        if phase == "lifecycle":
            phase = "duplicate"
        elif phase == "duplicate":
            phase = "orphan"
        else:
            break
    settled = yield context.call_activity(
        "settle_drive_coordinator_activity",
        {"sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id},
    )
    if settled.get("error"):
        return {"status": "failed", "error": settled["error"]}
    return {
        "status": "completed",
        "checked": total_checked,
        "repaired": total_repaired,
        "failed": total_failed,
    }


@app.timer_trigger(schedule=AUDIO_POLL_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
@app.durable_client_input(client_name="client")
async def audio_transcription_poll_timer(timer: func.TimerRequest, client) -> None:
    """Advance audio documents awaiting batch transcription (poll the batch jobs, finalize)."""
    from config import load_config
    from ingestion.lifecycle_repository import AUDIO_POLL_TRIGGER_ID
    from ingestion.source_registry import config_for_source

    config = load_config()
    if not (config.audio_writer_enabled and config.audio_transcription_provider == "speech_batch"):
        return
    lifecycle_repository = _build_lifecycle_repository(config)
    source_registry_repository = _build_source_registry_repository(config)
    for plan in _load_validated_source_plans(config):
        source_config = config_for_source(config, plan)
        if not (source_config.audio_writer_enabled and source_config.audio_transcription_provider == "speech_batch"):
            continue
        await _release_coordinator_of_ended_owner(
            client, source_registry_repository, plan.source_id,
        )
        coordinator = source_registry_repository.get_drive_coordinator(plan.source_id)
        poll_mode = _audio_poll_dispatch_mode(plan.source.desired_state, coordinator)
        if poll_mode is None:
            continue
        coordinator_owner_id = (
            coordinator.get("ownerInstanceId")
            if coordinator is not None and coordinator.get("state") in {"active", "draining"}
            else None
        )
        client_input = plan.to_payload()
        if coordinator_owner_id:
            client_input["coordinatorOwnerId"] = coordinator_owner_id
        if plan.source.desired_state == "paused":
            client_input["allowPausedAudio"] = True
        if poll_mode == "settlement-only":
            client_input["allowDrainingWork"] = True
        await _start_if_not_running(
            client, lifecycle_repository, plan.source_id, AUDIO_POLL_TRIGGER_ID,
            "audio_transcription_poll_orchestrator", client_input=client_input,
        )


def _audio_poll_dispatch_mode(
    desired_state: str,
    coordinator: dict[str, Any] | None,
) -> str | None:
    coordinator_state = coordinator.get("state") if coordinator is not None else None
    if coordinator_state == "draining":
        return "settlement-only"
    if desired_state == "active":
        return "normal"
    if desired_state == "paused" and coordinator_state == "active":
        return "normal"
    return None


@app.orchestration_trigger(context_name="context")
def audio_transcription_poll_orchestrator(context: df.DurableOrchestrationContext):
    """Page through documents awaiting transcription and finalize the completed ones."""
    orchestration_input = context.get_input()
    is_continuation = orchestration_input.get("orchestrationPhase") == "audio-poll"
    source_plan = (
        orchestration_input.get("sourcePlan")
        if is_continuation else orchestration_input.get("sourcePlan", orchestration_input)
    )
    owner_instance_id = orchestration_input.get("coordinatorOwnerId") or context.instance_id
    allow_paused_audio = orchestration_input.get("allowPausedAudio") is True
    allow_draining_work = orchestration_input.get("allowDrainingWork") is True
    continuation_token = orchestration_input.get("continuationToken") if is_continuation else None
    checked = orchestration_input.get("checked", 0) if is_continuation else 0
    succeeded = orchestration_input.get("succeeded", 0) if is_continuation else 0
    failed = orchestration_input.get("failed", 0) if is_continuation else 0
    pending = orchestration_input.get("pending", 0) if is_continuation else 0
    settlement_only = orchestration_input.get("settlementOnly", False) if is_continuation else False
    audio_phase = orchestration_input.get("audioPhase", "poll") if is_continuation else "poll"
    delta_phase = orchestration_input.get("deltaPhase", "items") if is_continuation else "items"
    delta_token = orchestration_input.get("deltaContinuationToken") if is_continuation else None
    if audio_phase not in {"poll", "delta"}:
        return {"status": "failed", "error": "audio_poll_phase_invalid"}

    if audio_phase == "poll":
        result = yield context.call_activity(
            "audio_transcription_poll_page_activity",
            {
                "continuationToken": continuation_token,
                "sourcePlan": source_plan,
                "coordinatorOwnerId": owner_instance_id,
                "allowPausedAudio": allow_paused_audio,
                "allowDrainingWork": allow_draining_work,
            },
        )
        if result.get("error"):
            return {"status": "failed", "error": result["error"]}
        checked += result["checked"]
        succeeded += result["succeeded"]
        failed += result["failed"]
        pending += result["pending"]
        settlement_only = settlement_only or result.get("settlementOnly") is True
        continuation_token = result.get("continuationToken")
        if continuation_token is not None:
            context.continue_as_new({
                "orchestrationPhase": "audio-poll",
                "sourcePlan": source_plan,
                "coordinatorOwnerId": owner_instance_id,
                "allowPausedAudio": allow_paused_audio,
                "allowDrainingWork": allow_draining_work,
                "continuationToken": continuation_token,
                "checked": checked,
                "succeeded": succeeded,
                "failed": failed,
                "pending": pending,
                "settlementOnly": settlement_only,
            })
            return
        if (
            pending == 0 and failed == 0
            and source_plan.get("desiredState") == "active"
            and not settlement_only
        ):
            context.continue_as_new({
                "orchestrationPhase": "audio-poll",
                "audioPhase": "delta",
                "sourcePlan": source_plan,
                "coordinatorOwnerId": owner_instance_id,
                "allowPausedAudio": allow_paused_audio,
                "allowDrainingWork": allow_draining_work,
                "deltaPhase": "items",
                "deltaContinuationToken": None,
                "checked": checked,
                "succeeded": succeeded,
                "failed": failed,
                "pending": pending,
                "settlementOnly": settlement_only,
            })
            return
        if pending == 0 and not settlement_only:
            yield context.call_activity(
                "settle_drive_coordinator_activity",
                {"sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id},
            )
    else:
        delta = yield context.call_activity(
            "delta_sync_activity",
            {
                "sourcePlan": source_plan,
                "instanceId": owner_instance_id,
                "coordinatorOwnerId": owner_instance_id,
                "deltaExecutionId": f"audio-catchup-{context.instance_id}",
                "settleCoordinator": True,
                "reducedItemsContinuationToken": (
                    delta_token if delta_phase == "items" else None
                ),
                "processingPhase": delta_phase,
                "phaseContinuationToken": (
                    delta_token if delta_phase != "items" else None
                ),
            },
        )
        if delta.get("error"):
            failed += 1
        else:
            pending += delta.get("pending", 0)
            failed += delta.get("failed", 0)
            delta_token = delta.get("phaseContinuationToken")
            if delta_token is not None and pending == 0 and failed == 0:
                context.continue_as_new({
                    "orchestrationPhase": "audio-poll",
                    "audioPhase": "delta",
                    "sourcePlan": source_plan,
                    "coordinatorOwnerId": owner_instance_id,
                    "allowPausedAudio": allow_paused_audio,
                    "allowDrainingWork": allow_draining_work,
                    "deltaPhase": delta.get("continuationPhase"),
                    "deltaContinuationToken": delta_token,
                    "checked": checked,
                    "succeeded": succeeded,
                    "failed": failed,
                    "pending": pending,
                    "settlementOnly": settlement_only,
                })
                return
    return {
        "status": "failed" if failed else "pending" if pending else "completed",
        "checked": checked,
        "succeeded": succeeded,
        "failed": failed,
        "pending": pending,
        "settlementOnly": settlement_only,
    }


@app.activity_trigger(input_name="payload")
def audio_transcription_poll_page_activity(payload: dict) -> dict:
    from ingestion.services import run_audio_transcription_poll_page
    from ingestion.source_connector import SharePointConnector

    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        resolve_payload = dict(payload)
        if payload.get("allowDrainingWork") is not True:
            resolve_payload.pop("coordinatorOwnerId", None)
        config, source_plan, registry = _resolve_source_config(resolve_payload)
        requested_owner_id = owner_instance_id
        if not isinstance(requested_owner_id, str) or not requested_owner_id:
            raise ValueError("drive_coordinator_owner_required")
        coordinator = registry.get_drive_coordinator(config.source_id)
        settlement_only = coordinator is not None and coordinator.get("state") == "draining"
        if settlement_only:
            owner_instance_id = coordinator.get("ownerInstanceId")
            if not isinstance(owner_instance_id, str) or not owner_instance_id:
                raise ValueError("draining_coordinator_owner_missing")
        elif source_plan.source.desired_state == "active":
            owner_instance_id = requested_owner_id
            registry.acquire_drive_coordinator(
                source_plan,
                owner_instance_id=owner_instance_id,
                operation="audio-poll",
            )
        elif payload.get("allowPausedAudio") is True:
            owner_instance_id = requested_owner_id
            registry.assert_drive_coordinator_owner(config.source_id, owner_instance_id)
        else:
            raise ValueError("paused_audio_settlement_not_authorized")
        repository = _build_repository(config)
        lifecycle_repository = _build_lifecycle_repository(config)
        source_control = repository.get_source_control(config.source_id)
        current_run = (
            repository.get_run(config.source_id, source_control.record.current_run_id)
            if source_control is not None else None
        )
        if (
            payload.get("fullSyncCatchup") is not True
            and not settlement_only
            and current_run is not None
            and current_run.record.status.value == "running"
        ):
            return {
                "checked": 0,
                "succeeded": 0,
                "failed": 0,
                "pending": 1,
                "settlementOnly": False,
                "continuationToken": None,
            }
        graph_client = _build_graph_client(config)
        sp_client = _build_sharepoint_client(config)
        language_client = _build_language_client(config) if config.enrichment_enabled else None
        openai_client = _build_openai_client(config)
        audit_container = _build_audit_container(config)
        connector = SharePointConnector(graph_client, config.drive_id, sp_client=sp_client, site_url=config.sharepoint_site_url)
        speech_token_provider = _build_speech_token_provider(config)
        blob_service_client = _build_audio_staging_client(config)
        outcome, token = run_audio_transcription_poll_page(
            config, repository, lifecycle_repository, connector, language_client, openai_client,
            speech_token_provider=speech_token_provider, blob_service_client=blob_service_client,
            page_size=EXECUTION_CONFIG.audio_poll_page_size,
            continuation_token=payload.get("continuationToken"),
            audit_container=audit_container,
            settlement_only=settlement_only,
            work_registry=registry,
            owner_instance_id=owner_instance_id,
        )
        for work in outcome.terminal_work:
            operation_kind = registry.get_document_operation_kind(
                config.source_id,
                work.work_id,
                work.source_run_id,
                work.document_id,
            )
            if operation_kind == "delta-sync" and work.status == "succeeded":
                terminal_document = repository.get_document(work.source_run_id, work.document_id)
                if (
                    terminal_document is None
                    or terminal_document.record.status.value != "ready"
                    or terminal_document.record.stage.value != "terminal"
                    or terminal_document.record.transcription_work_id != work.work_id
                    or terminal_document.record.transcription_attempt_id != work.attempt_id
                ):
                    raise RuntimeError("delta_audio_ready_winner_unverified")
                registry.record_document_winner(
                    config.source_id,
                    owner_instance_id,
                    document_id=terminal_document.record.document_id,
                    drive_id=terminal_document.record.drive_id,
                    item_id=terminal_document.record.item_id,
                    source_run_id=terminal_document.record.source_run_id,
                    document_key=terminal_document.record.document_key,
                    source_etag=terminal_document.record.e_tag,
                    lifecycle_generation=terminal_document.record.lifecycle_generation,
                    processing_profile_digest=terminal_document.record.processing_profile_digest,
                )
                _retire_prior_version(
                    config,
                    work.document_id,
                    work.source_run_id,
                    audit_container,
                    method="delta_sync",
                )
            registry.settle_document_operation(
                config.source_id,
                owner_instance_id,
                work.work_id,
                physical_run_id=work.source_run_id,
                item_id=work.document_id,
                operation_kind=operation_kind,
                status="succeeded" if work.status == "succeeded" else "failed",
                outcome={"documentId": work.document_id, "status": work.status},
            )
            if not repository.clear_transcription_work_identity(
                work.source_run_id,
                work.document_id,
                work.work_id,
                work.attempt_id,
            ):
                raise RuntimeError("audio_document_work_settlement_unverified")
        logger.info(
            "audio_poll_page_completed checked=%d succeeded=%d failed=%d pending=%d has_more=%s",
            outcome.checked, outcome.succeeded, outcome.failed, outcome.pending, token is not None,
        )
        return {
            "checked": outcome.checked, "succeeded": outcome.succeeded,
            "failed": outcome.failed, "pending": outcome.pending,
            "settlementOnly": settlement_only, "continuationToken": token,
        }
    except Exception:
        logger.exception("audio_transcription_poll_page_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": "audio_transcription_poll_failed"}


@app.activity_trigger(input_name="payload")
def prepare_retry_failed_page_activity(payload: dict) -> dict:
    """Claim and reset one failed document, or recover its prior claimed attempt."""
    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        config, source_plan, registry = _resolve_source_config(payload)
        operation_kind = payload.get("operationKind")
        run_id = payload.get("runId")
        if not all(isinstance(value, str) and value for value in (
            owner_instance_id, operation_kind, run_id,
        )):
            raise ValueError("retry_page_context_invalid")
        repository = _build_repository(config)

        unfinished = registry.list_unresolved_document_operations_page(
            source_plan.source_id,
            owner_instance_id,
            operation_kind,
            page_size=1,
        )
        if unfinished:
            work = unfinished[0]
            if work.get("state") != "claimed":
                return {"status": "pending", "continuationToken": (
                    (work.get("metadata") or {}).get("retryContinuationToken")
                )}
            source_run_id = work.get("physicalRunId")
            document_id = work.get("itemId")
            stored = repository.get_document(source_run_id, document_id)
            if stored is None:
                registry.mark_drive_coordinator_draining(source_plan.source_id, owner_instance_id)
                return {"status": "draining", "error": "retry_document_missing"}
            if stored.record.status.value == "failed":
                retry_doc = stored.record.to_cosmos_item()
                retry_doc["_etag"] = stored.etag
                reset = repository.reset_failed_to_discovered(retry_doc)
                if reset is None:
                    registry.mark_drive_coordinator_draining(source_plan.source_id, owner_instance_id)
                    return {"status": "draining", "error": "retry_reset_unverified"}
            elif stored.record.status.value != "discovered":
                registry.mark_drive_coordinator_draining(source_plan.source_id, owner_instance_id)
                return {"status": "draining", "error": "retry_document_state_unexpected"}
            metadata = work.get("metadata") or {}
            return {
                "status": "ready",
                "document": {"documentId": document_id, "sourceRunId": source_run_id},
                "operationId": work["id"],
                "workId": work["id"],
                "attemptId": work["id"],
                "operationKind": operation_kind,
                "continuationToken": metadata.get("retryContinuationToken"),
            }

        page = repository.list_failed_documents_page(
            source_plan.source_id,
            run_id,
            page_size=1,
            continuation_token=payload.get("continuationToken"),
        )
        if not page.items:
            return {"status": "complete", "continuationToken": None}
        failed_document = page.items[0]
        source_run_id = failed_document["sourceRunId"]
        document_id = failed_document["id"]
        claim = registry.begin_document_operation(
            source_plan,
            owner_instance_id=owner_instance_id,
            physical_run_id=source_run_id,
            item_id=document_id,
            operation_kind=operation_kind,
            metadata={"retryContinuationToken": page.continuation_token},
        )
        if claim.get("status") == "completed":
            return {
                "status": "completed",
                "outcome": claim.get("outcome"),
                "continuationToken": (claim.get("metadata") or {}).get("retryContinuationToken"),
            }
        if claim.get("status") != "acquired":
            return {"status": "pending", "continuationToken": page.continuation_token}
        reset = repository.reset_failed_to_discovered(failed_document)
        if reset is None:
            registry.mark_drive_coordinator_draining(source_plan.source_id, owner_instance_id)
            return {"status": "draining", "error": "retry_reset_unverified"}
        return {
            "status": "ready",
            "document": {"documentId": document_id, "sourceRunId": source_run_id},
            "operationId": claim["operationId"],
            "workId": claim["operationId"],
            "attemptId": claim["operationId"],
            "operationKind": operation_kind,
            "continuationToken": page.continuation_token,
        }
    except Exception as error:
        logger.exception("prepare_retry_failed_page_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"status": "failed", "error": str(error)}


@app.orchestration_trigger(context_name="context")
def retry_failed_orchestrator(context: df.DurableOrchestrationContext):
    """Reprocess only the failed documents that were reset to discovered."""
    from datetime import timedelta
    orchestration_input = context.get_input()
    if orchestration_input.get("retryPaged") is True:
        source_plan = orchestration_input.get("sourcePlan")
        owner_instance_id = orchestration_input.get("coordinatorOwnerId")
        run_id = orchestration_input.get("runId")
        operation_kind = orchestration_input.get("operationKind")
        continuation_token = orchestration_input.get("continuationToken")
        succeeded = orchestration_input.get("succeeded", 0)
        failed = orchestration_input.get("failed", 0)
        pending = orchestration_input.get("pending", 0)
        if (
            not isinstance(source_plan, dict)
            or not isinstance(owner_instance_id, str)
            or not isinstance(run_id, str)
            or not isinstance(operation_kind, str)
        ):
            return {"status": "failed", "error": "retry_page_input_invalid"}

        prepared = yield context.call_activity(
            "prepare_retry_failed_page_activity",
            {
                "sourcePlan": source_plan,
                "coordinatorOwnerId": owner_instance_id,
                "runId": run_id,
                "operationKind": operation_kind,
                "continuationToken": continuation_token,
            },
        )
        if prepared.get("status") == "draining":
            return {"status": "draining", "error": prepared.get("error")}
        if prepared.get("status") == "pending":
            return {"status": "pending", "pending": pending + 1}
        if prepared.get("status") == "failed":
            return {"status": "failed", "error": prepared.get("error")}

        if prepared.get("status") == "completed":
            previous = prepared.get("outcome") or {}
            if previous.get("status") in {"succeeded", "skipped"}:
                succeeded += 1
            elif previous.get("status") == "pending":
                pending += 1
            else:
                failed += 1
        elif prepared.get("status") == "ready":
            document = prepared["document"]
            task = context.call_activity(
                "process_document_activity",
                {
                    "runId": run_id,
                    "document": document,
                    "operationId": prepared["operationId"],
                    "workId": prepared["workId"],
                    "attemptId": prepared["attemptId"],
                    "operationKind": prepared["operationKind"],
                    "sourcePlan": source_plan,
                    "coordinatorOwnerId": owner_instance_id,
                },
            )
            deadline = context.current_utc_datetime + timedelta(
                minutes=EXECUTION_CONFIG.wave_timeout_minutes
            )
            timer = context.create_timer(deadline)
            try:
                winner = yield context.task_any([task, timer])
            except Exception:
                timer.cancel()
                fenced = yield context.call_activity(
                    "timeout_wave_documents_activity",
                    {
                        "documents": [{
                            **document,
                            "operationId": prepared["operationId"],
                            "workId": prepared["workId"],
                            "attemptId": prepared["attemptId"],
                            "operationKind": prepared["operationKind"],
                        }], "errorCode": "retry_exhausted",
                        "sourcePlan": source_plan,
                        "coordinatorOwnerId": owner_instance_id,
                    },
                )
                if fenced.get("error"):
                    return {"status": "failed", "phase": "timeout_fence", "error": fenced["error"]}
                yield context.call_activity(
                    "mark_drive_coordinator_draining_activity",
                    {"sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id},
                )
                return {"status": "draining", "failed": failed + 1}
            if winner == timer:
                fenced = yield context.call_activity(
                    "timeout_wave_documents_activity",
                    {
                        "documents": [{
                            **document,
                            "operationId": prepared["operationId"],
                            "workId": prepared["workId"],
                            "attemptId": prepared["attemptId"],
                            "operationKind": prepared["operationKind"],
                        }], "errorCode": "retry_timeout",
                        "sourcePlan": source_plan,
                        "coordinatorOwnerId": owner_instance_id,
                    },
                )
                if fenced.get("error"):
                    return {"status": "failed", "phase": "timeout_fence", "error": fenced["error"]}
                yield context.call_activity(
                    "mark_drive_coordinator_draining_activity",
                    {"sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id},
                )
                return {"status": "draining", "failed": failed + 1}
            timer.cancel()
            result = task.result
            if result.get("status") == "succeeded":
                succeeded += 1
            elif result.get("status") == "pending":
                pending += 1
            else:
                failed += 1
        elif prepared.get("status") == "complete":
            pass
        else:
            return {"status": "failed", "error": "retry_page_outcome_invalid"}

        next_token = prepared.get("continuationToken")
        if next_token is not None:
            context.continue_as_new({
                "retryPaged": True,
                "sourcePlan": source_plan,
                "coordinatorOwnerId": owner_instance_id,
                "runId": run_id,
                "operationKind": operation_kind,
                "continuationToken": next_token,
                "succeeded": succeeded,
                "failed": failed,
                "pending": pending,
            })
            return
        if pending > 0:
            return {"status": "pending", "succeeded": succeeded, "failed": failed, "pending": pending}
        settled = yield context.call_activity(
            "settle_drive_coordinator_activity",
            {"sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id},
        )
        if settled.get("error"):
            return {"status": "failed", "error": settled["error"]}
        return {
            "status": "failed" if failed else "completed",
            "succeeded": succeeded,
            "failed": failed,
            "pending": pending,
        }

    documents = orchestration_input.get("documents", [])
    source_plan = orchestration_input.get("sourcePlan")
    owner_instance_id = orchestration_input.get("coordinatorOwnerId")
    if not documents:
        return {"status": "nothing_to_retry"}
    if not isinstance(source_plan, dict) or not isinstance(owner_instance_id, str):
        return {"status": "failed", "error": "source_plan_required"}
    run_id = documents[0].get("sourceRunId", "unknown")
    is_continuation = orchestration_input.get("orchestrationPhase") == "retry-failed"
    start_index = orchestration_input.get("nextDocumentIndex", 0) if is_continuation else 0
    succeeded = orchestration_input.get("succeeded", 0) if is_continuation else 0
    failed = orchestration_input.get("failed", 0) if is_continuation else 0
    pending = orchestration_input.get("pending", 0) if is_continuation else 0
    draining = orchestration_input.get("draining", False) if is_continuation else False

    def continue_retry(next_index: int) -> None:
        context.continue_as_new({
            "orchestrationPhase": "retry-failed",
            "documents": documents,
            "sourcePlan": source_plan,
            "coordinatorOwnerId": owner_instance_id,
            "nextDocumentIndex": next_index,
            "succeeded": succeeded,
            "failed": failed,
            "pending": pending,
            "draining": draining,
        })

    for document_index in range(start_index, len(documents)):
        doc_ref = documents[document_index]
        persisted_operation_id = doc_ref.get("operationId") or doc_ref.get("workId")
        if isinstance(persisted_operation_id, str) and persisted_operation_id:
            claim = {
                "status": "acquired",
                "operationId": persisted_operation_id,
                "workId": persisted_operation_id,
                "attemptId": persisted_operation_id,
            }
        else:
            claim = yield context.call_activity(
                "claim_document_work_activity",
                {
                    "runId": run_id,
                    "document": doc_ref,
                    "operationKind": doc_ref.get("operationKind", "retry-failed"),
                    "sourcePlan": source_plan,
                    "coordinatorOwnerId": owner_instance_id,
                },
            )
        if claim.get("status") == "completed":
            outcome = claim.get("outcome") or {}
            if outcome.get("status") in {"succeeded", "skipped"}:
                succeeded += 1
            elif outcome.get("status") == "pending":
                pending += 1
            else:
                failed += 1
            if document_index + 1 < len(documents):
                continue_retry(document_index + 1)
                return
            continue
        if claim.get("status") in {"pending", "unresolved", "duplicate_active"}:
            pending += 1
            if document_index + 1 < len(documents):
                continue_retry(document_index + 1)
                return
            continue
        if claim.get("status") != "acquired":
            failed += 1
            draining = True
            break
        task = context.call_activity(
            "process_document_activity",
            {
                "runId": run_id,
                "document": doc_ref,
                "operationId": claim["operationId"],
                "workId": claim.get("workId", claim["operationId"]),
                "attemptId": claim.get("attemptId", claim["operationId"]),
                "operationKind": doc_ref.get("operationKind", "retry-failed"),
                "sourcePlan": source_plan,
                "coordinatorOwnerId": owner_instance_id,
            },
        )
        deadline = context.current_utc_datetime + timedelta(
            minutes=EXECUTION_CONFIG.wave_timeout_minutes
        )
        timer = context.create_timer(deadline)
        try:
            winner = yield context.task_any([task, timer])
        except Exception:
            timer.cancel()
            interrupted = yield context.call_activity(
                "timeout_wave_documents_activity",
                {
                    "documents": [{
                        **doc_ref,
                        "operationId": claim["operationId"],
                        "workId": claim.get("workId", claim["operationId"]),
                        "attemptId": claim.get("attemptId", claim["operationId"]),
                        "operationKind": doc_ref.get("operationKind", "retry-failed"),
                    }], "errorCode": "retry_exhausted",
                    "sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id,
                },
            )
            if interrupted.get("error"):
                return {
                    "status": "failed",
                    "phase": "timeout_fence",
                    "error": interrupted["error"],
                }
            failed += 1
            draining = True
            break
        if winner == timer:
            timed_out = yield context.call_activity(
                "timeout_wave_documents_activity",
                {
                    "documents": [{
                        **doc_ref,
                        "operationId": claim["operationId"],
                        "workId": claim.get("workId", claim["operationId"]),
                        "attemptId": claim.get("attemptId", claim["operationId"]),
                        "operationKind": doc_ref.get("operationKind", "retry-failed"),
                    }], "errorCode": "retry_timeout",
                    "sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id,
                },
            )
            if timed_out.get("error"):
                return {
                    "status": "failed",
                    "phase": "timeout_fence",
                    "error": timed_out["error"],
                }
            failed += 1
            draining = True
            break
        else:
            timer.cancel()
            result = task.result
            if result.get("status") == "succeeded":
                succeeded += 1
            elif result.get("status") == "pending":
                pending += 1
            else:
                failed += 1
        if not draining and document_index + 1 < len(documents):
            continue_retry(document_index + 1)
            return
    if draining:
        yield context.call_activity(
            "mark_drive_coordinator_draining_activity",
            {"sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id},
        )
        status = "draining"
    else:
        if pending == 0:
            yield context.call_activity(
                "settle_drive_coordinator_activity",
                {"sourcePlan": source_plan, "coordinatorOwnerId": owner_instance_id},
            )
        status = "pending" if pending else "completed"
    return {
        "status": status,
        "sourceId": source_plan.get("sourceId", ""),
        "retried": len(documents),
        "succeeded": succeeded,
        "failed": failed,
        "pending": pending,
    }


@app.activity_trigger(input_name="payload")
def activate_run_activity(payload: dict) -> dict:
    from ingestion.services import activate
    registry = None
    source_plan = None
    owner_instance_id = payload.get("instanceId")
    try:
        config, source_plan, registry = _resolve_source_config(payload)
        acquire_kwargs: dict[str, Any] = {
            "owner_instance_id": owner_instance_id,
            "operation": "full-sync",
        }
        if payload.get("sourcePlan", {}).get("reprovisionBootstrap") is True:
            acquire_kwargs["allow_reprovision_bootstrap"] = True
        registry.acquire_drive_coordinator(source_plan, **acquire_kwargs)
        repository = _build_repository(config)
        activated = activate(config, repository, owner_instance_id)
        return {
            "runId": activated.run.record.run_id,
            "runEtag": activated.run.etag,
            "sourceId": source_plan.source_id,
        }
    except Exception as error:
        logger.exception("activate_run_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": str(error)}


@app.activity_trigger(input_name="payload")
def discover_all_activity(payload: dict) -> dict:
    from ingestion.graph import DiscoveryState
    from ingestion.services import discover_next_page
    from ingestion.source_connector import SharePointConnector
    registry = None
    source_id = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        resolve_payload = dict(payload)
        resolve_payload.pop("coordinatorOwnerId", None)
        config, source_plan, registry = _resolve_source_config(resolve_payload)
        source_id = source_plan.source_id
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        registry.acquire_drive_coordinator(
            source_plan,
            owner_instance_id=owner_instance_id,
            operation="acl-resync",
        )
        repository = _build_repository(config)
        graph_client = _build_graph_client(config)
        sp_client = _build_sharepoint_client(config)
        connector = SharePointConnector(
            graph_client,
            config.drive_id,
            sp_client=sp_client,
            site_url=config.sharepoint_site_url,
        )
        state_payload = payload.get("discoveryState")
        state = (
            DiscoveryState.from_dict(state_payload)
            if isinstance(state_payload, dict)
            else DiscoveryState.initial()
        )
        next_state, documents, skipped, rejected = discover_next_page(
            config,
            payload["runId"],
            repository,
            connector,
            state,
            work_registry=registry,
            source_plan=source_plan,
            owner_instance_id=owner_instance_id,
        )
        return {
            "discoveredCount": len(documents),
            "itemsScanned": next_state.items_scanned - state.items_scanned,
            "discoveryState": next_state.to_dict(),
            "complete": next_state.complete,
            "skippedUnchanged": skipped,
            "rejectedCount": rejected,
        }
    except Exception:
        logger.exception("discover_all_activity failed")
        if registry is not None and isinstance(source_id, str) and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_id, owner_instance_id)
        return {"error": "discovery_page_failed"}


@app.activity_trigger(input_name="payload")
def list_discovered_documents_page_activity(payload: dict) -> dict:
    try:
        config, _source_plan, _registry = _resolve_source_config(payload)
        repository = _build_repository(config)
        source_run_id = f"{config.source_id}:{payload['runId']}"
        page = repository.list_document_page(
            source_run_id,
            page_size=EXECUTION_CONFIG.wave_size,
            continuation_token=payload.get("continuationToken"),
        )
        return {
            "documents": [
                {"documentId": row["id"], "sourceRunId": row["sourceRunId"]}
                for row in page.items
            ],
            "continuationToken": page.continuation_token,
        }
    except Exception:
        logger.exception("list_discovered_documents_page_activity failed")
        return {"error": "discovered_document_page_failed"}


@app.activity_trigger(input_name="payload")
def claim_document_work_activity(payload: dict) -> dict:
    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        _config, source_plan, registry = _resolve_source_config(payload)
        document_ref = payload.get("document")
        if (
            not isinstance(owner_instance_id, str)
            or not owner_instance_id
            or not isinstance(document_ref, dict)
        ):
            raise ValueError("document_work_claim_request_invalid")
        operation = registry.begin_document_operation(
            source_plan,
            owner_instance_id=owner_instance_id,
            physical_run_id=document_ref["sourceRunId"],
            item_id=document_ref["documentId"],
            operation_kind=payload.get("operationKind", "full-sync"),
        )
        operation_id = operation.get("operationId")
        if isinstance(operation_id, str) and operation_id:
            operation["workId"] = operation_id
            operation["attemptId"] = operation_id
        return operation
    except Exception as error:
        from ingestion.models import safe_error_from_exception
        logger.exception("claim_document_work_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        safe = safe_error_from_exception(error, "activity")
        return {"status": "failed", "error": safe.code}


@app.activity_trigger(input_name="payload")
def process_document_activity(payload: dict) -> dict:
    from ingestion.services import process_document
    from ingestion.source_connector import SharePointConnector
    document_ref = payload["document"]
    registry = None
    source_id = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        config, source_plan, registry = _resolve_source_config(payload)
        source_id = source_plan.source_id
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        operation_id = payload.get("operationId") or payload.get("workId")
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("document_operation_claim_required")
        work_id = operation_id
        attempt_id = operation_id
        operation_kind = payload.get("operationKind", "full-sync")
        start = registry.start_document_operation(
            source_plan.source_id,
            owner_instance_id,
            operation_id,
            document_ref["sourceRunId"],
            document_ref["documentId"],
            operation_kind,
        )
        if start["status"] == "completed":
            return start["outcome"]
        if start["status"] != "started":
            return {
                "documentId": document_ref["documentId"],
                "status": "pending",
                "error": f"document_operation_{start['status']}",
            }

        def record_outcome(result: dict) -> dict:
            if result["status"] == "pending":
                registry.settle_document_operation(
                    source_plan.source_id, owner_instance_id, operation_id,
                    physical_run_id=document_ref["sourceRunId"],
                    item_id=document_ref["documentId"],
                    operation_kind=operation_kind,
                    status="unresolved", outcome=result,
                )
            elif result["status"] in {"succeeded", "failed", "skipped"}:
                registry.settle_document_operation(
                    source_plan.source_id, owner_instance_id, operation_id,
                    physical_run_id=document_ref["sourceRunId"],
                    item_id=document_ref["documentId"],
                    operation_kind=operation_kind,
                    status=(
                        "succeeded"
                        if result["status"] in {"succeeded", "skipped"}
                        else "failed"
                    ),
                    outcome=result,
                )
            return result

        repository = _build_repository(config)
        lifecycle_repository = _build_lifecycle_repository(config)
        graph_client = _build_graph_client(config)
        di_client, cu_client = _build_extraction_clients(config)
        language_client = _build_language_client(config) if config.enrichment_enabled else None
        openai_client = _build_openai_client(config)
        audit_container = _build_audit_container(config)
        speech_token_provider = _build_speech_token_provider(config) if config.audio_writer_enabled else None
        blob_service_client = (
            _build_audio_staging_client(config)
            if config.audio_writer_enabled and config.audio_transcription_provider == "speech_batch"
            else None
        )

        sp_client = _build_sharepoint_client(config)
        connector = SharePointConnector(graph_client, config.drive_id, sp_client=sp_client, site_url=config.sharepoint_site_url)
        for attempt in range(1, EXECUTION_CONFIG.process_document_max_attempts + 1):
            stored = repository.get_document(document_ref["sourceRunId"], document_ref["documentId"])
            if stored is None:
                return record_outcome({
                    "documentId": document_ref["documentId"], "status": "skipped",
                })

            outcome = process_document(
                config, stored.record, stored.etag, repository, lifecycle_repository,
                connector, di_client, language_client, openai_client,
                audit_container=audit_container,
                cu_client=cu_client,
                speech_token_provider=speech_token_provider,
                blob_service_client=blob_service_client,
                document_work_id=work_id,
                document_work_attempt_id=attempt_id,
            )
            if outcome.status.value == "pending":
                return record_outcome({
                    "documentId": outcome.document_id,
                    "status": outcome.status.value,
                    "chunksWritten": outcome.chunks_written,
                    "error": None,
                })
            if outcome.status.value == "succeeded":
                ready = repository.get_document(document_ref["sourceRunId"], document_ref["documentId"])
                if ready is None or ready.record.status.value != "ready" or ready.record.stage.value != "terminal":
                    raise RuntimeError("document_ready_winner_unverified")
                registry.record_document_winner(
                    source_plan.source_id,
                    owner_instance_id,
                    document_id=ready.record.document_id,
                    drive_id=ready.record.drive_id,
                    item_id=ready.record.item_id,
                    source_run_id=ready.record.source_run_id,
                    document_key=ready.record.document_key,
                    source_etag=ready.record.e_tag,
                    lifecycle_generation=ready.record.lifecycle_generation,
                    processing_profile_digest=ready.record.processing_profile_digest,
                )
                _retire_prior_version(config, document_ref["documentId"], document_ref["sourceRunId"], audit_container)
                return record_outcome({
                    "documentId": outcome.document_id,
                    "status": outcome.status.value,
                    "chunksWritten": outcome.chunks_written,
                    "error": None,
                })
            if (
                outcome.error is None
                or not outcome.error.retryable
                or outcome.error.code == "dependency_unauthorized"
                or attempt >= EXECUTION_CONFIG.process_document_max_attempts
            ):
                return record_outcome({
                    "documentId": outcome.document_id,
                    "status": outcome.status.value,
                    "chunksWritten": outcome.chunks_written,
                    "error": outcome.error.code if outcome.error else None,
                })

            failed = repository.get_document(document_ref["sourceRunId"], document_ref["documentId"])
            if failed is None or failed.record.status.value != "failed":
                return {
                    "documentId": outcome.document_id,
                    "status": "failed",
                    "chunksWritten": outcome.chunks_written,
                    "error": "retry_state_conflict",
                }
            failed_item = failed.record.to_cosmos_item()
            failed_item["_etag"] = failed.etag
            if repository.reset_failed_to_discovered(failed_item) is None:
                return {
                    "documentId": outcome.document_id,
                    "status": "failed",
                    "chunksWritten": outcome.chunks_written,
                    "error": "retry_reset_conflict",
                }
            jitter = int(document_ref["documentId"][:2], 16) % 30
            delay = EXECUTION_CONFIG.process_document_retry_delay_seconds + jitter
            logger.warning(
                "process_document_activity retrying document=%s attempt=%d delay_seconds=%d error=%s",
                document_ref["documentId"], attempt + 1, delay, outcome.error.code,
            )
            time.sleep(delay)

        raise RuntimeError("process document retry loop exhausted unexpectedly")
    except Exception as error:
        from ingestion.models import safe_error_from_exception
        logger.exception("process_document_activity failed")
        if registry is not None and isinstance(source_id, str) and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_id, owner_instance_id)
        safe = safe_error_from_exception(error, "activity")
        return {"documentId": document_ref.get("documentId", ""), "status": "failed", "error": safe.code}


@app.activity_trigger(input_name="payload")
def timeout_wave_documents_activity(payload: dict) -> dict:
    """Verify exact running operations and keep the run draining."""

    documents = payload.get("documents")
    if not isinstance(documents, list) or not documents:
        return {"error": "invalid_timeout_documents", "failedCount": 0}
    verified_count = 0
    error_count = 0
    registry = None
    owner_instance_id = payload.get("coordinatorOwnerId")

    def mark_draining() -> None:
        if registry is not None and isinstance(owner_instance_id, str) and owner_instance_id:
            try:
                registry.mark_drive_coordinator_draining(config.source_id, owner_instance_id)
            except Exception:
                logger.exception("timeout fence could not mark coordinator draining")

    try:
        config, _source_plan, registry = _resolve_source_config(payload)
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("timeout_fence_owner_required")
        for document in documents:
            try:
                if not isinstance(document, dict):
                    raise ValueError("timeout_fence_document_invalid")
                operation_id = document.get("operationId") or document.get("workId")
                operation_kind = document.get("operationKind")
                source_run_id = document.get("sourceRunId")
                document_id = document.get("documentId")
                if not all(isinstance(value, str) and value for value in (
                    operation_id, operation_kind, source_run_id, document_id,
                )):
                    raise ValueError("timeout_fence_operation_identity_missing")
                registry.assert_document_operation_unresolved(
                    config.source_id,
                    owner_instance_id,
                    operation_id,
                    source_run_id,
                    document_id,
                    operation_kind,
                )
                verified_count += 1
            except Exception:
                error_count += 1
                logger.exception(
                    "timeout closure failed for document=%s",
                    document.get("documentId", "") if isinstance(document, dict) else "",
                )
    except Exception:
        logger.exception("timeout_wave_documents_activity failed")
        mark_draining()
        return {"error": "timeout_fence_failed", "verifiedCount": verified_count}
    if error_count:
        mark_draining()
        return {
            "error": "timeout_fence_incomplete",
            "verifiedCount": verified_count,
            "errorCount": error_count,
        }
    mark_draining()
    return {"verifiedCount": verified_count, "draining": True}


@app.activity_trigger(input_name="payload")
def fail_wave_documents_activity(payload: dict) -> dict:
    """Keep the legacy activity name but refuse unregistered bulk document writes."""
    return {"error": "unregistered_bulk_failure_disabled", "failedCount": 0}


@app.activity_trigger(input_name="payload")
def finalize_run_activity(payload: dict) -> dict:
    from ingestion.services import finalize
    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        config, source_plan, registry = _resolve_source_config(payload)
        repository = _build_repository(config)
        finalized = finalize(config, payload["runEtag"], repository, payload.get("itemsScanned", 0))
        return {"runStatus": finalized.record.status.value}
    except Exception as error:
        logger.exception("finalize_run_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"runStatus": "finalization_failed", "error": str(error)}


@app.activity_trigger(input_name="payload")
def delta_sync_activity(payload: Any) -> dict:
    from ingestion.services import run_delta_sync
    from ingestion.source_connector import SharePointConnector
    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId") or payload.get("instanceId")
    try:
        resolve_payload = dict(payload)
        resolve_payload.pop("coordinatorOwnerId", None)
        config, source_plan, registry = _resolve_source_config(resolve_payload)
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        acquire_kwargs: dict[str, Any] = {
            "owner_instance_id": owner_instance_id,
            "operation": "delta-sync",
        }
        if resolve_payload.get("sourcePlan", {}).get("reprovisionBootstrap") is True:
            acquire_kwargs["allow_reprovision_bootstrap"] = True
        registry.acquire_drive_coordinator(source_plan, **acquire_kwargs)
        repository = _build_repository(config)
        lifecycle_repository = _build_lifecycle_repository(config)
        graph_client = _build_graph_client(config)
        di_client, cu_client = _build_extraction_clients(config)
        language_client = _build_language_client(config) if config.enrichment_enabled else None
        openai_client = _build_openai_client(config)
        audit_container = _build_audit_container(config)
        sp_client = _build_sharepoint_client(config)
        connector = SharePointConnector(graph_client, config.drive_id, sp_client=sp_client, site_url=config.sharepoint_site_url)
        speech_token_provider = _build_speech_token_provider(config) if config.audio_writer_enabled else None
        blob_service_client = (
            _build_audio_staging_client(config)
            if config.audio_writer_enabled and config.audio_transcription_provider == "speech_batch"
            else None
        )
        outcome = run_delta_sync(
            config, repository, lifecycle_repository, connector,
            di_client, language_client, openai_client,
            audit_container=audit_container,
            cu_client=cu_client,
            speech_token_provider=speech_token_provider,
            blob_service_client=blob_service_client,
            work_registry=registry,
            source_plan=source_plan,
            owner_instance_id=owner_instance_id,
            delta_execution_id=payload.get("deltaExecutionId") or owner_instance_id,
            reduced_items_continuation_token=payload.get("reducedItemsContinuationToken"),
            reduced_items_page_size=EXECUTION_CONFIG.source_dispatch_page_size * 5,
            processing_phase=payload.get("processingPhase", "items"),
            phase_continuation_token=payload.get("phaseContinuationToken"),
            defer_cursor_commit=payload.get("deferCursorCommit") is True,
            force_full_baseline=payload.get("fullSyncBaseline") is True,
            enumerate_only=payload.get("enumerateOnly") is True,
            round_start_link=payload.get("roundStartLink"),
        )
        logger.info(
            "delta_sync_completed source=%s bootstrapped=%s created_or_updated=%d deleted=%d acl_resynced=%d quarantined=%d failed=%d pending=%d items_seen=%d",
            config.source_id, outcome.bootstrapped, outcome.created_or_updated, outcome.deleted,
            outcome.acl_resynced, outcome.quarantined, outcome.failed, outcome.pending,
            outcome.items_seen,
        )
        if (
            outcome.continuation_token is None
            and
            payload.get("settleCoordinator", True)
            and outcome.failed == 0
            and outcome.pending == 0
        ):
            registry.settle_drive_coordinator(config.source_id, owner_instance_id)
        elif (
            outcome.continuation_token is None
            and
            payload.get("settleCoordinator", True)
            and outcome.failed > 0
            and outcome.pending == 0
        ):
            registry.settle_drive_coordinator(config.source_id, owner_instance_id)
        return {
            "bootstrapped": outcome.bootstrapped,
            "createdOrUpdated": outcome.created_or_updated,
            "deleted": outcome.deleted,
            "aclResynced": outcome.acl_resynced,
            "quarantined": outcome.quarantined,
            "failed": outcome.failed,
            "pending": outcome.pending,
            "itemsSeen": outcome.items_seen,
            "reducedItemsContinuationToken": outcome.continuation_token,
            "continuationPhase": outcome.continuation_phase,
            "phaseContinuationToken": outcome.continuation_token,
        }
    except Exception as error:
        logger.exception("delta_sync_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": str(error)}


@app.activity_trigger(input_name="payload")
def full_sync_absence_reconcile_page_activity(payload: dict) -> dict:
    from ingestion.models import create_source_run_id
    from ingestion.services import run_full_sync_absence_reconciliation_page
    from ingestion.source_connector import SharePointConnector

    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        config, source_plan, registry = _resolve_source_config(payload)
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        registry.assert_drive_coordinator_owner(config.source_id, owner_instance_id)
        lifecycle_repository = _build_lifecycle_repository(config)
        graph_client = _build_graph_client(config)
        sp_client = _build_sharepoint_client(config)
        connector = SharePointConnector(
            graph_client,
            config.drive_id,
            sp_client=sp_client,
            site_url=config.sharepoint_site_url,
        )
        outcome, token = run_full_sync_absence_reconciliation_page(
            config,
            lifecycle_repository,
            connector,
            current_source_run_id=create_source_run_id(config.source_id, payload["currentRunId"]),
            page_size=EXECUTION_CONFIG.lifecycle_reconcile_page_size,
            continuation_token=payload.get("continuationToken"),
            work_registry=registry,
            source_plan=source_plan,
            owner_instance_id=owner_instance_id,
        )
        if outcome.failed:
            _mark_coordinator_draining_safely(registry, config.source_id, owner_instance_id)
        return {
            "checked": outcome.checked,
            "repaired": outcome.repaired,
            "failed": outcome.failed,
            "continuationToken": token,
        }
    except Exception:
        logger.exception("full_sync_absence_reconcile_page_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": "full_sync_absence_reconciliation_failed"}


@app.orchestration_trigger(context_name="context")
def full_sync_absence_reconcile_orchestrator(context: df.DurableOrchestrationContext):
    orchestration_input = context.get_input()
    is_continuation = (
        isinstance(orchestration_input, dict)
        and orchestration_input.get("phase") == "absence-reconcile"
    )
    source_plan = orchestration_input.get("sourcePlan")
    owner_instance_id = orchestration_input.get("coordinatorOwnerId")
    current_run_id = orchestration_input.get("currentRunId")
    continuation_token = orchestration_input.get("continuationToken") if is_continuation else None
    checked = orchestration_input.get("checked", 0) if is_continuation else 0
    repaired = orchestration_input.get("repaired", 0) if is_continuation else 0
    failed = orchestration_input.get("failed", 0) if is_continuation else 0
    if (
        not isinstance(source_plan, dict)
        or not isinstance(owner_instance_id, str)
        or not isinstance(current_run_id, str)
        or (continuation_token is not None and not isinstance(continuation_token, str))
    ):
        return {"status": "failed", "error": "absence_reconcile_input_invalid"}
    page = yield context.call_activity(
        "full_sync_absence_reconcile_page_activity",
        {
            "sourcePlan": source_plan,
            "coordinatorOwnerId": owner_instance_id,
            "currentRunId": current_run_id,
            "continuationToken": continuation_token,
        },
    )
    if page.get("error"):
        return {"status": "failed", "error": page["error"]}
    checked += page.get("checked", 0)
    repaired += page.get("repaired", 0)
    failed += page.get("failed", 0)
    next_token = page.get("continuationToken")
    if next_token is not None and failed == 0:
        context.continue_as_new({
            "phase": "absence-reconcile",
            "sourcePlan": source_plan,
            "coordinatorOwnerId": owner_instance_id,
            "currentRunId": current_run_id,
            "continuationToken": next_token,
            "checked": checked,
            "repaired": repaired,
            "failed": failed,
        })
        return
    return {
        "status": "failed" if failed else "completed",
        "checked": checked,
        "repaired": repaired,
        "failed": failed,
    }


@app.activity_trigger(input_name="payload")
def commit_delta_cursor_activity(payload: dict) -> dict:
    from ingestion.models import RunStatus

    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        config, source_plan, registry = _resolve_source_config(payload)
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        repository = _build_repository(config)
        source_control = repository.get_source_control(config.source_id)
        parent_run = repository.get_run(config.source_id, payload.get("parentRunId", ""))
        if (
            source_control is None
            or source_control.record.current_run_id != payload.get("parentRunId")
            or parent_run is None
            or parent_run.record.status is not RunStatus.COMPLETED
        ):
            raise ValueError("full_sync_parent_not_successful")
        registry.commit_full_sync_progress(
            source_plan,
            owner_instance_id,
            round_id=payload["deltaExecutionId"],
            candidate_delta_link=payload["candidateDeltaLink"],
        )
        return {"committed": True}
    except Exception:
        logger.exception("deferred delta cursor commit failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": "delta_cursor_commit_failed"}


@app.activity_trigger(input_name="payload")
def settle_drive_coordinator_activity(payload: dict) -> dict:
    registry = None
    source_id = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        config, _source_plan, registry = _resolve_source_config(payload)
        source_id = config.source_id
        registry.settle_drive_coordinator(
            config.source_id,
            owner_instance_id,
        )
        return {"settled": True}
    except Exception:
        logger.exception("drive coordinator settlement failed")
        if registry is not None and isinstance(source_id, str) and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_id, owner_instance_id)
        return {"error": "drive_coordinator_settlement_failed"}


@app.activity_trigger(input_name="payload")
def mark_drive_coordinator_draining_activity(payload: dict) -> dict:
    try:
        config, _source_plan, registry = _resolve_source_config(payload)
        registry.mark_drive_coordinator_draining(
            config.source_id,
            payload["coordinatorOwnerId"],
        )
        return {"state": "draining"}
    except Exception:
        logger.exception("drive coordinator draining transition failed")
        return {"error": "drive_coordinator_draining_failed"}


@app.activity_trigger(input_name="payload")
def acl_resync_page_activity(payload: dict) -> dict:
    from ingestion.services import run_acl_resync_page
    from ingestion.source_connector import SharePointConnector
    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId") or payload.get("instanceId")
    try:
        resolve_payload = dict(payload)
        resolve_payload.pop("coordinatorOwnerId", None)
        config, source_plan, registry = _resolve_source_config(resolve_payload)
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        registry.acquire_drive_coordinator(
            source_plan,
            owner_instance_id=owner_instance_id,
            operation="acl-resync",
        )
        lifecycle_repository = _build_lifecycle_repository(config)
        graph_client = _build_graph_client(config)
        sp_client = _build_sharepoint_client(config)
        audit_container = _build_audit_container(config)
        connector = SharePointConnector(graph_client, config.drive_id, sp_client=sp_client, site_url=config.sharepoint_site_url)
        outcome, token = run_acl_resync_page(
            config, lifecycle_repository, connector,
            page_size=EXECUTION_CONFIG.acl_resync_page_size,
            continuation_token=payload.get("continuationToken"),
            audit_container=audit_container,
            work_registry=registry,
            source_plan=source_plan,
            owner_instance_id=owner_instance_id,
            work_scope_id=owner_instance_id,
        )
        logger.info(
            "acl_resync_page_completed checked=%d unchanged=%d updated=%d retired=%d has_more=%s",
            outcome.checked, outcome.unchanged, outcome.updated, outcome.retired, token is not None,
        )
        return {
            "checked": outcome.checked,
            "unchanged": outcome.unchanged,
            "updated": outcome.updated,
            "retired": outcome.retired,
            "continuationToken": token,
        }
    except Exception as error:
        logger.exception("acl_resync_page_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": str(error)}


@app.activity_trigger(input_name="payload")
def source_retirement_page_activity(payload: dict) -> dict:
    from config import load_config
    from ingestion.source_registry import (
        SourcePlan,
        SourceRegistryConflictError,
        config_for_source,
    )
    from ingestion.services import run_source_retirement_page

    try:
        plan = SourcePlan.from_payload(payload.get("sourcePlan"))
        config = load_config()
        registry = _build_source_registry_repository(config)
        registry.begin_operator_maintenance(
            plan,
            owner_instance_id=payload.get("coordinatorOwnerId"),
            operation="source-retirement",
        )
        config = config_for_source(config, plan)
        lifecycle_repository = _build_lifecycle_repository(config)
        deleted = run_source_retirement_page(
            lifecycle_repository,
            _build_audit_container(config),
            source_id=plan.source_id,
            page_size=EXECUTION_CONFIG.lifecycle_reconcile_page_size,
            work_registry=registry,
            owner_instance_id=payload["coordinatorOwnerId"],
            business_name=plan.source.business_name,
        )
        deleted_orphans = 0
        if not deleted:
            deleted_orphans = lifecycle_repository.delete_source_chunks_page(
                source_id=plan.source_id,
                page_size=EXECUTION_CONFIG.lifecycle_reconcile_page_size,
            )
        if registry.read_source_plan(plan.source_id) != plan:
            raise SourceRegistryConflictError("source_catalog_changed")
        return {
            "deletedDocuments": deleted,
            "deletedOrphanChunks": deleted_orphans,
            "hasMore": bool(deleted or deleted_orphans),
        }
    except Exception:
        logger.exception("source retirement page failed")
        return {"error": "source_retirement_failed"}


@app.activity_trigger(input_name="payload")
def settle_source_retirement_activity(payload: dict) -> dict:
    from config import load_config
    from ingestion.source_registry import SourcePlan, SourceRegistryConflictError

    try:
        plan = SourcePlan.from_payload(payload.get("sourcePlan"))
        registry = _build_source_registry_repository(load_config())
        coordinator = registry.get_drive_coordinator(plan.source_id)
        if (
            coordinator is None
            or coordinator.get("state") != "maintenance"
            or coordinator.get("operation") != "source-retirement"
            or coordinator.get("ownerInstanceId") != payload.get("coordinatorOwnerId")
        ):
            raise SourceRegistryConflictError("drive_coordinator_owner_mismatch")
        registry.settle_drive_coordinator(plan.source_id, payload["coordinatorOwnerId"])
        return {"settled": True}
    except Exception:
        logger.exception("source retirement settlement failed")
        return {"error": "source_retirement_settlement_failed"}


@app.activity_trigger(input_name="payload")
def source_reprovision_page_activity(payload: dict) -> dict:
    from config import load_config
    from ingestion.source_registry import SourcePlan, SourceRegistryConflictError
    from source_registry import SourceRegistryError

    try:
        plan = SourcePlan.from_payload(payload.get("sourcePlan"))
        result = _build_source_registry_repository(
            load_config()
        ).delete_source_reprovision_page(
            plan,
            owner_instance_id=payload.get("coordinatorOwnerId"),
            operator_object_id=payload.get("operatorObjectId"),
            operator=payload.get("operator") or "unknown",
            page_size=EXECUTION_CONFIG.lifecycle_reconcile_page_size,
        )
        return {
            "deletedRecords": result["deletedRecords"],
            "hasMore": result["hasMore"],
        }
    except (SourceRegistryConflictError, SourceRegistryError, ValueError) as error:
        logger.warning("source reprovision page rejected error=%s", error)
        return {"error": str(error)}
    except Exception:
        logger.exception("source reprovision page failed")
        return {"error": "source_reprovision_failed"}


@app.activity_trigger(input_name="payload")
def fail_source_reprovision_activity(payload: dict) -> dict:
    from config import load_config
    from ingestion.source_registry import SourcePlan

    try:
        plan = SourcePlan.from_payload(payload.get("sourcePlan"))
        _build_source_registry_repository(load_config()).fail_source_reprovision(
            plan,
            owner_instance_id=payload.get("coordinatorOwnerId"),
            error_code=payload.get("error") or "source_reprovision_failed",
        )
        return {"recorded": True}
    except Exception:
        logger.exception("source reprovision failure record unavailable")
        return {"error": "source_reprovision_failure_record_failed"}


@app.activity_trigger(input_name="payload")
def complete_source_reprovision_activity(payload: dict) -> dict:
    from config import load_config
    from ingestion.source_registry import SourcePlan, SourceRegistryConflictError
    from ingestion.telemetry import write_audit_record
    from source_registry import SourceRegistryError

    try:
        config = load_config()
        plan = SourcePlan.from_payload(payload.get("sourcePlan"))
        completed = _build_source_registry_repository(
            config
        ).complete_source_reprovision(
            plan,
            owner_instance_id=payload.get("coordinatorOwnerId"),
        )
    except (SourceRegistryConflictError, SourceRegistryError, ValueError) as error:
        logger.warning("source reprovision completion rejected error=%s", error)
        return {"error": str(error)}
    except Exception:
        logger.exception("source reprovision completion failed")
        return {"error": "source_reprovision_failed"}
    try:
        write_audit_record(
            _build_audit_container(config),
            completed.source_id,
            "source-reprovision",
            {
                "operation": "source_reprovision_prepared",
                "businessName": completed.source.business_name,
                "desiredState": completed.source.desired_state,
                "catalogEtag": completed.catalog_etag,
                "deletedOperationalRecords": payload.get("deletedRecords", 0),
                "fullSyncRequired": True,
                "operatorObjectId": payload.get("operatorObjectId"),
                "operator": payload.get("operator") or "unknown",
            },
        )
    except Exception:
        logger.exception("source reprovision audit unavailable source=%s", completed.source_id)
    return {
        "status": "completed",
        "sourceId": completed.source_id,
        "desiredState": completed.source.desired_state,
        "catalogEtag": completed.catalog_etag,
        "deletedOperationalRecords": payload.get("deletedRecords", 0),
        "fullSyncRequired": True,
    }


@app.activity_trigger(input_name="payload")
def complete_reprovision_full_sync_activity(payload: dict) -> dict:
    from config import load_config
    from ingestion.source_registry import SourcePlan
    from ingestion.telemetry import write_audit_record

    try:
        config = load_config()
        plan = SourcePlan.from_payload(payload.get("sourcePlan"))
        registry = _build_source_registry_repository(config)
        registry.complete_reprovision_full_sync(
            plan,
            owner_instance_id=payload.get("coordinatorOwnerId"),
        )
    except Exception:
        logger.exception("source reprovision bootstrap completion failed")
        return {"error": "source_reprovision_bootstrap_complete_failed"}
    try:
        write_audit_record(
            _build_audit_container(config),
            plan.source_id,
            "source-reprovision",
            {
                "operation": "source_reprovisioned",
                "businessName": plan.source.business_name,
                "desiredState": "paused",
                "catalogEtag": plan.catalog_etag,
                "fullSyncRequired": False,
            },
        )
    except Exception:
        logger.exception("source reprovision completion audit unavailable source=%s", plan.source_id)
    return {"completed": True}


@app.activity_trigger(input_name="payload")
def lifecycle_reconcile_page_activity(payload: dict) -> dict:
    from ingestion.services import run_lifecycle_reconciliation_page
    from ingestion.source_connector import SharePointConnector

    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        resolve_payload = dict(payload)
        resolve_payload.pop("coordinatorOwnerId", None)
        config, source_plan, registry = _resolve_source_config(resolve_payload)
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        registry.acquire_drive_coordinator(
            source_plan,
            owner_instance_id=owner_instance_id,
            operation="lifecycle-reconcile",
        )
        repository = _build_repository(config)
        lifecycle_repository = _build_lifecycle_repository(config)
        connector: SharePointConnector | None = None

        def connector_provider() -> SharePointConnector:
            nonlocal connector
            if connector is None:
                connector = SharePointConnector(
                    _build_graph_client(config),
                    config.drive_id,
                    sp_client=_build_sharepoint_client(config),
                    site_url=config.sharepoint_site_url,
                )
            return connector

        outcome, token = run_lifecycle_reconciliation_page(
            config,
            repository,
            lifecycle_repository,
            connector_provider,
            source_id=config.source_id,
            page_size=EXECUTION_CONFIG.lifecycle_reconcile_page_size,
            continuation_token=payload.get("continuationToken"),
            work_registry=registry,
            source_plan=source_plan,
            owner_instance_id=owner_instance_id,
            work_scope_id=owner_instance_id,
        )
        if outcome.failed:
            _mark_coordinator_draining_safely(registry, config.source_id, owner_instance_id)
        logger.info(
            "lifecycle_reconcile_page_completed checked=%d repaired=%d "
            "failed=%d has_more=%s",
            outcome.checked,
            outcome.repaired,
            outcome.failed,
            token is not None,
        )
        return {
            "checked": outcome.checked,
            "repaired": outcome.repaired,
            "failed": outcome.failed,
            "continuationToken": token,
        }
    except Exception:
        logger.exception("lifecycle_reconcile_page_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": "lifecycle_reconciliation_failed"}


@app.activity_trigger(input_name="payload")
def duplicate_version_reconcile_page_activity(payload: dict) -> dict:
    from ingestion.services import run_duplicate_version_reconciliation_page

    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        config, source_plan, registry = _resolve_source_config(payload)
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        registry.acquire_drive_coordinator(
            source_plan,
            owner_instance_id=owner_instance_id,
            operation="lifecycle-reconcile",
        )
        repository = _build_repository(config)
        lifecycle_repository = _build_lifecycle_repository(config)
        outcome, token = run_duplicate_version_reconciliation_page(
            config,
            repository,
            lifecycle_repository,
            page_size=EXECUTION_CONFIG.lifecycle_reconcile_page_size,
            continuation_token=payload.get("continuationToken"),
            work_registry=registry,
            source_plan=source_plan,
            owner_instance_id=owner_instance_id,
            work_scope_id=owner_instance_id,
        )
        if outcome.failed:
            _mark_coordinator_draining_safely(registry, config.source_id, owner_instance_id)
        return {
            "checked": outcome.checked,
            "repaired": outcome.repaired,
            "failed": outcome.failed,
            "continuationToken": token,
        }
    except Exception:
        logger.exception("duplicate_version_reconcile_page_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": "duplicate_version_reconciliation_failed"}


@app.activity_trigger(input_name="payload")
def orphan_chunk_reconcile_page_activity(payload: dict) -> dict:
    from ingestion.services import run_orphan_chunk_reconciliation_page

    registry = None
    source_plan = None
    owner_instance_id = payload.get("coordinatorOwnerId")
    try:
        config, source_plan, registry = _resolve_source_config(payload)
        if not isinstance(owner_instance_id, str) or not owner_instance_id:
            raise ValueError("drive_coordinator_owner_required")
        registry.acquire_drive_coordinator(
            source_plan,
            owner_instance_id=owner_instance_id,
            operation="lifecycle-reconcile",
        )
        repository = _build_repository(config)
        lifecycle_repository = _build_lifecycle_repository(config)
        outcome, token = run_orphan_chunk_reconciliation_page(
            repository,
            lifecycle_repository,
            source_id=config.source_id,
            page_size=EXECUTION_CONFIG.lifecycle_reconcile_page_size,
            continuation_token=payload.get("continuationToken"),
            work_registry=registry,
            source_plan=source_plan,
            owner_instance_id=owner_instance_id,
            work_scope_id=owner_instance_id,
        )
        if outcome.failed:
            _mark_coordinator_draining_safely(registry, config.source_id, owner_instance_id)
        return {
            "checked": outcome.checked,
            "repaired": outcome.repaired,
            "failed": outcome.failed,
            "continuationToken": token,
        }
    except Exception:
        logger.exception("orphan_chunk_reconcile_page_activity failed")
        if registry is not None and source_plan is not None and isinstance(owner_instance_id, str):
            _mark_coordinator_draining_safely(registry, source_plan.source_id, owner_instance_id)
        return {"error": "orphan_chunk_reconciliation_failed"}



def _retire_prior_version(
    config,
    document_id: str,
    current_source_run_id: str,
    audit_container=None,
    *,
    method: str = "full_sync",
) -> None:
    """After full-sync re-processes a file, hard-delete any old ready version from a prior run."""
    from ingestion.telemetry import write_audit_record
    lifecycle_repo = _build_lifecycle_repository(config)

    def versions():
        page_method = getattr(lifecycle_repo, "list_ready_document_versions_page", None)
        if not callable(page_method):
            yield from lifecycle_repo.list_ready_document_versions(config.source_id, document_id)
            return
        continuation_token = None
        while True:
            page = page_method(
                source_id=config.source_id,
                document_id=document_id,
                page_size=50,
                continuation_token=continuation_token,
            )
            yield from page.items
            continuation_token = page.continuation_token
            if continuation_token is None:
                break

    for ref in versions():
        if ref.source_run_id == current_source_run_id:
            continue
        if (
            ref.source_id != config.source_id
            or ref.drive_id != config.drive_id
            or ref.document_id != document_id
        ):
            raise RuntimeError("superseded_document_identity_mismatch")
        if ref.status.value in {
            "discovered", "processing", "admitting", "acl_refreshing", "retiring", "deleting",
        }:
            raise RuntimeError("superseded_document_mutation_unsettled")
        lifecycle_repo.delete_document_and_chunks(
            source_run_id=ref.source_run_id,
            document_id=document_id,
            document_key=ref.document_key,
            etag=ref.etag,
        )
        if audit_container is not None:
            write_audit_record(audit_container, config.source_id, current_source_run_id, {
                "operation": "document_deleted", "documentId": document_id,
                "reason": "superseded", "method": method,
                "replacedDocumentKey": ref.document_key,
            }, business_name=config.business_name)


# Built once per worker and reused across invocations (Azure Functions Python guidance).
_client_cache: dict[str, Any] = {}


def _build_cosmos_client(config):
    if "cosmos_client" not in _client_cache:
        from cosmos_connection import create_cosmos_client

        _client_cache["cosmos_client"] = create_cosmos_client(
            config.cosmos_endpoint, config.managed_identity_client_id,
        )
    return _client_cache["cosmos_client"]


def _content_repository_cache_key(config):
    return (
        config.cosmos_database,
        config.cosmos_source_documents_container,
        config.cosmos_search_chunks_container,
    )


def _build_repository(config):
    cache_key = f"repository:{_content_repository_cache_key(config)}"
    if cache_key not in _client_cache:
        from ingestion.repository import IngestionRepository
        cosmos = _build_cosmos_client(config)
        db = cosmos.get_database_client(config.cosmos_database)
        _client_cache[cache_key] = IngestionRepository(
            db.get_container_client(config.cosmos_ingestion_runs_container),
            db.get_container_client(config.cosmos_source_documents_container),
            db.get_container_client(config.cosmos_search_chunks_container),
        )
    return _client_cache[cache_key]


def _build_lifecycle_repository(config):
    cache_key = f"lifecycle_repository:{_content_repository_cache_key(config)}"
    if cache_key not in _client_cache:
        from ingestion.lifecycle_repository import DocumentLifecycleRepository
        cosmos = _build_cosmos_client(config)
        db = cosmos.get_database_client(config.cosmos_database)
        _client_cache[cache_key] = DocumentLifecycleRepository(
            db.get_container_client(config.cosmos_ingestion_runs_container),
            db.get_container_client(config.cosmos_source_documents_container),
            db.get_container_client(config.cosmos_search_chunks_container),
        )
    return _client_cache[cache_key]


def _build_source_registry_repository(config):
    if "source_registry_repository" not in _client_cache:
        from ingestion.source_registry import SourceRegistryRepository

        cosmos = _build_cosmos_client(config)
        db = cosmos.get_database_client(config.cosmos_database)
        _client_cache["source_registry_repository"] = SourceRegistryRepository(
            db.get_container_client(config.cosmos_ingestion_sources_container),
            db.get_container_client(config.cosmos_ingestion_runs_container),
            config.tenant_id,
        )
    return _client_cache["source_registry_repository"]


def _resolve_source_config(payload: dict[str, Any]):
    from config import load_config
    from ingestion.source_registry import SourcePlan, SourceRegistryConflictError, config_for_source

    source_plan = SourcePlan.from_payload(payload.get("sourcePlan"))
    config = load_config()
    registry = _build_source_registry_repository(config)
    current = registry.read_source_plan(source_plan.source_id)
    if current != source_plan:
        raise SourceRegistryConflictError("source_catalog_changed")
    source_payload = payload.get("sourcePlan")
    reprovision_bootstrap = (
        isinstance(source_payload, dict)
        and source_payload.get("reprovisionBootstrap") is True
        and current.source.desired_state == "paused"
        and registry.is_reprovision_full_sync_required(current.source_id)
    )
    paused_audio_settlement = (
        payload.get("allowPausedAudio") is True
        and current.source.desired_state == "paused"
    )
    owner_instance_id = payload.get("coordinatorOwnerId")
    coordinator = (
        registry.get_drive_coordinator(current.source_id)
        if payload.get("allowDrainingWork") is True
        and isinstance(owner_instance_id, str)
        and owner_instance_id
        else None
    )
    draining_work_settlement = (
        coordinator is not None
        and coordinator.get("state") == "draining"
        and coordinator.get("ownerInstanceId") == owner_instance_id
    )
    if (
        current.source.desired_state != "active"
        and not paused_audio_settlement
        and not draining_work_settlement
        and not reprovision_bootstrap
    ):
        raise SourceRegistryConflictError("source_not_active")
    if isinstance(owner_instance_id, str) and owner_instance_id:
        if not draining_work_settlement:
            registry.assert_drive_coordinator_owner(current.source_id, owner_instance_id)
    return config_for_source(config, source_plan), source_plan, registry


def _write_webhook_audit(
    source_id: str,
    business_name: str,
    operation: str,
    extra: dict,
) -> None:
    """Best-effort audit record for webhook events."""
    try:
        from config import load_config
        config = load_config()
        container = _build_audit_container(config)
        from ingestion.telemetry import write_audit_record
        write_audit_record(
            container,
            source_id,
            "webhook",
            {"operation": operation, **extra},
            business_name=business_name,
        )
    except Exception:
        logger.warning("webhook_audit_write_failed", exc_info=True)


def _build_graph_client(config):
    """Cached across invocations — do not wrap call sites in `with graph_client:`, that
    would close this shared client after first use."""
    if "graph_client" not in _client_cache:
        from azure.identity import CertificateCredential
        from azure.keyvault.secrets import SecretClient
        from ingestion.graph import GraphCredentialAuth
        import httpx
        import base64

        credential = create_azure_credential(config.managed_identity_client_id)
        secret_client = SecretClient(config.key_vault_uri, credential)
        try:
            cert_secret = secret_client.get_secret(config.certificate_secret_name)
            cert_data = base64.b64decode(cert_secret.value, validate=True)
        except Exception as error:
            raise RuntimeError(f"Failed to load Graph certificate from Key Vault: {type(error).__name__}") from error
        graph_credential = CertificateCredential(
            tenant_id=config.tenant_id,
            client_id=config.app_client_id,
            certificate_data=cert_data,
        )
        transport = httpx.HTTPTransport(retries=3)
        _client_cache["graph_client"] = httpx.Client(
            auth=GraphCredentialAuth(graph_credential, "https://graph.microsoft.com/.default"), transport=transport, timeout=120,
        )
    return _client_cache["graph_client"]


def _build_sharepoint_client(config):
    """Build an httpx.Client for SharePoint REST API using the same cert but SP scope. Cached (see _build_graph_client)."""
    if not config.sharepoint_site_url:
        raise EnvironmentError("Required SharePoint site URL is not configured")
    from urllib.parse import urlparse
    from ingestion.graph import GraphCredentialAuth, validate_sharepoint_drive_site
    host = (urlparse(config.sharepoint_site_url).hostname or "").casefold()
    if not host:
        raise EnvironmentError("Required SharePoint site URL is invalid")
    validate_sharepoint_drive_site(
        _build_graph_client(config),
        config.drive_id,
        config.sharepoint_site_url,
        config.execution.acl_max_pages,
        expected_site_id=getattr(config, "sharepoint_site_id", None),
    )
    cache_key = f"sharepoint_client:{host}"
    if cache_key not in _client_cache:
        from azure.identity import CertificateCredential
        from azure.keyvault.secrets import SecretClient
        import httpx
        import base64

        credential = create_azure_credential(config.managed_identity_client_id)
        secret_client = SecretClient(config.key_vault_uri, credential)
        cert_secret = secret_client.get_secret(config.certificate_secret_name)
        cert_data = base64.b64decode(cert_secret.value, validate=True)
        sp_scope = f"https://{host}/.default"
        sp_credential = CertificateCredential(
            tenant_id=config.tenant_id,
            client_id=config.app_client_id,
            certificate_data=cert_data,
        )
        transport = httpx.HTTPTransport(retries=3)
        _client_cache[cache_key] = httpx.Client(
            auth=GraphCredentialAuth(sp_credential, sp_scope),
            transport=transport,
            timeout=60,
        )
    return _client_cache[cache_key]


def _build_di_client(config):
    if "di_client" not in _client_cache:
        from azure.ai.documentintelligence import DocumentIntelligenceClient
        credential = create_azure_credential(config.managed_identity_client_id)
        _client_cache["di_client"] = DocumentIntelligenceClient(endpoint=config.document_intelligence_endpoint, credential=credential)
    return _client_cache["di_client"]


def _build_extraction_clients(config):
    from config import ExtractionProvider

    if not config.extraction_enabled:
        return None, None
    if config.extraction_provider is ExtractionProvider.CONTENT_UNDERSTANDING:
        return None, _build_cu_client(config)
    return _build_di_client(config), None


def _build_cu_client(config):
    if "cu_client" not in _client_cache:
        from azure.ai.contentunderstanding import ContentUnderstandingClient
        credential = create_azure_credential(config.managed_identity_client_id)
        _client_cache["cu_client"] = ContentUnderstandingClient(
            endpoint=config.content_understanding_endpoint.rstrip("/"),
            credential=credential,
        )
    return _client_cache["cu_client"]


def _build_language_client(config):
    if "language_client" not in _client_cache:
        from azure.ai.textanalytics import TextAnalyticsClient
        credential = create_azure_credential(config.managed_identity_client_id)
        _client_cache["language_client"] = TextAnalyticsClient(endpoint=config.language_endpoint, credential=credential, api_version="2023-04-01")
    return _client_cache["language_client"]


def _build_openai_client(config):
    if "openai_client" not in _client_cache:
        from azure.identity import get_bearer_token_provider
        from openai import AzureOpenAI
        credential = create_azure_credential(config.managed_identity_client_id)
        _client_cache["openai_client"] = AzureOpenAI(
            azure_endpoint=config.openai_endpoint,
            azure_ad_token_provider=get_bearer_token_provider(credential, "https://cognitiveservices.azure.com/.default"),
            api_version="2024-10-21",
        )
    return _client_cache["openai_client"]


def _build_speech_token_provider(config):
    if "speech_token_provider" not in _client_cache:
        from azure.identity import get_bearer_token_provider
        credential = create_azure_credential(config.managed_identity_client_id)
        _client_cache["speech_token_provider"] = get_bearer_token_provider(
            credential, "https://cognitiveservices.azure.com/.default",
        )
    return _client_cache["speech_token_provider"]


def _build_audio_staging_client(config):
    if "audio_staging_client" not in _client_cache:
        from azure.storage.blob import BlobServiceClient
        credential = create_azure_credential(config.managed_identity_client_id)
        _client_cache["audio_staging_client"] = BlobServiceClient(
            account_url=config.audio_staging_blob_endpoint, credential=credential,
        )
    return _client_cache["audio_staging_client"]


def _build_audit_container(config):
    if "audit_container" not in _client_cache:
        from ingestion.telemetry import COSMOS_AUDIT_CONTAINER_NAME
        cosmos = _build_cosmos_client(config)
        db = cosmos.get_database_client(config.cosmos_database)
        _client_cache["audit_container"] = db.get_container_client(COSMOS_AUDIT_CONTAINER_NAME)
    return _client_cache["audit_container"]


def _json_response(
    payload: dict,
    status_code: int,
    *,
    headers: dict[str, str] | None = None,
) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, default=str),
        status_code=status_code,
        mimetype="application/json",
        headers=headers,
    )
