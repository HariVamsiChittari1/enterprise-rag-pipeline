#!/usr/bin/env python3
"""Run a webhook-only SharePoint ingestion and ACL demo against the Function API."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_QUESTION = (
    "What are the employee responsibilities under the Anti-Bribery and "
    "Anti-Corruption Policy?"
)


class DemoError(RuntimeError):
    """An expected demo preflight, API, or timeout failure."""


@dataclass(frozen=True)
class DemoConfig:
    function_app: str
    client_id: str
    file_name: str
    editors_group_id: str
    question: str
    timeout_seconds: int
    poll_seconds: int
    max_ready_documents: int
    preflight_only: bool
    query_timeout_seconds: int
    resume_after_upload: bool

    @property
    def base_url(self) -> str:
        return f"https://{self.function_app}.azurewebsites.net"


@dataclass(frozen=True)
class DocumentSnapshot:
    document_id: str
    source_name: str
    status: str
    allowed_group_ids: tuple[str, ...]
    acl_hash: str | None
    acl_evaluated_at: str | None
    expected_chunks: int | None
    written_chunks: int | None
    retired_reason: str | None
    error: str | None
    updated_at: str | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "DocumentSnapshot":
        document_id = row.get("documentId")
        source_name = row.get("sourceName")
        status = row.get("status")
        if not all(isinstance(value, str) and value for value in (document_id, source_name, status)):
            raise DemoError("Target source-document record is missing required fields.")
        group_ids = row.get("allowedGroupIds") or []
        if not isinstance(group_ids, list) or not all(isinstance(value, str) for value in group_ids):
            raise DemoError("Target source-document record has invalid allowedGroupIds.")
        return cls(
            document_id=document_id,
            source_name=source_name,
            status=status,
            allowed_group_ids=tuple(sorted(group_ids)),
            acl_hash=_as_optional_string(row.get("aclHash")),
            acl_evaluated_at=_as_optional_string(row.get("aclEvaluatedAt")),
            expected_chunks=_as_optional_int(row.get("expectedChunkCount")),
            written_chunks=_as_optional_int(row.get("writtenChunkCount")),
            retired_reason=_as_optional_string(row.get("retiredReason")),
            error=_as_optional_string(row.get("error")),
            updated_at=_as_optional_string(row.get("updatedAt")),
        )


class FunctionApi:
    def __init__(self, config: DemoConfig, token: str) -> None:
        self._base_url = config.base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def get(self, path: str) -> dict[str, Any]:
        return self._request("GET", path)

    def get_optional(self, path: str) -> dict[str, Any] | None:
        return self._request("GET", path, allow_not_found=True)

    def post(
        self,
        path: str,
        payload: dict[str, Any],
        *,
        timeout_seconds: int = 30,
    ) -> dict[str, Any]:
        return self._request("POST", path, payload, timeout_seconds=timeout_seconds)

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        allow_not_found: bool = False,
        timeout_seconds: int = 30,
    ) -> dict[str, Any] | None:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(f"{self._base_url}{path}", data=body, headers=self._headers, method=method)
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                data = json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            details = error.read().decode("utf-8", errors="replace")
            if allow_not_found and error.code == 404:
                return None
            raise DemoError(f"{method} {path} returned HTTP {error.code}: {details}") from error
        except URLError as error:
            raise DemoError(f"{method} {path} could not reach the Function API: {error.reason}") from error
        except TimeoutError as error:
            raise DemoError(
                f"{method} {path} timed out after {timeout_seconds} seconds. "
                "Check the retrieval service health and retry the retrieval stage."
            ) from error
        except json.JSONDecodeError as error:
            raise DemoError(f"{method} {path} returned invalid JSON.") from error
        if not isinstance(data, dict):
            raise DemoError(f"{method} {path} returned an unexpected JSON shape.")
        return data


class DemoReporter:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def heading(self, text: str) -> None:
        print(f"\n{'=' * 68}\n{text}\n{'=' * 68}")

    def pass_(self, text: str) -> None:
        print(f"[PASS] {text}")
        self.events.append({"time": _utc_now(), "level": "PASS", "message": text})

    def info(self, text: str) -> None:
        print(f"[INFO] {text}")
        self.events.append({"time": _utc_now(), "level": "INFO", "message": text})

    def wait(self, text: str) -> None:
        print(f"[WAIT] {text}")

    def warn(self, text: str) -> None:
        print(f"[WARN] {text}")
        self.events.append({"time": _utc_now(), "level": "WARN", "message": text})

    def fail(self, text: str) -> None:
        print(f"[FAIL] {text}", file=sys.stderr)
        self.events.append({"time": _utc_now(), "level": "FAIL", "message": text})


def main() -> int:
    config = _parse_args()
    reporter = DemoReporter()
    report: dict[str, Any] = {
        "startedAt": _utc_now(),
        "config": {
            "functionApp": config.function_app,
            "fileName": config.file_name,
            "editorsGroupId": config.editors_group_id,
            "question": config.question,
        },
        "stages": {},
    }

    try:
        reporter.heading("ENTERPRISE RAG WEBHOOK + ACL DEMO")
        token = _get_access_token(config.client_id)
        reporter.pass_("Azure CLI access token acquired")
        api = FunctionApi(config, token)

        source_documents = _run_preflight(api, config, reporter, report)
        if config.preflight_only:
            reporter.pass_("Preflight completed; no SharePoint or Cosmos changes were made")
            return 0
        existing = _find_ready_target(source_documents, config.file_name)
        if existing is not None and not config.resume_after_upload:
            raise DemoError(
                f"{config.file_name} is already ready. Delete it before starting the upload demonstration, "
                "or rerun with --resume-after-upload to continue an interrupted demo."
            )

        if existing is not None:
            uploaded = existing
            reporter.info("Resuming from an existing ready target document")
        else:
            _pause(f"Upload {config.file_name} in SharePoint. Do not change permissions yet.")
            uploaded = _wait_for_document(
                api,
                config,
                reporter,
                "webhook-driven ingestion to produce a ready document",
                lambda document: document.status == "ready",
            )
        if config.editors_group_id not in uploaded.allowed_group_ids:
            raise DemoError(
                "The uploaded document does not contain SharePoint Editors in allowedGroupIds. "
                "Stop and verify the SharePoint permission model before the ACL demo."
            )
        if len(uploaded.allowed_group_ids) < 2:
            raise DemoError(
                "The target has only one allowed group. Removing it would retire the document, "
                "which cannot be restored by the ready-document ACL resync flow."
            )
        _show_document("Upload result", uploaded)
        report["stages"]["uploaded"] = asdict(uploaded)
        _show_delta_and_audit(api, config, reporter, uploaded.document_id)
        _run_retrieval(api, config, reporter, report, "after_upload", target_must_be_cited=True)

        _pause("Remove SharePoint Editors in SharePoint site permissions. Do not edit the PDF.")
        removed = _wait_for_captured_document(
            api,
            config,
            reporter,
            uploaded.document_id,
            "SharePoint Editors to be removed from allowedGroupIds",
            lambda document: (
                document.status == "ready"
                and config.editors_group_id not in document.allowed_group_ids
            ),
        )
        _show_acl_change("ACL removal result", uploaded, removed, config.editors_group_id)
        report["stages"]["acl_removed"] = asdict(removed)
        _show_delta_and_audit(api, config, reporter, uploaded.document_id)

        _pause("Add SharePoint Editors back in SharePoint site permissions. Do not edit the PDF.")
        restored = _wait_for_captured_document(
            api,
            config,
            reporter,
            uploaded.document_id,
            "SharePoint Editors to be restored to allowedGroupIds",
            lambda document: (
                document.status == "ready"
                and config.editors_group_id in document.allowed_group_ids
            ),
        )
        _show_acl_change("ACL restoration result", removed, restored, config.editors_group_id)
        report["stages"]["acl_restored"] = asdict(restored)
        _show_delta_and_audit(api, config, reporter, uploaded.document_id)

        _pause(f"Delete {config.file_name} in SharePoint.")
        _wait_for_document_absence(
            api,
            config,
            reporter,
            uploaded.document_id,
            "the target document to be hard-deleted (absent from source-documents)",
        )
        print(f"\nDelete result:\n  documentId: {uploaded.document_id}\n  status: hard-deleted (row removed)")
        report["stages"]["deleted"] = {"documentId": uploaded.document_id, "hardDeleted": True}
        _show_delta_and_audit(api, config, reporter, uploaded.document_id)
        _run_retrieval(api, config, reporter, report, "after_delete", target_must_be_cited=False)

        reporter.pass_("Webhook, delta-sync, ACL-sync, and deletion demo completed")
        return 0
    except DemoError as error:
        reporter.fail(str(error))
        _print_recovery_guidance()
        return 1
    finally:
        report["completedAt"] = _utc_now()
        report["events"] = reporter.events
        _write_report(report, reporter)


def _parse_args() -> DemoConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--function-app", required=True, help="Azure Function App name")
    parser.add_argument("--client-id", required=True, help="Admin API application client ID")
    parser.add_argument(
        "--file-name",
        default="sample-abac-policy-v1.pdf",
        help="Target SharePoint filename",
    )
    parser.add_argument(
        "--editors-group-id",
        required=True,
        help="SharePoint Editors Entra security group ID",
    )
    parser.add_argument("--question", default=DEFAULT_QUESTION, help="Retrieval question")
    parser.add_argument("--timeout-seconds", type=int, default=180, help="Maximum wait per stage")
    parser.add_argument("--poll-seconds", type=int, default=5, help="Polling delay")
    parser.add_argument(
        "--query-timeout-seconds",
        type=int,
        default=60,
        help="Maximum wait for the RAG query response",
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Validate deployed prerequisites without starting the interactive demo",
    )
    parser.add_argument(
        "--resume-after-upload",
        action="store_true",
        help="Continue from an existing ready target after an interrupted upload stage",
    )
    parser.add_argument(
        "--max-ready-documents",
        type=int,
        default=50,
        help="Maximum ready documents for the site-level ACL safety scan",
    )
    args = parser.parse_args()
    if (
        args.timeout_seconds < 10
        or args.poll_seconds < 1
        or args.max_ready_documents < 1
        or args.query_timeout_seconds < 10
    ):
        parser.error("timeouts must be >= 10; poll-seconds and max-ready-documents must be positive")
    return DemoConfig(**vars(args))


def _get_access_token(client_id: str) -> str:
    azure_cli = "az.cmd" if os.name == "nt" else "az"
    try:
        result = subprocess.run(
            [azure_cli, "account", "get-access-token", "--resource", f"api://{client_id}", "--query", "accessToken", "-o", "tsv"],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as error:
        raise DemoError("Azure CLI is not installed or is not on PATH.") from error
    except subprocess.CalledProcessError as error:
        details = error.stderr.strip() or error.stdout.strip()
        raise DemoError(f"Could not obtain an Azure CLI access token. Run 'az login'. {details}") from error
    token = result.stdout.strip()
    if not token:
        raise DemoError("Azure CLI returned an empty access token. Run 'az login'.")
    return token


def _run_preflight(
    api: FunctionApi, config: DemoConfig, reporter: DemoReporter, report: dict[str, Any]
) -> list[dict[str, Any]]:
    reporter.heading("PREFLIGHT")
    ingestion_runs = api.get("/api/ingestion/inspect?container=ingestion-runs&limit=200")
    subscription = next(
        (row for row in _rows(ingestion_runs) if row.get("id") == "webhook-subscription"), None
    )
    if subscription is None:
        reporter.warn(
            "Webhook subscription record is not present in the capped ingestion-runs sample. "
            "The upload/ACL/delete document state will be the authoritative webhook evidence."
        )
    else:
        reporter.pass_(f"Webhook subscription record found: {subscription.get('subscriptionId', '<unknown>')}")
        report["stages"]["subscription"] = subscription

    full_sync = api.get_optional("/api/ingestion/status")
    if full_sync is None:
        reporter.pass_("No full-sync orchestration instance is recorded")
    else:
        runtime_status = str(full_sync.get("runtimeStatus", ""))
        if runtime_status.endswith("Running") or runtime_status.endswith("Pending"):
            raise DemoError("Full sync is active. Wait for it to complete before changing SharePoint.")
        reporter.pass_(f"Full sync is not active: {runtime_status or 'no active instance'}")

    source_documents = _rows(api.get("/api/ingestion/inspect?container=source-documents&limit=200"))
    if len(source_documents) == 200:
        raise DemoError(
            "The source-document inspect response reached its 200-row cap. "
            "This demo cannot reliably locate the target document in the current corpus."
        )
    ready_documents = sum(1 for row in source_documents if row.get("status") == "ready")
    if ready_documents > config.max_ready_documents:
        raise DemoError(
            f"There are {ready_documents} ready documents, above the site-level ACL safety limit "
            f"of {config.max_ready_documents}. Use a file-level permission change for a deterministic demo."
        )
    reporter.pass_(f"Ready documents: {ready_documents} / safety limit: {config.max_ready_documents}")
    report["stages"]["preflight"] = {"readyDocuments": ready_documents}
    return source_documents


def _wait_for_document(
    api: FunctionApi,
    config: DemoConfig,
    reporter: DemoReporter,
    description: str,
    predicate: Callable[[DocumentSnapshot], bool],
) -> DocumentSnapshot:
    deadline = time.monotonic() + config.timeout_seconds
    while time.monotonic() < deadline:
        matches = [
            DocumentSnapshot.from_row(row)
            for row in _rows(api.get("/api/ingestion/inspect?container=source-documents&limit=200"))
            if row.get("sourceName") == config.file_name
        ]
        matches.sort(key=lambda document: document.updated_at or "", reverse=True)
        for document in matches:
            if predicate(document):
                reporter.pass_(f"Observed {description}")
                return document
        reporter.wait(f"Waiting for {description}...")
        time.sleep(config.poll_seconds)
    raise DemoError(f"Timed out after {config.timeout_seconds} seconds waiting for {description}.")


def _wait_for_captured_document(
    api: FunctionApi,
    config: DemoConfig,
    reporter: DemoReporter,
    document_id: str,
    description: str,
    predicate: Callable[[DocumentSnapshot], bool],
) -> DocumentSnapshot:
    deadline = time.monotonic() + config.timeout_seconds
    while time.monotonic() < deadline:
        rows = _rows(api.get("/api/ingestion/inspect?container=source-documents&limit=200"))
        match = next((row for row in rows if row.get("documentId") == document_id), None)
        if match is not None:
            document = DocumentSnapshot.from_row(match)
            if predicate(document):
                reporter.pass_(f"Observed {description}")
                return document
        reporter.wait(f"Waiting for {description}...")
        time.sleep(config.poll_seconds)
    raise DemoError(f"Timed out after {config.timeout_seconds} seconds waiting for {description}.")


def _wait_for_document_absence(
    api: FunctionApi,
    config: DemoConfig,
    reporter: DemoReporter,
    document_id: str,
    description: str,
) -> None:
    """Poll until a hard-deleted document row no longer appears in the inspect sample."""
    deadline = time.monotonic() + config.timeout_seconds
    while time.monotonic() < deadline:
        rows = _rows(api.get("/api/ingestion/inspect?container=source-documents&limit=200"))
        if not any(row.get("documentId") == document_id for row in rows):
            reporter.pass_(f"Observed {description}")
            return
        reporter.wait(f"Waiting for {description}...")
        time.sleep(config.poll_seconds)
    raise DemoError(f"Timed out after {config.timeout_seconds} seconds waiting for {description}.")


def _show_delta_and_audit(
    api: FunctionApi, config: DemoConfig, reporter: DemoReporter, document_id: str
) -> None:
    ingestion_runs = _rows(
        api.get("/api/ingestion/inspect?container=ingestion-runs&limit=200")
    )
    trigger = next((row for row in ingestion_runs if row.get("id") == "delta-sync-trigger"), None)
    instance_id = trigger.get("currentInstanceId") if trigger is not None else None
    if not isinstance(instance_id, str) or not instance_id:
        raise DemoError("The delta-sync trigger record has no current orchestration instance ID.")

    delta = api.get_optional(f"/api/ingestion/status?instanceId={instance_id}")
    if delta is None:
        raise DemoError(f"The current delta-sync orchestration {instance_id} was not found.")
    runtime_status = str(delta.get("runtimeStatus", ""))
    output = delta.get("output") if isinstance(delta.get("output"), dict) else {}
    if not runtime_status.endswith("Completed"):
        raise DemoError(
            f"The current delta-sync orchestration {instance_id} is not completed: "
            f"{runtime_status or 'unknown'}"
        )
    if output.get("failed") != 0:
        raise DemoError(
            f"The current delta-sync orchestration {instance_id} did not report failed=0."
        )
    reporter.info(
        f"Delta status: {runtime_status} | instanceId={instance_id} "
        f"createdOrUpdated={output.get('createdOrUpdated')} "
        f"deleted={output.get('deleted')} aclResynced={output.get('aclResynced')} failed={output.get('failed')}"
    )

    audit = _rows(api.get("/api/ingestion/inspect?container=service-audit&limit=200"))
    relevant = [
        row for row in audit
        if row.get("documentId") == document_id
        or row.get("sourceName") == config.file_name
        or row.get("operation") == "webhook_received"
    ]
    if relevant:
        operations = ", ".join(str(row.get("operation")) for row in relevant[:5])
        reporter.info(f"Supporting audit records found: {operations}")
    else:
        reporter.warn("No matching audit record in the capped service-audit sample; document state remains authoritative.")


def _run_retrieval(
    api: FunctionApi,
    config: DemoConfig,
    reporter: DemoReporter,
    report: dict[str, Any],
    stage: str,
    *,
    target_must_be_cited: bool,
) -> None:
    reporter.heading(f"RETRIEVAL {stage.replace('_', ' ').upper()}")
    response = api.post(
        "/api/query",
        {"question": config.question, "mode": "hybrid", "top_k": 5},
        timeout_seconds=config.query_timeout_seconds,
    )
    citations = response.get("citations")
    answer = response.get("answer")
    if not isinstance(citations, list) or not isinstance(answer, str):
        raise DemoError("Retrieval response is missing answer or citations.")
    target_cited = any(
        isinstance(citation, dict) and citation.get("source_name") == config.file_name
        for citation in citations
    )
    print(f"Request ID: {response.get('request_id', '<unknown>')}")
    print("Citations:")
    if not citations:
        print("  <none>")
    for citation in citations:
        if isinstance(citation, dict):
            print(f"  {citation.get('ref', '?')} {citation.get('source_name', '<unknown>')}")
    print("\nAnswer:\n")
    print(answer)
    report["stages"][stage] = {
        "requestId": response.get("request_id"),
        "citations": citations,
        "targetCited": target_cited,
        "answer": answer,
    }
    if target_cited == target_must_be_cited:
        expectation = "is cited" if target_must_be_cited else "is not cited"
        reporter.pass_(f"Target file {expectation} as expected")
        return
    expectation = "to be cited" if target_must_be_cited else "not to be cited"
    raise DemoError(f"Expected {config.file_name} {expectation}, but retrieval returned a different result.")


def _show_document(title: str, document: DocumentSnapshot) -> None:
    print(f"\n{title}:")
    print(f"  documentId: {document.document_id}")
    print(f"  status: {document.status}")
    print(f"  chunks: {document.expected_chunks} expected / {document.written_chunks} written")
    print(f"  allowedGroupIds: {', '.join(document.allowed_group_ids)}")
    print(f"  aclHash: {document.acl_hash}")
    print(f"  retiredReason: {document.retired_reason}")
    print(f"  error: {document.error}")


def _show_acl_change(
    title: str, before: DocumentSnapshot, after: DocumentSnapshot, editors_group_id: str
) -> None:
    print(f"\n{title}:")
    print(f"  status: {after.status}")
    print(f"  aclHash changed: {before.acl_hash != after.acl_hash}")
    print(f"  SharePoint Editors present before: {editors_group_id in before.allowed_group_ids}")
    print(f"  SharePoint Editors present after: {editors_group_id in after.allowed_group_ids}")
    print(f"  before: {', '.join(before.allowed_group_ids)}")
    print(f"  after:  {', '.join(after.allowed_group_ids)}")


def _pause(instruction: str) -> None:
    print(f"\nACTION REQUIRED: {instruction}")
    input("Press Enter after the SharePoint action is complete... ")


def _print_recovery_guidance() -> None:
    print(
        "\nRecovery guidance:\n"
        "1. Confirm the SharePoint action was saved.\n"
        "2. Confirm full-sync is not running.\n"
        "3. Check the source-document record and delta status in DEMO_RUNBOOK.md.\n"
        "4. Do not start full-sync or manually purge data during a webhook-only demonstration."
    )


def _write_report(report: dict[str, Any], reporter: DemoReporter) -> None:
    output_dir = Path("demo-output")
    output_dir.mkdir(exist_ok=True)
    filename = output_dir / f"webhook-acl-demo-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    filename.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    reporter.info(f"Local evidence report written: {filename}")


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise DemoError("Inspect API response is missing rows.")
    return rows


def _find_ready_target(rows: list[dict[str, Any]], file_name: str) -> DocumentSnapshot | None:
    matches = [
        DocumentSnapshot.from_row(row)
        for row in rows
        if row.get("sourceName") == file_name and row.get("status") == "ready"
    ]
    return max(matches, key=lambda document: document.updated_at or "", default=None)


def _as_optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _as_optional_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())