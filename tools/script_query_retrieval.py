#!/usr/bin/env python3
"""Query the live Function App endpoint that proxies to retrieval."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_QUESTION = (
    "What authentication topics are covered by the Password and Authentication Policy?"
)
DEFAULT_AGENTIC_QUESTION = (
    "Compare the password requirements from the identity policy with the access "
    "control requirements from the information security policy."
)
# Mirrors retrieval.service.NO_EVIDENCE_ANSWER. A grounded refusal (zero citations with
# this exact answer) is a valid RAG outcome, so the matrix accepts it instead of
# asserting recall: https://learn.microsoft.com/en-us/azure/ai-foundry/concepts/evaluation-evaluators/rag-evaluators
NO_EVIDENCE_ANSWER = "I could not find authorized evidence for this question."


class ScriptError(RuntimeError):
    """Expected invocation or API failure for the live query script."""


def _resolve_question(question: str | None) -> str:
    if question is not None:
        return question
    if sys.stdin.isatty():
        entered = input(f"Question [{DEFAULT_QUESTION}]: ").strip()
        if entered:
            return entered
    return DEFAULT_QUESTION


def _get_access_token(client_id: str) -> str:
    azure_cli = "az.cmd" if os.name == "nt" else "az"
    try:
        result = subprocess.run(
            [
                azure_cli,
                "account",
                "get-access-token",
                "--resource",
                f"api://{client_id}",
                "--query",
                "accessToken",
                "-o",
                "tsv",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as error:
        raise ScriptError("Azure CLI is not installed or is not on PATH.") from error
    except subprocess.CalledProcessError as error:
        details = error.stderr.strip() or error.stdout.strip()
        raise ScriptError(f"Could not obtain an Azure CLI access token. Run 'az login'. {details}") from error

    token = result.stdout.strip()
    if not token:
        raise ScriptError("Azure CLI returned an empty access token. Run 'az login'.")
    return token


def _post_json(url: str, *, token: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, headers=headers, method="POST")

    try:
        with urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise ScriptError(f"POST {url} returned HTTP {error.code}: {details}") from error
    except URLError as error:
        raise ScriptError(f"POST {url} could not reach the Function App: {error.reason}") from error
    except TimeoutError as error:
        raise ScriptError(f"POST {url} timed out after {timeout} seconds.") from error
    except json.JSONDecodeError as error:
        raise ScriptError(f"POST {url} returned invalid JSON.") from error

    if not isinstance(data, dict):
        raise ScriptError(f"POST {url} returned an unexpected JSON shape.")
    return data


def _get_json(url: str, *, token: str, timeout: float) -> dict[str, Any]:
    request = Request(url, headers={"Authorization": f"Bearer {token}"}, method="GET")
    try:
        with urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise ScriptError(f"GET {url} returned HTTP {error.code}: {details}") from error
    except URLError as error:
        raise ScriptError(f"GET {url} could not reach the Function App: {error.reason}") from error
    except TimeoutError as error:
        raise ScriptError(f"GET {url} timed out after {timeout} seconds.") from error
    except json.JSONDecodeError as error:
        raise ScriptError(f"GET {url} returned invalid JSON.") from error
    if not isinstance(data, dict):
        raise ScriptError(f"GET {url} returned an unexpected JSON shape.")
    return data


def _wait_for_query_audit(
    base_url: str,
    *,
    token: str,
    request_id: str,
    timeout: float,
    poll_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    url = f"{base_url}/api/ingestion/inspect?container=service-audit&limit=200"
    while time.monotonic() < deadline:
        payload = _get_json(url, token=token, timeout=min(timeout, 30.0))
        rows = payload.get("rows")
        if not isinstance(rows, list):
            raise ScriptError("The service-audit inspect response has no rows array.")
        match = next(
            (
                row
                for row in rows
                if isinstance(row, dict)
                and row.get("requestId") == request_id
                and row.get("operation") == "query_request"
            ),
            None,
        )
        effective_modes = sorted({
            row.get("retrieval_mode")
            for row in rows
            if isinstance(row, dict)
            and row.get("requestId") == request_id
            and row.get("operation") in {"retrieval_batch", "tool_invocation"}
            and isinstance(row.get("retrieval_mode"), str)
        })
        if match is not None and effective_modes:
            return {**match, "effective_retrieval_modes": effective_modes}
        time.sleep(poll_seconds)
    raise ScriptError(
        f"No query_request audit record for request_id={request_id} appeared within {timeout} seconds."
    )


def _validate_matrix_audit(
    audit: dict[str, Any],
    *,
    expected_path: str,
    expected_mode: str,
    expected_profile: str | None,
    expected_catalog_sha: str | None,
    expected_catalog_etag: str | None = None,
    grounded_refusal: bool = False,
) -> list[str]:
    failures = []
    if audit.get("path") != expected_path:
        failures.append(f"expected path={expected_path}, observed {audit.get('path')!r}")
    if audit.get("mode") != expected_mode:
        failures.append(f"expected mode={expected_mode}, observed {audit.get('mode')!r}")
    effective_modes = audit.get("effective_retrieval_modes")
    if effective_modes != [expected_mode]:
        failures.append(
            f"expected effective retrieval modes={[expected_mode]!r}, observed {effective_modes!r}"
        )
    if audit.get("retrieval_degraded") is not False:
        failures.append(
            "expected retrieval_degraded=false, observed "
            f"{audit.get('retrieval_degraded')!r}"
        )
    citations_count = audit.get("citations_count")
    citations_missing = (
        not isinstance(citations_count, int)
        or isinstance(citations_count, bool)
        or citations_count < 1
    )
    # A grounded refusal legitimately returns zero citations; accept it rather than
    # asserting retrieval recall, which belongs in a separate retrieval-quality eval.
    if citations_missing and not grounded_refusal:
        failures.append(
            f"expected citations_count>=1, observed {citations_count!r}"
        )
    if expected_profile is not None and audit.get("scoring_profile") != expected_profile:
        failures.append(
            f"expected scoring_profile={expected_profile!r}, observed "
            f"{audit.get('scoring_profile')!r}"
        )
    if expected_catalog_sha is not None and audit.get("catalog_version") != expected_catalog_sha:
        failures.append(
            f"expected catalog_version={expected_catalog_sha!r}, observed "
            f"{audit.get('catalog_version')!r}"
        )
    if expected_catalog_etag is not None and audit.get("catalog_etag") != expected_catalog_etag:
        failures.append(
            f"expected catalog_etag={expected_catalog_etag!r}, observed "
            f"{audit.get('catalog_etag')!r}"
        )
    return failures


def _run_matrix(
    *,
    base_url: str,
    token: str,
    standard_question: str,
    agentic_question: str,
    top_k: int,
    request_timeout: float,
    audit_timeout: float,
    poll_seconds: float,
    scoring_profile: str | None,
    expected_scoring_profile: str | None,
    expected_catalog_sha: str | None,
    expected_catalog_etag: str | None = None,
) -> list[dict[str, Any]]:
    results = []
    for expected_path, question in (
        ("standard", standard_question),
        ("agentic", agentic_question),
    ):
        for mode in ("hybrid", "vector", "full_text"):
            # Scenario report is written to disk and must not carry question text
            # or answer previews. Identity fields (path, mode, profile) are safe.
            scenario: dict[str, Any] = {"expectedPath": expected_path, "mode": mode}
            try:
                payload: dict[str, Any] = {
                    "question": question, "mode": mode, "top_k": top_k,
                }
                if scoring_profile is not None:
                    payload["scoring_profile"] = scoring_profile
                response = _post_json(
                    f"{base_url}/api/query",
                    token=token,
                    payload=payload,
                    timeout=request_timeout,
                )
                request_id = response.get("request_id")
                if not isinstance(request_id, str) or not request_id:
                    raise ScriptError("Query response has no request_id.")
                # Detected from the live response only; never persisted to the report.
                answer = response.get("answer")
                grounded_refusal = (
                    isinstance(answer, str) and answer.strip() == NO_EVIDENCE_ANSWER
                )
                audit = _wait_for_query_audit(
                    base_url,
                    token=token,
                    request_id=request_id,
                    timeout=audit_timeout,
                    poll_seconds=poll_seconds,
                )
                failures = _validate_matrix_audit(
                    audit,
                    expected_path=expected_path,
                    expected_mode=mode,
                    expected_profile=expected_scoring_profile,
                    expected_catalog_sha=expected_catalog_sha,
                    expected_catalog_etag=expected_catalog_etag,
                    grounded_refusal=grounded_refusal,
                )
                scenario.update({
                    "requestId": request_id,
                    "actualPath": audit.get("path"),
                    "actualMode": audit.get("mode"),
                    "effectiveRetrievalModes": audit.get("effective_retrieval_modes"),
                    "retrievalDegraded": audit.get("retrieval_degraded"),
                    "citationsCount": audit.get("citations_count"),
                    "groundedRefusal": grounded_refusal,
                    "catalogVersion": audit.get("catalog_version"),
                    "catalogEtag": audit.get("catalog_etag"),
                    "scoringProfile": audit.get("scoring_profile"),
                    "status": "passed" if not failures else "failed",
                    "failures": failures,
                })
            except ScriptError as error:
                scenario.update({"status": "failed", "failures": [str(error)]})
            results.append(scenario)
    return results


def _write_matrix_report(path: Path, function_app: str, results: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "functionApp": function_app,
        "results": results,
    }
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def _print_summary(
    response: dict[str, Any],
    request_payload: dict[str, Any],
    *,
    include_json: bool,
) -> None:
    if include_json:
        print(json.dumps(response, indent=2, ensure_ascii=False))
        print()

    print(f"QUESTION:\n{request_payload['question']}\n")
    print("PARAMETERS:")
    for name in ("mode", "top_k", "scoring_profile"):
        print(f"  {name}: {request_payload.get(name)}")

    answer = response.get("answer")
    citations = response.get("citations") or []
    if isinstance(answer, str):
        print(f"\nANSWER:\n{answer}\n")
    else:
        print("\nANSWER: <missing>")

    if citations:
        print("CITATIONS:")
        for index, citation in enumerate(citations, start=1):
            if isinstance(citation, dict):
                source = citation.get("source_name") or citation.get("sourceName") or "<unknown>"
                ref = citation.get("ref") or "<unknown>"
                url = citation.get("url") or "<unknown>"
                print(f"  {index}. {ref} | {source} | {url}")
    else:
        print("CITATIONS: <none>")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--function-app", required=True, help="Deployed Azure Function App name")
    parser.add_argument("--client-id", required=True, help="Admin API application client ID")
    parser.add_argument(
        "--question",
        help="RAG question; omit to prompt interactively, or use the verified default in automation",
    )
    parser.add_argument("--mode", choices=("hybrid", "vector", "full_text"), default="hybrid")
    parser.add_argument("--top-k", type=int, default=5, help="Top document count")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds")
    parser.add_argument(
        "--scenario-matrix",
        action="store_true",
        help="Run standard and agentic scenarios across all three retrieval modes",
    )
    parser.add_argument(
        "--agentic-question",
        default=DEFAULT_AGENTIC_QUESTION,
        help="Multi-part question expected to select the agentic path",
    )
    parser.add_argument("--audit-timeout", type=float, default=30.0)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument(
        "--scoring-profile",
        default=None,
        help="Scoring profile name to send with one query or every matrix request.",
    )
    parser.add_argument(
        "--expected-scoring-profile",
        default=None,
        help="Assert every audited request selected this scoring_profile.",
    )
    parser.add_argument(
        "--expected-catalog-sha",
        default=None,
        help="Assert every audited request used this catalog SHA.",
    )
    parser.add_argument(
        "--expected-catalog-etag",
        default=None,
        help="Assert every audited request used this exact Cosmos catalog ETag.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("demo-output/retrieval-scenario-matrix.json"),
        help="Token-free JSON report written by --scenario-matrix",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the raw JSON response before the single-query demo summary.",
    )
    args = parser.parse_args()

    if args.top_k < 1:
        parser.error("--top-k must be >= 1")
    if args.timeout <= 0 or args.audit_timeout <= 0 or args.poll_seconds <= 0:
        parser.error("timeouts and poll-seconds must be > 0")

    question = _resolve_question(args.question)
    base_url = f"https://{args.function_app}.azurewebsites.net"
    token = _get_access_token(args.client_id)
    if args.scenario_matrix:
        results = _run_matrix(
            base_url=base_url,
            token=token,
            standard_question=question,
            agentic_question=args.agentic_question,
            top_k=args.top_k,
            request_timeout=args.timeout,
            audit_timeout=args.audit_timeout,
            poll_seconds=args.poll_seconds,
            scoring_profile=args.scoring_profile,
            expected_scoring_profile=args.expected_scoring_profile,
            expected_catalog_sha=args.expected_catalog_sha,
            expected_catalog_etag=args.expected_catalog_etag,
        )
        _write_matrix_report(args.report, args.function_app, results)
        print(json.dumps(results, indent=2, ensure_ascii=False))
        failed = sum(result["status"] != "passed" for result in results)
        refusals = sum(1 for result in results if result.get("groundedRefusal"))
        note = f" ({refusals} grounded refusal)" if refusals else ""
        print(f"\nScenario matrix: {len(results) - failed} passed, {failed} failed{note}")
        print(f"Report: {args.report}")
        return 1 if failed else 0

    payload = {
        "question": question,
        "mode": args.mode,
        "top_k": args.top_k,
    }
    if args.scoring_profile is not None:
        payload["scoring_profile"] = args.scoring_profile

    try:
        response = _post_json(
            f"{base_url}/api/query", token=token, payload=payload, timeout=args.timeout,
        )
    except ScriptError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    _print_summary(response, payload, include_json=args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
