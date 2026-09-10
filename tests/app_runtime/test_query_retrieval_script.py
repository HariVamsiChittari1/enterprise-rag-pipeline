from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = PROJECT_ROOT / "tools" / "script_query_retrieval.py"
SPEC = importlib.util.spec_from_file_location("script_query_retrieval", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
query_script = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = query_script
SPEC.loader.exec_module(query_script)


def test_matrix_audit_accepts_matching_non_degraded_request() -> None:
    failures = query_script._validate_matrix_audit(
        {
            "path": "agentic",
            "mode": "vector",
            "effective_retrieval_modes": ["vector"],
            "retrieval_degraded": False,
            "citations_count": 1,
        },
        expected_path="agentic",
        expected_mode="vector",
        expected_profile=None,
        expected_catalog_sha=None,
    )

    assert failures == []


def test_matrix_audit_rejects_fallback_mode_drift_and_degradation() -> None:
    failures = query_script._validate_matrix_audit(
        {
            "path": "agentic_fallback",
            "mode": "hybrid",
            "effective_retrieval_modes": ["hybrid"],
            "retrieval_degraded": True,
            "citations_count": 1,
        },
        expected_path="agentic",
        expected_mode="full_text",
        expected_profile=None,
        expected_catalog_sha=None,
    )

    assert failures == [
        "expected path=agentic, observed 'agentic_fallback'",
        "expected mode=full_text, observed 'hybrid'",
        "expected effective retrieval modes=['full_text'], observed ['hybrid']",
        "expected retrieval_degraded=false, observed True",
    ]


def test_matrix_audit_rejects_scoring_profile_and_catalog_mismatch() -> None:
    failures = query_script._validate_matrix_audit(
        {
            "path": "standard",
            "mode": "hybrid",
            "effective_retrieval_modes": ["hybrid"],
            "retrieval_degraded": False,
            "citations_count": 1,
            "scoring_profile": "unexpected",
            "catalog_version": "sha256:bad",
        },
        expected_path="standard",
        expected_mode="hybrid",
        expected_profile="fresh",
        expected_catalog_sha="sha256:good",
    )
    assert any("scoring_profile" in message for message in failures)
    assert any("catalog_version" in message for message in failures)


@pytest.mark.parametrize("observed_etag", [None, '"later"', '"expected"'])
def test_matrix_audit_checks_generation_even_when_digest_matches(observed_etag) -> None:
    failures = query_script._validate_matrix_audit(
        {
            "path": "standard",
            "mode": "hybrid",
            "effective_retrieval_modes": ["hybrid"],
            "retrieval_degraded": False,
            "citations_count": 1,
            "catalog_version": "sha256:catalog",
            "catalog_etag": observed_etag,
        },
        expected_path="standard",
        expected_mode="hybrid",
        expected_profile=None,
        expected_catalog_sha="sha256:catalog",
        expected_catalog_etag='"expected"',
    )

    assert bool(failures) is (observed_etag != '"expected"')
    assert all("catalog_etag" in message for message in failures)


def test_matrix_audit_rejects_missing_citations() -> None:
    failures = query_script._validate_matrix_audit(
        {
            "path": "standard",
            "mode": "full_text",
            "effective_retrieval_modes": ["full_text"],
            "retrieval_degraded": False,
            "citations_count": 0,
        },
        expected_path="standard",
        expected_mode="full_text",
        expected_profile=None,
        expected_catalog_sha=None,
    )

    assert failures == ["expected citations_count>=1, observed 0"]


def test_matrix_runs_all_six_path_mode_combinations(monkeypatch) -> None:
    requests = []

    def fake_post(url, *, token, payload, timeout):
        requests.append(payload)
        return {"request_id": f"request-{len(requests)}"}

    def fake_audit(base_url, *, token, request_id, timeout, poll_seconds):
        request = requests[int(request_id.removeprefix("request-")) - 1]
        expected_path = "standard" if request["question"] == "simple" else "agentic"
        return {
            "path": expected_path,
            "mode": request["mode"],
            "effective_retrieval_modes": [request["mode"]],
            "retrieval_degraded": False,
            "citations_count": 1,
            "scoring_profile": request.get("scoring_profile"),
            "catalog_version": "sha256:catalog",
            "catalog_etag": '"generation"',
        }

    monkeypatch.setattr(query_script, "_post_json", fake_post)
    monkeypatch.setattr(query_script, "_wait_for_query_audit", fake_audit)

    results = query_script._run_matrix(
        base_url="https://func.azurewebsites.net",
        token="not-recorded",
        standard_question="simple",
        agentic_question="compare",
        top_k=5,
        request_timeout=30,
        audit_timeout=30,
        poll_seconds=1,
        scoring_profile="fresh",
        expected_scoring_profile="fresh",
        expected_catalog_sha="sha256:catalog",
        expected_catalog_etag='"generation"',
    )

    assert len(results) == 6
    assert all(result["status"] == "passed" for result in results)
    assert all(result["catalogEtag"] == '"generation"' for result in results)
    assert {(result["expectedPath"], result["mode"]) for result in results} == {
        (path, mode)
        for path in ("standard", "agentic")
        for mode in ("hybrid", "vector", "full_text")
    }
    # Scenario records must not carry question text.
    assert all("question" not in result for result in results)
    # Every request forwarded the selected scoring profile.
    assert all(request.get("scoring_profile") == "fresh" for request in requests)


def test_single_query_forwards_scoring_profile(monkeypatch) -> None:
    captured = {}

    monkeypatch.setattr(query_script, "_get_access_token", lambda client_id: "token")

    def fake_post(url, *, token, payload, timeout):
        captured.update(payload)
        return {"answer": "answer", "citations": []}

    monkeypatch.setattr(query_script, "_post_json", fake_post)
    monkeypatch.setattr(
        query_script,
        "_print_summary",
        lambda response, request_payload, *, include_json: None,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "--function-app",
            "func",
            "--client-id",
            "client",
            "--scoring-profile",
            "fresh",
        ],
    )

    assert query_script.main() == 0
    assert captured["scoring_profile"] == "fresh"


def test_single_query_summary_shows_question_parameters_answer_and_citations(capsys) -> None:
    query_script._print_summary(
        {
            "answer": "Use multifactor authentication [S1].",
            "citations": [
                {
                    "ref": "[S1]",
                    "source_name": "policy.pdf",
                    "url": "https://example.invalid/policy.pdf#page=3",
                }
            ],
            "request_id": "not-shown-without-json",
        },
        {
            "question": "What authentication is required?",
            "mode": "hybrid",
            "top_k": 5,
            "scoring_profile": "hr-relevance",
        },
        include_json=False,
    )

    output = capsys.readouterr().out
    assert "QUESTION:\nWhat authentication is required?" in output
    assert "mode: hybrid" in output
    assert "top_k: 5" in output
    assert "scoring_profile: hr-relevance" in output
    assert "ANSWER:\nUse multifactor authentication [S1]." in output
    assert "[S1] | policy.pdf | https://example.invalid/policy.pdf#page=3" in output
    assert "not-shown-without-json" not in output


def test_question_uses_explicit_argument(monkeypatch) -> None:
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt: (_ for _ in ()).throw(AssertionError(prompt)))

    assert query_script._resolve_question("What is the policy?") == "What is the policy?"


def test_question_prompts_interactive_user(monkeypatch) -> None:
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt: "What controls are required? ")

    assert query_script._resolve_question(None) == "What controls are required?"


def test_blank_interactive_question_uses_verified_default(monkeypatch) -> None:
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt: "  ")

    assert query_script._resolve_question(None) == query_script.DEFAULT_QUESTION


def test_noninteractive_question_uses_verified_default(monkeypatch) -> None:
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr("builtins.input", lambda prompt: (_ for _ in ()).throw(AssertionError(prompt)))

    assert query_script._resolve_question(None) == query_script.DEFAULT_QUESTION