"""Contract test for the retrieval ACR build context allowlist.

The retrieval image is built with ``az acr build ... --file retrieval/Dockerfile .``
from the ``app/`` directory. Only ``app/.dockerignore`` governs what actually
uploads to the remote builder. This test ensures the allowlist excludes local
settings, ingestion source, secrets, protected data, tests, caches, Kubernetes
manifests, and documentation while including the retrieval Python modules,
reviewed catalog, requirements, and Dockerfile.
"""

from __future__ import annotations

import fnmatch
import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
DOCKERIGNORE = APP_ROOT / ".dockerignore"


def _read_rules() -> list[str]:
    lines = DOCKERIGNORE.read_text("utf-8").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.startswith("#")]


def _pattern_regex(pattern: str) -> re.Pattern[str]:
    """Compile a subset of gitignore/dockerignore semantics into a regex."""
    dir_only = pattern.endswith("/")
    body = pattern.rstrip("/")
    # Escape then rewrite the glob metacharacters we support.
    regex = re.escape(body)
    regex = regex.replace(r"\*\*", ".*")
    regex = regex.replace(r"\*", "[^/]*")
    regex = regex.replace(r"\?", "[^/]")
    anchor = "" if body.startswith("**") else "^"
    trailing = "(/.*)?$" if dir_only else "(/.*)?$"
    return re.compile(anchor + regex + trailing)


def _is_uploaded(path: str, rules: list[str]) -> bool:
    """Return True if the relative path would upload to ACR under the rules."""
    included = True
    for rule in rules:
        negation = rule.startswith("!")
        pattern = rule[1:] if negation else rule
        if _pattern_regex(pattern).match(path):
            included = negation
    return included


def _walk_app(rules: list[str]) -> list[str]:
    uploaded: list[str] = []
    for path in APP_ROOT.rglob("*"):
        if path.is_dir():
            continue
        rel = path.relative_to(APP_ROOT).as_posix()
        if _is_uploaded(rel, rules):
            uploaded.append(rel)
    return sorted(uploaded)


def test_dockerignore_starts_with_deny_all() -> None:
    rules = _read_rules()
    assert rules, "app/.dockerignore must define at least one rule"
    assert rules[0] == "*", "deny-all baseline must be the first rule"


def test_dockerignore_reincludes_only_expected_retrieval_inputs() -> None:
    rules = _read_rules()
    required_allow = {
        "!retrieval/",
        "!retrieval/*.py",
        "!retrieval/catalog.example.json",
        "!retrieval/requirements.txt",
        "!retrieval/Dockerfile",
        "!source_registry.py",
        "!cosmos_connection.py",
    }
    assert required_allow.issubset(set(rules))


def test_dockerignore_never_re_includes_forbidden_paths() -> None:
    rules = _read_rules()
    forbidden_needles = [
        "local.settings.json",
        "function_app.py",
        "host.json",
        "ingestion/",
        "config.py",
    ]
    for needle in forbidden_needles:
        for rule in rules:
            if rule.startswith("!"):
                assert needle not in rule, (
                    f"forbidden path '{needle}' is re-included by rule '{rule}'"
                )


@pytest.mark.parametrize("forbidden", [
    "local.settings.json",
    "function_app.py",
    "host.json",
    "config.py",
    "ingestion/services.py",
    "ingestion/repository.py",
    "ingestion/lifecycle_repository.py",
    "retrieval/kubernetes/deployment.yaml",
    "retrieval/README.md",
])
def test_forbidden_files_are_excluded(forbidden: str) -> None:
    rules = _read_rules()
    assert not _is_uploaded(forbidden, rules), (
        f"'{forbidden}' must not enter the ACR build context"
    )


@pytest.mark.parametrize("required", [
    "retrieval/main.py",
    "retrieval/service.py",
    "retrieval/cosmos.py",
    "retrieval/scoring.py",
    "retrieval/config_loader.py",
    "retrieval/catalog.example.json",
    "retrieval/requirements.txt",
    "retrieval/Dockerfile",
    "source_registry.py",
    "cosmos_connection.py",
])
def test_required_files_are_included(required: str) -> None:
    rules = _read_rules()
    assert _is_uploaded(required, rules), (
        f"'{required}' is required by the retrieval image and must upload"
    )


def test_walked_app_context_only_contains_retrieval_image_inputs() -> None:
    rules = _read_rules()
    uploaded = _walk_app(rules)
    # Only the two shared runtime modules may enter alongside retrieval/.
    non_retrieval = [path for path in uploaded if not path.startswith("retrieval/")]
    assert non_retrieval == ["cosmos_connection.py", "source_registry.py"], (
        f"unexpected root files entered the build context: {non_retrieval}"
    )
    # The Dockerfile must be present so 'az acr build --file retrieval/Dockerfile' works.
    assert "retrieval/Dockerfile" in uploaded
    # No compiled or cached artifacts survive the allowlist.
    for path in uploaded:
        assert "__pycache__" not in path
        assert not fnmatch.fnmatch(path, "*.pyc")
        assert not fnmatch.fnmatch(path, "*.pyo")
