"""Tests for function_app._retire_prior_version (full-sync hard-delete path).

Monkeypatches only the two dependency-construction points _retire_prior_version
actually uses (_build_lifecycle_repository, ingestion.telemetry.write_audit_record).
Production code is never modified by these tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import function_app
import ingestion.telemetry as telemetry
from ingestion.lifecycle_repository import LifecycleConflictError, ReadyDocumentRef


class FakeConfig:
    def __init__(self, source_id: str = "source", business_name: str = "Contoso") -> None:
        self.source_id = source_id
        self.drive_id = "drive"
        self.business_name = business_name


class FakeLifecycleRepo:
    def __init__(
        self,
        ref: ReadyDocumentRef | None = None,
        raise_error: Exception | None = None,
        pages: list[tuple[tuple[ReadyDocumentRef, ...], str | None]] | None = None,
    ) -> None:
        self._ref = ref
        self._raise_error = raise_error
        self._pages = pages
        self.list_calls = 0
        self.page_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []

    def list_ready_document_versions(self, source_id: str, document_id: str) -> tuple[ReadyDocumentRef, ...]:
        self.list_calls += 1
        return (self._ref,) if self._ref is not None else ()

    def list_ready_document_versions_page(
        self,
        *,
        source_id: str,
        document_id: str,
        page_size: int,
        continuation_token: str | None = None,
    ) -> SimpleNamespace:
        self.page_calls.append({
            "source_id": source_id,
            "document_id": document_id,
            "page_size": page_size,
            "continuation_token": continuation_token,
        })
        if self._pages is None:
            return SimpleNamespace(
                items=(self._ref,) if self._ref is not None else (),
                continuation_token=None,
            )
        page_index = 0 if continuation_token is None else 1
        items, token = self._pages[page_index]
        return SimpleNamespace(items=items, continuation_token=token)

    def delete_document_and_chunks(
        self, *, source_run_id: str, document_id: str, document_key: str, etag: str
    ) -> None:
        if self._raise_error is not None:
            raise self._raise_error
        self.delete_calls.append({
            "source_run_id": source_run_id, "document_id": document_id,
            "document_key": document_key, "etag": etag,
        })


def _prior_ref(source_run_id: str = "source:run-old") -> ReadyDocumentRef:
    return ReadyDocumentRef(
        document_id="doc-1", source_run_id=source_run_id, document_key=f"{source_run_id}:doc-1",
        item_id="item-1", allowed_group_ids=("group-a",), acl_hash="hash-a", etag="etag-old",
        source_id="source", drive_id="drive",
    )


def _patch_repo(monkeypatch: pytest.MonkeyPatch, repo: FakeLifecycleRepo) -> None:
    monkeypatch.setattr(function_app, "_build_lifecycle_repository", lambda config: repo)


def _patch_audit(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    def _fake_write(
        container: Any,
        source_id: str,
        run_id: str,
        record: dict[str, Any],
        *,
        business_name: str | None = None,
    ) -> None:
        records.append({
            "container": container,
            "source_id": source_id,
            "run_id": run_id,
            "business_name": business_name,
            **record,
        })

    monkeypatch.setattr(telemetry, "write_audit_record", _fake_write)
    return records


def test_retire_prior_version_hard_deletes_and_audits_when_prior_ready_version_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ref = _prior_ref()
    repo = FakeLifecycleRepo(ref=ref)
    _patch_repo(monkeypatch, repo)
    records = _patch_audit(monkeypatch)

    function_app._retire_prior_version(FakeConfig(), "doc-1", "source:run-new", "sentinel-container")

    assert repo.delete_calls == [{
        "source_run_id": ref.source_run_id, "document_id": "doc-1",
        "document_key": ref.document_key, "etag": ref.etag,
    }]
    assert len(records) == 1
    assert records[0]["operation"] == "document_deleted"
    assert records[0]["reason"] == "superseded"
    assert records[0]["method"] == "full_sync"
    assert records[0]["replacedDocumentKey"] == ref.document_key
    assert records[0]["container"] == "sentinel-container"
    assert records[0]["run_id"] == "source:run-new"
    assert records[0]["business_name"] == "Contoso"


def test_retire_prior_version_is_a_no_op_when_no_prior_version_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = FakeLifecycleRepo(ref=None)
    _patch_repo(monkeypatch, repo)
    records = _patch_audit(monkeypatch)

    function_app._retire_prior_version(FakeConfig(), "doc-1", "source:run-new", "sentinel-container")

    assert repo.delete_calls == []
    assert records == []


def test_retire_prior_version_is_a_no_op_when_ref_is_the_current_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ref = _prior_ref(source_run_id="source:run-new")
    repo = FakeLifecycleRepo(ref=ref)
    _patch_repo(monkeypatch, repo)
    records = _patch_audit(monkeypatch)

    function_app._retire_prior_version(FakeConfig(), "doc-1", "source:run-new", "sentinel-container")

    assert repo.delete_calls == []
    assert records == []


def test_retire_prior_version_propagates_conflict_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ref = _prior_ref()
    repo = FakeLifecycleRepo(ref=ref, raise_error=LifecycleConflictError("already changed"))
    _patch_repo(monkeypatch, repo)
    records = _patch_audit(monkeypatch)

    with pytest.raises(LifecycleConflictError, match="already changed"):
        function_app._retire_prior_version(
            FakeConfig(), "doc-1", "source:run-new", "sentinel-container",
        )

    assert records == []  # delete failed before the audit write line was reached


def test_retire_prior_version_propagates_unexpected_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ref = _prior_ref()
    repo = FakeLifecycleRepo(ref=ref, raise_error=RuntimeError("boom"))
    _patch_repo(monkeypatch, repo)
    _patch_audit(monkeypatch)

    with pytest.raises(RuntimeError, match="boom"):
        function_app._retire_prior_version(
            FakeConfig(), "doc-1", "source:run-new", "sentinel-container",
        )


def test_retire_prior_version_omits_audit_when_container_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ref = _prior_ref()
    repo = FakeLifecycleRepo(ref=ref)
    _patch_repo(monkeypatch, repo)
    records = _patch_audit(monkeypatch)

    function_app._retire_prior_version(FakeConfig(), "doc-1", "source:run-new")

    assert len(repo.delete_calls) == 1
    assert records == []


def test_retire_prior_version_rejects_a_different_drive_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ref = _prior_ref()
    ref = ReadyDocumentRef(
        **{**ref.__dict__, "drive_id": "other-drive"},
    )
    repo = FakeLifecycleRepo(ref=ref)
    _patch_repo(monkeypatch, repo)

    with pytest.raises(RuntimeError, match="superseded_document_identity_mismatch"):
        function_app._retire_prior_version(FakeConfig(), "doc-1", "source:run-new")

    assert repo.delete_calls == []


def test_retire_prior_version_streams_all_pages_without_materializing_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _prior_ref("source:run-old-1")
    current = _prior_ref("source:run-new")
    second = _prior_ref("source:run-old-2")
    repo = FakeLifecycleRepo(pages=[((first,), "page-2"), ((current, second), None)])
    _patch_repo(monkeypatch, repo)
    _patch_audit(monkeypatch)

    function_app._retire_prior_version(FakeConfig(), "doc-1", "source:run-new")

    assert repo.list_calls == 0
    assert [call["source_run_id"] for call in repo.delete_calls] == [
        "source:run-old-1", "source:run-old-2",
    ]
    assert [call["continuation_token"] for call in repo.page_calls] == [None, "page-2"]
    assert all(call["page_size"] == 50 for call in repo.page_calls)
