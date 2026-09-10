from copy import deepcopy
import hashlib
from types import SimpleNamespace

import pytest

from tools.publish_retrieval_catalog import _cohort_converged
from tools import publish_retrieval_catalog as publisher


def _evidence():
    member = {
        "revision": "revision-a", "replica": "replica-a", "container": "retrieval",
        "restart_count": 0, "process_incarnation": "process-a",
    }
    before = {"observedAt": "2026-09-08T00:00:00Z", "members": [member]}
    after = {"observedAt": "2026-09-08T00:00:30Z", "members": [deepcopy(member)]}
    record = {
        "event": "catalog_observed", "revision": "revision-a", "replica": "replica-a",
        "process_incarnation": "process-a", "etag_hash": hashlib.sha256(b"etag-a").hexdigest(),
        "digest": "sha256:" + "a" * 64, "accepted_at": "2026-09-07T00:00:00Z",
        "observed_at": "2026-09-08T00:00:15Z",
    }
    return before, after, [record]


def test_unchanged_cohort_brackets_target_observations():
    before, after, records = _evidence()
    assert _cohort_converged(before, after, records, "etag-a", records[0]["digest"])


@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_same_timestamp_health_cannot_converge(reverse):
    before, after, records = _evidence()
    digest = records[0]["digest"]
    records.append({**records[0], "event": "catalog_degraded"})
    if reverse:
        records.reverse()
    assert not _cohort_converged(before, after, records, "etag-a", digest)


def test_active_revision_without_replicas_cannot_be_omitted(monkeypatch):
    def azure(arguments, deadline):
        if arguments[:3] == ["containerapp", "revision", "list"]:
            return [{"name": name, "properties": {"active": True}} for name in ("revision-a", "revision-b")]
        if arguments[-1] == "revision-b":
            return []
        return [{"name": "replica-a", "properties": {"containers": [{
            "name": "retrieval-agent", "containerId": "container-a", "ready": True, "restartCount": 0,
        }]}}]

    monkeypatch.setattr(publisher, "_azure_json", azure)
    args = SimpleNamespace(subscription="sub", resource_group="group", application="app")
    with pytest.raises(publisher.CatalogError, match="Active revision"):
        publisher._replica_inventory(args, 100)


@pytest.mark.parametrize("field,value", [
    ("revision", "revision-b"), ("replica", "replica-b"), ("container", "other"),
    ("restart_count", 1), ("process_incarnation", "process-b"),
])
def test_member_changes_reset_stabilization(field, value):
    before, after, records = _evidence()
    after["members"][0][field] = value
    assert not _cohort_converged(before, after, records, "etag-a", records[0]["digest"])


@pytest.mark.parametrize("failure", [
    "missing", "old", "future", "process", "digest", "etag", "rejection", "short",
    "empty", "scaled", "naive", "duplicate", "missing-field", "postdated-acceptance",
])
def test_incomplete_or_mismatched_evidence_cannot_converge(failure):
    before, after, records = _evidence()
    digest = records[0]["digest"]
    if failure == "missing":
        records = []
    elif failure == "old":
        records[0]["observed_at"] = "2026-09-07T23:59:59Z"
    elif failure == "future":
        records[0]["observed_at"] = "2026-09-08T00:00:31Z"
    elif failure == "process":
        records[0]["process_incarnation"] = "process-old"
    elif failure in ("digest", "etag"):
        records[0]["digest" if failure == "digest" else "etag_hash"] = "other"
    elif failure == "rejection":
        records.append({**records[0], "event": "catalog_rejected", "observed_at": "2026-09-08T00:00:20Z"})
    elif failure == "short":
        after["observedAt"] = "2026-09-08T00:00:29Z"
    elif failure == "empty":
        before["members"] = after["members"] = []
    elif failure == "scaled":
        after["members"].append({**after["members"][0], "replica": "replica-b"})
    elif failure == "naive":
        records[0]["observed_at"] = "2026-09-08T00:00:15"
    elif failure == "duplicate":
        before["members"].append(deepcopy(before["members"][0]))
        after["members"] = deepcopy(before["members"])
    elif failure == "missing-field":
        del records[0]["process_incarnation"]
    else:
        records[0]["accepted_at"] = "2026-09-08T00:00:16Z"
    assert not _cohort_converged(before, after, records, "etag-a", digest)


@pytest.mark.parametrize("available", [True, False])
def test_observer_is_bounded_and_never_opens_cosmos(monkeypatch, capsys, available):
    before, after, records = _evidence()
    for inventory in (before, after):
        inventory["startedAt"] = inventory["observedAt"]
        del inventory["members"][0]["process_incarnation"]
    clock = [0.0]
    monkeypatch.setattr(publisher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(publisher.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(publisher, "_replica_inventory", lambda *_: before if clock[0] == 0 else after)
    monkeypatch.setattr(publisher, "_observation_records", lambda *_: records if available else [])
    monkeypatch.setattr(publisher, "_container", lambda *_: pytest.fail("observe must never open Cosmos"))
    result = publisher.main([
        "observe", "--deployment-instance-id", "synthetic", "--subscription", "synthetic",
        "--resource-group", "synthetic", "--application", "synthetic", "--workspace", "synthetic",
        "--target-etag", "etag-a", "--target-digest", records[0]["digest"], "--observation-timeout", "60",
    ])
    assert result == (0 if available else 3)
    assert clock[0] <= 60
    assert ('"status": "converged"' in capsys.readouterr().out) is available


def test_observer_interruption_does_not_mutate(monkeypatch):
    monkeypatch.setattr(publisher, "_replica_inventory", lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    monkeypatch.setattr(publisher, "_container", lambda *_: pytest.fail("unexpected Cosmos access"))
    with pytest.raises(KeyboardInterrupt):
        publisher.main([
            "observe", "--deployment-instance-id", "synthetic", "--subscription", "synthetic",
            "--resource-group", "synthetic", "--application", "synthetic", "--workspace", "synthetic",
            "--target-etag", "etag-a", "--target-digest", "sha256:" + "a" * 64,
        ])


def test_inventory_uses_explicit_revision_and_container_identity(monkeypatch):
    calls = []

    def azure(arguments, deadline):
        calls.append(arguments)
        if arguments[:3] == ["containerapp", "revision", "list"]:
            return [{"name": "revision-a", "properties": {"active": True}},
                    {"name": "old", "properties": {"active": False}}]
        assert arguments[:3] == ["containerapp", "replica", "list"]
        assert arguments[-2:] == ["--revision", "revision-a"]
        return [{"name": "replica-a", "properties": {"containers": [{
            "name": "retrieval-agent", "containerId": "container-a", "ready": True, "restartCount": 2,
        }]}}]

    monkeypatch.setattr(publisher, "_azure_json", azure)
    args = SimpleNamespace(subscription="sub", resource_group="group", application="app")
    inventory = publisher._replica_inventory(args, 100)
    assert inventory["members"] == [{
        "revision": "revision-a", "replica": "replica-a", "container": "container-a", "restart_count": 2,
    }]
    assert len(calls) == 2
    assert all(command[3:9] == ["--subscription", "sub", "--resource-group", "group", "--name", "app"] for command in calls)


def test_log_query_scopes_application_instance_and_explicit_time(monkeypatch):
    calls = []
    monkeypatch.setattr(publisher, "_azure_json", lambda command, deadline: calls.append(command) or [])
    args = SimpleNamespace(subscription="sub", application="app", workspace="workspace", deployment_instance_id="synthetic")
    assert publisher._observation_records(args, "2026-09-08T00:00:00+00:00", 100) == []
    command = calls[0]
    assert command[:3] == ["monitor", "log-analytics", "query"]
    query = command[command.index("--analytics-query") + 1]
    assert 'Properties.application) == "app"' in query
    assert hashlib.sha256(b"synthetic").hexdigest() in query
    assert "catalog_rejected" in query
    assert "catalog_degraded" in query
    assert "take 10001" in query
    assert command[command.index("--timespan") + 1].startswith("2026-09-08T00:00:00+00:00/")