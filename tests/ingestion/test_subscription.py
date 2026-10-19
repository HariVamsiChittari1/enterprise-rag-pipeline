"""Tests for Microsoft Graph webhook subscription management."""

from __future__ import annotations

import pytest
import httpx

from ingestion.subscription import (
    find_drive_subscriptions,
    reconcile_drive_subscription,
    SubscriptionInfo,
    SubscriptionNotFoundError,
    validate_webhook_notification,
)


class TestValidateWebhookNotification:
    def test_valid_notification_passes(self) -> None:
        payload = {"value": [{"clientState": "secret-123", "resource": "/drives/d1/root"}]}
        assert validate_webhook_notification(payload, "secret-123") is True

    def test_mismatched_client_state_fails(self) -> None:
        payload = {"value": [{"clientState": "wrong-secret", "resource": "/drives/d1/root"}]}
        assert validate_webhook_notification(payload, "secret-123") is False

    def test_empty_value_list_fails(self) -> None:
        payload = {"value": []}
        assert validate_webhook_notification(payload, "secret-123") is False

    def test_missing_value_key_fails(self) -> None:
        payload = {"other": "data"}
        assert validate_webhook_notification(payload, "secret-123") is False

    def test_non_dict_notification_fails(self) -> None:
        payload = {"value": ["not-a-dict"]}
        assert validate_webhook_notification(payload, "secret-123") is False

    def test_multiple_notifications_all_must_match(self) -> None:
        payload = {"value": [
            {"clientState": "secret-123", "resource": "/drives/d1/root"},
            {"clientState": "secret-123", "resource": "/drives/d1/root"},
        ]}
        assert validate_webhook_notification(payload, "secret-123") is True

    def test_multiple_notifications_one_mismatch_fails(self) -> None:
        payload = {"value": [
            {"clientState": "secret-123", "resource": "/drives/d1/root"},
            {"clientState": "wrong", "resource": "/drives/d1/root"},
        ]}
        assert validate_webhook_notification(payload, "secret-123") is False


class TestSubscriptionNotFoundError:
    def test_contains_subscription_id(self) -> None:
        err = SubscriptionNotFoundError("sub-abc")
        assert err.subscription_id == "sub-abc"
        assert "sub-abc" in str(err)


class TestSubscriptionInfo:
    def test_frozen_dataclass(self) -> None:
        info = SubscriptionInfo(
            subscription_id="sub-1",
            resource="/drives/d1/root",
            expiration="2026-09-01T00:00:00Z",
        )
        assert info.subscription_id == "sub-1"
        with pytest.raises(AttributeError):
            info.subscription_id = "modified"  # type: ignore[misc]


def test_find_drive_subscriptions_pages_and_matches_exact_resource_and_client_state() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if len(calls) == 1:
            return httpx.Response(200, json={
                "value": [
                    {"id": "wrong-drive", "resource": "/drives/other/root", "clientState": "state",
                     "expirationDateTime": "2026-10-01T00:00:00Z"},
                    {"id": "wrong-state", "resource": "/drives/drive-a/root", "clientState": "other",
                     "expirationDateTime": "2026-10-01T00:00:00Z"},
                ],
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/subscriptions?$skip=2",
            })
        return httpx.Response(200, json={
            "value": [{
                "id": "matching", "resource": "/drives/drive-a/root", "clientState": "state",
                "expirationDateTime": "2026-10-02T00:00:00Z",
            }],
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        matches = find_drive_subscriptions(client, "drive-a", "state")

    assert [match.subscription_id for match in matches] == ["matching"]
    assert len(calls) == 2


def test_find_drive_subscriptions_rejects_untrusted_continuation_url() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "value": [],
            "@odata.nextLink": "https://attacker.invalid/subscriptions",
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError, match="continuation URL is invalid"):
            find_drive_subscriptions(client, "drive-a", "state")


def test_reconcile_drive_subscription_recovers_create_saved_late() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, str(request.url)))
        if request.method == "GET":
            return httpx.Response(200, json={"value": [{
                "id": "created-before-save",
                "resource": "/drives/drive-a/root",
                "clientState": "state",
                "expirationDateTime": "2026-10-02T00:00:00Z",
            }]})
        if request.method == "PATCH":
            return httpx.Response(200, json={
                "id": "created-before-save",
                "resource": "/drives/drive-a/root",
                "expirationDateTime": "2026-11-02T00:00:00Z",
            })
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        info = reconcile_drive_subscription(
            client, "drive-a", "https://function.invalid/webhook",
            "https://function.invalid/lifecycle", "state", None,
        )

    assert info.subscription_id == "created-before-save"
    assert [method for method, _ in calls] == ["GET", "PATCH"]
