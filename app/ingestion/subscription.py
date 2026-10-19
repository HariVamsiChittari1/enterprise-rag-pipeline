"""Microsoft Graph webhook subscription lifecycle for SharePoint drive change notifications."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

GRAPH_SUBSCRIPTIONS_URL = "https://graph.microsoft.com/v1.0/subscriptions"
# driveItem subscriptions support max 42,300 minutes (~30 days)
_SUBSCRIPTION_LIFETIME_MINUTES = 41_000  # renew with ~1 day margin


@dataclass(frozen=True)
class SubscriptionInfo:
    subscription_id: str
    resource: str
    expiration: str


def create_subscription(
    client: httpx.Client,
    drive_id: str,
    notification_url: str,
    lifecycle_notification_url: str,
    client_state: str,
) -> SubscriptionInfo:
    """Create a Graph subscription for drive changes."""
    expiration = datetime.now(timezone.utc) + timedelta(minutes=_SUBSCRIPTION_LIFETIME_MINUTES)
    body = {
        "changeType": "updated",
        "notificationUrl": notification_url,
        "lifecycleNotificationUrl": lifecycle_notification_url,
        "resource": f"/drives/{drive_id}/root",
        "expirationDateTime": expiration.isoformat(),
        "clientState": client_state,
    }
    response = client.post(
        GRAPH_SUBSCRIPTIONS_URL,
        json=body,
        headers={"Prefer": "includesecuritywebhooks"},
    )
    response.raise_for_status()
    data = response.json()
    return SubscriptionInfo(
        subscription_id=data["id"],
        resource=data["resource"],
        expiration=data["expirationDateTime"],
    )


def find_drive_subscriptions(
    client: httpx.Client,
    drive_id: str,
    client_state: str,
) -> tuple[SubscriptionInfo, ...]:
    """Find existing subscriptions for one exact drive/clientState after ambiguous create."""
    if not drive_id or not client_state:
        raise ValueError("drive_id and client_state are required")
    expected_resource = f"/drives/{drive_id}/root"
    url = GRAPH_SUBSCRIPTIONS_URL
    matches: list[SubscriptionInfo] = []
    while url:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
            raise ValueError("Graph subscription listing is invalid")
        for item in payload["value"]:
            if not isinstance(item, dict):
                raise ValueError("Graph subscription record is invalid")
            if (
                item.get("resource") == expected_resource
                and item.get("clientState") == client_state
                and isinstance(item.get("id"), str)
                and isinstance(item.get("expirationDateTime"), str)
            ):
                matches.append(SubscriptionInfo(
                    item["id"], item["resource"], item["expirationDateTime"],
                ))
        next_url = payload.get("@odata.nextLink")
        if next_url is not None:
            if (
                not isinstance(next_url, str)
                or not next_url.startswith("https://graph.microsoft.com/")
            ):
                raise ValueError("Graph subscription continuation URL is invalid")
            url = next_url
        else:
            url = ""
    return tuple(matches)


def reconcile_drive_subscription(
    client: httpx.Client,
    drive_id: str,
    notification_url: str,
    lifecycle_notification_url: str,
    client_state: str,
    existing_subscription_id: str | None,
) -> SubscriptionInfo:
    if existing_subscription_id:
        try:
            return renew_subscription(client, existing_subscription_id)
        except SubscriptionNotFoundError:
            pass
    matches = find_drive_subscriptions(client, drive_id, client_state)
    if matches:
        selected = matches[0]
        for duplicate in matches[1:]:
            delete_subscription(client, duplicate.subscription_id)
        try:
            return renew_subscription(client, selected.subscription_id)
        except SubscriptionNotFoundError:
            pass
    return create_subscription(
        client,
        drive_id,
        notification_url,
        lifecycle_notification_url,
        client_state,
    )


def renew_subscription(
    client: httpx.Client,
    subscription_id: str,
) -> SubscriptionInfo:
    """Extend a subscription's expiration."""
    expiration = datetime.now(timezone.utc) + timedelta(minutes=_SUBSCRIPTION_LIFETIME_MINUTES)
    response = client.patch(
        f"{GRAPH_SUBSCRIPTIONS_URL}/{subscription_id}",
        json={"expirationDateTime": expiration.isoformat()},
    )
    if response.status_code == 404:
        raise SubscriptionNotFoundError(subscription_id)
    response.raise_for_status()
    data = response.json()
    return SubscriptionInfo(
        subscription_id=data["id"],
        resource=data["resource"],
        expiration=data["expirationDateTime"],
    )


def delete_subscription(client: httpx.Client, subscription_id: str) -> None:
    """Remove a subscription. Tolerates 404 (already gone)."""
    response = client.delete(f"{GRAPH_SUBSCRIPTIONS_URL}/{subscription_id}")
    if response.status_code != 404:
        response.raise_for_status()


class SubscriptionNotFoundError(Exception):
    def __init__(self, subscription_id: str) -> None:
        super().__init__(f"Subscription {subscription_id} not found (expired or deleted)")
        self.subscription_id = subscription_id


def validate_webhook_notification(
    payload: dict[str, Any],
    expected_client_state: str,
) -> bool:
    """Validate that a webhook notification originated from our subscription."""
    notifications = payload.get("value")
    if not isinstance(notifications, list) or not notifications:
        return False
    for notification in notifications:
        if not isinstance(notification, dict):
            return False
        if notification.get("clientState") != expected_client_state:
            logger.warning("webhook_client_state_mismatch")
            return False
    return True
