"""Identity contracts for requests authenticated by App Service Authentication."""

from __future__ import annotations

import base64
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote

import httpx


CLAIM_TYPE_ALIASES = {
    "http://schemas.microsoft.com/identity/claims/objectidentifier": "oid",
    "http://schemas.microsoft.com/identity/claims/tenantid": "tid",
    "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups": "groups",
    "http://schemas.microsoft.com/ws/2008/06/identity/claims/role": "roles",
    "http://schemas.microsoft.com/identity/claims/scope": "scp",
    "http://schemas.microsoft.com/identity/claims/idtyp": "idtyp",
}

GATEWAY_CONTEXT_HEADER = "X-RAG-GATEWAY-CONTEXT"
GATEWAY_REQUEST_ID_HEADER = "X-RAG-REQUEST-ID"
GATEWAY_CONTEXT_MAX_BYTES = 1024
AUTH_PRINCIPAL_MAX_BYTES = 64 * 1024
_BASE64URL_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class AuthorizationError(ValueError):
    pass


class GroupResolver(Protocol):
    def resolve_transitive_security_groups(self, user_id: str) -> set[str]: ...


class GraphGroupResolver:
    def __init__(self, client: httpx.Client, max_pages: int = 20) -> None:
        if max_pages < 1:
            raise ValueError("max_pages must be positive")
        self._client = client
        self._max_pages = max_pages

    def resolve_transitive_security_groups(self, user_id: str) -> set[str]:
        if not user_id:
            raise AuthorizationError("user_id_required")
        url: str | None = (
            "https://graph.microsoft.com/v1.0/users/"
            f"{quote(user_id, safe='')}/transitiveMemberOf/microsoft.graph.group"
            "?$select=id,securityEnabled"
        )
        group_ids: set[str] = set()
        pages = 0
        while url:
            if pages >= self._max_pages:
                raise AuthorizationError("group_page_limit_exceeded")
            parsed = httpx.URL(url)
            if parsed.scheme != "https" or parsed.host != "graph.microsoft.com":
                raise AuthorizationError("invalid_group_paging_url")
            response = self._client.get(url)
            response.raise_for_status()
            payload = response.json()
            values = payload.get("value") if isinstance(payload, dict) else None
            if not isinstance(values, list) or not all(isinstance(item, dict) for item in values):
                raise AuthorizationError("invalid_group_response")
            for item in values:
                group_id = item.get("id")
                security_enabled = item.get("securityEnabled")
                if not isinstance(group_id, str) or not isinstance(security_enabled, bool):
                    raise AuthorizationError("invalid_group_response")
                if security_enabled:
                    group_ids.add(group_id)
            next_link = payload.get("@odata.nextLink")
            if next_link is not None and not isinstance(next_link, str):
                raise AuthorizationError("invalid_group_paging_url")
            url = next_link
            pages += 1
        return group_ids


@dataclass(frozen=True)
class Principal:
    user_id: str
    tenant_id: str
    security_group_ids: frozenset[str]

    @property
    def acl_ids(self) -> list[str]:
        return sorted(self.security_group_ids)


@dataclass(frozen=True)
class GatewayContext:
    oid: str
    tid: str
    version: int = 1

    def encode(self) -> str:
        payload = json.dumps(
            {"v": self.version, "oid": self.oid, "tid": self.tid},
            separators=(",", ":"),
        ).encode("ascii")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def gateway_context_from_easy_auth_user(
    encoded_principal: str | None,
    *,
    expected_tenant_id: str,
    expected_audience: str,
    required_scope: str = "user_impersonation",
) -> GatewayContext:
    claims = _claims_by_type(encoded_principal)
    tenant_id = _canonical_guid(_single_claim(claims, "tid"), "tid")
    user_id = _canonical_guid(_single_claim(claims, "oid"), "oid")
    if tenant_id != _canonical_guid(expected_tenant_id, "expected_tid"):
        raise AuthorizationError("unexpected_tenant")
    if _single_claim(claims, "aud") != expected_audience:
        raise AuthorizationError("unexpected_audience")
    identity_types = claims.get("idtyp", [])
    if len(identity_types) > 1 or (
        identity_types and identity_types[0] != "user"
    ):
        raise AuthorizationError("user_token_required")
    scopes = set(_single_claim(claims, "scp").split())
    if required_scope not in scopes:
        raise AuthorizationError("required_scope_missing")
    return GatewayContext(oid=user_id, tid=tenant_id)


def principal_from_gateway(
    encoded_service_principal: str | None,
    encoded_context: str | None,
    *,
    expected_tenant_id: str,
    expected_audience: str,
    expected_gateway_client_id: str,
    expected_gateway_principal_id: str,
    group_resolver: GroupResolver | None,
    acl_enabled: bool = True,
    required_role: str = "Retrieval.Gateway",
) -> Principal:
    claims = _claims_by_type(encoded_service_principal)
    tenant_id = _canonical_guid(_single_claim(claims, "tid"), "tid")
    if tenant_id != _canonical_guid(expected_tenant_id, "expected_tid"):
        raise AuthorizationError("unexpected_tenant")
    if _single_claim(claims, "aud") != expected_audience:
        raise AuthorizationError("unexpected_audience")
    if _single_claim(claims, "idtyp") != "app":
        raise AuthorizationError("service_token_required")
    if _canonical_guid(_single_claim(claims, "azp"), "azp") != _canonical_guid(
        expected_gateway_client_id, "expected_azp"
    ):
        raise AuthorizationError("unexpected_gateway_client")
    if _canonical_guid(_single_claim(claims, "oid"), "oid") != _canonical_guid(
        expected_gateway_principal_id, "expected_oid"
    ):
        raise AuthorizationError("unexpected_gateway_principal")
    if required_role not in claims.get("roles", []):
        raise AuthorizationError("required_role_missing")

    context = parse_gateway_context(encoded_context)
    if context.tid != tenant_id:
        raise AuthorizationError("gateway_context_tenant_mismatch")
    if not acl_enabled:
        return Principal(context.oid, context.tid, frozenset())
    if group_resolver is None:
        raise AuthorizationError("security_groups_unresolved")
    try:
        group_ids = group_resolver.resolve_transitive_security_groups(context.oid)
    except Exception as error:
        raise AuthorizationError("security_groups_unresolved") from error
    if not group_ids or not all(
        isinstance(group_id, str) and group_id for group_id in group_ids
    ):
        raise AuthorizationError("security_groups_unresolved")
    return Principal(context.oid, context.tid, frozenset(group_ids))


def parse_gateway_context(encoded_context: str | None) -> GatewayContext:
    if (
        not isinstance(encoded_context, str)
        or not encoded_context
        or len(encoded_context) > 2048
        or not _BASE64URL_RE.fullmatch(encoded_context)
    ):
        raise AuthorizationError("invalid_gateway_context")
    try:
        padding = "=" * (-len(encoded_context) % 4)
        raw = base64.b64decode(
            encoded_context + padding,
            altchars=b"-_",
            validate=True,
        )
        if len(raw) > GATEWAY_CONTEXT_MAX_BYTES:
            raise AuthorizationError("gateway_context_too_large")
        payload = json.loads(raw, object_pairs_hook=_reject_duplicate_json_keys)
    except AuthorizationError:
        raise
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise AuthorizationError("invalid_gateway_context") from error
    if not isinstance(payload, dict) or set(payload) != {"v", "oid", "tid"}:
        raise AuthorizationError("invalid_gateway_context")
    if payload["v"] != 1 or isinstance(payload["v"], bool):
        raise AuthorizationError("unsupported_gateway_context_version")
    return GatewayContext(
        oid=_canonical_guid(payload["oid"], "gateway_oid"),
        tid=_canonical_guid(payload["tid"], "gateway_tid"),
    )


def parse_gateway_request_id(value: str | None) -> str:
    if not isinstance(value, str):
        raise AuthorizationError("invalid_gateway_request_id")
    try:
        canonical = str(uuid.UUID(value))
    except (ValueError, AttributeError) as error:
        raise AuthorizationError("invalid_gateway_request_id") from error
    if value != canonical:
        raise AuthorizationError("invalid_gateway_request_id")
    return canonical


def require_easy_auth_role(
    encoded_principal: str | None,
    expected_tenant_id: str,
    required_role: str,
) -> None:
    if not required_role:
        raise AuthorizationError("required_role_missing")
    claims = _claims_by_type(encoded_principal)
    if _single_claim(claims, "tid") != expected_tenant_id:
        raise AuthorizationError("unexpected_tenant")
    if required_role not in claims.get("roles", []):
        raise AuthorizationError("required_role_missing")


def _claims_by_type(encoded_principal: str | None) -> dict[str, list[str]]:
    if not encoded_principal:
        raise AuthorizationError("missing_authenticated_principal")
    try:
        padding = "=" * (-len(encoded_principal) % 4)
        raw = base64.b64decode(encoded_principal + padding, validate=True)
        if len(raw) > AUTH_PRINCIPAL_MAX_BYTES:
            raise AuthorizationError("authenticated_principal_too_large")
        payload = json.loads(raw, object_pairs_hook=_reject_duplicate_json_keys)
    except AuthorizationError:
        raise
    except (ValueError, json.JSONDecodeError) as error:
        raise AuthorizationError("invalid_authenticated_principal") from error
    if not isinstance(payload, dict):
        raise AuthorizationError("invalid_authenticated_principal")
    claims = payload.get("claims")
    if not isinstance(claims, list) or not all(isinstance(claim, dict) for claim in claims):
        raise AuthorizationError("invalid_authenticated_claims")
    by_type: dict[str, list[str]] = {}
    for claim in claims:
        claim_type = claim.get("typ")
        value = claim.get("val")
        if isinstance(claim_type, str) and isinstance(value, str) and value:
            normalized_type = CLAIM_TYPE_ALIASES.get(claim_type, claim_type)
            by_type.setdefault(normalized_type, []).append(value)
    return by_type


def _single_claim(claims: dict[str, list[str]], claim_type: str) -> str:
    values = claims.get(claim_type, [])
    if len(values) != 1:
        raise AuthorizationError(f"invalid_{claim_type}_claim")
    return values[0]


def _canonical_guid(value: Any, claim_name: str) -> str:
    if not isinstance(value, str):
        raise AuthorizationError(f"invalid_{claim_name}_claim")
    try:
        canonical = str(uuid.UUID(value))
    except (ValueError, AttributeError) as error:
        raise AuthorizationError(f"invalid_{claim_name}_claim") from error
    if value != canonical:
        raise AuthorizationError(f"noncanonical_{claim_name}_claim")
    return canonical


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AuthorizationError("duplicate_json_key")
        result[key] = value
    return result