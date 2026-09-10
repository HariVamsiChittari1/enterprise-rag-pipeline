from __future__ import annotations

import base64
import json
from unittest.mock import Mock

import pytest

from retrieval.auth import (
    AuthorizationError,
    GatewayContext,
    gateway_context_from_easy_auth_user,
    parse_gateway_context,
    principal_from_gateway,
    require_easy_auth_role,
)


TENANT_ID = "11111111-1111-1111-1111-111111111111"
USER_ID = "22222222-2222-4222-8222-abcdefabcdef"
GATEWAY_CLIENT_ID = "33333333-3333-3333-3333-333333333333"
GATEWAY_PRINCIPAL_ID = "44444444-4444-4444-4444-444444444444"
FUNCTION_AUDIENCE = "api://function-api"
RETRIEVAL_AUDIENCE = "33333333-3333-4333-8333-333333333333"


def encode(claims: list[dict[str, str]]) -> str:
    payload = json.dumps({"claims": claims}).encode()
    return base64.b64encode(payload).decode()


@pytest.mark.parametrize(
    "group_claims",
    [
        [{"typ": "groups", "val": "group-2"}, {"typ": "groups", "val": "group-1"}],
        [{"typ": "hasgroups", "val": "true"}],
    ],
    ids=["token-groups", "group-overage"],
)
def test_gateway_resolves_user_groups_with_graph(
    group_claims: list[dict[str, str]],
) -> None:
    resolver = Mock()
    resolver.resolve_transitive_security_groups.return_value = {"security-group"}
    context = gateway_context_from_easy_auth_user(
        encode(_user_claims() + group_claims),
        expected_tenant_id=TENANT_ID,
        expected_audience=FUNCTION_AUDIENCE,
    )

    principal = principal_from_gateway(
        encode(_service_claims()),
        context.encode(),
        expected_tenant_id=TENANT_ID,
        expected_audience=RETRIEVAL_AUDIENCE,
        expected_gateway_client_id=GATEWAY_CLIENT_ID,
        expected_gateway_principal_id=GATEWAY_PRINCIPAL_ID,
        group_resolver=resolver,
    )

    assert principal.acl_ids == ["security-group"]
    resolver.resolve_transitive_security_groups.assert_called_once_with(USER_ID)


def test_easy_auth_uri_claim_names_are_normalized() -> None:
    resolver = Mock()
    resolver.resolve_transitive_security_groups.return_value = {"security-group"}
    aliases = {
        "oid": "http://schemas.microsoft.com/identity/claims/objectidentifier",
        "tid": "http://schemas.microsoft.com/identity/claims/tenantid",
        "roles": "http://schemas.microsoft.com/ws/2008/06/identity/claims/role",
    }
    encoded = encode(
        [
            {"typ": aliases.get(claim["typ"], claim["typ"]), "val": claim["val"]}
            for claim in _service_claims()
        ]
    )

    principal = principal_from_gateway(
        encoded,
        GatewayContext(USER_ID, TENANT_ID).encode(),
        expected_tenant_id=TENANT_ID,
        expected_audience=RETRIEVAL_AUDIENCE,
        expected_gateway_client_id=GATEWAY_CLIENT_ID,
        expected_gateway_principal_id=GATEWAY_PRINCIPAL_ID,
        group_resolver=resolver,
    )
    require_easy_auth_role(encoded, TENANT_ID, "Retrieval.Gateway")

    assert principal.user_id == USER_ID
    assert principal.acl_ids == ["security-group"]


@pytest.mark.parametrize(
    "encoded",
    [
        None,
        "not-base64",
        encode([{"typ": "oid", "val": GATEWAY_PRINCIPAL_ID}]),
    ],
)
def test_gateway_principal_fails_closed(encoded: str | None) -> None:
    resolver = Mock()

    with pytest.raises(AuthorizationError):
        principal_from_gateway(
            encoded,
            GatewayContext(USER_ID, TENANT_ID).encode(),
            expected_tenant_id=TENANT_ID,
            expected_audience=RETRIEVAL_AUDIENCE,
            expected_gateway_client_id=GATEWAY_CLIENT_ID,
            expected_gateway_principal_id=GATEWAY_PRINCIPAL_ID,
            group_resolver=resolver,
        )

    resolver.resolve_transitive_security_groups.assert_not_called()


def test_gateway_skips_group_resolution_when_acl_disabled() -> None:
    resolver = Mock()
    principal = principal_from_gateway(
        encode(_service_claims()),
        GatewayContext(USER_ID, TENANT_ID).encode(),
        expected_tenant_id=TENANT_ID,
        expected_audience=RETRIEVAL_AUDIENCE,
        expected_gateway_client_id=GATEWAY_CLIENT_ID,
        expected_gateway_principal_id=GATEWAY_PRINCIPAL_ID,
        group_resolver=resolver,
        acl_enabled=False,
    )
    assert principal.user_id == USER_ID
    assert principal.tenant_id == TENANT_ID
    assert principal.security_group_ids == frozenset()
    resolver.resolve_transitive_security_groups.assert_not_called()


def test_gateway_still_validates_tenant_when_acl_disabled() -> None:
    with pytest.raises(AuthorizationError, match="unexpected_tenant"):
        principal_from_gateway(
            encode(_service_claims(tid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
            GatewayContext(USER_ID, TENANT_ID).encode(),
            expected_tenant_id=TENANT_ID,
            expected_audience=RETRIEVAL_AUDIENCE,
            expected_gateway_client_id=GATEWAY_CLIENT_ID,
            expected_gateway_principal_id=GATEWAY_PRINCIPAL_ID,
            group_resolver=None,
            acl_enabled=False,
        )


def _user_claims(**overrides: str) -> list[dict[str, str]]:
    values = {
        "oid": USER_ID,
        "tid": TENANT_ID,
        "aud": FUNCTION_AUDIENCE,
        "idtyp": "user",
        "scp": "user_impersonation other.scope",
    }
    values.update(overrides)
    return [{"typ": key, "val": value} for key, value in values.items()]


def _service_claims(**overrides: str) -> list[dict[str, str]]:
    values = {
        "oid": GATEWAY_PRINCIPAL_ID,
        "tid": TENANT_ID,
        "aud": RETRIEVAL_AUDIENCE,
        "idtyp": "app",
        "azp": GATEWAY_CLIENT_ID,
        "roles": "Retrieval.Gateway",
    }
    values.update(overrides)
    return [{"typ": key, "val": value} for key, value in values.items()]


def test_function_user_claims_create_canonical_gateway_context() -> None:
    context = gateway_context_from_easy_auth_user(
        encode(_user_claims()),
        expected_tenant_id=TENANT_ID,
        expected_audience=FUNCTION_AUDIENCE,
    )

    assert context == GatewayContext(USER_ID, TENANT_ID)
    assert parse_gateway_context(context.encode()) == context
    assert "=" not in context.encode()


def test_function_user_claims_accept_existing_registration_without_idtyp() -> None:
    claims = [claim for claim in _user_claims() if claim["typ"] != "idtyp"]

    context = gateway_context_from_easy_auth_user(
        encode(claims),
        expected_tenant_id=TENANT_ID,
        expected_audience=FUNCTION_AUDIENCE,
    )

    assert context == GatewayContext(USER_ID, TENANT_ID)


@pytest.mark.parametrize(
    ("claim", "value", "message"),
    [
        ("tid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "unexpected_tenant"),
        ("aud", "api://other", "unexpected_audience"),
        ("idtyp", "app", "user_token_required"),
        ("scp", "other.scope", "required_scope_missing"),
        ("oid", USER_ID.upper(), "noncanonical_oid_claim"),
    ],
)
def test_function_user_claims_fail_closed(
    claim: str, value: str, message: str,
) -> None:
    with pytest.raises(AuthorizationError, match=message):
        gateway_context_from_easy_auth_user(
            encode(_user_claims(**{claim: value})),
            expected_tenant_id=TENANT_ID,
            expected_audience=FUNCTION_AUDIENCE,
        )


def test_gateway_accepts_only_expected_app_and_resolves_context_user() -> None:
    resolver = Mock()
    resolver.resolve_transitive_security_groups.return_value = {"security-group"}
    context = GatewayContext(USER_ID, TENANT_ID)

    principal = principal_from_gateway(
        encode(_service_claims()),
        context.encode(),
        expected_tenant_id=TENANT_ID,
        expected_audience=RETRIEVAL_AUDIENCE,
        expected_gateway_client_id=GATEWAY_CLIENT_ID,
        expected_gateway_principal_id=GATEWAY_PRINCIPAL_ID,
        group_resolver=resolver,
    )

    assert principal.user_id == USER_ID
    assert principal.acl_ids == ["security-group"]
    resolver.resolve_transitive_security_groups.assert_called_once_with(USER_ID)


@pytest.mark.parametrize(
    ("claim", "value", "message"),
    [
        ("tid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "unexpected_tenant"),
        ("aud", "api://other", "unexpected_audience"),
        ("idtyp", "user", "service_token_required"),
        ("azp", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "unexpected_gateway_client"),
        ("oid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "unexpected_gateway_principal"),
        ("roles", "Other.Role", "required_role_missing"),
    ],
)
def test_gateway_service_claims_fail_closed(
    claim: str, value: str, message: str,
) -> None:
    with pytest.raises(AuthorizationError, match=message):
        principal_from_gateway(
            encode(_service_claims(**{claim: value})),
            GatewayContext(USER_ID, TENANT_ID).encode(),
            expected_tenant_id=TENANT_ID,
            expected_audience=RETRIEVAL_AUDIENCE,
            expected_gateway_client_id=GATEWAY_CLIENT_ID,
            expected_gateway_principal_id=GATEWAY_PRINCIPAL_ID,
            group_resolver=Mock(),
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"v": 2, "oid": USER_ID, "tid": TENANT_ID}, "unsupported_gateway_context_version"),
        ({"v": 1, "oid": USER_ID, "tid": TENANT_ID, "extra": True}, "invalid_gateway_context"),
        ({"v": 1, "oid": USER_ID.upper(), "tid": TENANT_ID}, "noncanonical_gateway_oid_claim"),
    ],
)
def test_gateway_context_rejects_invalid_contract(
    payload: dict[str, object], message: str,
) -> None:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip("=")

    with pytest.raises(AuthorizationError, match=message):
        parse_gateway_context(encoded)


def test_gateway_context_rejects_duplicate_keys_and_oversize() -> None:
    duplicate = (
        f'{{"v":1,"oid":"{USER_ID}","oid":"{USER_ID}","tid":"{TENANT_ID}"}}'
    ).encode()
    duplicate_encoded = base64.urlsafe_b64encode(duplicate).decode().rstrip("=")
    oversized = base64.urlsafe_b64encode(b"{" + b" " * 1024 + b"}").decode().rstrip("=")

    with pytest.raises(AuthorizationError, match="duplicate_json_key"):
        parse_gateway_context(duplicate_encoded)
    with pytest.raises(AuthorizationError, match="gateway_context_too_large"):
        parse_gateway_context(oversized)


def test_gateway_context_tenant_must_match_service_token() -> None:
    context = GatewayContext(USER_ID, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

    with pytest.raises(AuthorizationError, match="gateway_context_tenant_mismatch"):
        principal_from_gateway(
            encode(_service_claims()),
            context.encode(),
            expected_tenant_id=TENANT_ID,
            expected_audience=RETRIEVAL_AUDIENCE,
            expected_gateway_client_id=GATEWAY_CLIENT_ID,
            expected_gateway_principal_id=GATEWAY_PRINCIPAL_ID,
            group_resolver=Mock(),
        )