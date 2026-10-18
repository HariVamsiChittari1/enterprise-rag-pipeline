from __future__ import annotations

import base64
import json
from unittest.mock import Mock

import httpx
import pytest
from retrieval.auth import (
    AuthorizationError,
    GraphGroupResolver,
    Principal,
    principal_from_easy_auth_user,
    principal_with_security_groups,
    require_easy_auth_role,
)

TENANT_ID = "11111111-1111-1111-1111-111111111111"
USER_ID = "22222222-2222-4222-8222-abcdefabcdef"
FUNCTION_AUDIENCE = "api://function-api"


def test_graph_group_resolver_excludes_microsoft_365_groups() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["$select"] == "id,securityEnabled,groupTypes"
        return httpx.Response(200, json={"value": [
            {
                "id": "static-security-group",
                "securityEnabled": True,
                "groupTypes": [],
            },
            {
                "id": "dynamic-security-group",
                "securityEnabled": True,
                "groupTypes": ["DynamicMembership"],
            },
            {
                "id": "microsoft-365-group",
                "securityEnabled": True,
                "groupTypes": ["Unified"],
            },
        ]})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        groups = GraphGroupResolver(client).resolve_transitive_security_groups(USER_ID)

    assert groups == {"static-security-group", "dynamic-security-group"}


@pytest.mark.parametrize("group_types", [None, "Unified", [None]])
def test_graph_group_resolver_rejects_invalid_group_types(
    group_types: object,
) -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(
        200,
        json={"value": [{
            "id": "group",
            "securityEnabled": True,
            "groupTypes": group_types,
        }]},
    ))

    with httpx.Client(transport=transport) as client:
        with pytest.raises(AuthorizationError, match="invalid_group_response"):
            GraphGroupResolver(client).resolve_transitive_security_groups(USER_ID)


def test_empty_security_group_membership_is_a_valid_principal() -> None:
    resolver = Mock()
    resolver.resolve_transitive_security_groups.return_value = set()
    principal = Principal(USER_ID, TENANT_ID, frozenset())

    resolved = principal_with_security_groups(principal, resolver)

    assert resolved == principal


def test_invalid_security_group_result_fails_closed() -> None:
    resolver = Mock()
    resolver.resolve_transitive_security_groups.return_value = None
    principal = Principal(USER_ID, TENANT_ID, frozenset())

    with pytest.raises(AuthorizationError, match="security_groups_unresolved"):
        principal_with_security_groups(principal, resolver)


def encode(claims: list[dict[str, str]]) -> str:
    payload = json.dumps({"claims": claims}).encode()
    return base64.b64encode(payload).decode()


def test_easy_auth_uri_claim_names_are_normalized() -> None:
    aliases = {
        "oid": "http://schemas.microsoft.com/identity/claims/objectidentifier",
        "tid": "http://schemas.microsoft.com/identity/claims/tenantid",
        "scp": "http://schemas.microsoft.com/identity/claims/scope",
    }
    encoded = encode(
        [
            {"typ": aliases.get(claim["typ"], claim["typ"]), "val": claim["val"]}
            for claim in _user_claims()
        ]
    )

    principal = principal_from_easy_auth_user(
        encoded,
        expected_tenant_id=TENANT_ID,
        expected_audience=FUNCTION_AUDIENCE,
    )

    assert principal.user_id == USER_ID


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


def test_direct_user_claims_accept_existing_registration_without_idtyp() -> None:
    claims = [claim for claim in _user_claims() if claim["typ"] != "idtyp"]

    principal = principal_from_easy_auth_user(
        encode(claims),
        expected_tenant_id=TENANT_ID,
        expected_audience=FUNCTION_AUDIENCE,
    )

    assert principal == Principal(USER_ID, TENANT_ID, frozenset())


def test_direct_easy_auth_user_returns_principal_without_groups() -> None:
    principal = principal_from_easy_auth_user(
        encode(_user_claims()),
        expected_tenant_id=TENANT_ID,
        expected_audience=FUNCTION_AUDIENCE,
    )

    assert principal == Principal(USER_ID, TENANT_ID, frozenset())


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
def test_direct_user_claims_fail_closed(
    claim: str, value: str, message: str,
) -> None:
    with pytest.raises(AuthorizationError, match=message):
        principal_from_easy_auth_user(
            encode(_user_claims(**{claim: value})),
            expected_tenant_id=TENANT_ID,
            expected_audience=FUNCTION_AUDIENCE,
        )


@pytest.mark.parametrize(
    ("claim", "value", "message"),
    [
        ("tid", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "unexpected_tenant"),
        ("aud", "api://other", "unexpected_audience"),
        ("idtyp", "app", "user_token_required"),
        ("scp", "other.scope", "required_scope_missing"),
    ],
)
def test_direct_easy_auth_user_fails_closed(
    claim: str,
    value: str,
    message: str,
) -> None:
    with pytest.raises(AuthorizationError, match=message):
        principal_from_easy_auth_user(
            encode(_user_claims(**{claim: value})),
            expected_tenant_id=TENANT_ID,
            expected_audience=FUNCTION_AUDIENCE,
        )


def test_ingestion_operator_role_remains_supported() -> None:
    encoded = encode([
        {"typ": "tid", "val": TENANT_ID},
        {"typ": "roles", "val": "Ingestion.Operator"},
    ])

    require_easy_auth_role(encoded, TENANT_ID, "Ingestion.Operator")