"""Security-focused tests for the Function-to-ACA query gateway."""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

import function_app
from retrieval.auth import GATEWAY_CONTEXT_HEADER, GATEWAY_REQUEST_ID_HEADER, parse_gateway_context


TENANT_ID = "11111111-1111-4111-8111-111111111111"
USER_ID = "22222222-2222-4222-8222-abcdefabcdef"


def _principal(**overrides: str) -> str:
    claims = {
        "oid": USER_ID,
        "tid": TENANT_ID,
        "aud": "api://function-api",
        "idtyp": "user",
        "scp": "user_impersonation",
    }
    claims.update(overrides)
    raw = json.dumps({
        "claims": [{"typ": key, "val": value} for key, value in claims.items()]
    }).encode()
    return base64.b64encode(raw).decode()


class FakeRequest:
    def __init__(self, principal: str, *, inbound_context: str = "forged") -> None:
        self.headers = {
            "X-MS-CLIENT-PRINCIPAL": principal,
            GATEWAY_CONTEXT_HEADER: inbound_context,
        }

    def get_json(self) -> dict[str, Any]:
        return {"question": "What is RAG?"}


class FakeResponse:
    def __init__(self, status_code: int, payload: Any, *, content: bytes | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = content if content is not None else json.dumps(payload).encode()

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeClient:
    response: FakeResponse | Exception
    calls: list[dict[str, Any]] = []

    def __init__(self, *, timeout: float) -> None:
        self.timeout = timeout

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


@pytest.fixture(autouse=True)
def gateway_environment(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest,
) -> None:
    monkeypatch.setenv(
        "RETRIEVAL_SERVICE_URL",
        "https://retrieval.environment.eastus2.azurecontainerapps.io",
    )
    monkeypatch.setenv("TENANT_ID", TENANT_ID)
    monkeypatch.setenv("FUNCTION_API_AUDIENCE", "api://function-api")
    monkeypatch.setenv("RETRIEVAL_SERVICE_SCOPE", "api://retrieval-api/.default")
    monkeypatch.setenv("QUERY_PROXY_TIMEOUT_SECONDS", "30")
    monkeypatch.setattr(httpx, "Client", FakeClient)
    if request.node.name != "test_retrieval_service_credential_is_reused":
        monkeypatch.setattr(
            function_app,
            "_build_retrieval_service_credential",
            lambda: SimpleNamespace(get_token=lambda scope: SimpleNamespace(token="service-token")),
        )
    FakeClient.calls = []


def _body(response: Any) -> dict[str, Any]:
    return json.loads(response.get_body())


def test_query_gateway_overwrites_context_and_owns_request_id() -> None:
    FakeClient.response = FakeResponse(200, {
        "answer": "Grounded answer.",
        "citations": [],
        "request_id": "upstream-value",
    })

    response = function_app.query_endpoint(FakeRequest(_principal()))

    assert response.status_code == 200
    payload = _body(response)
    assert payload["answer"] == "Grounded answer."
    assert payload["request_id"] != "upstream-value"
    outgoing = FakeClient.calls[0]["headers"]
    assert outgoing["Authorization"] == "Bearer service-token"
    assert outgoing[GATEWAY_REQUEST_ID_HEADER] == payload["request_id"]
    context = parse_gateway_context(outgoing[GATEWAY_CONTEXT_HEADER])
    assert context.oid == USER_ID
    assert context.tid == TENANT_ID
    assert outgoing[GATEWAY_CONTEXT_HEADER] != "forged"


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"idtyp": "app"}, "unauthorized"),
        ({"aud": "api://other"}, "unauthorized"),
        ({"scp": "other.scope"}, "unauthorized"),
        ({"tid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, "unauthorized"),
    ],
)
def test_query_gateway_rejects_invalid_user_claims(
    overrides: dict[str, str], code: str,
) -> None:
    FakeClient.response = FakeResponse(200, {"answer": "x", "citations": []})

    response = function_app.query_endpoint(FakeRequest(_principal(**overrides)))

    assert response.status_code == 401
    assert _body(response)["error"]["code"] == code
    assert FakeClient.calls == []


def test_query_gateway_maps_timeout() -> None:
    FakeClient.response = httpx.TimeoutException("timed out")

    response = function_app.query_endpoint(FakeRequest(_principal()))

    assert response.status_code == 504
    assert _body(response)["error"]["code"] == "retrieval_timeout"


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example.com",
        "https://retrieval.azurecontainerapps.io",
        "https://user@retrieval.environment.eastus2.azurecontainerapps.io",
        "https://retrieval.environment.eastus2.azurecontainerapps.io:444",
        "https://retrieval.environment.eastus2.azurecontainerapps.io/other",
        "https://retrieval.environment.eastus2.azurecontainerapps.io?redirect=evil",
    ],
)
def test_query_gateway_rejects_non_internal_aca_url(
    monkeypatch: pytest.MonkeyPatch, url: str,
) -> None:
    monkeypatch.setenv("RETRIEVAL_SERVICE_URL", url)
    FakeClient.response = FakeResponse(200, {"answer": "x", "citations": []})

    response = function_app.query_endpoint(FakeRequest(_principal()))

    assert response.status_code == 503
    assert _body(response)["error"]["code"] == "gateway_not_configured"
    assert FakeClient.calls == []


def test_query_gateway_normalizes_service_auth_failure() -> None:
    FakeClient.response = FakeResponse(401, {"detail": "token internals"})

    response = function_app.query_endpoint(FakeRequest(_principal()))

    assert response.status_code == 502
    assert _body(response)["error"]["code"] == "retrieval_auth_failed"
    assert "token internals" not in response.get_body().decode()


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(200, ValueError("not json"), content=b"<html>bad</html>"),
        FakeResponse(200, {"unexpected": True}),
        FakeResponse(200, {"answer": "x", "citations": []}, content=b"x" * 1_048_577),
    ],
)
def test_query_gateway_rejects_malformed_or_oversized_response(
    response: FakeResponse,
) -> None:
    FakeClient.response = response

    result = function_app.query_endpoint(FakeRequest(_principal()))

    assert result.status_code == 502
    assert _body(result)["error"]["code"] == "invalid_retrieval_response"


@pytest.mark.parametrize(
    ("status", "payload", "content", "expected_code"),
    [
        (502, {"error": {"code": "answer_citation_invalid", "message": "upstream-private"},
               "request_id": "upstream-id", "answer": "upstream-private"}, None, "answer_citation_invalid"),
        (502, {"error": {"code": "other", "message": "upstream-private"}}, None, "retrieval_unavailable"),
        (502, {"error": "answer_citation_invalid"}, None, "retrieval_unavailable"),
        (502, {"error": None}, None, "retrieval_unavailable"),
        (502, ["answer_citation_invalid"], None, "retrieval_unavailable"),
        (502, ValueError("upstream-private"), b"not json", "retrieval_unavailable"),
        (502, {"error": {"code": "answer_citation_invalid"}}, b"x" * 1_048_577, "retrieval_unavailable"),
        (503, {"error": {"code": "answer_citation_invalid"}}, None, "retrieval_unavailable"),
        (400, {"error": {"code": "answer_citation_invalid"}}, None, "retrieval_request_failed"),
        (401, {"error": {"code": "answer_citation_invalid"}}, None, "retrieval_auth_failed"),
        (403, {"error": {"code": "answer_citation_invalid"}}, None, "retrieval_auth_failed"),
    ],
    ids=["citation", "unknown", "string-error", "null-error", "array", "invalid-json",
         "oversized", "wrong-server-status", "client-status", "unauthorized", "forbidden"],
)
def test_given_upstream_citation_error_when_proxying_then_only_allowlists_exact_contract(
    status: int, payload: Any, content: bytes | None, expected_code: str,
) -> None:
    FakeClient.response = FakeResponse(status, payload, content=content)

    response = function_app.query_endpoint(FakeRequest(_principal()))

    body = _body(response)
    assert response.status_code == (400 if status == 400 else 502)
    assert body["error"]["code"] == expected_code
    if expected_code == "answer_citation_invalid":
        assert body["error"]["message"] == "The generated answer could not be validated."
    assert body["request_id"] == FakeClient.calls[0]["headers"][GATEWAY_REQUEST_ID_HEADER]
    assert body["request_id"] != "upstream-id"
    assert "answer" not in body
    assert "upstream-private" not in response.get_body().decode()


@pytest.mark.parametrize(
    ("status", "payload", "content", "expected_status", "expected_code"),
    [
        (422, {"error": {"code": "content_filtered", "message": "upstream-private"},
               "request_id": "upstream-id", "answer": "upstream-private"}, None, 422, "content_filtered"),
        (422, {"error": {"code": "other"}}, None, 422, "retrieval_request_failed"),
        (422, {"error": "content_filtered"}, None, 422, "retrieval_request_failed"),
        (422, {"error": None}, None, 422, "retrieval_request_failed"),
        (422, ["content_filtered"], None, 422, "retrieval_request_failed"),
        (422, ValueError("upstream-private"), b"not json", 502, "invalid_retrieval_response"),
        (422, {"error": {"code": "content_filtered"}}, b"x" * 1_048_577, 502, "invalid_retrieval_response"),
        (502, {"error": {"code": "content_filtered"}}, None, 502, "retrieval_unavailable"),
        (400, {"error": {"code": "content_filtered"}}, None, 400, "retrieval_request_failed"),
        (401, {"error": {"code": "content_filtered"}}, None, 502, "retrieval_auth_failed"),
        (403, {"error": {"code": "content_filtered"}}, None, 502, "retrieval_auth_failed"),
        (422, {"error": {"code": "answer_citation_invalid"}}, None, 422, "retrieval_request_failed"),
    ],
    ids=["filter", "unknown", "string-error", "null-error", "array", "invalid-json",
         "oversized", "wrong-server-status", "client-status", "unauthorized", "forbidden", "citation-wrong-status"],
)
def test_given_upstream_filter_error_when_proxying_then_only_allowlists_exact_contract(
    status: int, payload: Any, content: bytes | None, expected_status: int, expected_code: str,
) -> None:
    FakeClient.response = FakeResponse(status, payload, content=content)

    response = function_app.query_endpoint(FakeRequest(_principal()))

    body = _body(response)
    assert response.status_code == expected_status
    assert body["error"]["code"] == expected_code
    if expected_code == "content_filtered":
        assert body["error"]["message"] == "The request could not be completed under the content safety policy."
    assert body["request_id"] == FakeClient.calls[0]["headers"][GATEWAY_REQUEST_ID_HEADER]
    assert body["request_id"] != "upstream-id"
    assert "answer" not in body
    assert "upstream-private" not in response.get_body().decode()


def test_retrieval_service_credential_is_reused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import azure.identity

    created: list[str] = []

    def _credential(*, client_id: str) -> object:
        created.append(client_id)
        return object()

    function_app._client_cache.pop("retrieval_service_credential", None)
    monkeypatch.setenv("MANAGED_IDENTITY_CLIENT_ID", "gateway-client")
    monkeypatch.setattr(azure.identity, "ManagedIdentityCredential", _credential)

    first = function_app._build_retrieval_service_credential()
    second = function_app._build_retrieval_service_credential()

    assert first is second
    assert created == ["gateway-client"]
    function_app._client_cache.pop("retrieval_service_credential", None)
