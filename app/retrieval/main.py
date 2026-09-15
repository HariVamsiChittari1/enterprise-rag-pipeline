"""FastAPI application for RAG retrieval service on AKS."""

from __future__ import annotations

import asyncio
import collections
import hashlib
import os
import re
import time
import threading
import uuid
from contextlib import AsyncExitStack, ExitStack, asynccontextmanager
from dataclasses import asdict
from typing import Any, Mapping

import httpx
import structlog
from azure.cosmos import CosmosClient
from azure.identity import ManagedIdentityCredential
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from openai import AzureOpenAI, DefaultAsyncHttpxClient, DefaultHttpxClient
from pydantic import BaseModel, Field

from retrieval.auth import (
    AuthorizationError,
    GATEWAY_CONTEXT_HEADER,
    GATEWAY_REQUEST_ID_HEADER,
    GraphGroupResolver,
    Principal,
    principal_from_gateway,
    parse_gateway_request_id,
)
from retrieval.catalog import CatalogError, RequestPolicy, RuntimeCatalogLoader
from retrieval.config import RetrievalConfig, load_retrieval_config
from retrieval.cosmos import RetrievalMode, RetrievedChunk
from retrieval.cosmos_registry import CosmosRegistry, build_cosmos_registry, load_cosmos_instance_configs
from retrieval.pipeline import (
    RetrievalDependencyError,
    citation_label,
    citation_location,
    citation_url,
)
from retrieval.service import (
    AnswerCitationError, ContentFilteredError, RagService, UnknownScoringProfileError,
    accept_answer, suppress_content_filter_retry, suppress_content_filter_retry_async,
)
from retrieval.runtime_catalog import RuntimeCatalogProvider
from retrieval.telemetry import CATALOG_LOGGER_NAME, catalog_event_emitter, write_audit_records

try:
    from retrieval.agent import create_rag_agent
    from retrieval.tools import make_search_tool
    _AGENT_AVAILABLE = True
except ImportError:
    _AGENT_AVAILABLE = False

logger = structlog.get_logger()


class _TokenRefreshAuth(httpx.Auth):
    """httpx auth handler that refreshes MI tokens before expiry."""

    def __init__(self, credential: ManagedIdentityCredential) -> None:
        self._credential = credential
        self._scope = "https://graph.microsoft.com/.default"

    def auth_flow(self, request: httpx.Request):
        token = self._credential.get_token(self._scope)
        request.headers["Authorization"] = f"Bearer {token.token}"
        yield request


class _AppState:
    config: RetrievalConfig
    registry: CosmosRegistry
    credential: ManagedIdentityCredential
    openai_client: AzureOpenAI
    rag_service: RagService
    group_resolver: GraphGroupResolver
    audit_container: Any
    agent_chat_client: Any
    catalog_provider: RuntimeCatalogProvider


_state = _AppState()


def _configure_tracing(config: RetrievalConfig, *, credential: ManagedIdentityCredential | None = None) -> None:
    """Disable GenAI capture and configure optional Azure Monitor telemetry."""
    if _AGENT_AVAILABLE:
        from agent_framework.observability import disable_instrumentation

        disable_instrumentation()
    if not config.app_insights_connection_string:
        logger.info("tracing_not_configured", reason="APPLICATIONINSIGHTS_CONNECTION_STRING unset")
        return
    try:
        from azure.monitor.opentelemetry import configure_azure_monitor

        configure_azure_monitor(
            connection_string=config.app_insights_connection_string,
            logger_name=CATALOG_LOGGER_NAME,
            credential=credential,
        )
        logger.info("tracing_configured")
    except Exception:
        logger.warning("tracing_configuration_failed")


@asynccontextmanager
async def _lifespan(app: FastAPI):
    resources = ExitStack()
    async_resources = AsyncExitStack()
    provider: RuntimeCatalogProvider | None = None
    try:
        config = load_retrieval_config()
        credential = ManagedIdentityCredential(client_id=config.managed_identity_client_id)
        resources.callback(_close_resource, credential)
        _configure_tracing(config, credential=credential)

        instance_configs = load_cosmos_instance_configs(
            default_source_id=os.getenv("INGESTION_SOURCE_ID", "default"),
            default_endpoint=config.cosmos_endpoint,
            default_database=config.cosmos_database,
            default_chunks_container=config.cosmos_chunks_container,
            default_manifests_container=config.cosmos_manifests_container,
        )
        registry = build_cosmos_registry(instance_configs, credential, acl_enabled=config.acl_enabled)
        resources.callback(_close_resource, registry)

        cosmos = CosmosClient(url=config.cosmos_endpoint, credential=credential)
        resources.callback(_close_resource, cosmos)
        db = cosmos.get_database_client(config.cosmos_database)
        audit_container = db.get_container_client(config.cosmos_audit_container)

        _state.credential = credential
        _state.registry = registry
        _state.audit_container = audit_container

        def _openai_token_provider() -> str:
            return credential.get_token("https://cognitiveservices.azure.com/.default").token

        openai_http_client = DefaultHttpxClient(event_hooks={"response": [suppress_content_filter_retry]})
        resources.callback(openai_http_client.close)
        _state.openai_client = AzureOpenAI(
            azure_endpoint=config.openai_endpoint,
            azure_ad_token_provider=_openai_token_provider,
            api_version=config.openai_api_version,
            max_retries=2,
            http_client=openai_http_client,
        )
        resources.callback(_close_resource, _state.openai_client)

        if _AGENT_AVAILABLE:
            try:
                from agent_framework.openai import OpenAIChatClient

                async def _agent_token_provider() -> str:
                    return await asyncio.to_thread(_openai_token_provider)

                _state.agent_chat_client = OpenAIChatClient(
                    model=config.chat_deployment,
                    base_url=f"{config.openai_endpoint.rstrip('/')}/openai/v1",
                    api_version=config.agent_api_version,
                    api_key=_agent_token_provider,
                )
                original_client = _state.agent_chat_client.client
                async_resources.push_async_callback(original_client.close)
                agent_http_client = DefaultAsyncHttpxClient(
                    event_hooks={"response": [suppress_content_filter_retry_async]},
                )
                async_resources.push_async_callback(agent_http_client.aclose)
                _state.agent_chat_client.client = original_client.with_options(http_client=agent_http_client)
            except Exception:
                logger.warning("agent_chat_client_init_failed")
                _state.agent_chat_client = None
        else:
            _state.agent_chat_client = None

        catalog_container = db.get_container_client(config.catalog_container)
        provider = RuntimeCatalogProvider(
            RuntimeCatalogLoader(catalog_container, config.deployment_instance_id),
            config.catalog_poll_seconds,
            emit=catalog_event_emitter(
                os.getenv("CONTAINER_APP_REVISION", ""),
                os.getenv("CONTAINER_APP_REPLICA_NAME", ""),
                application=os.getenv("CONTAINER_APP_NAME", ""),
                deployment_instance_hash=hashlib.sha256(config.deployment_instance_id.encode("utf-8")).hexdigest(),
            ),
        )
        await provider.start()
        _state.catalog_provider = provider
        _state.config = config

        _state.rag_service = RagService(
            _state.openai_client,
            _state.registry,
            config.embedding_deployment,
            config.chat_deployment,
            retrieval_timeout_seconds=config.retrieval_timeout_seconds,
            generation_timeout_seconds=config.generation_timeout_seconds,
            max_evidence=config.max_evidence_chunks,
            max_planned_queries=config.max_planned_queries,
            acl_enabled=config.acl_enabled,
            catalog_provider=provider,
        )
        resources.callback(_close_resource, _state.rag_service)
        graph_client = httpx.Client(
            auth=_TokenRefreshAuth(credential), timeout=config.graph_group_timeout_seconds,
        )
        resources.callback(_close_resource, graph_client)
        _state.group_resolver = GraphGroupResolver(graph_client)

        logger.info(
            "retrieval_service_started",
            cosmos_endpoint=config.cosmos_endpoint,
            cosmos_instances=len(registry),
            acl_enabled=config.acl_enabled,
            catalog_version=provider.snapshot.digest,
            catalog_etag=provider.snapshot.etag,
        )
        yield
    finally:
        try:
            if provider is not None:
                await provider.close()
        finally:
            try:
                await async_resources.aclose()
            finally:
                resources.close()
        logger.info("retrieval_service_stopped")


def _close_resource(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception:
        logger.warning("retrieval_resource_close_failed", resource=type(resource).__name__)


app = FastAPI(title="RAG Retrieval Agent", lifespan=_lifespan)


def _error_request_id(request: Request) -> str:
    values = request.headers.getlist(GATEWAY_REQUEST_ID_HEADER)
    if len(values) == 1:
        try:
            return parse_gateway_request_id(values[0])
        except AuthorizationError:
            pass
    return str(uuid.uuid4())


def _error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {"code": code, "message": message},
            "request_id": _error_request_id(request),
        },
    )


@app.middleware("http")
async def _query_deadline(request: Request, call_next):
    if request.url.path != "/api/query":
        return await call_next(request)
    config = getattr(_state, "config", None)
    timeout_seconds = getattr(config, "operation_timeout_seconds", 27.0)
    try:
        async with asyncio.timeout(timeout_seconds):
            return await call_next(request)
    except TimeoutError:
        return _error_response(
            request,
            status_code=504,
            code="operation_timeout",
            message="The retrieval operation timed out.",
        )


@app.exception_handler(UnknownScoringProfileError)
async def _unknown_scoring_profile_handler(
    request: Request, error: UnknownScoringProfileError,
) -> JSONResponse:
    return _error_response(
        request,
        status_code=400,
        code="unknown_scoring_profile",
        message="The requested scoring profile is unavailable.",
    )


@app.exception_handler(RetrievalDependencyError)
async def _retrieval_dependency_handler(
    request: Request, error: RetrievalDependencyError,
) -> JSONResponse:
    return _error_response(
        request,
        status_code=503,
        code="retrieval_dependency_unavailable",
        message="Retrieval is temporarily unavailable.",
    )


@app.exception_handler(AnswerCitationError)
async def _answer_citation_handler(
    request: Request, error: AnswerCitationError,
) -> JSONResponse:
    return _error_response(
        request,
        status_code=502,
        code="answer_citation_invalid",
        message="The generated answer could not be validated.",
    )


@app.exception_handler(ContentFilteredError)
async def _content_filtered_handler(
    request: Request, error: ContentFilteredError,
) -> JSONResponse:
    return _error_response(
        request,
        status_code=422,
        code="content_filtered",
        message="The request could not be completed under the content safety policy.",
    )


@app.exception_handler(RequestValidationError)
async def _request_validation_handler(
    request: Request, error: RequestValidationError,
) -> JSONResponse:
    return _error_response(
        request,
        status_code=422,
        code="invalid_request",
        message="The request is invalid.",
    )


@app.exception_handler(HTTPException)
async def _http_exception_handler(
    request: Request, error: HTTPException,
) -> JSONResponse:
    code = str(error.detail) if isinstance(error.detail, str) else "request_failed"
    message = (
        "Authentication is required."
        if error.status_code in (401, 403)
        else "The request could not be completed."
    )
    return _error_response(
        request,
        status_code=error.status_code,
        code=code,
        message=message,
    )

_RATE_LIMIT_RPM = int(os.getenv("RATE_LIMIT_RPM", "30"))
_RATE_LIMIT_WINDOW = 60.0


class _SlidingWindowLimiter:
    """Per-user sliding window rate limiter (in-memory, per-instance)."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self._max = max_requests
        self._window = window_seconds
        self._lock = threading.Lock()
        self._requests: dict[str, collections.deque] = {}

    def is_allowed(self, user_id: str) -> bool:
        now = time.monotonic()
        with self._lock:
            window = self._requests.setdefault(user_id, collections.deque())
            while window and window[0] <= now - self._window:
                window.popleft()
            if len(window) >= self._max:
                return False
            window.append(now)
            # Evict users with no recent requests to bound memory
            if len(self._requests) > 10_000:
                stale = [k for k, v in self._requests.items() if not v]
                for k in stale:
                    del self._requests[k]
            return True


_rate_limiter = _SlidingWindowLimiter(_RATE_LIMIT_RPM, _RATE_LIMIT_WINDOW)


class QueryRequest(BaseModel):
    question: str = Field(min_length=1, max_length=4000)
    history: list[dict[str, str]] = Field(default_factory=list)
    mode: str = Field(default="hybrid", pattern="^(hybrid|vector|full_text)$")
    top_k: int | None = Field(default=None, ge=1, le=20)
    scoring_profile: str | None = Field(default=None, max_length=200)
    expand_synonyms: bool | None = Field(default=None)


class Citation(BaseModel):
    ref: str
    source_name: str
    location: str
    url: str


class QueryResponse(BaseModel):
    answer: str
    citations: list[Citation]
    request_id: str


def _citation_from_result(index: int, chunk: Mapping[str, Any]) -> Citation:
    return Citation(
        ref=citation_label(index),
        source_name=str(chunk["source_name"]),
        location=citation_location(chunk),
        url=citation_url(chunk),
    )


@app.post("/api/query", response_model=QueryResponse)
async def query(request: Request, body: QueryRequest, background_tasks: BackgroundTasks):
    principal = await asyncio.to_thread(_resolve_principal, request)
    request_id_headers = request.headers.getlist(GATEWAY_REQUEST_ID_HEADER)
    if len(request_id_headers) != 1:
        raise HTTPException(status_code=401, detail="invalid_gateway_request_id")
    try:
        request_id = parse_gateway_request_id(request_id_headers[0])
    except AuthorizationError as error:
        raise HTTPException(status_code=401, detail=str(error)) from error
    start = time.perf_counter()
    log = logger.bind(request_id=request_id)

    if not _rate_limiter.is_allowed(principal.user_id):
        raise HTTPException(status_code=429, detail="rate_limit_exceeded")

    mode = RetrievalMode(body.mode)
    policy = _state.rag_service.capture_policy(body.scoring_profile, body.expand_synonyms)

    queries, planning_usage = await asyncio.to_thread(
        _state.rag_service.plan_queries, body.question, body.history or None,
    )

    use_agent = len(queries) >= 2 and _state.agent_chat_client is not None and _AGENT_AVAILABLE
    path = "standard"

    if use_agent:
        log.info("query_start", user_id=principal.user_id, mode=body.mode, path="agentic",
                 planned_queries=len(queries))
        result = await _run_agentic_path(
            body.question, principal, mode, planning_usage, log,
            body.scoring_profile,
            body.expand_synonyms,
            body.top_k,
            policy=policy,
        )
        if result is not None:
            path = "agentic"
        else:
            path = "agentic_fallback"
            log.warning("agentic_path_fallback")

    if not use_agent or result is None:
        log.info("query_start", user_id=principal.user_id, mode=body.mode, path=path,
                 planned_queries=len(queries))
        result = await asyncio.to_thread(
            _state.rag_service.answer_with_queries,
            body.question, queries, principal, mode, planning_usage,
            body.top_k,
            body.scoring_profile,
            body.expand_synonyms,
            policy=policy,
        )

    background_tasks.add_task(
        write_audit_records,
        _state.audit_container,
        request_id,
        principal.user_id,
        principal.tenant_id,
        body.mode,
        result.get("usage", []),
    )

    elapsed = time.perf_counter() - start
    log.info("query_complete", latency_ms=int(elapsed * 1000),
             chunks=len(result["citations"]), path=path)

    answer_text = result["answer"]
    background_tasks.add_task(
        _write_query_summary,
        _state.audit_container,
        request_id, principal.user_id, principal.tenant_id,
        len(result["citations"]),
        path, body.mode, len(queries), int(elapsed * 1000),
        policy.snapshot.digest,
        policy.profile.name if policy.profile is not None else None,
        policy.expander.map_name if policy.expander is not None else None,
        any(record.get("degraded") is True or record.get("retrieval_degraded") is True
            for record in result.get("usage", [])),
        policy.snapshot.etag,
        policy.snapshot.operation_id,
    )

    if not _state.config.include_citations:
        answer_text = re.sub(r"[ \t]*\[S[1-9][0-9]*\]", "", answer_text).strip()

    return QueryResponse(
        answer=answer_text,
        citations=[
            _citation_from_result(i, c)
            for i, c in enumerate(result["citations"], start=1)
        ] if _state.config.include_citations else [],
        request_id=request_id,
    )


def _write_query_summary(
    container: Any, request_id: str, user_id: str, tenant_id: str,
    citations_count: int,
    path: str, mode: str, planned_queries: int, e2e_latency_ms: int,
    catalog_version: str | None, scoring_profile: str | None,
    synonym_map: str | None, retrieval_degraded: bool,
    catalog_etag: str | None = None, catalog_operation_id: str | None = None,
) -> None:
    from retrieval.telemetry import write_audit_records
    write_audit_records(container, request_id, user_id, tenant_id, mode, [{
        "operation": "query_request",
        "citations_count": citations_count,
        "path": path,
        "planned_queries": planned_queries,
        "e2e_latency_ms": e2e_latency_ms,
        "catalog_version": catalog_version,
        "catalog_etag": catalog_etag,
        "catalog_operation_id": catalog_operation_id,
        "scoring_profile": scoring_profile,
        "synonym_map": synonym_map,
        "retrieval_degraded": retrieval_degraded,
    }])


async def _run_agentic_path(
    question: str,
    principal: Principal,
    mode: RetrievalMode,
    planning_usage: list[dict[str, Any]],
    log: Any,
    scoring_profile: str | None = None,
    expand_synonyms: bool | None = None,
    top_k: int | None = None,
    *,
    policy: RequestPolicy | None = None,
) -> dict[str, Any] | None:
    """Run the Agent Framework agent. Returns None on timeout/error (caller falls back)."""
    # Resolved outside the try/except so a config error (unknown profile) fails fast
    # instead of routing silently through the standard-path fallback.
    policy = policy or _state.rag_service.capture_policy(scoring_profile, expand_synonyms)
    try:
        agent_deadline = time.monotonic() + _state.config.agent_timeout_seconds
        retrieved_chunks: list[RetrievedChunk] = []
        agentic_usage: list[dict[str, Any]] = []
        search_tool = make_search_tool(
            rag_service=_state.rag_service,
            principal=principal,
            retrieved_chunks=retrieved_chunks,
            usage=agentic_usage,
            scoring_profile=scoring_profile,
            max_evidence=top_k or _state.config.max_evidence_chunks,
            expand_synonyms=expand_synonyms,
            forced_mode=mode,
            deadline_monotonic=agent_deadline,
            policy=policy,
        )
        agent = create_rag_agent(
            _state.agent_chat_client, search_tool, model=_state.config.chat_deployment,
        )
        agent_start = time.perf_counter()
        response = await asyncio.wait_for(
            agent.run(question),
            timeout=_state.config.agent_timeout_seconds,
        )
        agent_latency_ms = int((time.perf_counter() - agent_start) * 1000)
        answer_text = str(response)
        if not answer_text.strip():
            log.warning("agent_empty_response")
            return None

        answer_text, retrieved_chunks = accept_answer(answer_text, retrieved_chunks)

        # Opportunistically capture agent LLM token usage
        agent_usage = getattr(response, "usage", None)
        if agent_usage:
            agentic_usage.append({
                "operation": "agent_generation",
                "model": _state.config.chat_deployment,
                "prompt_tokens": getattr(agent_usage, "prompt_tokens", 0),
                "completion_tokens": getattr(agent_usage, "completion_tokens", 0),
                "latency_ms": agent_latency_ms,
            })

        return {
            "answer": answer_text.strip(),
            "citations": [asdict(c) for c in retrieved_chunks],
            "usage": planning_usage + agentic_usage,
            "policy": policy.metadata(),
        }
    except asyncio.TimeoutError:
        log.warning("agent_timeout", timeout=_state.config.agent_timeout_seconds)
        return None
    except (RetrievalDependencyError, AnswerCitationError, ContentFilteredError):
        raise
    except Exception:
        log.warning("agent_error")
        return None


@app.get("/health/live")
async def health_live():
    return {"status": "alive"}


@app.get("/health/ready")
async def health_ready():
    try:
        _source_id, retriever = _state.registry.items()[0]
        items = retriever._chunks.query_items(
            query="SELECT TOP 1 c.id FROM c",
            enable_cross_partition_query=True,
            max_item_count=1,
        )
        await asyncio.to_thread(lambda: list(items))
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="cosmos_unavailable")


def _resolve_principal(request: Request) -> Principal:
    service_headers = request.headers.getlist("X-MS-CLIENT-PRINCIPAL")
    context_headers = request.headers.getlist(GATEWAY_CONTEXT_HEADER)
    if len(service_headers) != 1 or len(context_headers) != 1:
        raise HTTPException(status_code=401, detail="invalid_gateway_headers")
    try:
        return principal_from_gateway(
            service_headers[0],
            context_headers[0],
            expected_tenant_id=_state.config.tenant_id,
            expected_audience=_state.config.retrieval_audience,
            expected_gateway_client_id=_state.config.gateway_client_id,
            expected_gateway_principal_id=_state.config.gateway_principal_id,
            group_resolver=_state.group_resolver,
            acl_enabled=_state.config.acl_enabled,
        )
    except AuthorizationError as e:
        raise HTTPException(status_code=401, detail=str(e))
