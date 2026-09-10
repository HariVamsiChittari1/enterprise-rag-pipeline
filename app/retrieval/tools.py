"""Agent Framework function tools for ACL-filtered knowledge base retrieval."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Annotated, Any

from retrieval.auth import Principal
from retrieval.catalog import RequestPolicy
from retrieval.cosmos import RetrievalMode, RetrievedChunk
from retrieval.pipeline import (
    citation_label,
    citation_source,
    citation_url,
    evidence_identity,
)
from retrieval.service import RagService, _sanitize_chunk


def make_search_tool(
    rag_service: RagService,
    principal: Principal,
    retrieved_chunks: list[RetrievedChunk],
    *,
    usage: list[dict[str, Any]] | None = None,
    scoring_profile: str | None = None,
    max_evidence: int = 5,
    expand_synonyms: bool | None = None,
    forced_mode: RetrievalMode | None = None,
    now: datetime | None = None,
    deadline_monotonic: float | None = None,
    policy: RequestPolicy | None = None,
):
    """Create a search tool closure bound to the caller's ACL.

    RagService owns retrieval behavior. This adapter only applies the agent's cumulative
    evidence cap, stable citation labels, usage forwarding, and tool-text formatting.
    """
    policy = policy or rag_service.capture_policy(scoring_profile, expand_synonyms)

    async def search_knowledge_base(
        query: Annotated[str, "Search query describing what information to find"],
        mode: Annotated[str, "Retrieval mode: hybrid, vector, or full_text"] = "hybrid",
    ) -> str:
        """Search the enterprise knowledge base for documents the caller is authorized to access.

        Returns the top relevant chunks with source attribution. Use this tool
        when the user asks a factual question that requires grounding from
        indexed documents.
        """
        retrieval_mode = forced_mode or (
            RetrievalMode(mode) if mode in RetrievalMode.__members__.values() else RetrievalMode.HYBRID
        )
        tool_start = time.perf_counter()
        remaining = (
            deadline_monotonic - time.monotonic()
            if deadline_monotonic is not None
            else None
        )
        if remaining is not None and remaining <= 0:
            raise asyncio.TimeoutError("agent retrieval deadline exceeded")
        operation = asyncio.to_thread(
            rag_service.search,
            query,
            [query],
            principal,
            mode=retrieval_mode,
            top_k=max_evidence,
            scoring_profile=scoring_profile,
            expand_synonyms=expand_synonyms,
            now=now,
            policy=policy,
        )
        result = (
            await asyncio.wait_for(operation, timeout=remaining)
            if remaining is not None
            else await operation
        )
        chunks = list(result.chunks)
        existing = {
            evidence_identity(chunk): index
            for index, chunk in enumerate(retrieved_chunks, start=1)
        }
        labeled_chunks: list[tuple[str, RetrievedChunk]] = []
        for chunk in chunks:
            identity = evidence_identity(chunk)
            if identity in existing:
                labeled_chunks.append((citation_label(existing[identity]), chunk))
                continue
            if len(retrieved_chunks) >= max_evidence:
                continue
            retrieved_chunks.append(chunk)
            index = len(retrieved_chunks)
            existing[identity] = index
            labeled_chunks.append((citation_label(index), chunk))

        if usage is not None:
            usage.extend(result.usage)
            usage.append({
                "operation": "tool_invocation",
                "tool": "search_knowledge_base",
                "retrieval_mode": retrieval_mode.value,
                "model": None,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "chunks_returned": len(chunks),
                "scoring_profile": policy.profile.name if policy.profile is not None else None,
                "latency_ms": int((time.perf_counter() - tool_start) * 1000),
            })

        if not labeled_chunks:
            return "No authorized documents found matching this query."

        return "\n\n".join(
            f"{label} {citation_source(chunk)} ({citation_url(chunk)})\n{_sanitize_chunk(chunk.content)}"
            for label, chunk in labeled_chunks
        )

    return search_knowledge_base
