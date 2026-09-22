"""ACL-filtered Cosmos DB retrieval and publication validation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Iterable, Mapping

from azure.cosmos.exceptions import CosmosResourceNotFoundError


class RetrievalMode(StrEnum):
    HYBRID = "hybrid"
    VECTOR = "vector"
    FULL_TEXT = "full_text"


class RetrievalLocatorKind(StrEnum):
    PAGE = "page"
    SECTION = "section"
    SLIDE = "slide"
    WORKSHEET = "worksheet"
    TIME = "time"


# Reranker input cap; matches AI Search's semantic-ranker top-50 rerank cap.
MAX_CANDIDATE_POOL_TOTAL = 50
SOURCE_DOCUMENT_SCHEMA_VERSION = 1
SOURCE_DOCUMENT_RECORD_TYPE = "source_document"
_AUDIO_MIME_TYPES = ("audio/wav", "audio/mpeg", "audio/flac")


@dataclass(frozen=True)
class RetrievedChunk:
    chunk_id: str
    document_id: str
    content: str
    source_name: str
    source_url: str
    locator_kind: RetrievalLocatorKind
    locator_label: str
    locator_ordinal_start: int
    locator_ordinal_end: int
    source_modified_at: str | None = None
    start_ms: int | None = None
    end_ms: int | None = None
    evidence_version: str | None = None


_PROJECTION = (
    "c.id, c.schemaVersion, c.documentId, c.sourceRunId, c.content, "
    "c.sourceName, c.sourceUrl, c.locatorKind, c.locatorLabel, "
    "c.locatorOrdinalStart, c.locatorOrdinalEnd, c.sourceModifiedAt, "
    "c.sectionPath, c.keyPhrases, c.createdAt, c.lifecycleGeneration, "
    "c.documentKey, c.audio, c.startMs, c.endMs"
)
_ACL_FILTER = (
    "EXISTS(SELECT VALUE gid FROM gid IN c.allowedGroupIds "
    "WHERE ARRAY_CONTAINS(@principalIds, gid))"
)
_RETRIEVABLE_FILTER = "c.isRetrievable = true"
_DOCUMENT_FILTER = (
    "(c.schemaVersion = 1 AND "
    "c.locatorKind IN ('page', 'section', 'slide', 'worksheet'))"
)
_AUDIO_FILTER = "(c.schemaVersion = 1 AND c.locatorKind = 'time')"
# Positional weight order MUST match the argument order of the two RRF scoring
# functions below: index 0 = VectorDistance (vector weight), index 1 = FullTextScore
# (BM25 weight). See https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/rrf.
_HYBRID_RRF_WEIGHTED = (
    "ORDER BY RANK RRF(VectorDistance(c.embedding, @embedding), "
    "FullTextScore(c.searchableText, @searchText), @rrfWeights)"
)
_HYBRID_RRF_UNWEIGHTED = (
    "ORDER BY RANK RRF(VectorDistance(c.embedding, @embedding), "
    "FullTextScore(c.searchableText, @searchText))"
)
_ORDER_BY_VECTOR = "ORDER BY VectorDistance(c.embedding, @embedding)"
_ORDER_BY_FULL_TEXT = "ORDER BY RANK FullTextScore(c.searchableText, @searchText)"

# Multi-FullTextScore synonym term cap; matches SynonymExpander's ceiling and bounds
# the SDK-verified RRF-fused pattern below.
_MAX_TERMS_PER_QUERY = 8

# FULLTEXTSCORE takes single keyword arguments, not phrases: a multi-word argument is
# treated as one term and matches almost nothing, so multi-word queries must be
# tokenized into keywords for lexical (full-text and hybrid) retrieval to work.
_KEYWORD_PATTERN = re.compile(r"[a-z0-9]+")
_STOPWORDS = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "by", "do", "does", "for", "from",
    "how", "in", "is", "it", "of", "on", "or", "that", "the", "this", "to", "was",
    "what", "which", "who", "why", "with",
})


def _tokenize_keywords(text: str) -> list[str]:
    keywords: list[str] = []
    seen: set[str] = set()
    for token in _KEYWORD_PATTERN.findall(text.casefold()):
        if len(token) < 2 or token in _STOPWORDS or token in seen:
            continue
        seen.add(token)
        keywords.append(token)
    return keywords


def _normalize_search_terms(search_terms: list[str] | None, fallback_query: str) -> list[str]:
    sources = search_terms if search_terms else [fallback_query]
    if not all(isinstance(term, str) and term.strip() for term in sources):
        raise ValueError("search_terms_invalid")
    keywords: list[str] = []
    seen: set[str] = set()
    for source in sources:
        for keyword in _tokenize_keywords(source):
            if keyword in seen:
                continue
            seen.add(keyword)
            keywords.append(keyword)
            if len(keywords) >= _MAX_TERMS_PER_QUERY:
                return keywords
    # A query of only stopwords/short tokens yields no keywords; fall back to the
    # trimmed raw query so FULLTEXTSCORE still receives one valid term.
    return keywords or [fallback_query.strip()]


def _build_full_text_score(term_count: int) -> str:
    parameters = ", ".join(f"@t{i}" for i in range(term_count))
    return f"FullTextScore(c.searchableText, {parameters})"


def _build_hybrid_multi_term_clause(term_count: int, *, weighted: bool) -> str:
    full_text_score = _build_full_text_score(term_count)
    weights_arg = ", @rrfWeights" if weighted else ""
    return (
        f"ORDER BY RANK RRF(VectorDistance(c.embedding, @embedding), "
        f"{full_text_score}{weights_arg})"
    )


def _build_full_text_multi_term_clause(term_count: int) -> str:
    return f"ORDER BY RANK {_build_full_text_score(term_count)}"


class SecureCosmosRetriever:
    def __init__(
        self, chunks: Any, manifests: Any, *, acl_enabled: bool = True,
        audio_retrieval_enabled: bool = False,
        audio_max_acl_age_seconds: int | None = None,
        audio_max_source_age_seconds: int | None = None,
    ) -> None:
        if audio_retrieval_enabled and (
            not acl_enabled
            or any(type(value) is not int or value <= 0 for value in (
                audio_max_acl_age_seconds, audio_max_source_age_seconds,
            ))
        ):
            raise ValueError("audio_requires_acl_and_freshness_limits")
        self._chunks = chunks
        self._manifests = manifests
        self._acl_enabled = acl_enabled
        self._audio_enabled = audio_retrieval_enabled
        self._audio_max_acl_age_seconds = audio_max_acl_age_seconds
        self._audio_max_source_age_seconds = audio_max_source_age_seconds

    def retrieve(
        self,
        query_text: str,
        embedding: list[float],
        principal_ids: list[str],
        *,
        mode: RetrievalMode = RetrievalMode.HYBRID,
        top_k: int = 5,
        over_fetch_factor: int = 1,
        rrf_weights: tuple[float, float] | None = None,
        full_text_score_scope: str | None = None,
        raw: bool = False,
        search_terms: list[str] | None = None,
    ) -> list[RetrievedChunk] | list[dict[str, Any]]:
        if not query_text.strip():
            raise ValueError("query_text_required")
        if self._acl_enabled and not principal_ids:
            raise ValueError("principal_ids_required")
        if top_k < 1 or top_k > 50:
            raise ValueError("top_k_out_of_range")
        if over_fetch_factor < 1:
            raise ValueError("over_fetch_factor_out_of_range")
        if mode in {RetrievalMode.HYBRID, RetrievalMode.VECTOR} and not embedding:
            raise ValueError("embedding_required")
        if full_text_score_scope is not None and full_text_score_scope not in ("Local", "Global"):
            raise ValueError("full_text_score_scope_invalid")
        if rrf_weights is not None and mode is not RetrievalMode.HYBRID:
            raise ValueError("rrf_weights_only_valid_for_hybrid")
        effective_terms = _normalize_search_terms(search_terms, query_text)

        effective_top = min(top_k * over_fetch_factor, MAX_CANDIDATE_POOL_TOTAL)

        parameters: list[dict[str, Any]] = [
            {"name": "@topK", "value": effective_top},
        ]
        media_filter = (
            f"({_DOCUMENT_FILTER} OR {_AUDIO_FILTER})"
            if self._audio_enabled else _DOCUMENT_FILTER
        )
        filters = [_RETRIEVABLE_FILTER, media_filter]
        if self._acl_enabled:
            filters.append(_ACL_FILTER)
            parameters.append(
                {"name": "@principalIds", "value": sorted(set(principal_ids))}
            )
        where_clause = f"WHERE {' AND '.join(filters)} "

        if mode is RetrievalMode.HYBRID:
            parameters.append({"name": "@embedding", "value": embedding})
            if len(effective_terms) <= 1:
                parameters.append({"name": "@searchText", "value": effective_terms[0]})
                if rrf_weights is not None:
                    parameters.append({"name": "@rrfWeights", "value": list(rrf_weights)})
                    order_clause = _HYBRID_RRF_WEIGHTED
                else:
                    order_clause = _HYBRID_RRF_UNWEIGHTED
            else:
                for i, term in enumerate(effective_terms):
                    parameters.append({"name": f"@t{i}", "value": term})
                if rrf_weights is not None:
                    parameters.append({"name": "@rrfWeights", "value": list(rrf_weights)})
                    order_clause = _build_hybrid_multi_term_clause(len(effective_terms), weighted=True)
                else:
                    order_clause = _build_hybrid_multi_term_clause(len(effective_terms), weighted=False)
        elif mode is RetrievalMode.VECTOR:
            parameters.append({"name": "@embedding", "value": embedding})
            order_clause = _ORDER_BY_VECTOR
        else:
            if len(effective_terms) <= 1:
                parameters.append({"name": "@searchText", "value": effective_terms[0]})
                order_clause = _ORDER_BY_FULL_TEXT
            else:
                for i, term in enumerate(effective_terms):
                    parameters.append({"name": f"@t{i}", "value": term})
                order_clause = _build_full_text_multi_term_clause(len(effective_terms))

        query = (
            f"SELECT TOP @topK {_PROJECTION} FROM c {where_clause}"
            f"{order_clause}"
        )
        query_kwargs: dict[str, Any] = {
            "query": query,
            "parameters": parameters,
            "enable_cross_partition_query": True,
        }
        if full_text_score_scope is not None:
            query_kwargs["full_text_score_scope"] = full_text_score_scope
        candidates = self._chunks.query_items(**query_kwargs)
        materialized: list[dict[str, Any]] = []
        for candidate in candidates:
            manifest = self._active_manifest(candidate, principal_ids)
            if manifest is None:
                continue
            manifest_source_name = manifest.get("sourceName")
            enriched = dict(candidate)
            if isinstance(manifest_source_name, str) and manifest_source_name:
                enriched["sourceName"] = manifest_source_name
            try:
                _to_chunk(enriched)
            except ValueError:
                continue
            materialized.append(enriched)
        if raw:
            return materialized[:effective_top]
        return [_to_chunk(candidate) for candidate in materialized[:top_k]]

    def to_chunks(self, candidates: Iterable[Mapping[str, Any]]) -> list[RetrievedChunk]:
        return [_to_chunk(candidate) for candidate in candidates]

    def _active_manifest(
        self, candidate: dict[str, Any], principal_ids: list[str],
    ) -> dict[str, Any] | None:
        document_id = candidate.get("documentId")
        source_run_id = candidate.get("sourceRunId")
        if not isinstance(document_id, str) or not isinstance(source_run_id, str):
            return None
        try:
            manifest = self._manifests.read_item(
                item=document_id,
                partition_key=source_run_id,
            )
        except CosmosResourceNotFoundError:
            return None
        schema_version = candidate.get("schemaVersion")
        if (
            type(schema_version) is not int
            or schema_version != SOURCE_DOCUMENT_SCHEMA_VERSION
            or type(manifest.get("schemaVersion")) is not int
            or manifest.get("schemaVersion") != schema_version
            or manifest.get("recordType") != SOURCE_DOCUMENT_RECORD_TYPE
            or manifest.get("status") != "ready"
        ):
            return None
        if candidate.get("locatorKind") != RetrievalLocatorKind.TIME:
            if manifest.get("audio") is not None or manifest.get("mimeType") in _AUDIO_MIME_TYPES:
                return None
            return manifest
        if not self._audio_enabled:
            return None
        audio = candidate.get("audio")
        generation = candidate.get("lifecycleGeneration")
        manifest_groups = manifest.get("allowedGroupIds")
        if (
            not isinstance(audio, dict)
            or manifest.get("mimeType") not in _AUDIO_MIME_TYPES
            or manifest.get("audio") != audio
            or manifest.get("eTag") != audio.get("sourceVersion")
            or manifest.get("contentHash") != audio.get("sourceContentHash")
            or manifest.get("documentId") != document_id
            or manifest.get("sourceRunId") != source_run_id
            or not isinstance(candidate.get("documentKey"), str)
            or not candidate["documentKey"]
            or manifest.get("documentKey") != candidate["documentKey"]
            or type(generation) is not int or generation < 0
            or type(manifest.get("lifecycleGeneration")) is not int
            or manifest.get("lifecycleGeneration") != generation
            or not isinstance(manifest_groups, list)
            or not all(isinstance(group, str) for group in manifest_groups)
            or not set(principal_ids).intersection(manifest_groups)
        ):
            return None
        now = datetime.now(timezone.utc)
        if not _is_fresh(manifest.get("aclEvaluatedAt"), self._audio_max_acl_age_seconds, now):
            return None
        if not _is_fresh(manifest.get("sourceVerifiedAt"), self._audio_max_source_age_seconds, now):
            return None
        return manifest


def _is_fresh(value: Any, maximum_age: int | None, now: datetime) -> bool:
    if not isinstance(value, str) or maximum_age is None:
        return False
    try:
        verified = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    if verified.tzinfo is None or verified.utcoffset() != timezone.utc.utcoffset(verified):
        return False
    return 0 <= (now - verified).total_seconds() <= maximum_age


def _to_chunk(candidate: Mapping[str, Any]) -> RetrievedChunk:
    schema_version = candidate.get("schemaVersion")
    if type(schema_version) is not int or schema_version != SOURCE_DOCUMENT_SCHEMA_VERSION:
        raise ValueError("invalid_retrieval_record")
    source_url = candidate.get("sourceUrl") or ""
    source_modified_at = candidate.get("sourceModifiedAt")
    values = {
        "chunk_id": candidate.get("id"),
        "document_id": candidate.get("documentId"),
        "content": candidate.get("content"),
        "source_name": candidate.get("sourceName"),
        "source_url": source_url,
        "locator_label": candidate.get("locatorLabel"),
        "locator_ordinal_start": candidate.get("locatorOrdinalStart"),
        "locator_ordinal_end": candidate.get("locatorOrdinalEnd"),
    }
    try:
        locator_kind = RetrievalLocatorKind(candidate.get("locatorKind"))
    except (TypeError, ValueError) as error:
        raise ValueError("invalid_retrieval_record") from error
    temporal: dict[str, Any] = {}
    if locator_kind is RetrievalLocatorKind.TIME:
        audio = candidate.get("audio")
        start_ms, end_ms = candidate.get("startMs"), candidate.get("endMs")
        if (
            not isinstance(audio, dict)
            or type(audio.get("durationMs")) is not int
            or not 0 < audio["durationMs"] <= 1_800_000
            or (audio.get("channelCount") is not None
                and (type(audio.get("channelCount")) is not int or audio["channelCount"] not in (1, 2)))
            or not isinstance(audio.get("locale"), str)
            or re.fullmatch(r"en-[A-Z]{2}", audio["locale"]) is None
            or not (
                (audio.get("mode") in ("fast", "enhanced") and audio.get("apiVersion") == "2025-10-15")
                or (audio.get("mode") == "batch" and audio.get("apiVersion") == "2024-11-15")
            )
            or any(not isinstance(audio.get(name), str) or not audio[name].strip()
                   for name in ("sourceVersion", "sourceContentHash", "profileVersion"))
            or re.fullmatch(r"[0-9a-f]{64}", audio["sourceContentHash"]) is None
            or type(start_ms) is not int or type(end_ms) is not int
            or not 0 <= start_ms < end_ms <= audio["durationMs"]
        ):
            raise ValueError("invalid_retrieval_record")
        temporal = {"start_ms": start_ms, "end_ms": end_ms, "evidence_version": audio["sourceVersion"]}
    elif any(candidate.get(name) is not None for name in ("audio", "startMs", "endMs")):
        raise ValueError("invalid_retrieval_record")
    if (
        not all(
            isinstance(values[name], str) and values[name]
            for name in values
            if name not in ("source_url", "locator_ordinal_start", "locator_ordinal_end")
        )
        or type(values["locator_ordinal_start"]) is not int
        or type(values["locator_ordinal_end"]) is not int
        or values["locator_ordinal_start"] < 1
        or values["locator_ordinal_end"] < values["locator_ordinal_start"]
    ):
        raise ValueError("invalid_retrieval_record")
    if source_modified_at is not None and not isinstance(source_modified_at, str):
        source_modified_at = None
    return RetrievedChunk(
        locator_kind=locator_kind,
        source_modified_at=source_modified_at,
        **temporal,
        **values,
    )