"""Tests for ingestion audit — embedding calls write to service-audit container."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

from ingestion.embedding import embed_texts
from ingestion.telemetry import usage_record, write_audit_record


@dataclass
class FakeUsage:
    prompt_tokens: int = 500
    completion_tokens: int = 0


@dataclass
class FakeEmbeddingItem:
    index: int
    embedding: tuple[float, ...]


@dataclass
class FakeResponse:
    data: list[FakeEmbeddingItem]
    usage: FakeUsage


def _fake_openai_client(batch_size: int = 2) -> MagicMock:
    client = MagicMock()

    def _create(**kwargs: Any) -> FakeResponse:
        texts = kwargs.get("input", [])
        return FakeResponse(
            data=[FakeEmbeddingItem(index=i, embedding=tuple([0.1] * 3072)) for i in range(len(texts))],
            usage=FakeUsage(prompt_tokens=len(texts) * 250),
        )

    client.embeddings.create.side_effect = _create
    return client


def test_embed_texts_writes_audit_record_per_batch() -> None:
    client = _fake_openai_client()
    audit_container = MagicMock()

    embed_texts(
        client, ["text-1", "text-2"], "test-embedding",
        audit_container=audit_container,
        source_id="source-a",
        run_id="run-1",
        business_name="Business A",
    )

    assert audit_container.create_item.call_count == 1
    item = audit_container.create_item.call_args[0][0]
    assert item["sourceId"] == "source-a"
    assert item["runId"] == "run-1"
    assert item["businessName"] == "Business A"
    assert item["operation"] == "ingestion_embedding"
    assert item["prompt_tokens"] == 500
    assert item["completion_tokens"] == 0
    assert "latency_ms" in item
    assert "id" in item
    assert "recordedAt" in item


def test_embed_texts_skips_audit_when_container_is_none() -> None:
    client = _fake_openai_client()

    result = embed_texts(client, ["text-1"], "test-embedding")

    assert len(result) == 1
    assert len(result[0]) == 3072


def test_audit_write_failure_does_not_break_embedding() -> None:
    client = _fake_openai_client()
    audit_container = MagicMock()
    audit_container.create_item.side_effect = RuntimeError("cosmos unavailable")

    result = embed_texts(
        client, ["text-1"], "test-embedding",
        audit_container=audit_container,
        source_id="src",
        run_id="run",
    )

    assert len(result) == 1
    audit_container.create_item.assert_called_once()


def test_usage_record_extracts_token_counts() -> None:
    response = FakeResponse(
        data=[],
        usage=FakeUsage(prompt_tokens=100, completion_tokens=0),
    )
    record = usage_record("ingestion_embedding", "text-embedding-3-large", response, 0.0)

    assert record["operation"] == "ingestion_embedding"
    assert record["model"] == "text-embedding-3-large"
    assert record["prompt_tokens"] == 100
    assert record["completion_tokens"] == 0
    assert isinstance(record["latency_ms"], int)


def test_write_audit_record_is_best_effort() -> None:
    container = MagicMock()
    container.create_item.side_effect = Exception("boom")

    write_audit_record(container, "src", "run", {"operation": "test"})

    container.create_item.assert_called_once()


def test_write_audit_record_omits_business_name_without_source_context() -> None:
    container = MagicMock()

    write_audit_record(container, "", "global", {"operation": "global_event"})

    item = container.create_item.call_args.args[0]
    assert "businessName" not in item
