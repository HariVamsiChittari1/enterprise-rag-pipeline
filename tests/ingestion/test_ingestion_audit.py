"""Tests for ingestion audit — embedding calls write to service-audit container."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from ingestion.embedding import embed_texts
from ingestion.errors import TerminalDocumentError
from ingestion.telemetry import usage_record, write_audit_record
from openai import RateLimitError


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


def _throttled(headers: dict[str, str]) -> RateLimitError:
    return RateLimitError(
        "synthetic throttling",
        response=httpx.Response(
            429,
            headers=headers,
            request=httpx.Request("POST", "https://example.test/embeddings"),
        ),
        body=None,
    )


def test_embedding_throttle_retries_only_current_batch(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    client = _fake_openai_client()
    success = client.embeddings.create.side_effect
    calls = []
    audit = MagicMock()
    sleep = MagicMock()
    monkeypatch.setattr("ingestion.embedding.time.sleep", sleep)

    def create(**kwargs: Any) -> FakeResponse:
        calls.append(list(kwargs["input"]))
        if len(calls) == 2:
            raise _throttled({"Retry-After": "60"})
        return success(**kwargs)

    client.embeddings.create.side_effect = create
    vectors = embed_texts(
        client, ["first", "second"], "test", audit_container=audit, batch_size=1,
    )
    assert len(vectors) == 2
    assert calls == [["first"], ["second"], ["second"]]
    sleep.assert_called_once_with(60.0)
    assert audit.create_item.call_count == 2
    assert "ingestion_embedding_throttled" in caplog.text
    assert "second" not in caplog.text


@pytest.mark.parametrize("headers,delay", [
    ({"retry-after-ms": "1500", "Retry-After": "20"}, 1.5),
    ({"Retry-After": "2.5"}, 2.5),
    ({"Retry-After": "0"}, 0.0),
    ({"Retry-After": "invalid"}, 1.0),
    ({"Retry-After": "nan"}, 1.0),
    ({"Retry-After": "-5"}, 1.0),
    ({}, 1.0),
])
def test_embedding_retry_after_formats(
    monkeypatch: pytest.MonkeyPatch, headers: dict[str, str], delay: float,
) -> None:
    client = _fake_openai_client()
    success = client.embeddings.create.side_effect(input=["text"])
    client.embeddings.create.side_effect = [_throttled(headers), success]
    sleep = MagicMock()
    monkeypatch.setattr("ingestion.embedding.time.sleep", sleep)
    assert len(embed_texts(client, ["text"], "test")) == 1
    sleep.assert_called_once_with(delay)


def test_embedding_retry_after_http_date(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _fake_openai_client()
    success = client.embeddings.create.side_effect(input=["text"])
    client.embeddings.create.side_effect = [
        _throttled({"Retry-After": "Wed, 21 Oct 2015 07:28:10 GMT"}), success,
    ]
    monkeypatch.setattr("ingestion.embedding.time.time", lambda: 1445412480.0)
    sleep = MagicMock()
    monkeypatch.setattr("ingestion.embedding.time.sleep", sleep)
    assert len(embed_texts(client, ["text"], "test")) == 1
    sleep.assert_called_once_with(10.0)


def test_embedding_throttle_retries_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _fake_openai_client()
    client.embeddings.create.side_effect = _throttled({"Retry-After": "1"})
    sleep = MagicMock()
    monkeypatch.setattr("ingestion.embedding.time.sleep", sleep)
    with pytest.raises(TimeoutError, match="openai_throttled:RateLimitError"):
        embed_texts(client, ["text"], "test")
    assert client.embeddings.create.call_count == 5
    assert sleep.call_count == 4


def test_embedding_does_not_retry_before_server_delay_beyond_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _fake_openai_client()
    client.embeddings.create.side_effect = _throttled({"Retry-After": "181"})
    sleep = MagicMock()
    monkeypatch.setattr("ingestion.embedding.time.sleep", sleep)
    with pytest.raises(TimeoutError, match="openai_throttled"):
        embed_texts(client, ["text"], "test")
    client.embeddings.create.assert_called_once()
    sleep.assert_not_called()


def test_embedding_non_rate_limit_error_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _fake_openai_client()
    client.embeddings.create.side_effect = ValueError("synthetic")
    sleep = MagicMock()
    monkeypatch.setattr("ingestion.embedding.time.sleep", sleep)
    with pytest.raises(TerminalDocumentError, match="openai_failed:ValueError"):
        embed_texts(client, ["text"], "test")
    client.embeddings.create.assert_called_once()
    sleep.assert_not_called()


@pytest.mark.parametrize("response,error", [
    (FakeResponse(data=[], usage=FakeUsage()), "embedding_count_mismatch"),
    (
        FakeResponse(data=[FakeEmbeddingItem(index=0, embedding=(0.1,))], usage=FakeUsage()),
        "embedding_dimension_mismatch",
    ),
])
def test_embedding_retry_preserves_response_validation(
    monkeypatch: pytest.MonkeyPatch, response: FakeResponse, error: str,
) -> None:
    client = _fake_openai_client()
    client.embeddings.create.side_effect = [_throttled({"Retry-After": "0"}), response]
    monkeypatch.setattr("ingestion.embedding.time.sleep", MagicMock())

    with pytest.raises(TerminalDocumentError, match=error):
        embed_texts(client, ["text"], "test")

    assert client.embeddings.create.call_count == 2


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
