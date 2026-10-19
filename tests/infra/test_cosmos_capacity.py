from __future__ import annotations

from ingestion.models import (
    MAX_CHUNK_ITEM_BYTES,
    MAX_DOCUMENT_ITEM_BYTES,
    SourceAdmissionConfig,
)
COSMOS_LOGICAL_PARTITION_BYTES = 20 * 1024 * 1024 * 1024


def test_source_document_partition_stays_below_cosmos_limit() -> None:
    limits = SourceAdmissionConfig()

    worst_case_partition_bytes = limits.max_eligible_sources * MAX_DOCUMENT_ITEM_BYTES

    assert worst_case_partition_bytes < COSMOS_LOGICAL_PARTITION_BYTES


def test_document_chunk_partition_stays_below_cosmos_limit() -> None:
    limits = SourceAdmissionConfig()

    worst_case_partition_bytes = limits.max_chunks_per_source * MAX_CHUNK_ITEM_BYTES

    assert worst_case_partition_bytes < COSMOS_LOGICAL_PARTITION_BYTES


def test_approved_capacity_has_headroom() -> None:
    limits = SourceAdmissionConfig()
    document_headroom = COSMOS_LOGICAL_PARTITION_BYTES / (
        limits.max_eligible_sources * MAX_DOCUMENT_ITEM_BYTES
    )
    chunk_headroom = COSMOS_LOGICAL_PARTITION_BYTES / (
        limits.max_chunks_per_source * MAX_CHUNK_ITEM_BYTES
    )

    assert document_headroom >= 16
    assert chunk_headroom >= 40
