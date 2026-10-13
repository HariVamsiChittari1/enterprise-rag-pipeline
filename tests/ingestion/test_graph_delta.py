"""Tests for Graph delta query's 410/DeltaResetRequired handling (Scenario 12 gap:
previously untested, though the mechanism already existed in graph.py)."""

from __future__ import annotations

import httpx

from ingestion.graph import read_drive_delta


def test_read_drive_delta_first_call_establishes_baseline() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "/drives/drive-1/root/delta" in str(request.url)
        return httpx.Response(
            200,
            json={
                "value": [{"id": "item-1"}, {"id": "item-2"}],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=first",
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        delta = read_drive_delta(client, "drive-1", max_pages=10)

    assert delta.is_full_baseline is True
    assert delta.delta_link == "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=first"
    assert {item["id"] for item in delta.items} == {"item-1", "item-2"}


def test_read_drive_delta_resumes_normally_with_a_stored_cursor() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "token=old" in str(request.url)
        return httpx.Response(
            200,
            json={
                "value": [{"id": "item-3"}],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=new",
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        delta = read_drive_delta(
            client, "drive-1", max_pages=10,
            delta_link="https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=old",
        )

    assert delta.is_full_baseline is False
    assert delta.delta_link == "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=new"
    assert [item["id"] for item in delta.items] == ["item-3"]


def test_read_drive_delta_handles_410_reset_with_a_full_rebaseline() -> None:
    reset_location = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta"
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            assert "token=expired" in str(request.url)
            return httpx.Response(410, headers={"location": reset_location})
        # The reset re-request must go to the location the server provided, without a token.
        assert str(request.url) == reset_location
        return httpx.Response(
            200,
            json={
                "value": [{"id": "item-1"}, {"id": "item-2"}],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=rebaselined",
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        delta = read_drive_delta(
            client, "drive-1", max_pages=10,
            delta_link="https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=expired",
        )

    assert call_count == 2
    assert delta.is_full_baseline is True  # caller must treat this as a full re-sync, not an incremental diff
    assert delta.delta_link == "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=rebaselined"
    assert {item["id"] for item in delta.items} == {"item-1", "item-2"}


def test_read_drive_delta_restarts_when_410_repeats_expired_location() -> None:
    expired = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=expired"
    baseline = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta"
    final_link = f"{baseline}?token=fresh"
    calls: list[str] = []

    class Capture:
        def __init__(self) -> None:
            self.resets: dict[str, str] = {}

        def begin_page(self, request_url, _page_ordinal, _predecessor_receipt_id):
            location = self.resets.get(request_url)
            if location is not None:
                return {"state": "reset", "location": location}
            return {"state": "capture", "receiptId": "receipt"}

        def record_reset(self, request_url, _page_ordinal, location):
            self.resets[request_url] = location

        def seal_page(self, *_args):
            return "receipt"

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if str(request.url) == expired:
            return httpx.Response(410, headers={"location": expired})
        assert str(request.url) == baseline
        return httpx.Response(
            200,
            json={"value": [{"id": "item-1"}], "@odata.deltaLink": final_link},
        )

    capture = Capture()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        delta = read_drive_delta(
            client,
            "drive-1",
            max_pages=10,
            delta_link=expired,
            page_capture=capture,
        )

    assert calls == [expired, baseline]
    assert capture.resets == {expired: baseline}
    assert delta.is_full_baseline is True
    assert delta.delta_link == final_link


def test_read_drive_delta_deduplicates_repeated_item_ids_keeping_latest_state() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "value": [
                    {"id": "item-1", "name": "old-name.pdf"},
                    {"id": "item-1", "name": "renamed.pdf"},
                ],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=x",
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        delta = read_drive_delta(client, "drive-1", max_pages=10)

    assert len(delta.items) == 1
    assert delta.items[0]["name"] == "renamed.pdf"


def test_read_drive_delta_replays_sealed_pages_without_graph_refetch() -> None:
    first_url = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta"
    next_url = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?skip=2"
    final_link = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=sealed"

    class Capture:
        def __init__(self) -> None:
            self.begun: list[tuple[str, int, str | None]] = []

        def begin_page(self, request_url, page_ordinal, predecessor_receipt_id):
            self.begun.append((request_url, page_ordinal, predecessor_receipt_id))
            if page_ordinal == 0:
                return {
                    "state": "sealed", "receiptId": "receipt-0",
                    "items": [{"id": "item-1", "name": "one.pdf"}],
                    "nextLink": next_url, "deltaLink": None,
                }
            return {
                "state": "sealed", "receiptId": "receipt-1",
                "items": [{"id": "item-2", "name": "two.pdf"}],
                "nextLink": None, "deltaLink": final_link,
            }

    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("sealed Graph pages must replay without a provider refetch")

    capture = Capture()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        delta = read_drive_delta(client, "drive-1", max_pages=10, page_capture=capture)

    assert [item["id"] for item in delta.items] == ["item-1", "item-2"]
    assert delta.delta_link == final_link
    assert capture.begun == [(first_url, 0, None), (next_url, 1, "receipt-0")]


def test_read_drive_delta_seals_fresh_pages_with_their_begin_ordinal() -> None:
    first_url = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta"
    next_url = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?skip=1"
    final_link = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=t"

    class Capture:
        def __init__(self) -> None:
            self.begun: list[tuple[str, int]] = []
            self.sealed: list[tuple[str, int, str | None]] = []

        def begin_page(self, request_url, page_ordinal, predecessor_receipt_id):
            self.begun.append((request_url, page_ordinal))
            return {"state": "capturing", "receiptId": f"receipt-{page_ordinal}"}

        def seal_page(self, request_url, page_ordinal, predecessor_receipt_id, *_rest):
            self.sealed.append((request_url, page_ordinal, predecessor_receipt_id))
            return f"receipt-{page_ordinal}"

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == first_url:
            return httpx.Response(200, json={"value": [{"id": "a"}], "@odata.nextLink": next_url})
        return httpx.Response(200, json={"value": [{"id": "b"}], "@odata.deltaLink": final_link})

    capture = Capture()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        read_drive_delta(client, "drive-1", max_pages=10, page_capture=capture)

    assert capture.begun == [(first_url, 0), (next_url, 1)]
    assert capture.sealed == [(first_url, 0, None), (next_url, 1, "receipt-0")]


def test_read_drive_delta_handles_double_410_reset() -> None:
    """Graph can return 410 on the reset URL itself during server-side transitions."""
    first_reset = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?reset=1"
    second_reset = "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?reset=2"
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(410, headers={"location": first_reset})
        if call_count == 2:
            return httpx.Response(410, headers={"location": second_reset})
        return httpx.Response(
            200,
            json={
                "value": [{"id": "item-1"}],
                "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=fresh",
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        delta = read_drive_delta(
            client, "drive-1", max_pages=10,
            delta_link="https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=stale",
        )

    assert call_count == 3
    assert delta.is_full_baseline is True
    assert delta.delta_link == "https://graph.microsoft.com/v1.0/drives/drive-1/root/delta?token=fresh"
    assert [item["id"] for item in delta.items] == ["item-1"]
