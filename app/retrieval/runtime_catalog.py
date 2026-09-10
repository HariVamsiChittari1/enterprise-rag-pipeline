"""Serialized runtime catalog refresh with atomic last-known-good publication."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable

from retrieval.catalog import (
    CatalogError,
    CatalogMissingError,
    RuntimeCatalogLoader,
    RuntimeCatalogSnapshot,
)


CATALOG_READ_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class CatalogHealth:
    degraded: bool = True
    reason: str | None = "not_loaded"
    last_observed_at: datetime | None = None


class RuntimeCatalogProvider:
    def __init__(
        self,
        loader: RuntimeCatalogLoader,
        poll_seconds: int,
        *,
        emit: Callable[[str, dict[str, Any]], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        utcnow: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if type(poll_seconds) is not int or not 60 <= poll_seconds <= 86_400:
            raise ValueError("catalog poll seconds must be an integer from 60 through 86400")
        self._loader = loader
        self._poll_seconds = poll_seconds
        self._emit = emit
        self._monotonic = monotonic
        self._utcnow = utcnow
        self._snapshot: RuntimeCatalogSnapshot | None = None
        self._health = CatalogHealth()
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="catalog-reader")
        self._pending: asyncio.Future[RuntimeCatalogSnapshot] | None = None
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._last_started = 0.0
        self._emitted_rejections: set[tuple[str | None, str]] = set()

    @property
    def snapshot(self) -> RuntimeCatalogSnapshot:
        if self._snapshot is None:
            raise CatalogError("runtime catalog has not loaded")
        return self._snapshot

    @property
    def health(self) -> CatalogHealth:
        return self._health

    async def start(self) -> None:
        if self._closed or self._task is not None:
            raise CatalogError("runtime catalog provider cannot be started")
        if not await self.refresh():
            await self.close()
            raise CatalogError("runtime catalog startup failed")
        self._task = asyncio.create_task(self._poll(), name="runtime-catalog-poll")

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._executor.shutdown(wait=False, cancel_futures=True)

    async def _poll(self) -> None:
        while not self._closed:
            remaining = self._last_started + self._poll_seconds - self._monotonic()
            await asyncio.sleep(max(0.0, remaining))
            await self.refresh()

    async def refresh(self) -> bool:
        async with self._lock:
            if self._closed:
                return False
            self._last_started = self._monotonic()
            if self._pending is not None:
                if not self._pending.done():
                    self._reject(None, "read_pending")
                    return False
                self._pending = None
            pending = asyncio.get_running_loop().run_in_executor(self._executor, self._loader.load)
            self._pending = pending
            pending.add_done_callback(self._consume_result)
            try:
                candidate = await asyncio.wait_for(
                    asyncio.shield(pending), timeout=CATALOG_READ_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                self._reject(None, "read_timeout")
                return False
            except asyncio.CancelledError:
                raise
            except CatalogMissingError:
                candidate = self._loader.baseline()
            except CatalogError as error:
                self._reject(error.etag, "read_or_validation_failed")
                return False
            except Exception:
                self._reject(None, "read_or_validation_failed")
                return False
            finally:
                if pending.done():
                    self._pending = None
            if self._closed:
                return False
            previous = self._snapshot
            if candidate.etag is None:
                self._reject(None, "missing_etag")
                return False
            if previous is not None and candidate.etag == previous.etag:
                if candidate.digest != previous.digest:
                    self._reject(candidate.etag, "inconsistent_generation")
                    return False
            else:
                self._snapshot = replace(candidate, accepted_at=self._utcnow())
            self._health = CatalogHealth(False, None, self._utcnow())
            self._event("catalog_observed", {
                "etag_hash": hashlib.sha256(self.snapshot.etag.encode("utf-8")).hexdigest(),
                "digest": self.snapshot.digest,
                "accepted_at": self.snapshot.accepted_at.isoformat(),
                "observed_at": self._health.last_observed_at.isoformat(),
            })
            return True

    @staticmethod
    def _consume_result(future: asyncio.Future[RuntimeCatalogSnapshot]) -> None:
        if not future.cancelled():
            future.exception()

    def _reject(self, etag: str | None, reason: str) -> None:
        was_degraded = self._health.degraded
        self._health = replace(self._health, degraded=True, reason=reason)
        etag_hash = hashlib.sha256(etag.encode("utf-8")).hexdigest() if etag is not None else None
        if not was_degraded:
            self._event("catalog_degraded", {
                "etag_hash": etag_hash, "reason": reason,
                "observed_at": self._utcnow().isoformat(),
            })
        key = (etag_hash, reason)
        if key not in self._emitted_rejections:
            self._event("catalog_rejected", {
                "etag_hash": etag_hash, "reason": reason,
                "observed_at": self._utcnow().isoformat(),
            })
            self._emitted_rejections.add(key)

    def _event(self, name: str, fields: dict[str, Any]) -> None:
        if self._emit is not None:
            try:
                self._emit(name, fields)
            except Exception:
                logging.getLogger(__name__).warning("catalog_event_export_failed")