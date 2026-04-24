from __future__ import annotations

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import requests

from ..config import settings

log = logging.getLogger(__name__)


# Target: stay safely under SEC's 10 req/sec cap. 9/sec pacing = ~111ms between
# request starts. `_WORKERS` caps true concurrency so a handful of slow responses
# can't starve other waiters.
_MIN_INTERVAL = 1.0 / 9.0
_WORKERS = 10


class _SyncResponse:
    """Tiny wrapper around requests.Response that exposes only what we use —
    keeps the call sites' API identical to the old httpx one."""

    def __init__(self, r: requests.Response) -> None:
        self._r = r

    @property
    def status_code(self) -> int:
        return self._r.status_code

    @property
    def text(self) -> str:
        return self._r.text

    def json(self) -> Any:
        return self._r.json()

    def raise_for_status(self) -> None:
        self._r.raise_for_status()


class EdgarClient:
    """Thread-pool based client for data.sec.gov / www.sec.gov / efts.sec.gov.

    Previous async implementations (aiolimiter, then asyncio.Semaphore) both wedged
    reproducibly on Python 3.14 + Windows proactor after ~560 requests. The network
    IO now runs in a ThreadPoolExecutor — plain blocking `requests` calls — and only
    the awaitable wrapper touches the event loop. No more asyncio-bound HTTP state
    to deadlock.
    """

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": settings.sec_user_agent,
                "Accept-Encoding": "gzip, deflate",
            }
        )
        self._executor = ThreadPoolExecutor(
            max_workers=_WORKERS, thread_name_prefix="edgar"
        )
        self._pace_lock = threading.Lock()
        self._last_request_at = 0.0

    def _pace(self) -> None:
        """Serialize a short sleep so no two requests *start* within _MIN_INTERVAL."""
        with self._pace_lock:
            now = time.monotonic()
            wait = _MIN_INTERVAL - (now - self._last_request_at)
            if wait > 0:
                time.sleep(wait)
            self._last_request_at = time.monotonic()

    def _get_sync(self, url: str, accept: str, max_retries: int) -> _SyncResponse:
        delays = [1.0, 2.0, 4.0, 8.0]
        last_exc: Exception | None = None
        for attempt in range(max_retries + 1):
            self._pace()
            try:
                resp = self._session.get(
                    url,
                    headers={"Accept": accept},
                    timeout=(5.0, 15.0),  # (connect, read)
                )
            except requests.RequestException as e:
                last_exc = e
                log.warning("EDGAR transport error %s at %s (attempt %d)", e, url, attempt + 1)
                time.sleep(delays[min(attempt, len(delays) - 1)])
                continue
            if resp.status_code == 429:
                log.warning("EDGAR 429 at %s — backing off", url)
                time.sleep(delays[min(attempt, len(delays) - 1)])
                continue
            if resp.status_code >= 500:
                log.warning("EDGAR %s at %s", resp.status_code, url)
                time.sleep(delays[min(attempt, len(delays) - 1)])
                continue
            return _SyncResponse(resp)
        if last_exc:
            raise last_exc
        raise RuntimeError(f"EDGAR request failed after {max_retries} retries: {url}")

    async def get(
        self,
        url: str,
        *,
        accept: str = "application/json",
        max_retries: int = 3,
    ) -> _SyncResponse:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, self._get_sync, url, accept, max_retries
        )

    async def close(self) -> None:
        self._executor.shutdown(wait=False)
        self._session.close()


_edgar: EdgarClient | None = None


def edgar_client() -> EdgarClient:
    global _edgar
    if _edgar is None:
        _edgar = EdgarClient()
    return _edgar


async def close_clients() -> None:
    global _edgar
    if _edgar is not None:
        await _edgar.close()
        _edgar = None
