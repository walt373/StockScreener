from __future__ import annotations

import asyncio
import logging
import time

import httpx

from ..config import settings

log = logging.getLogger(__name__)


# Target: stay safely under SEC's 10 req/sec cap. 9/sec spacing = ~111ms min between
# request starts. Plus a 10-way concurrency cap so a handful of slow responses can't
# exhaust httpx's connection pool.
_MIN_INTERVAL = 1.0 / 9.0
_CONCURRENCY = 10


class EdgarClient:
    """Async client for data.sec.gov / www.sec.gov / efts.sec.gov.

    Uses a plain asyncio.Semaphore + time-spacing lock instead of aiolimiter —
    aiolimiter has shown stalls on Python 3.14 + Windows proactor under load,
    and we really only need two primitives:
      - max N concurrent in-flight requests (semaphore)
      - min gap between request starts (spacing lock)
    """

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            headers={
                "User-Agent": settings.sec_user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
            follow_redirects=True,
            http2=False,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        self._sem = asyncio.Semaphore(_CONCURRENCY)
        self._pace_lock = asyncio.Lock()
        self._last_request_at = 0.0

    async def _pace(self) -> None:
        """Block briefly so no two requests start within _MIN_INTERVAL of each other."""
        async with self._pace_lock:
            now = time.monotonic()
            wait = _MIN_INTERVAL - (now - self._last_request_at)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = time.monotonic()

    async def get(
        self,
        url: str,
        *,
        accept: str = "application/json",
        max_retries: int = 3,
    ) -> httpx.Response:
        delays = [1.0, 2.0, 4.0, 8.0]
        last_exc: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                async with self._sem:
                    await self._pace()
                    resp = await self._client.get(url, headers={"Accept": accept})
                if resp.status_code == 429:
                    log.warning("EDGAR 429 at %s — backing off", url)
                    await asyncio.sleep(delays[min(attempt, len(delays) - 1)])
                    continue
                if resp.status_code >= 500:
                    log.warning("EDGAR %s at %s", resp.status_code, url)
                    await asyncio.sleep(delays[min(attempt, len(delays) - 1)])
                    continue
                return resp
            except (httpx.HTTPError, httpx.TransportError) as e:
                last_exc = e
                log.warning("EDGAR transport error %s at %s (attempt %d)", e, url, attempt + 1)
                await asyncio.sleep(delays[min(attempt, len(delays) - 1)])
        if last_exc:
            raise last_exc
        raise RuntimeError(f"EDGAR request failed after {max_retries} retries: {url}")

    async def close(self) -> None:
        await self._client.aclose()


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
