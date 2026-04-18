from __future__ import annotations

import asyncio
import logging

import httpx
from aiolimiter import AsyncLimiter

from ..config import settings

log = logging.getLogger(__name__)


class EdgarClient:
    """Shared EDGAR HTTP client: UA header, global 10 req/sec limit, retries."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            headers={
                "User-Agent": settings.sec_user_agent,
                "Accept-Encoding": "gzip, deflate",
            },
            timeout=httpx.Timeout(30.0, connect=10.0),
            follow_redirects=True,
            http2=False,
        )
        self._limiter = AsyncLimiter(max_rate=settings.edgar_rate_per_sec, time_period=1.0)

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
                async with self._limiter:
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
