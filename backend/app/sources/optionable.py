"""Bulk source for the set of US-listed optionable equity symbols.

Tries CBOE first (covers all US options), falls back to Nasdaq/PHLX. Returns a set
of bare tickers. If all sources fail, returns None and the pipeline falls back to
per-ticker yfinance checks on survivors.
"""

from __future__ import annotations

import io
import logging

import httpx
import pandas as pd

from ..config import settings

log = logging.getLogger(__name__)


_CBOE_URL = "https://www.cboe.com/us/options/symboldir/equity_index_options/?download=csv"
_NASDAQ_TRADER_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/options.txt"


async def fetch_optionable_symbols() -> set[str] | None:
    """Return the set of optionable US equity tickers, or None if all sources failed."""
    headers = {"User-Agent": settings.sec_user_agent}
    async with httpx.AsyncClient(timeout=60.0, headers=headers, follow_redirects=True) as c:
        syms = await _try_cboe(c)
        if syms:
            log.info("Optionable universe from CBOE: %d symbols", len(syms))
            return syms
        syms = await _try_nasdaq_trader(c)
        if syms:
            log.info("Optionable universe from Nasdaqtrader: %d symbols", len(syms))
            return syms
    log.warning("No bulk optionable source succeeded — will fall back to per-ticker yfinance")
    return None


async def _try_cboe(c: httpx.AsyncClient) -> set[str] | None:
    try:
        r = await c.get(_CBOE_URL)
    except httpx.HTTPError as e:
        log.debug("CBOE fetch error: %s", e)
        return None
    if r.status_code != 200 or len(r.content) < 1000:
        log.debug("CBOE fetch status %s", r.status_code)
        return None
    try:
        df = pd.read_csv(io.StringIO(r.text), dtype=str)
    except Exception as e:  # noqa: BLE001
        log.debug("CBOE parse error: %s", e)
        return None
    # The CBOE CSV has columns like "Stock Symbol" or "Ticker" — try both.
    for col in df.columns:
        if col.strip().lower() in ("stock symbol", "ticker", "symbol", "underlying"):
            return {s.strip().upper() for s in df[col].dropna() if s.strip()}
    log.debug("CBOE columns unexpected: %s", list(df.columns))
    return None


async def _try_nasdaq_trader(c: httpx.AsyncClient) -> set[str] | None:
    try:
        r = await c.get(_NASDAQ_TRADER_URL)
    except httpx.HTTPError as e:
        log.debug("Nasdaqtrader options fetch error: %s", e)
        return None
    if r.status_code != 200 or len(r.content) < 500:
        log.debug("Nasdaqtrader options status %s", r.status_code)
        return None
    # Pipe-delimited like the other SymDir files. First column is typically the symbol.
    lines = r.text.splitlines()
    if len(lines) < 10:
        return None
    header = lines[0].split("|")
    sym_idx = None
    for i, col in enumerate(header):
        cl = col.strip().lower()
        if cl in ("symbol", "root symbol", "underlying symbol", "underlying"):
            sym_idx = i
            break
    if sym_idx is None:
        return None
    out: set[str] = set()
    for line in lines[1:]:
        if line.startswith("File Creation Time"):
            break
        parts = line.split("|")
        if len(parts) <= sym_idx:
            continue
        sym = parts[sym_idx].strip().upper()
        if sym and sym.isalpha() and 1 <= len(sym) <= 5:
            out.add(sym)
    return out or None
