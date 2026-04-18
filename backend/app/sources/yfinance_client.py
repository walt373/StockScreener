from __future__ import annotations

import asyncio
import logging
import math
import random
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

from ..config import settings
from ..util.numbers import safe_float

log = logging.getLogger(__name__)

_sem = asyncio.Semaphore(settings.yf_concurrency)


def _is_rate_limited(e: BaseException) -> bool:
    name = type(e).__name__
    msg = str(e).lower()
    return (
        name == "YFRateLimitError"
        or "too many requests" in msg
        or "rate limit" in msg
        or "429" in msg
    )


async def _backoff_if_ratelimited(e: BaseException, attempt: int) -> bool:
    """Returns True if we should retry after sleeping."""
    if not _is_rate_limited(e):
        return False
    if attempt >= 3:
        return False
    delay = 30.0 * (2**attempt) + random.uniform(0, 5)  # 30, 60, 120s
    log.warning("yfinance rate-limited — sleeping %.0fs (attempt %d)", delay, attempt + 1)
    await asyncio.sleep(delay)
    return True


@dataclass
class FastInfo:
    price: float | None
    market_cap: float | None
    avg_volume: float | None


@dataclass
class FundamentalsBlob:
    cash: float | None
    current_assets: float | None
    total_liabilities: float | None
    equity: float | None
    revenue_growth: float | None
    net_income: float | None
    free_cash_flow: float | None
    shares_short: float | None
    shares_outstanding: float | None


async def _to_thread(fn, *args, **kwargs):
    return await asyncio.to_thread(fn, *args, **kwargs)


def _fi_get(fi: object, *keys: str):
    """Best-effort lookup across yfinance versions (0.2 snake_case, 1.x camelCase)."""
    for k in keys:
        if hasattr(fi, "get"):
            try:
                v = fi.get(k)
                if v is not None:
                    return v
            except Exception:  # noqa: BLE001
                pass
        v = getattr(fi, k, None)
        if v is not None:
            return v
    return None


def _fast_info_sync(ticker: str) -> tuple[float | None, float | None, float | None, BaseException | None]:
    try:
        t = yf.Ticker(ticker)
        fi = t.fast_info
        price = safe_float(_fi_get(fi, "lastPrice", "last_price"))
        mcap = safe_float(_fi_get(fi, "marketCap", "market_cap"))
        avg = safe_float(
            _fi_get(
                fi,
                "threeMonthAverageVolume",
                "three_month_average_volume",
                "tenDayAverageVolume",
                "ten_day_average_volume",
            )
        )
        return price, mcap, avg, None
    except BaseException as e:  # noqa: BLE001
        log.debug("fast_info failed for %s: %s", ticker, e)
        return None, None, None, e


async def fast_info(ticker: str) -> FastInfo:
    for attempt in range(4):
        async with _sem:
            await asyncio.sleep(random.uniform(0.3, 0.9))
            price, mcap, avg, err = await _to_thread(_fast_info_sync, ticker)
        if err is not None and await _backoff_if_ratelimited(err, attempt):
            continue
        return FastInfo(price=price, market_cap=mcap, avg_volume=avg)
    return FastInfo(None, None, None)


def _options_sync(ticker: str) -> tuple[list[str], BaseException | None]:
    try:
        t = yf.Ticker(ticker)
        opts = t.options
        return (list(opts) if opts else []), None
    except BaseException as e:  # noqa: BLE001
        log.debug("options failed for %s: %s", ticker, e)
        return [], e


async def option_expiries(ticker: str) -> list[str]:
    for attempt in range(4):
        async with _sem:
            await asyncio.sleep(random.uniform(0.3, 0.9))
            opts, err = await _to_thread(_options_sync, ticker)
        if err is not None and await _backoff_if_ratelimited(err, attempt):
            continue
        return opts
    return []


def _pick_row(df: pd.DataFrame | None, candidates: tuple[str, ...]) -> float | None:
    if df is None or df.empty:
        return None
    idx = [str(i).strip() for i in df.index]
    lower_idx = [i.lower() for i in idx]
    for cand in candidates:
        cand_l = cand.lower()
        for i, name in enumerate(lower_idx):
            if name == cand_l:
                series = df.iloc[i]
                if series.empty:
                    return None
                val = series.iloc[0]
                return safe_float(val)
    # looser contains match
    for cand in candidates:
        cand_l = cand.lower()
        for i, name in enumerate(lower_idx):
            if cand_l in name:
                series = df.iloc[i]
                if series.empty:
                    continue
                val = series.iloc[0]
                v = safe_float(val)
                if v is not None:
                    return v
    return None


async def fundamentals(ticker: str) -> tuple[FundamentalsBlob, dict[str, Any]]:
    async with _sem:
        await asyncio.sleep(random.uniform(0.0, 0.25))
        try:
            t = await _to_thread(yf.Ticker, ticker)
            bs = await _to_thread(lambda: t.balance_sheet)
            fin = await _to_thread(lambda: t.financials)
            cf = await _to_thread(lambda: t.cashflow)
            info: dict[str, Any] = {}
            try:
                info = await _to_thread(lambda: t.info) or {}
            except Exception:  # noqa: BLE001
                info = {}

            cash = _pick_row(
                bs,
                (
                    "Cash And Cash Equivalents",
                    "Cash Cash Equivalents And Short Term Investments",
                    "Cash",
                ),
            )
            current_assets = _pick_row(bs, ("Current Assets", "Total Current Assets"))
            total_liab = _pick_row(
                bs,
                ("Total Liabilities Net Minority Interest", "Total Liab", "Total Liabilities"),
            )
            equity = _pick_row(
                bs,
                (
                    "Stockholders Equity",
                    "Total Stockholder Equity",
                    "Common Stock Equity",
                ),
            )
            net_income = _pick_row(fin, ("Net Income", "Net Income Common Stockholders"))
            fcf = _pick_row(cf, ("Free Cash Flow",))
            if fcf is None:
                ocf = _pick_row(cf, ("Operating Cash Flow", "Cash Flow From Continuing Operating Activities"))
                capex = _pick_row(cf, ("Capital Expenditure", "Capital Expenditures"))
                if ocf is not None and capex is not None:
                    fcf = ocf + capex  # capex is negative already

            return (
                FundamentalsBlob(
                    cash=cash,
                    current_assets=current_assets,
                    total_liabilities=total_liab,
                    equity=equity,
                    revenue_growth=safe_float(info.get("revenueGrowth")),
                    net_income=net_income,
                    free_cash_flow=fcf,
                    shares_short=safe_float(info.get("sharesShort")),
                    shares_outstanding=safe_float(info.get("sharesOutstanding")),
                ),
                info,
            )
        except Exception as e:  # noqa: BLE001
            log.debug("fundamentals failed for %s: %s", ticker, e)
            return FundamentalsBlob(None, None, None, None, None, None, None, None, None), {}


def _compute_history_metrics(closes: pd.Series) -> tuple[float | None, float | None]:
    closes = closes.dropna()
    if len(closes) < 30:
        return None, None
    first = float(closes.iloc[0])
    last = float(closes.iloc[-1])
    ret = (last / first) - 1 if first > 0 else None
    log_rets = np.log(closes / closes.shift(1)).dropna()
    if len(log_rets) < 20:
        return ret, None
    std = float(log_rets.std())
    vol = std * math.sqrt(252) if std and math.isfinite(std) else None
    return ret, vol


async def price_volume_batch(tickers: list[str]) -> dict[str, tuple[float | None, float | None]]:
    """Batch fetch recent price + avg volume via yf.download (much cheaper than per-ticker)."""
    if not tickers:
        return {}
    async with _sem:
        await asyncio.sleep(random.uniform(0.3, 0.9))
        try:
            df = await _to_thread(
                yf.download,
                tickers=" ".join(tickers),
                period="1mo",
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
                group_by="ticker",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("price_volume batch failed (%d tickers): %s", len(tickers), e)
            return {t: (None, None) for t in tickers}

    out: dict[str, tuple[float | None, float | None]] = {}
    if df is None or df.empty:
        return {t: (None, None) for t in tickers}
    if len(tickers) == 1:
        closes = df["Close"] if "Close" in df.columns else pd.Series(dtype=float)
        vols = df["Volume"] if "Volume" in df.columns else pd.Series(dtype=float)
        last = safe_float(closes.dropna().iloc[-1]) if not closes.dropna().empty else None
        avg = safe_float(vols.dropna().mean()) if not vols.dropna().empty else None
        out[tickers[0]] = (last, avg)
        return out
    for t in tickers:
        try:
            if (t, "Close") in df.columns:
                closes = df[(t, "Close")].dropna()
                vols = df[(t, "Volume")].dropna() if (t, "Volume") in df.columns else pd.Series(dtype=float)
            else:
                out[t] = (None, None)
                continue
            last = safe_float(closes.iloc[-1]) if not closes.empty else None
            avg = safe_float(vols.mean()) if not vols.empty else None
            out[t] = (last, avg)
        except Exception as e:  # noqa: BLE001
            log.debug("price_volume parse fail %s: %s", t, e)
            out[t] = (None, None)
    return out


async def history_batch(
    tickers: list[str], period: str = "1y"
) -> dict[str, tuple[float | None, float | None]]:
    """Return {ticker: (trailing_return, realized_vol)}. Missing tickers map to (None, None)."""
    if not tickers:
        return {}
    async with _sem:
        await asyncio.sleep(random.uniform(0.0, 0.5))
        try:
            df = await _to_thread(
                yf.download,
                tickers=" ".join(tickers),
                period=period,
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
                group_by="ticker",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("yf.download batch failed (%d tickers): %s", len(tickers), e)
            return {t: (None, None) for t in tickers}

    out: dict[str, tuple[float | None, float | None]] = {}
    if df is None or df.empty:
        return {t: (None, None) for t in tickers}
    if len(tickers) == 1:
        closes = df["Close"] if "Close" in df.columns else pd.Series(dtype=float)
        out[tickers[0]] = _compute_history_metrics(closes)
        return out
    for t in tickers:
        try:
            if (t, "Close") in df.columns:
                closes = df[(t, "Close")]
            elif t in df.columns.get_level_values(0):
                closes = df[t]["Close"]
            else:
                out[t] = (None, None)
                continue
            out[t] = _compute_history_metrics(closes)
        except Exception as e:  # noqa: BLE001
            log.debug("history parse fail %s: %s", t, e)
            out[t] = (None, None)
    return out
