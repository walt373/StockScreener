from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import date, timedelta

import httpx
import pandas as pd

from ..config import settings

log = logging.getLogger(__name__)


@dataclass
class ShortInterestRow:
    ticker: str
    settlement_date: str
    short_interest: float | None
    avg_daily_volume: float | None


async def fetch_latest_short_interest() -> list[ShortInterestRow]:
    """FINRA publishes a bi-monthly equity short interest file.
    URL pattern: https://cdn.finra.org/equity/regsho/monthly/shrt{YYYYMMDD}.txt
    Settlement dates are ~mid-month and ~end-of-month.
    We probe the last ~45 days for the newest available file.
    """
    headers = {"User-Agent": settings.sec_user_agent}
    today = date.today()
    async with httpx.AsyncClient(timeout=60.0, headers=headers, follow_redirects=True) as c:
        for delta in range(0, 45):
            d = today - timedelta(days=delta)
            url = f"https://cdn.finra.org/equity/regsho/monthly/shrt{d.strftime('%Y%m%d')}.txt"
            try:
                r = await c.get(url)
            except httpx.HTTPError as e:
                log.debug("FINRA short interest probe %s: %s", d, e)
                continue
            if r.status_code != 200 or len(r.content) < 1000:
                continue
            log.info("FINRA short interest: using %s", url)
            return _parse(r.text, d.isoformat())
    log.warning("FINRA short interest file not found in last 45 days")
    return []


def _parse(text: str, settlement_date: str) -> list[ShortInterestRow]:
    try:
        df = pd.read_csv(io.StringIO(text), sep="|", dtype=str, on_bad_lines="skip")
    except Exception as e:  # noqa: BLE001
        log.warning("FINRA parse error: %s", e)
        return []
    sym_col = next(
        (c for c in df.columns if c.lower() in ("symbolcode", "symbol", "shortcode")),
        None,
    )
    si_col = next(
        (c for c in df.columns if c.lower() in ("currentshortpositionquantity", "shortinterest")),
        None,
    )
    adv_col = next(
        (c for c in df.columns if c.lower() in ("averagedailyvolumequantity", "avgdailyvol")),
        None,
    )
    if not sym_col:
        log.warning("FINRA columns unexpected: %s", list(df.columns))
        return []
    rows: list[ShortInterestRow] = []
    for _, r in df.iterrows():
        sym = (r.get(sym_col) or "").strip().upper()
        if not sym:
            continue
        try:
            si = float(r.get(si_col)) if si_col and pd.notna(r.get(si_col)) else None
        except (TypeError, ValueError):
            si = None
        try:
            adv = float(r.get(adv_col)) if adv_col and pd.notna(r.get(adv_col)) else None
        except (TypeError, ValueError):
            adv = None
        rows.append(
            ShortInterestRow(
                ticker=sym,
                settlement_date=settlement_date,
                short_interest=si,
                avg_daily_volume=adv,
            )
        )
    return rows
