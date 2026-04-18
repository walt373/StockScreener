"""Pipeline stages A–H.

Consolidated into one module so the data flow is readable end-to-end.
Each stage returns a dict keyed by ticker; the orchestrator merges them.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..models import (
    BondManualOverride,
    DebtMaturity,
    EdgarCompanyCache,
    FilingsFlags,
    FundamentalsCache,
    RefreshRun,
    HistoryCache,
    OptionsCache,
    ShortInterestFinra,
    Ticker,
    TickerError,
    utcnow,
)
from ..sources import edgar, finra_shortint, universe, yfinance_client
from ..util.filings_text import analyze_filing_html
from ..util.numbers import safe_float

log = logging.getLogger(__name__)


@dataclass
class Row:
    ticker: str
    name: str | None = None
    exchange: str | None = None
    financial_status: str | None = None
    cik: str | None = None
    price: float | None = None
    market_cap: float | None = None
    avg_volume: float | None = None
    cash: float | None = None
    current_assets: float | None = None
    total_liabilities: float | None = None
    equity: float | None = None
    revenue_growth: float | None = None
    net_income: float | None = None
    free_cash_flow: float | None = None
    shares_short: float | None = None
    shares_outstanding: float | None = None
    trailing_1y_return: float | None = None
    realized_vol_1y: float | None = None
    has_options: bool = False
    furthest_expiry: str | None = None
    has_us_filing: bool = False
    nearest_debt_maturity: str | None = None
    going_concern_flag: bool | None = None
    ch11_mentions: int | None = None
    bond_price: float | None = None
    bond_yield: float | None = None
    bond_last_traded: str | None = None
    errors: list[str] = field(default_factory=list)


def _fresh(ts: datetime | None, hours: int) -> bool:
    if ts is None:
        return False
    ts = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts) < timedelta(hours=hours)


def _log_err(db: Session, run_id: int, ticker: str | None, stage: str, e: Exception) -> None:
    db.add(
        TickerError(
            run_id=run_id,
            ticker=ticker,
            stage=stage,
            error_class=type(e).__name__,
            error_message=str(e)[:500],
        )
    )


class ProgressTracker:
    """Increments run.progress_done as workers finish; commits every N ticks."""

    def __init__(self, db: Session, run: RefreshRun | None, total: int, flush_every: int = 20):
        self.db = db
        self.run = run
        self.total = total
        self.done = 0
        self.flush_every = flush_every
        if run is not None:
            run.progress_done = 0
            run.progress_total = total
            db.commit()

    def tick(self) -> None:
        self.done += 1
        if self.run is None:
            return
        if self.done % self.flush_every == 0 or self.done == self.total:
            self.run.progress_done = self.done
            self.db.commit()


# ---------------------------------------------------------------------------
# Stage A — universe
# ---------------------------------------------------------------------------
async def stage_universe(db: Session) -> list[Row]:
    rows = await universe.fetch_universe()
    cik_map = await universe.fetch_sec_ticker_cik_map()
    now = utcnow()
    out: list[Row] = []
    existing = {t.ticker: t for t in db.execute(select(Ticker)).scalars()}
    seen_symbols: set[str] = set()
    for r in rows:
        sym = r.ticker.upper()
        seen_symbols.add(sym)
        exch = r.exchange if r.exchange in ("NYSE", "NASDAQ", "NYSE_AMERICAN") else r.exchange
        cik = cik_map.get(sym)
        tk = existing.get(sym)
        if tk is None:
            tk = Ticker(
                ticker=sym,
                name=r.name,
                exchange=exch,
                cik=cik,
                financial_status=r.financial_status,
                is_active=True,
                first_seen_at=now,
                last_seen_at=now,
            )
            db.add(tk)
        else:
            tk.name = r.name
            tk.exchange = exch
            tk.cik = cik or tk.cik
            tk.financial_status = r.financial_status
            tk.is_active = True
            tk.last_seen_at = now
        out.append(
            Row(
                ticker=sym,
                name=r.name,
                exchange=exch,
                financial_status=r.financial_status,
                cik=cik,
            )
        )
    # Mark disappeared tickers inactive
    for sym, tk in existing.items():
        if sym not in seen_symbols:
            tk.is_active = False
    db.commit()
    return out


# ---------------------------------------------------------------------------
# Stage B — cheap filter: price, mcap, avg volume
# ---------------------------------------------------------------------------
async def stage_cheap_filter(
    db: Session, rows: list[Row], run_id: int, *, force: bool, run: RefreshRun | None = None
) -> list[Row]:
    max_age = settings.incremental_max_age_hours
    cached: dict[str, FundamentalsCache] = {
        f.ticker: f for f in db.execute(select(FundamentalsCache)).scalars()
    }
    prog = ProgressTracker(db, run, total=len(rows))

    async def work(r: Row) -> None:
        try:
            c = cached.get(r.ticker)
            if not force and c and _fresh(c.fetched_at, max_age) and c.price is not None:
                r.price = c.price
                r.market_cap = c.market_cap
                r.avg_volume = c.avg_volume
                return
            try:
                fi = await yfinance_client.fast_info(r.ticker)
            except Exception as e:  # noqa: BLE001
                _log_err(db, run_id, r.ticker, "cheap_filter", e)
                return
            r.price = fi.price
            r.market_cap = fi.market_cap
            r.avg_volume = fi.avg_volume
            if c is None:
                c = FundamentalsCache(ticker=r.ticker)
                db.add(c)
            c.price = fi.price
            c.market_cap = fi.market_cap
            c.avg_volume = fi.avg_volume
            c.fetched_at = utcnow()
        finally:
            prog.tick()

    await asyncio.gather(*(work(r) for r in rows), return_exceptions=False)
    db.commit()
    # Pre-apply minimal gates to prune before options fetch
    keep = [
        r
        for r in rows
        if r.price is not None
        and r.price >= 0.20
        and r.market_cap is not None
        and r.market_cap >= 10_000_000
        and r.avg_volume is not None
        and r.avg_volume >= 100_000
        and r.exchange in ("NYSE", "NASDAQ", "NYSE_AMERICAN")
        and r.financial_status != "Q"
    ]
    log.info("Cheap filter: %d → %d", len(rows), len(keep))
    return keep


# ---------------------------------------------------------------------------
# Stage C — options gate
# ---------------------------------------------------------------------------
async def stage_options(
    db: Session, rows: list[Row], run_id: int, *, force: bool, run: RefreshRun | None = None
) -> list[Row]:
    max_age = settings.incremental_max_age_hours
    cached: dict[str, OptionsCache] = {
        o.ticker: o for o in db.execute(select(OptionsCache)).scalars()
    }
    prog = ProgressTracker(db, run, total=len(rows))

    async def work(r: Row) -> None:
        try:
            c = cached.get(r.ticker)
            if not force and c and _fresh(c.fetched_at, max_age):
                r.has_options = c.has_options
                r.furthest_expiry = c.furthest_expiry
                return
            try:
                expiries = await yfinance_client.option_expiries(r.ticker)
            except Exception as e:  # noqa: BLE001
                _log_err(db, run_id, r.ticker, "options", e)
                expiries = []
            r.has_options = bool(expiries)
            r.furthest_expiry = expiries[-1] if expiries else None
            if c is None:
                c = OptionsCache(ticker=r.ticker)
                db.add(c)
            c.has_options = r.has_options
            c.furthest_expiry = r.furthest_expiry
            c.fetched_at = utcnow()
        finally:
            prog.tick()

    await asyncio.gather(*(work(r) for r in rows))
    db.commit()
    keep = [r for r in rows if r.has_options]
    log.info("Options gate: %d → %d", len(rows), len(keep))
    return keep


# ---------------------------------------------------------------------------
# Stage D — filer check via EDGAR submissions (drops ADRs/20-F)
# ---------------------------------------------------------------------------
async def stage_filer_check(
    db: Session, rows: list[Row], run_id: int, *, force: bool, run: RefreshRun | None = None
) -> list[Row]:
    max_age_days = 7  # submissions change when a new filing drops
    cached: dict[str, EdgarCompanyCache] = {
        c.cik: c for c in db.execute(select(EdgarCompanyCache)).scalars()
    }
    prog = ProgressTracker(db, run, total=len(rows))

    async def work(r: Row) -> None:
        try:
            if not r.cik:
                return
            c = cached.get(r.cik)
            if not force and c and _fresh(c.submissions_fetched_at, hours=24 * max_age_days):
                r.has_us_filing = bool(c.has_us_filing)
                return
            try:
                lf = await edgar.latest_10k_10q(r.cik)
            except Exception as e:  # noqa: BLE001
                _log_err(db, run_id, r.ticker, "filer_check", e)
                return
            has_us = bool(lf.latest_10k_accession or lf.latest_10q_accession)
            r.has_us_filing = has_us
            if c is None:
                c = EdgarCompanyCache(cik=r.cik, ticker=r.ticker)
                db.add(c)
            c.ticker = r.ticker
            c.latest_10k_accession = lf.latest_10k_accession
            c.latest_10k_primary_doc = lf.latest_10k_primary_doc
            c.latest_10k_filed = (
                datetime.combine(lf.latest_10k_filed, datetime.min.time()) if lf.latest_10k_filed else None
            )
            c.latest_10q_accession = lf.latest_10q_accession
            c.latest_10q_primary_doc = lf.latest_10q_primary_doc
            c.latest_10q_filed = (
                datetime.combine(lf.latest_10q_filed, datetime.min.time()) if lf.latest_10q_filed else None
            )
            c.has_us_filing = has_us
            c.submissions_fetched_at = utcnow()
        finally:
            prog.tick()

    await asyncio.gather(*(work(r) for r in rows))
    db.commit()
    keep = [r for r in rows if r.has_us_filing]
    log.info("Filer check: %d → %d", len(rows), len(keep))
    return keep


# ---------------------------------------------------------------------------
# Stage E — history / 1y return / realized vol
# ---------------------------------------------------------------------------
async def stage_history(
    db: Session, rows: list[Row], run_id: int, *, force: bool, run: RefreshRun | None = None
) -> None:
    max_age = settings.incremental_max_age_hours
    cached: dict[str, HistoryCache] = {
        h.ticker: h for h in db.execute(select(HistoryCache)).scalars()
    }

    todo: list[Row] = []
    for r in rows:
        c = cached.get(r.ticker)
        if not force and c and _fresh(c.fetched_at, max_age):
            r.trailing_1y_return = c.trailing_1y_return
            r.realized_vol_1y = c.realized_vol_1y
        else:
            todo.append(r)

    prog = ProgressTracker(db, run, total=len(todo), flush_every=1)

    # Batch in chunks of 50
    CHUNK = 50
    for i in range(0, len(todo), CHUNK):
        batch = todo[i : i + CHUNK]
        tickers = [r.ticker for r in batch]
        try:
            results = await yfinance_client.history_batch(tickers, period="1y")
        except Exception as e:  # noqa: BLE001
            for r in batch:
                _log_err(db, run_id, r.ticker, "history", e)
            continue
        for r in batch:
            ret, vol = results.get(r.ticker, (None, None))
            r.trailing_1y_return = ret
            r.realized_vol_1y = vol
            c = cached.get(r.ticker)
            if c is None:
                c = HistoryCache(ticker=r.ticker)
                db.add(c)
            c.trailing_1y_return = ret
            c.realized_vol_1y = vol
            c.fetched_at = utcnow()
            prog.tick()
        db.commit()
    log.info("History: computed for %d tickers", len(todo))


# ---------------------------------------------------------------------------
# Stage F — fundamentals (balance sheet, income, cashflow, info)
# ---------------------------------------------------------------------------
async def stage_fundamentals(
    db: Session, rows: list[Row], run_id: int, *, force: bool, run: RefreshRun | None = None
) -> None:
    max_age = settings.incremental_max_age_hours
    cached: dict[str, FundamentalsCache] = {
        f.ticker: f for f in db.execute(select(FundamentalsCache)).scalars()
    }
    prog = ProgressTracker(db, run, total=len(rows))

    async def work(r: Row) -> None:
        try:
            c = cached.get(r.ticker)
            if (
                not force
                and c
                and _fresh(c.fetched_at, max_age)
                and c.total_liabilities is not None
            ):
                r.cash = c.cash
                r.current_assets = c.current_assets
                r.total_liabilities = c.total_liabilities
                r.equity = c.equity
                r.revenue_growth = c.revenue_growth
                r.net_income = c.net_income
                r.free_cash_flow = c.free_cash_flow
                r.shares_short = c.shares_short
                r.shares_outstanding = c.shares_outstanding
                return
            try:
                blob, info = await yfinance_client.fundamentals(r.ticker)
            except Exception as e:  # noqa: BLE001
                _log_err(db, run_id, r.ticker, "fundamentals", e)
                return
            r.cash = blob.cash
            r.current_assets = blob.current_assets
            r.total_liabilities = blob.total_liabilities
            r.equity = blob.equity
            r.revenue_growth = blob.revenue_growth
            r.net_income = blob.net_income
            r.free_cash_flow = blob.free_cash_flow
            r.shares_short = blob.shares_short
            r.shares_outstanding = blob.shares_outstanding

            if c is None:
                c = FundamentalsCache(ticker=r.ticker)
                db.add(c)
            c.cash = blob.cash
            c.current_assets = blob.current_assets
            c.total_liabilities = blob.total_liabilities
            c.equity = blob.equity
            c.revenue_growth = blob.revenue_growth
            c.net_income = blob.net_income
            c.free_cash_flow = blob.free_cash_flow
            c.shares_short = blob.shares_short
            c.shares_outstanding = blob.shares_outstanding
            try:
                c.raw_json = json.dumps({k: info.get(k) for k in list(info)[:50]}, default=str)[:4000]
            except Exception:  # noqa: BLE001
                c.raw_json = None
            c.fetched_at = utcnow()
        finally:
            prog.tick()

    await asyncio.gather(*(work(r) for r in rows))
    db.commit()
    log.info("Fundamentals: processed %d rows", len(rows))


# ---------------------------------------------------------------------------
# Stage G — EDGAR filings analysis: going concern, Ch.11, debt maturity
# ---------------------------------------------------------------------------
async def _xbrl_backfill(r: Row, facts: dict | None) -> None:
    """Fill missing balance-sheet fields from XBRL when yfinance was sparse."""
    balance = edgar.extract_xbrl_balance(facts)
    r.cash = r.cash if r.cash is not None else balance["cash"]
    r.current_assets = r.current_assets if r.current_assets is not None else balance["current_assets"]
    r.total_liabilities = (
        r.total_liabilities if r.total_liabilities is not None else balance["total_liabilities"]
    )
    r.equity = r.equity if r.equity is not None else balance["equity"]
    r.net_income = r.net_income if r.net_income is not None else balance["net_income"]
    r.free_cash_flow = r.free_cash_flow if r.free_cash_flow is not None else balance["fcf"]
    r.shares_outstanding = (
        r.shares_outstanding if r.shares_outstanding is not None else balance["shares_out"]
    )


async def stage_filings(
    db: Session, rows: list[Row], run_id: int, *, force: bool, run: RefreshRun | None = None
) -> None:
    companies: dict[str, EdgarCompanyCache] = {
        c.cik: c for c in db.execute(select(EdgarCompanyCache)).scalars() if c.cik
    }
    flags_by_accession: dict[str, FilingsFlags] = {
        f.accession: f for f in db.execute(select(FilingsFlags)).scalars()
    }
    maturities_by_cik: dict[str, DebtMaturity] = {
        m.cik: m for m in db.execute(select(DebtMaturity)).scalars()
    }
    prog = ProgressTracker(db, run, total=len(rows))

    async def work(r: Row) -> None:
        try:
            if not r.cik:
                return
            c = companies.get(r.cik)
            if c is None:
                return
            candidates = [
                (c.latest_10k_accession, c.latest_10k_primary_doc, c.latest_10k_filed, "10-K"),
                (c.latest_10q_accession, c.latest_10q_primary_doc, c.latest_10q_filed, "10-Q"),
            ]
            candidates = [x for x in candidates if x[0] and x[1]]
            candidates.sort(key=lambda x: x[2] or datetime.min, reverse=True)

            gc_flag = False
            ch11 = 0
            if candidates:
                acc, primary, filed, form = candidates[0]
                cached_flag = flags_by_accession.get(acc)
                if cached_flag and not force:
                    gc_flag = cached_flag.going_concern_flag
                    ch11 = cached_flag.ch11_mention_count
                else:
                    try:
                        has_phrase = (
                            await edgar.fulltext_has_phrase(r.cik, acc, "substantial doubt")
                            or await edgar.fulltext_has_phrase(r.cik, acc, "chapter 11")
                        )
                    except Exception as e:  # noqa: BLE001
                        _log_err(db, run_id, r.ticker, "filings_fulltext", e)
                        has_phrase = True
                    if has_phrase:
                        try:
                            html = await edgar.fetch_filing_html(r.cik, acc, primary)
                            gc_flag, ch11 = analyze_filing_html(html)
                        except Exception as e:  # noqa: BLE001
                            _log_err(db, run_id, r.ticker, "filings_fetch", e)
                    filed_dt = filed if isinstance(filed, datetime) else None
                    if cached_flag is None:
                        cached_flag = FilingsFlags(accession=acc, cik=r.cik)
                        db.add(cached_flag)
                    cached_flag.form_type = form
                    cached_flag.filed_at = filed_dt
                    cached_flag.going_concern_flag = gc_flag
                    cached_flag.ch11_mention_count = ch11
                    cached_flag.fetched_at = utcnow()
            r.going_concern_flag = gc_flag
            r.ch11_mentions = ch11

            m = maturities_by_cik.get(r.cik)
            need_xbrl = force or m is None or not _fresh(m.fetched_at, 24 * 7)
            facts: dict | None = None
            if need_xbrl:
                try:
                    facts = await edgar.fetch_companyfacts(r.cik)
                except Exception as e:  # noqa: BLE001
                    _log_err(db, run_id, r.ticker, "xbrl", e)
                    facts = None
                d_iso, src = edgar.extract_nearest_debt_maturity(facts)
                if m is None:
                    m = DebtMaturity(cik=r.cik)
                    db.add(m)
                m.nearest_maturity_date = d_iso
                m.source_fact = src
                m.fetched_at = utcnow()
                if c is not None:
                    c.companyfacts_fetched_at = utcnow()
                r.nearest_debt_maturity = d_iso
            else:
                r.nearest_debt_maturity = m.nearest_maturity_date

            if facts is not None:
                await _xbrl_backfill(r, facts)
        finally:
            prog.tick()

    await asyncio.gather(*(work(r) for r in rows))
    db.commit()
    log.info("Filings analysis: done for %d rows", len(rows))


# ---------------------------------------------------------------------------
# Stage — attach FINRA short interest + bond overrides
# ---------------------------------------------------------------------------
async def stage_shortint_and_bonds(db: Session, rows: list[Row], run_id: int) -> None:
    # FINRA: refresh bi-monthly file if latest row is >14 days old
    latest_date = db.execute(
        select(ShortInterestFinra.settlement_date)
        .order_by(ShortInterestFinra.settlement_date.desc())
        .limit(1)
    ).scalar()
    need_fetch = True
    if latest_date:
        try:
            days_old = (datetime.now(timezone.utc).date() - datetime.strptime(latest_date, "%Y-%m-%d").date()).days
            need_fetch = days_old > 14
        except ValueError:
            need_fetch = True
    if need_fetch:
        try:
            finra_rows = await finra_shortint.fetch_latest_short_interest()
        except Exception as e:  # noqa: BLE001
            _log_err(db, run_id, None, "finra", e)
            finra_rows = []
        for fr in finra_rows:
            existing = db.get(ShortInterestFinra, (fr.ticker, fr.settlement_date))
            if existing:
                existing.short_interest = fr.short_interest
                existing.avg_daily_volume = fr.avg_daily_volume
            else:
                db.add(
                    ShortInterestFinra(
                        ticker=fr.ticker,
                        settlement_date=fr.settlement_date,
                        short_interest=fr.short_interest,
                        avg_daily_volume=fr.avg_daily_volume,
                    )
                )
        db.commit()

    # Attach latest FINRA short_interest per ticker; fallback to yfinance sharesShort (already on row)
    finra_latest: dict[str, float] = {}
    q = db.execute(
        select(ShortInterestFinra.ticker, ShortInterestFinra.short_interest, ShortInterestFinra.settlement_date)
    ).all()
    # keep the max settlement date per ticker
    latest_per: dict[str, tuple[str, float | None]] = {}
    for t, si, d in q:
        cur = latest_per.get(t)
        if cur is None or d > cur[0]:
            latest_per[t] = (d, si)
    for t, (_, si) in latest_per.items():
        if si is not None:
            finra_latest[t] = si

    overrides = {
        o.ticker: o for o in db.execute(select(BondManualOverride)).scalars()
    }
    for r in rows:
        finra_si = finra_latest.get(r.ticker)
        if finra_si is not None:
            r.shares_short = finra_si
        o = overrides.get(r.ticker)
        if o:
            r.bond_price = safe_float(o.price)
            r.bond_yield = safe_float(o.yield_pct)
            r.bond_last_traded = o.last_traded_date
