from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any

from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import RefreshRun, ScreenerResult, utcnow
from ..screeners import get_screener, list_screeners
from . import stages

log = logging.getLogger(__name__)


def _set_stage(db: Session, run: RefreshRun, name: str, done: int, total: int) -> None:
    run.stage = name
    run.progress_done = done
    run.progress_total = total
    db.commit()


async def run_refresh(
    *,
    screener_ids: list[str] | None = None,
    force: bool = False,
    limit: int | None = None,
    run_id: int | None = None,
) -> int:
    """Executes the full pipeline. Returns the refresh_runs.id."""
    db: Session = SessionLocal()
    try:
        if run_id is None:
            run = RefreshRun(
                screener_id=",".join(screener_ids) if screener_ids else None,
                status="running",
                force=force,
                limit=limit,
            )
            db.add(run)
            db.commit()
            run_id = run.id
        else:
            run = db.get(RefreshRun, run_id)
            if run is None:
                raise RuntimeError(f"refresh run {run_id} not found")

        try:
            await _run_stages(db, run, screener_ids=screener_ids, force=force, limit=limit)
            run.status = "ok"
            run.finished_at = utcnow()
            db.commit()
        except Exception as e:  # noqa: BLE001
            log.exception("refresh failed")
            run.status = "error"
            run.error_summary = f"{type(e).__name__}: {e}"[:2000]
            run.finished_at = utcnow()
            db.commit()
            raise
        return run.id
    finally:
        db.close()


async def _run_stages(
    db: Session,
    run: RefreshRun,
    *,
    screener_ids: list[str] | None,
    force: bool,
    limit: int | None,
) -> None:
    ids = screener_ids or [s.meta.id for s in list_screeners()]
    screeners = [get_screener(sid) for sid in ids if get_screener(sid)]
    if not screeners:
        raise RuntimeError(f"no screeners matched: {ids}")

    # A. Universe
    _set_stage(db, run, "universe", 0, 1)
    rows = await stages.stage_universe(db)
    if limit:
        rows = rows[:limit]
    run.tickers_in = len(rows)
    db.commit()
    _set_stage(db, run, "universe", 1, 1)

    # B. Batch market data (price + volume + 1y history) — ONE yf.download per 50 tickers
    run.stage = "batch_market"
    db.commit()
    await stages.stage_batch_market(db, rows, run.id, force=force, run=run)

    # B.5. Bulk optionable-symbol tag — CBOE/Nasdaqtrader, one fetch, no per-ticker yfinance.
    run.stage = "optionable_tag"
    db.commit()
    await stages.stage_optionable_tag(db, rows, run.id, force=force, run=run)

    # B.6. PRE-FILTER — batch-data-only gate (price / vol / exchange / has_options /
    # not-bankrupt). Drops most of universe before EDGAR.
    run.stage = "pre_filter"
    db.commit()
    before = len(rows)
    rows = [r for r in rows if any(s.pre_filter(asdict(r)) for s in screeners)]
    log.info("Pre-filter: %d → %d", before, len(rows))

    # C. Filer check (EDGAR submissions — always refresh; short 1h TTL within a run).
    # Populates latest 10-K / 10-Q accession numbers we need for cache invalidation.
    run.stage = "filer_check"
    db.commit()
    rows = await stages.stage_filer_check(db, rows, run.id, force=force, run=run)

    # C.5. Hydrate cached fundamentals where EDGAR's current latest accession
    # matches the accession that produced the cache. Rows without cache or with a
    # newer filing stay un-hydrated and will get fresh XBRL in stage_filings.
    stages.stage_hydrate_cached_fundamentals(db, rows)

    # C.6. CACHE FILTER — per-screener drop using freshly-hydrated cache values.
    # Skips EDGAR XBRL refetches and per-ticker yfinance option_expiries for rows
    # we already know will fail hard_filters.
    before = len(rows)
    rows = [r for r in rows if any(s.cache_filter(asdict(r)) for s in screeners)]
    log.info("Cache filter: %d → %d (cache-confirmed fails dropped)", before, len(rows))

    # D. Filings analysis + XBRL fundamentals (only rows without a fresh cache hit
    # will incur a companyfacts fetch; the rest skip via accession match).
    run.stage = "filings"
    db.commit()
    await stages.stage_filings(db, rows, run.id, force=force, run=run)

    # E. Compute mcap = price × shares_out (free, in-memory)
    stages.stage_compute_mcap(rows)

    # E.5. Pre-option-expiries drop — after we have mcap + fresh fundamentals, any
    # row that fails every screener's hard_filters is dead weight. Dropping here
    # avoids a yfinance option_expiries call per row.
    before = len(rows)
    rows = [r for r in rows if any(s.hard_filters(asdict(r)) for s in screeners)]
    log.info("Pre-expiry filter: %d → %d", before, len(rows))

    # F. Option expiries — per-ticker yfinance, bounded by hard-filter survivors.
    run.stage = "option_expiries"
    db.commit()
    await stages.stage_option_expiries(db, rows, run.id, force=force, run=run)

    # G. Shortint (FINRA) + bond overrides
    run.stage = "shortint_bonds"
    run.progress_done, run.progress_total = 0, 1
    db.commit()
    await stages.stage_shortint_and_bonds(db, rows, run.id)
    run.progress_done = 1
    db.commit()

    # H. Materialize per screener
    _set_stage(db, run, "materialize", 0, len(screeners))
    run.tickers_out = 0
    for i, s in enumerate(screeners):
        out_count = _materialize(db, run.id, s, rows)
        run.tickers_out += out_count
        _set_stage(db, run, "materialize", i + 1, len(screeners))
    db.commit()


def _materialize(db: Session, run_id: int, screener: Any, rows: list[stages.Row]) -> int:
    raw_rows: list[dict[str, Any]] = [asdict(r) for r in rows]
    filtered = [r for r in raw_rows if screener.hard_filters(r)]
    projected = [screener.project(r) for r in filtered]
    sort_key = screener.meta.default_sort_key
    reverse = screener.meta.default_sort_dir == "desc"

    def sort_tuple(r: dict[str, Any]) -> tuple:
        v = r.get(sort_key)
        if v is None:
            return (1, 0.0)
        val = float(v) if isinstance(v, (int, float)) else 0.0
        return (0, -val if reverse else val)

    projected.sort(key=sort_tuple)

    # Clear any partial rows in this run for this screener
    from sqlalchemy import delete

    db.execute(
        delete(ScreenerResult).where(
            ScreenerResult.run_id == run_id,
            ScreenerResult.screener_id == screener.meta.id,
        )
    )
    for i, p in enumerate(projected):
        db.add(
            ScreenerResult(
                run_id=run_id,
                screener_id=screener.meta.id,
                ticker=p["ticker"],
                rank=i,
                name=p.get("name"),
                sector=p.get("sector"),
                exchange=p.get("exchange"),
                price=p.get("price"),
                market_cap=p.get("market_cap"),
                avg_volume=p.get("avg_volume"),
                cash=p.get("cash"),
                current_assets=p.get("current_assets"),
                current_liabilities=p.get("current_liabilities"),
                current_ratio=p.get("current_ratio"),
                total_liabilities=p.get("total_liabilities"),
                total_assets=p.get("total_assets"),
                equity=p.get("equity"),
                liabilities_over_assets=p.get("liabilities_over_assets"),
                revenue_growth=p.get("revenue_growth"),
                short_interest=p.get("short_interest"),
                trailing_1y_return=p.get("trailing_1y_return"),
                realized_vol_1y=p.get("realized_vol_1y"),
                furthest_option_expiry=p.get("furthest_option_expiry"),
                net_income=p.get("net_income"),
                operating_cash_flow=p.get("operating_cash_flow"),
                free_cash_flow=p.get("free_cash_flow"),
                ni_over_mcap=p.get("ni_over_mcap"),
                fcf_over_mcap=p.get("fcf_over_mcap"),
                price_to_book=p.get("price_to_book"),
                nearest_debt_maturity=p.get("nearest_debt_maturity"),
                bond_price=p.get("bond_price"),
                bond_yield=p.get("bond_yield"),
                bond_last_traded=p.get("bond_last_traded"),
                going_concern_flag=p.get("going_concern_flag"),
                ch11_mentions=p.get("ch11_mentions"),
                nt_10k_filed_at=p.get("nt_10k_filed_at"),
                nt_10q_filed_at=p.get("nt_10q_filed_at"),
            )
        )
    db.commit()
    return len(projected)
