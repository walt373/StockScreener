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

_STAGE_NAMES = [
    "universe",
    "cheap_filter",
    "options",
    "filer_check",
    "history",
    "fundamentals",
    "filings",
    "shortint_bonds",
    "materialize",
]


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

    # B. Cheap filter
    _set_stage(db, run, "cheap_filter", 0, len(rows))
    rows = await stages.stage_cheap_filter(db, rows, run.id, force=force)
    _set_stage(db, run, "cheap_filter", len(rows), len(rows))

    # C. Options gate
    _set_stage(db, run, "options", 0, len(rows))
    rows = await stages.stage_options(db, rows, run.id, force=force)
    _set_stage(db, run, "options", len(rows), len(rows))

    # D. Filer check
    _set_stage(db, run, "filer_check", 0, len(rows))
    rows = await stages.stage_filer_check(db, rows, run.id, force=force)
    _set_stage(db, run, "filer_check", len(rows), len(rows))

    # E. History
    _set_stage(db, run, "history", 0, len(rows))
    await stages.stage_history(db, rows, run.id, force=force)
    _set_stage(db, run, "history", len(rows), len(rows))

    # F. Fundamentals
    _set_stage(db, run, "fundamentals", 0, len(rows))
    await stages.stage_fundamentals(db, rows, run.id, force=force)
    _set_stage(db, run, "fundamentals", len(rows), len(rows))

    # G. Filings
    _set_stage(db, run, "filings", 0, len(rows))
    await stages.stage_filings(db, rows, run.id, force=force)
    _set_stage(db, run, "filings", len(rows), len(rows))

    # Shortint + bonds
    _set_stage(db, run, "shortint_bonds", 0, 1)
    await stages.stage_shortint_and_bonds(db, rows, run.id)
    _set_stage(db, run, "shortint_bonds", 1, 1)

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
                exchange=p.get("exchange"),
                price=p.get("price"),
                market_cap=p.get("market_cap"),
                avg_volume=p.get("avg_volume"),
                cash=p.get("cash"),
                current_assets=p.get("current_assets"),
                total_liabilities=p.get("total_liabilities"),
                equity=p.get("equity"),
                revenue_growth=p.get("revenue_growth"),
                short_interest=p.get("short_interest"),
                trailing_1y_return=p.get("trailing_1y_return"),
                realized_vol_1y=p.get("realized_vol_1y"),
                furthest_option_expiry=p.get("furthest_option_expiry"),
                ni_over_mcap=p.get("ni_over_mcap"),
                fcf_over_mcap=p.get("fcf_over_mcap"),
                nearest_debt_maturity=p.get("nearest_debt_maturity"),
                bond_price=p.get("bond_price"),
                bond_yield=p.get("bond_yield"),
                bond_last_traded=p.get("bond_last_traded"),
                going_concern_flag=p.get("going_concern_flag"),
                ch11_mentions=p.get("ch11_mentions"),
            )
        )
    db.commit()
    return len(projected)
