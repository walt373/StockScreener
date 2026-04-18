from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import get_session
from ..models import RefreshRun, ScreenerResult
from ..screeners import get_screener, list_screeners
from ..util.numbers import iso_utc

router = APIRouter()


@router.get("/screeners")
def api_list_screeners() -> dict[str, Any]:
    out = []
    for s in list_screeners():
        m = s.meta
        out.append(
            {
                "id": m.id,
                "name": m.name,
                "description": m.description,
                "columns": [asdict(c) for c in m.columns],
                "default_sort": {"key": m.default_sort_key, "dir": m.default_sort_dir},
            }
        )
    return {"screeners": out}


@router.get("/screeners/{screener_id}/results")
def api_screener_results(
    screener_id: str,
    db: Session = Depends(get_session),
) -> dict[str, Any]:
    s = get_screener(screener_id)
    if s is None:
        raise HTTPException(404, f"unknown screener: {screener_id}")

    # Latest completed (status=ok) run
    run = db.execute(
        select(RefreshRun)
        .where(RefreshRun.status == "ok")
        .order_by(RefreshRun.finished_at.desc())
        .limit(1)
    ).scalar_one_or_none()

    if run is None:
        return {
            "screener": {
                "id": s.meta.id,
                "name": s.meta.name,
                "description": s.meta.description,
                "columns": [asdict(c) for c in s.meta.columns],
                "default_sort": {"key": s.meta.default_sort_key, "dir": s.meta.default_sort_dir},
            },
            "run": None,
            "rows": [],
        }

    results = db.execute(
        select(ScreenerResult)
        .where(
            ScreenerResult.run_id == run.id,
            ScreenerResult.screener_id == screener_id,
        )
        .order_by(ScreenerResult.rank.asc())
    ).scalars().all()

    return {
        "screener": {
            "id": s.meta.id,
            "name": s.meta.name,
            "description": s.meta.description,
            "columns": [asdict(c) for c in s.meta.columns],
            "default_sort": {"key": s.meta.default_sort_key, "dir": s.meta.default_sort_dir},
        },
        "run": {
            "id": run.id,
            "started_at": iso_utc(run.started_at),
            "finished_at": iso_utc(run.finished_at),
            "tickers_in": run.tickers_in,
            "tickers_out": run.tickers_out,
        },
        "rows": [
            {
                "ticker": r.ticker,
                "name": r.name,
                "exchange": r.exchange,
                "price": r.price,
                "market_cap": r.market_cap,
                "avg_volume": r.avg_volume,
                "cash": r.cash,
                "current_assets": r.current_assets,
                "total_liabilities": r.total_liabilities,
                "equity": r.equity,
                "revenue_growth": r.revenue_growth,
                "short_interest": r.short_interest,
                "trailing_1y_return": r.trailing_1y_return,
                "realized_vol_1y": r.realized_vol_1y,
                "furthest_option_expiry": r.furthest_option_expiry,
                "ni_over_mcap": r.ni_over_mcap,
                "fcf_over_mcap": r.fcf_over_mcap,
                "nearest_debt_maturity": r.nearest_debt_maturity,
                "bond_price": r.bond_price,
                "bond_yield": r.bond_yield,
                "bond_last_traded": r.bond_last_traded,
                "going_concern_flag": r.going_concern_flag,
                "ch11_mentions": r.ch11_mentions,
            }
            for r in results
        ],
    }
