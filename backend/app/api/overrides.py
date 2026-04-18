from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..db import get_session
from ..models import BondManualOverride, utcnow

router = APIRouter()


class BondOverrideInput(BaseModel):
    price: float | None = None
    yield_pct: float | None = None
    last_traded_date: str | None = None  # ISO YYYY-MM-DD
    notes: str | None = None


@router.get("/bond-overrides")
def list_overrides(db: Session = Depends(get_session)) -> dict[str, Any]:
    rows = db.query(BondManualOverride).order_by(BondManualOverride.ticker.asc()).all()
    return {
        "overrides": [
            {
                "ticker": r.ticker,
                "price": r.price,
                "yield_pct": r.yield_pct,
                "last_traded_date": r.last_traded_date,
                "notes": r.notes,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            }
            for r in rows
        ]
    }


@router.put("/bond-overrides/{ticker}")
def upsert_override(
    ticker: str,
    body: BondOverrideInput,
    db: Session = Depends(get_session),
) -> dict[str, Any]:
    ticker = ticker.upper()
    existing = db.get(BondManualOverride, ticker)
    if existing is None:
        existing = BondManualOverride(ticker=ticker)
        db.add(existing)
    existing.price = body.price
    existing.yield_pct = body.yield_pct
    existing.last_traded_date = body.last_traded_date
    existing.notes = body.notes
    existing.updated_at = utcnow()
    db.commit()
    return {"ticker": ticker, "ok": True}


@router.delete("/bond-overrides/{ticker}")
def delete_override(
    ticker: str,
    db: Session = Depends(get_session),
) -> dict[str, Any]:
    ticker = ticker.upper()
    existing = db.get(BondManualOverride, ticker)
    if existing is not None:
        db.delete(existing)
        db.commit()
    return {"ticker": ticker, "ok": True}
