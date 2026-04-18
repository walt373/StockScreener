from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import get_session
from ..models import RefreshRun
from ..util.numbers import iso_utc

router = APIRouter()


@router.get("/health")
def health(db: Session = Depends(get_session)) -> dict:
    last_ok = db.execute(
        select(RefreshRun).where(RefreshRun.status == "ok").order_by(RefreshRun.finished_at.desc()).limit(1)
    ).scalar_one_or_none()
    return {
        "ok": True,
        "db": "ok",
        "last_successful_run_at": iso_utc(last_ok.finished_at) if last_ok else None,
    }
