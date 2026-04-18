from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..db import get_session
from ..jobs.runner import runner
from ..models import RefreshRun
from ..util.numbers import iso_utc

router = APIRouter()


class RefreshRequest(BaseModel):
    force: bool = False
    limit: int | None = None


@router.post("/refresh", status_code=202)
async def api_refresh(req: RefreshRequest) -> dict[str, Any]:
    run_id, status = runner.start(force=req.force, limit=req.limit)
    return {"run_id": run_id, "status": status}


@router.get("/refresh/status")
def api_refresh_status(
    run_id: int | None = None,
    db: Session = Depends(get_session),
) -> dict[str, Any]:
    if run_id is None:
        run_id = runner.current_run_id()
        if run_id is None:
            # return latest run overall
            row = (
                db.query(RefreshRun)
                .order_by(RefreshRun.started_at.desc())
                .first()
            )
            if row is None:
                return {"status": "idle"}
            run_id = row.id
    run = db.get(RefreshRun, run_id)
    if run is None:
        raise HTTPException(404, f"no such run {run_id}")
    return {
        "run_id": run.id,
        "status": run.status,
        "stage": run.stage,
        "progress": {"done": run.progress_done, "total": run.progress_total},
        "started_at": iso_utc(run.started_at),
        "finished_at": iso_utc(run.finished_at),
        "tickers_in": run.tickers_in,
        "tickers_out": run.tickers_out,
        "error": run.error_summary,
    }
