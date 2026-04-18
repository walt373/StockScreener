from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import RefreshRun, utcnow
from ..pipeline.orchestrator import run_refresh

log = logging.getLogger(__name__)


class RefreshRunner:
    """Single-writer guard + in-process task handle for the refresh pipeline."""

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._current_run_id: int | None = None

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def current_run_id(self) -> int | None:
        return self._current_run_id if self.is_running() else None

    def start(self, *, force: bool = False, limit: int | None = None) -> tuple[int, str]:
        """Kick off a refresh. Returns (run_id, status) where status is 'queued' or 'already_running'."""
        db: Session = SessionLocal()
        try:
            # Persist the run row first so we can return its id synchronously
            if self.is_running() and self._current_run_id is not None:
                return self._current_run_id, "already_running"
            run = RefreshRun(status="running", force=force, limit=limit)
            db.add(run)
            db.commit()
            run_id = run.id
        finally:
            db.close()

        self._current_run_id = run_id
        self._task = asyncio.create_task(self._run(run_id, force, limit))
        return run_id, "queued"

    async def _run(self, run_id: int, force: bool, limit: int | None) -> None:
        try:
            await run_refresh(force=force, limit=limit, run_id=run_id)
        except Exception:
            log.exception("refresh run %s failed", run_id)
        finally:
            self._current_run_id = None


def reconcile_orphaned(stale_after_hours: int = 2) -> int:
    """Mark any 'running' refresh rows older than N hours as crashed errors.
    Called at app startup.
    """
    cutoff = utcnow() - timedelta(hours=stale_after_hours)
    db = SessionLocal()
    count = 0
    try:
        stmt = select(RefreshRun).where(RefreshRun.status == "running", RefreshRun.started_at < cutoff)
        for run in db.execute(stmt).scalars():
            run.status = "error"
            run.error_summary = "process crashed (orphan reconciled at startup)"
            run.finished_at = utcnow()
            count += 1
        db.commit()
    finally:
        db.close()
    if count:
        log.warning("Reconciled %d orphaned refresh run(s)", count)
    return count


runner = RefreshRunner()
