from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from ..config import settings
from .runner import runner

log = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def _nightly_job() -> None:
    # Called from scheduler thread — schedule the task on the main loop.
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        log.error("nightly job: no running event loop")
        return

    def _kick() -> None:
        if runner.is_running():
            log.info("nightly refresh skipped: one already running")
            return
        run_id, status = runner.start(force=False)
        log.info("nightly refresh started run_id=%s (%s)", run_id, status)

    loop.call_soon_threadsafe(_kick)


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    _scheduler = AsyncIOScheduler()
    trigger = CronTrigger(
        hour=settings.nightly_refresh_hour,
        minute=settings.nightly_refresh_minute,
    )
    _scheduler.add_job(_nightly_job, trigger, id="nightly_refresh", replace_existing=True)
    _scheduler.start()
    log.info(
        "Scheduler started — nightly at %02d:%02d",
        settings.nightly_refresh_hour,
        settings.nightly_refresh_minute,
    )


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
