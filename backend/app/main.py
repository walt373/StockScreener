from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import screeners as _screeners  # noqa: F401 triggers registration
from .api import health, overrides, refresh, screeners
from .db import init_db
from .jobs.runner import reconcile_orphaned
from .jobs.scheduler import start_scheduler, stop_scheduler
from .util.http import close_clients

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    reconcile_orphaned()
    start_scheduler()
    try:
        yield
    finally:
        stop_scheduler()
        await close_clients()


def create_app() -> FastAPI:
    app = FastAPI(title="StockScreener", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=False,
    )
    app.include_router(health.router, prefix="/api")
    app.include_router(screeners.router, prefix="/api")
    app.include_router(refresh.router, prefix="/api")
    app.include_router(overrides.router, prefix="/api")
    return app


app = create_app()
