"""Seed the tickers table from nasdaqtrader + SEC EDGAR ticker→CIK map."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import SessionLocal, init_db  # noqa: E402
from app.pipeline.stages import stage_universe  # noqa: E402
from app.util.http import close_clients  # noqa: E402


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    init_db()
    db = SessionLocal()
    try:
        rows = await stage_universe(db)
        print(f"universe size: {len(rows)}")
    finally:
        db.close()
        await close_clients()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
