"""CLI wrapper around the refresh pipeline.

Usage:
    python scripts/run_refresh.py [--force] [--limit N] [--screener bankruptcy]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Allow running from repo root OR from backend/
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import screeners  # noqa: E402,F401  triggers registration
from app.db import init_db  # noqa: E402
from app.jobs.runner import reconcile_orphaned  # noqa: E402
from app.pipeline.orchestrator import run_refresh  # noqa: E402
from app.util.http import close_clients  # noqa: E402


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Ignore cache freshness")
    parser.add_argument("--limit", type=int, default=None, help="Cap universe size (debug)")
    parser.add_argument(
        "--screener",
        action="append",
        default=None,
        help="Screener id(s) to materialize; default = all registered",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    init_db()
    reconcile_orphaned()
    run_id = await run_refresh(screener_ids=args.screener, force=args.force, limit=args.limit)
    print(f"run_id={run_id}")
    await close_clients()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
