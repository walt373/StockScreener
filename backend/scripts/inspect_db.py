"""Quick introspection of cache.db — run any time to check refresh progress.

Usage:  python scripts/inspect_db.py
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


def _db_path() -> str:
    env = os.environ.get("DB_PATH")
    if env:
        return str(Path(env).expanduser().resolve())
    here = Path(__file__).resolve().parents[1]
    return str((here / ".." / "data" / "cache.db").resolve())


def main() -> int:
    db = sqlite3.connect(_db_path())
    db.row_factory = sqlite3.Row

    def q(sql: str, title: str) -> None:
        print(f"\n=== {title} ===")
        for row in db.execute(sql):
            print(dict(row))

    q(
        "SELECT id, stage, progress_done, progress_total, status, started_at, finished_at, "
        "tickers_in, tickers_out, error_summary FROM refresh_runs ORDER BY id DESC LIMIT 5",
        "Recent refresh runs",
    )
    q("SELECT COUNT(*) AS total FROM tickers", "Tickers total")
    q(
        "SELECT COUNT(*) AS total, MIN(fetched_at) AS first, MAX(fetched_at) AS last "
        "FROM fundamentals_cache",
        "Fundamentals cache",
    )
    q(
        "SELECT COUNT(*) AS total, SUM(CASE WHEN has_options THEN 1 ELSE 0 END) AS optionable "
        "FROM options_cache",
        "Options cache",
    )
    q(
        "SELECT COUNT(*) AS total, MAX(fetched_at) AS last FROM history_cache",
        "History cache",
    )
    q(
        "SELECT stage, COUNT(*) AS count FROM ticker_errors GROUP BY stage ORDER BY count DESC",
        "Errors by stage",
    )
    q(
        "SELECT run_id, screener_id, COUNT(*) AS rows FROM screener_results "
        "GROUP BY run_id, screener_id ORDER BY run_id DESC LIMIT 5",
        "Screener results by run",
    )
    q(
        "SELECT "
        "  SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END) AS have_price, "
        "  SUM(CASE WHEN market_cap IS NOT NULL THEN 1 ELSE 0 END) AS have_mcap, "
        "  SUM(CASE WHEN avg_volume IS NOT NULL THEN 1 ELSE 0 END) AS have_vol, "
        "  SUM(CASE WHEN price >= 0.20 THEN 1 ELSE 0 END) AS pass_price, "
        "  SUM(CASE WHEN market_cap >= 10000000 THEN 1 ELSE 0 END) AS pass_mcap, "
        "  SUM(CASE WHEN avg_volume >= 100000 THEN 1 ELSE 0 END) AS pass_vol, "
        "  SUM(CASE WHEN price >= 0.20 AND market_cap >= 10000000 AND avg_volume >= 100000 "
        "         THEN 1 ELSE 0 END) AS pass_all "
        "FROM fundamentals_cache",
        "Cheap-filter coverage",
    )
    q(
        "SELECT ticker, price, market_cap, avg_volume FROM fundamentals_cache "
        "WHERE market_cap IS NOT NULL ORDER BY market_cap DESC LIMIT 5",
        "Top 5 mcap (sanity check)",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
