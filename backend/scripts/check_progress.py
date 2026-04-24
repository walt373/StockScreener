"""Quick liveness check for the current refresh run.

Usage:
    python scripts/check_progress.py

Run it twice in ~30s and compare `recently_updated_10min` — if it's
growing, the pipeline is making progress; if flat, it's genuinely stuck.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings  # noqa: E402


def main() -> int:
    db = sqlite3.connect(settings.db_path)
    db.row_factory = sqlite3.Row

    def q(sql: str, title: str) -> None:
        print(f"\n=== {title} ===")
        for row in db.execute(sql):
            print(dict(row))

    q(
        "SELECT id, stage, progress_done, progress_total, status, started_at, error_summary "
        "FROM refresh_runs ORDER BY id DESC LIMIT 3",
        "Recent runs",
    )
    q(
        "SELECT COUNT(*) AS recently_updated_10min "
        "FROM edgar_company_cache "
        "WHERE submissions_fetched_at > datetime('now', '-10 minutes')",
        "EDGAR company cache activity (last 10m)",
    )
    q(
        "SELECT stage, COUNT(*) AS count, error_class, "
        "  MIN(occurred_at) AS first_seen, MAX(occurred_at) AS last_seen "
        "FROM ticker_errors "
        "GROUP BY stage, error_class "
        "ORDER BY count DESC LIMIT 10",
        "Errors by stage + class",
    )
    q(
        "SELECT ticker, stage, error_class, error_message "
        "FROM ticker_errors "
        "ORDER BY occurred_at DESC LIMIT 5",
        "Most recent errors (full detail)",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
