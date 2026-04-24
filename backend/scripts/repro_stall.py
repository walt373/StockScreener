"""Minimal reproducer: run latest_10k_10q for N CIKs in parallel, print progress.

If this hangs on the user's box, I can see exactly where and what it's doing.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

from app.sources import edgar, universe  # noqa: E402
from app.util.http import close_clients  # noqa: E402

N = 700
CHUNK = 100


async def main() -> int:
    print(f"Fetching universe + CIK map…", flush=True)
    cik_map = await universe.fetch_sec_ticker_cik_map()
    ciks = list(cik_map.values())[:N]
    print(f"Have {len(ciks)} CIKs. Starting fetches (CHUNK={CHUNK})…", flush=True)

    done = 0
    errors = 0
    start = time.monotonic()

    async def work(cik: str) -> None:
        nonlocal done, errors
        try:
            await edgar.latest_10k_10q(cik)
        except Exception as e:  # noqa: BLE001
            errors += 1
            if errors <= 5:
                print(f"  ERROR {cik}: {type(e).__name__}: {e}", flush=True)
        finally:
            done += 1

    async def progress_watcher() -> None:
        last = -1
        while True:
            await asyncio.sleep(2)
            if done == last:
                print(
                    f"  [STALLED] done={done}/{N} errors={errors} elapsed={time.monotonic() - start:.1f}s",
                    flush=True,
                )
            else:
                print(
                    f"  progress: done={done}/{N} errors={errors} elapsed={time.monotonic() - start:.1f}s",
                    flush=True,
                )
            last = done
            if done >= N:
                return

    watcher = asyncio.create_task(progress_watcher())

    try:
        for i in range(0, len(ciks), CHUNK):
            batch = ciks[i : i + CHUNK]
            await asyncio.gather(*(work(c) for c in batch), return_exceptions=True)
            print(
                f"  chunk {i // CHUNK + 1} done. total={done}/{N} errors={errors}",
                flush=True,
            )
    finally:
        watcher.cancel()
        await close_clients()

    elapsed = time.monotonic() - start
    print(
        f"\nFINAL: {done}/{N} completed, {errors} errors, {elapsed:.1f}s ({N / elapsed:.1f}/sec)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
