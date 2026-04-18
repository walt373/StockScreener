# StockScreener

Local-only webapp for screening NYSE/Nasdaq equities. First screener: **bankruptcy candidates** (distressed names for buying puts). Second screener (**strong balance sheets**) slots into the same pipeline later.

- Backend: Python 3.11+ / FastAPI / SQLite (WAL) / APScheduler
- Frontend: React + Vite + TypeScript, TanStack Query + Table
- Data: free-tier only — yfinance, SEC EDGAR, FINRA

## Prerequisites

- Python 3.11+ (3.14 tested)
- Node.js 18+ (install from https://nodejs.org — this machine doesn't have it yet)

## One-time setup

```bash
# Backend
cd backend
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"
cp .env.example .env
# edit .env to set SEC_USER_AGENT="Your Name your@email.com"

# Frontend
cd ../frontend
npm install
```

## Run

Two terminals:

```bash
# Terminal 1 — backend
cd backend
.venv/Scripts/python -m uvicorn app.main:app --reload --port 8000

# Terminal 2 — frontend
cd frontend
npm run dev
# open http://localhost:5173
```

## First refresh

From the UI, click **Refresh** (or **Force Refresh**). Or from CLI:

```bash
cd backend
.venv/Scripts/python scripts/bootstrap_universe.py      # seeds the tickers table (~5500 rows)
.venv/Scripts/python scripts/run_refresh.py --force     # full scan, ~15-25 min
# Debug / smoke test:
.venv/Scripts/python scripts/run_refresh.py --limit 25
```

A nightly refresh runs automatically at the time set in `.env` (`NIGHTLY_REFRESH_HOUR`, default 02:00 local time).

## Hard filters for screener #1

- Exchange: NYSE, NYSE American, or NASDAQ
- Market cap ≥ $10M
- Price ≥ $0.20
- Avg volume ≥ 100K
- Has listed options
- Not already bankrupt (nasdaqtrader `Financial Status` ≠ `Q`)
- US-domiciled filer (has a 10-K or 10-Q on EDGAR — drops ADRs that file 20-F)

Default sort: `trailing_1y_return` ascending (biggest decliners first).

## Bond columns

`Bond price`, `Bond yield`, `Bond last traded` — not feasible from free sources at scale (FINRA TRACE only supports one-bond-at-a-time lookup). Click **Bond Overrides** in the header to paste values for tickers you watch; the pipeline joins them into results.

## Tests

```bash
cd backend
.venv/Scripts/python -m pytest -q
```

## Project layout

- `backend/app/screeners/` — screener definitions (add new files here; they self-register)
- `backend/app/pipeline/` — staged refresh pipeline
- `backend/app/sources/` — one file per external data source
- `backend/app/api/` — FastAPI routers
- `backend/app/util/filings_text.py` — going-concern / Chapter 11 classifiers
- `data/cache.db` — SQLite cache (gitignored)
- `data/edgar/` — cached 10-K/10-Q HTML (gitignored)

See `C:\Users\walte\.claude\plans\i-want-to-make-tender-cat.md` for the full architecture plan.
