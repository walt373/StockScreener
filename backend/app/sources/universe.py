from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from ..config import settings

log = logging.getLogger(__name__)

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


@dataclass
class UniverseRow:
    ticker: str
    name: str
    exchange: str          # "NASDAQ" | "NYSE"
    financial_status: str  # "N" normal, "Q" bankrupt, "D" deficient, etc.
    is_etf: bool
    is_test: bool


def _parse_nasdaq_listed(text: str) -> list[UniverseRow]:
    rows: list[UniverseRow] = []
    lines = text.splitlines()
    if not lines:
        return rows
    # File ends with "File Creation Time:..." footer
    header = lines[0].split("|")
    try:
        idx_sym = header.index("Symbol")
        idx_name = header.index("Security Name")
        idx_test = header.index("Test Issue")
        idx_status = header.index("Financial Status")
        idx_etf = header.index("ETF")
    except ValueError:
        log.error("Unexpected nasdaqlisted.txt header: %s", header)
        return rows
    for line in lines[1:]:
        if line.startswith("File Creation Time"):
            break
        parts = line.split("|")
        if len(parts) <= max(idx_sym, idx_name, idx_test, idx_status, idx_etf):
            continue
        rows.append(
            UniverseRow(
                ticker=parts[idx_sym].strip(),
                name=parts[idx_name].strip(),
                exchange="NASDAQ",
                financial_status=parts[idx_status].strip() or "N",
                is_etf=parts[idx_etf].strip().upper() == "Y",
                is_test=parts[idx_test].strip().upper() == "Y",
            )
        )
    return rows


def _parse_other_listed(text: str) -> list[UniverseRow]:
    rows: list[UniverseRow] = []
    lines = text.splitlines()
    if not lines:
        return rows
    header = lines[0].split("|")
    try:
        idx_sym = header.index("ACT Symbol")
        idx_name = header.index("Security Name")
        idx_exch = header.index("Exchange")
        idx_test = header.index("Test Issue")
        idx_etf = header.index("ETF")
    except ValueError:
        log.error("Unexpected otherlisted.txt header: %s", header)
        return rows
    # Map exchange code -> name. N=NYSE, A=NYSE American, P=Arca, Z=BATS
    ex_map = {"N": "NYSE", "A": "NYSE_AMERICAN"}
    for line in lines[1:]:
        if line.startswith("File Creation Time"):
            break
        parts = line.split("|")
        if len(parts) <= max(idx_sym, idx_name, idx_exch, idx_test, idx_etf):
            continue
        ex_code = parts[idx_exch].strip()
        exchange = ex_map.get(ex_code)
        if exchange is None:
            continue
        rows.append(
            UniverseRow(
                ticker=parts[idx_sym].strip(),
                name=parts[idx_name].strip(),
                exchange=exchange,
                financial_status="N",  # not exposed in otherlisted; treat as normal
                is_etf=parts[idx_etf].strip().upper() == "Y",
                is_test=parts[idx_test].strip().upper() == "Y",
            )
        )
    return rows


_SECURITY_SUFFIX_KEYWORDS = (
    "common stock",
    "common share",
    "ordinary share",
    "ordinary stock",
    "class ",  # catches "Class A", "Class B", etc.
    "depositary",
    "depository",
    "american depository",
    "american depositary",
    "adr",
    "ads",
    "receipt",
    "preferred",
    "pfd",
)


def clean_company_name(name: str) -> str:
    """Strip security-type suffixes like ' - Common Stock', ' - Class A Ordinary Shares'.

    Uses rsplit to keep only the rightmost ' - ' as the separator, so names
    containing a dash (e.g. 'Some Company - Holdings Inc.') aren't over-stripped
    unless the suffix clearly names a security type.
    """
    if not name or " - " not in name:
        return name.strip() if name else name
    head, _, tail = name.rpartition(" - ")
    tail_lower = tail.lower().strip()
    if any(kw in tail_lower for kw in _SECURITY_SUFFIX_KEYWORDS):
        return head.strip()
    return name.strip()


def _is_common_stock_symbol(sym: str, name: str = "") -> bool:
    if not sym:
        return False
    if any(ch in sym for ch in ("$", "^")):
        return False
    if "." in sym:
        return False
    # Exclude warrants/units/rights/preferreds based on security name
    nl = name.lower()
    for keyword in (
        " warrant",
        " warrants",
        " right",
        " rights",
        " unit",
        " units",
        " preferred",
        " depositary",
        " depository",
        " subordinate",
        " pfd",
    ):
        if keyword in nl:
            return False
    return True


async def fetch_universe() -> list[UniverseRow]:
    headers = {"User-Agent": settings.sec_user_agent}
    async with httpx.AsyncClient(timeout=60.0, headers=headers, follow_redirects=True) as c:
        r1 = await c.get(NASDAQ_LISTED_URL)
        r1.raise_for_status()
        r2 = await c.get(OTHER_LISTED_URL)
        r2.raise_for_status()
    rows = _parse_nasdaq_listed(r1.text) + _parse_other_listed(r2.text)
    filtered: list[UniverseRow] = []
    seen: set[str] = set()
    for r in rows:
        if r.is_test or r.is_etf:
            continue
        if not _is_common_stock_symbol(r.ticker, r.name):
            continue
        # Exclude names already flagged Q=bankrupt
        if r.financial_status == "Q":
            continue
        if r.ticker in seen:
            continue
        seen.add(r.ticker)
        r.name = clean_company_name(r.name)
        filtered.append(r)
    log.info("Universe parsed: %d raw rows → %d filtered", len(rows), len(filtered))
    return filtered


async def fetch_sec_ticker_cik_map() -> dict[str, str]:
    """Ticker -> zero-padded 10-digit CIK. Missing tickers simply not present."""
    headers = {"User-Agent": settings.sec_user_agent}
    async with httpx.AsyncClient(timeout=60.0, headers=headers, follow_redirects=True) as c:
        r = await c.get(SEC_TICKERS_URL)
        r.raise_for_status()
        data = r.json()
    out: dict[str, str] = {}
    for _, row in data.items():
        sym = row.get("ticker", "").upper()
        cik_raw = row.get("cik_str")
        if not sym or cik_raw is None:
            continue
        out[sym] = str(cik_raw).zfill(10)
    log.info("SEC ticker→CIK map: %d entries", len(out))
    return out
