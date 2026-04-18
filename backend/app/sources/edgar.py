from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import date, datetime

from ..config import settings
from ..util.http import edgar_client

log = logging.getLogger(__name__)


@dataclass
class LatestFilings:
    cik: str
    latest_10k_accession: str | None
    latest_10k_primary_doc: str | None
    latest_10k_filed: date | None
    latest_10q_accession: str | None
    latest_10q_primary_doc: str | None
    latest_10q_filed: date | None
    latest_nt_10k_filed: date | None
    latest_nt_10q_filed: date | None


def _accession_nodash(acc: str) -> str:
    return acc.replace("-", "")


def archive_primary_url(cik: str, accession: str, primary_doc: str) -> str:
    cik_int = int(cik)
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik_int}/"
        f"{_accession_nodash(accession)}/{primary_doc}"
    )


async def fetch_submissions(cik: str) -> dict:
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    resp = await edgar_client().get(url, accept="application/json")
    resp.raise_for_status()
    return resp.json()


def _latest_by_form(submissions: dict, forms: tuple[str, ...]) -> tuple[str, str, date] | None:
    recent = submissions.get("filings", {}).get("recent", {})
    forms_arr = recent.get("form", [])
    acc_arr = recent.get("accessionNumber", [])
    date_arr = recent.get("filingDate", [])
    primary_arr = recent.get("primaryDocument", [])
    best: tuple[str, str, date] | None = None
    for f, acc, d, primary in zip(forms_arr, acc_arr, date_arr, primary_arr):
        if f not in forms:
            continue
        try:
            filed = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        if best is None or filed > best[2]:
            best = (acc, primary, filed)
    return best


async def latest_10k_10q(cik: str) -> LatestFilings:
    subs = await fetch_submissions(cik)
    k = _latest_by_form(subs, ("10-K", "10-K/A"))
    q = _latest_by_form(subs, ("10-Q", "10-Q/A"))
    nk = _latest_by_form(subs, ("NT 10-K", "NT 10-K/A"))
    nq = _latest_by_form(subs, ("NT 10-Q", "NT 10-Q/A"))
    return LatestFilings(
        cik=cik,
        latest_10k_accession=k[0] if k else None,
        latest_10k_primary_doc=k[1] if k else None,
        latest_10k_filed=k[2] if k else None,
        latest_10q_accession=q[0] if q else None,
        latest_10q_primary_doc=q[1] if q else None,
        latest_10q_filed=q[2] if q else None,
        latest_nt_10k_filed=nk[2] if nk else None,
        latest_nt_10q_filed=nq[2] if nq else None,
    )


async def fetch_filing_html(cik: str, accession: str, primary_doc: str) -> str:
    """Cached on disk, content-addressed by accession+primary_doc."""
    key = hashlib.sha1(f"{accession}/{primary_doc}".encode()).hexdigest()[:16]
    cache_path = settings.edgar_dir / f"{cik}_{_accession_nodash(accession)}_{key}.html"
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="replace")
    url = archive_primary_url(cik, accession, primary_doc)
    resp = await edgar_client().get(url, accept="text/html,application/xhtml+xml")
    resp.raise_for_status()
    text = resp.text
    try:
        cache_path.write_text(text, encoding="utf-8")
    except OSError as e:
        log.warning("Failed to cache filing %s: %s", cache_path, e)
    return text


async def fetch_companyfacts(cik: str) -> dict | None:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    try:
        resp = await edgar_client().get(url, accept="application/json")
    except Exception as e:  # noqa: BLE001
        log.debug("companyfacts fetch error %s: %s", cik, e)
        return None
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        log.debug("companyfacts %s status %s", cik, resp.status_code)
        return None
    return resp.json()


def _latest_usd_fact(fact: dict) -> tuple[float | None, date | None]:
    units = fact.get("units", {})
    usd = units.get("USD") or next(iter(units.values()), [])
    best: tuple[float | None, date | None] = (None, None)
    for row in usd:
        try:
            end = datetime.strptime(row["end"], "%Y-%m-%d").date()
        except (KeyError, ValueError):
            continue
        if best[1] is None or end > best[1]:
            best = (row.get("val"), end)
    return best


def extract_xbrl_balance(facts: dict | None) -> dict[str, float | None]:
    """Extract latest balance sheet numbers from companyfacts.
    Returns dict with keys: cash, current_assets, total_liabilities, equity,
    net_income, fcf, shares_out, revenue_growth.
    """
    out: dict[str, float | None] = {
        "cash": None,
        "current_assets": None,
        "total_liabilities": None,
        "equity": None,
        "net_income": None,
        "fcf": None,
        "shares_out": None,
        "revenue_growth": None,
    }
    if not facts:
        return out
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    dei = facts.get("facts", {}).get("dei", {})

    def _pick(ns: dict, keys: tuple[str, ...]) -> float | None:
        for k in keys:
            if k in ns:
                v, _ = _latest_usd_fact(ns[k])
                if v is not None:
                    return float(v)
        return None

    out["cash"] = _pick(
        us_gaap,
        (
            "CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
            "Cash",
        ),
    )
    out["current_assets"] = _pick(us_gaap, ("AssetsCurrent",))
    out["total_liabilities"] = _pick(us_gaap, ("Liabilities",))
    out["equity"] = _pick(
        us_gaap,
        (
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        ),
    )
    out["net_income"] = _pick(
        us_gaap,
        ("NetIncomeLoss", "ProfitLoss"),
    )
    out["fcf"] = None  # derived if needed; XBRL rarely tags FCF directly
    ocf = _pick(
        us_gaap,
        (
            "NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        ),
    )
    capex = _pick(us_gaap, ("PaymentsToAcquirePropertyPlantAndEquipment",))
    if ocf is not None and capex is not None:
        out["fcf"] = ocf - capex  # capex here is a positive outflow
    shares = _pick(dei, ("EntityCommonStockSharesOutstanding",))
    out["shares_out"] = shares

    # Revenue growth: compare latest two annual revenues
    for key in ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"):
        fact = us_gaap.get(key)
        if not fact:
            continue
        usd = fact.get("units", {}).get("USD") or []
        annuals = [r for r in usd if r.get("fp") == "FY" and r.get("val") is not None]
        annuals.sort(key=lambda r: r.get("end", ""), reverse=True)
        if len(annuals) >= 2 and annuals[1]["val"]:
            try:
                cur = float(annuals[0]["val"])
                prev = float(annuals[1]["val"])
                if prev != 0:
                    out["revenue_growth"] = (cur - prev) / abs(prev)
                    break
            except (TypeError, ValueError):
                continue
    return out


def extract_nearest_debt_maturity(facts: dict | None) -> tuple[str | None, str | None]:
    """Return (nearest_maturity_iso_date, source_fact_name) or (None, None)."""
    if not facts:
        return None, None
    us_gaap = facts.get("facts", {}).get("us-gaap", {})

    # 1) DebtInstrumentMaturityDate — explicit dates across instruments
    fact = us_gaap.get("DebtInstrumentMaturityDate")
    today = date.today()
    if fact:
        best: date | None = None
        for _, rows in fact.get("units", {}).items():
            for row in rows:
                val = row.get("val")
                if not val:
                    continue
                try:
                    d = datetime.strptime(val, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if d < today:
                    continue
                if best is None or d < best:
                    best = d
        if best is not None:
            return best.isoformat(), "DebtInstrumentMaturityDate"

    # 2) fall back to the 5 "maturities in next N years" buckets
    bucket_order = (
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths", 12),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo", 24),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearThree", 36),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour", 48),
        ("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFive", 60),
    )
    for name, months in bucket_order:
        bucket = us_gaap.get(name)
        if not bucket:
            continue
        v, _ = _latest_usd_fact(bucket)
        try:
            if v is not None and float(v) > 0:
                # approximate the maturity horizon; label with fact name
                y = today.year + (months // 12)
                return f"<={y}-{str(today.month).zfill(2)}", name
        except (TypeError, ValueError):
            continue
    return None, None


async def fulltext_has_phrase(cik: str, accession: str, phrase: str) -> bool:
    """Cheap pre-check via EDGAR full-text search before downloading the filing."""
    from urllib.parse import quote

    q = quote(f'"{phrase}"')
    url = f"https://efts.sec.gov/LATEST/search-index?q={q}&ciks={cik}&forms=10-K,10-Q"
    try:
        resp = await edgar_client().get(url, accept="application/json")
    except Exception as e:  # noqa: BLE001
        log.debug("efts error %s/%s: %s", cik, accession, e)
        return True  # be conservative; force download rather than skip
    if resp.status_code != 200:
        return True
    data = resp.json()
    hits = data.get("hits", {}).get("hits", [])
    acc_nodash = _accession_nodash(accession)
    for h in hits:
        src = h.get("_source", {})
        adsh = h.get("_id", "") or src.get("adsh", "")
        if acc_nodash in adsh.replace("-", ""):
            return True
    return False
