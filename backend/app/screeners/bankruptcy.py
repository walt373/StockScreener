from __future__ import annotations

from typing import Any

from ..util.numbers import safe_div
from .base import ColumnSpec, ScreenerMeta, register

MIN_MARKET_CAP = 10_000_000.0
MIN_PRICE = 0.20
MIN_AVG_VOLUME = 100_000.0

# Pre-filter: distress signal from cheap batch data. A stock that's UP on the year
# is extremely unlikely to go bankrupt in the next 12 months.
PREFILTER_MAX_1Y_RETURN = -0.20  # down at least 20% trailing year

# Post-filter: at least one of these must be true for a "bankruptcy candidate"
DISTRESS_NI_MCAP = -0.30  # losing > 30% of market cap per year in net income
DISTRESS_FCF_MCAP = -0.20  # burning > 20% of market cap per year in FCF
DISTRESS_1Y_RETURN = -0.50  # down > 50% trailing year (deep distress)

META = ScreenerMeta(
    id="bankruptcy",
    name="Bankruptcy candidates",
    description=(
        "NYSE/Nasdaq US-domiciled equities that have listed options, ≥$10M mcap, "
        "≥$0.20 price, ≥100K avg volume, and are not already bankrupt. Sorted by "
        "trailing 1-year return ascending."
    ),
    default_sort_key="trailing_1y_return",
    default_sort_dir="asc",
    required_stages={
        "universe",
        "cheap_filter",
        "options",
        "filer_check",
        "history",
        "fundamentals",
        "filings",
    },
    columns=[
        ColumnSpec("ticker", "Ticker", "string", nullable=False),
        ColumnSpec("name", "Name", "string"),
        ColumnSpec("exchange", "Exchange", "string"),
        ColumnSpec("price", "Price", "money"),
        ColumnSpec("market_cap", "Market cap", "money"),
        ColumnSpec("avg_volume", "Avg volume", "int"),
        ColumnSpec("cash", "Cash", "money"),
        ColumnSpec("current_assets", "Current assets", "money"),
        ColumnSpec("total_liabilities", "Total liabilities", "money"),
        ColumnSpec("equity", "Equity", "money"),
        ColumnSpec("revenue_growth", "Revenue growth", "pct"),
        ColumnSpec("short_interest", "Short interest", "int"),
        ColumnSpec("trailing_1y_return", "1y return", "pct"),
        ColumnSpec("realized_vol_1y", "Realized vol (1y)", "pct"),
        ColumnSpec("furthest_option_expiry", "Furthest opt. expiry", "date"),
        ColumnSpec("ni_over_mcap", "NI / mcap", "ratio"),
        ColumnSpec("fcf_over_mcap", "FCF / mcap", "ratio"),
        ColumnSpec("nearest_debt_maturity", "Nearest debt maturity", "string"),
        ColumnSpec(
            "bond_price",
            "Bond price",
            "money",
            tooltip="Not available on free tier; set in Bond Overrides.",
        ),
        ColumnSpec(
            "bond_yield",
            "Bond yield",
            "pct",
            tooltip="Not available on free tier; set in Bond Overrides.",
        ),
        ColumnSpec(
            "bond_last_traded",
            "Bond last traded",
            "date",
            tooltip="Not available on free tier; set in Bond Overrides.",
        ),
        ColumnSpec("going_concern_flag", "Going concern", "flag"),
        ColumnSpec("ch11_mentions", "Ch.11 mentions", "int"),
    ],
)


class BankruptcyScreener:
    meta = META

    def pre_filter(self, row: dict[str, Any]) -> bool:
        """Cheap: uses only batch data (price, avg_volume, 1y return, exchange,
        financial_status). Drops ~80% of the universe before any EDGAR or
        per-ticker yfinance call."""
        if row.get("financial_status") == "Q":
            return False
        ex = row.get("exchange")
        if ex not in ("NYSE", "NASDAQ", "NYSE_AMERICAN"):
            return False
        price = row.get("price")
        vol = row.get("avg_volume")
        if price is None or price < MIN_PRICE:
            return False
        if vol is None or vol < MIN_AVG_VOLUME:
            return False
        ret = row.get("trailing_1y_return")
        # Only candidates that are down on the year (or missing data — give benefit of doubt)
        if ret is not None and ret > PREFILTER_MAX_1Y_RETURN:
            return False
        return True

    def hard_filters(self, row: dict[str, Any]) -> bool:
        """Final gate: requires full data + at least one distress signal."""
        if not self.pre_filter(row):
            return False
        mcap = row.get("market_cap")
        if mcap is None or mcap < MIN_MARKET_CAP:
            return False
        if not row.get("has_options"):
            return False
        if not row.get("has_us_filing"):
            return False

        # Distress signal — at least one must be true
        ret = row.get("trailing_1y_return")
        ni = row.get("net_income")
        fcf = row.get("free_cash_flow")
        equity = row.get("equity")
        ni_mcap = (ni / mcap) if ni is not None and mcap else None
        fcf_mcap = (fcf / mcap) if fcf is not None and mcap else None
        distress_hits = [
            bool(row.get("going_concern_flag")),
            (row.get("ch11_mentions") or 0) >= 1,
            equity is not None and equity < 0,
            ni_mcap is not None and ni_mcap < DISTRESS_NI_MCAP,
            fcf_mcap is not None and fcf_mcap < DISTRESS_FCF_MCAP,
            ret is not None and ret < DISTRESS_1Y_RETURN,
        ]
        return any(distress_hits)

    def project(self, row: dict[str, Any]) -> dict[str, Any]:
        mcap = row.get("market_cap")
        ni = row.get("net_income")
        fcf = row.get("free_cash_flow")
        # Normalize exchange label
        ex_raw = row.get("exchange")
        ex_label = {"NYSE": "NYSE", "NYSE_AMERICAN": "NYSE American", "NASDAQ": "NASDAQ"}.get(
            ex_raw, ex_raw
        )
        return {
            "ticker": row["ticker"],
            "name": row.get("name"),
            "exchange": ex_label,
            "price": row.get("price"),
            "market_cap": mcap,
            "avg_volume": row.get("avg_volume"),
            "cash": row.get("cash"),
            "current_assets": row.get("current_assets"),
            "total_liabilities": row.get("total_liabilities"),
            "equity": row.get("equity"),
            "revenue_growth": row.get("revenue_growth"),
            "short_interest": row.get("shares_short"),
            "trailing_1y_return": row.get("trailing_1y_return"),
            "realized_vol_1y": row.get("realized_vol_1y"),
            "furthest_option_expiry": row.get("furthest_expiry"),
            "ni_over_mcap": safe_div(ni, mcap),
            "fcf_over_mcap": safe_div(fcf, mcap),
            "nearest_debt_maturity": row.get("nearest_debt_maturity"),
            "bond_price": row.get("bond_price"),
            "bond_yield": row.get("bond_yield"),
            "bond_last_traded": row.get("bond_last_traded"),
            "going_concern_flag": row.get("going_concern_flag"),
            "ch11_mentions": row.get("ch11_mentions"),
        }


register(BankruptcyScreener())
