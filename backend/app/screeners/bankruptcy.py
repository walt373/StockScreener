from __future__ import annotations

from typing import Any

from ..util.numbers import safe_div
from .base import ColumnSpec, ScreenerMeta, register

MIN_MARKET_CAP = 10_000_000.0
MAX_MARKET_CAP = 2_000_000_000.0  # exclude large caps — they rarely go bankrupt
MIN_PRICE = 0.20
MIN_AVG_VOLUME = 100_000.0
MAX_1Y_RETURN = -0.50  # must be down > 50% trailing year

META = ScreenerMeta(
    id="bankruptcy",
    name="Bankruptcy candidates",
    description=(
        "NYSE/Nasdaq US-domiciled equities that have listed options, "
        "market cap $10M–$2B, price ≥ $0.20, avg volume ≥ 100K, "
        "1-year return < -50%, trailing net income < $0, not already bankrupt. "
        "Sorted by trailing 1-year return ascending. "
        "Flag columns (going concern, Ch.11, NT 10-K/Q) surface additional distress signals."
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
        ColumnSpec("sector", "Sector", "string", tooltip="SIC description from SEC EDGAR."),
        ColumnSpec("exchange", "Exchange", "string"),
        ColumnSpec("price", "Price", "money"),
        ColumnSpec("market_cap", "Market cap", "money"),
        ColumnSpec("avg_volume", "Avg volume", "int"),
        ColumnSpec("cash", "Cash", "money"),
        ColumnSpec("current_assets", "Current assets", "money"),
        ColumnSpec("total_liabilities", "Total liabilities", "money"),
        ColumnSpec("equity", "Equity", "money"),
        ColumnSpec(
            "liabilities_over_assets",
            "Liab / Assets",
            "pct",
            tooltip="Total liabilities / total assets. >100% means negative equity.",
        ),
        ColumnSpec("revenue_growth", "Revenue growth", "pct"),
        ColumnSpec("trailing_1y_return", "1y return", "pct"),
        ColumnSpec("realized_vol_1y", "Realized vol (1y)", "pct"),
        ColumnSpec("furthest_option_expiry", "Furthest opt. expiry", "date"),
        ColumnSpec("ni_over_mcap", "NI / mcap", "ratio"),
        ColumnSpec("fcf_over_mcap", "FCF / mcap", "ratio"),
        ColumnSpec("nearest_debt_maturity", "Nearest debt maturity", "string"),
        ColumnSpec("going_concern_flag", "Going concern", "flag"),
        ColumnSpec("ch11_mentions", "Ch.11 mentions", "int"),
        ColumnSpec(
            "nt_10k_filed_at",
            "NT 10-K filed",
            "date",
            tooltip="Date of most recent NT 10-K (late-filing notice). Strong distress signal.",
        ),
        ColumnSpec(
            "nt_10q_filed_at",
            "NT 10-Q filed",
            "date",
            tooltip="Date of most recent NT 10-Q (late-filing notice). Strong distress signal.",
        ),
    ],
)


class BankruptcyScreener:
    meta = META

    def pre_filter(self, row: dict[str, Any]) -> bool:
        """Cheap: uses only batch data (price, avg_volume, 1y return, exchange,
        financial_status, has_options from bulk list)."""
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
        if not row.get("has_options"):
            return False
        ret = row.get("trailing_1y_return")
        if ret is None or ret >= MAX_1Y_RETURN:
            return False
        return True

    def hard_filters(self, row: dict[str, Any]) -> bool:
        """Final gate: adds mcap band + US filer + net-income-negative on top of pre_filter."""
        if not self.pre_filter(row):
            return False
        mcap = row.get("market_cap")
        if mcap is None or mcap < MIN_MARKET_CAP or mcap > MAX_MARKET_CAP:
            return False
        if not row.get("has_us_filing"):
            return False
        ni = row.get("net_income")
        if ni is None or ni >= 0:
            return False
        return True

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
            "sector": row.get("sector"),
            "exchange": ex_label,
            "price": row.get("price"),
            "market_cap": mcap,
            "avg_volume": row.get("avg_volume"),
            "cash": row.get("cash"),
            "current_assets": row.get("current_assets"),
            "total_liabilities": row.get("total_liabilities"),
            "total_assets": row.get("total_assets"),
            "equity": row.get("equity"),
            "liabilities_over_assets": safe_div(row.get("total_liabilities"), row.get("total_assets")),
            "revenue_growth": row.get("revenue_growth"),
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
            "nt_10k_filed_at": row.get("nt_10k_filed_at"),
            "nt_10q_filed_at": row.get("nt_10q_filed_at"),
        }


register(BankruptcyScreener())
