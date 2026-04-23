from __future__ import annotations

from typing import Any

from ..util.numbers import safe_div
from .base import ColumnSpec, ScreenerMeta, register

MIN_MARKET_CAP = 100_000_000.0
MIN_PRICE = 1.0
MIN_AVG_VOLUME = 100_000.0
MIN_CURRENT_RATIO = 3.0
MAX_PRICE_TO_BOOK = 3.0
# Operating cash flow must be > -cash / 4. i.e. annual cash burn can't exceed
# 25% of the cash pile — otherwise the company runs out of runway within 4 years.
OCF_TO_CASH_THRESHOLD = -0.25

META = ScreenerMeta(
    id="strong_balance",
    name="Strong balance sheets",
    description=(
        "NYSE/Nasdaq US-domiciled equities with listed options, "
        "market cap ≥ $100M, price ≥ $1, avg volume ≥ 100K, "
        "current ratio > 3, price/book < 3, and operating cash flow "
        "burn < 25% of cash. Sorted by price/book ascending "
        "(cheapest relative to book value first)."
    ),
    default_sort_key="price_to_book",
    default_sort_dir="asc",
    required_stages={
        "universe",
        "batch_market",
        "optionable_tag",
        "filer_check",
        "filings",
    },
    columns=[
        ColumnSpec("ticker", "Ticker", "string", nullable=False),
        ColumnSpec("name", "Name", "string"),
        ColumnSpec("sector", "Sector", "string", tooltip="SIC description from SEC EDGAR."),
        ColumnSpec("exchange", "Exchange", "string"),
        ColumnSpec("price", "Price", "money"),
        ColumnSpec("avg_volume", "Avg volume", "int"),
        ColumnSpec("furthest_option_expiry", "Furthest opt. expiry", "date"),
        ColumnSpec("market_cap", "Market cap", "money"),
        ColumnSpec("cash", "Cash", "money"),
        ColumnSpec("operating_cash_flow", "Operating CF", "money"),
        ColumnSpec("free_cash_flow", "Free cash flow", "money"),
        ColumnSpec(
            "current_ratio",
            "Current ratio",
            "multiple",
            tooltip="Current assets / current liabilities.",
        ),
        ColumnSpec(
            "price_to_book",
            "P / Book",
            "multiple",
            tooltip="Market cap / equity. <1 means trading below book value.",
        ),
        ColumnSpec("total_liabilities", "Total liabilities", "money"),
        ColumnSpec("equity", "Equity", "money"),
        ColumnSpec("revenue_growth", "Revenue growth", "pct"),
        ColumnSpec("trailing_1y_return", "1y return", "pct"),
        ColumnSpec("realized_vol_1y", "Realized vol (1y)", "pct"),
    ],
)


class StrongBalanceScreener:
    meta = META

    def pre_filter(self, row: dict[str, Any]) -> bool:
        """Cheap: price, volume, exchange, not-bankrupt, has_options.
        No 1y-return restriction — we don't care which direction the stock went."""
        if row.get("financial_status") == "Q":
            return False
        ex = row.get("exchange")
        if ex not in ("NYSE", "NASDAQ", "NYSE_AMERICAN"):
            return False
        price = row.get("price")
        if price is None or price < MIN_PRICE:
            return False
        vol = row.get("avg_volume")
        if vol is None or vol < MIN_AVG_VOLUME:
            return False
        if not row.get("has_options"):
            return False
        return True

    def hard_filters(self, row: dict[str, Any]) -> bool:
        if not self.pre_filter(row):
            return False
        mcap = row.get("market_cap")
        if mcap is None or mcap < MIN_MARKET_CAP:
            return False
        if not row.get("has_us_filing"):
            return False

        # Current ratio > 3
        ca = row.get("current_assets")
        cl = row.get("current_liabilities")
        if ca is None or cl is None or cl <= 0:
            return False
        if ca / cl <= MIN_CURRENT_RATIO:
            return False

        # P/Book < 3 (requires positive equity)
        equity = row.get("equity")
        if equity is None or equity <= 0:
            return False
        if mcap / equity >= MAX_PRICE_TO_BOOK:
            return False

        # Operating cash flow not burning > 25% of cash per year
        cash = row.get("cash")
        ocf = row.get("operating_cash_flow")
        if cash is None or ocf is None or cash <= 0:
            return False
        if ocf <= OCF_TO_CASH_THRESHOLD * cash:
            return False

        return True

    def project(self, row: dict[str, Any]) -> dict[str, Any]:
        mcap = row.get("market_cap")
        equity = row.get("equity")
        ca = row.get("current_assets")
        cl = row.get("current_liabilities")
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
            "avg_volume": row.get("avg_volume"),
            "furthest_option_expiry": row.get("furthest_expiry"),
            "market_cap": mcap,
            "cash": row.get("cash"),
            "operating_cash_flow": row.get("operating_cash_flow"),
            "free_cash_flow": row.get("free_cash_flow"),
            "current_assets": ca,
            "current_liabilities": cl,
            "current_ratio": safe_div(ca, cl),
            "price_to_book": safe_div(mcap, equity) if equity and equity > 0 else None,
            "total_liabilities": row.get("total_liabilities"),
            "equity": equity,
            "revenue_growth": row.get("revenue_growth"),
            "trailing_1y_return": row.get("trailing_1y_return"),
            "realized_vol_1y": row.get("realized_vol_1y"),
        }


register(StrongBalanceScreener())
