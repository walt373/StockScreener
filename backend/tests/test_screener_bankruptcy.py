from app.screeners.bankruptcy import BankruptcyScreener


def _base_row() -> dict:
    return {
        "ticker": "XYZ",
        "name": "XYZ Corp",
        "exchange": "NASDAQ",
        "financial_status": "N",
        "price": 1.0,
        "market_cap": 50_000_000,
        "avg_volume": 500_000,
        "has_options": True,
        "has_us_filing": True,
        "cash": 10.0,
        "current_assets": 20.0,
        "total_liabilities": 80.0,
        "equity": -5.0,
        "revenue_growth": -0.4,
        "shares_short": 1000.0,
        "trailing_1y_return": -0.85,
        "realized_vol_1y": 1.2,
        "furthest_expiry": "2026-01-17",
        "net_income": -5_000_000,
        "free_cash_flow": -3_000_000,
        "nearest_debt_maturity": "2026-06-15",
        "going_concern_flag": True,
        "ch11_mentions": 4,
    }


def test_passes_happy_path():
    s = BankruptcyScreener()
    assert s.hard_filters(_base_row()) is True


def test_filters_reject_small_cap():
    s = BankruptcyScreener()
    row = _base_row()
    row["market_cap"] = 5_000_000
    assert s.hard_filters(row) is False


def test_filters_reject_low_price():
    s = BankruptcyScreener()
    row = _base_row()
    row["price"] = 0.10
    assert s.hard_filters(row) is False


def test_filters_reject_low_volume():
    s = BankruptcyScreener()
    row = _base_row()
    row["avg_volume"] = 50_000
    assert s.hard_filters(row) is False


def test_filters_reject_no_options():
    s = BankruptcyScreener()
    row = _base_row()
    row["has_options"] = False
    assert s.hard_filters(row) is False


def test_filters_reject_bankrupt_flag():
    s = BankruptcyScreener()
    row = _base_row()
    row["financial_status"] = "Q"
    assert s.hard_filters(row) is False


def test_filters_reject_non_us_filer():
    s = BankruptcyScreener()
    row = _base_row()
    row["has_us_filing"] = False
    assert s.hard_filters(row) is False


def test_filters_reject_wrong_exchange():
    s = BankruptcyScreener()
    row = _base_row()
    row["exchange"] = "OTC"
    assert s.hard_filters(row) is False


def test_projection_computes_ratios():
    s = BankruptcyScreener()
    row = _base_row()
    p = s.project(row)
    assert p["ticker"] == "XYZ"
    assert p["ni_over_mcap"] == -5_000_000 / 50_000_000
    assert p["fcf_over_mcap"] == -3_000_000 / 50_000_000
    assert p["going_concern_flag"] is True
    assert p["ch11_mentions"] == 4
    assert p["short_interest"] == 1000.0
    assert p["exchange"] == "NASDAQ"


def test_filters_reject_mega_cap():
    s = BankruptcyScreener()
    row = _base_row()
    row["market_cap"] = 3_000_000_000  # $3B > $2B max
    assert s.hard_filters(row) is False


def test_filters_boundary_mega_cap():
    s = BankruptcyScreener()
    row = _base_row()
    row["market_cap"] = 2_000_000_000  # exactly $2B — passes
    assert s.hard_filters(row) is True
