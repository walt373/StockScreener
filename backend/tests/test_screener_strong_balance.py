from app.screeners.strong_balance import StrongBalanceScreener


def _base_row() -> dict:
    # Passes every criterion by default. Individual tests tweak one field.
    return {
        "ticker": "GOOD",
        "name": "Good Co",
        "sector": "Pharmaceutical Preparations",
        "exchange": "NASDAQ",
        "financial_status": "N",
        "price": 5.0,
        "market_cap": 500_000_000,
        "avg_volume": 500_000,
        "has_options": True,
        "has_us_filing": True,
        "current_assets": 400_000_000,
        "current_liabilities": 100_000_000,  # current ratio 4.0
        "cash": 200_000_000,
        "operating_cash_flow": -10_000_000,  # burn of $10M vs cash of $200M (-5% per year)
        "equity": 200_000_000,                # P/B = 500M / 200M = 2.5
        "total_liabilities": 100_000_000,
    }


def test_passes_happy_path():
    s = StrongBalanceScreener()
    assert s.hard_filters(_base_row()) is True


def test_rejects_price_below_one():
    s = StrongBalanceScreener()
    row = _base_row()
    row["price"] = 0.99
    assert s.hard_filters(row) is False


def test_rejects_mcap_below_100m():
    s = StrongBalanceScreener()
    row = _base_row()
    row["market_cap"] = 50_000_000
    assert s.hard_filters(row) is False


def test_rejects_current_ratio_at_threshold():
    s = StrongBalanceScreener()
    row = _base_row()
    row["current_assets"] = 300_000_000  # 300 / 100 = 3.0, must be > 3
    assert s.hard_filters(row) is False


def test_rejects_negative_equity():
    s = StrongBalanceScreener()
    row = _base_row()
    row["equity"] = -100_000_000
    assert s.hard_filters(row) is False


def test_rejects_high_price_to_book():
    s = StrongBalanceScreener()
    row = _base_row()
    row["equity"] = 100_000_000  # P/B = 500M / 100M = 5.0, must be < 3
    assert s.hard_filters(row) is False


def test_rejects_excessive_cash_burn():
    s = StrongBalanceScreener()
    row = _base_row()
    # cash=200M, threshold -25% * 200M = -50M. OCF must be > -50M.
    row["operating_cash_flow"] = -60_000_000
    assert s.hard_filters(row) is False


def test_allows_ocf_exactly_at_threshold_fails():
    """Strictly greater than -cash/4, so equal should fail."""
    s = StrongBalanceScreener()
    row = _base_row()
    row["operating_cash_flow"] = -50_000_000  # exactly -25% of 200M cash
    assert s.hard_filters(row) is False


def test_rejects_no_options():
    s = StrongBalanceScreener()
    row = _base_row()
    row["has_options"] = False
    assert s.hard_filters(row) is False


def test_project_computes_price_to_book():
    s = StrongBalanceScreener()
    p = s.project(_base_row())
    assert p["price_to_book"] == 500_000_000 / 200_000_000
    assert p["current_ratio"] == 400_000_000 / 100_000_000
    assert p["operating_cash_flow"] == -10_000_000
