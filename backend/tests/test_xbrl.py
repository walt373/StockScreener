import pytest

from app.sources.edgar import extract_nearest_debt_maturity, extract_xbrl_balance


def test_extract_xbrl_balance_happy():
    facts = {
        "facts": {
            "us-gaap": {
                "CashAndCashEquivalentsAtCarryingValue": {
                    "units": {
                        "USD": [
                            {"end": "2024-12-31", "val": 100},
                            {"end": "2025-06-30", "val": 150},
                        ]
                    }
                },
                "AssetsCurrent": {
                    "units": {"USD": [{"end": "2025-06-30", "val": 500}]}
                },
                "Liabilities": {
                    "units": {"USD": [{"end": "2025-06-30", "val": 900}]}
                },
                "StockholdersEquity": {
                    "units": {"USD": [{"end": "2025-06-30", "val": -50}]}
                },
                "NetIncomeLoss": {
                    "units": {"USD": [{"end": "2025-06-30", "val": -30}]}
                },
                "NetCashProvidedByUsedInOperatingActivities": {
                    "units": {"USD": [{"end": "2025-06-30", "val": 40}]}
                },
                "PaymentsToAcquirePropertyPlantAndEquipment": {
                    "units": {"USD": [{"end": "2025-06-30", "val": 10}]}
                },
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {"USD": [{"end": "2025-06-30", "val": 1_000_000}]}
                },
            },
        }
    }
    out = extract_xbrl_balance(facts)
    assert out["cash"] == 150
    assert out["current_assets"] == 500
    assert out["total_liabilities"] == 900
    assert out["equity"] == -50
    assert out["net_income"] == -30
    assert out["fcf"] == 30  # 40 - 10


def test_extract_nearest_debt_maturity_from_explicit_dates():
    facts = {
        "facts": {
            "us-gaap": {
                "DebtInstrumentMaturityDate": {
                    "units": {
                        "pure": [
                            {"val": "2030-01-01"},
                            {"val": "2026-06-15"},
                            {"val": "2028-03-20"},
                        ]
                    }
                },
            }
        }
    }
    d, src = extract_nearest_debt_maturity(facts)
    assert d == "2026-06-15"
    assert src == "DebtInstrumentMaturityDate"


def test_extract_nearest_debt_maturity_none():
    assert extract_nearest_debt_maturity(None) == (None, None)
    assert extract_nearest_debt_maturity({"facts": {"us-gaap": {}}}) == (None, None)


def _annual(start, end, val, accn):
    return {"start": start, "end": end, "val": val, "fp": "FY", "accn": accn}


def _quarter(start, end, val, accn):
    return {"start": start, "end": end, "val": val, "fp": "FY", "accn": accn}


def test_revenue_growth_dedupes_by_end_date():
    """Same fiscal year reported in multiple filings — must compare distinct ends."""
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            _annual("2023-01-01", "2023-12-31", 100, "0001-23-001"),
                            _annual("2024-01-01", "2024-12-31", 120, "0001-24-001"),
                            # FY2024 restated in the FY2025 10-K (later accn wins)
                            _annual("2024-01-01", "2024-12-31", 122, "0001-25-001"),
                            # FY2023 re-tagged in FY2024 10-K as comparative
                            _annual("2023-01-01", "2023-12-31", 100, "0001-24-001"),
                        ]
                    }
                }
            },
            "dei": {},
        }
    }
    out = extract_xbrl_balance(facts)
    # FY2024 latest restated 122, vs FY2023 = 100 → +22%
    assert out["revenue_growth"] == pytest.approx(0.22)


def test_revenue_growth_ignores_quarterly_entries_with_fp_fy():
    """AAPL-style: 10-K tags BOTH full-year and Q4-only revenues with fp=FY.
    The Q4 entry has a ~90-day span and must be excluded."""
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            # Full year FY2024 (365-day span) — the one we want
                            _annual("2023-10-01", "2024-09-28", 383_000_000_000, "a"),
                            # Q4 FY2024 (92-day span) — same end date, but quarterly!
                            _quarter("2024-06-30", "2024-09-28", 94_900_000_000, "a"),
                            # Full year FY2023
                            _annual("2022-10-02", "2023-09-30", 383_000_000_000, "b"),
                            # Q4 FY2023 (quarterly)
                            _quarter("2023-07-02", "2023-09-30", 89_500_000_000, "b"),
                        ]
                    }
                }
            },
            "dei": {},
        }
    }
    out = extract_xbrl_balance(facts)
    # Full year comparison: 383B vs 383B → ~0% (not 0% vs quarterly!)
    assert out["revenue_growth"] == pytest.approx(0.0, abs=0.01)


def test_revenue_growth_falls_back_to_newer_concept():
    facts = {
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            _annual("2023-01-01", "2023-12-31", 200, "a"),
                            _annual("2024-01-01", "2024-12-31", 150, "b"),
                        ]
                    }
                }
            },
            "dei": {},
        }
    }
    out = extract_xbrl_balance(facts)
    assert out["revenue_growth"] == pytest.approx(-0.25)  # 150/200 - 1


def test_revenue_growth_prefers_concept_with_most_recent_data():
    """Filer migrated from Revenues to RevenueFromContract after ASC 606.
    Legacy Revenues has old data; must pick the newer concept."""
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            _annual("2017-01-01", "2017-12-31", 200, "old-a"),
                            _annual("2018-01-01", "2018-12-31", 232, "old-b"),
                        ]
                    }
                },
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            _annual("2023-01-01", "2023-12-31", 900, "new-a"),
                            _annual("2024-01-01", "2024-12-31", 1000, "new-b"),
                        ]
                    }
                },
            },
            "dei": {},
        }
    }
    out = extract_xbrl_balance(facts)
    # Should use the newer concept: 1000 vs 900 = +11.1%, not legacy +16%
    assert out["revenue_growth"] == pytest.approx(1000 / 900 - 1)


def test_revenue_growth_single_year_returns_none():
    facts = {
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [_annual("2024-01-01", "2024-12-31", 100, "a")]
                    }
                }
            },
            "dei": {},
        }
    }
    assert extract_xbrl_balance(facts)["revenue_growth"] is None
