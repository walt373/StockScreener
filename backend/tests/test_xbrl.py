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
