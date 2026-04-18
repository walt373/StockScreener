from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite, isnan
from typing import Any


def iso_utc(d: datetime | None) -> str | None:
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.isoformat()


def safe_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if isnan(v) or not isfinite(v):
        return None
    return v


def safe_int(x: Any) -> int | None:
    v = safe_float(x)
    return int(v) if v is not None else None


def safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or den == 0:
        return None
    return num / den
