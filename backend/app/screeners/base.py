from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

ColumnType = Literal["string", "money", "int", "pct", "date", "flag", "ratio", "multiple"]


@dataclass
class ColumnSpec:
    key: str
    label: str
    type: ColumnType = "string"
    tooltip: str | None = None
    nullable: bool = True
    # Reverses coloring for "multiple" type: low values turn green instead of red.
    # Used for columns like price-to-book where a lower number is the good outcome.
    lower_is_better: bool = False


@dataclass
class ScreenerMeta:
    id: str
    name: str
    description: str
    columns: list[ColumnSpec]
    default_sort_key: str
    default_sort_dir: Literal["asc", "desc"] = "asc"
    required_stages: set[str] = field(default_factory=set)


class Screener(Protocol):
    meta: ScreenerMeta

    def pre_filter(self, row: dict[str, Any]) -> bool:
        """Cheap filter using only batch-stage data (price, avg_volume, 1y return,
        exchange, financial_status). Runs BEFORE EDGAR and per-ticker yfinance,
        so expensive stages only touch candidates."""
        ...

    def cache_filter(self, row: dict[str, Any]) -> bool:
        """Runs after cached fundamentals are hydrated (if cache_fresh=True).
        Should return False only when cached data confirms this row will fail
        hard_filters — saves an EDGAR XBRL fetch and yfinance option call.
        Default (and when row is not cache_fresh): return True."""
        return True

    def hard_filters(self, row: dict[str, Any]) -> bool:
        """Final gate using all fetched data. Applied at materialization."""
        ...

    def project(self, row: dict[str, Any]) -> dict[str, Any]: ...


_REGISTRY: dict[str, Screener] = {}


def register(s: Screener) -> None:
    _REGISTRY[s.meta.id] = s


def list_screeners() -> list[Screener]:
    return list(_REGISTRY.values())


def get_screener(screener_id: str) -> Screener | None:
    return _REGISTRY.get(screener_id)
