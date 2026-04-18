from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

ColumnType = Literal["string", "money", "int", "pct", "date", "flag", "ratio"]


@dataclass
class ColumnSpec:
    key: str
    label: str
    type: ColumnType = "string"
    tooltip: str | None = None
    nullable: bool = True


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
