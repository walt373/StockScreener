from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Ticker(Base):
    __tablename__ = "tickers"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    exchange: Mapped[str | None] = mapped_column(String, nullable=True)
    cik: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    financial_status: Mapped[str | None] = mapped_column(String, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class RefreshRun(Base):
    __tablename__ = "refresh_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    screener_id: Mapped[str | None] = mapped_column(String, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String, default="running")  # running|ok|error
    stage: Mapped[str | None] = mapped_column(String, nullable=True)
    progress_done: Mapped[int] = mapped_column(Integer, default=0)
    progress_total: Mapped[int] = mapped_column(Integer, default=0)
    tickers_in: Mapped[int] = mapped_column(Integer, default=0)
    tickers_out: Mapped[int] = mapped_column(Integer, default=0)
    force: Mapped[bool] = mapped_column(Boolean, default=False)
    limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)


class FundamentalsCache(Base):
    __tablename__ = "fundamentals_cache"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_cap: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    cash: Mapped[float | None] = mapped_column(Float, nullable=True)
    current_assets: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_assets: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_liabilities: Mapped[float | None] = mapped_column(Float, nullable=True)
    equity: Mapped[float | None] = mapped_column(Float, nullable=True)
    revenue_growth: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_income: Mapped[float | None] = mapped_column(Float, nullable=True)
    free_cash_flow: Mapped[float | None] = mapped_column(Float, nullable=True)
    shares_short: Mapped[float | None] = mapped_column(Float, nullable=True)
    shares_outstanding: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class HistoryCache(Base):
    __tablename__ = "history_cache"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    trailing_1y_return: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_vol_1y: Mapped[float | None] = mapped_column(Float, nullable=True)


class OptionsCache(Base):
    __tablename__ = "options_cache"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    has_options: Mapped[bool] = mapped_column(Boolean, default=False)
    furthest_expiry: Mapped[str | None] = mapped_column(String, nullable=True)


class EdgarCompanyCache(Base):
    __tablename__ = "edgar_company_cache"
    cik: Mapped[str] = mapped_column(String, primary_key=True)
    ticker: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    sic: Mapped[str | None] = mapped_column(String, nullable=True)
    sic_description: Mapped[str | None] = mapped_column(String, nullable=True)
    latest_10k_accession: Mapped[str | None] = mapped_column(String, nullable=True)
    latest_10k_primary_doc: Mapped[str | None] = mapped_column(String, nullable=True)
    latest_10k_filed: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    latest_10q_accession: Mapped[str | None] = mapped_column(String, nullable=True)
    latest_10q_primary_doc: Mapped[str | None] = mapped_column(String, nullable=True)
    latest_10q_filed: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    latest_nt_10k_filed: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    latest_nt_10q_filed: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    has_us_filing: Mapped[bool] = mapped_column(Boolean, default=False)
    submissions_fetched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    companyfacts_fetched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class FilingsFlags(Base):
    __tablename__ = "filings_flags"
    accession: Mapped[str] = mapped_column(String, primary_key=True)
    cik: Mapped[str] = mapped_column(String, index=True)
    form_type: Mapped[str | None] = mapped_column(String, nullable=True)
    filed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    going_concern_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    ch11_mention_count: Mapped[int] = mapped_column(Integer, default=0)
    parse_version: Mapped[int] = mapped_column(Integer, default=1)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class DebtMaturity(Base):
    __tablename__ = "debt_maturities"
    cik: Mapped[str] = mapped_column(String, primary_key=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    nearest_maturity_date: Mapped[str | None] = mapped_column(String, nullable=True)
    source_fact: Mapped[str | None] = mapped_column(String, nullable=True)


class ShortInterestFinra(Base):
    __tablename__ = "short_interest_finra"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    settlement_date: Mapped[str] = mapped_column(String, primary_key=True)
    short_interest: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_daily_volume: Mapped[float | None] = mapped_column(Float, nullable=True)


class BondManualOverride(Base):
    __tablename__ = "bond_manual_overrides"
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    yield_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_traded_date: Mapped[str | None] = mapped_column(String, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class ScreenerResult(Base):
    __tablename__ = "screener_results"
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("refresh_runs.id"), primary_key=True)
    screener_id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker: Mapped[str] = mapped_column(String, primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, default=0)
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    sector: Mapped[str | None] = mapped_column(String, nullable=True)
    exchange: Mapped[str | None] = mapped_column(String, nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_cap: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    cash: Mapped[float | None] = mapped_column(Float, nullable=True)
    current_assets: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_assets: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_liabilities: Mapped[float | None] = mapped_column(Float, nullable=True)
    equity: Mapped[float | None] = mapped_column(Float, nullable=True)
    liabilities_over_assets: Mapped[float | None] = mapped_column(Float, nullable=True)
    revenue_growth: Mapped[float | None] = mapped_column(Float, nullable=True)
    short_interest: Mapped[float | None] = mapped_column(Float, nullable=True)
    trailing_1y_return: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_vol_1y: Mapped[float | None] = mapped_column(Float, nullable=True)
    furthest_option_expiry: Mapped[str | None] = mapped_column(String, nullable=True)
    ni_over_mcap: Mapped[float | None] = mapped_column(Float, nullable=True)
    fcf_over_mcap: Mapped[float | None] = mapped_column(Float, nullable=True)
    nearest_debt_maturity: Mapped[str | None] = mapped_column(String, nullable=True)
    bond_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    bond_yield: Mapped[float | None] = mapped_column(Float, nullable=True)
    bond_last_traded: Mapped[str | None] = mapped_column(String, nullable=True)
    going_concern_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    ch11_mentions: Mapped[int | None] = mapped_column(Integer, nullable=True)
    nt_10k_filed_at: Mapped[str | None] = mapped_column(String, nullable=True)
    nt_10q_filed_at: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_screener_results_screener_run", ScreenerResult.screener_id, ScreenerResult.run_id)


class TickerError(Base):
    __tablename__ = "ticker_errors"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("refresh_runs.id"), index=True)
    ticker: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    stage: Mapped[str] = mapped_column(String)
    error_class: Mapped[str | None] = mapped_column(String, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
