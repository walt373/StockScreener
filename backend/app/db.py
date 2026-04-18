from collections.abc import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from .config import settings

engine = create_engine(
    settings.db_url,
    connect_args={"check_same_thread": False, "timeout": 30},
    future=True,
)


@event.listens_for(engine, "connect")
def _sqlite_pragma(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.execute("PRAGMA foreign_keys=ON")
    cur.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_session() -> Iterator[Session]:
    s = SessionLocal()
    try:
        yield s
    finally:
        s.close()


_MIGRATIONS: list[tuple[str, str]] = [
    # (table, "column_name TYPE") — SQLite requires one ADD COLUMN per statement
    ("edgar_company_cache", "latest_nt_10k_filed DATETIME"),
    ("edgar_company_cache", "latest_nt_10q_filed DATETIME"),
    ("screener_results", "nt_10k_filed_at VARCHAR"),
    ("screener_results", "nt_10q_filed_at VARCHAR"),
    ("fundamentals_cache", "total_assets FLOAT"),
    ("screener_results", "total_assets FLOAT"),
    ("screener_results", "liabilities_over_assets FLOAT"),
]


def _run_migrations() -> None:
    """Idempotent ALTER TABLE for columns added after initial schema."""
    import logging
    from sqlalchemy import text

    log = logging.getLogger(__name__)
    with engine.begin() as conn:
        for table, col_def in _MIGRATIONS:
            col_name = col_def.split()[0]
            existing = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
            names = {row[1] for row in existing}
            if col_name in names:
                continue
            log.info("Migrating: adding %s.%s", table, col_name)
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col_def}"))


def init_db() -> None:
    from . import models  # noqa: F401

    models.Base.metadata.create_all(engine)
    _run_migrations()
