from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    sec_user_agent: str = "StockScreener dev@example.com"
    db_path: str = "../data/cache.db"
    edgar_cache_dir: str = "../data/edgar"
    nightly_refresh_hour: int = 2
    nightly_refresh_minute: int = 0
    yf_concurrency: int = 2
    edgar_rate_per_sec: int = 9
    incremental_max_age_hours: int = 20

    @property
    def db_url(self) -> str:
        p = Path(self.db_path).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{p.as_posix()}"

    @property
    def edgar_dir(self) -> Path:
        p = Path(self.edgar_cache_dir).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p


settings = Settings()
