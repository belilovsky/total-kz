"""Application settings – extends QazStack BaseConfig."""

from qazstack.core import BaseConfig


class Settings(BaseConfig):
    app_name: str = "Total.kz"
    version: str = "5.0.0"

    # Data directory
    data_dir: str = "data"

    # PostgreSQL
    pg_database_url: str = "postgresql://total_kz:total_kz@db:5432/total_kz"
    use_postgres: bool = False  # Set True to switch from SQLite to PostgreSQL

    # Scraper settings
    scraper_max_pages: int = 100


settings = Settings()
