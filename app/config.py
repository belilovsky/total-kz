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

    # Umami analytics
    umami_share_url: str = ""
    umami_api_url: str = "http://127.0.0.1:3000"
    umami_username: str = "admin"
    umami_password: str = "umami"
    umami_website_id: str = "be22e361-8abe-4f3e-be5f-80529fb98789"


settings = Settings()
