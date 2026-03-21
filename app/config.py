"""Application settings — extends QazStack BaseConfig."""

from qazstack.core import BaseConfig


class Settings(BaseConfig):
    app_name: str = "Total.kz"
    version: str = "5.0.0"

    # Data directory
    data_dir: str = "data"

    # Scraper settings
    scraper_max_pages: int = 100


settings = Settings()
