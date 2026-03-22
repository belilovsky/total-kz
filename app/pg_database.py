"""PostgreSQL database engine and session factory (SQLAlchemy 2.0).

Parallel to the existing SQLite ``database.py`` — this module provides
the async-ready PostgreSQL connection layer for Phase 2+ migration.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

engine = create_engine(
    settings.pg_database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_pg_db():
    """FastAPI dependency — yields a PostgreSQL session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
