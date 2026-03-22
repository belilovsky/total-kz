#!/usr/bin/env python3
"""Idempotent SQLite → PostgreSQL data migration for total-kz.

Reads from the existing SQLite database at ``data/total.db`` and writes
into the PostgreSQL instance defined by ``PG_DATABASE_URL``.

Usage:
    python -m scripts.migrate_sqlite_to_pg          # default paths
    python -m scripts.migrate_sqlite_to_pg --sqlite data/total.db

The script:
  1. Creates all tables via ``Base.metadata.create_all`` (safe if they exist).
  2. Migrates each table with INSERT … ON CONFLICT DO NOTHING (idempotent).
  3. Resets PostgreSQL sequences to max(id)+1 for each serial column.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from sqlalchemy import inspect, text

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.models import Base
from app.pg_database import SessionLocal, engine

# Ordered by foreign-key dependency (parents first)
TABLE_ORDER = [
    "articles",
    "entities",
    "article_entities",
    "article_tags",
    "scrape_runs",
    "scrape_log",
    "media",
    "article_revisions",
    "users",
    "categories",
    "authors_managed",
    "audit_log",
    "article_comments",
    "stories",
    "article_stories",
    "article_enrichments",
]

# Columns that store JSON arrays/objects in SQLite (TEXT) but JSONB in PG
JSONB_COLUMNS = {
    "articles": {"tags", "inline_images", "body_blocks"},
    "article_enrichments": {"keywords"},
}


def get_sqlite_conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def table_exists_sqlite(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row[0] > 0


def get_sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def migrate_table(
    sqlite_conn: sqlite3.Connection,
    pg_session,
    table: str,
    pg_columns: set[str],
    batch_size: int = 500,
) -> int:
    """Migrate a single table. Returns row count."""
    if not table_exists_sqlite(sqlite_conn, table):
        print(f"  ⏭  {table}: not in SQLite, skipping")
        return 0

    sqlite_cols = get_sqlite_columns(sqlite_conn, table)
    # Only migrate columns that exist in both SQLite and PG models
    common_cols = [c for c in sqlite_cols if c in pg_columns]
    if not common_cols:
        print(f"  ⏭  {table}: no common columns, skipping")
        return 0

    jsonb_cols = JSONB_COLUMNS.get(table, set())

    rows = sqlite_conn.execute(
        f"SELECT {', '.join(common_cols)} FROM {table}"
    ).fetchall()

    if not rows:
        print(f"  ⏭  {table}: empty in SQLite")
        return 0

    # Determine conflict target
    inspector = inspect(engine)
    pk_cols = inspector.get_pk_constraint(table).get("constrained_columns", [])
    if not pk_cols:
        pk_cols = ["id"]

    conflict_clause = ", ".join(pk_cols)
    col_list = ", ".join(common_cols)
    param_list = ", ".join(f":{c}" for c in common_cols)

    insert_sql = text(
        f"INSERT INTO {table} ({col_list}) VALUES ({param_list}) "
        f"ON CONFLICT ({conflict_clause}) DO NOTHING"
    )

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        params = []
        for row in batch:
            d = dict(row)
            # Parse JSON TEXT → Python dict/list for JSONB columns
            for jcol in jsonb_cols:
                if jcol in d and isinstance(d[jcol], str):
                    try:
                        d[jcol] = json.loads(d[jcol])
                    except (json.JSONDecodeError, TypeError):
                        d[jcol] = None
            params.append(d)
        pg_session.execute(insert_sql, params)
        pg_session.commit()
        total += len(batch)

    return total


def reset_sequences(pg_session) -> None:
    """Reset PostgreSQL serial sequences to max(id) + 1."""
    tables_with_serial = [
        "articles", "entities", "scrape_runs", "scrape_log",
        "media", "article_revisions", "users", "categories",
        "authors_managed", "audit_log", "article_comments", "stories",
    ]
    for table in tables_with_serial:
        try:
            seq_name = f"{table}_id_seq"
            pg_session.execute(
                text(
                    f"SELECT setval('{seq_name}', COALESCE((SELECT MAX(id) FROM {table}), 0) + 1, false)"
                )
            )
        except Exception as e:
            print(f"  ⚠  Could not reset sequence for {table}: {e}")
            pg_session.rollback()
    pg_session.commit()


def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite → PostgreSQL")
    parser.add_argument(
        "--sqlite",
        default=str(Path(__file__).resolve().parent.parent / "data" / "total.db"),
        help="Path to SQLite database",
    )
    args = parser.parse_args()

    sqlite_path = args.sqlite
    if not Path(sqlite_path).exists():
        print(f"SQLite database not found: {sqlite_path}")
        sys.exit(1)

    print(f"Source:  {sqlite_path}")
    print(f"Target:  {settings.pg_database_url}")
    print()

    # Step 1: Create all tables
    print("Creating PostgreSQL tables …")
    Base.metadata.create_all(engine)
    print("  ✓ Tables created\n")

    # Step 2: Migrate data
    sqlite_conn = get_sqlite_conn(sqlite_path)
    pg_session = SessionLocal()

    inspector = inspect(engine)
    pg_tables = set(inspector.get_table_names())

    try:
        for table in TABLE_ORDER:
            if table not in pg_tables:
                print(f"  ⏭  {table}: not in PG schema, skipping")
                continue
            pg_cols = {c["name"] for c in inspector.get_columns(table)}
            count = migrate_table(sqlite_conn, pg_session, table, pg_cols)
            if count:
                print(f"  ✓ {table}: {count:,} rows")

        # Step 3: Reset sequences
        print("\nResetting sequences …")
        reset_sequences(pg_session)
        print("  ✓ Sequences reset")

    finally:
        pg_session.close()
        sqlite_conn.close()

    print("\nMigration complete.")


if __name__ == "__main__":
    main()
