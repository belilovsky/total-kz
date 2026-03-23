#!/usr/bin/env python3
"""Create persons/person_positions tables and seed data.

Run inside Docker:
  docker compose exec app python scripts/migrate_persons.py

Or locally:
  python scripts/migrate_persons.py
"""

import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"
SEED_PATH = Path(__file__).resolve().parent / "persons_seed.json"


def main():
    print(f"Database: {DB_PATH} (exists={DB_PATH.exists()})")
    conn = sqlite3.connect(str(DB_PATH))

    # Check if persons table already exists
    existing = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('persons','person_positions')"
    ).fetchall()]

    if "persons" in existing:
        count = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        print(f"persons table already exists with {count} rows")
        if count > 0:
            print("Skipping — data already present.")
            conn.close()
            return

    # Create tables
    print("Creating persons table...")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS persons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER REFERENCES entities(id),
            slug TEXT UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            short_name TEXT,
            birth_date TEXT,
            birth_place TEXT,
            photo_url TEXT,
            current_position TEXT,
            current_org TEXT,
            bio_summary TEXT,
            education TEXT,
            languages TEXT,
            awards TEXT,
            zakon_doc_id TEXT,
            person_type TEXT DEFAULT 'government',
            is_featured INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)

    print("Creating person_positions table...")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS person_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER REFERENCES persons(id),
            position_title TEXT NOT NULL,
            organization TEXT,
            start_date TEXT,
            end_date TEXT,
            decree_url TEXT,
            sort_order INTEGER DEFAULT 0,
            source TEXT DEFAULT 'seed'
        )
    """)

    # Also create ad_placements if missing
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ad_placements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slot_id TEXT UNIQUE NOT NULL,
            label TEXT,
            width INTEGER,
            height INTEGER,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.commit()

    # Load seed data
    if not SEED_PATH.exists():
        print(f"No seed file at {SEED_PATH}, tables created but empty.")
        conn.close()
        return

    print(f"Loading seed data from {SEED_PATH}...")
    with open(SEED_PATH) as f:
        data = json.load(f)

    # Insert persons
    persons = data.get("persons", [])
    for p in persons:
        cols = [k for k in p.keys() if k != "id"]
        vals = [p[k] for k in cols]
        placeholders = ",".join(["?"] * len(cols))
        col_names = ",".join(cols)
        conn.execute(f"INSERT OR IGNORE INTO persons ({col_names}) VALUES ({placeholders})", vals)

    # Insert positions
    positions = data.get("positions", [])
    for pos in positions:
        cols = [k for k in pos.keys() if k != "id"]
        vals = [pos[k] for k in cols]
        placeholders = ",".join(["?"] * len(cols))
        col_names = ",".join(cols)
        conn.execute(f"INSERT OR IGNORE INTO person_positions ({col_names}) VALUES ({placeholders})", vals)

    conn.commit()

    # Verify
    p_count = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    pp_count = conn.execute("SELECT COUNT(*) FROM person_positions").fetchone()[0]
    print(f"Done! persons: {p_count}, positions: {pp_count}")

    conn.close()


if __name__ == "__main__":
    main()
