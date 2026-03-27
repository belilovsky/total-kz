"""Create article_translations table in PostgreSQL.

Usage:
    python scripts/create_translations_table.py
"""

import os
import psycopg2


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)

SQL = """
CREATE TABLE IF NOT EXISTS article_translations (
    id SERIAL PRIMARY KEY,
    article_id INTEGER NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    lang CHAR(2) NOT NULL DEFAULT 'kz',
    title TEXT,
    excerpt TEXT,
    body_html TEXT,
    body_text TEXT,
    meta_description TEXT,
    translated_at TIMESTAMP DEFAULT NOW(),
    translation_quality FLOAT,
    reviewed BOOLEAN DEFAULT FALSE,
    reviewed_by TEXT,
    reviewed_at TIMESTAMP,
    UNIQUE(article_id, lang)
);

CREATE INDEX IF NOT EXISTS idx_translations_article
    ON article_translations(article_id, lang);
"""


def main():
    print(f"Connecting to {DATABASE_URL.split('@')[1]}...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(SQL)
    print("article_translations table created (or already exists).")

    # Verify
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'article_translations' ORDER BY ordinal_position"
    )
    cols = cur.fetchall()
    print(f"Columns ({len(cols)}):")
    for name, dtype in cols:
        print(f"  {name}: {dtype}")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
