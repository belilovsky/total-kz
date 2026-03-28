#!/usr/bin/env python3
"""
Create article_nlp table in PostgreSQL for GPT-powered NLP extraction results.

Usage:
  python scripts/create_nlp_table.py           # create table
  python scripts/create_nlp_table.py --drop     # drop and recreate
"""

import argparse
import os
import sys

import psycopg2

PG_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS article_nlp (
    article_id INTEGER PRIMARY KEY REFERENCES articles(id) ON DELETE CASCADE,
    sentiment TEXT,
    sentiment_score FLOAT,
    importance INTEGER,
    geo_focus TEXT,
    topics JSONB DEFAULT '[]',
    key_facts JSONB DEFAULT '[]',
    events JSONB DEFAULT '[]',
    money_mentions JSONB DEFAULT '[]',
    laws_mentioned JSONB DEFAULT '[]',
    related_context TEXT,
    topics_kz JSONB DEFAULT '[]',
    key_facts_kz JSONB DEFAULT '[]',
    processed_at TIMESTAMP DEFAULT NOW(),
    model TEXT DEFAULT 'gpt-4o-mini'
);

CREATE INDEX IF NOT EXISTS idx_article_nlp_sentiment ON article_nlp(sentiment);
CREATE INDEX IF NOT EXISTS idx_article_nlp_importance ON article_nlp(importance);
CREATE INDEX IF NOT EXISTS idx_article_nlp_geo ON article_nlp(geo_focus);
CREATE INDEX IF NOT EXISTS idx_article_nlp_processed ON article_nlp(processed_at);
"""

DROP_SQL = "DROP TABLE IF EXISTS article_nlp CASCADE;"


def main():
    parser = argparse.ArgumentParser(description="Create article_nlp table")
    parser.add_argument("--drop", action="store_true", help="Drop and recreate table")
    args = parser.parse_args()

    print(f"Connecting to PostgreSQL...")
    conn = psycopg2.connect(PG_URL)
    conn.autocommit = True
    cur = conn.cursor()

    if args.drop:
        print("Dropping article_nlp table...")
        cur.execute(DROP_SQL)

    print("Creating article_nlp table...")
    cur.execute(CREATE_SQL)

    # Verify
    cur.execute("SELECT COUNT(*) FROM article_nlp")
    count = cur.fetchone()[0]
    print(f"article_nlp table ready — {count} rows")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
