"""Build FTS5 index on articles for content-based similarity search.

Creates a virtual table `articles_fts` that indexes title, excerpt, and keywords.
This enables BM25-based related article discovery that works for ALL articles,
even those without entities/tags/enrichments.

Usage:
    python scripts/build_fts_index.py
"""

import json
import sqlite3
import time
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"


def build_fts_index(db_path: str = str(DB_PATH)):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    print("Building FTS5 index for articles...")
    t0 = time.time()

    # Drop old FTS table if exists
    conn.execute("DROP TABLE IF EXISTS articles_fts")

    # Create FTS5 table with title + excerpt + keywords
    # Using external content table to save space
    conn.execute("""
        CREATE VIRTUAL TABLE articles_fts USING fts5(
            title,
            excerpt,
            keywords,
            content='articles_fts_content',
            content_rowid='rowid',
            tokenize='unicode61 remove_diacritics 2'
        )
    """)

    # Create the content table
    conn.execute("DROP TABLE IF EXISTS articles_fts_content")
    conn.execute("""
        CREATE TABLE articles_fts_content (
            rowid INTEGER PRIMARY KEY,
            title TEXT,
            excerpt TEXT,
            keywords TEXT
        )
    """)

    # Populate content table from articles + enrichments
    print("  Collecting content from articles + enrichments...")
    cursor = conn.execute("""
        SELECT a.id, a.title, a.excerpt,
               COALESCE(e.keywords, '') as keywords
        FROM articles a
        LEFT JOIN article_enrichments e ON e.article_id = a.id
        WHERE a.status IS NULL OR a.status != 'deleted'
        ORDER BY a.id
    """)

    batch = []
    count = 0
    for row in cursor:
        article_id, title, excerpt, keywords_raw = row
        # Parse keywords JSON array into space-separated string
        kw_text = ""
        if keywords_raw and keywords_raw.strip():
            try:
                kw_list = json.loads(keywords_raw)
                if isinstance(kw_list, list):
                    kw_text = " ".join(kw_list)
            except (json.JSONDecodeError, TypeError):
                kw_text = keywords_raw

        batch.append((article_id, title or "", excerpt or "", kw_text))
        count += 1

        if len(batch) >= 5000:
            conn.executemany(
                "INSERT INTO articles_fts_content (rowid, title, excerpt, keywords) VALUES (?, ?, ?, ?)",
                batch,
            )
            batch.clear()
            print(f"  ... {count:,} articles processed")

    if batch:
        conn.executemany(
            "INSERT INTO articles_fts_content (rowid, title, excerpt, keywords) VALUES (?, ?, ?, ?)",
            batch,
        )

    print(f"  Total: {count:,} articles in content table")

    # Populate FTS index from content table
    print("  Building FTS index...")
    conn.execute("""
        INSERT INTO articles_fts (rowid, title, excerpt, keywords)
        SELECT rowid, title, excerpt, keywords FROM articles_fts_content
    """)

    # Optimize FTS index
    print("  Optimizing FTS index...")
    conn.execute("INSERT INTO articles_fts(articles_fts) VALUES('optimize')")

    conn.commit()

    # Test the index
    print("\n  Testing FTS5 search...")
    test_queries = [
        "Токаев Казахстан",
        "нефть цены экономика",
        "спорт олимпиада медаль",
    ]
    for q in test_queries:
        t1 = time.time()
        rows = conn.execute(
            """
            SELECT rowid, title, rank
            FROM articles_fts
            WHERE articles_fts MATCH ?
            ORDER BY rank
            LIMIT 5
        """,
            (q,),
        ).fetchall()
        t2 = time.time()
        print(f"  '{q}': {len(rows)} results in {t2-t1:.3f}s")
        for r in rows[:2]:
            print(f"    [{r[2]:.2f}] {r[1][:70]}")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s. FTS5 index ready.")

    # Check size
    conn.execute("ANALYZE")
    sz = conn.execute(
        "SELECT page_count * page_size FROM pragma_page_count, pragma_page_size"
    ).fetchone()[0]
    print(f"Total DB size: {sz / 1024 / 1024:.0f} MB")

    conn.close()


if __name__ == "__main__":
    build_fts_index()
