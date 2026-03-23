"""Full reindex of all articles into Meilisearch.

Usage: python -m scraper.reindex_meilisearch

Standalone — does NOT import app.database (avoids qazstack dependency).
Reads SQLite directly.
"""

import json
import os
import sqlite3
import sys

import httpx

MEILI_URL = os.environ.get("MEILI_URL", "http://meilisearch:7700")
MEILI_KEY = os.environ.get("MEILI_MASTER_KEY", "total-kz-search-key-2026")
INDEX = "articles"

DB_PATH = os.environ.get("DB_PATH", "/app/data/total.db")

_headers = {"Authorization": f"Bearer {MEILI_KEY}", "Content-Type": "application/json"}


def setup_index():
    """Create index with settings."""
    httpx.post(f"{MEILI_URL}/indexes", json={"uid": INDEX, "primaryKey": "id"}, headers=_headers, timeout=5)
    settings = {
        "searchableAttributes": ["title", "excerpt", "body_text", "author", "tags"],
        "filterableAttributes": ["sub_category", "status", "author", "pub_date"],
        "sortableAttributes": ["pub_date"],
        "displayedAttributes": ["id", "title", "excerpt", "author", "sub_category", "pub_date", "tags", "thumbnail", "main_image", "url", "status"],
    }
    httpx.patch(f"{MEILI_URL}/indexes/{INDEX}/settings", json=settings, headers=_headers, timeout=10)
    print(f"Index '{INDEX}' configured.")


def reindex_all():
    """Bulk reindex all articles from SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, title, excerpt, body_text, author, sub_category, pub_date, tags, status, thumbnail, main_image, url
        FROM articles
    """).fetchall()
    conn.close()

    docs = []
    for r in rows:
        tags = []
        try:
            tags = json.loads(r["tags"] or "[]")
        except Exception:
            pass
        if not isinstance(tags, list):
            tags = []
        docs.append({
            "id": r["id"],
            "title": r["title"] or "",
            "excerpt": r["excerpt"] or "",
            "body_text": (r["body_text"] or "")[:5000],
            "author": r["author"] or "",
            "sub_category": r["sub_category"] or "",
            "pub_date": r["pub_date"] or "",
            "tags": tags,
            "status": r["status"] or "published",
            "thumbnail": r["thumbnail"] or r["main_image"] or "",
            "main_image": r["main_image"] or "",
            "url": r["url"] or "",
        })

    total = len(docs)
    print(f"Found {total} articles to index.")

    batch_size = 1000
    for i in range(0, total, batch_size):
        chunk = docs[i:i + batch_size]
        r = httpx.post(f"{MEILI_URL}/indexes/{INDEX}/documents", json=chunk, headers=_headers, timeout=30)
        print(f"  Batch {i // batch_size + 1}: sent {len(chunk)} docs (status {r.status_code})")

    print(f"Done — {total} articles sent to Meilisearch.")


if __name__ == "__main__":
    print("Setting up Meilisearch index...")
    setup_index()
    print("Reindexing all articles...")
    reindex_all()
