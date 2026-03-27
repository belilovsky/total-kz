#!/usr/bin/env python3
"""Backfill the `media` table from existing article images.

Scans the `articles` table for non-null `main_image` and `thumbnail` URLs,
then creates corresponding records in the `media` table (skipping duplicates).

Usage (inside Docker):
    python scripts/backfill_media.py                # full run
    python scripts/backfill_media.py --dry-run      # preview without inserting
    python scripts/backfill_media.py --batch-size 500
    python scripts/backfill_media.py --host-mode     # connect via localhost:5439

Environment:
    PG_DATABASE_URL   override the connection string
"""

import argparse
import logging
import mimetypes
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

# ── Config ───────────────────────────────────────────
DOCKER_PG_URL = "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz"
HOST_PG_URL = "postgresql://total_kz:T0tal_kz_2026!@localhost:5439/total_kz"

LOG_DIR = Path(__file__).resolve().parent.parent / "data"
LOG_FILE = LOG_DIR / "backfill_media.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE), mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def guess_mime_type(url: str) -> str:
    """Guess MIME type from URL extension."""
    path = urlparse(url).path.lower()
    mime, _ = mimetypes.guess_type(path)
    if mime:
        return mime
    # Common image extensions not always in mimetypes DB
    ext = Path(path).suffix
    ext_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".gif": "image/gif",
        ".webp": "image/webp", ".svg": "image/svg+xml",
        ".avif": "image/avif", ".bmp": "image/bmp",
    }
    return ext_map.get(ext, "image/jpeg")  # default to jpeg for article images


def extract_filename(url: str) -> str:
    """Extract a filename from a URL."""
    path = urlparse(url).path
    name = Path(path).name
    return name if name else "image"


def backfill(pg_url: str, batch_size: int, dry_run: bool):
    """Main backfill logic."""
    log.info("Connecting to PostgreSQL: %s", pg_url.split("@")[-1])
    conn = psycopg2.connect(pg_url)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Ensure media table exists (should already from Alembic, but just in case)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS media (
            id SERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            original_name TEXT,
            mime_type TEXT,
            file_size INTEGER,
            url TEXT NOT NULL,
            uploaded_at TEXT DEFAULT NOW()::TEXT,
            uploaded_by TEXT,
            width INTEGER,
            height INTEGER,
            alt_text TEXT DEFAULT '',
            credit TEXT DEFAULT ''
        )
    """)
    # Add unique index on url if it doesn't exist (idempotent)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_media_url ON media(url)
    """)
    conn.commit()

    # Get already-indexed URLs to skip
    cur.execute("SELECT url FROM media")
    existing_urls = {row["url"] for row in cur.fetchall()}
    log.info("Already in media table: %d URLs", len(existing_urls))

    # Count articles with images
    cur.execute("""
        SELECT COUNT(*) as cnt FROM articles
        WHERE main_image IS NOT NULL AND main_image != ''
    """)
    total_articles = cur.fetchone()["cnt"]
    log.info("Articles with main_image: %d", total_articles)

    # Process in batches using server-side cursor for memory efficiency
    cur.execute("DECLARE img_cursor CURSOR FOR "
                "SELECT DISTINCT ON (main_image) "
                "  main_image, image_credit, pub_date, title "
                "FROM articles "
                "WHERE main_image IS NOT NULL AND main_image != '' "
                "ORDER BY main_image, pub_date DESC")

    inserted = 0
    skipped = 0
    errors = 0
    batch_num = 0

    while True:
        cur.execute(f"FETCH {batch_size} FROM img_cursor")
        rows = cur.fetchall()
        if not rows:
            break

        batch_num += 1
        batch_inserts = []

        for row in rows:
            url = (row["main_image"] or "").strip()
            if not url:
                continue
            if url in existing_urls:
                skipped += 1
                continue

            filename = extract_filename(url)
            mime = guess_mime_type(url)
            credit = (row.get("image_credit") or "").strip()
            pub_date = row.get("pub_date") or ""
            title = (row.get("title") or "").strip()

            # Use article title as alt_text (truncated)
            alt_text = title[:255] if title else ""

            batch_inserts.append((
                filename,       # filename
                filename,       # original_name
                mime,           # mime_type
                url,            # url
                pub_date,       # uploaded_at (use article pub_date)
                "backfill",     # uploaded_by
                alt_text,       # alt_text
                credit,         # credit
            ))
            existing_urls.add(url)

        if batch_inserts and not dry_run:
            try:
                psycopg2.extras.execute_batch(
                    cur,
                    """INSERT INTO media
                       (filename, original_name, mime_type, url,
                        uploaded_at, uploaded_by, alt_text, credit)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (url) DO NOTHING
                    """,
                    batch_inserts,
                    page_size=100,
                )
                conn.commit()
                inserted += len(batch_inserts)
            except Exception as e:
                conn.rollback()
                log.error("Batch %d insert error: %s", batch_num, e)
                errors += len(batch_inserts)
        elif batch_inserts and dry_run:
            inserted += len(batch_inserts)

        if batch_num % 10 == 0:
            log.info(
                "Progress: batch %d | inserted %d | skipped %d | errors %d",
                batch_num, inserted, skipped, errors,
            )

    # Also process thumbnail URLs (distinct from main_image)
    log.info("Processing thumbnail URLs...")
    cur.execute("CLOSE img_cursor")
    conn.commit()

    cur.execute("DECLARE thumb_cursor CURSOR FOR "
                "SELECT DISTINCT ON (thumbnail) "
                "  thumbnail, pub_date, title "
                "FROM articles "
                "WHERE thumbnail IS NOT NULL AND thumbnail != '' "
                "ORDER BY thumbnail, pub_date DESC")

    while True:
        cur.execute(f"FETCH {batch_size} FROM thumb_cursor")
        rows = cur.fetchall()
        if not rows:
            break

        batch_num += 1
        batch_inserts = []

        for row in rows:
            url = (row["thumbnail"] or "").strip()
            if not url:
                continue
            if url in existing_urls:
                skipped += 1
                continue

            filename = extract_filename(url)
            mime = guess_mime_type(url)
            pub_date = row.get("pub_date") or ""
            title = (row.get("title") or "").strip()
            alt_text = title[:255] if title else ""

            batch_inserts.append((
                filename, filename, mime, url,
                pub_date, "backfill", alt_text, "",
            ))
            existing_urls.add(url)

        if batch_inserts and not dry_run:
            try:
                psycopg2.extras.execute_batch(
                    cur,
                    """INSERT INTO media
                       (filename, original_name, mime_type, url,
                        uploaded_at, uploaded_by, alt_text, credit)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (url) DO NOTHING
                    """,
                    batch_inserts,
                    page_size=100,
                )
                conn.commit()
                inserted += len(batch_inserts)
            except Exception as e:
                conn.rollback()
                log.error("Thumb batch %d insert error: %s", batch_num, e)
                errors += len(batch_inserts)
        elif batch_inserts and dry_run:
            inserted += len(batch_inserts)

    cur.execute("CLOSE thumb_cursor")
    conn.commit()
    cur.close()
    conn.close()

    log.info("=" * 50)
    log.info("Backfill complete%s", " (DRY RUN)" if dry_run else "")
    log.info("  Inserted: %d", inserted)
    log.info("  Skipped (already exists): %d", skipped)
    log.info("  Errors: %d", errors)


def main():
    parser = argparse.ArgumentParser(description="Backfill media table from article images")
    parser.add_argument("--dry-run", action="store_true", help="Preview without inserting")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per batch (default: 1000)")
    parser.add_argument("--host-mode", action="store_true", help="Use localhost:5439 instead of Docker db:5432")
    args = parser.parse_args()

    pg_url = os.environ.get("PG_DATABASE_URL")
    if not pg_url:
        pg_url = HOST_PG_URL if args.host_mode else DOCKER_PG_URL

    t0 = time.time()
    backfill(pg_url, args.batch_size, args.dry_run)
    log.info("Elapsed: %.1f seconds", time.time() - t0)


if __name__ == "__main__":
    main()
