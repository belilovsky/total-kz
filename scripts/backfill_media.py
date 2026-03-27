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
    ext = Path(path).suffix
    ext_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".gif": "image/gif",
        ".webp": "image/webp", ".svg": "image/svg+xml",
        ".avif": "image/avif", ".bmp": "image/bmp",
    }
    return ext_map.get(ext, "image/jpeg")


def extract_filename(url: str) -> str:
    """Extract a filename from a URL."""
    path = urlparse(url).path
    name = Path(path).name
    return name if name else "image"


def backfill(pg_url: str, batch_size: int, dry_run: bool):
    """Main backfill logic — uses two connections: one for reading, one for writing."""
    log.info("Connecting to PostgreSQL: %s", pg_url.split("@")[-1])

    # Write connection — for DDL and inserts
    conn_w = psycopg2.connect(pg_url)
    conn_w.autocommit = True
    cur_w = conn_w.cursor()

    # Ensure media table exists
    cur_w.execute("""
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
    cur_w.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_media_url ON media(url)
    """)

    # Get already-indexed URLs
    cur_w.execute("SELECT url FROM media")
    existing_urls = {row[0] for row in cur_w.fetchall()}
    log.info("Already in media table: %d URLs", len(existing_urls))

    # Read connection — for server-side cursor (stays in one transaction)
    conn_r = psycopg2.connect(pg_url)
    conn_r.autocommit = False

    # Count
    cur_count = conn_r.cursor()
    cur_count.execute("""
        SELECT COUNT(*) FROM articles
        WHERE main_image IS NOT NULL AND main_image != ''
    """)
    total_articles = cur_count.fetchone()[0]
    log.info("Articles with main_image: %d", total_articles)
    cur_count.close()

    # ── Phase 1: main_image ──────────────────────────
    log.info("Phase 1: Processing main_image URLs...")
    cur_r = conn_r.cursor("img_cursor", cursor_factory=psycopg2.extras.RealDictCursor)
    cur_r.itersize = batch_size
    cur_r.execute(
        "SELECT DISTINCT ON (main_image) "
        "  main_image, image_credit, pub_date, title "
        "FROM articles "
        "WHERE main_image IS NOT NULL AND main_image != '' "
        "ORDER BY main_image, pub_date DESC"
    )

    inserted = 0
    skipped = 0
    errors = 0
    batch_num = 0
    batch_inserts = []

    for row in cur_r:
        url = (row["main_image"] or "").strip()
        if not url or url in existing_urls:
            skipped += 1
            continue

        filename = extract_filename(url)
        mime = guess_mime_type(url)
        credit = (row.get("image_credit") or "").strip()
        pub_date = row.get("pub_date") or ""
        title = (row.get("title") or "").strip()
        alt_text = title[:255] if title else ""

        batch_inserts.append((
            filename, filename, mime, url,
            str(pub_date), "backfill", alt_text, credit,
        ))
        existing_urls.add(url)

        if len(batch_inserts) >= batch_size:
            batch_num += 1
            if not dry_run:
                try:
                    psycopg2.extras.execute_batch(
                        cur_w,
                        """INSERT INTO media
                           (filename, original_name, mime_type, url,
                            uploaded_at, uploaded_by, alt_text, credit)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (url) DO NOTHING
                        """,
                        batch_inserts,
                        page_size=100,
                    )
                    inserted += len(batch_inserts)
                except Exception as e:
                    log.error("Batch %d insert error: %s", batch_num, e)
                    errors += len(batch_inserts)
            else:
                inserted += len(batch_inserts)
            batch_inserts = []

            if batch_num % 10 == 0:
                log.info(
                    "Progress: batch %d | inserted %d | skipped %d | errors %d",
                    batch_num, inserted, skipped, errors,
                )

    # Flush remaining
    if batch_inserts:
        batch_num += 1
        if not dry_run:
            try:
                psycopg2.extras.execute_batch(
                    cur_w,
                    """INSERT INTO media
                       (filename, original_name, mime_type, url,
                        uploaded_at, uploaded_by, alt_text, credit)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (url) DO NOTHING
                    """,
                    batch_inserts,
                    page_size=100,
                )
                inserted += len(batch_inserts)
            except Exception as e:
                log.error("Final batch insert error: %s", e)
                errors += len(batch_inserts)
        else:
            inserted += len(batch_inserts)
        batch_inserts = []

    cur_r.close()
    log.info("Phase 1 done: %d inserted, %d skipped", inserted, skipped)

    # ── Phase 2: thumbnail URLs ──────────────────────
    log.info("Phase 2: Processing thumbnail URLs...")
    cur_r2 = conn_r.cursor("thumb_cursor", cursor_factory=psycopg2.extras.RealDictCursor)
    cur_r2.itersize = batch_size
    cur_r2.execute(
        "SELECT DISTINCT ON (thumbnail) "
        "  thumbnail, pub_date, title "
        "FROM articles "
        "WHERE thumbnail IS NOT NULL AND thumbnail != '' "
        "ORDER BY thumbnail, pub_date DESC"
    )

    for row in cur_r2:
        url = (row["thumbnail"] or "").strip()
        if not url or url in existing_urls:
            skipped += 1
            continue

        filename = extract_filename(url)
        mime = guess_mime_type(url)
        pub_date = row.get("pub_date") or ""
        title = (row.get("title") or "").strip()
        alt_text = title[:255] if title else ""

        batch_inserts.append((
            filename, filename, mime, url,
            str(pub_date), "backfill", alt_text, "",
        ))
        existing_urls.add(url)

        if len(batch_inserts) >= batch_size:
            batch_num += 1
            if not dry_run:
                try:
                    psycopg2.extras.execute_batch(
                        cur_w,
                        """INSERT INTO media
                           (filename, original_name, mime_type, url,
                            uploaded_at, uploaded_by, alt_text, credit)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (url) DO NOTHING
                        """,
                        batch_inserts,
                        page_size=100,
                    )
                    inserted += len(batch_inserts)
                except Exception as e:
                    log.error("Thumb batch %d insert error: %s", batch_num, e)
                    errors += len(batch_inserts)
            else:
                inserted += len(batch_inserts)
            batch_inserts = []

    # Flush remaining
    if batch_inserts:
        if not dry_run:
            try:
                psycopg2.extras.execute_batch(
                    cur_w,
                    """INSERT INTO media
                       (filename, original_name, mime_type, url,
                        uploaded_at, uploaded_by, alt_text, credit)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (url) DO NOTHING
                    """,
                    batch_inserts,
                    page_size=100,
                )
                inserted += len(batch_inserts)
            except Exception as e:
                log.error("Final thumb insert error: %s", e)
                errors += len(batch_inserts)
        else:
            inserted += len(batch_inserts)

    cur_r2.close()
    conn_r.close()
    cur_w.close()
    conn_w.close()

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
