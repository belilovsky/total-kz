#!/usr/bin/env python3
"""Backfill missing article images from inline_images, body_html, and total.kz pages.

Strategy (in order):
1. Use first image from inline_images JSONB column (fastest, ~2800 articles)
2. Extract first image from body_html (<img> tags with /storage/ or /application/uploads/)
3. Fetch the original total.kz page and extract the article image from HTML

For steps 1-2, we only store URLs (not downloading images). imgproxy on the
site already proxies from total.kz URLs.

For step 3, we use async httpx with rate limiting to be polite to total.kz.

Usage:
    python scripts/backfill_images.py                    # run full backfill
    python scripts/backfill_images.py --dry-run           # show what would be done
    python scripts/backfill_images.py --web-only           # skip DB-based, only fetch from web
    python scripts/backfill_images.py --db-only            # skip web fetching
    python scripts/backfill_images.py --limit 100          # process only N articles from web
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import httpx
import psycopg2
import psycopg2.extras

# ── Config ──────────────────────────────────────────────
PG_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOG_FILE = DATA_DIR / "backfill_images.log"

WORKERS = 10
REQUEST_DELAY = 0.5  # seconds between requests per worker
WEB_TIMEOUT = 15
USER_AGENT = "TotalKZ-ImageBackfill/1.0 (https://total.kz; admin@total.kz)"

# ── Logging ─────────────────────────────────────────────
DATA_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("backfill_images")


# ── Helpers ─────────────────────────────────────────────

def normalize_image_url(url: str) -> str:
    """Ensure image URL is absolute with https://total.kz prefix."""
    if not url:
        return ""
    url = url.strip()
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return "https://total.kz" + url
    if not url.startswith("http"):
        return "https://total.kz/" + url
    return url


def extract_image_from_html(html: str) -> str | None:
    """Extract the main article image from a total.kz page HTML."""
    if not html:
        return None

    # Strategy 1: og:image meta tag
    og = re.search(
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)',
        html,
    )
    if og:
        return normalize_image_url(og.group(1))

    # Also check reversed attribute order
    og2 = re.search(
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image',
        html,
    )
    if og2:
        return normalize_image_url(og2.group(1))

    # Strategy 2: First storage/resize image inside cs-article section
    article_start = html.find("cs-article")
    if article_start > 0:
        section = html[article_start : article_start + 5000]
        m = re.search(
            r'<img[^>]+src=["\']([^"\']*(?:/storage/|/application/uploads/)[^"\']+)',
            section,
        )
        if m:
            return normalize_image_url(m.group(1))

    # Strategy 3: Any storage/uploads image on the page (before sidebar)
    sidebar = html.find("article__sidebar")
    search_area = html[: sidebar] if sidebar > 0 else html[:30000]
    m = re.search(
        r'<img[^>]+src=["\']([^"\']*(?:/storage/|/application/uploads/)[^"\']+)',
        search_area,
    )
    if m:
        return normalize_image_url(m.group(1))

    return None


def get_first_inline_image(inline_images) -> str | None:
    """Get the first valid image URL from inline_images JSONB."""
    if not inline_images:
        return None
    if isinstance(inline_images, str):
        try:
            inline_images = json.loads(inline_images)
        except (json.JSONDecodeError, TypeError):
            return None
    if isinstance(inline_images, list) and len(inline_images) > 0:
        url = inline_images[0]
        if isinstance(url, str) and url.strip():
            return normalize_image_url(url.strip())
        if isinstance(url, dict):
            # Could be {"url": "..."} or {"src": "..."}
            u = url.get("url") or url.get("src") or url.get("image")
            if u:
                return normalize_image_url(u)
    return None


def extract_image_from_body_html(body_html: str) -> str | None:
    """Extract the first meaningful image from article body_html."""
    if not body_html:
        return None
    imgs = re.findall(
        r'<img[^>]+src=["\']([^"\']*(?:/storage/|/application/uploads/)[^"\']+)',
        body_html,
    )
    if imgs:
        return normalize_image_url(imgs[0])
    return None


# ── Phase 1: Fill from inline_images column ─────────────

def phase1_inline_images(dry_run: bool = False) -> int:
    """Fill main_image from inline_images JSONB column."""
    log.info("Phase 1: Filling from inline_images column...")
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, url, inline_images FROM articles
        WHERE (main_image IS NULL OR main_image = '')
        AND inline_images IS NOT NULL
        AND inline_images::text != 'null'
        AND inline_images::text != '[]'
    """)
    rows = cur.fetchall()
    log.info(f"  Found {len(rows)} articles with inline_images but no main_image")

    updated = 0
    for article_id, article_url, inline_images in rows:
        img_url = get_first_inline_image(inline_images)
        if img_url:
            if not dry_run:
                cur.execute(
                    "UPDATE articles SET main_image = %s, thumbnail = %s WHERE id = %s",
                    (img_url, img_url, article_id),
                )
            updated += 1

    if not dry_run:
        conn.commit()
    cur.close()
    conn.close()
    log.info(f"  Phase 1 complete: {updated} articles updated from inline_images")
    return updated


# ── Phase 2: Fill from body_html ────────────────────────

def phase2_body_html(dry_run: bool = False) -> int:
    """Fill main_image from body_html <img> tags."""
    log.info("Phase 2: Filling from body_html images...")
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, url, body_html FROM articles
        WHERE (main_image IS NULL OR main_image = '')
        AND body_html IS NOT NULL AND body_html != ''
        AND (body_html LIKE '%%/storage/%%' OR body_html LIKE '%%/application/uploads/%%')
    """)
    rows = cur.fetchall()
    log.info(f"  Found {len(rows)} articles with storage/upload images in body_html")

    updated = 0
    for article_id, article_url, body_html in rows:
        img_url = extract_image_from_body_html(body_html)
        if img_url:
            if not dry_run:
                cur.execute(
                    "UPDATE articles SET main_image = %s, thumbnail = %s WHERE id = %s",
                    (img_url, img_url, article_id),
                )
            updated += 1

    if not dry_run:
        conn.commit()
    cur.close()
    conn.close()
    log.info(f"  Phase 2 complete: {updated} articles updated from body_html")
    return updated


# ── Phase 3: Fetch from total.kz pages ─────────────────

async def fetch_article_image(
    client: httpx.AsyncClient,
    article_id: int,
    article_url: str,
    semaphore: asyncio.Semaphore,
) -> tuple[int, str | None]:
    """Fetch article page from total.kz and extract image URL."""
    async with semaphore:
        try:
            r = await client.get(article_url, timeout=WEB_TIMEOUT)
            if r.status_code != 200:
                return (article_id, None)
            img_url = extract_image_from_html(r.text)
            return (article_id, img_url)
        except Exception as e:
            log.debug(f"  Error fetching {article_url}: {e}")
            return (article_id, None)
        finally:
            await asyncio.sleep(REQUEST_DELAY)


async def phase3_web_fetch(
    dry_run: bool = False,
    limit: int = 0,
) -> int:
    """Fetch images from original total.kz pages for remaining imageless articles."""
    log.info("Phase 3: Fetching from total.kz pages...")
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor()

    query = """
        SELECT id, url FROM articles
        WHERE (main_image IS NULL OR main_image = '')
        ORDER BY pub_date DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    cur.execute(query)
    rows = cur.fetchall()
    total = len(rows)
    log.info(f"  Found {total} articles still without images")

    if total == 0:
        cur.close()
        conn.close()
        return 0

    semaphore = asyncio.Semaphore(WORKERS)
    updated = 0
    batch_size = 500

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
        timeout=WEB_TIMEOUT,
    ) as client:
        for batch_start in range(0, total, batch_size):
            batch = rows[batch_start : batch_start + batch_size]
            tasks = [
                fetch_article_image(client, aid, aurl, semaphore)
                for aid, aurl in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_updated = 0
            for result in results:
                if isinstance(result, Exception):
                    continue
                article_id, img_url = result
                if img_url:
                    if not dry_run:
                        cur.execute(
                            "UPDATE articles SET main_image = %s, thumbnail = %s WHERE id = %s",
                            (img_url, img_url, article_id),
                        )
                    batch_updated += 1
                    updated += 1

            if not dry_run:
                conn.commit()

            processed = min(batch_start + batch_size, total)
            log.info(
                f"  Progress: {processed}/{total} checked, "
                f"{batch_updated} found in this batch, {updated} total updated"
            )

    cur.close()
    conn.close()
    log.info(f"  Phase 3 complete: {updated} articles updated from web")
    return updated


# ── Main ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill missing article images")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--web-only", action="store_true", help="Skip DB-based phases")
    parser.add_argument("--db-only", action="store_true", help="Skip web fetching")
    parser.add_argument("--limit", type=int, default=0, help="Limit web fetch articles")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Image backfill started")
    if args.dry_run:
        log.info("DRY RUN — no changes will be written")
    log.info("=" * 60)

    # Get initial count
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM articles WHERE main_image IS NULL OR main_image = ''")
    initial_missing = cur.fetchone()[0]
    cur.close()
    conn.close()
    log.info(f"Articles without images: {initial_missing}")

    total_updated = 0

    if not args.web_only:
        total_updated += phase1_inline_images(args.dry_run)
        total_updated += phase2_body_html(args.dry_run)

    if not args.db_only:
        total_updated += asyncio.run(
            phase3_web_fetch(args.dry_run, args.limit)
        )

    # Final count
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM articles WHERE main_image IS NULL OR main_image = ''")
    final_missing = cur.fetchone()[0]
    cur.close()
    conn.close()

    log.info("=" * 60)
    log.info(f"Backfill complete!")
    log.info(f"  Before: {initial_missing} articles without images")
    log.info(f"  After:  {final_missing} articles without images")
    log.info(f"  Filled: {total_updated} articles")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
