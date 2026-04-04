#!/usr/bin/env python3
"""
Fill missing main_image for articles by fetching og:image from source URLs.
Also stores image_credit (copyright) from og:image:alt or site name.
"""
import json
import logging
import os
import re
import sys
import time
from urllib.parse import urlparse

import psycopg2
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("fill-images")

DB_URL = os.environ.get("PG_DATABASE_URL", "postgresql://total_kz:T0tal_kz_2026!@localhost:5432/total_kz")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Total.kz Bot/1.0; +https://total.kz)",
    "Accept": "text/html,application/xhtml+xml",
}


def extract_image_and_credit(url: str) -> tuple[str | None, str | None]:
    """Extract og:image and credit from source article page."""
    try:
        resp = requests.get(url, timeout=15, headers=HEADERS, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text[:50000]  # only first 50KB

        # Try multiple og:image patterns (different attribute orders)
        image = None
        for pattern in [
            r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
            r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
            r'"og:image"\s*:\s*"([^"]+)"',
            r'<meta\s+name=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']',
            r'<meta\s+content=["\']([^"\']+)["\']\s+name=["\']twitter:image["\']',
        ]:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                image = m.group(1)
                break

        if not image:
            return None, None

        # Clean up relative URLs
        if image.startswith("//"):
            image = "https:" + image
        elif image.startswith("/"):
            parsed = urlparse(url)
            image = f"{parsed.scheme}://{parsed.netloc}{image}"

        # Extract credit: og:site_name or domain
        credit = None
        for cp in [
            r'<meta\s+property=["\']og:site_name["\']\s+content=["\']([^"\']+)["\']',
            r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:site_name["\']',
        ]:
            cm = re.search(cp, html, re.IGNORECASE)
            if cm:
                credit = cm.group(1).strip()
                break

        if not credit:
            # Use domain as fallback
            parsed = urlparse(url)
            domain = parsed.netloc.replace("www.", "")
            credit = domain

        # Prefix with "Фото:"
        credit = f"Фото: {credit}"

        return image, credit

    except Exception as e:
        log.debug("Failed to fetch %s: %s", url[:60], e)
        return None, None


def main():
    batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 100

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Get articles without images
    cur.execute("""
        SELECT id, url, title, sub_category
        FROM articles
        WHERE (main_image IS NULL OR main_image = '')
        AND url IS NOT NULL AND url != ''
        AND url NOT LIKE '%%news.google.com%%'
        ORDER BY pub_date DESC
        LIMIT %s
    """, (batch_size,))

    articles = cur.fetchall()
    log.info("Found %d articles without images (batch=%d)", len(articles), batch_size)

    updated = 0
    failed = 0

    for art_id, url, title, sub_cat in articles:
        image, credit = extract_image_and_credit(url)

        if image:
            cur.execute(
                "UPDATE articles SET main_image = %s, image_credit = %s WHERE id = %s",
                (image, credit, art_id)
            )
            conn.commit()
            updated += 1
            log.info("[%d/%d] ✓ %s → %s", updated, len(articles), title[:50], credit)
        else:
            failed += 1
            log.info("[%d/%d] ✗ No image: %s (%s)", failed, len(articles), title[:50], url[:60])

        # Rate limit: 0.5s between requests
        time.sleep(0.5)

    conn.close()
    log.info("Done: %d updated, %d failed out of %d", updated, failed, len(articles))


if __name__ == "__main__":
    main()
