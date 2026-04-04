#!/usr/bin/env python3
"""Fill missing main_image for articles by fetching og:image from source URLs."""
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

DB_URL = os.environ.get("PG_DATABASE_URL", "postgresql://total_kz:T0tal_kz_2026!@127.0.0.1:5437/total_kz")

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def extract_image_and_credit(url):
    """Extract og:image and credit from source article page."""
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": UA}, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text[:80000]

        image = None
        # Flexible patterns: allow any attributes between meta and property/content
        patterns = [
            r'property=["\']og:image["\'][^>]*?content=["\']([^"\']+)["\']',
            r'content=["\']([^"\']+)["\'][^>]*?property=["\']og:image["\']',
            r'name=["\']twitter:image["\'][^>]*?content=["\']([^"\']+)["\']',
            r'content=["\']([^"\']+)["\'][^>]*?name=["\']twitter:image["\']',
        ]
        for pat in patterns:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                image = m.group(1)
                break

        if not image:
            return None, None

        # Fix relative URLs
        if image.startswith("//"):
            image = "https:" + image
        elif image.startswith("/"):
            parsed = urlparse(url)
            image = f"{parsed.scheme}://{parsed.netloc}{image}"

        # Extract credit from og:site_name or domain
        credit = None
        credit_patterns = [
            r'property=["\']og:site_name["\'][^>]*?content=["\']([^"\']+)["\']',
            r'content=["\']([^"\']+)["\'][^>]*?property=["\']og:site_name["\']',
        ]
        for cp in credit_patterns:
            cm = re.search(cp, html, re.IGNORECASE)
            if cm:
                credit = cm.group(1).strip()
                break
        if not credit:
            parsed = urlparse(url)
            credit = parsed.netloc.replace("www.", "")

        credit = f"Фото: {credit}"
        return image, credit

    except Exception as e:
        log.debug("Failed %s: %s", url[:60], e)
        return None, None


def main():
    batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 100

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

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
    log.info("Found %d articles without images", len(articles))

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
            log.info("[%d] + %s -> %s", updated, title[:50], credit)
        else:
            failed += 1

        time.sleep(0.3)

    conn.close()
    log.info("Done: %d updated, %d failed out of %d", updated, failed, len(articles))


if __name__ == "__main__":
    main()
