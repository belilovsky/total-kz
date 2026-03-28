#!/usr/bin/env python3
"""
Fetch latest articles from total.kz RSS feed, download full content,
and import into PostgreSQL. Skips articles already in DB.

Usage:
    python scraper/fetch_latest.py              # fetch & import new articles
    python scraper/fetch_latest.py --dry-run    # just show what's new
"""

import argparse
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import httpx
import psycopg2
from selectolax.parser import HTMLParser

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_PATH = BASE_DIR / "data" / "fetch_latest.log"
RSS_URL = "https://total.kz/rss"
BASE_URL = "https://total.kz"
DB_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def parse_rss():
    """Fetch and parse RSS feed, return list of {url, title, excerpt, pub_date, image}."""
    resp = httpx.get(RSS_URL, timeout=15, follow_redirects=True)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    items = []
    for item in root.findall(".//item"):
        url = item.findtext("link", "").strip()
        title = item.findtext("title", "").strip()
        excerpt = item.findtext("description", "").strip()
        pub_date_raw = item.findtext("pubDate", "")
        enc = item.find("enclosure")
        image = enc.attrib.get("url", "") if enc is not None else ""
        items.append({
            "url": url,
            "title": title,
            "excerpt": excerpt,
            "pub_date_raw": pub_date_raw,
            "image": image,
        })
    return items


def get_existing_urls(conn):
    """Get set of URLs already in DB (PostgreSQL)."""
    cur = conn.cursor()
    cur.execute("SELECT url FROM articles")
    return {r[0] for r in cur.fetchall()}


def download_article(url):
    """Download and parse full article content from total.kz."""
    try:
        resp = httpx.get(url, timeout=httpx.Timeout(10, read=20), follow_redirects=True)
        if resp.status_code != 200:
            return None
        tree = HTMLParser(resp.text)
    except Exception as e:
        log(f"  Download error: {e}")
        return None

    # Title
    title_el = tree.css_first("h1")
    title = title_el.text(strip=True) if title_el else ""

    # Author
    author = ""
    meta_el = tree.css_first("div.article__meta")
    if meta_el:
        spans = meta_el.css("span.gray-text")
        for span in spans:
            cls = span.attributes.get("class", "") or ""
            if "meta__date" not in cls:
                author = span.text(strip=True)
                break

    # Date from URL: date_YYYY_MM_DD_HH_MM_SS
    pub_date = ""
    m = re.search(r"date_(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})", url)
    if m:
        pub_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}:{m.group(5)}:{m.group(6)}"

    # Category from URL: /ru/news/{category}/...
    category = ""
    m2 = re.search(r"/ru/news/([^/]+)/", url)
    if m2:
        category = m2.group(1)

    # Body
    body_el = tree.css_first("div.article__post__body") or tree.css_first("div.article__body")
    body_html = body_el.html if body_el else ""
    body_text = body_el.text(separator="\n", strip=True) if body_el else ""

    # Excerpt / lead
    excerpt = ""
    lead_el = tree.css_first("p.article__lead") or tree.css_first("div.article__lead")
    if lead_el:
        excerpt = lead_el.text(strip=True)

    # Main image from og:image
    main_image = ""
    thumbnail = ""
    og_img = tree.css_first("meta[property='og:image']")
    if og_img:
        og_url = og_img.attributes.get("content", "") or ""
        thumbnail = og_url
        main_image = og_url.replace("_resize_w_600_h_315", "_resize_w_830_h_465") if og_url else ""

    # Tags
    tags = []
    meta_bottom = tree.css_first("div.meta--bottom")
    if meta_bottom:
        for tag_el in meta_bottom.css("a"):
            t = tag_el.text(strip=True)
            if t and not t.startswith("#"):
                tags.append(t)

    return {
        "url": url,
        "title": title,
        "author": author,
        "pub_date": pub_date,
        "sub_category": category,
        "category_label": "",
        "excerpt": excerpt,
        "body_html": body_html,
        "body_text": body_text,
        "main_image": main_image,
        "thumbnail": thumbnail,
        "tags": json.dumps(tags, ensure_ascii=False),
        "inline_images": "[]",
        "image_credit": "",
    }


def import_article(conn, data):
    """Insert article into PostgreSQL. Skip if already exists."""
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM articles WHERE url = %s", (data["url"],))
    if cur.fetchone():
        return False  # already exists

    cur.execute("""
        INSERT INTO articles
        (url, pub_date, sub_category, category_label, title, author, excerpt,
         body_text, body_html, main_image, image_credit, thumbnail, tags, inline_images,
         imported_at, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'published')
    """, (
        data["url"], data["pub_date"], data["sub_category"], data["category_label"],
        data["title"], data["author"], data["excerpt"],
        data["body_text"], data["body_html"], data["main_image"],
        data["image_credit"], data["thumbnail"], data["tags"], data["inline_images"],
    ))
    conn.commit()
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log("Fetching RSS feed...")
    rss_items = parse_rss()
    log(f"RSS: {len(rss_items)} items")

    conn = psycopg2.connect(DB_URL)
    existing = get_existing_urls(conn)

    new_items = [item for item in rss_items if item["url"] not in existing]
    log(f"New articles: {len(new_items)} (already in DB: {len(rss_items) - len(new_items)})")

    if not new_items:
        log("Nothing new.")
        conn.close()
        return

    if args.dry_run:
        for item in new_items:
            log(f"  NEW: {item['title'][:60]}")
        conn.close()
        return

    imported = 0
    for item in new_items:
        log(f"  Downloading: {item['title'][:60]}...")
        data = download_article(item["url"])
        if data:
            if not data.get("excerpt") and item.get("excerpt"):
                data["excerpt"] = item["excerpt"]
            if not data.get("main_image") and item.get("image"):
                data["main_image"] = item["image"]
            if not data.get("thumbnail") and item.get("image"):
                data["thumbnail"] = item["image"]
            
            if import_article(conn, data):
                imported += 1
                if data.get("body_text"):
                    log(f"    OK: {len(data['body_text'])} chars")
                else:
                    log(f"    WARNING: empty body!")
        time.sleep(0.5)

    conn.close()
    log(f"Done: {imported} new articles imported")


if __name__ == "__main__":
    main()
