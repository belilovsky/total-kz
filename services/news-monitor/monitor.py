#!/usr/bin/env python3
"""News monitor for Total.kz — main loop.

Checks RSS feeds, rewrites via GPT, publishes to CMS, notifies via Telegram.

Usage:
    python monitor.py              # Normal mode
    python monitor.py --dry-run    # Check sources, show what would be processed
"""

import argparse
import hashlib
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import httpx
import yaml
from bs4 import BeautifulSoup

from notifier import init_telegram, notify_sync
from publisher import publish_article
from rewriter import rewrite_article

# ── Logging ────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("news-monitor")

# ── Dedup DB (SQLite, local to container) ──────────────

SEEN_DB = Path("/app/data/seen_urls.db")


def init_seen_db() -> sqlite3.Connection:
    """Initialize SQLite database for tracking seen URLs."""
    SEEN_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(SEEN_DB))
    conn.execute(
        "CREATE TABLE IF NOT EXISTS seen ("
        "  url_hash TEXT PRIMARY KEY,"
        "  url TEXT,"
        "  source TEXT,"
        "  seen_at TEXT"
        ")"
    )
    conn.commit()
    return conn


def is_seen(conn: sqlite3.Connection, url: str) -> bool:
    h = hashlib.sha256(url.encode()).hexdigest()
    row = conn.execute("SELECT 1 FROM seen WHERE url_hash = ?", (h,)).fetchone()
    return row is not None


def mark_seen(conn: sqlite3.Connection, url: str, source: str):
    h = hashlib.sha256(url.encode()).hexdigest()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO seen (url_hash, url, source, seen_at) VALUES (?, ?, ?, ?)",
        (h, url, source, now),
    )
    conn.commit()


# ── Source loading ─────────────────────────────────────


def load_sources() -> tuple[list[dict], dict[str, int]]:
    """Load sources.yaml, return (sources, intervals)."""
    config_path = Path(__file__).parent / "sources.yaml"
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return cfg["sources"], cfg["intervals"]


# ── RSS fetching ───────────────────────────────────────

HTTP_CLIENT = httpx.Client(
    timeout=30,
    follow_redirects=True,
    headers={"User-Agent": "TotalKZ-NewsMonitor/1.0"},
)


def fetch_rss(url: str) -> list[dict]:
    """Fetch RSS feed and return list of entries."""
    try:
        resp = HTTP_CLIENT.get(url)
        resp.raise_for_status()
        feed = feedparser.parse(resp.text)
        entries = []
        for entry in feed.entries[:10]:  # max 10 per source per check
            entries.append({
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "summary": entry.get("summary", ""),
                "published": entry.get("published", ""),
            })
        return entries
    except Exception as e:
        log.error("Failed to fetch RSS %s: %s", url, e)
        return []


def extract_body(url: str) -> str:
    """Fetch full article body from URL."""
    try:
        resp = HTTP_CLIENT.get(url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Try common article body selectors
        for selector in [
            "article",
            ".article-body",
            ".article__body",
            ".story-body",
            ".entry-content",
            "[itemprop='articleBody']",
            ".content-body",
            "main",
        ]:
            body = soup.select_one(selector)
            if body:
                # Remove scripts, styles, nav
                for tag in body.find_all(["script", "style", "nav", "aside", "footer"]):
                    tag.decompose()
                return body.get_text(separator="\n", strip=True)[:5000]

        # Fallback: grab all <p> tags
        paragraphs = soup.find_all("p")
        text = "\n".join(p.get_text(strip=True) for p in paragraphs)
        return text[:5000] if text else ""

    except Exception as e:
        log.warning("Failed to extract body from %s: %s", url, e)
        return ""


def matches_keywords(text: str, keywords: list[str] | None) -> bool:
    """Check if text contains any of the keywords (case-insensitive)."""
    if not keywords:
        return True  # No filter — accept all
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


# ── Main processing ───────────────────────────────────


def process_entry(
    entry: dict,
    source: dict,
    seen_conn: sqlite3.Connection,
    dry_run: bool = False,
) -> bool:
    """Process a single feed entry. Returns True if new article was handled."""
    url = entry["url"]
    if not url or is_seen(seen_conn, url):
        return False

    title = entry["title"]
    summary = entry["summary"]

    # Keyword filtering for sources that have keywords
    keywords = source.get("keywords")
    if keywords and not matches_keywords(f"{title} {summary}", keywords):
        return False

    if dry_run:
        log.info(
            "[DRY-RUN] Would process: %s\n  Source: %s\n  URL: %s",
            title[:80],
            source["name"],
            url,
        )
        mark_seen(seen_conn, url, source["name"])
        return True

    # Fetch full body
    body = extract_body(url)
    if not body and summary:
        body = summary

    if not body:
        log.warning("No body for %s — skipping", url)
        mark_seen(seen_conn, url, source["name"])
        return False

    # Rewrite via GPT
    needs_rewrite = source.get("rewrite", True)
    rewritten = rewrite_article(
        original_title=title,
        original_body=body,
        source_name=source["name"],
        source_lang=source.get("language", "ru"),
        needs_rewrite=needs_rewrite,
    )

    if not rewritten:
        log.warning("Rewrite failed for: %s", title[:60])
        mark_seen(seen_conn, url, source["name"])
        return False

    # Publish to CMS
    article_id = publish_article(
        original_url=url,
        rewritten=rewritten,
        source_name=source["name"],
        source_category=source.get("category", "general"),
    )

    if article_id:
        # Notify via Telegram
        notify_sync(
            article_id=article_id,
            title=rewritten["title"],
            excerpt=rewritten["excerpt"],
            source_name=source["name"],
            category=rewritten.get("category", ""),
        )

    mark_seen(seen_conn, url, source["name"])
    return True


def check_sources(
    sources: list[dict],
    intervals: dict[str, int],
    seen_conn: sqlite3.Connection,
    dry_run: bool = False,
    last_check: dict[str, float] | None = None,
) -> dict[str, float]:
    """Check all sources respecting their priority intervals.

    Returns updated last_check dict.
    """
    if last_check is None:
        last_check = {}

    now = time.time()

    for source in sources:
        name = source["name"]
        priority = source.get("priority", "low")
        interval = intervals.get(priority, 900)

        # Skip if not enough time has passed
        if name in last_check and (now - last_check[name]) < interval:
            continue

        log.info("Checking: %s (%s priority)", name, priority)
        entries = fetch_rss(source["url"])
        new_count = 0

        for entry in entries:
            if process_entry(entry, source, seen_conn, dry_run=dry_run):
                new_count += 1

        if new_count:
            log.info("  → %d new article(s) from %s", new_count, name)
        else:
            log.debug("  → No new articles from %s", name)

        last_check[name] = now

    return last_check


# ── Entry point ───────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Total.kz news monitor")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check sources without calling GPT or writing to DB",
    )
    args = parser.parse_args()

    log.info("=== Total.kz News Monitor started ===")
    if args.dry_run:
        log.info("DRY-RUN mode — no GPT calls, no DB writes")

    # Init components
    sources, intervals = load_sources()
    log.info("Loaded %d sources", len(sources))

    seen_conn = init_seen_db()
    log.info("Seen-URL database: %s", SEEN_DB)

    if not args.dry_run:
        init_telegram()

    last_check: dict[str, float] = {}

    if args.dry_run:
        # Single pass in dry-run mode
        check_sources(sources, intervals, seen_conn, dry_run=True)
        log.info("=== Dry-run complete ===")
        return

    # Main loop
    log.info("Starting monitoring loop (Ctrl+C to stop)")
    while True:
        try:
            last_check = check_sources(sources, intervals, seen_conn, last_check=last_check)
        except Exception as e:
            log.error("Error in main loop: %s", e)

        time.sleep(30)  # Check every 30s, per-source interval handled inside


if __name__ == "__main__":
    main()
