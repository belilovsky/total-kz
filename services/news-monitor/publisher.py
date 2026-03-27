"""Publish rewritten articles to PostgreSQL (Total.kz CMS)."""

import json
import logging
import os
import re
from datetime import datetime, timezone

import psycopg2

log = logging.getLogger("news-monitor.publisher")


def get_db_url() -> str:
    """Get PostgreSQL connection URL from environment."""
    url = os.environ.get("PG_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("PG_DATABASE_URL or DATABASE_URL not set")
    return url


def publish_article(
    original_url: str,
    rewritten: dict,
    source_name: str,
    source_category: str,
) -> int | None:
    """Insert article into articles table with status='review'.

    Returns article ID on success, None on failure.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Map GPT category to CMS sub_category
    category_map = {
        "политика": "politika",
        "экономика": "ekonomika",
        "общество": "obshchestvo",
        "мир": "mir",
        "спорт": "sport",
        "технологии": "tekhnologii",
        "культура": "kultura",
    }
    sub_cat = category_map.get(rewritten.get("category", "").lower(), "obshchestvo")

    tags_json = json.dumps(rewritten.get("tags", []), ensure_ascii=False)

    # Generate plain-text body from HTML for body_text field
    body_html = rewritten.get("body_html", "")
    body_text = re.sub(r"<[^>]+>", "", body_html).strip()

    try:
        conn = psycopg2.connect(get_db_url())
        cur = conn.cursor()

        # Check if article already exists by URL
        cur.execute("SELECT id FROM articles WHERE url = %s", (original_url,))
        existing = cur.fetchone()
        if existing:
            log.info("Article already exists (id=%d): %s", existing[0], original_url)
            conn.close()
            return None

        cur.execute(
            """
            INSERT INTO articles
                (url, title, excerpt, body_html, body_text, sub_category, tags,
                 author, status, pub_date, imported_at, editor_note)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                original_url,
                rewritten["title"],
                rewritten["excerpt"],
                body_html,
                body_text,
                sub_cat,
                tags_json,
                f"news-monitor ({source_name})",
                "review",
                now,
                now,
                f"Автоматически: {source_name} | Категория: {source_category}",
            ),
        )
        article_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

        log.info("Published article id=%d: %s", article_id, rewritten["title"][:60])
        return article_id

    except Exception as e:
        log.error("Failed to publish article: %s", e)
        return None
