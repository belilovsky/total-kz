"""Publish rewritten articles to PostgreSQL (Total.kz CMS)."""

import json
import logging
import os
import re
from datetime import datetime, timezone

import psycopg2
import requests

log = logging.getLogger("news-monitor.publisher")


def extract_og_image(url: str) -> tuple:
    """Extract og:image and credit from source article."""
    from urllib.parse import urlparse
    UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": UA}, allow_redirects=True)
        html = resp.text[:80000]
        image = None
        for pat in [
            r"""property=["']og:image["'][^>]*?content=["']([^"']+)["']""",
            r"""content=["']([^"']+)["'][^>]*?property=["']og:image["']""",
            r"""name=["']twitter:image["'][^>]*?content=["']([^"']+)["']""",
        ]:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                image = m.group(1)
                if image.startswith("//"):
                    image = "https:" + image
                break
        if not image:
            return None, None
        credit = None
        cm = re.search(r"""property=["']og:site_name["'][^>]*?content=["']([^"']+)["']""", html)
        if cm:
            credit = cm.group(1).strip()
        if not credit:
            credit = urlparse(url).netloc.replace("www.", "")
        return image, "\u0424\u043e\u0442\u043e: " + credit
    except Exception:
        return None, None


def notify_telegram(title: str, excerpt: str, article_id: int) -> None:
    """Send Telegram notification about a new article for review."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        return
    admin_url = os.environ.get("SITE_DOMAIN", "https://total.qdev.run")
    article_url = f"{admin_url}/admin/article/{article_id}"
    text = (
        f"\U0001f4f0 *Новая статья на рецензии*\n\n"
        f"*{title}*\n"
        f"{excerpt[:200]}\n\n"
        f"[Открыть в админке]({article_url})"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        log.warning("Telegram notification failed: %s", e)


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

    # Extract og:image from source article
    main_image, image_credit = extract_og_image(original_url)

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
                 author, status, pub_date, imported_at, editor_note, main_image, image_credit)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s)
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
                main_image,
                image_credit,
            ),
        )
        article_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

        log.info("Published article id=%d: %s", article_id, rewritten["title"][:60])

        # Send Telegram notification (optional, only if configured)
        notify_telegram(rewritten["title"], rewritten.get("excerpt", ""), article_id)

        return article_id

    except Exception as e:
        log.error("Failed to publish article: %s", e)
        return None
