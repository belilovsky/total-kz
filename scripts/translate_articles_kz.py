"""Translate articles from Russian to Kazakh using GPT.

Usage:
    python scripts/translate_articles_kz.py --batch 100 --workers 1
    python scripts/translate_articles_kz.py --batch 50 --dry-run

Cost estimate: ~$0.003/article with gpt-4o-mini
"""

import argparse
import json
import logging
import os
import sys
import time

import psycopg2
import psycopg2.extras
from openai import OpenAI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("translate_kz")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
MODEL = os.getenv("TRANSLATE_MODEL", "gpt-4o-mini")

SYSTEM_PROMPT = (
    "Ты — профессиональный переводчик с русского на казахский язык. "
    "Переведи следующий новостной текст на литературный казахский язык.\n\n"
    "Требования:\n"
    "1. Используй литературный казахский язык (әдеби қазақ тілі), не разговорный\n"
    "2. Имена собственные людей НЕ переводи — оставь как есть "
    "(Касым-Жомарт Токаев, Аида Балаева)\n"
    "3. Казахстанские названия используй на казахском "
    "(ВКО→ШҚО, Восточный Казахстан→Шығыс Қазақстан)\n"
    "4. Сохрани всю HTML-разметку (<p>, <a href=\"...\">, <blockquote>, "
    "<strong>, <em>) — переводи только текст внутри тегов\n"
    "5. НЕ переводи: URL-адреса, числа, даты в формате цифр\n"
    "6. Перевод должен быть естественным и читаемым, не дословным\n"
    "7. Ответ — СТРОГО JSON без пояснений:\n"
    '{"title_kz": "...", "excerpt_kz": "...", "body_html_kz": "..."}'
)


def get_untranslated(conn, batch: int) -> list[dict]:
    """Fetch articles not yet translated to Kazakh."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT a.id, a.title, a.excerpt, a.body_html
        FROM articles a
        LEFT JOIN article_translations t
            ON t.article_id = a.id AND t.lang = 'kz'
        WHERE t.id IS NULL
          AND a.title IS NOT NULL
          AND a.title != ''
          AND a.body_html IS NOT NULL
          AND a.body_html != ''
        ORDER BY a.pub_date DESC NULLS LAST
        LIMIT %s
        """,
        (batch,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def translate_article(client: OpenAI, article: dict) -> dict | None:
    """Send article to GPT for translation. Returns parsed JSON or None."""
    title = article["title"] or ""
    excerpt = article["excerpt"] or ""
    body_html = (article["body_html"] or "")[:12000]  # Limit for token budget

    user_msg = f"Заголовок: {title}\nЛид: {excerpt}\nТекст: {body_html}"

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,
            max_tokens=4096,
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content
        data = json.loads(content)
        return data
    except json.JSONDecodeError:
        log.warning("Invalid JSON from GPT for article %s", article["id"])
        return None
    except Exception as e:
        log.error("GPT error for article %s: %s", article["id"], e)
        return None


def save_translation(conn, article_id: int, data: dict):
    """Insert translation into article_translations table."""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO article_translations
            (article_id, lang, title, excerpt, body_html, body_text, meta_description)
        VALUES (%s, 'kz', %s, %s, %s, NULL, NULL)
        ON CONFLICT (article_id, lang) DO UPDATE SET
            title = EXCLUDED.title,
            excerpt = EXCLUDED.excerpt,
            body_html = EXCLUDED.body_html,
            translated_at = NOW()
        """,
        (
            article_id,
            data.get("title_kz", ""),
            data.get("excerpt_kz", ""),
            data.get("body_html_kz", ""),
        ),
    )
    conn.commit()
    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Translate articles to Kazakh")
    parser.add_argument("--batch", type=int, default=100, help="Number of articles")
    parser.add_argument("--workers", type=int, default=1, help="Concurrent workers (unused, sequential)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save, just preview")
    args = parser.parse_args()

    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set")
        sys.exit(1)

    log.info("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)

    log.info("Fetching up to %d untranslated articles...", args.batch)
    articles = get_untranslated(conn, args.batch)
    log.info("Found %d articles to translate", len(articles))

    if not articles:
        log.info("Nothing to translate")
        conn.close()
        return

    client = OpenAI(api_key=OPENAI_API_KEY)
    translated = 0
    errors = 0

    for i, art in enumerate(articles, 1):
        log.info(
            "[%d/%d] Translating: %s (id=%d)",
            i, len(articles), art["title"][:60], art["id"],
        )

        data = translate_article(client, art)
        if data is None:
            errors += 1
            continue

        if args.dry_run:
            log.info("  KZ title: %s", data.get("title_kz", "")[:80])
            translated += 1
            continue

        try:
            save_translation(conn, art["id"], data)
            translated += 1
        except Exception as e:
            log.error("  DB save error for %d: %s", art["id"], e)
            conn.rollback()
            errors += 1

        # Rate limit: ~1 req/sec for gpt-4o-mini
        if i < len(articles):
            time.sleep(0.5)

    conn.close()
    log.info("Done: %d translated, %d errors out of %d", translated, errors, len(articles))


if __name__ == "__main__":
    main()
