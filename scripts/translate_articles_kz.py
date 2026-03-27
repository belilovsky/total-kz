"""Translate articles from Russian to Kazakh using GPT.

Usage:
    python scripts/translate_articles_kz.py --batch 100
    python scripts/translate_articles_kz.py --batch 50 --model gpt-4o
    python scripts/translate_articles_kz.py --batch 10 --dry-run

Cost: ~$0.0002/article (gpt-4o-mini), ~$0.0035/article (gpt-4o)
"""

import argparse
import json
import logging
import os
import re
import sys
import time

import httpx
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("translate_kz")

DATABASE_URL = os.getenv(
    "PG_DATABASE_URL",
    os.getenv("DATABASE_URL", "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz"),
)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

SYSTEM_PROMPT = (
    "Ты — профессиональный переводчик с русского на казахский язык "
    "для казахстанского новостного портала Total.kz.\n\n"
    "Требования к переводу:\n"
    "1. Используй литературный казахский язык (әдеби қазақ тілі), не разговорный\n"
    "2. Имена собственные людей НЕ переводи — оставь как есть "
    "(Касым-Жомарт Токаев, Аида Балаева)\n"
    "3. Казахстанские названия используй на казахском "
    "(ВКО→ШҚО, ЗКО→БҚО, СКО→СҚО, Восточный Казахстан→Шығыс Қазақстан, "
    "Министерство внутренних дел→Ішкі істер министрлігі)\n"
    "4. Сохрани ВСЮ HTML-разметку (<p>, <a href=\"...\">, <blockquote>, "
    "<strong>, <em>) — переводи ТОЛЬКО текст внутри тегов\n"
    "5. НЕ переводи: URL-адреса, числа, даты в формате цифр\n"
    "6. Перевод должен быть естественным и читаемым, не дословным\n"
    "7. Ответ — СТРОГО JSON без пояснений:\n"
    '{"title_kz": "...", "excerpt_kz": "...", "body_html_kz": "..."}'
)


def get_untranslated(conn, batch: int) -> list:
    """Fetch articles not yet translated to Kazakh — latest first."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT a.id, a.title, a.excerpt, a.body_html
        FROM articles a
        LEFT JOIN article_translations t
            ON t.article_id = a.id AND t.lang = 'kz'
        WHERE t.id IS NULL
          AND a.title IS NOT NULL AND a.title != ''
          AND a.body_html IS NOT NULL AND a.body_html != ''
          AND a.status = 'published'
        ORDER BY a.pub_date DESC NULLS LAST
        LIMIT %s
        """,
        (batch,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def translate_article(article: dict, model: str) -> dict | None:
    """Send article to GPT for translation via httpx. Returns parsed JSON or None."""
    title = article["title"] or ""
    excerpt = article["excerpt"] or ""
    body_html = (article["body_html"] or "")[:12000]  # Limit for token budget

    # Escape problematic characters in body_html for JSON safety
    body_html = body_html.replace('\x00', '')
    user_msg = f"Заголовок: {title}\nЛид: {excerpt}\nТекст: {body_html}"

    try:
        resp = httpx.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": 0.3,
                "max_tokens": 4096,
                "response_format": {"type": "json_object"},
            },
            timeout=90,
        )
        data = resp.json()

        if "error" in data:
            log.error("  API error: %s", data["error"].get("message", ""))
            return None

        content = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {})
        in_tok = usage.get("prompt_tokens", 0)
        out_tok = usage.get("completion_tokens", 0)

        # Cost estimate
        if "mini" in model:
            cost = in_tok * 0.15 / 1_000_000 + out_tok * 0.6 / 1_000_000
        else:
            cost = in_tok * 2.5 / 1_000_000 + out_tok * 10.0 / 1_000_000

        log.info("  tokens: %d+%d, cost: $%.4f", in_tok, out_tok, cost)

        parsed = json.loads(content)
        return parsed

    except json.JSONDecodeError:
        log.warning("  Invalid JSON from GPT for article %s", article["id"])
        # Try to extract JSON from response
        try:
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception:
            pass
        return None
    except Exception as e:
        log.error("  GPT error for article %s: %s", article["id"], e)
        return None


def save_translation(conn, article_id: int, data: dict):
    """Insert translation into article_translations table."""
    cur = conn.cursor()
    # Strip HTML for body_text
    body_html = data.get("body_html_kz", "")
    body_text = re.sub(r'<[^>]+>', ' ', body_html).strip()
    body_text = re.sub(r'\s+', ' ', body_text)

    cur.execute(
        """
        INSERT INTO article_translations
            (article_id, lang, title, excerpt, body_html, body_text, meta_description)
        VALUES (%s, 'kz', %s, %s, %s, %s, NULL)
        ON CONFLICT (article_id, lang) DO UPDATE SET
            title = EXCLUDED.title,
            excerpt = EXCLUDED.excerpt,
            body_html = EXCLUDED.body_html,
            body_text = EXCLUDED.body_text,
            translated_at = NOW()
        """,
        (
            article_id,
            data.get("title_kz", ""),
            data.get("excerpt_kz", ""),
            body_html,
            body_text,
        ),
    )
    conn.commit()
    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Translate articles to Kazakh")
    parser.add_argument("--batch", type=int, default=100, help="Number of articles")
    parser.add_argument("--model", type=str, default="gpt-4o", help="GPT model")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save, just preview")
    args = parser.parse_args()

    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set")
        sys.exit(1)

    log.info("Model: %s, batch: %d, delay: %.1fs", args.model, args.batch, args.delay)
    log.info("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)

    log.info("Fetching untranslated articles (latest first)...")
    articles = get_untranslated(conn, args.batch)
    log.info("Found %d articles to translate", len(articles))

    if not articles:
        log.info("Nothing to translate")
        conn.close()
        return

    translated = 0
    errors = 0
    total_cost = 0.0

    for i, art in enumerate(articles, 1):
        log.info(
            "[%d/%d] id=%d: %s",
            i, len(articles), art["id"], art["title"][:70],
        )

        data = translate_article(art, args.model)
        if data is None:
            errors += 1
            continue

        if args.dry_run:
            log.info("  → %s", data.get("title_kz", "")[:80])
            translated += 1
            continue

        try:
            save_translation(conn, art["id"], data)
            translated += 1
        except Exception as e:
            log.error("  DB error for %d: %s", art["id"], e)
            conn.rollback()
            errors += 1

        # Rate limiting — be gentle on VPS
        if i < len(articles):
            time.sleep(args.delay)

    conn.close()
    log.info(
        "Done: %d translated, %d errors out of %d",
        translated, errors, len(articles),
    )


if __name__ == "__main__":
    main()
