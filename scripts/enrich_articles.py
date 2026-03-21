#!/usr/bin/env python3
"""
Article enrichment pipeline via GPT-4o-mini.
Generates: summary, meta_description, keywords, quote for each article.

Usage:
  # Test on 50 articles:
  python scripts/enrich_articles.py --limit 50

  # Full run (resumes from where it left off):
  python scripts/enrich_articles.py

  # Re-process specific articles:
  python scripts/enrich_articles.py --ids 1234,5678
"""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path

try:
    from openai import OpenAI
except ImportError:
    print("pip install openai")
    sys.exit(1)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"
LOG_PATH = Path(__file__).resolve().parent.parent / "data" / "enrich.log"

SYSTEM_PROMPT = """Ты — редактор казахстанского новостного портала. Для каждой статьи создай:

1. **summary** — краткое описание для карточки на главной (1-2 предложения, 60-120 символов). Не начинай с "В статье...", пиши как лид новости.

2. **meta_description** — SEO-описание для Google (120-155 символов). Включи ключевые слова, город/регион если есть. Не дублируй summary.

3. **keywords** — ровно 5 ключевых слов/фраз для тегов. Правила:
   - Именительный падеж, единственное число где уместно
   - Конкретные: "авария на шахте Костенко" а не "происшествия"
   - Без общих слов типа "Казахстан", "новости", "статистика"
   - Если есть имя персоны — включи (Имя Фамилия)
   - Если есть организация — включи
   - Без опечаток

4. **quote** — самая яркая прямая цитата из текста (если есть). Дословно из текста, в кавычках. Если прямых цитат нет — null.

5. **quote_author** — автор цитаты (если есть). Имя и должность. Если цитаты нет — null.

Ответь JSON:
{
  "summary": "...",
  "meta_description": "...",
  "keywords": ["тег1", "тег2", "тег3", "тег4", "тег5"],
  "quote": "..." или null,
  "quote_author": "..." или null
}"""


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def ensure_table(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS article_enrichments (
            article_id INTEGER PRIMARY KEY REFERENCES articles(id),
            summary TEXT,
            meta_description TEXT,
            keywords TEXT,  -- JSON array
            quote TEXT,
            quote_author TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_enrichments_article
            ON article_enrichments(article_id);
    """)
    conn.commit()


def get_unprocessed(conn, limit=None, ids=None):
    if ids:
        placeholders = ','.join('?' * len(ids))
        return conn.execute(f"""
            SELECT id, title, sub_category, body_text
            FROM articles WHERE id IN ({placeholders})
            ORDER BY pub_date DESC
        """, ids).fetchall()

    query = """
        SELECT a.id, a.title, a.sub_category, a.body_text
        FROM articles a
        LEFT JOIN article_enrichments ae ON ae.article_id = a.id
        WHERE ae.article_id IS NULL
        ORDER BY a.pub_date DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchall()


def enrich_article(client, title, category, body_text):
    # Send first 800 chars of body — enough for context, saves tokens
    body_preview = (body_text or "")[:800]
    user_msg = f"Категория: {category}\nЗаголовок: {title}\nТекст: {body_preview}"

    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg}
                ],
                response_format={"type": "json_object"},
                temperature=0.2,
                max_tokens=400,
            )
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            if "rate_limit" in str(e).lower() or "429" in str(e):
                wait = 30 * (attempt + 1)
                log(f"  Rate limit, waiting {wait}s...")
                time.sleep(wait)
            elif "insufficient_quota" in str(e).lower() or "billing" in str(e).lower():
                log(f"  BILLING ERROR: {e}")
                log("  Stopping — add credits at https://platform.openai.com/settings/organization/billing/overview")
                sys.exit(1)
            else:
                log(f"  API error (attempt {attempt+1}): {e}")
                if attempt == 2:
                    return None
                time.sleep(5)
    return None


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    parser.add_argument("--ids", type=str)
    args = parser.parse_args()

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        log("ERROR: OPENAI_API_KEY not set")
        sys.exit(1)

    client = OpenAI(api_key=api_key)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_table(conn)

    ids = [int(x) for x in args.ids.split(",")] if args.ids else None
    articles = get_unprocessed(conn, limit=args.limit, ids=ids)
    total = len(articles)
    log(f"START: {total} articles to enrich")

    if total == 0:
        log("Nothing to do.")
        return

    stats = {"ok": 0, "errors": 0, "quotes": 0}
    start_time = time.time()

    for i, art in enumerate(articles):
        result = enrich_article(client, art["title"], art["sub_category"], art["body_text"])

        if result:
            conn.execute("""
                INSERT OR REPLACE INTO article_enrichments
                (article_id, summary, meta_description, keywords, quote, quote_author)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                art["id"],
                result.get("summary", ""),
                result.get("meta_description", ""),
                json.dumps(result.get("keywords", []), ensure_ascii=False),
                result.get("quote"),
                result.get("quote_author"),
            ))
            conn.commit()
            stats["ok"] += 1
            if result.get("quote"):
                stats["quotes"] += 1
        else:
            stats["errors"] += 1

        if (i + 1) % 100 == 0 or i == total - 1:
            elapsed = time.time() - start_time
            rate = (stats["ok"] + stats["errors"]) / elapsed if elapsed > 0 else 0
            remaining = total - i - 1
            eta = remaining / rate / 60 if rate > 0 else 0
            log(f"PROGRESS: {i+1}/{total} ({rate:.1f} art/s, ETA {eta:.0f}m) | "
                f"ok: {stats['ok']} err: {stats['errors']} quotes: {stats['quotes']}")

    elapsed = time.time() - start_time
    log(f"DONE: {stats['ok']} enriched, {stats['errors']} errors, "
        f"{stats['quotes']} with quotes in {elapsed/60:.0f}m")
    conn.close()
    log("=== ENRICHMENT COMPLETE ===")


if __name__ == "__main__":
    main()
