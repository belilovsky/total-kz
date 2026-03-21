#!/usr/bin/env python3
"""
Background runner for story extraction. Writes progress to a log file.
Runs with --resume to skip already-processed articles.
Updates story_stats every 500 articles.
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
LOG_PATH = Path(__file__).resolve().parent.parent / "data" / "stories_extract.log"

SYSTEM_PROMPT = """Ты — аналитик казахстанских новостей. Для каждой статьи определи ОДИН конкретный новостной сюжет (story).

Сюжет — это конкретное событие или серия связанных событий, а НЕ общая тема.

Примеры:
- ✅ "Дело о коррупции в Минобороны 2024" — конкретное дело
- ✅ "Авария на шахте Костенко ноябрь 2023" — конкретное событие
- ✅ "Саммит ШОС в Астане 2024" — конкретное мероприятие
- ❌ "Коррупция в Казахстане" — слишком общее
- ❌ "Спорт" — это тема, не сюжет

Ответь JSON:
{
  "story_slug": "краткий-идентификатор-через-дефис-латиницей (3-6 слов, транслит)",
  "story_title_ru": "Название сюжета по-русски (5-10 слов)",
  "confidence": 0.0-1.0
}

Если статья — общая новость без конкретного сюжета (курс валют, прогноз погоды), верни confidence < 0.3.
Используй транслит для slug: ш→sh, ч→ch, щ→shch, ж→zh, ц→ts, и т.д."""


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS stories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            title_ru TEXT NOT NULL,
            article_count INTEGER DEFAULT 0,
            first_date TEXT,
            last_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS article_stories (
            article_id INTEGER NOT NULL REFERENCES articles(id),
            story_id INTEGER NOT NULL REFERENCES stories(id),
            confidence REAL DEFAULT 0.5,
            PRIMARY KEY (article_id, story_id)
        );
        CREATE INDEX IF NOT EXISTS idx_article_stories_story ON article_stories(story_id);
        CREATE INDEX IF NOT EXISTS idx_article_stories_article ON article_stories(article_id);
        CREATE INDEX IF NOT EXISTS idx_stories_slug ON stories(slug);
    """)
    conn.commit()


def get_unprocessed(conn):
    return conn.execute("""
        SELECT a.id, a.title, a.excerpt, a.sub_category, a.pub_date
        FROM articles a
        LEFT JOIN article_stories as2 ON as2.article_id = a.id
        WHERE as2.article_id IS NULL
        ORDER BY a.pub_date DESC
    """).fetchall()


def extract_story(client, title, excerpt, category):
    user_msg = f"Категория: {category}\nЗаголовок: {title}"
    if excerpt:
        user_msg += f"\nЛид: {excerpt[:300]}"
    
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg}
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=200,
            )
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            if "rate_limit" in str(e).lower() or "429" in str(e):
                wait = 30 * (attempt + 1)
                log(f"  Rate limit hit, waiting {wait}s...")
                time.sleep(wait)
            else:
                log(f"  API error (attempt {attempt+1}): {e}")
                if attempt == 2:
                    return {"story_slug": "unknown", "story_title_ru": "Неизвестно", "confidence": 0.0}
                time.sleep(5)
    return {"story_slug": "unknown", "story_title_ru": "Неизвестно", "confidence": 0.0}


def upsert_story(conn, slug, title_ru):
    row = conn.execute("SELECT id FROM stories WHERE slug = ?", (slug,)).fetchone()
    if row:
        return row[0]
    conn.execute("INSERT INTO stories (slug, title_ru) VALUES (?, ?)", (slug, title_ru))
    conn.commit()
    return conn.execute("SELECT id FROM stories WHERE slug = ?", (slug,)).fetchone()[0]


def update_story_stats(conn):
    conn.executescript("""
        UPDATE stories SET
            article_count = (
                SELECT COUNT(*) FROM article_stories WHERE story_id = stories.id
            ),
            first_date = (
                SELECT MIN(a.pub_date) FROM articles a
                JOIN article_stories as2 ON as2.article_id = a.id
                WHERE as2.story_id = stories.id
            ),
            last_date = (
                SELECT MAX(a.pub_date) FROM articles a
                JOIN article_stories as2 ON as2.article_id = a.id
                WHERE as2.story_id = stories.id
            );
    """)
    conn.commit()


def main():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        log("ERROR: OPENAI_API_KEY not set")
        sys.exit(1)

    client = OpenAI(api_key=api_key)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_tables(conn)

    articles = get_unprocessed(conn)
    total = len(articles)
    log(f"START: {total} articles to process")

    if total == 0:
        log("Nothing to do.")
        return

    stats = {"processed": 0, "low_conf": 0, "errors": 0}
    start_time = time.time()

    for i, art in enumerate(articles):
        art_id = art["id"]
        title = art["title"]
        excerpt = art["excerpt"] or ""
        category = art["sub_category"] or ""

        result = extract_story(client, title, excerpt, category)
        slug = result.get("story_slug", "unknown")
        title_ru = result.get("story_title_ru", "Неизвестно")
        confidence = result.get("confidence", 0.0)

        if confidence < 0.3:
            stats["low_conf"] += 1
            slug = f"misc-{category}" if category else "misc"
            title_ru = f"Разное: {category}" if category else "Разное"

        story_id = upsert_story(conn, slug, title_ru)
        conn.execute(
            "INSERT OR REPLACE INTO article_stories (article_id, story_id, confidence) VALUES (?, ?, ?)",
            (art_id, story_id, confidence)
        )
        conn.commit()
        stats["processed"] += 1

        # Progress every 100 articles
        if (i + 1) % 100 == 0 or i == total - 1:
            elapsed = time.time() - start_time
            rate = stats["processed"] / elapsed if elapsed > 0 else 0
            remaining = total - i - 1
            eta = remaining / rate / 60 if rate > 0 else 0
            story_count = conn.execute("SELECT COUNT(*) FROM stories").fetchone()[0]
            log(f"PROGRESS: {i+1}/{total} ({rate:.1f} art/s, ETA {eta:.0f}m) | stories: {story_count} | low: {stats['low_conf']}")

        # Update stats every 500 articles
        if (i + 1) % 500 == 0:
            log("Updating story stats...")
            update_story_stats(conn)

    # Final
    log("Final stats update...")
    update_story_stats(conn)

    story_count = conn.execute("SELECT COUNT(*) FROM stories").fetchone()[0]
    multi = conn.execute("SELECT COUNT(*) FROM stories WHERE article_count > 1").fetchone()[0]
    elapsed = time.time() - start_time
    log(f"DONE: {stats['processed']} articles → {story_count} stories ({multi} with 2+ articles) in {elapsed/60:.0f}m")
    log(f"Low confidence: {stats['low_conf']}")

    # Top stories
    log("Top 20 stories:")
    for row in conn.execute("""
        SELECT slug, title_ru, article_count, first_date, last_date
        FROM stories ORDER BY article_count DESC LIMIT 20
    """).fetchall():
        first = row['first_date'][:10] if row['first_date'] else '?'
        last = row['last_date'][:10] if row['last_date'] else '?'
        log(f"  {row['article_count']:4d} | {row['title_ru'][:60]} | {first}–{last}")

    conn.close()
    log("=== EXTRACTION COMPLETE ===")


if __name__ == "__main__":
    main()
