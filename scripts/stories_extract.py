#!/usr/bin/env python3
"""
Story extraction pipeline for Total.kz articles.

Uses GPT-4o-mini to extract a short story slug from each article's
title + excerpt. Articles sharing the same slug form a "story timeline".

Usage:
  # Test on 100 articles:
  python scripts/stories_extract.py --limit 100

  # Full run:
  python scripts/stories_extract.py

  # Resume after interruption (skips already processed):
  python scripts/stories_extract.py --resume

  # Re-process specific article IDs:
  python scripts/stories_extract.py --ids 1234,5678
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

# ── OpenAI setup ──────────────────────────────────────────────────
try:
    from openai import OpenAI
except ImportError:
    print("pip install openai")
    sys.exit(1)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"

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

def ensure_tables(conn: sqlite3.Connection):
    """Create stories tables if they don't exist."""
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

        CREATE INDEX IF NOT EXISTS idx_article_stories_story
            ON article_stories(story_id);
        CREATE INDEX IF NOT EXISTS idx_article_stories_article
            ON article_stories(article_id);
        CREATE INDEX IF NOT EXISTS idx_stories_slug
            ON stories(slug);
    """)
    conn.commit()


def get_unprocessed_articles(conn, limit=None, ids=None, resume=False):
    """Get articles that haven't been processed yet."""
    if ids:
        placeholders = ','.join('?' * len(ids))
        return conn.execute(f"""
            SELECT id, title, excerpt, sub_category, pub_date
            FROM articles WHERE id IN ({placeholders})
            ORDER BY pub_date DESC
        """, ids).fetchall()

    if resume:
        query = """
            SELECT a.id, a.title, a.excerpt, a.sub_category, a.pub_date
            FROM articles a
            LEFT JOIN article_stories as2 ON as2.article_id = a.id
            WHERE as2.article_id IS NULL
            ORDER BY a.pub_date DESC
        """
    else:
        query = """
            SELECT id, title, excerpt, sub_category, pub_date
            FROM articles ORDER BY pub_date DESC
        """

    if limit:
        query += f" LIMIT {limit}"

    return conn.execute(query).fetchall()


def extract_story(client, title: str, excerpt: str, category: str) -> dict:
    """Call GPT-4o-mini to extract story from article."""
    user_msg = f"Категория: {category}\nЗаголовок: {title}"
    if excerpt:
        user_msg += f"\nЛид: {excerpt[:300]}"

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
        print(f"  API error: {e}")
        return {"story_slug": "unknown", "story_title_ru": "Неизвестно", "confidence": 0.0}


def upsert_story(conn, slug: str, title_ru: str) -> int:
    """Insert or get existing story, return story_id."""
    row = conn.execute("SELECT id FROM stories WHERE slug = ?", (slug,)).fetchone()
    if row:
        return row[0]
    conn.execute(
        "INSERT INTO stories (slug, title_ru) VALUES (?, ?)",
        (slug, title_ru)
    )
    conn.commit()
    return conn.execute("SELECT id FROM stories WHERE slug = ?", (slug,)).fetchone()[0]


def link_article_to_story(conn, article_id: int, story_id: int, confidence: float):
    """Link article to story."""
    conn.execute("""
        INSERT OR REPLACE INTO article_stories (article_id, story_id, confidence)
        VALUES (?, ?, ?)
    """, (article_id, story_id, confidence))
    conn.commit()


def update_story_stats(conn):
    """Update aggregate stats on stories table."""
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
    parser = argparse.ArgumentParser(description="Extract stories from articles via GPT")
    parser.add_argument("--limit", type=int, help="Process only N articles")
    parser.add_argument("--resume", action="store_true", help="Skip already processed articles")
    parser.add_argument("--ids", type=str, help="Comma-separated article IDs")
    parser.add_argument("--batch-size", type=int, default=50, help="Commit every N articles")
    parser.add_argument("--dry-run", action="store_true", help="Print prompts without calling API")
    args = parser.parse_args()

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key and not args.dry_run:
        print("Error: OPENAI_API_KEY not set")
        sys.exit(1)

    client = OpenAI(api_key=api_key) if not args.dry_run else None

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_tables(conn)

    ids = [int(x) for x in args.ids.split(",")] if args.ids else None
    articles = get_unprocessed_articles(conn, limit=args.limit, ids=ids, resume=args.resume)
    total = len(articles)
    print(f"Articles to process: {total}")

    if total == 0:
        print("Nothing to do.")
        return

    stats = {"processed": 0, "stories_created": 0, "low_confidence": 0, "errors": 0}
    start_time = time.time()

    for i, art in enumerate(articles):
        art_id = art["id"]
        title = art["title"]
        excerpt = art["excerpt"] or ""
        category = art["sub_category"] or ""
        pub_date = art["pub_date"] or ""

        if args.dry_run:
            print(f"[{i+1}/{total}] #{art_id}: {title[:60]}...")
            continue

        result = extract_story(client, title, excerpt, category)
        slug = result.get("story_slug", "unknown")
        title_ru = result.get("story_title_ru", "Неизвестно")
        confidence = result.get("confidence", 0.0)

        if confidence < 0.3:
            stats["low_confidence"] += 1
            slug = f"misc-{category}" if category else "misc"
            title_ru = f"Разное: {category}" if category else "Разное"

        story_id = upsert_story(conn, slug, title_ru)
        link_article_to_story(conn, art_id, story_id, confidence)
        stats["processed"] += 1

        # Progress
        if (i + 1) % 10 == 0 or i == total - 1:
            elapsed = time.time() - start_time
            rate = stats["processed"] / elapsed if elapsed > 0 else 0
            eta = (total - i - 1) / rate if rate > 0 else 0
            print(f"  [{i+1}/{total}] {rate:.1f} art/s, ETA {eta/60:.0f}m | "
                  f"stories: {conn.execute('SELECT COUNT(*) FROM stories').fetchone()[0]} | "
                  f"low: {stats['low_confidence']}")

    # Final stats update
    if not args.dry_run:
        print("\nUpdating story statistics...")
        update_story_stats(conn)

        story_count = conn.execute("SELECT COUNT(*) FROM stories").fetchone()[0]
        multi = conn.execute("SELECT COUNT(*) FROM stories WHERE article_count > 1").fetchone()[0]
        print(f"\nDone! {stats['processed']} articles → {story_count} stories ({multi} with 2+ articles)")
        print(f"Low confidence (misc): {stats['low_confidence']}")

        # Top stories
        print("\nTop 10 stories by article count:")
        for row in conn.execute("""
            SELECT slug, title_ru, article_count, first_date, last_date
            FROM stories ORDER BY article_count DESC LIMIT 10
        """).fetchall():
            print(f"  {row['article_count']:4d} | {row['title_ru'][:50]} | {row['first_date'][:10] if row['first_date'] else '?'}–{row['last_date'][:10] if row['last_date'] else '?'}")

    conn.close()


if __name__ == "__main__":
    main()
