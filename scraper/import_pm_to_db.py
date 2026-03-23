#!/usr/bin/env python3
"""
Import primeminister.kz events and photos into SQLite database.
Creates tables: pm_events, pm_photos.
Then matches PM-related articles by date and assigns photos.
"""

import json
import sqlite3
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DATA_DIR / "total.db"
PM_JSON = DATA_DIR / "pm_events.json"


def create_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pm_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            title TEXT,
            event_date TEXT,
            thumbnail TEXT,
            event_type TEXT DEFAULT 'news',
            photo_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pm_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER REFERENCES pm_events(id),
            photo_url TEXT NOT NULL,
            is_cover INTEGER DEFAULT 0,
            credit TEXT DEFAULT 'primeminister.kz',
            used_in_article_id INTEGER DEFAULT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pm_events_date ON pm_events(event_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pm_photos_event ON pm_photos(event_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pm_photos_used ON pm_photos(used_in_article_id)")


def import_data(conn):
    with open(PM_JSON) as f:
        events = json.load(f)

    print(f"Loaded {len(events)} events from JSON")

    inserted_events = 0
    inserted_photos = 0
    skipped = 0

    for event in events:
        # Skip if already exists
        exists = conn.execute(
            "SELECT id FROM pm_events WHERE url = ?", (event["url"],)
        ).fetchone()
        if exists:
            skipped += 1
            continue

        cur = conn.execute("""
            INSERT INTO pm_events (url, title, event_date, thumbnail, event_type, photo_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            event["url"],
            event["title"],
            event.get("date"),
            event.get("thumbnail"),
            event.get("type", "news"),
            event.get("photo_count", 0),
        ))
        event_id = cur.lastrowid
        inserted_events += 1

        # Insert photos
        photos = event.get("photos", [])
        for i, photo_url in enumerate(photos):
            conn.execute("""
                INSERT INTO pm_photos (event_id, photo_url, is_cover, credit)
                VALUES (?, ?, ?, 'primeminister.kz')
            """, (event_id, photo_url, 1 if i == 0 else 0))
            inserted_photos += 1

    conn.commit()
    print(f"Inserted: {inserted_events} events, {inserted_photos} photos")
    print(f"Skipped (already in DB): {skipped}")


def match_articles(conn):
    """Match PM-related articles with primeminister.kz photos by date."""

    # Find articles mentioning PM/government
    pm_articles = conn.execute("""
        SELECT id, title, pub_date, main_image, image_credit
        FROM articles
        WHERE (
            title LIKE '%ремьер%'
            OR title LIKE '%равительств%'
            OR title LIKE '%Бектенов%'
            OR title LIKE '%Смаилов%'
            OR title LIKE '%Мамин%'
            OR body_text LIKE '%Бектенов%'
        )
        AND pub_date IS NOT NULL
        AND pub_date != ''
    """).fetchall()

    print(f"\nPM-related articles: {len(pm_articles)}")

    matched = 0
    credited = 0

    for art_id, title, pub_date, main_image, image_credit in pm_articles:
        # Extract date (YYYY-MM-DD) from pub_date
        date_str = pub_date[:10] if pub_date else None
        if not date_str:
            continue

        # Find PM photo for this date (prefer gallery, then news)
        photo = conn.execute("""
            SELECT p.id, p.photo_url, e.event_type
            FROM pm_photos p
            JOIN pm_events e ON p.event_id = e.id
            WHERE e.event_date = ?
            AND p.used_in_article_id IS NULL
            ORDER BY
                CASE e.event_type WHEN 'gallery' THEN 0 ELSE 1 END,
                p.is_cover DESC
            LIMIT 1
        """, (date_str,)).fetchone()

        if photo:
            photo_id, photo_url, event_type = photo
            # Mark photo as used
            conn.execute(
                "UPDATE pm_photos SET used_in_article_id = ? WHERE id = ?",
                (art_id, photo_id)
            )
            matched += 1

        # Add credit if missing
        if not image_credit or image_credit.strip() == '':
            if main_image and ('primeminister' in (main_image or '') or 'pm.kz' in (main_image or '')):
                conn.execute(
                    "UPDATE articles SET image_credit = 'Фото: primeminister.kz' WHERE id = ?",
                    (art_id,)
                )
                credited += 1

    conn.commit()
    print(f"Matched photos by date: {matched}")
    print(f"Credits added: {credited}")


def print_stats(conn):
    total_events = conn.execute("SELECT COUNT(*) FROM pm_events").fetchone()[0]
    total_photos = conn.execute("SELECT COUNT(*) FROM pm_photos").fetchone()[0]
    galleries = conn.execute("SELECT COUNT(*) FROM pm_events WHERE event_type='gallery'").fetchone()[0]
    news = conn.execute("SELECT COUNT(*) FROM pm_events WHERE event_type='news'").fetchone()[0]
    used = conn.execute("SELECT COUNT(*) FROM pm_photos WHERE used_in_article_id IS NOT NULL").fetchone()[0]

    print(f"\n{'=' * 50}")
    print(f"PM Events: {total_events:,}")
    print(f"  Galleries: {galleries:,}")
    print(f"  News: {news:,}")
    print(f"PM Photos: {total_photos:,}")
    print(f"  Used in articles: {used}")


def main():
    conn = sqlite3.connect(str(DB_PATH))

    print("Creating tables...")
    create_tables(conn)

    print("\nImporting data...")
    import_data(conn)

    print("\nMatching with articles...")
    match_articles(conn)

    print_stats(conn)

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
