#!/usr/bin/env python3
"""
Import akorda_events.json into the SQLite database.
Creates akorda_photos table and populates it.
Then matches Tokayev articles with closest Akorda events by date.
"""

import sqlite3
import json
from pathlib import Path

DB = Path(__file__).parent.parent / "data" / "total.db"
EVENTS_JSON = Path(__file__).parent.parent / "data" / "akorda_events.json"


def create_tables(conn: sqlite3.Connection):
    """Create akorda_events and akorda_photos tables."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS akorda_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            slug TEXT,
            title TEXT NOT NULL,
            event_date TEXT,
            cover_thumb TEXT,
            photo_count INTEGER DEFAULT 0,
            imported_at TEXT DEFAULT (datetime('now'))
        );
        
        CREATE TABLE IF NOT EXISTS akorda_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL REFERENCES akorda_events(id),
            photo_url TEXT NOT NULL,
            is_cover INTEGER DEFAULT 0,
            credit TEXT DEFAULT 'Фото: akorda.kz',
            local_path TEXT,
            width INTEGER,
            height INTEGER,
            used_in_article_id INTEGER,
            UNIQUE(event_id, photo_url)
        );
        
        CREATE INDEX IF NOT EXISTS idx_akorda_events_date ON akorda_events(event_date);
        CREATE INDEX IF NOT EXISTS idx_akorda_photos_event ON akorda_photos(event_id);
        CREATE INDEX IF NOT EXISTS idx_akorda_photos_used ON akorda_photos(used_in_article_id);
    """)
    print("Tables created/verified.")


def import_events(conn: sqlite3.Connection):
    """Import events from JSON into the database."""
    with open(EVENTS_JSON) as f:
        data = json.load(f)
    
    events = data['events']
    print(f"JSON contains {len(events)} events, {data['total_photos']} photos")
    
    new_events = 0
    new_photos = 0
    
    for ev in events:
        # Insert event
        cur = conn.execute("""
            INSERT OR IGNORE INTO akorda_events (url, slug, title, event_date, cover_thumb, photo_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ev['url'], ev['slug'], ev['title'], ev.get('date', ''), 
              ev.get('cover_thumb', ''), len(ev.get('photos', []))))
        
        if cur.rowcount > 0:
            new_events += 1
        
        # Get event_id
        row = conn.execute("SELECT id FROM akorda_events WHERE url = ?", (ev['url'],)).fetchone()
        event_id = row[0]
        
        # Insert photos
        for i, photo_url in enumerate(ev.get('photos', [])):
            is_cover = 1 if (photo_url == ev.get('cover_thumb', '')) else 0
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO akorda_photos (event_id, photo_url, is_cover)
                    VALUES (?, ?, ?)
                """, (event_id, photo_url, is_cover))
                new_photos += conn.total_changes  # approximate
            except sqlite3.IntegrityError:
                pass
    
    conn.commit()
    
    # Stats
    total_events = conn.execute("SELECT COUNT(*) FROM akorda_events").fetchone()[0]
    total_photos = conn.execute("SELECT COUNT(*) FROM akorda_photos").fetchone()[0]
    print(f"Imported: {new_events} new events")
    print(f"Database totals: {total_events} events, {total_photos} photos")
    
    # Date range
    row = conn.execute("SELECT MIN(event_date), MAX(event_date) FROM akorda_events WHERE event_date != ''").fetchone()
    print(f"Date range: {row[0]} — {row[1]}")


def analyze_matching(conn: sqlite3.Connection):
    """Analyze how Akorda events match with our Tokayev articles."""
    
    # Our Tokayev articles
    tokayev_articles = conn.execute("""
        SELECT id, title, main_image, image_credit, pub_date, thumbnail
        FROM articles 
        WHERE title LIKE '%Токаев%' OR title LIKE '%токаев%' 
            OR title LIKE '%Тоқаев%' OR title LIKE '%глав_ государств%'
            OR title LIKE '%президент%Казахстан%'
        ORDER BY pub_date DESC
    """).fetchall()
    print(f"\nArticles about Tokayev/President: {len(tokayev_articles)}")
    
    # Articles without image_credit
    no_credit = [a for a in tokayev_articles if not a[3]]
    print(f"Without image_credit: {len(no_credit)}")
    
    # Find duplicate images
    img_counts = {}
    for a in tokayev_articles:
        img = a[2]
        if img:
            img_counts[img] = img_counts.get(img, 0) + 1
    dupes = {k: v for k, v in img_counts.items() if v > 1}
    print(f"Duplicate images: {len(dupes)} unique images used in {sum(dupes.values())} articles")
    
    # Match by date
    matched = 0
    for a in tokayev_articles[:20]:  # Check first 20
        article_date = a[4][:10] if a[4] else ''
        if not article_date:
            continue
        
        # Find Akorda event on same date
        events = conn.execute("""
            SELECT id, title, event_date, photo_count FROM akorda_events 
            WHERE event_date = ? AND photo_count > 0
            ORDER BY photo_count DESC
        """, (article_date,)).fetchall()
        
        if events:
            matched += 1
            if matched <= 5:
                print(f"\n  MATCH: [{a[0]}] {a[1][:50]}")
                print(f"    Date: {article_date}")
                print(f"    Akorda events on this date: {len(events)}")
                for ev in events[:3]:
                    print(f"      → {ev[1][:50]} ({ev[3]} photos)")
    
    print(f"\n  Matched {matched}/20 recent articles to Akorda events by date")


def main():
    conn = sqlite3.connect(str(DB))
    try:
        create_tables(conn)
        import_events(conn)
        analyze_matching(conn)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
