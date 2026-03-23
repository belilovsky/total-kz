#!/usr/bin/env python3
"""
Download person photos from zakon.kz (prg.kz) and save locally.
Updates DB photo_url to local path.
"""
import sqlite3, httpx, time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"
PHOTOS_DIR = Path(__file__).resolve().parent.parent / "app" / "static" / "img" / "persons"
PHOTOS_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def download_photo(url: str, slug: str, client: httpx.Client) -> str | None:
    """Download photo and return local path."""
    try:
        r = client.get(url, timeout=15, follow_redirects=True)
        if r.status_code == 200 and len(r.content) > 1000:
            # Determine extension from content
            content = r.content[:4]
            if content[:2] == b'\xff\xd8':
                ext = "jpg"
            elif content[:4] == b'\x89PNG':
                ext = "png"
            elif content[:4] == b'RIFF':
                ext = "webp"
            else:
                ext = "jpg"  # default
            
            filepath = PHOTOS_DIR / f"{slug}.{ext}"
            filepath.write_bytes(r.content)
            return f"/static/img/persons/{slug}.{ext}"
    except Exception as e:
        print(f"    Error: {e}")
    return None


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    persons = conn.execute("""
        SELECT id, slug, short_name, photo_url
        FROM persons
        WHERE photo_url IS NOT NULL AND photo_url != ''
        AND photo_url LIKE 'http%'
        ORDER BY id
    """).fetchall()
    
    print(f"Downloading photos for {len(persons)} persons...")
    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30)
    
    success = 0
    for p in persons:
        local_path = download_photo(p["photo_url"], p["slug"], client)
        if local_path:
            conn.execute("UPDATE persons SET photo_url = ?, updated_at = datetime('now') WHERE id = ?",
                        (local_path, p["id"]))
            print(f"  [+] {p['short_name']}: {local_path}")
            success += 1
        else:
            print(f"  [-] {p['short_name']}: failed")
        time.sleep(0.3)
    
    conn.commit()
    client.close()
    
    print(f"\nDone: {success}/{len(persons)} photos downloaded")
    
    # Show who still needs photos
    missing = conn.execute("""
        SELECT short_name FROM persons 
        WHERE (photo_url IS NULL OR photo_url = '' OR photo_url LIKE 'http%')
        ORDER BY (SELECT COUNT(*) FROM article_entities ae WHERE ae.entity_id = persons.entity_id) DESC
    """).fetchall()
    if missing:
        print(f"\nStill need photos ({len(missing)}):")
        for m in missing[:15]:
            print(f"  - {m['short_name']}")
    
    conn.close()


if __name__ == "__main__":
    main()
