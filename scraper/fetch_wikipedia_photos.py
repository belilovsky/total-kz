#!/usr/bin/env python3
"""
Fetch person photos from Wikipedia API (MediaWiki) and save locally.
Falls back from Russian Wikipedia to English Wikipedia.
Updates the persons DB photo_url to the local path.
"""
import sqlite3, httpx, time, re, unicodedata
from pathlib import Path
from urllib.parse import quote

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"
PHOTOS_DIR = Path(__file__).resolve().parent.parent / "app" / "static" / "img" / "persons"
PHOTOS_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "TotalKZ-PhotoFetcher/1.0 (https://total.kz; admin@total.kz)",
}

# Manual overrides for persons where Wikipedia photo is missing/wrong.
# Map short_name -> direct image URL.
PHOTO_OVERRIDES: dict[str, str] = {
    # Example:
    # 'Олжас Бектенов': 'https://example.com/photo.jpg',
}

TRANSLIT = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh',
    'з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o',
    'п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts',
    'ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu',
    'я':'ya','ә':'a','і':'i','ң':'n','ғ':'g','ү':'u','ұ':'u','қ':'q',
    'ө':'o','һ':'h',
}


def make_slug(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'\s+', '-', s)
    s = re.sub(r'-+', '-', s).strip('-')
    return s


def fetch_wiki_thumb(name: str, client: httpx.Client, lang: str = "ru") -> str | None:
    """Query Wikipedia API for the main page image thumbnail."""
    api_url = f"https://{lang}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "titles": name,
        "prop": "pageimages",
        "format": "json",
        "pithumbsize": 400,
        "redirects": 1,
    }
    try:
        r = client.get(api_url, params=params, timeout=15)
        data = r.json()
        pages = data.get("query", {}).get("pages", {})
        for page_id, page_data in pages.items():
            if page_id == "-1":
                continue
            thumb = page_data.get("thumbnail", {}).get("source")
            if thumb:
                return thumb
    except Exception as e:
        print(f"    Wiki API error ({lang}): {e}")
    return None


def download_image(url: str, slug: str, client: httpx.Client) -> str | None:
    """Download image from URL and save locally. Returns local path or None."""
    try:
        r = client.get(url, timeout=20, follow_redirects=True)
        if r.status_code == 200 and len(r.content) > 500:
            content = r.content[:4]
            if content[:2] == b'\xff\xd8':
                ext = "jpg"
            elif content[:4] == b'\x89PNG':
                ext = "png"
            elif content[:4] == b'RIFF':
                ext = "webp"
            else:
                ext = "jpg"
            filepath = PHOTOS_DIR / f"{slug}.{ext}"
            filepath.write_bytes(r.content)
            return f"/static/img/persons/{slug}.{ext}"
    except Exception as e:
        print(f"    Download error: {e}")
    return None


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    persons = conn.execute("""
        SELECT id, slug, short_name, full_name, photo_url
        FROM persons
        ORDER BY id
    """).fetchall()

    print(f"Processing {len(persons)} persons for Wikipedia photos...\n")
    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30)

    updated = 0
    skipped = 0
    failed = 0

    for p in persons:
        name = p["short_name"] or p["full_name"]
        slug = p["slug"]

        # Skip if already has a local photo
        if p["photo_url"] and p["photo_url"].startswith("/static/"):
            local_file = Path(__file__).resolve().parent.parent / p["photo_url"].lstrip("/")
            if local_file.exists():
                skipped += 1
                continue

        print(f"  {name}...", end=" ")

        # Check manual overrides first
        if name in PHOTO_OVERRIDES:
            local_path = download_image(PHOTO_OVERRIDES[name], slug, client)
            if local_path:
                conn.execute(
                    "UPDATE persons SET photo_url = ?, updated_at = datetime('now') WHERE id = ?",
                    (local_path, p["id"]),
                )
                print(f"override -> {local_path}")
                updated += 1
                time.sleep(0.3)
                continue

        # Try Russian Wikipedia
        thumb_url = fetch_wiki_thumb(name, client, lang="ru")

        # Fallback to English Wikipedia
        if not thumb_url:
            thumb_url = fetch_wiki_thumb(name, client, lang="en")

        if thumb_url:
            local_path = download_image(thumb_url, slug, client)
            if local_path:
                conn.execute(
                    "UPDATE persons SET photo_url = ?, updated_at = datetime('now') WHERE id = ?",
                    (local_path, p["id"]),
                )
                print(f"ok -> {local_path}")
                updated += 1
            else:
                print("download failed")
                failed += 1
        else:
            print("no photo found")
            failed += 1

        time.sleep(0.5)  # Be nice to Wikipedia API

    conn.commit()
    client.close()

    print(f"\nDone: {updated} updated, {skipped} already had photos, {failed} failed")

    # Show persons still without photos
    missing = conn.execute("""
        SELECT short_name FROM persons
        WHERE photo_url IS NULL OR photo_url = ''
        ORDER BY short_name
    """).fetchall()
    if missing:
        print(f"\nStill missing photos ({len(missing)}):")
        for m in missing:
            print(f"  - {m['short_name']}")

    conn.close()


if __name__ == "__main__":
    main()
