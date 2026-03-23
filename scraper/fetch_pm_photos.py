#!/usr/bin/env python3
"""
Fetch photos from primeminister.kz — photo galleries + news pages.

Strategy:
1. Collect all photo gallery listings (12 pages × 8 items = ~96 galleries)
2. Collect news listings (1246 pages × 10 items = ~12460 news) — only metadata from listings
3. Visit each gallery page to extract all inline photos
4. Visit news pages that match our articles by date to extract hero images

Output: data/pm_events.json
"""

import httpx
import re
import json
import time
import sys
from pathlib import Path

BASE = "https://primeminister.kz"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TotalKZ/1.0)"}

MONTHS_RU = {
    "Январь": "01", "Февраль": "02", "Март": "03", "Апрель": "04",
    "Май": "05", "Июнь": "06", "Июль": "07", "Август": "08",
    "Сентябрь": "09", "Октябрь": "10", "Ноябрь": "11", "Декабрь": "12",
    "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
    "мая": "05", "июня": "06", "июля": "07", "августа": "08",
    "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",
    "Янв": "01", "Фев": "02", "Мар": "03", "Апр": "04",
    "Мая": "05", "Июн": "06", "Июл": "07", "Авг": "08",
    "Сен": "09", "Окт": "10", "Ноя": "11", "Дек": "12",
}


def parse_date(text: str) -> str | None:
    """Parse date like '20 Март 2026, 14:05' → '2026-03-20'"""
    text = text.strip()
    # Pattern: DD Month YYYY
    m = re.search(r'(\d{1,2})\s+([А-Яа-яёЁ]+)\s+(\d{4})', text)
    if not m:
        return None
    day, month_str, year = m.groups()
    month_num = MONTHS_RU.get(month_str)
    if not month_num:
        return None
    return f"{year}-{month_num}-{int(day):02d}"


def fetch_page(client: httpx.Client, url: str, retries: int = 3) -> str | None:
    """Fetch a page with retries."""
    for attempt in range(retries):
        try:
            r = client.get(url, headers=HEADERS, timeout=30, follow_redirects=True)
            if r.status_code == 200:
                return r.text
            print(f"  HTTP {r.status_code} for {url}", file=sys.stderr)
        except Exception as e:
            print(f"  Error fetching {url}: {e}", file=sys.stderr)
        if attempt < retries - 1:
            time.sleep(2)
    return None


def extract_listing_items(html: str, section: str = "news") -> list[dict]:
    """Extract items from a listing page (news or photos)."""
    items = []
    # Pattern: <a href="/ru/{section}/{slug}" class="article">
    # followed by background-image and date/title
    pattern = rf'<a\s+href="(/ru/{section}/[^"]+)"\s+class="article">'
    blocks = re.split(pattern, html)

    for i in range(1, len(blocks), 2):
        url_path = blocks[i]
        block = blocks[i + 1] if i + 1 < len(blocks) else ""

        # Extract thumbnail
        thumb_m = re.search(r"background-image:\s*url\('([^']+)'\)", block)
        thumb = thumb_m.group(1) if thumb_m else None

        # Extract date
        date_m = re.search(r'class="article__date">\s*(.*?)\s*</span>', block, re.DOTALL)
        date_str = parse_date(date_m.group(1)) if date_m else None

        # Extract title
        title_m = re.search(r'class="article__heading">(.*?)</h3>', block, re.DOTALL)
        title = title_m.group(1).strip() if title_m else ""

        items.append({
            "url": url_path,
            "title": title,
            "date": date_str,
            "thumbnail": thumb,
        })

    return items


def extract_article_photos(html: str) -> list[str]:
    """Extract all full-resolution photo URLs from an article page."""
    # Inline images
    imgs = re.findall(r'src="(/assets/media/[^"]+)"', html)
    # Filter out thumbs and icons
    photos = []
    seen = set()
    for img in imgs:
        if '_mediumThumb' in img or '_smallThumb' in img:
            continue
        if img not in seen:
            seen.add(img)
            photos.append(img)
    return photos


def main():
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    output_file = data_dir / "pm_events.json"

    # Resume from existing data if available
    if output_file.exists():
        with open(output_file) as f:
            all_events = json.load(f)
        print(f"Resuming: {len(all_events)} events already collected")
    else:
        all_events = []

    existing_urls = {e["url"] for e in all_events}

    client = httpx.Client(http2=False)

    # ========== Phase 1: Collect photo gallery listings ==========
    print("=" * 60)
    print("Phase 1: Photo gallery listings (12 pages)")
    print("=" * 60)

    gallery_items = []
    for page in range(1, 13):
        url = f"{BASE}/ru/media/photos?page={page}"
        print(f"  Fetching gallery page {page}/12...")
        html = fetch_page(client, url)
        if not html:
            print(f"  FAILED page {page}")
            continue
        items = extract_listing_items(html, section="media/photos")
        print(f"    Found {len(items)} gallery items")
        gallery_items.extend(items)
        time.sleep(0.5)

    print(f"\nTotal gallery items: {len(gallery_items)}")

    # ========== Phase 2: Visit each gallery page for photos ==========
    print("\n" + "=" * 60)
    print("Phase 2: Fetching photos from each gallery")
    print("=" * 60)

    new_gallery_events = 0
    for i, item in enumerate(gallery_items, 1):
        if item["url"] in existing_urls:
            print(f"  [{i}/{len(gallery_items)}] Skip (already have): {item['title'][:60]}")
            continue

        full_url = BASE + item["url"]
        print(f"  [{i}/{len(gallery_items)}] {item['title'][:70]}...")
        html = fetch_page(client, full_url)
        if not html:
            continue

        photos = extract_article_photos(html)
        event = {
            "url": item["url"],
            "title": item["title"],
            "date": item["date"],
            "thumbnail": item["thumbnail"],
            "type": "gallery",
            "photos": [BASE + p for p in photos],
            "photo_count": len(photos),
        }
        all_events.append(event)
        existing_urls.add(item["url"])
        new_gallery_events += 1
        print(f"    → {len(photos)} photos")
        time.sleep(0.3)

    print(f"\nNew gallery events: {new_gallery_events}")

    # ========== Phase 3: Collect news listings ==========
    print("\n" + "=" * 60)
    print("Phase 3: News listings (all pages)")
    print("=" * 60)

    # First, find last page number
    html_p1 = fetch_page(client, f"{BASE}/ru/news?page=1")
    last_page = 1
    if html_p1:
        pages = re.findall(r'page=(\d+)', html_p1)
        if pages:
            last_page = max(int(p) for p in pages)
    print(f"  Total news pages: {last_page}")

    news_items = []
    for page in range(1, last_page + 1):
        url = f"{BASE}/ru/news?page={page}"
        if page % 50 == 0 or page <= 3:
            print(f"  Fetching news page {page}/{last_page}...")
        html = fetch_page(client, url)
        if not html:
            print(f"  FAILED page {page}")
            continue
        items = extract_listing_items(html, section="news")
        news_items.extend(items)
        time.sleep(0.2)

        # Save checkpoint every 100 pages
        if page % 100 == 0:
            print(f"  ... {len(news_items)} news items so far")

    print(f"\nTotal news items collected: {len(news_items)}")

    # ========== Phase 4: Add news items (with hero image from listing) ==========
    print("\n" + "=" * 60)
    print("Phase 4: Adding news events with cover images")
    print("=" * 60)

    new_news = 0
    for item in news_items:
        if item["url"] in existing_urls:
            continue

        # For news, use the thumbnail from listing as the main photo
        # Convert mediumThumb → full resolution
        photos = []
        if item["thumbnail"]:
            full_img = item["thumbnail"].replace("_mediumThumb", "").replace("_smallThumb", "")
            photos.append(BASE + full_img)

        event = {
            "url": item["url"],
            "title": item["title"],
            "date": item["date"],
            "thumbnail": item["thumbnail"],
            "type": "news",
            "photos": photos,
            "photo_count": len(photos),
        }
        all_events.append(event)
        existing_urls.add(item["url"])
        new_news += 1

    print(f"New news events: {new_news}")

    # ========== Save ==========
    all_events.sort(key=lambda e: e.get("date") or "0000", reverse=True)

    with open(output_file, "w") as f:
        json.dump(all_events, f, ensure_ascii=False, indent=2)

    # Stats
    galleries = [e for e in all_events if e["type"] == "gallery"]
    news = [e for e in all_events if e["type"] == "news"]
    total_photos = sum(e["photo_count"] for e in all_events)

    print(f"\n{'=' * 60}")
    print(f"DONE!")
    print(f"  Galleries: {len(galleries)} ({sum(e['photo_count'] for e in galleries)} photos)")
    print(f"  News: {len(news)} ({sum(e['photo_count'] for e in news)} photos)")
    print(f"  Total events: {len(all_events)}")
    print(f"  Total photos: {total_photos}")
    print(f"  Saved to: {output_file}")

    client.close()


if __name__ == "__main__":
    main()
