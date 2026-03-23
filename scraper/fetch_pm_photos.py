#!/usr/bin/env python3
"""
Fetch photos from primeminister.kz — photo galleries + news pages.
Optimized: concurrent fetching, minimal delays, incremental saves.
"""

import httpx
import re
import json
import sys
import asyncio
from pathlib import Path

BASE = "https://primeminister.kz"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TotalKZ/1.0)"}
DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT = DATA_DIR / "pm_events.json"
CONCURRENCY = 10

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
    text = text.strip()
    m = re.search(r'(\d{1,2})\s+([А-Яа-яёЁ]+)\s+(\d{4})', text)
    if not m:
        return None
    day, month_str, year = m.groups()
    month_num = MONTHS_RU.get(month_str)
    if not month_num:
        return None
    return f"{year}-{month_num}-{int(day):02d}"


def extract_listing_items(html: str, section: str) -> list[dict]:
    items = []
    pattern = rf'<a\s+href="(/ru/{section}/[^"]+)"\s+class="article">'
    blocks = re.split(pattern, html)
    for i in range(1, len(blocks), 2):
        url_path = blocks[i]
        block = blocks[i + 1] if i + 1 < len(blocks) else ""
        thumb_m = re.search(r"background-image:\s*url\('([^']+)'\)", block)
        thumb = thumb_m.group(1) if thumb_m else None
        date_m = re.search(r'class="article__date">\s*(.*?)\s*</span>', block, re.DOTALL)
        date_str = parse_date(date_m.group(1)) if date_m else None
        title_m = re.search(r'class="article__heading">(.*?)</h3>', block, re.DOTALL)
        title = title_m.group(1).strip() if title_m else ""
        items.append({"url": url_path, "title": title, "date": date_str, "thumbnail": thumb})
    return items


def extract_article_photos(html: str) -> list[str]:
    imgs = re.findall(r'src="(/assets/media/[^"]+)"', html)
    photos = []
    seen = set()
    for img in imgs:
        if '_mediumThumb' in img or '_smallThumb' in img:
            continue
        if img not in seen:
            seen.add(img)
            photos.append(img)
    return photos


def save_data(events):
    DATA_DIR.mkdir(exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(events, f, ensure_ascii=False, indent=1)


async def fetch(client, url, retries=3):
    for attempt in range(retries):
        try:
            r = await client.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
            if r.status_code == 200:
                return r.text
        except Exception as e:
            if attempt == retries - 1:
                print(f"  FAIL: {url}: {e}", file=sys.stderr)
        await asyncio.sleep(0.5)
    return None


async def fetch_listings(client, section, max_pages):
    """Fetch all listing pages concurrently in batches."""
    all_items = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async def fetch_page(page):
        async with sem:
            url = f"{BASE}/ru/{section}?page={page}"
            html = await fetch(client, url)
            if html:
                return extract_listing_items(html, section)
            return []

    # Batch in groups of 50
    for batch_start in range(1, max_pages + 1, 50):
        batch_end = min(batch_start + 50, max_pages + 1)
        tasks = [fetch_page(p) for p in range(batch_start, batch_end)]
        results = await asyncio.gather(*tasks)
        for items in results:
            all_items.extend(items)
        print(f"  [{section}] pages {batch_start}-{batch_end-1}: {len(all_items)} items total")

    return all_items


async def fetch_gallery_photos(client, gallery_items, existing_urls):
    """Fetch photos from each gallery page concurrently."""
    events = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async def process_gallery(item):
        if item["url"] in existing_urls:
            return None
        async with sem:
            html = await fetch(client, BASE + item["url"])
            if not html:
                return None
            photos = extract_article_photos(html)
            return {
                "url": item["url"],
                "title": item["title"],
                "date": item["date"],
                "thumbnail": item["thumbnail"],
                "type": "gallery",
                "photos": [BASE + p for p in photos],
                "photo_count": len(photos),
            }

    tasks = [process_gallery(item) for item in gallery_items]
    results = await asyncio.gather(*tasks)
    for r in results:
        if r:
            events.append(r)
    return events


async def main():
    if OUTPUT.exists():
        with open(OUTPUT) as f:
            all_events = json.load(f)
        print(f"Resuming: {len(all_events)} events already collected")
    else:
        all_events = []

    existing_urls = {e["url"] for e in all_events}

    async with httpx.AsyncClient(http2=False) as client:
        # Phase 1: Photo gallery listings (12 pages)
        print("=" * 60)
        print("Phase 1: Photo galleries")
        print("=" * 60)
        gallery_items = await fetch_listings(client, "media/photos", 12)
        print(f"Gallery items: {len(gallery_items)}")

        # Phase 2: Fetch photos from each gallery
        print("\n" + "=" * 60)
        print("Phase 2: Gallery photos")
        print("=" * 60)
        gallery_events = await fetch_gallery_photos(client, gallery_items, existing_urls)
        all_events.extend(gallery_events)
        existing_urls.update(e["url"] for e in gallery_events)
        print(f"New gallery events: {len(gallery_events)}")
        save_data(all_events)

        # Phase 3: News listings — find total pages first
        print("\n" + "=" * 60)
        print("Phase 3: News listings")
        print("=" * 60)
        html_p1 = await fetch(client, f"{BASE}/ru/news?page=1")
        last_page = 1
        if html_p1:
            pages = re.findall(r'page=(\d+)', html_p1)
            if pages:
                last_page = max(int(p) for p in pages)
        print(f"Total news pages: {last_page}")

        news_items = await fetch_listings(client, "news", last_page)
        print(f"Total news items: {len(news_items)}")

        # Phase 4: Add news with cover images
        print("\n" + "=" * 60)
        print("Phase 4: News events")
        print("=" * 60)
        new_news = 0
        for item in news_items:
            if item["url"] in existing_urls:
                continue
            photos = []
            if item["thumbnail"]:
                full_img = item["thumbnail"].replace("_mediumThumb", "").replace("_smallThumb", "")
                photos.append(BASE + full_img)
            all_events.append({
                "url": item["url"],
                "title": item["title"],
                "date": item["date"],
                "thumbnail": item["thumbnail"],
                "type": "news",
                "photos": photos,
                "photo_count": len(photos),
            })
            existing_urls.add(item["url"])
            new_news += 1

        print(f"New news events: {new_news}")

    # Final save
    all_events.sort(key=lambda e: e.get("date") or "0000", reverse=True)
    save_data(all_events)

    galleries = [e for e in all_events if e["type"] == "gallery"]
    news = [e for e in all_events if e["type"] == "news"]
    total_photos = sum(e["photo_count"] for e in all_events)

    print(f"\n{'=' * 60}")
    print(f"DONE!")
    print(f"  Galleries: {len(galleries)} ({sum(e['photo_count'] for e in galleries)} photos)")
    print(f"  News: {len(news)} ({sum(e['photo_count'] for e in news)} photos)")
    print(f"  Total events: {len(all_events)}")
    print(f"  Total photos: {total_photos}")
    print(f"  Saved to: {OUTPUT}")


if __name__ == "__main__":
    asyncio.run(main())
