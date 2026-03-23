#!/usr/bin/env python3
"""
Fetch Tokayev photos from akorda.kz/ru/events.
Step 1: Collect events list (title, date, url, cover thumbnail)
Step 2: For each event, collect all gallery image URLs
Saves results to data/akorda_events.json
"""

import httpx
import json
import time
import re
import sys
from selectolax.parser import HTMLParser
from pathlib import Path

BASE = "https://www.akorda.kz"
EVENTS_URL = f"{BASE}/ru/events"
OUTPUT = Path(__file__).parent.parent / "data" / "akorda_events.json"

MONTH_MAP = {
    'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
    'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
    'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.5',
}


def parse_date(text: str) -> str:
    """Parse '21 марта 2026 года' -> '2026-03-21'"""
    text = text.strip()
    m = re.search(r'(\d{1,2})\s+(' + '|'.join(MONTH_MAP.keys()) + r')\s+(\d{4})', text)
    if not m:
        return ""
    day, month_word, year = m.groups()
    mm = MONTH_MAP.get(month_word, '00')
    return f"{year}-{mm}-{int(day):02d}"


def fetch_events_page(client: httpx.Client, page: int) -> list[dict]:
    """Fetch one page of events listing using div.card structure."""
    url = f"{EVENTS_URL}?page={page}"
    resp = client.get(url, timeout=30, follow_redirects=True)
    resp.raise_for_status()
    tree = HTMLParser(resp.text)
    
    events = []
    cards = tree.css('div.card')
    
    for card in cards:
        # Find the event link (the one with text, inside h3)
        h3 = card.css_first('h3 a[href]')
        if not h3:
            continue
        href = h3.attributes.get('href', '')
        if not re.match(r'/ru/[a-z0-9-]+-\d{4,}$', href):
            continue
        if 'deklaracii' in href:
            continue
        
        title = h3.text(strip=True)
        
        # Find date from h5
        h5 = card.css_first('h5')
        date = parse_date(h5.text(strip=True)) if h5 else ''
        
        # Find cover thumbnail
        img = card.css_first('img.image_main_block, img.card-img-top, img')
        cover = ''
        if img:
            src = img.attributes.get('src', '')
            if src:
                cover = src if src.startswith('http') else BASE + src
        
        events.append({
            'url': BASE + href,
            'slug': href,
            'title': title,
            'date': date,
            'cover_thumb': cover,
            'photos': []
        })
    
    return events


def fetch_event_photos(client: httpx.Client, event_url: str) -> tuple[str, list[str]]:
    """Fetch an event page and extract all photo URLs. Returns (cover_img, [gallery_imgs])."""
    try:
        resp = client.get(event_url, timeout=30, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        print(f"  Error fetching {event_url}: {e}")
        return '', []
    
    tree = HTMLParser(resp.text)
    photos = []
    cover = ''
    
    # Find all images — both direct src and data-src (lazy loading)
    for img in tree.css('img'):
        for attr in ('src', 'data-src', 'data-original'):
            src = img.attributes.get(attr, '')
            if not src:
                continue
            if 'uploadMedia' in src or '/assets/media/' in src:
                full_url = src if src.startswith('http') else BASE + src
                # Skip tiny thumbnails/icons
                if '_smallThumb' in full_url or '_miniThumb' in full_url:
                    continue
                if full_url not in photos:
                    photos.append(full_url)
                    if not cover:
                        cover = full_url
    
    # Check for <a> links to full-size images (lightbox pattern)
    for a in tree.css('a[href]'):
        href = a.attributes.get('href', '')
        if ('uploadMedia' in href or '/assets/media/' in href) and href.endswith(('.jpg', '.jpeg', '.png', '.JPG')):
            full_url = href if href.startswith('http') else BASE + href
            if full_url not in photos:
                photos.append(full_url)
    
    # Check for background-image URLs in style attributes
    for node in tree.css('[style]'):
        style = node.attributes.get('style', '')
        if 'background' not in style:
            continue
        urls = re.findall(r'url\(["\']?([^"\')\s]+)["\']?\)', style)
        for u in urls:
            if 'uploadMedia' in u or '/assets/media/' in u:
                full_url = u if u.startswith('http') else BASE + u
                if full_url not in photos:
                    photos.append(full_url)
    
    return cover, photos


def main():
    max_pages = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    fetch_photos = '--no-photos' not in sys.argv  # can skip photo fetching for speed
    
    # Load existing data if present
    existing = {}
    if OUTPUT.exists():
        with open(OUTPUT) as f:
            data = json.load(f)
            existing = {e['url']: e for e in data.get('events', [])}
        print(f"Loaded {len(existing)} existing events")
    
    all_events = dict(existing)  # url -> event
    new_urls = []
    
    client = httpx.Client(headers=HEADERS, http2=True)
    
    try:
        print(f"Fetching up to {max_pages} pages of events...")
        for page in range(1, max_pages + 1):
            print(f"  Page {page}/{max_pages}...", end=' ', flush=True)
            try:
                events = fetch_events_page(client, page)
                print(f"{len(events)} events", flush=True)
            except Exception as e:
                print(f"Error: {e}", flush=True)
                time.sleep(2)
                continue
            
            if not events:
                print("  No more events, stopping.")
                break
            
            for ev in events:
                if ev['url'] not in all_events:
                    all_events[ev['url']] = ev
                    new_urls.append(ev['url'])
            
            time.sleep(0.5)
        
        print(f"\nNew events to process: {len(new_urls)}")
        
        # Now fetch photos for each new event
        if fetch_photos and new_urls:
            for i, url in enumerate(new_urls):
                ev = all_events[url]
                print(f"  [{i+1}/{len(new_urls)}] {ev['title'][:55]}...", end=' ', flush=True)
                cover, photos = fetch_event_photos(client, ev['url'])
                ev['cover_thumb'] = cover or ev.get('cover_thumb', '')
                ev['photos'] = photos
                print(f"{len(photos)} photos", flush=True)
                
                time.sleep(0.3)
                
                # Save periodically
                if (i + 1) % 20 == 0:
                    _save(list(all_events.values()))
                    print(f"  --- saved {len(all_events)} events ---", flush=True)
    
    finally:
        client.close()
    
    events_list = list(all_events.values())
    _save(events_list)
    
    # Stats
    total_photos = sum(len(e['photos']) for e in events_list)
    with_photos = sum(1 for e in events_list if e['photos'])
    dates = [e['date'] for e in events_list if e['date']]
    min_date = min(dates) if dates else 'N/A'
    max_date = max(dates) if dates else 'N/A'
    print(f"\nDone!")
    print(f"  Events: {len(events_list)} ({min_date} — {max_date})")
    print(f"  With photos: {with_photos}")
    print(f"  Total photos: {total_photos}")


def _save(events: list[dict]):
    events_sorted = sorted(events, key=lambda e: e.get('date', ''), reverse=True)
    OUTPUT.parent.mkdir(exist_ok=True)
    with open(OUTPUT, 'w', encoding='utf-8') as f:
        json.dump({
            'updated_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'total_events': len(events_sorted),
            'total_photos': sum(len(e['photos']) for e in events_sorted),
            'events': events_sorted
        }, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    main()
