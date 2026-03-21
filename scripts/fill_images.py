#!/usr/bin/env python3
"""
Fill missing article images from Wikimedia Commons + category fallbacks.

Strategy:
1. For each article without an image, generate search query from title + category
2. Search Wikimedia Commons for relevant CC-licensed photos
3. Download best match, save locally in /app/static/images/stock/
4. Update DB with local path
5. If no good match found, assign category fallback image

Usage:
    python scripts/fill_images.py                    # process all imageless articles
    python scripts/fill_images.py --dry-run          # just show what would be done
    python scripts/fill_images.py --limit 10         # process first N
    python scripts/fill_images.py --category-only    # only assign category fallbacks
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import time
from pathlib import Path

import httpx

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "total.db"
STOCK_DIR = BASE_DIR / "app" / "static" / "images" / "stock"
LOG_PATH = BASE_DIR / "data" / "fill_images.log"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# Category fallback images — will be downloaded from Wikimedia once
CATEGORY_FALLBACKS = {
    "vnutrennyaya_politika": {
        "query": "Ak Orda presidential palace Kazakhstan",
        "fallback_title": "Ак Орда",
    },
    "vneshnyaya_politika": {
        "query": "Kazakhstan Ministry of Foreign Affairs diplomacy",
        "fallback_title": "Дипломатия",
    },
    "ekonomika_sobitiya": {
        "query": "Kazakhstan economy astana business center",
        "fallback_title": "Экономика",
    },
    "finansi": {
        "query": "Kazakhstan National Bank tenge currency",
        "fallback_title": "Финансы",
    },
    "gossektor": {
        "query": "Kazakhstan government building Nur-Sultan",
        "fallback_title": "Госсектор",
    },
    "zhizn": {
        "query": "Kazakhstan people daily life Almaty",
        "fallback_title": "Жизнь",
    },
    "obshchestvo_sobitiya": {
        "query": "Kazakhstan society people Astana",
        "fallback_title": "Общество",
    },
    "bezopasnost": {
        "query": "Kazakhstan police security forces",
        "fallback_title": "Безопасность",
    },
    "proisshestviya": {
        "query": "Kazakhstan emergency services police",
        "fallback_title": "Происшествия",
    },
    "mir": {
        "query": "United Nations diplomacy international",
        "fallback_title": "Мир",
    },
    "sport": {
        "query": "Kazakhstan sport athletes Olympic",
        "fallback_title": "Спорт",
    },
    "nauka": {
        "query": "science research laboratory",
        "fallback_title": "Наука и техника",
    },
    "tehno": {
        "query": "technology digital innovation Kazakhstan",
        "fallback_title": "Технологии",
    },
    "mneniya": {
        "query": "Kazakhstan discussion parliament debate",
        "fallback_title": "Мнения",
    },
    "kultura": {
        "query": "Kazakhstan culture traditional art",
        "fallback_title": "Культура",
    },
}

WIKIMEDIA_API = "https://commons.wikimedia.org/w/api.php"
HEADERS = {"User-Agent": "TotalKZ-ImageBot/1.0 (https://total.kz; admin@total.kz)"}


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def search_wikimedia(query: str, limit: int = 5) -> list[dict]:
    """Search Wikimedia Commons for images matching query."""
    try:
        resp = httpx.get(WIKIMEDIA_API, params={
            "action": "query",
            "list": "search",
            "srsearch": f"{query} filetype:bitmap",
            "srnamespace": "6",
            "srlimit": str(limit),
            "format": "json",
        }, headers=HEADERS, timeout=15)
        data = resp.json()
        return data.get("query", {}).get("search", [])
    except Exception as e:
        log(f"  Wikimedia search error: {e}")
        return []


def get_image_url(file_title: str) -> str | None:
    """Get direct image URL from Wikimedia file title."""
    try:
        resp = httpx.get(WIKIMEDIA_API, params={
            "action": "query",
            "titles": file_title,
            "prop": "imageinfo",
            "iiprop": "url|size|mime",
            "iiurlwidth": "830",
            "format": "json",
        }, headers=HEADERS, timeout=15)
        data = resp.json()
        pages = data.get("query", {}).get("pages", {})
        for page in pages.values():
            info = page.get("imageinfo", [{}])[0]
            # Prefer thumbnail (resized) URL
            thumb_url = info.get("thumburl", "")
            original_url = info.get("url", "")
            mime = info.get("mime", "")
            # Skip SVGs, GIFs, very small images
            if "svg" in mime or "gif" in mime:
                return None
            width = info.get("thumbwidth", 0) or info.get("width", 0)
            if width < 300:
                return None
            return thumb_url or original_url
    except Exception as e:
        log(f"  Image URL fetch error: {e}")
    return None


def download_image(url: str, filename: str) -> str | None:
    """Download image to stock directory, return local path."""
    STOCK_DIR.mkdir(parents=True, exist_ok=True)
    # Determine extension from URL
    ext = ".jpg"
    for e in [".jpg", ".jpeg", ".png", ".webp"]:
        if e in url.lower():
            ext = e
            break
    
    filepath = STOCK_DIR / f"{filename}{ext}"
    if filepath.exists():
        return f"/static/images/stock/{filename}{ext}"
    
    try:
        resp = httpx.get(url, timeout=30, follow_redirects=True, headers=HEADERS)
        if resp.status_code == 200 and len(resp.content) > 5000:
            filepath.write_bytes(resp.content)
            return f"/static/images/stock/{filename}{ext}"
    except Exception as e:
        log(f"  Download error: {e}")
    return None


def generate_search_query(title: str, category: str) -> str:
    """Generate a Wikimedia search query from article title using GPT."""
    if not OPENAI_API_KEY:
        # Fallback: extract key entities from title
        return extract_keywords(title, category)
    
    try:
        resp = httpx.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": (
                        "Generate a short English search query (2-4 words) for finding a relevant "
                        "photo on Wikimedia Commons for this Kazakh news article title. "
                        "Focus on the main subject: person, place, organization, or event. "
                        "Return ONLY the search query, nothing else."
                    )},
                    {"role": "user", "content": f"Title: {title}\nCategory: {category}"},
                ],
                "max_tokens": 30,
                "temperature": 0.3,
            },
            timeout=10,
        )
        return resp.json()["choices"][0]["message"]["content"].strip().strip('"')
    except Exception as e:
        log(f"  GPT query gen error: {e}")
        return extract_keywords(title, category)


def extract_keywords(title: str, category: str) -> str:
    """Simple keyword extraction fallback."""
    # Remove common Kazakh/Russian news words
    stopwords = {
        "в", "на", "и", "по", "из", "за", "для", "от", "при", "о", "об", "с", "к",
        "не", "более", "свыше", "около", "тысяч", "миллионов", "миллиардов", "тенге",
        "будет", "будут", "могут", "может", "новые", "новый", "новая",
        "казахстане", "казахстана", "казахстан",
    }
    words = re.findall(r'[а-яА-ЯёЁa-zA-Z]+', title)
    keywords = [w for w in words if w.lower() not in stopwords and len(w) > 3]
    # Take first 3 meaningful words
    return " ".join(keywords[:3]) + " Kazakhstan"


def find_best_image(query: str) -> str | None:
    """Search Wikimedia and return best image URL."""
    results = search_wikimedia(query, limit=5)
    for result in results:
        file_title = result.get("title", "")
        if not file_title:
            continue
        url = get_image_url(file_title)
        if url:
            return url
    return None


def process_article(conn, article: dict, dry_run: bool = False) -> bool:
    """Find and assign image for a single article. Returns True if image assigned."""
    article_id = article["id"]
    title = article["title"] or ""
    category = article["sub_category"] or ""
    
    # Step 1: Try specific search
    query = generate_search_query(title, category)
    log(f"  Search query: {query}")
    
    if not dry_run:
        img_url = find_best_image(query)
        if img_url:
            filename = f"wm_{article_id}"
            local_path = download_image(img_url, filename)
            if local_path:
                conn.execute(
                    "UPDATE articles SET main_image = ?, thumbnail = ? WHERE id = ?",
                    (local_path, local_path, article_id)
                )
                conn.commit()
                log(f"  ✓ Assigned specific image: {local_path}")
                return True
        
        # Step 2: Assign category fallback
        fallback = CATEGORY_FALLBACKS.get(category)
        if fallback:
            fallback_path = f"/static/images/stock/cat_{category}.jpg"
            fallback_file = STOCK_DIR / f"cat_{category}.jpg"
            
            if not fallback_file.exists():
                # Download category image
                fb_url = find_best_image(fallback["query"])
                if fb_url:
                    download_image(fb_url, f"cat_{category}")
            
            if fallback_file.exists():
                conn.execute(
                    "UPDATE articles SET main_image = ?, thumbnail = ? WHERE id = ?",
                    (fallback_path, fallback_path, article_id)
                )
                conn.commit()
                log(f"  ✓ Assigned category fallback: {fallback_path}")
                return True
    
    log(f"  ✗ No image found")
    return False


def setup_category_fallbacks(dry_run: bool = False):
    """Pre-download one representative image per category."""
    STOCK_DIR.mkdir(parents=True, exist_ok=True)
    log("Setting up category fallback images...")
    
    for cat, info in CATEGORY_FALLBACKS.items():
        filepath = STOCK_DIR / f"cat_{cat}.jpg"
        if filepath.exists():
            log(f"  {cat}: already exists")
            continue
        
        if dry_run:
            log(f"  {cat}: would search '{info['query']}'")
            continue
        
        log(f"  {cat}: searching '{info['query']}'...")
        img_url = find_best_image(info["query"])
        if img_url:
            result = download_image(img_url, f"cat_{cat}")
            if result:
                log(f"  {cat}: ✓ downloaded")
            else:
                log(f"  {cat}: ✗ download failed")
        else:
            log(f"  {cat}: ✗ no results")
        
        time.sleep(1)  # rate limit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--category-only", action="store_true",
                        help="Only download category fallbacks, assign to all imageless articles")
    args = parser.parse_args()

    # Step 1: Setup category fallbacks
    setup_category_fallbacks(args.dry_run)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Get articles without images
    query = """
        SELECT id, title, sub_category, pub_date 
        FROM articles 
        WHERE main_image IS NULL OR main_image = '' 
        ORDER BY pub_date DESC
    """
    if args.limit:
        query += f" LIMIT {args.limit}"
    
    articles = conn.execute(query).fetchall()
    log(f"Articles without images: {len(articles)}")

    if not articles:
        log("All articles have images!")
        conn.close()
        return

    success = 0
    for i, article in enumerate(articles):
        log(f"[{i+1}/{len(articles)}] {article['title'][:60]}")
        
        if args.category_only:
            # Just assign category fallback
            cat = article["sub_category"] or ""
            fallback = CATEGORY_FALLBACKS.get(cat)
            if fallback and not args.dry_run:
                fallback_path = f"/static/images/stock/cat_{cat}.jpg"
                fallback_file = STOCK_DIR / f"cat_{cat}.jpg"
                if fallback_file.exists():
                    conn.execute(
                        "UPDATE articles SET main_image = ?, thumbnail = ? WHERE id = ?",
                        (fallback_path, fallback_path, article["id"])
                    )
                    conn.commit()
                    success += 1
                    log(f"  ✓ Category fallback: {fallback_path}")
                    continue
            log(f"  Category: {cat}")
        else:
            if process_article(conn, dict(article), args.dry_run):
                success += 1
        
        time.sleep(1)  # Rate limiting for Wikimedia

    conn.close()
    log(f"Done: {success}/{len(articles)} articles got images")


if __name__ == "__main__":
    main()
