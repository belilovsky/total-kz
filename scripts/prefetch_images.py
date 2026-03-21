"""Pre-fetch all article images into local cache.
Run inside Docker: python scripts/prefetch_images.py
Or from host: docker exec total_kz_app python scripts/prefetch_images.py
"""
import httpx
import sys
import os
from pathlib import Path

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import database as db

ORIGIN = "https://total.kz/storage"
CACHE_DIR = Path(__file__).parent.parent / "data" / "img_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def extract_storage_path(url: str) -> str | None:
    """Extract path after /storage/ from URL."""
    for prefix in ("https://total.kz/storage/", "http://total.kz/storage/"):
        if url and url.startswith(prefix):
            return url[len(prefix):]
    return None

def main():
    print("Fetching article list from DB...")
    # Get all articles with images
    articles = db.get_latest_articles(limit=5000)
    
    urls = set()
    for art in articles:
        for field in ("main_image", "thumbnail"):
            path = extract_storage_path(art.get(field, "") or "")
            if path:
                urls.add(path)
    
    print(f"Found {len(urls)} unique image URLs")
    
    fetched = 0
    skipped = 0
    failed = 0
    
    with httpx.Client(timeout=15.0, follow_redirects=True) as client:
        for i, path in enumerate(sorted(urls)):
            cache_path = CACHE_DIR / path
            if cache_path.exists():
                skipped += 1
                continue
            
            origin_url = f"{ORIGIN}/{path}"
            try:
                resp = client.get(origin_url)
                if resp.status_code == 200:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(resp.content)
                    fetched += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
            
            if (i + 1) % 50 == 0:
                print(f"  Progress: {i+1}/{len(urls)} (fetched={fetched}, cached={skipped}, failed={failed})")
    
    print(f"\nDone! Fetched: {fetched}, Already cached: {skipped}, Failed: {failed}")

if __name__ == "__main__":
    main()
