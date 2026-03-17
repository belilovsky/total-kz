"""Collect article URLs from total.kz with pagination."""
import asyncio
import json
import httpx
from pathlib import Path
from datetime import datetime, timedelta
from selectolax.parser import HTMLParser

BASE = "https://total.kz"
CATEGORIES = [
    "politika", "ekonomika", "obshchestvo", "drugoe", "media", "special"
]
DATA_DIR = Path(__file__).parent.parent / "data"
URLS_FILE = DATA_DIR / "urls.jsonl"

# DB integration
import sys; sys.path.insert(0, str(Path(__file__).parent.parent))
from app.database import get_db, init_db


async def collect_category(client: httpx.AsyncClient, category: str, since_days: int = 365):
    """Collect URLs from a category, paginating until we hit old articles."""
    cutoff = datetime.now() - timedelta(days=since_days)
    urls = []
    page = 1

    while True:
        url = f"{BASE}/ru/news/{category}/page-{page}"
        try:
            resp = await client.get(url, timeout=30)
            if resp.status_code != 200:
                break
        except Exception as e:
            print(f"  Error on {url}: {e}")
            break

        tree = HTMLParser(resp.text)
        links = tree.css("a[href*='/ru/news/']")
        if not links:
            break

        found = 0
        too_old = False
        for link in links:
            href = link.attributes.get("href", "")
            if "_date_" not in href:
                continue
            full_url = href if href.startswith("http") else BASE + href
            if full_url not in [u["url"] for u in urls]:
                # Parse date from URL
                try:
                    parts = full_url.split("_date_")[1].split("_")
                    pub_date = f"{parts[0]}-{parts[1]}-{parts[2]}T{parts[3]}:{parts[4]}:{parts[5]}"
                    dt = datetime.fromisoformat(pub_date)
                    if dt < cutoff:
                        too_old = True
                        continue
                except (IndexError, ValueError):
                    pub_date = None

                sub = href.split("/ru/news/")[-1].split("/")[0] if "/ru/news/" in href else category
                urls.append({"url": full_url, "sub_category": sub, "pub_date": pub_date})
                found += 1

        print(f"  {category} page {page}: +{found} URLs")
        if too_old or found == 0:
            break
        page += 1
        await asyncio.sleep(0.3)

    return urls


async def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()

    # Log run
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_runs (started_at, phase, status) VALUES (?, 'urls', 'running')",
            (datetime.now().isoformat(),)
        )
        run_id = cursor.lastrowid

    all_urls = []
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0"},
        follow_redirects=True,
        limits=httpx.Limits(max_connections=5)
    ) as client:
        for cat in CATEGORIES:
            print(f"Collecting {cat}...")
            urls = await collect_category(client, cat)
            all_urls.extend(urls)
            print(f"  {cat}: {len(urls)} URLs")

    # Deduplicate
    seen = set()
    unique = []
    for u in all_urls:
        if u["url"] not in seen:
            seen.add(u["url"])
            unique.append(u)

    # Save to JSONL
    with open(URLS_FILE, "w") as f:
        for u in unique:
            f.write(json.dumps(u, ensure_ascii=False) + "\n")

    # Update run
    with get_db() as conn:
        conn.execute(
            "UPDATE scrape_runs SET finished_at=?, status='completed', articles_found=? WHERE id=?",
            (datetime.now().isoformat(), len(unique), run_id)
        )

    print(f"\nTotal: {len(unique)} unique URLs saved to {URLS_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
