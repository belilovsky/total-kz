"""Download full article content from collected URLs."""
import asyncio
import json
import httpx
from pathlib import Path
from datetime import datetime
from selectolax.parser import HTMLParser

DATA_DIR = Path(__file__).parent.parent / "data"
URLS_FILE = DATA_DIR / "urls.jsonl"
ARTICLES_FILE = DATA_DIR / "articles.jsonl"

import sys; sys.path.insert(0, str(Path(__file__).parent.parent))
from app.database import get_db, init_db

SEMAPHORE = asyncio.Semaphore(10)


async def download_article(client: httpx.AsyncClient, url_data: dict) -> dict | None:
    async with SEMAPHORE:
        try:
            resp = await client.get(url_data["url"], timeout=30)
            if resp.status_code != 200:
                return None
        except Exception:
            return None

        tree = HTMLParser(resp.text)

        title_el = tree.css_first("h1") or tree.css_first(".article-title") or tree.css_first("title")
        title = title_el.text(strip=True) if title_el else ""

        author_el = tree.css_first(".author-name") or tree.css_first("[rel='author']") or tree.css_first(".article-author")
        author = author_el.text(strip=True) if author_el else ""

        body_el = tree.css_first(".article-body") or tree.css_first(".article-content") or tree.css_first(".entry-content") or tree.css_first("article")
        body_html = body_el.html if body_el else ""
        body_text = body_el.text(strip=True) if body_el else ""

        excerpt_el = tree.css_first(".article-excerpt") or tree.css_first(".article-lead") or tree.css_first("meta[name='description']")
        if excerpt_el:
            excerpt = excerpt_el.attributes.get("content", "") if excerpt_el.tag == "meta" else excerpt_el.text(strip=True)
        else:
            excerpt = body_text[:300] if body_text else ""

        img_el = tree.css_first(".article-image img") or tree.css_first("article img") or tree.css_first("meta[property='og:image']")
        if img_el:
            main_image = img_el.attributes.get("src") or img_el.attributes.get("content", "")
        else:
            main_image = ""

        credit_el = tree.css_first(".image-credit") or tree.css_first(".photo-credit")
        image_credit = credit_el.text(strip=True) if credit_el else ""

        thumb_el = tree.css_first("meta[property='og:image']")
        thumbnail = thumb_el.attributes.get("content", "") if thumb_el else main_image

        tags = []
        for tag_el in tree.css(".tags a, .article-tags a, [rel='tag']"):
            t = tag_el.text(strip=True)
            if t:
                tags.append(t)

        inline_images = []
        if body_el:
            for img in body_el.css("img"):
                src = img.attributes.get("src", "")
                if src and src != main_image:
                    inline_images.append(src)

        cat_el = tree.css_first(".category-label") or tree.css_first(".breadcrumb a:last-child")
        category_label = cat_el.text(strip=True) if cat_el else ""

        return {
            "url": url_data["url"],
            "pub_date": url_data.get("pub_date"),
            "sub_category": url_data.get("sub_category", ""),
            "category_label": category_label,
            "title": title,
            "author": author,
            "excerpt": excerpt,
            "body_text": body_text,
            "body_html": body_html,
            "main_image": main_image,
            "image_credit": image_credit,
            "thumbnail": thumbnail,
            "tags": tags,
            "inline_images": inline_images,
        }


async def main():
    init_db()

    # Load URLs
    urls = []
    with open(URLS_FILE) as f:
        for line in f:
            if line.strip():
                urls.append(json.loads(line))

    # Check already downloaded
    existing = set()
    if ARTICLES_FILE.exists():
        with open(ARTICLES_FILE) as f:
            for line in f:
                if line.strip():
                    art = json.loads(line)
                    existing.add(art.get("url"))

    to_download = [u for u in urls if u["url"] not in existing]
    print(f"Total URLs: {len(urls)}, already downloaded: {len(existing)}, to download: {len(to_download)}")

    # Log run
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_runs (started_at, phase, status) VALUES (?, 'content', 'running')",
            (datetime.now().isoformat(),)
        )
        run_id = cursor.lastrowid

    downloaded = 0
    errors = 0

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0"},
        follow_redirects=True,
        limits=httpx.Limits(max_connections=10)
    ) as client:
        batch_size = 50
        for i in range(0, len(to_download), batch_size):
            batch = to_download[i:i + batch_size]
            tasks = [download_article(client, u) for u in batch]
            results = await asyncio.gather(*tasks)

            with open(ARTICLES_FILE, "a") as f:
                for art in results:
                    if art:
                        f.write(json.dumps(art, ensure_ascii=False) + "\n")
                        downloaded += 1
                    else:
                        errors += 1

            print(f"  Progress: {i + len(batch)}/{len(to_download)} (downloaded: {downloaded}, errors: {errors})")

    # Update run
    with get_db() as conn:
        conn.execute(
            "UPDATE scrape_runs SET finished_at=?, status='completed', articles_downloaded=?, errors=? WHERE id=?",
            (datetime.now().isoformat(), downloaded, errors, run_id)
        )

    print(f"\nDone! Downloaded: {downloaded}, Errors: {errors}")


if __name__ == "__main__":
    asyncio.run(main())
