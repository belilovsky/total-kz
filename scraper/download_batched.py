#!/usr/bin/env python3
"""
Скачивание контента статей партиями с надёжным сохранением прогресса.

Каждые BATCH_SIZE статей:
  1. Дозаписывает в articles.jsonl
  2. Импортирует в БД
  3. Логирует прогресс

При прерывании — безопасно продолжить с того же места.

Запуск:
    python scraper/download_batched.py
    python scraper/download_batched.py --workers 20 --batch 5000
"""
import json
import asyncio
import time
import sys
import signal
import argparse
import os
from pathlib import Path
from datetime import datetime

import httpx
from selectolax.parser import HTMLParser

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
URLS_FILE = DATA_DIR / "urls.jsonl"
ARTICLES_FILE = DATA_DIR / "articles.jsonl"
PROGRESS_FILE = DATA_DIR / "download_progress.json"
LOG_FILE = DATA_DIR / "download_batched.log"
BASE_URL = "https://total.kz"

sys.path.insert(0, str(BASE_DIR))
from app.database import get_db, init_db, import_jsonl

# Graceful shutdown
_shutdown = False
def _signal_handler(sig, frame):
    global _shutdown
    _shutdown = True
    print("\n  ⚠ Получен сигнал завершения, сохраняю прогресс...", flush=True)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def make_full_url(src):
    if not src:
        return ""
    if src.startswith("http"):
        return src
    return BASE_URL + src


async def download_article(client, url_data, semaphore, max_retries=3):
    """Скачать и распарсить одну статью."""
    async with semaphore:
        url = url_data["url"]
        for attempt in range(max_retries):
            try:
                resp = await client.get(url, timeout=httpx.Timeout(10, read=20))
                if resp.status_code == 429:
                    await asyncio.sleep(5 * (attempt + 1))
                    continue
                if resp.status_code != 200:
                    return None
                break
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError):
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))
                continue
            except Exception:
                return None
        else:
            return None

        try:
            tree = HTMLParser(resp.text)
        except Exception:
            return None

        title_el = tree.css_first("h1.article__title") or tree.css_first("h1")
        title = title_el.text(strip=True) if title_el else ""

        author = ""
        meta_el = tree.css_first(".article__meta")
        if meta_el:
            for span in meta_el.css("span.gray-text"):
                if "meta__date" not in (span.attributes.get("class", "")):
                    author = span.text(strip=True)
                    break

        date_el = tree.css_first("span.meta__date")
        date_text = date_el.text(strip=True) if date_el else ""

        body_el = tree.css_first("div.article__post__body")
        if body_el:
            for script in body_el.css("script"):
                script.decompose()
            for ad in body_el.css(".adfox, .ya-partner, [id^='adfox'], ins"):
                ad.decompose()
            body_html = body_el.html
            body_text = body_el.text(strip=True)
        else:
            body_html = ""
            body_text = ""

        if not body_text:
            return None

        excerpt = ""
        strong_el = body_el.css_first("strong > p") if body_el else None
        if strong_el:
            excerpt = strong_el.text(strip=True)
        if not excerpt:
            meta_desc = tree.css_first("meta[name='description']")
            if meta_desc:
                excerpt = meta_desc.attributes.get("content", "")
        if not excerpt:
            excerpt = body_text[:300]

        img_el = tree.css_first("div.post__image img.img-responsive")
        main_image = ""
        if img_el:
            main_image = make_full_url(img_el.attributes.get("src", ""))
        if not main_image:
            og = tree.css_first("meta[property='og:image']")
            if og:
                main_image = make_full_url(og.attributes.get("content", ""))

        credit_el = tree.css_first("div.post__image_author")
        image_credit = credit_el.text(strip=True) if credit_el else ""

        og_img = tree.css_first("meta[property='og:image']")
        thumbnail = make_full_url(og_img.attributes.get("content", "")) if og_img else main_image

        tags = []
        for tag_el in tree.css("ul.meta__tags li a"):
            t = tag_el.text(strip=True).lstrip("#")
            if t:
                tags.append(t)

        inline_images = []
        if body_el:
            for img in body_el.css("img"):
                src = make_full_url(img.attributes.get("src", ""))
                if src and src != main_image:
                    inline_images.append(src)

        return {
            "url": url,
            "pub_date": url_data.get("pub_date"),
            "sub_category": url_data.get("sub_category", ""),
            "category_label": url_data.get("category_label", ""),
            "title": title,
            "author": author,
            "date_text": date_text,
            "main_image": main_image,
            "image_credit": image_credit,
            "excerpt": excerpt,
            "body_text": body_text,
            "body_html": body_html,
            "thumbnail": thumbnail,
            "tags": tags,
            "inline_images": inline_images,
        }


def load_progress():
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"downloaded": 0, "errors": 0, "last_batch": 0}


def save_progress(data):
    PROGRESS_FILE.write_text(json.dumps(data, indent=2))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=15)
    parser.add_argument("--batch", type=int, default=10000, help="Размер партии для импорта в БД")
    parser.add_argument("--limit", type=int, default=0, help="Макс. статей за запуск (0=все)")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()

    # Собираем все URL
    urls = []
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    try:
                        urls.append(json.loads(line))
                    except:
                        pass
    log(f"URL в файле: {len(urls)}")

    # Уже скачанные (в articles.jsonl + в БД)
    existing = set()
    if ARTICLES_FILE.exists():
        with open(ARTICLES_FILE) as f:
            for line in f:
                try:
                    existing.add(json.loads(line).get("url"))
                except:
                    pass

    with get_db() as conn:
        for row in conn.execute("SELECT url FROM articles WHERE body_text IS NOT NULL AND body_text != ''"):
            existing.add(row[0])

    to_download = [u for u in urls if u["url"] not in existing]
    log(f"Уже есть: {len(existing)}, осталось скачать: {len(to_download)}")

    if not to_download:
        log("Всё скачано!")
        return

    if args.limit > 0:
        to_download = to_download[:args.limit]
        log(f"Ограничение: {args.limit} статей за этот запуск")

    progress = load_progress()
    total_downloaded = progress["downloaded"]
    total_errors = progress["errors"]
    batch_downloaded = 0
    batch_errors = 0
    start_time = time.time()

    semaphore = asyncio.Semaphore(args.workers)
    transport = httpx.AsyncHTTPTransport(
        retries=2,
        limits=httpx.Limits(max_connections=args.workers + 5, max_keepalive_connections=args.workers),
    )

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9",
        },
        follow_redirects=True,
        transport=transport,
    ) as client:
        chunk_size = 50
        for i in range(0, len(to_download), chunk_size):
            if _shutdown:
                break

            chunk = to_download[i : i + chunk_size]
            tasks = [download_article(client, u, semaphore) for u in chunk]

            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=180,
                )
            except asyncio.TimeoutError:
                batch_errors += len(chunk)
                continue

            with open(ARTICLES_FILE, "a") as f:
                for art in results:
                    if isinstance(art, Exception) or not art or not art.get("body_text"):
                        batch_errors += 1
                    else:
                        f.write(json.dumps(art, ensure_ascii=False) + "\n")
                        batch_downloaded += 1

            current = i + len(chunk)
            elapsed = time.time() - start_time
            rate = batch_downloaded / elapsed if elapsed > 0 else 0
            eta_min = (len(to_download) - current) / rate / 60 if rate > 0 else 0

            if current % 500 < chunk_size:
                log(f"  {current}/{len(to_download)} | ok: {batch_downloaded} err: {batch_errors} | {rate:.1f} ст/сек | ETA: {eta_min:.0f} мин")

            # Каждые batch_size — импорт в БД
            if batch_downloaded > 0 and batch_downloaded % args.batch < chunk_size and batch_downloaded >= args.batch:
                log(f"  → Импортирую в БД (batch checkpoint)...")
                result = import_jsonl(str(ARTICLES_FILE))
                log(f"  → Импорт: {result['imported']} новых, всего в БД: {result.get('total', '?')}")
                total_downloaded += batch_downloaded
                total_errors += batch_errors
                save_progress({"downloaded": total_downloaded, "errors": total_errors, "last_batch": datetime.now().isoformat()})

    # Финальный импорт
    if batch_downloaded > 0:
        log(f"Финальный импорт в БД...")
        result = import_jsonl(str(ARTICLES_FILE))
        log(f"Импорт: {result['imported']} новых, {result.get('errors', 0)} ошибок")

    total_downloaded += batch_downloaded
    total_errors += batch_errors
    save_progress({"downloaded": total_downloaded, "errors": total_errors, "last_batch": datetime.now().isoformat()})

    status = "ПРЕРВАНО" if _shutdown else "Готово"
    log(f"\n{status}: за запуск +{batch_downloaded} скачано, {batch_errors} ошибок")
    log(f"Всего накоплено: {total_downloaded} скачано, {total_errors} ошибок")

    # Итоговые цифры БД
    with get_db() as conn:
        total_db = conn.execute("SELECT count(*) FROM articles").fetchone()[0]
        log(f"В БД: {total_db} статей")


if __name__ == "__main__":
    asyncio.run(main())
