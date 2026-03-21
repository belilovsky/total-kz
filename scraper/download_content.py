#!/usr/bin/env python3
"""
Загрузка полного контента статей по собранным URL.
Надёжная версия: retry, batch checkpointing, graceful shutdown.

Запуск:
    python scraper/download_content.py                 # скачать все недостающие
    python scraper/download_content.py --workers 20    # больше параллельных запросов
    python scraper/download_content.py --import-db     # после скачивания импортировать в БД
    python scraper/download_content.py --redownload    # перекачать ВСЕ статьи
"""
import json
import asyncio
import time
import sys
import signal
import argparse
from pathlib import Path
from datetime import datetime

import httpx
from selectolax.parser import HTMLParser

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
URLS_FILE = DATA_DIR / "urls.jsonl"
ARTICLES_FILE = DATA_DIR / "articles.jsonl"
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


def make_full_url(src):
    if not src:
        return ""
    if src.startswith("http"):
        return src
    return BASE_URL + src


async def download_article(client, url_data, semaphore, max_retries=3):
    """Скачать и распарсить одну статью с retry."""
    async with semaphore:
        url = url_data["url"]
        last_error = None

        for attempt in range(max_retries):
            try:
                resp = await client.get(url, timeout=httpx.Timeout(10, read=20))
                if resp.status_code == 429:
                    # Rate limited – wait and retry
                    wait = 5 * (attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                if resp.status_code != 200:
                    return None
                break
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))
                continue
            except Exception as e:
                last_error = e
                return None
        else:
            return None

        try:
            tree = HTMLParser(resp.text)
        except Exception:
            return None

        # === Заголовок ===
        title_el = tree.css_first("h1.article__title") or tree.css_first("h1")
        title = title_el.text(strip=True) if title_el else ""

        # === Автор ===
        author = ""
        meta_el = tree.css_first(".article__meta")
        if meta_el:
            for span in meta_el.css("span.gray-text"):
                if "meta__date" not in (span.attributes.get("class", "")):
                    author = span.text(strip=True)
                    break

        # === Дата ===
        date_el = tree.css_first("span.meta__date")
        date_text = date_el.text(strip=True) if date_el else ""

        # === Тело ===
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

        # === Excerpt ===
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

        # === Изображения ===
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

        # === Теги ===
        tags = []
        for tag_el in tree.css("ul.meta__tags li a"):
            t = tag_el.text(strip=True).lstrip("#")
            if t:
                tags.append(t)

        # === Inline images ===
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


async def main():
    parser = argparse.ArgumentParser(description="Загрузка контента статей total.kz")
    parser.add_argument("--workers", type=int, default=15, help="Параллельных запросов")
    parser.add_argument("--import-db", action="store_true", help="Импортировать в БД")
    parser.add_argument("--redownload", action="store_true", help="Перекачать ВСЕ")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()

    # Загружаем URLs
    urls = []
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    urls.append(json.loads(line))

    url_set = {u["url"] for u in urls}
    with get_db() as conn:
        rows = conn.execute("SELECT url, pub_date, sub_category FROM articles").fetchall()
        for row in rows:
            if row[0] not in url_set:
                urls.append({"url": row[0], "pub_date": row[1], "sub_category": row[2] or ""})
                url_set.add(row[0])

    if not urls:
        print("Нет URL для скачивания.")
        sys.exit(1)

    # Определяем что скачано
    if args.redownload:
        to_download = urls
        ARTICLES_FILE.unlink(missing_ok=True)
        print(f"Перекачка: {len(urls)} статей")
    else:
        existing = set()
        if ARTICLES_FILE.exists():
            with open(ARTICLES_FILE) as f:
                for line in f:
                    if line.strip():
                        try:
                            existing.add(json.loads(line).get("url"))
                        except json.JSONDecodeError:
                            continue

        # Также проверяем БД – если статья уже в БД с body_text, пропускаем
        with get_db() as conn:
            db_urls = conn.execute(
                "SELECT url FROM articles WHERE body_text IS NOT NULL AND body_text != ''"
            ).fetchall()
            for row in db_urls:
                existing.add(row[0])

        to_download = [u for u in urls if u["url"] not in existing]
        print(f"Всего URL: {len(urls)}, уже есть: {len(existing)}, осталось: {len(to_download)}")

    if not to_download:
        print("Все статьи уже скачаны.")
        if args.import_db:
            print("Импортирую в БД...")
            result = import_jsonl(str(ARTICLES_FILE))
            print(f"Импорт: {result['imported']} новых, {result['errors']} ошибок")
        return

    # Логируем запуск
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_runs (started_at, phase, status) VALUES (?, 'content', 'running')",
            (datetime.now().isoformat(),)
        )
        run_id = cursor.lastrowid

    downloaded = 0
    errors = 0
    start_time = time.time()
    semaphore = asyncio.Semaphore(args.workers)

    # Клиент с connection limits и retry transport
    transport = httpx.AsyncHTTPTransport(
        retries=2,
        limits=httpx.Limits(
            max_connections=args.workers + 5,
            max_keepalive_connections=args.workers,
        ),
    )

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9",
        },
        follow_redirects=True,
        transport=transport,
    ) as client:
        batch_size = 50
        for i in range(0, len(to_download), batch_size):
            if _shutdown:
                break

            batch = to_download[i : i + batch_size]
            tasks = [download_article(client, u, semaphore) for u in batch]

            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=180  # 3 минуты на пачку из 50
                )
            except asyncio.TimeoutError:
                print(f"  ⚠ Таймаут пачки {i}-{i+batch_size}, пропускаю", flush=True)
                errors += len(batch)
                continue

            # Записываем
            with open(ARTICLES_FILE, "a") as f:
                for art in results:
                    if isinstance(art, Exception):
                        errors += 1
                    elif art and art.get("body_text"):
                        f.write(json.dumps(art, ensure_ascii=False) + "\n")
                        downloaded += 1
                    else:
                        errors += 1

            elapsed = time.time() - start_time
            rate = downloaded / elapsed if elapsed > 0 else 0
            print(
                f"  {i + len(batch)}/{len(to_download)} | "
                f"ok: {downloaded}, ошибки: {errors} | "
                f"{rate:.1f} ст/сек",
                flush=True,
            )

    # Обновляем статус
    status = 'completed' if not _shutdown else 'interrupted'
    with get_db() as conn:
        conn.execute(
            "UPDATE scrape_runs SET finished_at=?, status=?, articles_downloaded=?, errors=? WHERE id=?",
            (datetime.now().isoformat(), status, downloaded, errors, run_id),
        )

    print(f"\n{'ПРЕРВАНО' if _shutdown else 'Готово'}: {downloaded} скачано, {errors} ошибок")

    # Импорт в БД
    if args.import_db and not _shutdown:
        print("\nИмпортирую в БД...")
        result = import_jsonl(str(ARTICLES_FILE))
        print(f"Импорт: {result['imported']} новых, {result['errors']} ошибок")


if __name__ == "__main__":
    asyncio.run(main())
