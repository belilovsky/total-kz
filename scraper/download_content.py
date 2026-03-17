#!/usr/bin/env python3
"""
Загрузка полного контента статей по собранным URL.
Скачивает параллельно, записывает инкрементально.

Запуск:
    python scraper/download_content.py                 # скачать все недостающие
    python scraper/download_content.py --workers 20    # больше параллельных запросов
    python scraper/download_content.py --import-db     # после скачивания импортировать в БД
"""
import json
import asyncio
import time
import sys
import argparse
from pathlib import Path
from datetime import datetime

import httpx
from selectolax.parser import HTMLParser

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
URLS_FILE = DATA_DIR / "urls.jsonl"
ARTICLES_FILE = DATA_DIR / "articles.jsonl"

sys.path.insert(0, str(BASE_DIR))
from app.database import get_db, init_db, import_jsonl


async def download_article(client, url_data, semaphore):
    """Скачать и распарсить одну статью."""
    async with semaphore:
        url = url_data["url"]
        try:
            resp = await client.get(url, timeout=30)
            if resp.status_code != 200:
                return None
        except Exception:
            return None

        tree = HTMLParser(resp.text)

        # Заголовок
        title_el = tree.css_first("h1") or tree.css_first(".article-title")
        title = title_el.text(strip=True) if title_el else ""

        # Автор
        author_el = (
            tree.css_first(".author-name")
            or tree.css_first("[rel='author']")
            or tree.css_first(".article-author")
        )
        author = author_el.text(strip=True) if author_el else ""

        # Тело статьи
        body_el = (
            tree.css_first(".article-body")
            or tree.css_first(".article-content")
            or tree.css_first(".entry-content")
            or tree.css_first("article")
        )
        body_html = body_el.html if body_el else ""
        body_text = body_el.text(strip=True) if body_el else ""

        # Аннотация
        excerpt_el = (
            tree.css_first(".article-excerpt")
            or tree.css_first(".article-lead")
            or tree.css_first("meta[name='description']")
        )
        if excerpt_el:
            excerpt = (
                excerpt_el.attributes.get("content", "")
                if excerpt_el.tag == "meta"
                else excerpt_el.text(strip=True)
            )
        else:
            excerpt = body_text[:300] if body_text else ""

        # Главное изображение
        img_el = (
            tree.css_first(".article-image img")
            or tree.css_first("article img")
            or tree.css_first("meta[property='og:image']")
        )
        main_image = ""
        if img_el:
            main_image = img_el.attributes.get("src") or img_el.attributes.get("content", "")

        # Кредит фото
        credit_el = tree.css_first(".image-credit") or tree.css_first(".photo-credit")
        image_credit = credit_el.text(strip=True) if credit_el else ""

        # Миниатюра
        thumb_el = tree.css_first("meta[property='og:image']")
        thumbnail = thumb_el.attributes.get("content", "") if thumb_el else main_image

        # Теги
        tags = []
        for tag_el in tree.css(".tags a, .article-tags a, [rel='tag']"):
            t = tag_el.text(strip=True)
            if t:
                tags.append(t)

        # Картинки из тела
        inline_images = []
        if body_el:
            for img in body_el.css("img"):
                src = img.attributes.get("src", "")
                if src and src != main_image:
                    inline_images.append(src)

        # Метка категории
        cat_el = tree.css_first(".category-label") or tree.css_first(".breadcrumb a:last-child")
        category_label = cat_el.text(strip=True) if cat_el else ""

        return {
            "url": url,
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
    parser = argparse.ArgumentParser(description="Загрузка контента статей total.kz")
    parser.add_argument("--workers", type=int, default=15, help="Параллельных запросов (по умолчанию 15)")
    parser.add_argument("--import-db", action="store_true", help="Импортировать в БД после скачивания")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()

    # Загружаем список URL
    urls = []
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    urls.append(json.loads(line))

    if not urls:
        print("Нет URL для скачивания. Сначала запустите scrape_urls.py")
        sys.exit(1)

    # Определяем, что уже скачано
    existing = set()
    if ARTICLES_FILE.exists():
        with open(ARTICLES_FILE) as f:
            for line in f:
                if line.strip():
                    existing.add(json.loads(line).get("url"))

    to_download = [u for u in urls if u["url"] not in existing]
    print(f"Всего URL: {len(urls)}, уже скачано: {len(existing)}, осталось: {len(to_download)}")

    if not to_download:
        print("Все статьи уже скачаны.")
        if args.import_db:
            print("Импортирую в базу данных...")
            result = import_jsonl(str(ARTICLES_FILE))
            print(f"Импорт: {result['imported']} новых, {result['skipped']} пропущено, {result['errors']} ошибок")
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

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        },
        follow_redirects=True,
        limits=httpx.Limits(max_connections=args.workers),
    ) as client:
        batch_size = 50
        for i in range(0, len(to_download), batch_size):
            batch = to_download[i : i + batch_size]
            tasks = [download_article(client, u, semaphore) for u in batch]
            results = await asyncio.gather(*tasks)

            # Записываем сразу после каждой пачки
            with open(ARTICLES_FILE, "a") as f:
                for art in results:
                    if art and art.get("title"):
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
    with get_db() as conn:
        conn.execute(
            "UPDATE scrape_runs SET finished_at=?, status='completed', articles_downloaded=?, errors=? WHERE id=?",
            (datetime.now().isoformat(), downloaded, errors, run_id),
        )

    print(f"\nГотово: {downloaded} скачано, {errors} ошибок")

    # Импорт в БД
    if args.import_db:
        print("\nИмпортирую в базу данных...")
        result = import_jsonl(str(ARTICLES_FILE))
        print(f"Импорт: {result['imported']} новых, {result['skipped']} пропущено, {result['errors']} ошибок")


if __name__ == "__main__":
    asyncio.run(main())
