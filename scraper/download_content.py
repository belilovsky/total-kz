#!/usr/bin/env python3
"""
Загрузка полного контента статей по собранным URL.
Скачивает параллельно, записывает инкрементально.

Запуск:
    python scraper/download_content.py                 # скачать все недостающие
    python scraper/download_content.py --workers 20    # больше параллельных запросов
    python scraper/download_content.py --import-db     # после скачивания импортировать в БД
    python scraper/download_content.py --redownload    # перекачать ВСЕ статьи (заменяет articles.jsonl)
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
BASE_URL = "https://total.kz"

sys.path.insert(0, str(BASE_DIR))
from app.database import get_db, init_db, import_jsonl


def make_full_url(src):
    """Превратить относительный URL в абсолютный."""
    if not src:
        return ""
    if src.startswith("http"):
        return src
    return BASE_URL + src


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

        # === Заголовок ===
        title_el = tree.css_first("h1.article__title") or tree.css_first("h1")
        title = title_el.text(strip=True) if title_el else ""

        # === Автор ===
        # Автор находится в span.gray-text внутри .article__meta, но не в span.meta__date
        author = ""
        meta_el = tree.css_first(".article__meta")
        if meta_el:
            for span in meta_el.css("span.gray-text"):
                if "meta__date" not in (span.attributes.get("class", "")):
                    author = span.text(strip=True)
                    break

        # === Дата из текста ===
        date_el = tree.css_first("span.meta__date")
        date_text = date_el.text(strip=True) if date_el else ""

        # === Тело статьи ===
        body_el = tree.css_first("div.article__post__body")
        if body_el:
            # Удаляем рекламные скрипты и блоки из body
            for script in body_el.css("script"):
                script.decompose()
            for ad in body_el.css(".adfox, .ya-partner, [id^='adfox'], ins"):
                ad.decompose()

            body_html = body_el.html
            body_text = body_el.text(strip=True)
        else:
            body_html = ""
            body_text = ""

        # Если body пустой — статья не загрузилась (404 или JS-only)
        if not body_text:
            return None

        # === Аннотация (excerpt) ===
        # Обычно первый <strong><p>...</p></strong> в body
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

        # === Главное изображение ===
        img_el = tree.css_first("div.post__image img.img-responsive")
        main_image = ""
        if img_el:
            main_image = make_full_url(img_el.attributes.get("src", ""))

        # Фолбэк на og:image
        if not main_image:
            og = tree.css_first("meta[property='og:image']")
            if og:
                main_image = make_full_url(og.attributes.get("content", ""))

        # === Кредит фото ===
        credit_el = tree.css_first("div.post__image_author")
        image_credit = credit_el.text(strip=True) if credit_el else ""

        # === Миниатюра ===
        og_img = tree.css_first("meta[property='og:image']")
        thumbnail = make_full_url(og_img.attributes.get("content", "")) if og_img else main_image

        # === Теги ===
        tags = []
        for tag_el in tree.css("ul.meta__tags li a"):
            t = tag_el.text(strip=True).lstrip("#")
            if t:
                tags.append(t)

        # === Картинки из тела ===
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
    parser.add_argument("--workers", type=int, default=15, help="Параллельных запросов (по умолчанию 15)")
    parser.add_argument("--import-db", action="store_true", help="Импортировать в БД после скачивания")
    parser.add_argument("--redownload", action="store_true", help="Перекачать ВСЕ статьи (заменяет articles.jsonl)")
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

    # Также добавляем URL из БД, которых нет в urls.jsonl
    url_set = {u["url"] for u in urls}
    with get_db() as conn:
        rows = conn.execute("SELECT url, pub_date, sub_category FROM articles").fetchall()
        for row in rows:
            if row[0] not in url_set:
                urls.append({
                    "url": row[0],
                    "pub_date": row[1],
                    "sub_category": row[2] or "",
                })
                url_set.add(row[0])

    if not urls:
        print("Нет URL для скачивания. Сначала запустите scrape_urls.py")
        sys.exit(1)

    # Определяем, что уже скачано
    if args.redownload:
        to_download = urls
        # Создаём новый файл
        ARTICLES_FILE.unlink(missing_ok=True)
        print(f"Режим перекачки: будет скачано {len(urls)} статей")
    else:
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
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9",
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
                    if art and art.get("body_text"):
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
