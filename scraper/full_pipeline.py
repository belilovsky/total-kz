#!/usr/bin/env python3
"""
Полный пайплайн сбора total.kz: URL → контент → БД → Meilisearch.

Три этапа:
  1. collect_urls  — пагинация всех категорий, сбор URL в urls.jsonl
  2. download      — параллельное скачивание контента → articles.jsonl
  3. import_db     — импорт JSONL → SQLite + Meilisearch

Запуск полного цикла:
  python scraper/full_pipeline.py

Отдельные этапы:
  python scraper/full_pipeline.py --stage urls
  python scraper/full_pipeline.py --stage download --workers 20
  python scraper/full_pipeline.py --stage import

Возобновление:
  python scraper/full_pipeline.py --resume

Статус:
  python scraper/full_pipeline.py --status
"""
import re
import json
import time
import sys
import signal
import asyncio
import sqlite3
import argparse
import os
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import httpx
from selectolax.parser import HTMLParser

# ══════════════════════════════════════════════
#  PATHS & CONFIG
# ══════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
URLS_FILE = DATA_DIR / "urls_full.jsonl"       # отдельный файл, не затираем старый
ARTICLES_FILE = DATA_DIR / "articles_full.jsonl"
PROGRESS_FILE = DATA_DIR / "pipeline_progress.json"
LOG_FILE = DATA_DIR / "pipeline.log"
DB_PATH = os.environ.get("DB_PATH", str(DATA_DIR / "total.db"))

BASE_URL = "https://total.kz"

# Категории с примерным кол-вом страниц (обновлено)
CATEGORIES = {
    "obshchestvo": 8600,
    "politika": 4600,
    "ekonomika": 4700,
    "drugoe": 3300,
    "media": 600,
    "special": 10,
}

TOTAL_EXPECTED = 186000  # примерно

# Регексы
DATE_RE = re.compile(r'_date_(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})')
ARTICLE_HREF_RE = re.compile(r'/ru/news/[^/]+/(?!page-)[^/]+')

RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}
RU_DATE_RE = re.compile(r'(\d{1,2})\s+(\S+)\s+(\d{4})(?:,\s*(\d{1,2}):(\d{2}))?')

# Graceful shutdown
_shutdown = False
def _signal_handler(sig, frame):
    global _shutdown
    _shutdown = True
    log("⚠ Получен сигнал завершения, сохраняю прогресс...")
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def log(msg):
    """Лог в файл и stdout."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"urls": {}, "download_offset": 0, "imported": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


# ══════════════════════════════════════════════
#  ЭТАП 1: СБОР URL
# ══════════════════════════════════════════════

def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ru-RU,ru;q=0.9",
    })
    return session


def parse_date_from_url(url):
    m = DATE_RE.search(url)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                            int(m.group(4)), int(m.group(5)), int(m.group(6)))
        except ValueError:
            return None
    return None


def parse_date_from_text(text):
    if not text:
        return None
    m = RU_DATE_RE.search(text)
    if m:
        month = RU_MONTHS.get(m.group(2).lower())
        if month:
            try:
                return datetime(int(m.group(3)), month, int(m.group(1)),
                                int(m.group(4) or 0), int(m.group(5) or 0))
            except ValueError:
                return None
    return None


def extract_sub_category(url):
    parts = url.strip("/").split("/")
    for i, p in enumerate(parts):
        if p == "news" and i + 1 < len(parts):
            return parts[i + 1]
    return "unknown"


def fetch_listing_page(session, category, page):
    """Загрузить одну страницу листинга."""
    url = (f"{BASE_URL}/ru/news/{category}" if page == 1
           else f"{BASE_URL}/ru/news/{category}/page-{page}")
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return page, []
    except Exception:
        return page, []

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")
    except ImportError:
        soup = None
    
    if soup is None:
        # Fallback to selectolax
        tree = HTMLParser(resp.text)
        articles = []
        seen = set()
        for node in tree.css("a"):
            href = node.attributes.get("href", "")
            if ARTICLE_HREF_RE.search(href):
                full_url = urljoin(BASE_URL, href)
                if full_url not in seen:
                    seen.add(full_url)
                    articles.append({
                        "url": full_url, "title": "", "excerpt": "",
                        "category_label": "", "thumbnail": "",
                        "card_date": parse_date_from_url(full_url),
                    })
        return page, articles

    articles = []
    seen = set()

    # Главная карточка
    featured = soup.find("a", class_="image-news-card")
    if featured:
        href = featured.get("href", "")
        full_url = urljoin(BASE_URL, href)
        if ARTICLE_HREF_RE.search(href) and full_url not in seen:
            seen.add(full_url)
            articles.append({
                "url": full_url, "title": "", "excerpt": "",
                "category_label": "", "thumbnail": "",
                "card_date": parse_date_from_url(full_url),
            })

    # Стандартные карточки
    for card in soup.find_all("div", class_="b-news-list__item"):
        link = card.find("a", href=ARTICLE_HREF_RE)
        if link:
            href = link.get("href", "")
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen:
                seen.add(full_url)
                title_el = card.find("h3", class_="item-title")
                text_el = card.find("div", class_="item-text")
                cat_el = card.find("a", class_="category")
                img = card.find("img")
                pub_date = parse_date_from_url(full_url)
                if not pub_date:
                    date_div = card.find("div", class_="item-date")
                    if date_div:
                        date_span = date_div.find("span")
                        if date_span:
                            pub_date = parse_date_from_text(date_span.get_text(strip=True))
                articles.append({
                    "url": full_url,
                    "title": title_el.get_text(strip=True) if title_el else "",
                    "excerpt": text_el.get_text(strip=True) if text_el else "",
                    "category_label": cat_el.get_text(strip=True) if cat_el else "",
                    "thumbnail": urljoin(BASE_URL, img.get("src", "")) if img else "",
                    "card_date": pub_date,
                })

    # Sidebar cards
    for card_div in soup.find_all("div", class_="card"):
        link = card_div.find("a", href=ARTICLE_HREF_RE)
        if link:
            href = link.get("href", "")
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen:
                seen.add(full_url)
                articles.append({
                    "url": full_url, "title": "", "excerpt": "",
                    "category_label": "", "thumbnail": "",
                    "card_date": parse_date_from_url(full_url),
                })

    return page, articles


def stage_collect_urls(resume=False):
    """Этап 1: Собрать все URL."""
    global _shutdown
    progress = load_progress() if resume else {"urls": {}, "download_offset": 0, "imported": 0}

    # Загрузить уже известные URL
    seen_urls = set()
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    try:
                        seen_urls.add(json.loads(line)["url"])
                    except (json.JSONDecodeError, KeyError):
                        continue

    # Также из БД
    try:
        conn = sqlite3.connect(DB_PATH)
        for row in conn.execute("SELECT url FROM articles").fetchall():
            seen_urls.add(row[0])
        conn.close()
    except Exception:
        pass

    log(f"ЭТАП 1: Сбор URL | Известно: {len(seen_urls):,}")

    total_new = 0
    session = create_session()

    for cat, max_pages in CATEGORIES.items():
        if _shutdown:
            break

        # Определяем начальную страницу
        cat_progress = progress.get("urls", {}).get(cat)
        if resume and cat_progress == "done":
            log(f"  {cat}: уже завершён, пропускаю")
            continue
        start = cat_progress if (resume and isinstance(cat_progress, int)) else 1

        log(f"  {cat}: стр {start} → {max_pages}")

        page = start
        batch_size = 5
        consecutive_empty = 0
        consecutive_errors = 0
        cat_new = 0
        last_save = time.time()

        while page <= max_pages and not _shutdown:
            pages_to_fetch = list(range(page, min(page + batch_size, max_pages + 1)))
            batch_results = {}

            try:
                with ThreadPoolExecutor(max_workers=batch_size) as executor:
                    futures = {
                        executor.submit(fetch_listing_page, session, cat, p): p
                        for p in pages_to_fetch
                    }
                    for future in as_completed(futures, timeout=60):
                        try:
                            p, arts = future.result(timeout=30)
                            batch_results[p] = arts
                        except Exception:
                            pass
            except (FuturesTimeout, TimeoutError):
                consecutive_errors += 1
                if consecutive_errors >= 5:
                    time.sleep(30)
                    session.close()
                    session = create_session()
                    consecutive_errors = 0
                else:
                    time.sleep(3)
                continue

            if batch_results:
                consecutive_errors = 0

            batch_records = []
            for p in pages_to_fetch:
                articles = batch_results.get(p, [])
                if not articles:
                    consecutive_empty += 1
                    if consecutive_empty >= 15:
                        log(f"    15 пустых страниц подряд — конец {cat} на стр {p}")
                        page = max_pages + 1
                        break
                    continue
                else:
                    consecutive_empty = 0

                for art in articles:
                    if art["url"] in seen_urls:
                        continue
                    seen_urls.add(art["url"])
                    pub_date = art.get("card_date") or parse_date_from_url(art["url"])
                    batch_records.append({
                        "url": art["url"],
                        "pub_date": pub_date.isoformat() if pub_date else None,
                        "sub_category": extract_sub_category(art["url"]),
                        "category_label": art.get("category_label", ""),
                        "title": art.get("title", ""),
                        "excerpt": art.get("excerpt", ""),
                        "thumbnail": art.get("thumbnail", ""),
                    })

            if batch_records:
                with open(URLS_FILE, "a") as f:
                    for rec in batch_records:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                cat_new += len(batch_records)
                total_new += len(batch_records)

            # Прогресс каждые 50 страниц
            if page % 50 == 0 or page == start:
                pct = page / max_pages * 100
                log(f"    {cat} p{page}/{max_pages} ({pct:.0f}%) | +{cat_new} new | total: {len(seen_urls):,}")

            # Сохраняем прогресс каждые 60с
            if time.time() - last_save > 60:
                progress["urls"][cat] = page + batch_size
                save_progress(progress)
                last_save = time.time()

            page += batch_size
            time.sleep(0.15)

        # Категория завершена
        if not _shutdown:
            progress["urls"][cat] = "done"
        else:
            progress["urls"][cat] = page
        save_progress(progress)
        log(f"  {cat}: +{cat_new} URL")
        session.close()
        session = create_session()

    session.close()

    # Финальный подсчёт
    total_urls = 0
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            total_urls = sum(1 for _ in f)

    log(f"ЭТАП 1 {'ПРЕРВАН' if _shutdown else 'ЗАВЕРШЁН'}: +{total_new} новых | всего URL: {total_urls:,}")
    return total_urls


# ══════════════════════════════════════════════
#  ЭТАП 2: СКАЧИВАНИЕ КОНТЕНТА
# ══════════════════════════════════════════════

def make_full_url(src):
    if not src:
        return ""
    return src if src.startswith("http") else BASE_URL + src


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
        main_image = make_full_url(img_el.attributes.get("src", "")) if img_el else ""
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


async def stage_download(workers=15, resume=False):
    """Этап 2: Скачать контент всех URL."""
    global _shutdown
    progress = load_progress()

    # Все URL
    urls = []
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    try:
                        urls.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

    # Также URL из БД, которых нет в файле
    url_set = {u["url"] for u in urls}
    try:
        conn = sqlite3.connect(DB_PATH)
        for row in conn.execute("SELECT url, pub_date, sub_category FROM articles").fetchall():
            if row[0] not in url_set:
                urls.append({"url": row[0], "pub_date": row[1], "sub_category": row[2] or ""})
                url_set.add(row[0])
        conn.close()
    except Exception:
        pass

    # Уже скачанные
    existing = set()
    if ARTICLES_FILE.exists():
        with open(ARTICLES_FILE) as f:
            for line in f:
                if line.strip():
                    try:
                        existing.add(json.loads(line).get("url"))
                    except json.JSONDecodeError:
                        continue

    # Также в БД (с body_text)
    try:
        conn = sqlite3.connect(DB_PATH)
        for row in conn.execute("SELECT url FROM articles WHERE body_text IS NOT NULL AND body_text != ''").fetchall():
            existing.add(row[0])
        conn.close()
    except Exception:
        pass

    to_download = [u for u in urls if u["url"] not in existing]
    log(f"ЭТАП 2: Скачивание | Всего URL: {len(urls):,} | Уже есть: {len(existing):,} | Осталось: {len(to_download):,}")

    if not to_download:
        log("Все статьи уже скачаны.")
        return

    downloaded = 0
    errors = 0
    start_time = time.time()
    semaphore = asyncio.Semaphore(workers)

    transport = httpx.AsyncHTTPTransport(
        retries=2,
        limits=httpx.Limits(max_connections=workers + 5, max_keepalive_connections=workers),
    )

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0",
            "Accept-Language": "ru-RU,ru;q=0.9",
        },
        follow_redirects=True,
        transport=transport,
    ) as client:
        batch_size = 100
        for i in range(0, len(to_download), batch_size):
            if _shutdown:
                break

            batch = to_download[i:i + batch_size]
            tasks = [download_article(client, u, semaphore) for u in batch]

            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=300,
                )
            except asyncio.TimeoutError:
                errors += len(batch)
                continue

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
            total_done = len(existing) + downloaded
            pct = total_done / len(urls) * 100 if urls else 0

            if (i // batch_size) % 5 == 0 or i + batch_size >= len(to_download):
                log(f"  {i + len(batch):,}/{len(to_download):,} | "
                    f"ok: {downloaded:,}, err: {errors:,} | "
                    f"{rate:.1f} ст/с | "
                    f"всего: {total_done:,}/{len(urls):,} ({pct:.1f}%)")

            # Сохраняем прогресс
            progress["download_offset"] = i + batch_size
            if (i // batch_size) % 20 == 0:
                save_progress(progress)

    save_progress(progress)
    log(f"ЭТАП 2 {'ПРЕРВАН' if _shutdown else 'ЗАВЕРШЁН'}: +{downloaded:,} скачано, {errors:,} ошибок")


# ══════════════════════════════════════════════
#  ЭТАП 3: ИМПОРТ В БД + MEILISEARCH
# ══════════════════════════════════════════════

def stage_import():
    """Этап 3: Импорт articles.jsonl → SQLite + Meilisearch."""
    log("ЭТАП 3: Импорт в БД")

    if not ARTICLES_FILE.exists():
        log("Файл articles_full.jsonl не найден.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    imported = 0
    skipped = 0
    errors = 0
    batch = []

    with open(ARTICLES_FILE) as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                art = json.loads(line)
            except json.JSONDecodeError:
                errors += 1
                continue

            if not art.get("body_text"):
                skipped += 1
                continue

            tags_json = json.dumps(art.get("tags", []), ensure_ascii=False)
            images_json = json.dumps(art.get("inline_images", []), ensure_ascii=False)

            batch.append((
                art["url"],
                art.get("pub_date"),
                art.get("sub_category", ""),
                art.get("category_label", ""),
                art.get("title", ""),
                art.get("author", ""),
                art.get("excerpt", ""),
                art.get("body_text", ""),
                art.get("body_html", ""),
                art.get("main_image", ""),
                art.get("image_credit", ""),
                art.get("thumbnail", ""),
                tags_json,
                images_json,
            ))

            if len(batch) >= 500:
                imported += _insert_batch(conn, batch)
                batch.clear()
                if line_num % 5000 == 0:
                    log(f"  Импортировано: {imported:,} из {line_num:,} строк")

    if batch:
        imported += _insert_batch(conn, batch)

    conn.close()

    # Обновляем Meilisearch
    log(f"Импорт завершён: {imported:,} статей, {skipped:,} пропущено, {errors:,} ошибок")
    log("Запускаю переиндексацию Meilisearch...")

    try:
        # Используем тот же standalone reindex
        from scraper.reindex_meilisearch import setup_index, reindex_all
        setup_index()
        reindex_all()
    except Exception as e:
        log(f"⚠ Meilisearch переиндексация: {e}")
        log("  Запустите вручную: python -m scraper.reindex_meilisearch")


def _insert_batch(conn, batch):
    """Вставить пачку статей с UPSERT."""
    count = 0
    for row in batch:
        try:
            conn.execute("""
                INSERT INTO articles
                (url, pub_date, sub_category, category_label, title, author,
                 excerpt, body_text, body_html, main_image, image_credit,
                 thumbnail, tags, inline_images)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    pub_date = COALESCE(excluded.pub_date, articles.pub_date),
                    title = CASE WHEN excluded.title != '' THEN excluded.title ELSE articles.title END,
                    author = CASE WHEN excluded.author != '' THEN excluded.author ELSE articles.author END,
                    excerpt = CASE WHEN excluded.excerpt != '' THEN excluded.excerpt ELSE articles.excerpt END,
                    body_text = CASE WHEN excluded.body_text != '' THEN excluded.body_text ELSE articles.body_text END,
                    body_html = CASE WHEN excluded.body_html != '' THEN excluded.body_html ELSE articles.body_html END,
                    main_image = CASE WHEN excluded.main_image != '' THEN excluded.main_image ELSE articles.main_image END,
                    image_credit = excluded.image_credit,
                    thumbnail = CASE WHEN excluded.thumbnail != '' THEN excluded.thumbnail ELSE articles.thumbnail END,
                    tags = excluded.tags,
                    inline_images = excluded.inline_images,
                    imported_at = datetime('now')
            """, row)
            count += 1
        except Exception:
            pass
    conn.commit()
    return count


# ══════════════════════════════════════════════
#  СТАТУС
# ══════════════════════════════════════════════

def show_status():
    """Показать текущий статус пайплайна."""
    progress = load_progress()

    print("=" * 60)
    print("  СТАТУС ПАЙПЛАЙНА СБОРА total.kz")
    print("=" * 60)

    # URLs
    url_count = 0
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            url_count = sum(1 for _ in f)
    print(f"\n  URL собрано: {url_count:,}")
    url_progress = progress.get("urls", {})
    for cat, max_p in CATEGORIES.items():
        status = url_progress.get(cat, "не начато")
        if status == "done":
            print(f"    {cat}: ✅ завершён")
        elif isinstance(status, int):
            pct = status / max_p * 100
            print(f"    {cat}: стр {status}/{max_p} ({pct:.0f}%)")
        else:
            print(f"    {cat}: ⏳ {status}")

    # Articles
    art_count = 0
    if ARTICLES_FILE.exists():
        with open(ARTICLES_FILE) as f:
            art_count = sum(1 for _ in f)
    print(f"\n  Статей скачано: {art_count:,}")
    if url_count > 0:
        print(f"  Прогресс скачивания: {art_count:,}/{url_count:,} ({art_count/url_count*100:.1f}%)")

    # DB
    try:
        conn = sqlite3.connect(DB_PATH)
        db_count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        db_with_body = conn.execute("SELECT COUNT(*) FROM articles WHERE body_text IS NOT NULL AND body_text != ''").fetchone()[0]
        conn.close()
        print(f"\n  В базе данных: {db_count:,} статей ({db_with_body:,} с текстом)")
    except Exception:
        print(f"\n  БД: недоступна")

    print(f"\n  Цель: ~{TOTAL_EXPECTED:,} статей")
    print("=" * 60)


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Полный пайплайн сбора total.kz")
    parser.add_argument("--stage", choices=["urls", "download", "import", "all"], default="all",
                        help="Какой этап запустить (default: all)")
    parser.add_argument("--workers", type=int, default=15, help="Параллельных загрузок (этап 2)")
    parser.add_argument("--resume", action="store_true", help="Продолжить с прогресса")
    parser.add_argument("--status", action="store_true", help="Показать статус")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.status:
        show_status()
        return

    start = time.time()
    log(f"{'='*60}")
    log(f"  ПАЙПЛАЙН СБОРА total.kz | этап: {args.stage} | resume: {args.resume}")
    log(f"  DB: {DB_PATH}")
    log(f"{'='*60}")

    if args.stage in ("urls", "all"):
        stage_collect_urls(resume=args.resume)

    if args.stage in ("download", "all") and not _shutdown:
        asyncio.run(stage_download(workers=args.workers, resume=args.resume))

    if args.stage in ("import", "all") and not _shutdown:
        stage_import()

    elapsed = time.time() - start
    hours = int(elapsed // 3600)
    mins = int((elapsed % 3600) // 60)
    log(f"\n{'='*60}")
    log(f"  ПАЙПЛАЙН {'ПРЕРВАН' if _shutdown else 'ЗАВЕРШЁН'} за {hours}ч {mins}м")
    show_status()


if __name__ == "__main__":
    main()
