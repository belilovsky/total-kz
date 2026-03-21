#!/usr/bin/env python3
"""
Полный сбор URL всех статей total.kz (включая архив 2011-2017).

Ключевое отличие от scrape_urls.py:
- Использует ОСНОВНЫЕ категории (obshchestvo, politika, ekonomika, drugoe) –
  у них полная пагинация до 2011 года
- Проходит ВСЕ страницы до конца, не останавливаясь на all-known
- Старые статьи не имеют _date_ в URL – дата будет получена при download_content

Запуск:
    python scraper/scrape_full_archive.py                    # собрать весь архив
    python scraper/scrape_full_archive.py --category politika  # одна категория
    python scraper/scrape_full_archive.py --start-page 5000    # начать с конкретной страницы
    python scraper/scrape_full_archive.py --resume             # продолжить с сохранённого прогресса
"""
import re
import json
import time
import sys
import argparse
import signal
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
URLS_FILE = DATA_DIR / "urls.jsonl"
PROGRESS_FILE = DATA_DIR / "archive_progress.json"

sys.path.insert(0, str(BASE_DIR))
from app.database import get_db, init_db

BASE_URL = "https://total.kz"

# Основные категории – содержат ВСЮ историю публикаций с 2011
MAIN_CATEGORIES = {
    "obshchestvo": 8504,
    "politika": 4463,
    "ekonomika": 4605,
    "drugoe": 3205,
    "media": 554,
    "special": 8,
}

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
    print("\n  ⚠ Получен сигнал завершения, сохраняю прогресс...", flush=True)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def create_session():
    """Создать сессию с retry и таймаутами."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ru-RU,ru;q=0.9",
    })
    return session


def parse_date_from_url(url):
    """Извлечь дату из URL (только для новых статей с _date_)."""
    m = DATE_RE.search(url)
    if m:
        try:
            return datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)), int(m.group(6))
            )
        except ValueError:
            return None
    return None


def parse_date_from_text(text):
    """Парсить русскую дату с карточки листинга."""
    if not text:
        return None
    m = RU_DATE_RE.search(text)
    if m:
        day = int(m.group(1))
        month = RU_MONTHS.get(m.group(2).lower())
        year = int(m.group(3))
        hour = int(m.group(4)) if m.group(4) else 0
        minute = int(m.group(5)) if m.group(5) else 0
        if month:
            try:
                return datetime(year, month, day, hour, minute)
            except ValueError:
                return None
    return None


def extract_sub_category(url):
    """Извлечь подкатегорию из URL."""
    parts = url.strip("/").split("/")
    for i, p in enumerate(parts):
        if p == "news" and i + 1 < len(parts):
            return parts[i + 1]
    return "unknown"


def fetch_listing_page(session, category, page):
    """Загрузить одну страницу листинга."""
    url = (
        f"{BASE_URL}/ru/news/{category}"
        if page == 1
        else f"{BASE_URL}/ru/news/{category}/page-{page}"
    )
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return page, []
    except Exception:
        return page, []

    soup = BeautifulSoup(resp.text, "lxml")
    articles = []
    seen = set()

    # Главная карточка
    featured = soup.find("a", class_="image-news-card")
    if featured:
        href = featured.get("href", "")
        full_url = urljoin(BASE_URL, href)
        if ARTICLE_HREF_RE.search(href) and full_url not in seen:
            seen.add(full_url)
            pub_date = parse_date_from_url(full_url)
            articles.append({
                "url": full_url,
                "title": "",
                "excerpt": "",
                "category_label": "",
                "thumbnail": "",
                "card_date": pub_date,
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

    # Боковые карточки (sidebar)
    for card_div in soup.find_all("div", class_="card"):
        link = card_div.find("a", href=ARTICLE_HREF_RE)
        if link:
            href = link.get("href", "")
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen:
                seen.add(full_url)
                pub_date = parse_date_from_url(full_url)
                articles.append({
                    "url": full_url,
                    "title": "",
                    "excerpt": "",
                    "category_label": "",
                    "thumbnail": "",
                    "card_date": pub_date,
                })

    return page, articles


def load_progress():
    """Загрузить прогресс из файла."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {}


def save_progress(progress):
    """Сохранить прогресс."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def scrape_category_full(category, max_pages, start_page, seen_urls):
    """Собрать ВСЕ URL одной категории, от start_page до max_pages."""
    global _shutdown
    session = create_session()
    progress = load_progress()

    print(f"\n{'='*60}")
    print(f"  {category} | стр {start_page} → {max_pages} | known: {len(seen_urls)}")
    print(f"{'='*60}")

    all_new = []
    page = start_page
    batch_size = 5
    consecutive_errors = 0
    consecutive_empty = 0
    last_heartbeat = time.time()
    last_save_time = time.time()

    while page <= max_pages and not _shutdown:
        # Heartbeat
        now = time.time()
        if now - last_heartbeat > 30:
            print(f"  ♥ p{page}/{max_pages}, new={len(all_new)}", flush=True)
            last_heartbeat = now

        # Загружаем пачку страниц
        batch_results = {}
        pages_to_fetch = list(range(page, min(page + batch_size, max_pages + 1)))

        try:
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = {
                    executor.submit(fetch_listing_page, session, category, p): p
                    for p in pages_to_fetch
                }
                for future in as_completed(futures, timeout=45):
                    try:
                        p, arts = future.result(timeout=30)
                        batch_results[p] = arts
                    except Exception as e:
                        p = futures[future]
                        print(f"  ⚠ Ошибка стр {p}: {type(e).__name__}", flush=True)
        except (FuturesTimeout, TimeoutError):
            consecutive_errors += 1
            print(f"  ⚠ Таймаут пачки p{page} (ошибок: {consecutive_errors})", flush=True)
            if consecutive_errors >= 5:
                print(f"  → Пауза 30 сек, пересоздаю сессию", flush=True)
                time.sleep(30)
                session.close()
                session = create_session()
                consecutive_errors = 0
            else:
                time.sleep(3)
            continue

        if batch_results:
            consecutive_errors = 0

        # Обрабатываем результаты
        batch_records = []
        for p in pages_to_fetch:
            articles = batch_results.get(p, [])

            if not articles:
                consecutive_empty += 1
                if consecutive_empty >= 10:
                    print(f"  → 10 пустых страниц подряд – конец категории на стр {p}", flush=True)
                    page = max_pages + 1  # exit loop
                    break
                continue
            else:
                consecutive_empty = 0

            new_count = 0
            already_known = 0

            for art in articles:
                if art["url"] in seen_urls:
                    already_known += 1
                    continue

                seen_urls.add(art["url"])
                pub_date = art.get("card_date") or parse_date_from_url(art["url"])

                record = {
                    "url": art["url"],
                    "pub_date": pub_date.isoformat() if pub_date else None,
                    "sub_category": extract_sub_category(art["url"]),
                    "category_label": art.get("category_label", ""),
                    "title": art.get("title", ""),
                    "excerpt": art.get("excerpt", ""),
                    "thumbnail": art.get("thumbnail", ""),
                }
                batch_records.append(record)
                new_count += 1

            # Прогресс – показываем каждые 100 страниц или если есть новые
            if p % 100 == 0 or new_count > 0:
                pct = (p / max_pages * 100) if max_pages > 0 else 0
                print(f"  p{p:>5}/{max_pages} ({pct:.1f}%) | +{new_count} new, {already_known} known | total new: {len(all_new) + len(batch_records)}", flush=True)

        # Записываем новые URL
        if batch_records:
            with open(URLS_FILE, "a") as f:
                for record in batch_records:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            all_new.extend(batch_records)

        # Сохраняем прогресс каждые 60 секунд
        now = time.time()
        if now - last_save_time > 60:
            progress[category] = page + len(pages_to_fetch)
            save_progress(progress)
            last_save_time = now

        page += batch_size
        time.sleep(0.2)

    # Финальное сохранение прогресса
    if _shutdown:
        progress[category] = page
        save_progress(progress)
        print(f"  → Прервано на стр {page}, прогресс сохранён", flush=True)
    else:
        progress[category] = "done"
        save_progress(progress)

    session.close()
    print(f"  → {category}: {len(all_new)} новых URL")
    return len(all_new)


def main():
    parser = argparse.ArgumentParser(description="Полный сбор архива total.kz")
    parser.add_argument("--category", type=str, help="Одна конкретная категория")
    parser.add_argument("--start-page", type=int, default=1, help="Начать с конкретной страницы")
    parser.add_argument("--resume", action="store_true", help="Продолжить с сохранённого прогресса")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()

    # Загружаем known URLs
    seen_urls = set()
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    try:
                        seen_urls.add(json.loads(line)["url"])
                    except (json.JSONDecodeError, KeyError):
                        continue

    with get_db() as conn:
        rows = conn.execute("SELECT url FROM articles").fetchall()
        for row in rows:
            seen_urls.add(row[0])

    print(f"Известно URL: {len(seen_urls)}")

    # Прогресс
    progress = load_progress() if args.resume else {}

    # Определяем категории
    if args.category:
        if args.category not in MAIN_CATEGORIES:
            print(f"⚠ Категория '{args.category}' не найдена.")
            print(f"Доступные: {', '.join(MAIN_CATEGORIES.keys())}")
            sys.exit(1)
        categories = {args.category: MAIN_CATEGORIES[args.category]}
    else:
        categories = MAIN_CATEGORIES

    total_new = 0
    for cat, max_pages in categories.items():
        if _shutdown:
            break

        # Определяем начальную страницу
        if args.resume and cat in progress:
            if progress[cat] == "done":
                print(f"\n  {cat}: уже завершён, пропускаю")
                continue
            start = progress[cat]
            print(f"\n  {cat}: возобновляю со стр {start}")
        elif args.start_page > 1:
            start = args.start_page
        else:
            start = 1

        count = scrape_category_full(cat, max_pages, start, seen_urls)
        total_new += count

    # Итоговая статистика
    total_urls = 0
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            total_urls = sum(1 for _ in f)

    print(f"\n{'='*60}")
    print(f"  {'ПРЕРВАНО' if _shutdown else 'ГОТОВО'}: {total_new} новых URL")
    print(f"  Всего в urls.jsonl: {total_urls}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
