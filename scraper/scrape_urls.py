#!/usr/bin/env python3
"""
Сбор URL статей с total.kz.
Надёжная версия: retry, таймауты, graceful recovery.

Запуск:
    python scraper/scrape_urls.py                    # собрать за последний год
    python scraper/scrape_urls.py --days 30          # собрать за последние 30 дней
    python scraper/scrape_urls.py --since 2024-01-01 # собрать с конкретной даты
"""
import re
import json
import time
import sys
import argparse
import signal
from datetime import datetime, timedelta
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

sys.path.insert(0, str(BASE_DIR))
from app.database import get_db, init_db

BASE_URL = "https://total.kz"
SUBCATEGORIES = [
    "vnutrennyaya_politika",
    "vneshnyaya_politika",
    "mir",
    "bezopasnost",
    "mneniya",
    "ekonomika_sobitiya",
    "biznes",
    "finansi",
    "gossektor",
    "tehno",
    "obshchestvo_sobitiya",
    "proisshestviya",
    "zhizn",
    "kultura",
    "religiya",
    "den_v_istorii",
    "sport",
    "nauka",
    "stil_zhizni",
    "redaktsiya_tandau",
]
DATE_RE = re.compile(r'_date_(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})')

RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}
RU_DATE_RE = re.compile(r'(\d{1,2})\s+(\S+)\s+(\d{4})(?:,\s*(\d{1,2}):(\d{2}))?')
ARTICLE_HREF_RE = re.compile(r'/ru/news/[^/]+/(?!page-)[^/]+')

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
    """Извлечь дату публикации из URL."""
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
    """Парсить русскую дату."""
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
    """Загрузить одну страницу листинга. Таймаут 15 сек, retry встроен в сессию."""
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
            articles.append({"url": full_url, "title": "", "excerpt": "", "category_label": "", "thumbnail": "", "card_date": pub_date})

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

    # Боковые карточки
    for card_div in soup.find_all("div", class_="card"):
        link = card_div.find("a", href=ARTICLE_HREF_RE)
        if link:
            href = link.get("href", "")
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen:
                seen.add(full_url)
                pub_date = parse_date_from_url(full_url)
                articles.append({"url": full_url, "title": "", "excerpt": "", "category_label": "", "thumbnail": "", "card_date": pub_date})

    return page, articles


def get_oldest_date_on_page(session, category, page):
    """Получить самую старую дату на странице."""
    _, articles = fetch_listing_page(session, category, page)
    dates = []
    for art in articles:
        d = art.get("card_date") or parse_date_from_url(art["url"])
        if d:
            dates.append(d)
    return min(dates) if dates else None


def find_start_page(session, category, target_date, seen_urls):
    """Бинарный поиск стартовой страницы."""
    _, arts = fetch_listing_page(session, category, 1)
    has_new = any(a["url"] not in seen_urls for a in arts)
    if has_new:
        return 1

    oldest_50 = get_oldest_date_on_page(session, category, 50)
    if not oldest_50:
        return 1
    if oldest_50 <= target_date:
        return 1

    now = datetime.now()
    days_per_page = (now - oldest_50).days / 50.0
    if days_per_page <= 0:
        return 1

    days_to_target = (now - target_date).days
    estimated_page = int(days_to_target / days_per_page)

    lo = 50
    hi = min(int(estimated_page * 1.5) + 100, 10000)

    print(f"  → Быстрый поиск: ~{days_per_page:.1f} дн/стр, оценка: стр {estimated_page}, ищем в [{lo}..{hi}]")

    oldest_hi = get_oldest_date_on_page(session, category, hi)
    if oldest_hi and oldest_hi > target_date:
        hi = min(hi * 2, 10000)

    best_page = lo
    for _ in range(15):
        if hi - lo < 10:
            break
        mid = (lo + hi) // 2
        oldest_mid = get_oldest_date_on_page(session, category, mid)
        if not oldest_mid:
            hi = mid
            continue
        if oldest_mid > target_date:
            lo = mid
            best_page = mid
        else:
            hi = mid

    start = max(1, best_page - 20)
    print(f"  → Начинаем со страницы {start} (пропущено {start - 1} страниц)")
    return start


def scrape_category(category, cutoff_date, seen_urls, force=False):
    """Собрать все URL одной категории."""
    global _shutdown
    session = create_session()

    print(f"\n{'='*60}")
    print(f"  {category} | cutoff: {cutoff_date.strftime('%Y-%m-%d')}{' [FORCE]' if force else ''}")
    print(f"{'='*60}")

    # Определяем oldest known
    known_dates = []
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    rec = json.loads(line)
                    url = rec.get("url", "")
                    if f"/news/{category}/" in url:
                        d = parse_date_from_url(url)
                        if d:
                            known_dates.append(d)

    with get_db() as conn:
        rows = conn.execute("SELECT url FROM articles").fetchall()
        for row in rows:
            url = row[0]
            if f"/news/{category}/" in url:
                d = parse_date_from_url(url)
                if d:
                    known_dates.append(d)

    if known_dates:
        oldest_known = min(known_dates)
        newest_known = max(known_dates)
        print(f"  Известно: {len(known_dates)} URL, от {oldest_known.strftime('%Y-%m-%d')} до {newest_known.strftime('%Y-%m-%d')}")

        if not force and oldest_known <= cutoff_date:
            from collections import Counter
            month_counts = Counter(d.strftime('%Y-%m') for d in known_dates)
            sparse_months = [
                m for m in month_counts
                if m >= cutoff_date.strftime('%Y-%m') and month_counts[m] < 50
            ]
            if sparse_months:
                print(f"  ⚠ Месяцы с неполным покрытием: {', '.join(sorted(sparse_months)[:6])}...")
                print(f"  → Пересобираем для заполнения пробелов")
                earliest_sparse = datetime.strptime(min(sparse_months), '%Y-%m')
                start_page = find_start_page(session, category, earliest_sparse, seen_urls)
            else:
                print(f"  → Уже собрано до {oldest_known.strftime('%Y-%m-%d')} – пропускаем")
                return 0
        elif force:
            print(f"  → Принудительный пересбор – начинаем с page 1")
            start_page = 1
        else:
            start_page = find_start_page(session, category, oldest_known, seen_urls)
    else:
        start_page = 1
        print(f"  Нет известных URL – начинаем с начала")

    all_new = []
    page = start_page
    done = False
    batch_size = 5
    consecutive_past_cutoff = 0
    consecutive_all_known = 0
    prev_oldest_date = None
    stale_date_count = 0
    consecutive_errors = 0
    last_heartbeat = time.time()

    while not done and not _shutdown:
        # Heartbeat каждые 60 секунд
        now = time.time()
        if now - last_heartbeat > 60:
            print(f"  ♥ heartbeat: p{page}, new={len(all_new)}, elapsed={int(now - last_heartbeat)}s", flush=True)
            last_heartbeat = now

        # Загружаем пачку страниц параллельно с жёстким таймаутом
        batch_results = {}
        try:
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = {
                    executor.submit(fetch_listing_page, session, category, p): p
                    for p in range(page, page + batch_size)
                }
                for future in as_completed(futures, timeout=45):
                    try:
                        p, arts = future.result(timeout=30)
                        batch_results[p] = arts
                    except Exception as e:
                        p = futures[future]
                        print(f"  ⚠ Ошибка стр {p}: {type(e).__name__}: {e}", flush=True)
        except (FuturesTimeout, TimeoutError):
            consecutive_errors += 1
            print(f"  ⚠ Таймаут пачки p{page}-{page+batch_size-1} (ошибок подряд: {consecutive_errors})", flush=True)
            if consecutive_errors >= 5:
                print(f"  ❌ 5 таймаутов подряд – пауза 30 сек", flush=True)
                time.sleep(30)
                # Пересоздаём сессию
                session.close()
                session = create_session()
                consecutive_errors = 0
            else:
                time.sleep(3)
            # Не переходим к следующей пачке – повторяем
            continue

        # Если пачка вернула хотя бы что-то – сбрасываем ошибки
        if batch_results:
            consecutive_errors = 0

        # Обрабатываем по порядку
        batch_records = []
        for p in range(page, page + batch_size):
            articles = batch_results.get(p, [])

            if not articles:
                done = True
                break

            new_count = 0
            old_count = 0
            already_known = 0
            all_page_dates = []

            for art in articles:
                pub_date = art.get("card_date") or parse_date_from_url(art["url"])
                if pub_date:
                    all_page_dates.append(pub_date)

                if art["url"] in seen_urls:
                    already_known += 1
                    continue

                if pub_date and pub_date < cutoff_date:
                    old_count += 1
                    continue

                seen_urls.add(art["url"])
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

            # Проверки cutoff
            content_dates = all_page_dates  # используем все даты для cutoff-логики
            if content_dates and all(d < cutoff_date for d in content_dates):
                consecutive_past_cutoff += 1
            elif new_count == 0 and old_count > 0:
                consecutive_past_cutoff += 1
            elif new_count == 0 and old_count == 0 and already_known > 0:
                if all_page_dates and all(d < cutoff_date for d in all_page_dates):
                    consecutive_past_cutoff += 1
            else:
                consecutive_past_cutoff = 0

            # Зацикливание
            oldest_date = min(all_page_dates).strftime("%Y-%m-%d") if all_page_dates else None
            if oldest_date and oldest_date == prev_oldest_date:
                stale_date_count += 1
            else:
                stale_date_count = 0
                prev_oldest_date = oldest_date

            # All-known counter
            if new_count == 0 and old_count == 0 and already_known > 0:
                consecutive_all_known += 1
            else:
                consecutive_all_known = 0

            # Прогресс
            if p <= start_page + 4 or p % 50 == 0 or new_count > 0:
                print(f"  p{p:>4}: +{new_count} new, {already_known} known, {old_count} old | oldest: {oldest_date or '?'} | total: {len(all_new) + len(batch_records)}", flush=True)

            # Стоп: cutoff
            if consecutive_past_cutoff >= 10:
                print(f"  → Дошли до cutoff на стр {p}", flush=True)
                done = True
                break

            # Стоп: all-known
            if not force and consecutive_all_known >= 30:
                jump_page = p + 200
                print(f"  → 30 стр all-known, прыгаем на {jump_page}...", flush=True)
                _, jump_arts = fetch_listing_page(session, category, jump_page)
                jump_new = sum(1 for a in jump_arts if a["url"] not in seen_urls)
                if jump_new == 0:
                    print(f"  → После прыжка тоже 0 new – заканчиваем", flush=True)
                    done = True
                    break
                else:
                    print(f"  → После прыжка +{jump_new} new – продолжаем с {jump_page}", flush=True)
                    page = jump_page
                    consecutive_all_known = 0
                    continue
            elif force and consecutive_all_known >= 50 and consecutive_all_known % 50 == 0:
                print(f"  → {consecutive_all_known} стр all-known (force, продолжаем)...", flush=True)

            # Стоп: зацикливание
            if stale_date_count >= 50:
                print(f"  → Зацикливание: {oldest_date} не меняется 50 стр – стоп", flush=True)
                done = True
                break

        # Записываем
        if batch_records:
            with open(URLS_FILE, "a") as f:
                for record in batch_records:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            all_new.extend(batch_records)

        page += batch_size
        time.sleep(0.2)

        if page > 15000:
            print(f"  → Лимит 15000 страниц", flush=True)
            done = True

    if _shutdown:
        print(f"  → Прервано сигналом, сохранено {len(all_new)} URL", flush=True)

    session.close()
    print(f"  → {category}: {len(all_new)} новых URL")
    return len(all_new)


def main():
    parser = argparse.ArgumentParser(description="Сбор URL статей с total.kz")
    parser.add_argument("--days", type=int, default=365, help="За сколько дней (по умолчанию 365)")
    parser.add_argument("--since", type=str, help="С какой даты (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Принудительный пересбор")
    parser.add_argument("--resume-from", type=str, help="Продолжить с конкретной категории (slug)")
    args = parser.parse_args()

    if args.since:
        cutoff_date = datetime.strptime(args.since, "%Y-%m-%d")
    else:
        cutoff_date = datetime.now() - timedelta(days=args.days)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()

    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_runs (started_at, phase, status) VALUES (?, 'urls', 'running')",
            (datetime.now().isoformat(),)
        )
        run_id = cursor.lastrowid

    # Загружаем known URLs
    seen_urls = set()
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    seen_urls.add(json.loads(line)["url"])

    with get_db() as conn:
        rows = conn.execute("SELECT url FROM articles").fetchall()
        for row in rows:
            seen_urls.add(row[0])

    print(f"Известно URL: {len(seen_urls)}")
    print(f"Cutoff: {cutoff_date.strftime('%Y-%m-%d')}")

    # Поддержка --resume-from
    cats = SUBCATEGORIES
    if args.resume_from:
        if args.resume_from in cats:
            idx = cats.index(args.resume_from)
            cats = cats[idx:]
            print(f"Продолжаем с категории: {args.resume_from} (индекс {idx})")
        else:
            print(f"⚠ Категория '{args.resume_from}' не найдена, начинаем сначала")

    total_new = 0
    for cat in cats:
        if _shutdown:
            break
        count = scrape_category(cat, cutoff_date, seen_urls, force=args.force)
        total_new += count

    # Обновляем статус
    total_urls = sum(1 for _ in open(URLS_FILE)) if URLS_FILE.exists() else 0
    status = 'completed' if not _shutdown else 'interrupted'
    with get_db() as conn:
        conn.execute(
            "UPDATE scrape_runs SET finished_at=?, status=?, articles_found=? WHERE id=?",
            (datetime.now().isoformat(), status, total_new, run_id)
        )

    print(f"\n{'='*60}")
    print(f"  {'ПРЕРВАНО' if _shutdown else 'ГОТОВО'}: {total_new} новых URL (всего: {total_urls})")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
