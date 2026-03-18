#!/usr/bin/env python3
"""
Сбор URL статей с total.kz.
Использует многопоточные запросы, пагинация по категориям.
Автоматически определяет, какие статьи уже есть в базе, и собирает только новые.

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
from datetime import datetime, timedelta
from urllib.parse import urljoin
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
URLS_FILE = DATA_DIR / "urls.jsonl"

sys.path.insert(0, str(BASE_DIR))
from app.database import get_db, init_db

BASE_URL = "https://total.kz"
# 20 subcategories — обходим каждую напрямую для полного покрытия
# (Старые parent: politika, ekonomika, obshchestvo, drugoe, media, special — агрегировали подкатегории,
#  из-за чего пропускались статьи в глубокой пагинации)
SUBCATEGORIES = [
    # Политика
    "vnutrennyaya_politika",
    "vneshnyaya_politika",
    "mir",
    "bezopasnost",
    "mneniya",
    # Экономика
    "ekonomika_sobitiya",
    "biznes",
    "finansi",
    "gossektor",
    "tehno",
    # Общество
    "obshchestvo_sobitiya",
    "proisshestviya",
    "zhizn",
    "kultura",
    "religiya",
    "den_v_istorii",
    # Другое / Медиа / Спецпроекты
    "sport",
    "nauka",
    "stil_zhizni",
    "redaktsiya_tandau",
]
DATE_RE = re.compile(r'_date_(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})')


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


def extract_sub_category(url):
    """Извлечь подкатегорию из URL."""
    parts = url.strip("/").split("/")
    for i, p in enumerate(parts):
        if p == "news" and i + 1 < len(parts):
            return parts[i + 1]
    return "unknown"


def fetch_listing_page(session, category, page):
    """Загрузить одну страницу листинга и вернуть найденные статьи."""
    url = (
        f"{BASE_URL}/ru/news/{category}"
        if page == 1
        else f"{BASE_URL}/ru/news/{category}/page-{page}"
    )
    try:
        resp = session.get(url, timeout=20)
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
        if DATE_RE.search(href) and full_url not in seen:
            seen.add(full_url)
            articles.append({"url": full_url, "title": "", "excerpt": "", "category_label": "", "thumbnail": ""})

    # Стандартные карточки
    for card in soup.find_all("div", class_="b-news-list__item"):
        link = card.find("a", href=DATE_RE)
        if link:
            href = link.get("href", "")
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen:
                seen.add(full_url)
                title_el = card.find("h3", class_="item-title")
                text_el = card.find("div", class_="item-text")
                cat_el = card.find("a", class_="category")
                img = card.find("img")
                articles.append({
                    "url": full_url,
                    "title": title_el.get_text(strip=True) if title_el else "",
                    "excerpt": text_el.get_text(strip=True) if text_el else "",
                    "category_label": cat_el.get_text(strip=True) if cat_el else "",
                    "thumbnail": urljoin(BASE_URL, img.get("src", "")) if img else "",
                })

    # Боковые карточки
    for card_div in soup.find_all("div", class_="card"):
        link = card_div.find("a", href=DATE_RE)
        if link:
            href = link.get("href", "")
            full_url = urljoin(BASE_URL, href)
            if full_url not in seen:
                seen.add(full_url)
                articles.append({"url": full_url, "title": "", "excerpt": "", "category_label": "", "thumbnail": ""})

    return page, articles


def get_oldest_date_on_page(session, category, page):
    """Получить самую старую дату на конкретной странице листинга."""
    _, articles = fetch_listing_page(session, category, page)
    dates = []
    for art in articles:
        d = parse_date_from_url(art["url"])
        if d:
            dates.append(d)
    return min(dates) if dates else None


def find_start_page(session, category, target_date, seen_urls):
    """
    Бинарный поиск: найти страницу, где самая старая дата ≈ target_date.
    Вместо пролистывания тысяч известных страниц — прыгаем сразу к нужному месту.
    Возвращает номер страницы, с которой нужно начать полный сбор.
    """
    # Сначала проверим, сколько уникальных (не из сайдбара) URL на странице
    # Из логов: ~4 уникальных на страницу для основных категорий
    # Попробуем грубо оценить: page 50 -> oldest ~2 months ago

    # Проверяем page 1 — если уже есть новые, начинаем с 1
    _, arts = fetch_listing_page(session, category, 1)
    has_new = any(a["url"] not in seen_urls for a in arts)
    if has_new:
        return 1

    # Пробуем page 50
    oldest_50 = get_oldest_date_on_page(session, category, 50)
    if not oldest_50:
        return 1

    # Если page 50 уже ниже target_date — значит категория маленькая, начнём с 1
    if oldest_50 <= target_date:
        return 1

    # Вычисляем скорость: сколько дней на страницу
    now = datetime.now()
    days_per_page = (now - oldest_50).days / 50.0
    if days_per_page <= 0:
        return 1

    # Оценочная страница для target_date
    days_to_target = (now - target_date).days
    estimated_page = int(days_to_target / days_per_page)

    # Бинарный поиск между page 50 и estimated_page * 1.5
    lo = 50
    hi = min(int(estimated_page * 1.5) + 100, 10000)

    print(f"  → Быстрый поиск: ~{days_per_page:.1f} дн/стр, оценка: стр {estimated_page}, ищем в [{lo}..{hi}]")

    # Проверяем верхнюю границу
    oldest_hi = get_oldest_date_on_page(session, category, hi)
    if oldest_hi and oldest_hi > target_date:
        # Даже на странице hi дата новее target — расширяем
        hi = min(hi * 2, 10000)

    best_page = lo
    for _ in range(15):  # максимум 15 итераций бинарного поиска
        if hi - lo < 10:
            break
        mid = (lo + hi) // 2
        oldest_mid = get_oldest_date_on_page(session, category, mid)
        if not oldest_mid:
            hi = mid
            continue

        if oldest_mid > target_date:
            # Ещё не добрались до target — нужно дальше
            lo = mid
            best_page = mid
        else:
            # Перескочили — отступаем назад
            hi = mid

    # Отступаем на 20 страниц назад для подстраховки
    start = max(1, best_page - 20)
    print(f"  → Начинаем со страницы {start} (пропущено {start - 1} страниц)")
    return start


def scrape_category(category, cutoff_date, seen_urls, force=False):
    """Собрать все URL одной категории до даты cutoff_date."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ru-RU,ru;q=0.9",
    })

    print(f"\n{'='*60}")
    print(f"  {category} | cutoff: {cutoff_date.strftime('%Y-%m-%d')}{' [FORCE]' if force else ''}")
    print(f"{'='*60}")

    # Определяем самую старую дату среди уже известных URL этой категории
    known_dates = []
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    rec = json.loads(line)
                    sub = rec.get("sub_category", "")
                    url = rec.get("url", "")
                    # Проверяем по подкатегории (точное совпадение slug в URL)
                    if f"/news/{category}/" in url:
                        d = parse_date_from_url(url)
                        if d:
                            known_dates.append(d)

    # Также из БД
    with get_db() as conn:
        rows = conn.execute("SELECT url FROM articles").fetchall()
        for row in rows:
            url = row[0]
            # Проверяем по подкатегории (точное совпадение slug в URL)
            if f"/news/{category}/" in url:
                d = parse_date_from_url(url)
                if d:
                    known_dates.append(d)

    if known_dates:
        oldest_known = min(known_dates)
        newest_known = max(known_dates)
        print(f"  Известно: {len(known_dates)} URL, от {oldest_known.strftime('%Y-%m-%d')} до {newest_known.strftime('%Y-%m-%d')}")

        if not force and oldest_known <= cutoff_date:
            # Дополнительная проверка: смотрим покрытие по месяцам
            # Если есть месяцы с < 50 статей — собираем заново
            from collections import Counter
            month_counts = Counter(d.strftime('%Y-%m') for d in known_dates)
            sparse_months = [
                m for m in month_counts
                if m >= cutoff_date.strftime('%Y-%m') and month_counts[m] < 50
            ]
            if sparse_months:
                print(f"  ⚠ Обнаружены месяцы с неполным покрытием: {', '.join(sorted(sparse_months)[:6])}...")
                print(f"  → Пересобираем для заполнения пробелов")
                # Ищем стартовую страницу для самого раннего sparse месяца
                earliest_sparse = datetime.strptime(min(sparse_months), '%Y-%m')
                start_page = find_start_page(session, category, earliest_sparse, seen_urls)
            else:
                print(f"  → Уже собрано до {oldest_known.strftime('%Y-%m-%d')}, cutoff {cutoff_date.strftime('%Y-%m-%d')} — пропускаем")
                return 0
        elif force:
            print(f"  → Принудительный пересбор (--force)")
            start_page = find_start_page(session, category, cutoff_date, seen_urls)
        else:
            # Нужно собрать от oldest_known вниз до cutoff_date
            # Быстро находим стартовую страницу через бинарный поиск
            start_page = find_start_page(session, category, oldest_known, seen_urls)
    else:
        start_page = 1
        print(f"  Нет известных URL — начинаем с начала")

    all_new = []
    page = start_page
    done = False
    batch_size = 5
    consecutive_past_cutoff = 0
    consecutive_all_known = 0  # счётчик страниц где все URL уже known
    prev_oldest_date = None
    stale_date_count = 0

    while not done:
        # Загружаем пачку страниц параллельно
        batch_results = {}
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = {
                executor.submit(fetch_listing_page, session, category, p): p
                for p in range(page, page + batch_size)
            }
            for future in as_completed(futures):
                p, arts = future.result()
                batch_results[p] = arts

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
            content_dates = []  # даты только из основного контента (не сайдбар)

            for art in articles:
                pub_date = parse_date_from_url(art["url"])

                if art["url"] in seen_urls:
                    already_known += 1
                    continue

                if pub_date and pub_date < cutoff_date:
                    old_count += 1
                    if pub_date:
                        content_dates.append(pub_date)
                    continue

                if pub_date:
                    content_dates.append(pub_date)

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

            # Проверяем, вышли ли за cutoff
            # Используем content_dates (без known/sidebar) для точной проверки
            if content_dates and all(d < cutoff_date for d in content_dates):
                consecutive_past_cutoff += 1
            elif new_count == 0 and old_count > 0 and already_known > 0:
                # Нет новых, есть старые — тоже считаем как past cutoff
                consecutive_past_cutoff += 1
            elif new_count == 0 and old_count == 0 and already_known > 0:
                # Все known — не сбрасываем счётчик, но и не увеличиваем
                pass
            else:
                consecutive_past_cutoff = 0

            # Обнаружение зацикливания: если oldest дата не продвигается
            oldest_date = min(content_dates).strftime("%Y-%m-%d") if content_dates else None
            if oldest_date and oldest_date == prev_oldest_date:
                stale_date_count += 1
            else:
                stale_date_count = 0
                prev_oldest_date = oldest_date

            # Счётчик «все known» — когда на странице 0 new и 0 old
            if new_count == 0 and old_count == 0 and already_known > 0:
                consecutive_all_known += 1
            else:
                consecutive_all_known = 0

            # Выводим прогресс
            if p <= start_page + 4 or p % 50 == 0 or new_count > 0:
                print(f"  p{p:>4}: +{new_count} new, {already_known} known, {old_count} old | oldest: {oldest_date or '?'} | total: {len(all_new) + len(batch_records)}", flush=True)

            # Стоп: нет новых URL и есть old — дошли до cutoff
            if consecutive_past_cutoff >= 10:
                print(f"  → Дошли до cutoff на странице {p}", flush=True)
                done = True
                break

            # Стоп: все URL known 30 страниц подряд — прыгаем или стопаем
            if consecutive_all_known >= 30:
                # Пробуем прыгнуть на 200 страниц вперёд
                jump_page = p + 200
                print(f"  → 30 стр подряд all-known, прыгаем на стр {jump_page}...", flush=True)
                _, jump_arts = fetch_listing_page(session, category, jump_page)
                jump_new = sum(1 for a in jump_arts if a["url"] not in seen_urls)
                if jump_new == 0:
                    # И после прыжка ничего нового — заканчиваем
                    print(f"  → После прыжка тоже 0 new — заканчиваем {category}", flush=True)
                    done = True
                    break
                else:
                    # Есть новые — перемещаемся туда
                    print(f"  → После прыжка +{jump_new} new — продолжаем с {jump_page}", flush=True)
                    page = jump_page
                    consecutive_all_known = 0
                    continue  # пропускаем page += batch_size внизу

            # Стоп: зацикливание (50 страниц подряд с одной и той же oldest датой)
            if stale_date_count >= 50:
                print(f"  → Зацикливание: oldest дата {oldest_date} не меняется 50 страниц — стоп на p{p}", flush=True)
                done = True
                break

        # Записываем сразу после каждой пачки (защита от таймаутов)
        if batch_records:
            with open(URLS_FILE, "a") as f:
                for record in batch_records:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            all_new.extend(batch_records)

        page += batch_size
        time.sleep(0.2)

        if page > 15000:
            print(f"  → Достигнут лимит 15000 страниц", flush=True)
            done = True

    print(f"  → {category}: {len(all_new)} новых URL")
    return len(all_new)


def main():
    parser = argparse.ArgumentParser(description="Сбор URL статей с total.kz")
    parser.add_argument("--days", type=int, default=365, help="За сколько дней собирать (по умолчанию 365)")
    parser.add_argument("--since", type=str, help="Собирать начиная с даты (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="Принудительный пересбор (игнорировать проверку oldest_known)")
    args = parser.parse_args()

    if args.since:
        cutoff_date = datetime.strptime(args.since, "%Y-%m-%d")
    else:
        cutoff_date = datetime.now() - timedelta(days=args.days)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    init_db()

    # Логируем запуск в БД
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_runs (started_at, phase, status) VALUES (?, 'urls', 'running')",
            (datetime.now().isoformat(),)
        )
        run_id = cursor.lastrowid

    # Загружаем уже известные URL
    seen_urls = set()
    if URLS_FILE.exists():
        with open(URLS_FILE) as f:
            for line in f:
                if line.strip():
                    seen_urls.add(json.loads(line)["url"])

    # Также из БД
    with get_db() as conn:
        rows = conn.execute("SELECT url FROM articles").fetchall()
        for row in rows:
            seen_urls.add(row[0])

    print(f"Уже известно URL: {len(seen_urls)}")
    print(f"Cutoff: {cutoff_date.strftime('%Y-%m-%d')}")

    total_new = 0
    for cat in SUBCATEGORIES:
        count = scrape_category(cat, cutoff_date, seen_urls, force=args.force)
        total_new += count

    # Обновляем статус запуска
    total_urls = sum(1 for _ in open(URLS_FILE)) if URLS_FILE.exists() else 0
    with get_db() as conn:
        conn.execute(
            "UPDATE scrape_runs SET finished_at=?, status='completed', articles_found=? WHERE id=?",
            (datetime.now().isoformat(), total_new, run_id)
        )

    print(f"\n{'='*60}")
    print(f"  ГОТОВО: {total_new} новых URL (всего в файле: {total_urls})")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
