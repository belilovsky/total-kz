#!/usr/bin/env python3
"""Диагностика провала в апреле-июне 2015."""
import json
import sqlite3
from collections import Counter

DB_PATH = "data/total.db"
URLS_FILE = "data/urls.jsonl"
ARTICLES_FILE = "data/articles.jsonl"

# 1. Проверяем БД: сколько статей по месяцам 2015
print("=== БД: статьи по месяцам 2015 ===")
conn = sqlite3.connect(DB_PATH)
rows = conn.execute("""
    SELECT substr(pub_date, 1, 7) as month, COUNT(*) as cnt
    FROM articles 
    WHERE pub_date LIKE '2015%'
    GROUP BY month ORDER BY month
""").fetchall()
for month, cnt in rows:
    marker = " <<<< ПРОВАЛ" if cnt < 100 else ""
    print(f"  {month}: {cnt}{marker}")

# 2. Сколько статей БЕЗ даты
null_count = conn.execute("SELECT COUNT(*) FROM articles WHERE pub_date IS NULL OR pub_date = ''").fetchone()[0]
print(f"\n=== Статьи без даты (pub_date IS NULL): {null_count} ===")

# 3. Проверяем urls.jsonl — сколько URL с датой апр-июн 2015
print("\n=== urls.jsonl: URL с pub_date в апр-июн 2015 ===")
url_months = Counter()
url_no_date = 0
url_total = 0
with open(URLS_FILE) as f:
    for line in f:
        if not line.strip():
            continue
        url_total += 1
        data = json.loads(line)
        pd = data.get("pub_date")
        if pd:
            m = pd[:7]
            if m.startswith("2015"):
                url_months[m] += 1
        else:
            url_no_date += 1

print(f"  Всего URL: {url_total}, без даты: {url_no_date}")
for m in sorted(url_months):
    marker = " <<<< ПРОВАЛ" if url_months[m] < 100 else ""
    print(f"  {m}: {url_months[m]}{marker}")

# 4. Проверяем articles.jsonl — статьи с pub_date/date_text за апр-июн 2015
print("\n=== articles.jsonl: статьи апр-июн 2015 ===")
art_months = Counter()
art_no_date = 0
art_with_datetext = 0
art_total = 0
sample_no_date = []
with open(ARTICLES_FILE) as f:
    for line in f:
        if not line.strip():
            continue
        art_total += 1
        data = json.loads(line)
        pd = data.get("pub_date")
        dt = data.get("date_text", "")
        if pd:
            m = pd[:7]
            if m.startswith("2015"):
                art_months[m] += 1
        else:
            art_no_date += 1
            if dt:
                art_with_datetext += 1
            if len(sample_no_date) < 5:
                sample_no_date.append({"url": data.get("url", "")[:80], "date_text": dt[:50]})

print(f"  Всего статей: {art_total}")
print(f"  Без pub_date: {art_no_date}")
print(f"  Без pub_date но с date_text: {art_with_datetext}")
for m in sorted(art_months):
    marker = " <<<< ПРОВАЛ" if art_months[m] < 100 else ""
    print(f"  {m}: {art_months[m]}{marker}")

if sample_no_date:
    print("\n=== Примеры статей без pub_date ===")
    for s in sample_no_date:
        print(f"  URL: {s['url']}")
        print(f"  date_text: {s['date_text']}")
        print()

# 5. Проверяем — есть ли URL за апрель 2015 вообще?
print("\n=== Поиск URL с '2015/04' или '2015/05' или '2015/06' в пути ===")
april_urls = []
with open(URLS_FILE) as f:
    for line in f:
        if not line.strip():
            continue
        data = json.loads(line)
        url = data.get("url", "")
        if "/2015/04/" in url or "/2015/05/" in url or "/2015/06/" in url:
            april_urls.append(url)
        elif "2015_04" in url or "2015_05" in url or "2015_06" in url:
            april_urls.append(url)

print(f"  URL с 2015/04-06 в пути: {len(april_urls)}")
if april_urls[:5]:
    for u in april_urls[:5]:
        print(f"    {u}")

conn.close()
