#!/usr/bin/env python3
"""Полный аудит данных total.kz."""
import sqlite3
import json
from collections import Counter, defaultdict

DB_PATH = "data/total.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

print("=" * 70)
print("  ПОЛНЫЙ АУДИТ ДАННЫХ total.kz")
print("=" * 70)

# ═══════════════════════════════════════════════════════
# 1. ОБЩАЯ СТАТИСТИКА
# ═══════════════════════════════════════════════════════
print("\n\n═══ 1. ОБЩАЯ СТАТИСТИКА ═══")
total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
print(f"Всего статей: {total}")

date_range = conn.execute("SELECT MIN(pub_date), MAX(pub_date) FROM articles WHERE pub_date IS NOT NULL").fetchone()
print(f"Период: {date_range[0][:10] if date_range[0] else 'N/A'} — {date_range[1][:10] if date_range[1] else 'N/A'}")

null_date = conn.execute("SELECT COUNT(*) FROM articles WHERE pub_date IS NULL OR pub_date = ''").fetchone()[0]
print(f"Без даты: {null_date}")

# ═══════════════════════════════════════════════════════
# 2. ПРОБЕЛЫ ПО МЕСЯЦАМ
# ═══════════════════════════════════════════════════════
print("\n\n═══ 2. ПОМЕСЯЧНАЯ ДИНАМИКА (пробелы) ═══")
months = conn.execute("""
    SELECT substr(pub_date, 1, 7) as month, COUNT(*) as cnt
    FROM articles WHERE pub_date IS NOT NULL
    GROUP BY month ORDER BY month
""").fetchall()

prev_month = None
gaps = []
for row in months:
    m, cnt = row[0], row[1]
    # Определяем аномалии
    marker = ""
    if cnt < 50:
        marker = " <<<< ПОЧТИ ПУСТО"
        gaps.append((m, cnt))
    elif cnt < 200:
        marker = " << МАЛО"
        gaps.append((m, cnt))
    print(f"  {m}: {cnt:>5}{marker}")

if gaps:
    print(f"\n  ПРОБЕЛЫ ({len(gaps)} месяцев с <200 статей):")
    for m, cnt in gaps:
        print(f"    {m}: {cnt}")

# ═══════════════════════════════════════════════════════
# 3. ПОЛНОТА КОНТЕНТА
# ═══════════════════════════════════════════════════════
print("\n\n═══ 3. ПОЛНОТА КОНТЕНТА ═══")

fields = {
    "title": "SELECT COUNT(*) FROM articles WHERE title IS NULL OR title = ''",
    "body_text": "SELECT COUNT(*) FROM articles WHERE body_text IS NULL OR body_text = ''",
    "body_html": "SELECT COUNT(*) FROM articles WHERE body_html IS NULL OR body_html = ''",
    "excerpt": "SELECT COUNT(*) FROM articles WHERE excerpt IS NULL OR excerpt = ''",
    "author": "SELECT COUNT(*) FROM articles WHERE author IS NULL OR author = ''",
    "main_image": "SELECT COUNT(*) FROM articles WHERE main_image IS NULL OR main_image = ''",
    "thumbnail": "SELECT COUNT(*) FROM articles WHERE thumbnail IS NULL OR thumbnail = ''",
    "image_credit": "SELECT COUNT(*) FROM articles WHERE image_credit IS NULL OR image_credit = ''",
    "tags (JSON)": "SELECT COUNT(*) FROM articles WHERE tags IS NULL OR tags = '' OR tags = '[]'",
    "inline_images (JSON)": "SELECT COUNT(*) FROM articles WHERE inline_images IS NULL OR inline_images = '' OR inline_images = '[]'",
    "sub_category": "SELECT COUNT(*) FROM articles WHERE sub_category IS NULL OR sub_category = ''",
    "category_label": "SELECT COUNT(*) FROM articles WHERE category_label IS NULL OR category_label = ''",
}

for name, sql in fields.items():
    empty = conn.execute(sql).fetchone()[0]
    pct = (empty / total * 100) if total > 0 else 0
    status = "✓" if pct < 5 else "⚠" if pct < 30 else "✗"
    print(f"  {status} {name}: {empty:>7} пустых ({pct:.1f}%)")

# Средняя длина body_text
avg_body = conn.execute("SELECT AVG(LENGTH(body_text)) FROM articles WHERE body_text IS NOT NULL AND body_text != ''").fetchone()[0]
min_body = conn.execute("SELECT MIN(LENGTH(body_text)) FROM articles WHERE body_text IS NOT NULL AND body_text != ''").fetchone()[0]
print(f"\n  Средняя длина body_text: {int(avg_body or 0)} символов")
print(f"  Мин. длина body_text: {min_body}")

# Короткие тексты (< 100 символов)
short_body = conn.execute("SELECT COUNT(*) FROM articles WHERE LENGTH(body_text) < 100 AND body_text IS NOT NULL AND body_text != ''").fetchone()[0]
print(f"  Коротких body_text (<100 символов): {short_body}")

# Примеры коротких
short_samples = conn.execute("""
    SELECT title, LENGTH(body_text) as len, url FROM articles 
    WHERE LENGTH(body_text) < 50 AND body_text IS NOT NULL AND body_text != ''
    LIMIT 5
""").fetchall()
if short_samples:
    print("  Примеры очень коротких:")
    for s in short_samples:
        print(f"    [{s[1]} сим.] {s[0][:60]}  →  {s[2][-40:]}")

# ═══════════════════════════════════════════════════════
# 4. КАТЕГОРИИ
# ═══════════════════════════════════════════════════════
print("\n\n═══ 4. КАТЕГОРИИ ═══")
cats = conn.execute("""
    SELECT sub_category, COUNT(*) as cnt
    FROM articles GROUP BY sub_category ORDER BY cnt DESC
""").fetchall()
for c in cats:
    print(f"  {c[1]:>6}  {c[0] or '(пусто)'}")

# ═══════════════════════════════════════════════════════
# 5. АВТОРЫ
# ═══════════════════════════════════════════════════════
print("\n\n═══ 5. АВТОРЫ (топ-30) ═══")
authors = conn.execute("""
    SELECT author, COUNT(*) as cnt
    FROM articles WHERE author IS NOT NULL AND author != ''
    GROUP BY author ORDER BY cnt DESC LIMIT 30
""").fetchall()
for a in authors:
    print(f"  {a[1]:>6}  {a[0]}")

total_with_author = conn.execute("SELECT COUNT(*) FROM articles WHERE author IS NOT NULL AND author != ''").fetchone()[0]
unique_authors = conn.execute("SELECT COUNT(DISTINCT author) FROM articles WHERE author IS NOT NULL AND author != ''").fetchone()[0]
print(f"\n  Статей с автором: {total_with_author} ({total_with_author/total*100:.1f}%)")
print(f"  Уникальных авторов: {unique_authors}")

# Подозрительные авторы (слишком длинные, HTML, спецсимволы)
suspicious_authors = conn.execute("""
    SELECT author, COUNT(*) as cnt FROM articles 
    WHERE author IS NOT NULL AND author != ''
    AND (LENGTH(author) > 50 OR author LIKE '%<%' OR author LIKE '%@%' OR author LIKE '%http%')
    GROUP BY author ORDER BY cnt DESC LIMIT 10
""").fetchall()
if suspicious_authors:
    print("\n  Подозрительные авторы:")
    for a in suspicious_authors:
        print(f"    [{a[1]}] {a[0][:80]}")

# ═══════════════════════════════════════════════════════
# 6. ТЕГИ
# ═══════════════════════════════════════════════════════
print("\n\n═══ 6. ТЕГИ ═══")
total_tags = conn.execute("SELECT COUNT(*) FROM article_tags").fetchone()[0]
unique_tags = conn.execute("SELECT COUNT(DISTINCT tag) FROM article_tags").fetchone()[0]
articles_with_tags = conn.execute("SELECT COUNT(DISTINCT article_id) FROM article_tags").fetchone()[0]
print(f"  Всего связей тег-статья: {total_tags}")
print(f"  Уникальных тегов: {unique_tags}")
print(f"  Статей с тегами: {articles_with_tags} ({articles_with_tags/total*100:.1f}%)")

# Топ-30 тегов
print("\n  Топ-30 тегов:")
top_tags = conn.execute("""
    SELECT tag, COUNT(*) as cnt FROM article_tags
    GROUP BY tag ORDER BY cnt DESC LIMIT 30
""").fetchall()
for t in top_tags:
    print(f"    {t[1]:>5}  {t[0]}")

# Теги с потенциальными дубликатами (разный регистр)
print("\n  Потенциальные дубликаты тегов (разный регистр):")
tag_groups = defaultdict(list)
all_tags = conn.execute("SELECT DISTINCT tag FROM article_tags").fetchall()
for t in all_tags:
    tag_groups[t[0].lower().strip()].append(t[0])

dupes = {k: v for k, v in tag_groups.items() if len(v) > 1}
for lower_tag, variants in sorted(dupes.items(), key=lambda x: -len(x[1]))[:30]:
    counts = []
    for v in variants:
        c = conn.execute("SELECT COUNT(*) FROM article_tags WHERE tag = ?", (v,)).fetchone()[0]
        counts.append(f"'{v}'({c})")
    print(f"    {' | '.join(counts)}")

# Теги на разных языках (кириллица vs латиница)
print("\n  Теги только латиницей (может быть транслит или английские):")
latin_tags = conn.execute("""
    SELECT tag, COUNT(*) as cnt FROM article_tags 
    WHERE tag NOT GLOB '*[а-яА-ЯёЁ]*' AND tag != ''
    GROUP BY tag ORDER BY cnt DESC LIMIT 20
""").fetchall()
for t in latin_tags:
    print(f"    {t[1]:>5}  {t[0]}")

# Очень длинные теги
long_tags = conn.execute("""
    SELECT tag, COUNT(*) as cnt FROM article_tags 
    WHERE LENGTH(tag) > 50
    GROUP BY tag ORDER BY cnt DESC LIMIT 10
""").fetchall()
if long_tags:
    print("\n  Слишком длинные теги (>50 символов):")
    for t in long_tags:
        print(f"    [{t[1]}] {t[0][:80]}")

# ═══════════════════════════════════════════════════════
# 7. СУЩНОСТИ (ENTITIES)
# ═══════════════════════════════════════════════════════
print("\n\n═══ 7. СУЩНОСТИ (ENTITIES) ═══")
total_entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
total_links = conn.execute("SELECT COUNT(*) FROM article_entities").fetchone()[0]
print(f"  Всего сущностей: {total_entities}")
print(f"  Всего связей: {total_links}")

for etype in ['person', 'org', 'location', 'event']:
    cnt = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type = ?", (etype,)).fetchone()[0]
    print(f"  {etype}: {cnt}")

# Топ-15 по типу
for etype in ['person', 'org', 'location']:
    print(f"\n  Топ-15 {etype}:")
    top = conn.execute("""
        SELECT e.name, e.normalized, COUNT(ae.article_id) as cnt
        FROM entities e
        JOIN article_entities ae ON ae.entity_id = e.id
        WHERE e.entity_type = ?
        GROUP BY e.id ORDER BY cnt DESC LIMIT 15
    """, (etype,)).fetchall()
    for e in top:
        norm_note = f"  (norm: {e[1]})" if e[1] != e[0].lower().strip() else ""
        print(f"    {e[2]:>5}  {e[0]}{norm_note}")

# Дубликаты сущностей (разные name, одинаковый normalized)
print("\n  Потенциальные дубликаты сущностей (разные имена → один normalized):")
entity_dupes = conn.execute("""
    SELECT normalized, entity_type, GROUP_CONCAT(name, ' | ') as names, COUNT(*) as cnt
    FROM entities
    GROUP BY normalized, entity_type
    HAVING cnt > 1
    ORDER BY cnt DESC
    LIMIT 20
""").fetchall()
for d in entity_dupes:
    print(f"    [{d[1]}] '{d[0]}' → {d[3]} вариантов: {d[2][:100]}")

# Сущности без связей
orphan = conn.execute("""
    SELECT COUNT(*) FROM entities e
    LEFT JOIN article_entities ae ON ae.entity_id = e.id
    WHERE ae.article_id IS NULL
""").fetchone()[0]
print(f"\n  Сущности без статей (сироты): {orphan}")

# Потенциальные проблемы: entity name = entity normalized? 
not_normalized = conn.execute("""
    SELECT COUNT(*) FROM entities WHERE normalized IS NULL OR normalized = ''
""").fetchone()[0]
print(f"  Сущности без нормализации: {not_normalized}")

# ═══════════════════════════════════════════════════════
# 8. БИТЫЕ URL / ДУБЛИКАТЫ
# ═══════════════════════════════════════════════════════
print("\n\n═══ 8. URL ПРОВЕРКИ ═══")
dup_urls = conn.execute("""
    SELECT url, COUNT(*) as cnt FROM articles 
    GROUP BY url HAVING cnt > 1
""").fetchall()
print(f"  Дубликаты URL: {len(dup_urls)}")

no_url = conn.execute("SELECT COUNT(*) FROM articles WHERE url IS NULL OR url = ''").fetchone()[0]
print(f"  Без URL: {no_url}")

# URL форматы
old_format = conn.execute("SELECT COUNT(*) FROM articles WHERE url LIKE '%total.kz/ru/news/%'").fetchone()[0]
other_format = total - old_format
print(f"  URL /ru/news/ формат: {old_format}")
print(f"  Другой формат URL: {other_format}")
if other_format > 0:
    samples = conn.execute("""
        SELECT url FROM articles WHERE url NOT LIKE '%total.kz/ru/news/%' LIMIT 5
    """).fetchall()
    for s in samples:
        print(f"    {s[0][:100]}")

# ═══════════════════════════════════════════════════════
# ИТОГО
# ═══════════════════════════════════════════════════════
print("\n\n" + "=" * 70)
print("  ИТОГО:")
print("=" * 70)
print(f"  Статей: {total}")
print(f"  С датой: {total - null_date} ({(total-null_date)/total*100:.1f}%)")
print(f"  С текстом: {total - conn.execute('SELECT COUNT(*) FROM articles WHERE body_text IS NULL OR body_text = chr(10)').fetchone()[0]}")
print(f"  Пробелы в данных: {len(gaps)} месяцев с <200 статей")
print(f"  Тегов: {unique_tags}, сущностей: {total_entities}")

conn.close()
