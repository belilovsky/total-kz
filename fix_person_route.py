#!/usr/bin/env python3
"""
Fix person_page route to use PG for article queries while keeping
SQLite for person/entity data.
"""
import re

content = open('app/public_routes.py').read()

# Find the person_page function and replace the articles query
# The issue: articles are in PG, but person route queries SQLite
# Solution: use db_backend.execute_raw_many for article queries

# 1. Replace the articles_raw query in person_page
old_articles = '''    # Articles grouped by month (latest first, limit 200)
    articles_raw = conn.execute(\"\"\"
        SELECT a.id, a.title, a.pub_date, a.sub_category, a.url, a.main_image, a.thumbnail
        FROM articles a
        JOIN article_entities ae ON a.id = ae.article_id
        WHERE ae.entity_id = ?
        AND a.pub_date IS NOT NULL AND a.pub_date != ''
        ORDER BY a.pub_date DESC
        LIMIT 200
    \"\"\", (person["entity_id"],)).fetchall()'''

new_articles = '''    # Articles grouped by month (latest first, limit 200)
    # Use PG backend for articles (they live in PostgreSQL)
    from app.db_backend import execute_raw_many as _pg_many
    try:
        articles_raw = _pg_many(\"\"\"
            SELECT a.id, a.title, a.pub_date, a.sub_category, a.url, a.main_image, a.thumbnail
            FROM articles a
            JOIN article_entities ae ON a.id = ae.article_id
            WHERE ae.entity_id = %s
            AND a.pub_date IS NOT NULL AND a.pub_date != ''
            ORDER BY a.pub_date DESC
            LIMIT 200
        \"\"\", (person["entity_id"],))
    except Exception:
        articles_raw = []'''

if old_articles in content:
    content = content.replace(old_articles, new_articles)
    print("Replaced articles_raw query")
else:
    print("articles_raw pattern not found")

# 2. Replace the article_count query
old_count = '''    # Article count
    article_count = conn.execute(
        "SELECT COUNT(*) FROM article_entities WHERE entity_id = ?",
        (person["entity_id"],)
    ).fetchone()[0]'''

new_count = '''    # Article count (from PG)
    try:
        _count_row = _pg_many("SELECT COUNT(*) as cnt FROM article_entities WHERE entity_id = %s", (person["entity_id"],))
        article_count = _count_row[0]["cnt"] if _count_row else 0
    except Exception:
        article_count = conn.execute(
            "SELECT COUNT(*) FROM article_entities WHERE entity_id = ?",
            (person["entity_id"],)
        ).fetchone()[0]'''

if old_count in content:
    content = content.replace(old_count, new_count)
    print("Replaced article_count query")
else:
    print("article_count pattern not found")

# 3. Replace the date_range query
old_dates = '''    # First/last mention date range
    date_range = conn.execute(\"\"\"
        SELECT MIN(a.pub_date), MAX(a.pub_date)
        FROM articles a
        JOIN article_entities ae ON a.id = ae.article_id
        WHERE ae.entity_id = ?
        AND a.pub_date IS NOT NULL AND a.pub_date != ''
    \"\"\", (person["entity_id"],)).fetchone()'''

new_dates = '''    # First/last mention date range (from PG)
    try:
        _dr = _pg_many(\"\"\"
            SELECT MIN(a.pub_date) as min_date, MAX(a.pub_date) as max_date
            FROM articles a
            JOIN article_entities ae ON a.id = ae.article_id
            WHERE ae.entity_id = %s
            AND a.pub_date IS NOT NULL AND a.pub_date != ''
        \"\"\", (person["entity_id"],))
        date_range = (_dr[0]["min_date"], _dr[0]["max_date"]) if _dr else (None, None)
    except Exception:
        date_range = conn.execute(\"\"\"
            SELECT MIN(a.pub_date), MAX(a.pub_date)
            FROM articles a
            JOIN article_entities ae ON a.id = ae.article_id
            WHERE ae.entity_id = ?
            AND a.pub_date IS NOT NULL AND a.pub_date != ''
        \"\"\", (person["entity_id"],)).fetchone()'''

if old_dates in content:
    content = content.replace(old_dates, new_dates)
    print("Replaced date_range query")
else:
    print("date_range pattern not found")

open('app/public_routes.py', 'w').write(content)
print("Done!")
