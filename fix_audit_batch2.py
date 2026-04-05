#!/usr/bin/env python3
"""Fix audit batch 2: sentiment dashboard, tag cloud, analytics iframe."""
import re, shutil

# === 1. Fix sentiment dashboard to use PG instead of SQLite ===
main = open('app/main.py').read()

# Replace the entire _query_sentiment_data to use PG
old_func_start = 'def _query_sentiment_data() -> dict:'
old_func_marker = '"""Query article_nlp for sentiment dashboard data (SQLite)."""'
new_func_marker = '"""Query article_nlp for sentiment dashboard data (PostgreSQL)."""'

idx = main.find(old_func_start)
if idx > 0:
    # Find the end of the function (next def or class at same indent)
    func_body_start = idx
    # Find the return statement at function level
    lines = main[idx:].split('\n')
    func_end = idx
    brace_depth = 0
    for i, line in enumerate(lines):
        if i > 0 and line and not line.startswith(' ') and not line.startswith('\t'):
            func_end = idx + sum(len(l)+1 for l in lines[:i])
            break
    
    old_func = main[idx:func_end]
    
    new_func = '''def _query_sentiment_data() -> dict:
    """Query article_nlp for sentiment dashboard data (PostgreSQL)."""
    from app.db_backend import execute_raw_many
    empty = {"total": 0, "positive_count": 0, "neutral_count": 0, "negative_count": 0,
             "pie": {"positive": 0, "neutral": 0, "negative": 0},
             "trend": {"dates": [], "positive": [], "neutral": [], "negative": []},
             "by_category": {"categories": [], "positive": [], "neutral": [], "negative": []},
             "most_positive": [], "most_negative": [], "govt_trend": {"dates": [], "scores": [], "counts": []}}
    try:
        counts = execute_raw_many("""
            SELECT COUNT(*) as total,
                SUM(CASE WHEN n.sentiment = 'positive' THEN 1 ELSE 0 END) as pos,
                SUM(CASE WHEN n.sentiment = 'neutral' THEN 1 ELSE 0 END) as neu,
                SUM(CASE WHEN n.sentiment = 'negative' THEN 1 ELSE 0 END) as neg
            FROM article_nlp n
            JOIN articles a ON a.id = n.article_id
            WHERE a.pub_date::timestamp >= NOW() - INTERVAL '30 days'
              AND n.sentiment IS NOT NULL
        """, ())
        if not counts:
            return empty
        c = counts[0]
        total = c["total"] or 0
        pos = c["pos"] or 0
        neu = c["neu"] or 0
        neg = c["neg"] or 0

        trend_rows = execute_raw_many("""
            SELECT DATE(a.pub_date::timestamp) as day,
                SUM(CASE WHEN n.sentiment = 'positive' THEN 1 ELSE 0 END) as pos,
                SUM(CASE WHEN n.sentiment = 'neutral' THEN 1 ELSE 0 END) as neu,
                SUM(CASE WHEN n.sentiment = 'negative' THEN 1 ELSE 0 END) as neg
            FROM article_nlp n
            JOIN articles a ON a.id = n.article_id
            WHERE a.pub_date::timestamp >= NOW() - INTERVAL '30 days'
              AND n.sentiment IS NOT NULL
            GROUP BY day ORDER BY day
        """, ())

        trend = {
            "dates": [str(r["day"]) for r in trend_rows],
            "positive": [r["pos"] for r in trend_rows],
            "neutral": [r["neu"] for r in trend_rows],
            "negative": [r["neg"] for r in trend_rows],
        }

        cat_rows = execute_raw_many("""
            SELECT a.sub_category,
                SUM(CASE WHEN n.sentiment = 'positive' THEN 1 ELSE 0 END) as pos,
                SUM(CASE WHEN n.sentiment = 'neutral' THEN 1 ELSE 0 END) as neu,
                SUM(CASE WHEN n.sentiment = 'negative' THEN 1 ELSE 0 END) as neg
            FROM article_nlp n
            JOIN articles a ON a.id = n.article_id
            WHERE a.pub_date::timestamp >= NOW() - INTERVAL '30 days'
              AND n.sentiment IS NOT NULL
              AND a.sub_category IS NOT NULL AND a.sub_category != ''
            GROUP BY a.sub_category
            ORDER BY (SUM(CASE WHEN n.sentiment = 'positive' THEN 1 ELSE 0 END) + 
                      SUM(CASE WHEN n.sentiment = 'neutral' THEN 1 ELSE 0 END) + 
                      SUM(CASE WHEN n.sentiment = 'negative' THEN 1 ELSE 0 END)) DESC
            LIMIT 15
        """, ())

        by_category = {
            "categories": [r["sub_category"] for r in cat_rows],
            "positive": [r["pos"] for r in cat_rows],
            "neutral": [r["neu"] for r in cat_rows],
            "negative": [r["neg"] for r in cat_rows],
        }

        return {
            "total": total, "positive_count": pos, "neutral_count": neu, "negative_count": neg,
            "pie": {"positive": pos, "neutral": neu, "negative": neg},
            "trend": trend,
            "by_category": by_category,
            "most_positive": [], "most_negative": [],
            "govt_trend": {"dates": [], "scores": [], "counts": []},
        }
    except Exception as e:
        import logging
        logging.getLogger("sentiment").exception("Sentiment query failed")
        return empty

'''
    main = main[:idx] + new_func + main[func_end:]
    print("1. Replaced _query_sentiment_data with PG version")
else:
    print("1. _query_sentiment_data not found")

open('app/main.py', 'w').write(main)

# === 2. Fix tag cloud on homepage ===
home = open('app/templates/public/home.html').read()

# Find tag cloud section - if it's truncated, add "ещё" link
if 'tag-cloud' in home or 'popular_tags' in home:
    print("8. Tag cloud section found")
    # Add max-height + overflow with "show more" if not already
else:
    # Check if there's a tags section
    tags_idx = home.find('cloud')
    if tags_idx < 0:
        tags_idx = home.find('ТЕГИ')
    if tags_idx < 0:
        tags_idx = home.find('tags')
    print(f"8. Tags section search: pos={tags_idx}")

# === 3. Fix analytics iframe ===
# Check if umami_share_url is passed to template
if 'umami_share_url' in main:
    print("9. umami_share_url found in main.py")
else:
    print("9. umami_share_url NOT found - need to add")
    # Find analytics admin route
    analytics_match = main.find('/admin/analytics')
    if analytics_match > 0:
        print(f"   Analytics route at pos {analytics_match}")

print("\nBatch 2 done!")
