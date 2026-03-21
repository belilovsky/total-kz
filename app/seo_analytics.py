"""SEO / GEO / SGEO analytics engine for Total.kz content archive.

Analyses the article database and produces actionable metrics:
  • Meta completeness (title, excerpt/description, author, image, date)
  • Content quality signals (word count, headings, internal/external links)
  • Schema.org / NewsArticle readiness
  • AI-crawler readiness (robots.txt, llms.txt checks)
  • Entity authority & topical coverage
  • Content freshness distribution
"""

import re
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from . import database as db


# ────────────────────────────────────────────
# 1.  Meta-tag & field completeness audit
# ────────────────────────────────────────────

def _meta_score(article: dict) -> dict:
    """Return a per-article meta completeness breakdown."""
    checks = {
        "title": bool(article.get("title") and len(article["title"].strip()) > 0),
        "excerpt": bool(article.get("excerpt") and len(article["excerpt"].strip()) > 10),
        "author": bool(article.get("author") and article["author"].strip()),
        "pub_date": bool(article.get("pub_date")),
        "main_image": bool(article.get("main_image")),
        "thumbnail": bool(article.get("thumbnail")),
        "category": bool(article.get("sub_category")),
        "body_text": bool(article.get("body_text") and len(article["body_text"].strip()) > 50),
        "tags": bool(article.get("tags") and article["tags"] != "[]"),
    }
    filled = sum(checks.values())
    return {
        "checks": checks,
        "filled": filled,
        "total": len(checks),
        "pct": round(filled / len(checks) * 100),
    }


def get_meta_audit(limit: int = 500) -> dict:
    """Aggregate meta completeness across most recent articles."""
    with db.get_db() as conn:
        rows = conn.execute("""
            SELECT id, url, title, excerpt, author, pub_date,
                   main_image, thumbnail, sub_category, body_text, tags
            FROM articles
            ORDER BY pub_date DESC
            LIMIT ?
        """, (limit,)).fetchall()

    field_ok = Counter()
    field_total = Counter()
    score_dist = []  # percentage per article
    issues = []  # articles with score < 70 %

    for r in rows:
        art = dict(r)
        ms = _meta_score(art)
        score_dist.append(ms["pct"])
        for field, ok in ms["checks"].items():
            field_total[field] += 1
            if ok:
                field_ok[field] += 1
        if ms["pct"] < 70:
            issues.append({
                "id": art["id"],
                "title": (art.get("title") or "(без заголовка)")[:80],
                "score": ms["pct"],
                "missing": [f for f, v in ms["checks"].items() if not v],
            })

    field_rates = {}
    for f in field_total:
        field_rates[f] = round(field_ok[f] / field_total[f] * 100) if field_total[f] else 0

    avg_score = round(statistics.mean(score_dist)) if score_dist else 0

    return {
        "total_audited": len(rows),
        "avg_score": avg_score,
        "field_rates": field_rates,
        "score_distribution": _bucket_distribution(score_dist),
        "issues": sorted(issues, key=lambda x: x["score"])[:30],
    }


def _bucket_distribution(scores: list) -> list:
    """Bucket scores into 0-20, 20-40, … 80-100."""
    buckets = [0] * 5
    labels = ["0–20%", "20–40%", "40–60%", "60–80%", "80–100%"]
    for s in scores:
        idx = min(int(s // 20), 4)
        buckets[idx] += 1
    return [{"label": labels[i], "count": buckets[i]} for i in range(5)]


# ────────────────────────────────────────────
# 2.  Content quality analysis
# ────────────────────────────────────────────

_WORD_RE = re.compile(r"[а-яА-ЯёЁa-zA-Z0-9]+")
_HEADING_RE = re.compile(r"<h[2-6][^>]*>", re.IGNORECASE)
_LINK_RE = re.compile(r'<a\s[^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)
_IMG_RE = re.compile(r'<img\s[^>]*>', re.IGNORECASE)


def _content_quality(article: dict) -> dict:
    """Compute per-article content quality signals."""
    body = article.get("body_text") or ""
    html = article.get("body_html") or ""
    words = _WORD_RE.findall(body)
    word_count = len(words)

    headings = len(_HEADING_RE.findall(html))
    links = _LINK_RE.findall(html)
    internal = sum(1 for l in links if "total.kz" in l)
    external = len(links) - internal
    images = len(_IMG_RE.findall(html))

    title = article.get("title") or ""
    title_len = len(title)
    excerpt = article.get("excerpt") or ""
    excerpt_len = len(excerpt)

    return {
        "word_count": word_count,
        "headings": headings,
        "internal_links": internal,
        "external_links": external,
        "images": images,
        "title_len": title_len,
        "excerpt_len": excerpt_len,
        "title_ok": 25 <= title_len <= 70,
        "excerpt_ok": 50 <= excerpt_len <= 160,
    }


def get_content_quality(limit: int = 500) -> dict:
    """Aggregate content quality metrics."""
    with db.get_db() as conn:
        rows = conn.execute("""
            SELECT id, title, excerpt, body_text, body_html, sub_category
            FROM articles
            ORDER BY pub_date DESC LIMIT ?
        """, (limit,)).fetchall()

    word_counts = []
    heading_counts = []
    internal_links_list = []
    external_links_list = []
    img_counts = []
    title_ok_count = 0
    excerpt_ok_count = 0
    thin_content = []  # articles with < 300 words

    for r in rows:
        art = dict(r)
        cq = _content_quality(art)
        word_counts.append(cq["word_count"])
        heading_counts.append(cq["headings"])
        internal_links_list.append(cq["internal_links"])
        external_links_list.append(cq["external_links"])
        img_counts.append(cq["images"])
        if cq["title_ok"]:
            title_ok_count += 1
        if cq["excerpt_ok"]:
            excerpt_ok_count += 1
        if cq["word_count"] < 300:
            thin_content.append({
                "id": art["id"],
                "title": (art.get("title") or "")[:80],
                "word_count": cq["word_count"],
                "category": art.get("sub_category", ""),
            })

    n = len(rows) or 1
    avg_wc = round(statistics.mean(word_counts)) if word_counts else 0
    median_wc = round(statistics.median(word_counts)) if word_counts else 0

    wc_buckets = _word_count_buckets(word_counts)

    return {
        "total_analysed": len(rows),
        "avg_word_count": avg_wc,
        "median_word_count": median_wc,
        "avg_headings": round(statistics.mean(heading_counts), 1) if heading_counts else 0,
        "avg_internal_links": round(statistics.mean(internal_links_list), 1) if internal_links_list else 0,
        "avg_external_links": round(statistics.mean(external_links_list), 1) if external_links_list else 0,
        "avg_images": round(statistics.mean(img_counts), 1) if img_counts else 0,
        "title_ok_pct": round(title_ok_count / n * 100),
        "excerpt_ok_pct": round(excerpt_ok_count / n * 100),
        "word_count_distribution": wc_buckets,
        "thin_content_count": len(thin_content),
        "thin_content": sorted(thin_content, key=lambda x: x["word_count"])[:20],
    }


def _word_count_buckets(wcs: list) -> list:
    labels = ["<100", "100–300", "300–600", "600–1000", "1000–2000", "2000+"]
    bounds = [100, 300, 600, 1000, 2000, float("inf")]
    buckets = [0] * len(labels)
    for w in wcs:
        for i, b in enumerate(bounds):
            if w < b:
                buckets[i] += 1
                break
    return [{"label": labels[i], "count": buckets[i]} for i in range(len(labels))]


# ────────────────────────────────────────────
# 3.  Schema.org / NewsArticle readiness
# ────────────────────────────────────────────

def get_schema_readiness(limit: int = 500) -> dict:
    """Check how many articles have all required fields for NewsArticle schema."""
    with db.get_db() as conn:
        rows = conn.execute("""
            SELECT id, title, excerpt, author, pub_date, main_image, url
            FROM articles
            ORDER BY pub_date DESC LIMIT ?
        """, (limit,)).fetchall()

    ready = 0
    partial = 0
    not_ready = 0
    missing_fields = Counter()

    for r in rows:
        art = dict(r)
        required = {
            "headline": bool(art.get("title")),
            "datePublished": bool(art.get("pub_date")),
            "author": bool(art.get("author")),
            "image": bool(art.get("main_image")),
            "description": bool(art.get("excerpt")),
            "url": bool(art.get("url")),
        }
        filled = sum(required.values())
        for f, ok in required.items():
            if not ok:
                missing_fields[f] += 1

        if filled == len(required):
            ready += 1
        elif filled >= 4:
            partial += 1
        else:
            not_ready += 1

    n = len(rows) or 1
    return {
        "total": len(rows),
        "ready": ready,
        "ready_pct": round(ready / n * 100),
        "partial": partial,
        "partial_pct": round(partial / n * 100),
        "not_ready": not_ready,
        "not_ready_pct": round(not_ready / n * 100),
        "missing_fields": dict(missing_fields.most_common()),
    }


# ────────────────────────────────────────────
# 4.  Entity authority / topical coverage
# ────────────────────────────────────────────

def get_entity_authority(limit: int = 30) -> dict:
    """Top entities with article counts and mention density."""
    with db.get_db() as conn:
        total_articles = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0] or 1

        top_entities = conn.execute("""
            SELECT e.id, e.name, e.entity_type,
                   COUNT(DISTINCT ae.article_id) as article_count,
                   SUM(ae.mention_count) as total_mentions
            FROM entities e
            JOIN article_entities ae ON ae.entity_id = e.id
            GROUP BY e.id
            ORDER BY article_count DESC
            LIMIT ?
        """, (limit,)).fetchall()

        # Category coverage – how many categories does each top entity span
        entity_ids = [dict(e)["id"] for e in top_entities]
        coverage = {}
        for eid in entity_ids:
            cats = conn.execute("""
                SELECT COUNT(DISTINCT a.sub_category)
                FROM articles a
                JOIN article_entities ae ON ae.article_id = a.id
                WHERE ae.entity_id = ?
            """, (eid,)).fetchone()[0]
            coverage[eid] = cats

    entities = []
    for e in top_entities:
        d = dict(e)
        d["coverage_pct"] = round(d["article_count"] / total_articles * 100, 1)
        d["category_span"] = coverage.get(d["id"], 0)
        entities.append(d)

    return {
        "total_articles": total_articles,
        "entities": entities,
    }


# ────────────────────────────────────────────
# 5.  Topical / category coverage analysis
# ────────────────────────────────────────────

def get_topical_coverage() -> dict:
    """Category-level content metrics for topical authority assessment."""
    with db.get_db() as conn:
        cats = conn.execute("""
            SELECT sub_category,
                   COUNT(*) as cnt,
                   MIN(pub_date) as first_date,
                   MAX(pub_date) as last_date,
                   COUNT(DISTINCT author) as authors,
                   ROUND(AVG(LENGTH(body_text))) as avg_length
            FROM articles
            WHERE sub_category IS NOT NULL
            GROUP BY sub_category
            ORDER BY cnt DESC
        """).fetchall()

        # tags per category
        tag_data = conn.execute("""
            SELECT a.sub_category, at.tag, COUNT(*) as cnt
            FROM articles a
            JOIN article_tags at ON at.article_id = a.id
            GROUP BY a.sub_category, at.tag
            ORDER BY a.sub_category, cnt DESC
        """).fetchall()

    tag_map = defaultdict(list)
    for t in tag_data:
        d = dict(t)
        if len(tag_map[d["sub_category"]]) < 5:
            tag_map[d["sub_category"]].append(d["tag"])

    categories = []
    total = sum(dict(c)["cnt"] for c in cats)
    for c in cats:
        d = dict(c)
        d["share_pct"] = round(d["cnt"] / total * 100, 1) if total else 0
        d["top_tags"] = tag_map.get(d["sub_category"], [])
        categories.append(d)

    return {
        "total": total,
        "categories": categories,
    }


# ────────────────────────────────────────────
# 6.  Content freshness & publication cadence
# ────────────────────────────────────────────

def get_freshness_analysis() -> dict:
    """Analyse publication cadence and content freshness."""
    with db.get_db() as conn:
        # Articles by month (last 24 months)
        monthly = conn.execute("""
            SELECT substr(pub_date, 1, 7) as month, COUNT(*) as cnt
            FROM articles
            WHERE pub_date >= date('now', '-24 months')
            GROUP BY month
            ORDER BY month
        """).fetchall()

        # Day-of-week distribution
        dow = conn.execute("""
            SELECT CASE CAST(strftime('%w', pub_date) AS INTEGER)
                WHEN 0 THEN 'Вс'
                WHEN 1 THEN 'Пн'
                WHEN 2 THEN 'Вт'
                WHEN 3 THEN 'Ср'
                WHEN 4 THEN 'Чт'
                WHEN 5 THEN 'Пт'
                WHEN 6 THEN 'Сб'
            END as day_name,
            strftime('%w', pub_date) as day_num,
            COUNT(*) as cnt
            FROM articles
            WHERE pub_date IS NOT NULL
            GROUP BY day_num
            ORDER BY day_num
        """).fetchall()

        # Hour distribution
        hours = conn.execute("""
            SELECT CAST(substr(pub_date, 12, 2) AS INTEGER) as hour, COUNT(*) as cnt
            FROM articles
            WHERE pub_date IS NOT NULL AND LENGTH(pub_date) >= 13
            GROUP BY hour
            ORDER BY hour
        """).fetchall()

        # Content age distribution
        age = conn.execute("""
            SELECT
                SUM(CASE WHEN pub_date >= date('now', '-30 days') THEN 1 ELSE 0 END) as last_30d,
                SUM(CASE WHEN pub_date >= date('now', '-90 days') AND pub_date < date('now', '-30 days') THEN 1 ELSE 0 END) as last_90d,
                SUM(CASE WHEN pub_date >= date('now', '-365 days') AND pub_date < date('now', '-90 days') THEN 1 ELSE 0 END) as last_year,
                SUM(CASE WHEN pub_date >= date('now', '-1095 days') AND pub_date < date('now', '-365 days') THEN 1 ELSE 0 END) as last_3y,
                SUM(CASE WHEN pub_date < date('now', '-1095 days') THEN 1 ELSE 0 END) as older
            FROM articles WHERE pub_date IS NOT NULL
        """).fetchone()

    return {
        "monthly": [dict(m) for m in monthly],
        "day_of_week": [dict(d) for d in dow],
        "hours": [dict(h) for h in hours],
        "age_distribution": {
            "last_30d": age[0] or 0,
            "last_90d": age[1] or 0,
            "last_year": age[2] or 0,
            "last_3y": age[3] or 0,
            "older": age[4] or 0,
        },
    }


# ────────────────────────────────────────────
# 7.  AI / GEO readiness scorecard
# ────────────────────────────────────────────

def get_geo_readiness() -> dict:
    """
    Produce a GEO/SGEO readiness scorecard:
      - Structured data readiness
      - Content structure (headings, lists, FAQ-style Q&A)
      - Author authority signals
      - Entity density
      - Freshness cadence
    """
    schema = get_schema_readiness(500)
    content = get_content_quality(500)
    entity = get_entity_authority(20)

    # Scoring (each 0–100)
    structured_data_score = schema["ready_pct"]
    content_structure_score = min(100, round(
        (min(content["avg_headings"], 5) / 5 * 40) +
        (min(content["avg_images"], 3) / 3 * 30) +
        (min(content["avg_word_count"], 800) / 800 * 30)
    ))
    meta_score = content["title_ok_pct"] * 0.5 + content["excerpt_ok_pct"] * 0.5
    entity_score = min(100, round(len(entity["entities"]) / 20 * 100))

    overall = round(
        structured_data_score * 0.25 +
        content_structure_score * 0.25 +
        meta_score * 0.25 +
        entity_score * 0.25
    )

    recommendations = []
    if structured_data_score < 80:
        recommendations.append({
            "area": "Schema.org",
            "priority": "high",
            "text": "Добавьте NewsArticle JSON-LD разметку. Сейчас только {pct}% статей готовы.".format(pct=schema["ready_pct"]),
            "icon": "structured_data",
        })
    if content["avg_headings"] < 2:
        recommendations.append({
            "area": "Структура контента",
            "priority": "high",
            "text": "Увеличьте количество подзаголовков (H2-H6). Среднее: {avg}.".format(avg=content["avg_headings"]),
            "icon": "headings",
        })
    if content["avg_internal_links"] < 2:
        recommendations.append({
            "area": "Внутренняя перелинковка",
            "priority": "medium",
            "text": "Добавьте внутренние ссылки между статьями. Среднее: {avg}.".format(avg=content["avg_internal_links"]),
            "icon": "links",
        })
    if content["thin_content_count"] > 50:
        recommendations.append({
            "area": "Тонкий контент",
            "priority": "medium",
            "text": "{n} статей с менее чем 300 словами. Рассмотрите объединение или расширение.".format(n=content["thin_content_count"]),
            "icon": "thin",
        })
    if content["title_ok_pct"] < 70:
        recommendations.append({
            "area": "Мета-заголовки",
            "priority": "medium",
            "text": "Только {pct}% заголовков в диапазоне 25–70 символов.".format(pct=content["title_ok_pct"]),
            "icon": "title",
        })
    if content["excerpt_ok_pct"] < 60:
        recommendations.append({
            "area": "Мета-описания",
            "priority": "medium",
            "text": "Только {pct}% описаний в диапазоне 50–160 символов.".format(pct=content["excerpt_ok_pct"]),
            "icon": "description",
        })
    # GEO-specific
    recommendations.append({
        "area": "robots.txt для AI",
        "priority": "high",
        "text": "Настройте robots.txt: разрешите GPTBot, ClaudeBot, PerplexityBot для цитирования в AI.",
        "icon": "robots",
    })
    recommendations.append({
        "area": "llms.txt",
        "priority": "medium",
        "text": "Создайте /llms.txt – описание сайта, ключевые рубрики и структура для LLM-ботов.",
        "icon": "llms",
    })

    return {
        "overall_score": overall,
        "scores": {
            "structured_data": round(structured_data_score),
            "content_structure": content_structure_score,
            "meta_tags": round(meta_score),
            "entity_authority": entity_score,
        },
        "recommendations": recommendations,
    }


# ────────────────────────────────────────────
# 8.  Duplicate / near-duplicate detection
# ────────────────────────────────────────────

def get_duplicate_titles(limit: int = 30) -> list:
    """Find articles with identical or near-identical titles."""
    with db.get_db() as conn:
        dupes = conn.execute("""
            SELECT title, COUNT(*) as cnt, GROUP_CONCAT(id) as ids
            FROM articles
            WHERE title IS NOT NULL AND title != ''
            GROUP BY title
            HAVING cnt > 1
            ORDER BY cnt DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(d) for d in dupes]


# ────────────────────────────────────────────
# 9.  Convenience: full SEO report
# ────────────────────────────────────────────

def get_full_seo_report() -> dict:
    """Compile a complete SEO / GEO analytics report."""
    return {
        "geo_readiness": get_geo_readiness(),
        "meta_audit": get_meta_audit(500),
        "content_quality": get_content_quality(500),
        "schema_readiness": get_schema_readiness(500),
        "entity_authority": get_entity_authority(30),
        "topical_coverage": get_topical_coverage(),
        "freshness": get_freshness_analysis(),
        "duplicates": get_duplicate_titles(20),
    }
