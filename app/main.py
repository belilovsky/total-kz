"""FastAPI application — Total.kz Scraper Dashboard v4.0."""

import json
from pathlib import Path
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import database as db
from . import seo_analytics as seo
from . import search_analytics as search

app = FastAPI(title="Total.kz Dashboard")

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Category labels in Russian
CATEGORY_LABELS = {
    "vnutrennyaya_politika": "Внутренняя политика",
    "vneshnyaya_politika": "Внешняя политика",
    "politika": "Политика",
    "mir": "Мир",
    "bezopasnost": "Безопасность",
    "mneniya": "Мнения",
    "ekonomika_sobitiya": "Экономика (События)",
    "ekonomika": "Экономика",
    "biznes": "Бизнес",
    "finansi": "Финансы",
    "gossektor": "Госсектор",
    "tehno": "Технологии",
    "obshchestvo": "Общество",
    "obshchestvo_sobitiya": "Общество (События)",
    "proisshestviya": "Происшествия",
    "zhizn": "Жизнь",
    "kultura": "Культура",
    "religiya": "Религия",
    "den_v_istorii": "День в истории",
    "sport": "Спорт",
    "nauka": "Наука",
    "stil_zhizni": "Стиль жизни",
    "redaktsiya_tandau": "Выбор редакции",
    "drugoe": "Другое",
    "vladelets_qz": "Владелец QZ",
    "pisma_dedu_morozu": "Письма Деду Морозу",
}

ENTITY_TYPE_LABELS = {
    "person": "Персоны",
    "org": "Организации",
    "location": "Локации",
    "event": "События",
}


def cat_label(slug: str) -> str:
    return CATEGORY_LABELS.get(slug, slug)


def entity_type_label(t: str) -> str:
    return ENTITY_TYPE_LABELS.get(t, t)


@app.on_event("startup")
def startup():
    db.init_db()


# ══════════════════════════════════════════════
# 1.  ДАШБОРД  /
# ══════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    stats = db.get_stats()
    persons = db.get_entities(entity_type="person", limit=15)
    orgs = db.get_entities(entity_type="org", limit=15)
    locations = db.get_entities(entity_type="location", limit=15)

    # Chart data
    chart_months = json.dumps([m["month"] for m in stats["months"]])
    chart_counts = json.dumps([m["cnt"] for m in stats["months"]])
    chart_cats = json.dumps([cat_label(c["sub_category"]) for c in stats["categories"]])
    chart_cat_counts = json.dumps([c["cnt"] for c in stats["categories"]])
    chart_cat_slugs = json.dumps([c["sub_category"] for c in stats["categories"]])
    chart_years = json.dumps([y["year"] for y in stats["years"]])
    chart_year_counts = json.dumps([y["cnt"] for y in stats["years"]])

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats": stats,
        "persons": persons,
        "orgs": orgs,
        "locations": locations,
        "chart_months": chart_months,
        "chart_counts": chart_counts,
        "chart_cats": chart_cats,
        "chart_cat_counts": chart_cat_counts,
        "chart_cat_slugs": chart_cat_slugs,
        "chart_years": chart_years,
        "chart_year_counts": chart_year_counts,
        "cat_label": cat_label,
        "entity_type_label": entity_type_label,
    })


# ══════════════════════════════════════════════
# 2.  СТАТЬИ  /articles
# ══════════════════════════════════════════════

@app.get("/articles", response_class=HTMLResponse)
async def articles_list(
    request: Request,
    q: str = "",
    category: str = "",
    author: str = "",
    date_from: str = "",
    date_to: str = "",
    tag: str = "",
    entity_id: int = 0,
    page: int = Query(1, ge=1),
):
    result = db.search_articles(
        query=q, category=category, author=author,
        date_from=date_from, date_to=date_to,
        tag=tag, entity_id=entity_id, page=page,
    )
    stats = db.get_stats()
    authors = db.get_authors()
    tags = db.get_tags(limit=60)

    # If filtering by entity, get entity name for display
    entity_name = ""
    entity_type = ""
    if entity_id:
        with db.get_db() as conn:
            row = conn.execute("SELECT name, entity_type FROM entities WHERE id = ?", (entity_id,)).fetchone()
            if row:
                entity_name = row[0]
                entity_type = row[1]

    return templates.TemplateResponse("articles.html", {
        "request": request,
        "result": result,
        "q": q,
        "category": category,
        "author": author,
        "date_from": date_from,
        "date_to": date_to,
        "tag": tag,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "entity_type": entity_type,
        "categories": stats["categories"],
        "authors": authors,
        "tags": tags,
        "cat_label": cat_label,
        "entity_type_label": entity_type_label,
    })


@app.get("/article/{article_id}", response_class=HTMLResponse)
async def article_detail(request: Request, article_id: int):
    article = db.get_article(article_id)
    if not article:
        return HTMLResponse("Статья не найдена", status_code=404)
    return templates.TemplateResponse("article.html", {
        "request": request,
        "article": article,
        "cat_label": cat_label,
        "entity_type_label": entity_type_label,
    })


# ══════════════════════════════════════════════
# 3.  КОНТЕНТ  /content
# ══════════════════════════════════════════════

@app.get("/content", response_class=HTMLResponse)
async def content_page(request: Request):
    persons = db.get_entities(entity_type="person", limit=50)
    orgs = db.get_entities(entity_type="org", limit=50)
    locations = db.get_entities(entity_type="location", limit=50)
    tags = db.get_tags(limit=100)
    authors = db.get_authors()
    stats = db.get_stats()

    # Content quality from SEO module
    cq = seo.get_content_quality(500)

    # Duplicates
    dupes = seo.get_duplicate_titles(20)

    return templates.TemplateResponse("content.html", {
        "request": request,
        "persons": persons,
        "orgs": orgs,
        "locations": locations,
        "tags": tags,
        "authors": authors,
        "total_articles": stats["total"],
        "cq": cq,
        "dupes": dupes,
        "entity_type_label": entity_type_label,
    })


# ══════════════════════════════════════════════
# 4.  АНАЛИТИКА  /analytics
# ══════════════════════════════════════════════

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request):
    # GSC data
    gsc = search.get_search_data()
    gsc_json = json.dumps(gsc, ensure_ascii=False)

    # SEO report parts
    geo = seo.get_geo_readiness()
    schema = seo.get_schema_readiness(500)
    freshness = seo.get_freshness_analysis()

    schema_json = json.dumps(schema, ensure_ascii=False)
    fresh_json = json.dumps(freshness, ensure_ascii=False)

    return templates.TemplateResponse("analytics.html", {
        "request": request,
        "gsc": gsc,
        "gsc_json": gsc_json,
        "geo": geo,
        "schema": schema,
        "schema_json": schema_json,
        "fresh_json": fresh_json,
        "cat_label": cat_label,
    })


# ══════════════════════════════════════════════
# Redirects: старые URL → новые
# ══════════════════════════════════════════════

@app.get("/entities", response_class=RedirectResponse)
async def redirect_entities():
    return RedirectResponse(url="/content", status_code=301)


@app.get("/seo", response_class=RedirectResponse)
async def redirect_seo():
    return RedirectResponse(url="/analytics", status_code=301)


@app.get("/search", response_class=RedirectResponse)
async def redirect_search():
    return RedirectResponse(url="/analytics", status_code=301)


@app.get("/runs", response_class=RedirectResponse)
async def redirect_runs():
    return RedirectResponse(url="/", status_code=301)


# ══════════════════════════════════════════════
# API endpoints (сохраняем все)
# ══════════════════════════════════════════════

@app.get("/api/stats")
async def api_stats():
    return db.get_stats()


@app.get("/api/articles")
async def api_articles(
    q: str = "", category: str = "", author: str = "",
    date_from: str = "", date_to: str = "",
    tag: str = "", entity_id: int = 0, page: int = 1,
):
    return db.search_articles(
        query=q, category=category, author=author,
        date_from=date_from, date_to=date_to,
        tag=tag, entity_id=entity_id, page=page,
    )


@app.get("/api/article/{article_id}")
async def api_article(article_id: int):
    return db.get_article(article_id) or {"error": "not found"}


@app.get("/api/tags")
async def api_tags(limit: int = 100):
    return db.get_tags(limit=limit)


@app.get("/api/entities")
async def api_entities(entity_type: str = "", limit: int = 50):
    return db.get_entities(entity_type=entity_type, limit=limit)


@app.get("/api/search")
async def api_search_data():
    return search.get_search_data()


@app.get("/api/seo")
async def api_seo_report():
    return seo.get_full_seo_report()


@app.get("/api/seo/meta")
async def api_seo_meta(limit: int = 500):
    return seo.get_meta_audit(limit)


@app.get("/api/seo/content")
async def api_seo_content(limit: int = 500):
    return seo.get_content_quality(limit)


@app.get("/api/seo/schema")
async def api_seo_schema(limit: int = 500):
    return seo.get_schema_readiness(limit)


@app.get("/api/seo/geo")
async def api_seo_geo():
    return seo.get_geo_readiness()


@app.get("/api/seo/freshness")
async def api_seo_freshness():
    return seo.get_freshness_analysis()


@app.get("/api/seo/entities")
async def api_seo_entities(limit: int = 30):
    return seo.get_entity_authority(limit)


@app.get("/api/seo/topics")
async def api_seo_topics():
    return seo.get_topical_coverage()


@app.get("/api/seo/duplicates")
async def api_seo_duplicates(limit: int = 30):
    return seo.get_duplicate_titles(limit)


# ── Data Audit endpoint ─────────────────────

@app.get("/api/audit")
async def api_audit():
    """Full data quality audit."""
    from collections import defaultdict
    with db.get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]

        # 1. Content completeness
        fields_check = {}
        for field in ['title', 'body_text', 'body_html', 'excerpt', 'author',
                       'main_image', 'thumbnail', 'image_credit', 'sub_category', 'category_label']:
            empty = conn.execute(
                f"SELECT COUNT(*) FROM articles WHERE {field} IS NULL OR {field} = ''"
            ).fetchone()[0]
            fields_check[field] = {"empty": empty, "pct": round(empty / total * 100, 1) if total else 0}

        # Tags/images from JSON
        no_tags = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE tags IS NULL OR tags = '' OR tags = '[]'"
        ).fetchone()[0]
        fields_check['tags_json'] = {"empty": no_tags, "pct": round(no_tags / total * 100, 1)}

        no_inline = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE inline_images IS NULL OR inline_images = '' OR inline_images = '[]'"
        ).fetchone()[0]
        fields_check['inline_images_json'] = {"empty": no_inline, "pct": round(no_inline / total * 100, 1)}

        # 2. Body text stats
        avg_body = conn.execute(
            "SELECT AVG(LENGTH(body_text)) FROM articles WHERE body_text IS NOT NULL AND body_text != ''"
        ).fetchone()[0]
        short_body = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE LENGTH(body_text) < 100 AND body_text IS NOT NULL AND body_text != ''"
        ).fetchone()[0]

        # 3. Monthly gaps
        months = conn.execute("""
            SELECT substr(pub_date, 1, 7) as month, COUNT(*) as cnt
            FROM articles WHERE pub_date IS NOT NULL
            GROUP BY month ORDER BY month
        """).fetchall()
        monthly = [{"month": r[0], "count": r[1]} for r in months]
        gaps = [m for m in monthly if m["count"] < 200]

        # 4. Categories
        cats = conn.execute("""
            SELECT sub_category, COUNT(*) as cnt
            FROM articles GROUP BY sub_category ORDER BY cnt DESC
        """).fetchall()
        categories = [{"name": r[0] or "(empty)", "count": r[1]} for r in cats]

        # 5. Authors top-30
        authors = conn.execute("""
            SELECT author, COUNT(*) as cnt
            FROM articles WHERE author IS NOT NULL AND author != ''
            GROUP BY author ORDER BY cnt DESC LIMIT 30
        """).fetchall()
        author_list = [{"name": r[0], "count": r[1]} for r in authors]
        unique_authors = conn.execute(
            "SELECT COUNT(DISTINCT author) FROM articles WHERE author IS NOT NULL AND author != ''"
        ).fetchone()[0]

        # 6. Tags audit
        total_tag_links = conn.execute("SELECT COUNT(*) FROM article_tags").fetchone()[0]
        unique_tags = conn.execute("SELECT COUNT(DISTINCT tag) FROM article_tags").fetchone()[0]
        articles_with_tags = conn.execute("SELECT COUNT(DISTINCT article_id) FROM article_tags").fetchone()[0]

        top_tags = conn.execute("""
            SELECT tag, COUNT(*) as cnt FROM article_tags
            GROUP BY tag ORDER BY cnt DESC LIMIT 50
        """).fetchall()
        tag_list = [{"tag": r[0], "count": r[1]} for r in top_tags]

        # Tag duplicates (case variants)
        all_tags_raw = conn.execute("SELECT tag, COUNT(*) as cnt FROM article_tags GROUP BY tag").fetchall()
        tag_groups = defaultdict(list)
        for t in all_tags_raw:
            tag_groups[t[0].lower().strip()].append({"tag": t[0], "count": t[1]})
        tag_dupes = []
        for lower, variants in tag_groups.items():
            if len(variants) > 1:
                tag_dupes.append({"normalized": lower, "variants": variants})
        tag_dupes.sort(key=lambda x: -sum(v["count"] for v in x["variants"]))

        # Latin-only tags
        latin_tags = conn.execute("""
            SELECT tag, COUNT(*) as cnt FROM article_tags
            WHERE tag NOT GLOB '*[а-яА-ЯёЁ]*' AND tag != ''
            GROUP BY tag ORDER BY cnt DESC LIMIT 30
        """).fetchall()
        latin_tag_list = [{"tag": r[0], "count": r[1]} for r in latin_tags]

        # 7. Entities audit
        total_entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        total_entity_links = conn.execute("SELECT COUNT(*) FROM article_entities").fetchone()[0]
        entity_types = conn.execute("""
            SELECT entity_type, COUNT(*) FROM entities GROUP BY entity_type
        """).fetchall()
        entity_type_counts = {r[0]: r[1] for r in entity_types}

        # Top entities per type
        top_entities = {}
        for etype in ['person', 'org', 'location', 'event']:
            top = conn.execute("""
                SELECT e.name, e.normalized, COUNT(ae.article_id) as cnt
                FROM entities e
                JOIN article_entities ae ON ae.entity_id = e.id
                WHERE e.entity_type = ?
                GROUP BY e.id ORDER BY cnt DESC LIMIT 20
            """, (etype,)).fetchall()
            top_entities[etype] = [{"name": r[0], "normalized": r[1], "articles": r[2]} for r in top]

        # Entity duplicates
        entity_dupes = conn.execute("""
            SELECT normalized, entity_type, GROUP_CONCAT(name, ' | ') as names, COUNT(*) as cnt
            FROM entities
            GROUP BY normalized, entity_type
            HAVING cnt > 1
            ORDER BY cnt DESC
            LIMIT 30
        """).fetchall()
        entity_dupe_list = [{"normalized": r[0], "type": r[1], "names": r[2], "variant_count": r[3]} for r in entity_dupes]

        orphan_entities = conn.execute("""
            SELECT COUNT(*) FROM entities e
            LEFT JOIN article_entities ae ON ae.entity_id = e.id
            WHERE ae.article_id IS NULL
        """).fetchone()[0]

        return {
            "total_articles": total,
            "content_completeness": fields_check,
            "body_text_avg_length": int(avg_body or 0),
            "body_text_short_count": short_body,
            "monthly_gaps": gaps,
            "categories": categories,
            "authors": {"top_30": author_list, "unique_count": unique_authors},
            "tags": {
                "total_links": total_tag_links,
                "unique": unique_tags,
                "articles_with_tags": articles_with_tags,
                "top_50": tag_list,
                "case_duplicates": tag_dupes[:30],
                "latin_only": latin_tag_list,
            },
            "entities": {
                "total": total_entities,
                "total_links": total_entity_links,
                "by_type": entity_type_counts,
                "top_by_type": top_entities,
                "duplicates": entity_dupe_list,
                "orphans": orphan_entities,
            },
        }
