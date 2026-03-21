"""FastAPI application – Total.kz v5 (public frontend + admin dashboard)."""

import json
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.gzip import GZipMiddleware

from starlette.middleware.base import BaseHTTPMiddleware

from qazstack.core import health_router

from . import database as db
from . import seo_analytics as seo
from . import search_analytics as search
from .public_routes import router as public_router
from .social_routes import router as social_router


class CacheControlMiddleware(BaseHTTPMiddleware):
    """Add Cache-Control headers for static assets and HTML pages."""
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        path = request.url.path
        # Static assets: long cache (CSS, JS, fonts, images)
        if path.startswith("/static/"):
            if any(path.endswith(ext) for ext in (".css", ".js")):
                response.headers["Cache-Control"] = "public, max-age=604800, stale-while-revalidate=86400"
            elif any(path.endswith(ext) for ext in (".woff2", ".woff", ".ttf", ".otf")):
                response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
            elif any(path.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico")):
                response.headers["Cache-Control"] = "public, max-age=2592000"
        # Image proxy already has its own headers
        elif path.startswith("/img/"):
            pass
        # HTML pages: short cache with revalidation
        elif response.headers.get("content-type", "").startswith("text/html"):
            response.headers["Cache-Control"] = "public, max-age=120, stale-while-revalidate=300"
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses."""
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown."""
    db.init_db()
    yield


app = FastAPI(title="Total.kz", version="5.0.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CacheControlMiddleware)
app.add_middleware(SecurityHeadersMiddleware)

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


def _format_num(n) -> str:
    """Format number with non-breaking space as thousands separator."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return str(n)
    if abs(n) < 1000:
        return str(n)
    s = f"{abs(n):,}".replace(",", "\u00a0")
    return f"-{s}" if n < 0 else s


templates.env.filters["format_num"] = _format_num
templates.env.globals["format_num"] = _format_num

# Category labels in Russian (for admin dashboard)
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





# ══════════════════════════════════════════════
#  HEALTH CHECK
# ══════════════════════════════════════════════
app.include_router(health_router)

# ══════════════════════════════════════════════
#  PUBLIC FRONTEND (mounted at /)
# ══════════════════════════════════════════════
app.include_router(public_router)
app.include_router(social_router)


# ══════════════════════════════════════════════
#  ADMIN DASHBOARD  /admin/
# ══════════════════════════════════════════════

@app.get("/admin", response_class=HTMLResponse)
@app.get("/admin/", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    stats = db.get_stats()
    persons = db.get_entities(entity_type="person", limit=15)
    orgs = db.get_entities(entity_type="org", limit=15)
    locations = db.get_entities(entity_type="location", limit=15)

    chart_months = json.dumps([m["month"] for m in stats["months"]])
    chart_counts = json.dumps([m["cnt"] for m in stats["months"]])
    chart_cats = json.dumps([cat_label(c["sub_category"]) for c in stats["categories"]])
    chart_cat_counts = json.dumps([c["cnt"] for c in stats["categories"]])
    chart_cat_slugs = json.dumps([c["sub_category"] for c in stats["categories"]])
    chart_years = json.dumps([y["year"] for y in stats["years"]])
    chart_year_counts = json.dumps([y["cnt"] for y in stats["years"]])

    heatmap_data = json.dumps(stats.get("cat_by_year", []), ensure_ascii=False)
    cat_labels_json = json.dumps(CATEGORY_LABELS, ensure_ascii=False)

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
        "heatmap_data": heatmap_data,
        "cat_labels_json": cat_labels_json,
        "cat_label": cat_label,
        "entity_type_label": entity_type_label,
    })


@app.get("/admin/articles", response_class=HTMLResponse)
async def admin_articles_list(
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


@app.get("/admin/article/{article_id}", response_class=HTMLResponse)
async def admin_article_detail(request: Request, article_id: int):
    article = db.get_article(article_id)
    if not article:
        return HTMLResponse("Статья не найдена", status_code=404)
    stats = db.get_stats()
    cat_slugs = [c["sub_category"] for c in stats["categories"]]
    return templates.TemplateResponse("article.html", {
        "request": request,
        "article": article,
        "categories": cat_slugs,
        "cat_label": cat_label,
        "entity_type_label": entity_type_label,
    })


@app.get("/admin/content", response_class=HTMLResponse)
async def admin_content_page(request: Request):
    persons = db.get_entities(entity_type="person", limit=50)
    orgs = db.get_entities(entity_type="org", limit=50)
    locations = db.get_entities(entity_type="location", limit=50)
    tags = db.get_tags(limit=100)
    authors = db.get_authors()
    stats = db.get_stats()

    cq = seo.get_content_quality(500)
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


@app.get("/admin/analytics", response_class=HTMLResponse)
async def admin_analytics_page(request: Request):
    gsc = search.get_search_data()
    gsc_json = json.dumps(gsc, ensure_ascii=False)

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
#  REDIRECTS: old admin routes → /admin/*
# ══════════════════════════════════════════════

@app.get("/articles", response_class=RedirectResponse)
async def redirect_articles():
    return RedirectResponse(url="/admin/articles", status_code=301)

@app.get("/article/{article_id}", response_class=RedirectResponse)
async def redirect_article(article_id: int):
    return RedirectResponse(url=f"/admin/article/{article_id}", status_code=301)

@app.get("/content", response_class=RedirectResponse)
async def redirect_content():
    return RedirectResponse(url="/admin/content", status_code=301)

@app.get("/analytics", response_class=RedirectResponse)
async def redirect_analytics():
    return RedirectResponse(url="/admin/analytics", status_code=301)

@app.get("/entities", response_class=RedirectResponse)
async def redirect_entities():
    return RedirectResponse(url="/admin/content", status_code=301)

@app.get("/seo", response_class=RedirectResponse)
async def redirect_seo():
    return RedirectResponse(url="/admin/analytics", status_code=301)

@app.get("/runs", response_class=RedirectResponse)
async def redirect_runs():
    return RedirectResponse(url="/admin", status_code=301)


# ══════════════════════════════════════════════
# API endpoints (keep as-is for compatibility)
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

@app.patch("/api/article/{article_id}")
async def api_update_article(article_id: int, request: Request):
    body = await request.json()
    allowed = {"title", "excerpt", "sub_category", "author", "main_image", "tags"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        return {"ok": False, "error": "Нет полей для обновления"}
    try:
        db.update_article(article_id, updates)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

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

        all_tags_raw = conn.execute("SELECT tag, COUNT(*) as cnt FROM article_tags GROUP BY tag").fetchall()
        tag_groups = defaultdict(list)
        for t in all_tags_raw:
            tag_groups[t[0].lower().strip()].append({"tag": t[0], "count": t[1]})
        tag_dupes = []
        for lower, variants in tag_groups.items():
            if len(variants) > 1:
                tag_dupes.append({"normalized": lower, "variants": variants})
        tag_dupes.sort(key=lambda x: -sum(v["count"] for v in x["variants"]))

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
