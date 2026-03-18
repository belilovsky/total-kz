"""FastAPI application — Total.kz Scraper Dashboard."""

import json
from pathlib import Path
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import database as db

app = FastAPI(title="Total.kz Dashboard")

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Category labels in Russian
CATEGORY_LABELS = {
    "vnutrennyaya_politika": "Внутренняя политика",
    "vneshnyaya_politika": "Внешняя политика",
    "mir": "Мир",
    "bezopasnost": "Безопасность",
    "mneniya": "Мнения",
    "ekonomika_sobitiya": "Экономика",
    "biznes": "Бизнес",
    "finansi": "Финансы",
    "gossektor": "Госсектор",
    "tehno": "Технологии",
    "obshchestvo_sobitiya": "Общество",
    "proisshestviya": "Происшествия",
    "zhizn": "Жизнь",
    "kultura": "Культура",
    "religiya": "Религия",
    "den_v_istorii": "День в истории",
    "sport": "Спорт",
    "nauka": "Наука",
    "stil_zhizni": "Стиль жизни",
    "redaktsiya_tandau": "Выбор редакции",
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


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    stats = db.get_stats()
    tags = db.get_tags(limit=60)
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

    # Category-by-year heatmap data
    cat_by_year_json = json.dumps(stats["cat_by_year"])

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats": stats,
        "tags": tags,
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
        "cat_by_year_json": cat_by_year_json,
        "cat_label": cat_label,
        "entity_type_label": entity_type_label,
    })


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


@app.get("/runs", response_class=HTMLResponse)
async def scrape_runs(request: Request):
    stats = db.get_stats()
    return templates.TemplateResponse("runs.html", {
        "request": request,
        "runs": stats["runs"],
    })


@app.get("/entities", response_class=HTMLResponse)
async def entities_page(request: Request, entity_type: str = ""):
    persons = db.get_entities(entity_type="person", limit=50)
    orgs = db.get_entities(entity_type="org", limit=50)
    locations = db.get_entities(entity_type="location", limit=50)
    tags = db.get_tags(limit=100)

    return templates.TemplateResponse("entities.html", {
        "request": request,
        "persons": persons,
        "orgs": orgs,
        "locations": locations,
        "tags": tags,
        "entity_type": entity_type,
        "entity_type_label": entity_type_label,
    })


# API endpoints
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
