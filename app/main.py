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


def cat_label(slug: str) -> str:
    return CATEGORY_LABELS.get(slug, slug)


@app.on_event("startup")
def startup():
    db.init_db()


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    stats = db.get_stats()
    # Prepare chart data
    chart_months = json.dumps([m["month"] for m in stats["months"]])
    chart_counts = json.dumps([m["cnt"] for m in stats["months"]])
    chart_cats = json.dumps([cat_label(c["sub_category"]) for c in stats["categories"][:10]])
    chart_cat_counts = json.dumps([c["cnt"] for c in stats["categories"][:10]])
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats": stats,
        "chart_months": chart_months,
        "chart_counts": chart_counts,
        "chart_cats": chart_cats,
        "chart_cat_counts": chart_cat_counts,
        "cat_label": cat_label,
    })


@app.get("/articles", response_class=HTMLResponse)
async def articles_list(
    request: Request,
    q: str = "",
    category: str = "",
    author: str = "",
    date_from: str = "",
    date_to: str = "",
    page: int = Query(1, ge=1),
):
    result = db.search_articles(
        query=q, category=category, author=author,
        date_from=date_from, date_to=date_to, page=page,
    )
    stats = db.get_stats()
    authors = db.get_authors()
    return templates.TemplateResponse("articles.html", {
        "request": request,
        "result": result,
        "q": q,
        "category": category,
        "author": author,
        "date_from": date_from,
        "date_to": date_to,
        "categories": stats["categories"],
        "authors": authors,
        "cat_label": cat_label,
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
    })


@app.get("/runs", response_class=HTMLResponse)
async def scrape_runs(request: Request):
    stats = db.get_stats()
    return templates.TemplateResponse("runs.html", {
        "request": request,
        "runs": stats["runs"],
    })


# API endpoints for future use
@app.get("/api/stats")
async def api_stats():
    return db.get_stats()


@app.get("/api/articles")
async def api_articles(
    q: str = "", category: str = "", author: str = "",
    date_from: str = "", date_to: str = "", page: int = 1,
):
    return db.search_articles(
        query=q, category=category, author=author,
        date_from=date_from, date_to=date_to, page=page,
    )


@app.get("/api/article/{article_id}")
async def api_article(article_id: int):
    return db.get_article(article_id) or {"error": "not found"}
