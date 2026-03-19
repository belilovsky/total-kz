"""Public frontend routes for Total.kz news portal."""

from datetime import datetime
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from pathlib import Path

from . import database as db

router = APIRouter()

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Category labels
CATEGORY_LABELS = {
    "vnutrennyaya_politika": "Внутренняя политика",
    "vneshnyaya_politika": "Внешняя политика",
    "politika": "Политика",
    "mir": "Мир",
    "bezopasnost": "Безопасность",
    "mneniya": "Мнения",
    "ekonomika_sobitiya": "Экономика",
    "ekonomika": "Экономика",
    "biznes": "Бизнес",
    "finansi": "Финансы",
    "gossektor": "Госсектор",
    "tehno": "Технологии",
    "obshchestvo": "Общество",
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
    "drugoe": "Другое",
}

# Main nav categories (ordered)
NAV_CATEGORIES = [
    "vnutrennyaya_politika", "ekonomika_sobitiya", "obshchestvo_sobitiya",
    "bezopasnost", "finansi", "biznes", "tehno", "mir", "sport",
]


def cat_label(slug: str) -> str:
    return CATEGORY_LABELS.get(slug, slug.replace("_", " ").title())


def article_url(article: dict) -> str:
    """Build new clean URL from article data."""
    url = article.get("url", "")
    # Extract category and slug from old URL
    # Old: https://total.kz/ru/news/{category}/{slug}
    parts = url.replace("https://total.kz/ru/news/", "").strip("/").split("/")
    if len(parts) >= 2:
        return f"/news/{parts[0]}/{parts[1]}"
    elif len(parts) == 1 and parts[0]:
        return f"/news/{parts[0]}"
    return f"/news/article/{article.get('id', 0)}"


def format_date(date_str: str | None) -> str:
    """Format ISO date to Russian readable."""
    if not date_str:
        return ""
    try:
        months = ["января", "февраля", "марта", "апреля", "мая", "июня",
                  "июля", "августа", "сентября", "октября", "ноября", "декабря"]
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return f"{dt.day} {months[dt.month - 1]} {dt.year}, {dt.strftime('%H:%M')}"
    except Exception:
        return date_str[:10] if date_str else ""


def format_date_short(date_str: str | None) -> str:
    """Short date format for cards."""
    if not date_str:
        return ""
    try:
        months = ["янв", "фев", "мар", "апр", "мая", "июн",
                  "июл", "авг", "сен", "окт", "ноя", "дек"]
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return f"{dt.day} {months[dt.month - 1]}, {dt.strftime('%H:%M')}"
    except Exception:
        return date_str[:10] if date_str else ""


# ══════════════════════════════════════════════
#  301 REDIRECTS — old /ru/news/... → new /news/...
# ══════════════════════════════════════════════

@router.get("/ru/news/{category}/{slug}", response_class=RedirectResponse)
async def redirect_old_article(category: str, slug: str):
    """301 redirect from old article URLs to new clean URLs."""
    return RedirectResponse(url=f"/news/{category}/{slug}", status_code=301)


@router.get("/ru/news/{category}", response_class=RedirectResponse)
async def redirect_old_category(category: str):
    """301 redirect from old category URLs to new clean URLs."""
    return RedirectResponse(url=f"/news/{category}", status_code=301)


@router.get("/ru/news", response_class=RedirectResponse)
async def redirect_old_news():
    return RedirectResponse(url="/", status_code=301)


@router.get("/ru", response_class=RedirectResponse)
async def redirect_old_root():
    return RedirectResponse(url="/", status_code=301)


# ══════════════════════════════════════════════
#  PUBLIC PAGES
# ══════════════════════════════════════════════

@router.get("/", response_class=HTMLResponse)
async def homepage(request: Request):
    """Homepage: hero articles + category blocks."""
    hero_articles = db.get_latest_articles(limit=4)
    latest = db.get_latest_articles(limit=20, offset=4)
    trending_tags = db.get_trending_tags(limit=15)
    cat_counts = db.get_category_counts()

    # Get latest 5 articles for each main nav category
    category_blocks = {}
    for cat_slug in NAV_CATEGORIES[:6]:
        result = db.get_latest_by_category(cat_slug, limit=5)
        if result["articles"]:
            category_blocks[cat_slug] = result["articles"]

    return templates.TemplateResponse("public/home.html", {
        "request": request,
        "hero_articles": hero_articles,
        "latest": latest,
        "trending_tags": trending_tags,
        "cat_counts": cat_counts,
        "category_blocks": category_blocks,
        "nav_categories": NAV_CATEGORIES,
        "cat_label": cat_label,
        "article_url": article_url,
        "format_date": format_date,
        "format_date_short": format_date_short,
    })


@router.get("/news/{category}", response_class=HTMLResponse)
async def category_page(
    request: Request,
    category: str,
    page: int = Query(1, ge=1),
):
    """Category listing with pagination."""
    per_page = 20
    offset = (page - 1) * per_page
    result = db.get_latest_by_category(category, limit=per_page, offset=offset)

    if not result["articles"] and page == 1:
        return HTMLResponse("Категория не найдена", status_code=404)

    return templates.TemplateResponse("public/category.html", {
        "request": request,
        "articles": result["articles"],
        "total": result["total"],
        "pages": result["pages"],
        "page": page,
        "category": category,
        "category_name": cat_label(category),
        "nav_categories": NAV_CATEGORIES,
        "cat_label": cat_label,
        "article_url": article_url,
        "format_date": format_date,
        "format_date_short": format_date_short,
    })


@router.get("/news/{category}/{slug}", response_class=HTMLResponse)
async def article_page(request: Request, category: str, slug: str):
    """Single article page."""
    article = db.get_article_by_slug(category, slug)
    if not article:
        return HTMLResponse("Статья не найдена", status_code=404)

    related = db.get_related_articles(article["id"], category, limit=4)

    return templates.TemplateResponse("public/article.html", {
        "request": request,
        "article": article,
        "related": related,
        "category": category,
        "category_name": cat_label(category),
        "nav_categories": NAV_CATEGORIES,
        "cat_label": cat_label,
        "article_url": article_url,
        "format_date": format_date,
        "format_date_short": format_date_short,
    })


@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = "",
    page: int = Query(1, ge=1),
):
    """Search results page."""
    result = db.search_articles(query=q, page=page, per_page=20) if q else {
        "articles": [], "total": 0, "page": 1, "pages": 1, "per_page": 20,
    }

    return templates.TemplateResponse("public/search.html", {
        "request": request,
        "q": q,
        "result": result,
        "nav_categories": NAV_CATEGORIES,
        "cat_label": cat_label,
        "article_url": article_url,
        "format_date": format_date,
        "format_date_short": format_date_short,
    })


@router.get("/tag/{tag_name}", response_class=HTMLResponse)
async def tag_page(
    request: Request,
    tag_name: str,
    page: int = Query(1, ge=1),
):
    """Articles by tag."""
    result = db.search_articles(tag=tag_name, page=page, per_page=20)

    return templates.TemplateResponse("public/search.html", {
        "request": request,
        "q": f"#{tag_name}",
        "result": result,
        "nav_categories": NAV_CATEGORIES,
        "cat_label": cat_label,
        "article_url": article_url,
        "format_date": format_date,
        "format_date_short": format_date_short,
    })


# ══════════════════════════════════════════════
#  SEO: robots.txt, sitemap.xml
# ══════════════════════════════════════════════

@router.get("/robots.txt", response_class=Response)
async def robots_txt():
    content = """User-agent: *
Allow: /
Disallow: /admin/
Disallow: /api/

Sitemap: https://total.kz/sitemap.xml
"""
    return Response(content=content, media_type="text/plain")


@router.get("/sitemap.xml", response_class=Response)
async def sitemap_xml():
    """Dynamic sitemap from database."""
    urls = db.generate_sitemap_urls(limit=50000)

    xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml_parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

    # Homepage
    xml_parts.append("<url><loc>https://total.kz/</loc><changefreq>hourly</changefreq><priority>1.0</priority></url>")

    # Category pages
    seen_cats = set()
    for u in urls:
        cat = u["sub_category"]
        if cat and cat not in seen_cats:
            seen_cats.add(cat)
            xml_parts.append(f"<url><loc>https://total.kz/news/{cat}</loc><changefreq>hourly</changefreq><priority>0.8</priority></url>")

    # Article pages
    for u in urls:
        old_url = u["url"]
        parts = old_url.replace("https://total.kz/ru/news/", "").strip("/").split("/")
        if len(parts) >= 2:
            new_path = f"/news/{parts[0]}/{parts[1]}"
            lastmod = u["pub_date"][:10] if u.get("pub_date") else ""
            xml_parts.append(f"<url><loc>https://total.kz{new_path}</loc>")
            if lastmod:
                xml_parts.append(f"<lastmod>{lastmod}</lastmod>")
            xml_parts.append("<changefreq>monthly</changefreq><priority>0.6</priority></url>")

    xml_parts.append("</urlset>")
    return Response(content="\n".join(xml_parts), media_type="application/xml")
