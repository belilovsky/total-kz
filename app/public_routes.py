"""Public frontend routes for Total.kz news portal."""

import hashlib
import httpx
import logging
from datetime import datetime
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, Response, FileResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from . import database as db

logger = logging.getLogger(__name__)

router = APIRouter()

# ══════════════════════════════════════════════
#  IMAGE PROXY — serve images locally with disk cache
# ══════════════════════════════════════════════
IMAGE_CACHE_DIR = Path(__file__).parent.parent / "data" / "img_cache"
IMAGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

ORIGIN = "https://total.kz/storage"

# Content type mapping
EXT_CONTENT_TYPE = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
}


def rewrite_image_url(url: str | None) -> str | None:
    """Pass through image URLs directly (proxy disabled for now).
    Images load from https://total.kz/storage/ via Cloudflare CDN.
    Proxy can be re-enabled later for self-hosting."""
    return url


def rewrite_article_images(article: dict) -> dict:
    """Pass through article images (proxy disabled for now)."""
    return article


def rewrite_articles_images(articles: list) -> list:
    """Rewrite image URLs in a list of articles."""
    return [rewrite_article_images(a) for a in articles]

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")


def _error_response(request: Request, status_code: int = 503):
    """Return an error page response for DB or other failures."""
    return templates.TemplateResponse("public/404.html", {
        "request": request,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
        "cat_label": cat_label,
        "nav_slug_for": nav_slug_for,
        "article_url": article_url,
        "format_date": format_date,
        "format_date_short": format_date_short,
    }, status_code=status_code)


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

# ── Smart grouped navigation (covers 100% of content) ──
# Each nav section maps to multiple journalist-assigned sub_categories
NAV_SECTIONS = [
    {
        "slug": "politika",
        "label": "Политика",
        "subcats": ["vnutrennyaya_politika", "vneshnyaya_politika", "gossektor"],
    },
    {
        "slug": "ekonomika",
        "label": "Экономика",
        "subcats": ["ekonomika_sobitiya", "finansi", "biznes"],
    },
    {
        "slug": "obshchestvo",
        "label": "Общество",
        "subcats": ["obshchestvo_sobitiya", "zhizn", "proisshestviya", "bezopasnost", "stil_zhizni", "religiya"],
    },
    {
        "slug": "nauka",
        "label": "Наука и Техно",
        "subcats": ["tehno", "nauka"],
    },
    {
        "slug": "mir",
        "label": "Мир",
        "subcats": ["mir"],
    },
    {
        "slug": "sport",
        "label": "Спорт",
        "subcats": ["sport"],
    },
]

# Lookup: nav slug → subcats list
NAV_SLUG_MAP = {s["slug"]: s["subcats"] for s in NAV_SECTIONS}
# Lookup: subcat → nav slug (for breadcrumbs, badges)
SUBCAT_TO_NAV = {}
for section in NAV_SECTIONS:
    for sc in section["subcats"]:
        SUBCAT_TO_NAV[sc] = section["slug"]

# Legacy: flat list for templates that still need it
NAV_CATEGORIES = [s["slug"] for s in NAV_SECTIONS]


def cat_label(slug: str) -> str:
    """Get human label for any slug (nav section or sub_category)."""
    # Check nav sections first
    for s in NAV_SECTIONS:
        if s["slug"] == slug:
            return s["label"]
    return CATEGORY_LABELS.get(slug, slug.replace("_", " ").title())


def nav_slug_for(subcat: str) -> str:
    """Get the nav section slug for a sub_category."""
    return SUBCAT_TO_NAV.get(subcat, subcat)


def pluralize_articles(n: int) -> str:
    """Russian pluralization for articles count."""
    if 11 <= n % 100 <= 19:
        return f"{n} статей"
    last = n % 10
    if last == 1:
        return f"{n} статья"
    elif 2 <= last <= 4:
        return f"{n} статьи"
    return f"{n} статей"


def estimate_reading_time(text: str | None) -> int:
    """Estimate reading time in minutes."""
    if not text:
        return 1
    words = len(text.split())
    return max(1, round(words / 200))


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


# ── Register template utilities as Jinja2 globals ──
templates.env.globals["format_date"] = format_date
templates.env.globals["format_date_short"] = format_date_short
templates.env.globals["cat_label"] = cat_label
templates.env.globals["nav_slug_for"] = nav_slug_for
templates.env.globals["article_url"] = article_url
templates.env.globals["pluralize_articles"] = pluralize_articles
templates.env.globals["current_year"] = lambda: datetime.now().year


# ══════════════════════════════════════════════
#  IMAGE PROXY ENDPOINT
# ══════════════════════════════════════════════

@router.get("/img/{path:path}")
async def image_proxy(path: str):
    """Proxy and cache images from total.kz/storage/."""
    # Sanitize path
    if ".." in path or path.startswith("/"):
        return Response(status_code=400)

    # Determine cache path
    cache_path = IMAGE_CACHE_DIR / path

    # Serve from cache if exists
    if cache_path.exists():
        ext = cache_path.suffix.lower()
        ct = EXT_CONTENT_TYPE.get(ext, "image/jpeg")
        return FileResponse(
            cache_path,
            media_type=ct,
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )

    # Fetch from origin
    origin_url = f"{ORIGIN}/{path}"
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(origin_url)
        if resp.status_code != 200:
            return Response(status_code=resp.status_code)

        # Save to cache
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(resp.content)

        ext = cache_path.suffix.lower()
        ct = EXT_CONTENT_TYPE.get(ext, resp.headers.get("content-type", "image/jpeg"))
        return Response(
            content=resp.content,
            media_type=ct,
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )
    except Exception:
        return Response(status_code=502)


# ══════════════════════════════════════════════
#  301 REDIRECTS — old /ru/news/... → new /news/...
# ══════════════════════════════════════════════

@router.get("/ru/page/{page_slug}", response_class=RedirectResponse)
async def redirect_old_page(page_slug: str):
    """301 redirect from old /ru/page/... to new /page/..."""
    return RedirectResponse(url=f"/page/{page_slug}", status_code=301)


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
    """Homepage: hero + category highlights + chronological feed."""
    try:
        hero_articles = rewrite_articles_images(db.get_latest_articles(limit=5))
        latest = rewrite_articles_images(db.get_latest_articles(limit=30, offset=5))

        # Fetch 3 latest articles per nav section for category highlights
        category_highlights = []
        for section in NAV_SECTIONS:
            result = db.get_latest_by_categories(section["subcats"], limit=3, offset=0)
            if result["articles"]:
                category_highlights.append({
                    "slug": section["slug"],
                    "label": section["label"],
                    "articles": rewrite_articles_images(result["articles"]),
                })
    except Exception:
        logger.exception("Database error in homepage")
        return _error_response(request)

    return templates.TemplateResponse("public/home.html", {
        "request": request,
        "hero_articles": hero_articles,
        "latest": latest,
        "category_highlights": category_highlights,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


@router.get("/news/{category}", response_class=HTMLResponse)
async def category_page(
    request: Request,
    category: str,
    page: int = Query(1, ge=1),
):
    """Category listing — handles both nav section slugs and legacy sub_category slugs."""
    try:
        per_page = 20
        offset = (page - 1) * per_page

        # Check if this is a grouped nav section
        if category in NAV_SLUG_MAP:
            subcats = NAV_SLUG_MAP[category]
            result = db.get_latest_by_categories(subcats, limit=per_page, offset=offset)
        else:
            # Legacy: direct sub_category slug
            result = db.get_latest_by_category(category, limit=per_page, offset=offset)
    except Exception:
        logger.exception("Database error in category_page for %s", category)
        return _error_response(request)

    if not result["articles"] and page == 1:
        return templates.TemplateResponse("public/404.html", {
            "request": request,
            "nav_sections": NAV_SECTIONS,
            "nav_categories": NAV_CATEGORIES,
        }, status_code=404)

    return templates.TemplateResponse("public/category.html", {
        "request": request,
        "articles": rewrite_articles_images(result["articles"]),
        "total": result["total"],
        "pages": result["pages"],
        "page": page,
        "category": category,
        "category_name": cat_label(category),
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


@router.get("/news/{category}/{slug}", response_class=HTMLResponse)
async def article_page(request: Request, category: str, slug: str):
    """Single article page."""
    try:
        article = db.get_article_by_slug(category, slug)
    except Exception:
        logger.exception("Database error in article_page for %s/%s", category, slug)
        return _error_response(request)

    if not article:
        return templates.TemplateResponse("public/404.html", {
            "request": request,
            "nav_sections": NAV_SECTIONS,
            "nav_categories": NAV_CATEGORIES,
        }, status_code=404)

    rewrite_article_images(article)

    # Entity IDs for smart matching (timeline + related)
    entity_ids = [e["id"] for e in article.get("entities", [])]

    try:
        # Related: by shared entities first, then fill from category (6 cards)
        related = rewrite_articles_images(
            db.get_related_by_entities(article["id"], entity_ids, category, limit=6)
        )

        # Timeline: by the most specific entity (person/org > location)
        # Pick the best entity for timeline context
        timeline_entity = None
        if article.get("entities"):
            # Priority: person > org > location
            priority = {"person": 0, "org": 1, "location": 2}
            sorted_ents = sorted(
                article["entities"],
                key=lambda e: priority.get(e.get("entity_type", "location"), 3)
            )
            timeline_entity = sorted_ents[0] if sorted_ents else None

        timeline_entity_ids = [timeline_entity["id"]] if timeline_entity else None
        timeline_raw = db.get_timeline_articles(
            article["id"], category, article.get("pub_date", ""),
            entity_ids=timeline_entity_ids
        )
        timeline = {
            "prev": rewrite_articles_images(timeline_raw["prev"]),
            "next": rewrite_articles_images(timeline_raw["next"]),
        }
        timeline_topic = timeline_entity["name"] if timeline_entity else ""
    except Exception:
        logger.exception("Database error loading related/timeline for %s/%s", category, slug)
        related = []
        timeline = {"prev": [], "next": []}
        timeline_topic = ""

    # Extract slug from article URL for share buttons
    article_slug = article.get("url", "").replace(
        f"https://total.kz/ru/news/{category}/", ""
    ).strip("/")

    # Resolve nav section for this sub_category
    nav_section = nav_slug_for(category)

    return templates.TemplateResponse("public/article.html", {
        "request": request,
        "article": article,
        "related": related,
        "timeline": timeline,
        "timeline_topic": timeline_topic,
        "category": category,
        "category_name": cat_label(category),
        "nav_section": nav_section,
        "nav_section_name": cat_label(nav_section),
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
        "reading_time": estimate_reading_time(article.get("body_text", "")),
        "slug": article_slug,
    })


@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = "",
    page: int = Query(1, ge=1),
):
    """Search results page."""
    try:
        result = db.search_articles(query=q, page=page, per_page=20) if q else {
            "articles": [], "total": 0, "page": 1, "pages": 1, "per_page": 20,
        }
        if result.get("articles"):
            result["articles"] = rewrite_articles_images(result["articles"])

        # Pass popular tags for empty search page
        popular_tags = db.get_trending_tags(limit=20) if not q else None
    except Exception:
        logger.exception("Database error in search_page for q=%s", q)
        return _error_response(request)

    return templates.TemplateResponse("public/search.html", {
        "request": request,
        "q": q,
        "result": result,
        "popular_tags": popular_tags,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


@router.get("/tag/{tag_name}", response_class=HTMLResponse)
async def tag_page(
    request: Request,
    tag_name: str,
    page: int = Query(1, ge=1),
):
    """Articles by tag."""
    result = db.search_articles(tag=tag_name, page=page, per_page=20)
    if result.get("articles"):
        result["articles"] = rewrite_articles_images(result["articles"])

    return templates.TemplateResponse("public/search.html", {
        "request": request,
        "q": f"#{tag_name}",
        "result": result,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


ENTITY_TYPE_LABELS = {
    "person": "Персона",
    "org": "Организация",
    "location": "Локация",
}


@router.get("/entity/{entity_id}", response_class=HTMLResponse)
async def entity_page(
    request: Request,
    entity_id: int,
    page: int = Query(1, ge=1),
):
    """Articles linked to an entity."""
    try:
        entity = db.get_entity(entity_id)
    except Exception:
        logger.exception("Database error in entity_page for entity_id=%s", entity_id)
        return _error_response(request)

    if not entity:
        return templates.TemplateResponse("public/404.html", {
            "request": request,
            "nav_sections": NAV_SECTIONS,
            "nav_categories": NAV_CATEGORIES,
        }, status_code=404)

    try:
        result = db.get_articles_by_entity(entity_id, page=page, per_page=20)
        if result.get("articles"):
            result["articles"] = rewrite_articles_images(result["articles"])
    except Exception:
        logger.exception("Database error loading articles for entity_id=%s", entity_id)
        result = {"articles": [], "total": 0, "page": 1, "pages": 1}
    type_label = ENTITY_TYPE_LABELS.get(entity["entity_type"], entity["entity_type"])

    return templates.TemplateResponse("public/entity.html", {
        "request": request,
        "entity": entity,
        "type_label": type_label,
        "result": result,
        "page": page,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


# ══════════════════════════════════════════════
#  STATIC PAGES
# ══════════════════════════════════════════════

STATIC_PAGES = {
    "reklama": {"title": "Реклама", "template": "public/page_reklama.html"},
    "pravila": {"title": "Правила использования материалов", "template": "public/page_pravila.html"},
    "contacts": {"title": "Контакты", "template": "public/page_contacts.html"},
}


@router.get("/page/{page_slug}", response_class=HTMLResponse)
async def static_page(request: Request, page_slug: str):
    """Static content pages: reklama, pravila, contacts."""
    page = STATIC_PAGES.get(page_slug)
    if not page:
        return templates.TemplateResponse("public/404.html", {
            "request": request,
            "nav_sections": NAV_SECTIONS,
            "nav_categories": NAV_CATEGORIES,
        }, status_code=404)

    return templates.TemplateResponse(page["template"], {
        "request": request,
        "page_title": page["title"],
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
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
    try:
        urls = db.generate_sitemap_urls(limit=50000)
    except Exception:
        logger.exception("Database error in sitemap_xml")
        return Response(content="Service unavailable", status_code=503)

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
