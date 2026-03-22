"""Public frontend routes for Total.kz news portal."""

import hashlib
import httpx
import logging
import re
import sqlite3
from datetime import datetime
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, Response, FileResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from qazstack.content import reading_time_minutes, slug_from_url, category_from_url

from . import db_backend as db

logger = logging.getLogger(__name__)

router = APIRouter()

# ══════════════════════════════════════════════
#  IMAGE PROXY – serve images locally with disk cache
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
    """Rewrite total.kz/storage/... URLs to local /img/... proxy."""
    if not url:
        return url
    if url.startswith("https://total.kz/storage/"):
        return url.replace("https://total.kz/storage/", "/img/")
    if url.startswith("http://total.kz/storage/"):
        return url.replace("http://total.kz/storage/", "/img/")
    return url


def rewrite_article_images(article: dict) -> dict:
    """Rewrite image URLs in an article dict."""
    if article.get("main_image"):
        article["main_image"] = rewrite_image_url(article["main_image"])
    if article.get("thumbnail"):
        article["thumbnail"] = rewrite_image_url(article["thumbnail"])
    # Rewrite inline images in body_html
    if article.get("body_html"):
        article["body_html"] = (
            article["body_html"]
            .replace("https://total.kz/storage/", "/img/")
            .replace("http://total.kz/storage/", "/img/")
        )
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
        "format_date_day": format_date_day,
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
        "subcats": ["obshchestvo_sobitiya", "obshchestvo", "zhizn", "proisshestviya", "bezopasnost", "stil_zhizni", "religiya", "kultura", "mneniya", "den_v_istorii"],
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


def format_num(n) -> str:
    """Format number with non-breaking space as thousands separator.
    E.g. 33258 → '33\u00a0258', 1500 → '1\u00a0500'.
    """
    try:
        n = int(n)
    except (TypeError, ValueError):
        return str(n)
    if abs(n) < 1000:
        return str(n)
    s = f"{abs(n):,}".replace(",", "\u00a0")
    return f"-{s}" if n < 0 else s


def pluralize_articles(n: int) -> str:
    """Russian pluralization for articles count."""
    formatted = format_num(n)
    if 11 <= n % 100 <= 19:
        return f"{formatted} статей"
    last = n % 10
    if last == 1:
        return f"{formatted} статья"
    elif 2 <= last <= 4:
        return f"{formatted} статьи"
    return f"{formatted} статей"


def pluralize_materials(n: int) -> str:
    """Russian pluralization for materials count."""
    formatted = format_num(n)
    if 11 <= n % 100 <= 19:
        return f"{formatted} материалов"
    last = n % 10
    if last == 1:
        return f"{formatted} материал"
    elif 2 <= last <= 4:
        return f"{formatted} материала"
    return f"{formatted} материалов"


def estimate_reading_time(text: str | None) -> int:
    """Estimate reading time in minutes (delegates to qazstack.content)."""
    return reading_time_minutes(text or "")


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


def _parse_datetime(date_str: str) -> datetime | None:
    """Parse a date string trying multiple formats. Returns None on failure."""
    dt = None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        pass
    if dt is None:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                break
            except (ValueError, TypeError):
                continue
    if dt is None:
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
        if m:
            try:
                dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                pass
    return dt


def format_date(date_str: str | None) -> str:
    """Format ISO date to Russian readable (full month names).
    Today:           12 марта, 14:14
    Yesterday+/year: 12 марта
    Past years:      12 марта 2024
    """
    if not date_str:
        return ""
    months = ["января", "февраля", "марта", "апреля", "мая", "июня",
              "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    dt = _parse_datetime(date_str)
    if dt is None:
        return date_str[:10] if len(date_str) >= 10 else date_str
    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    if dt.date() == now.date():
        return f"{dt.day} {months[dt.month - 1]}, {dt.strftime('%H:%M')}"
    elif dt.year == now.year:
        return f"{dt.day} {months[dt.month - 1]}"
    else:
        return f"{dt.day} {months[dt.month - 1]} {dt.year}"


def format_date_short(date_str: str | None) -> str:
    """Short date format for cards (abbreviated months).
    Today:           12 мар, 14:14
    Yesterday+/year: 12 мар
    Past years:      12 мар, 2024
    """
    if not date_str:
        return ""
    months = ["янв", "фев", "мар", "апр", "мая", "июн",
              "июл", "авг", "сен", "окт", "ноя", "дек"]
    dt = _parse_datetime(date_str)
    if dt is None:
        return date_str[:10] if len(date_str) >= 10 else date_str
    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    if dt.date() == now.date():
        return f"{dt.day} {months[dt.month - 1]}, {dt.strftime('%H:%M')}"
    elif dt.year == now.year:
        return f"{dt.day} {months[dt.month - 1]}"
    else:
        return f"{dt.day} {months[dt.month - 1]}, {dt.year}"


def format_date_day(date_str: str | None) -> str:
    """Date-only format for timelines — no time, no year if current.
    Current year:  21 мар
    Past years:    12 мар, 2024
    """
    if not date_str:
        return ""
    months = ["янв", "фев", "мар", "апр", "мая", "июн",
              "июл", "авг", "сен", "окт", "ноя", "дек"]
    dt = _parse_datetime(date_str)
    if dt is None:
        return date_str[:10] if len(date_str) >= 10 else date_str
    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    if dt.year == now.year:
        return f"{dt.day} {months[dt.month - 1]}"
    else:
        return f"{dt.day} {months[dt.month - 1]}, {dt.year}"


def short_entity_name(entity: dict) -> str:
    """Short display name for entity pills.
    Strips АО, НК, ТОО, НАО, ОО prefixes and quotes from org names.
    E.g. 'АО \u00abНК \u00abКТЖ\u00bb' → 'КТЖ'
    """
    name = entity.get("short_name") or entity.get("name") or ""
    if entity.get("entity_type") == "org":
        # Strip organizational prefixes and quotes
        import re as _re
        cleaned = _re.sub(
            r'^(?:АО|НК|ТОО|НАО|ОО|ГКП|ГКПП|РГП|РГКП|КГП)\s*',
            '', name
        )
        # Remove guillemets and curly quotes
        cleaned = cleaned.replace('\u00ab', '').replace('\u00bb', '')
        cleaned = cleaned.replace('\u201c', '').replace('\u201d', '')
        cleaned = cleaned.replace('"', '')
        cleaned = cleaned.strip()
        # If we stripped too much (empty), fall back
        if cleaned:
            # Recursively strip again (for АО «НК «КТЖ»»)
            cleaned2 = _re.sub(
                r'^(?:АО|НК|ТОО|НАО|ОО|ГКП)\s*',
                '', cleaned
            ).strip()
            return cleaned2 if cleaned2 else cleaned
    return name


# ── Register template utilities as Jinja2 globals ──
templates.env.globals["format_date"] = format_date
templates.env.globals["format_date_short"] = format_date_short
templates.env.globals["format_date_day"] = format_date_day
templates.env.globals["cat_label"] = cat_label
templates.env.globals["nav_slug_for"] = nav_slug_for
templates.env.globals["article_url"] = article_url
templates.env.globals["pluralize_articles"] = pluralize_articles
templates.env.globals["pluralize_materials"] = pluralize_materials
templates.env.globals["format_num"] = format_num
templates.env.globals["short_entity_name"] = short_entity_name

# Currency rates (live from NB RK, cached 1h)
from app.currency import get_rates as _get_currency_rates, get_commodities as _get_commodities
templates.env.globals["get_currency_rates"] = _get_currency_rates
templates.env.globals["get_commodities"] = _get_commodities

# Weather (wttr.in, cached 30 min)
from app.weather import get_weather as _get_weather
templates.env.globals["get_weather"] = _get_weather


def _names_match(a: str, b: str) -> bool:
    """Check if two names refer to the same person/org (fuzzy).
    Handles Russian case/gender forms: Шведов/Шведова, Токаев/Токаеву.
    Strategy: compare word-by-word; words match if one starts with the other
    (after trimming last 1-2 chars as flex endings)."""
    wa = a.lower().split()
    wb = b.lower().split()
    if len(wa) != len(wb):
        return False
    for pa, pb in zip(wa, wb):
        # Exact match
        if pa == pb:
            continue
        # Prefix match: shorter must be prefix of longer (minus flex)
        short, long = (pa, pb) if len(pa) <= len(pb) else (pb, pa)
        # The longer word should start with at least len(short)-1 chars of short
        min_prefix = max(3, len(short) - 1)
        if long[:min_prefix] != short[:min_prefix]:
            return False
    return True


def dedup_entities(entities: list, max_count: int = 5) -> list:
    """Deduplicate entities by fuzzy name match, keep max_count, only person/org.
    Merges gender/case forms: Токаев/Токаеву, Шведова/Шведов."""
    result = []
    for ent in entities:
        if ent.get("entity_type") not in ("person", "org"):
            continue
        sn = short_entity_name(ent).strip()
        if not sn:
            continue
        # Check against already accepted names
        duplicate = False
        for existing in result:
            existing_sn = short_entity_name(existing).strip()
            if _names_match(sn, existing_sn):
                duplicate = True
                break
        if duplicate:
            continue
        result.append(ent)
        if len(result) >= max_count:
            break
    return result


def imgproxy_url(source_url: str, width: int = 800) -> str:
    """Generate imgproxy URL for an image."""
    if not source_url or not source_url.startswith("http"):
        return source_url or ""
    return f"/imgproxy/insecure/resize:fit:{width}:0/plain/{source_url}@webp"


templates.env.globals["dedup_entities"] = dedup_entities
templates.env.globals["current_year"] = lambda: datetime.now().year
templates.env.globals["imgproxy_url"] = imgproxy_url
templates.env.filters["format_num"] = format_num


def get_views_func(article):
    """Get real view count from article dict or fallback to deterministic fake."""
    if isinstance(article, dict):
        v = article.get("views", 0) or 0
        if v > 0:
            return v
        aid = article.get("id", 0)
    else:
        aid = article
        v = 0
    # Fallback for articles without views yet
    if v <= 0:
        h = int(hashlib.md5(str(aid).encode()).hexdigest()[:8], 16)
        return h % 4900 + 100
    return v


templates.env.filters["fake_views"] = get_views_func
templates.env.globals["fake_views"] = get_views_func
templates.env.filters["get_views"] = get_views_func
templates.env.globals["get_views"] = get_views_func


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
#  301 REDIRECTS – old /ru/news/... → new /news/...
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

    popular = sorted(latest, key=lambda a: get_views_func(a), reverse=True)

    return templates.TemplateResponse("public/home.html", {
        "request": request,
        "hero_articles": hero_articles,
        "latest": latest,
        "popular": popular,
        "category_highlights": category_highlights,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
        "ticker_articles": hero_articles[:5],
    })


@router.get("/api/feed", response_class=HTMLResponse)
async def api_feed_more(request: Request, offset: int = Query(30, ge=0), limit: int = Query(20, ge=1, le=50)):
    """Return more feed items as HTML fragment for infinite scroll / load-more."""
    try:
        articles = rewrite_articles_images(db.get_latest_articles(limit=limit, offset=offset + 5))  # +5 for hero
    except Exception:
        return HTMLResponse("")
    if not articles:
        return HTMLResponse("")
    html_parts = []
    for art in articles:
        cat_slug = nav_slug_for(art.get("sub_category", ""))
        cat = cat_label(cat_slug)
        img = imgproxy_url(art.get("main_image") or art.get("thumbnail", ""), 400)
        url = article_url(art)
        views = format_num(get_views_func(art))
        date_s = format_date_short(art.get("pub_date", ""))
        excerpt = (art.get("excerpt") or "")[:140]
        if len(art.get("excerpt") or "") > 140:
            excerpt += "\u2026"
        thumb_html = f'<div class="feed-item-thumb"><img src="{img}" alt="" loading="lazy"></div>' if img else '<div class="feed-item-thumb feed-item-thumb--ph"></div>'
        html_parts.append(f'''
        <article class="feed-item">
          <div class="feed-item-body">
            <div class="feed-item-meta">
              <span class="cat-badge" data-cat="{cat_slug}"><span class="cat-dot"></span><span class="cat-name">{cat}</span></span>
              <span class="sep">\u00b7</span><span>{date_s}</span>
              <span class="view-count"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>{views}</span>
            </div>
            <h3 class="feed-item-title"><a href="{url}">{art["title"]}</a></h3>
            {f"<p class='feed-item-excerpt'>{excerpt}</p>" if excerpt else ""}
          </div>
          {thumb_html}
        </article>''')
    return HTMLResponse("\n".join(html_parts))


@router.get("/news/{category}", response_class=HTMLResponse)
async def category_page(
    request: Request,
    category: str,
    page: int = Query(1, ge=1),
):
    """Category listing – handles both nav section slugs and legacy sub_category slugs."""
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

    articles_list = rewrite_articles_images(result["articles"])
    return templates.TemplateResponse("public/category.html", {
        "request": request,
        "articles": articles_list,
        "total": result["total"],
        "pages": result["pages"],
        "page": page,
        "category": category,
        "category_name": cat_label(category),
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
        "ticker_articles": articles_list[:5],
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

    # ── Strip duplicate lead: if body_html starts with the excerpt text,
    #    remove that first paragraph so it isn't shown twice.
    excerpt = (article.get("excerpt") or "").strip()
    body_html = (article.get("body_html") or "").strip()
    if excerpt and body_html:
        # Check if body starts with a <p> containing the excerpt text
        m = re.match(r'^<p[^>]*>(.*?)</p>', body_html, re.DOTALL | re.IGNORECASE)
        if m:
            first_p_text = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            # Compare normalized (no extra whitespace)
            norm_exc = ' '.join(excerpt.split())
            norm_fp = ' '.join(first_p_text.split())
            if norm_exc and norm_fp and (
                norm_fp.startswith(norm_exc) or norm_exc.startswith(norm_fp)
                or norm_fp == norm_exc
            ):
                article["body_html"] = body_html[m.end():].lstrip()

    # Entity IDs for smart matching (timeline + related)
    entity_ids = [e["id"] for e in article.get("entities", [])]

    # ── Related articles (independent try/except) ──
    try:
        related = rewrite_articles_images(
            db.get_related_by_entities(article["id"], entity_ids, category, limit=6)
        )
    except Exception:
        logger.exception("Error loading related for %s/%s", category, slug)
        related = []

    # ── Timeline (independent try/except) ──
    timeline = {"prev": [], "next": []}
    timeline_topic = ""
    timeline_total = 0
    try:
        story_tl = db.get_story_timeline(article["id"], article.get("pub_date", ""))
        if story_tl and (story_tl["prev"] or story_tl["next"]):
            timeline = {
                "prev": rewrite_articles_images(story_tl["prev"]),
                "next": rewrite_articles_images(story_tl["next"]),
            }
            timeline_topic = story_tl["story_title"]
            timeline_total = story_tl["total_articles"]
        else:
            # Fallback: entity-based timeline
            timeline_entity = None
            if article.get("entities"):
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
            timeline_total = 0
    except Exception:
        logger.exception("Error loading timeline for %s/%s", category, slug)

    # Extract slug from article URL for share buttons
    article_slug = article.get("url", "").replace(
        f"https://total.kz/ru/news/{category}/", ""
    ).strip("/")

    # Resolve nav section for this sub_category
    nav_section = nav_slug_for(category)

    # Ticker: use related articles, fallback to latest
    ticker_articles = related[:5] if related else []

    return templates.TemplateResponse("public/article.html", {
        "request": request,
        "article": article,
        "related": related,
        "timeline": timeline,
        "timeline_topic": timeline_topic,
        "timeline_total": timeline_total,
        "category": category,
        "category_name": cat_label(category),
        "nav_section": nav_section,
        "nav_section_name": cat_label(nav_section),
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
        "reading_time": estimate_reading_time(article.get("body_text", "")),
        "slug": article_slug,
        "ticker_articles": ticker_articles,
    })


@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = "",
    page: int = Query(1, ge=1),
):
    """Search results page — uses Meilisearch with SQLite fallback."""
    meili_results = None
    try:
        if q:
            # Try Meilisearch first
            try:
                from . import search_engine as meili
                meili_results = meili.search(q, page=page, per_page=20)
            except Exception:
                pass

            if meili_results and meili_results.get("hits"):
                result = {
                    "articles": rewrite_articles_images(meili_results["hits"]),
                    "total": meili_results["total"],
                    "page": page,
                    "per_page": 20,
                    "pages": max(1, (meili_results["total"] + 19) // 20),
                    "meili": True,
                }
            else:
                # Fallback to SQLite
                result = db.search_articles(query=q, page=page, per_page=20)
                if result.get("articles"):
                    result["articles"] = rewrite_articles_images(result["articles"])
        else:
            result = {"articles": [], "total": 0, "page": 1, "pages": 1, "per_page": 20}

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

    return templates.TemplateResponse("public/tag.html", {
        "request": request,
        "tag_name": tag_name,
        "result": result,
        "page": page,
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


@router.get("/rss.xml", response_class=Response)
@router.get("/feed", response_class=Response)
async def rss_feed():
    """RSS 2.0 feed — latest 50 articles."""
    try:
        articles = db.get_latest_articles(limit=50)
    except Exception:
        logger.exception("Database error in rss_feed")
        return Response(content="Service unavailable", status_code=503)

    import html as html_mod
    items = []
    for art in articles:
        url_parts = art["url"].replace("https://total.kz/ru/news/", "").strip("/").split("/")
        if len(url_parts) >= 2:
            link = f"https://total.kz/news/{url_parts[0]}/{url_parts[1]}"
        else:
            link = f"https://total.kz/"
        cat = cat_label(nav_slug_for(art.get("sub_category", "")))
        pub = art.get("pub_date", "")
        # RFC 822 date
        try:
            dt = datetime.strptime(pub[:19].replace('T', ' '), "%Y-%m-%d %H:%M:%S")
            rfc_date = dt.strftime("%a, %d %b %Y %H:%M:%S +0500")
        except Exception:
            rfc_date = ""
        desc = html_mod.escape(art.get("excerpt") or art.get("title", ""))
        items.append(f"""    <item>
      <title>{html_mod.escape(art['title'])}</title>
      <link>{link}</link>
      <description>{desc}</description>
      <category>{html_mod.escape(cat)}</category>
      <pubDate>{rfc_date}</pubDate>
      <guid isPermaLink="true">{link}</guid>
    </item>""")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>ТÓТАЛ — Новости Казахстана</title>
    <link>https://total.kz</link>
    <description>Последние новости Казахстана — политика, экономика, общество, спорт</description>
    <language>ru</language>
    <atom:link href="https://total.kz/rss.xml" rel="self" type="application/rss+xml"/>
    <lastBuildDate>{datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0500")}</lastBuildDate>
{chr(10).join(items)}
  </channel>
</rss>"""
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@router.get("/api/suggest")
async def api_suggest(q: str = Query("", min_length=2, max_length=100)):
    """Fast autocomplete: returns up to 7 article title suggestions."""
    import json as json_mod
    if len(q) < 2:
        return Response(content="[]", media_type="application/json")
    try:
        if db._BACKEND == "postgresql":
            rows = db.suggest_articles(q, limit=7)
            results = []
            for row in rows:
                url_parts = row["url"].replace("https://total.kz/ru/news/", "").strip("/").split("/")
                link = f"/news/{url_parts[0]}/{url_parts[1]}" if len(url_parts) >= 2 else "/"
                results.append({
                    "title": row["title"],
                    "url": link,
                    "cat": cat_label(nav_slug_for(row["sub_category"] or "")),
                })
        else:
            conn = sqlite3.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT title, sub_category, pub_date, url FROM articles "
                "WHERE title LIKE ? ORDER BY pub_date DESC LIMIT 7",
                (f"%{q}%",)
            )
            results = []
            for row in cur.fetchall():
                url_parts = row["url"].replace("https://total.kz/ru/news/", "").strip("/").split("/")
                link = f"/news/{url_parts[0]}/{url_parts[1]}" if len(url_parts) >= 2 else "/"
                results.append({
                    "title": row["title"],
                    "url": link,
                    "cat": cat_label(nav_slug_for(row["sub_category"] or "")),
                })
            conn.close()
        return Response(content=json_mod.dumps(results, ensure_ascii=False), media_type="application/json")
    except Exception:
        return Response(content="[]", media_type="application/json")


@router.post("/api/view/{article_id}")
async def api_track_view(article_id: int):
    """Increment view count for an article. Called client-side on page load."""
    try:
        if db._BACKEND == "postgresql":
            views = db.track_view(article_id)
        else:
            conn = sqlite3.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
            conn.execute("UPDATE articles SET views = COALESCE(views, 0) + 1 WHERE id = ?", (article_id,))
            conn.commit()
            cur = conn.execute("SELECT views FROM articles WHERE id = ?", (article_id,))
            row = cur.fetchone()
            conn.close()
            views = row[0] if row else 0
        return {"ok": True, "views": views}
    except Exception:
        return {"ok": False}
