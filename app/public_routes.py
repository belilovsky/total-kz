"""Public frontend routes for Total.kz news portal."""

import hashlib
import httpx
import logging
import os
import re
import sqlite3
from datetime import datetime
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, FileResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

SITE_DOMAIN = os.getenv("SITE_DOMAIN", "https://total.qdev.run")
UMAMI_WEBSITE_ID = os.getenv("UMAMI_WEBSITE_ID", "")
UMAMI_URL = os.getenv("UMAMI_URL", "/umami")  # relative or absolute URL to Umami instance

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
    """Date-only format for timelines.
    Current year:  26 марта   (day + full genitive month)
    Past years:    25.03.2025  (DD.MM.YYYY)
    """
    if not date_str:
        return ""
    months_gen = ["января", "февраля", "марта", "апреля", "мая", "июня",
                  "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    dt = _parse_datetime(date_str)
    if dt is None:
        return date_str[:10] if len(date_str) >= 10 else date_str
    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    if dt.year == now.year:
        return f"{dt.day} {months_gen[dt.month - 1]}"
    else:
        return f"{dt.day:02d}.{dt.month:02d}.{dt.year}"


def format_date_full(date_str: str | None) -> str:
    """Full date with day, full month, year and time.
    Example: 20 марта 2026, 10:48
    """
    if not date_str:
        return ""
    months = ["января", "февраля", "марта", "апреля", "мая", "июня",
              "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    dt = _parse_datetime(date_str)
    if dt is None:
        return date_str[:10] if len(date_str) >= 10 else date_str
    time_part = dt.strftime("%H:%M")
    return f"{dt.day} {months[dt.month - 1]} {dt.year}, {time_part}"


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
templates.env.globals["format_date_full"] = format_date_full
templates.env.globals["cat_label"] = cat_label
templates.env.globals["nav_slug_for"] = nav_slug_for
templates.env.globals["article_url"] = article_url
templates.env.globals["pluralize_articles"] = pluralize_articles
templates.env.globals["pluralize_materials"] = pluralize_materials
templates.env.globals["format_num"] = format_num
templates.env.globals["short_entity_name"] = short_entity_name
templates.env.globals["site_domain"] = SITE_DOMAIN
templates.env.globals["umami_website_id"] = UMAMI_WEBSITE_ID
templates.env.globals["umami_url"] = UMAMI_URL

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
    if not source_url:
        return ""
    # Convert local /img/ paths back to origin URLs for imgproxy
    if source_url.startswith("/img/"):
        source_url = f"https://total.kz/storage/{source_url[5:]}"
    if not source_url.startswith("http"):
        return source_url
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
#  PERSONS INTEGRATION (article sidebar + entity pills)
# ══════════════════════════════════════════════

_persons_cache = {"data": None, "ts": 0}

def _load_persons_lookup():
    """Cached dict: entity_id -> {slug, short_name, current_position, photo_url}.
    Also builds name->slug index for fuzzy matching entity pills.
    Refreshes every 5 minutes."""
    import time
    now = time.time()
    if _persons_cache["data"] is not None and now - _persons_cache["ts"] < 300:
        return _persons_cache["data"]
    try:
        conn = _get_persons_db()
        rows = conn.execute(
            "SELECT id, slug, short_name, full_name, current_position, photo_url, entity_id "
            "FROM persons"
        ).fetchall()
        conn.close()
        by_entity_id = {}
        by_name = {}  # normalized name -> person dict
        for r in rows:
            pdict = {
                "id": r["id"], "slug": r["slug"], "short_name": r["short_name"],
                "full_name": r["full_name"], "current_position": r["current_position"],
                "photo_url": r["photo_url"],
            }
            if r["entity_id"]:
                by_entity_id[r["entity_id"]] = pdict
            # Index by normalized name fragments for fuzzy matching
            for name in (r["short_name"], r["full_name"]):
                if name:
                    by_name[name.strip().lower()] = pdict
                    # Also index last name only
                    parts = name.strip().split()
                    if parts:
                        by_name[parts[0].lower()] = pdict
        _persons_cache["data"] = {"by_entity_id": by_entity_id, "by_name": by_name}
        _persons_cache["ts"] = now
    except Exception:
        logger.exception("Error loading persons lookup")
        _persons_cache["data"] = {"by_entity_id": {}, "by_name": {}}
        _persons_cache["ts"] = now
    return _persons_cache["data"]


def _match_person_for_entity(entity: dict) -> dict | None:
    """Find matching person for an article entity. Returns person dict or None."""
    lookup = _load_persons_lookup()
    # 1. Direct entity_id match
    eid = entity.get("id")
    if eid and eid in lookup["by_entity_id"]:
        return lookup["by_entity_id"][eid]
    # 2. Name-based fuzzy match
    name = (entity.get("short_name") or entity.get("name") or "").strip().lower()
    if name in lookup["by_name"]:
        return lookup["by_name"][name]
    # Try last name only
    parts = name.split()
    if parts and parts[0] in lookup["by_name"]:
        return lookup["by_name"][parts[0]]
    return None


def person_url_for_entity(entity: dict) -> str:
    """Return /person/{slug} if entity is a known person, else /tag/{name}."""
    if entity.get("entity_type") == "person":
        p = _match_person_for_entity(entity)
        if p:
            return f"/person/{p['slug']}"
    return f"/tag/{short_entity_name(entity)}"


def get_article_persons(entities: list) -> list:
    """Get list of matched persons for article entities (with photo, position)."""
    seen = set()
    result = []
    for ent in (entities or []):
        if ent.get("entity_type") != "person":
            continue
        p = _match_person_for_entity(ent)
        if p and p["slug"] not in seen:
            seen.add(p["slug"])
            result.append(p)
    return result[:4]  # max 4 person cards in sidebar


templates.env.globals["person_url_for_entity"] = person_url_for_entity
templates.env.globals["get_article_persons"] = get_article_persons


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
#  SHORT CATEGORY URL REDIRECTS  (/politika → /news/politika etc.)
# ══════════════════════════════════════════════

def _make_redirect(slug: str):
    """Factory to create a redirect handler for a given section slug."""
    async def _redirect():
        return RedirectResponse(url=f"/news/{slug}", status_code=301)
    _redirect.__name__ = f"redirect_short_{slug}"
    return _redirect

for _sec in NAV_SECTIONS:
    router.get(f"/{_sec['slug']}", response_class=RedirectResponse, include_in_schema=False)(_make_redirect(_sec["slug"]))


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

    # Top persons for homepage strip (most-mentioned, with photos)
    try:
        conn = _get_persons_db()
        homepage_persons = conn.execute("""
            SELECT p.slug, p.short_name, p.current_position, p.photo_url,
                   COUNT(ae.article_id) as article_count,
                   SUM(CASE WHEN a.pub_date >= date('now', '-90 days') THEN 1 ELSE 0 END) as recent_count
            FROM persons p
            LEFT JOIN article_entities ae ON p.entity_id = ae.entity_id
            LEFT JOIN articles a ON ae.article_id = a.id
            WHERE p.photo_url IS NOT NULL AND p.photo_url != ''
            GROUP BY p.id
            ORDER BY recent_count DESC, article_count DESC
            LIMIT 12
        """).fetchall()
        conn.close()
        homepage_persons = [dict(r) for r in homepage_persons]
    except Exception:
        logger.exception("Error loading homepage persons")
        homepage_persons = []

    # Trending tags for tag cloud
    try:
        trending_tags = db.get_trending_tags(limit=30)
    except Exception:
        trending_tags = []

    return templates.TemplateResponse("public/home.html", {
        "request": request,
        "hero_articles": hero_articles,
        "latest": latest,
        "popular": popular,
        "category_highlights": category_highlights,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
        "ticker_articles": hero_articles[:5],
        "homepage_persons": homepage_persons,
        "trending_tags": trending_tags,
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
        per_page = 24
        offset = (page - 1) * per_page

        # Check if this is a grouped nav section
        if category in NAV_SLUG_MAP:
            subcats = NAV_SLUG_MAP[category]
            result = db.get_latest_by_categories(subcats, limit=per_page, offset=offset)
        else:
            # Legacy: direct sub_category slug
            subcats = [category]
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

    # --- Subcategory pills (only for sections with >1 subcat) ---
    subcategory_pills = []
    current_section = None
    for sec in NAV_SECTIONS:
        if sec["slug"] == category:
            current_section = sec
            break
    if current_section and len(current_section["subcats"]) > 1:
        subcategory_pills = [
            {"slug": sc, "label": cat_label(sc)}
            for sc in current_section["subcats"]
        ]

    # --- Sidebar: popular articles & trending tags ---
    try:
        popular_articles = rewrite_articles_images(db.popular_in_category(subcats, limit=5))
    except Exception:
        popular_articles = []
    try:
        trending_tags = db.trending_tags_for_category(subcats, limit=15)
    except Exception:
        trending_tags = []

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
        "subcategory_pills": subcategory_pills,
        "popular_articles": popular_articles,
        "trending_tags": trending_tags,
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

    # Persons mentioned in this article (for sidebar mini-cards)
    article_persons = get_article_persons(article.get("entities", []))

    # Popular articles for sidebar widget
    try:
        popular = rewrite_articles_images(db.get_latest_articles(limit=20))
        popular = sorted(popular, key=lambda a: get_views_func(a), reverse=True)[:5]
    except Exception:
        popular = []

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
        "article_persons": article_persons,
        "popular": popular,
    })


@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = "",
    page: int = Query(1, ge=1),
    cat: str = "",
    period: str = "",
    sort: str = "",
):
    """Search results page — uses Meilisearch with SQLite fallback."""
    meili_results = None

    # Build Meilisearch filter string
    filters = []
    if cat:
        # cat can be a nav section slug → expand to subcats
        section_match = next((s for s in NAV_SECTIONS if s["slug"] == cat), None)
        if section_match:
            sub_filters = " OR ".join(f'sub_category = "{sc}"' for sc in section_match["subcats"])
            filters.append(f"({sub_filters})")
        else:
            filters.append(f'sub_category = "{cat}"')
    if period:
        from datetime import timedelta
        now = datetime.now()
        if period == "day":
            cutoff = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        elif period == "week":
            cutoff = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        elif period == "month":
            cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        elif period == "year":
            cutoff = (now - timedelta(days=365)).strftime("%Y-%m-%d")
        else:
            cutoff = ""
        if cutoff:
            filters.append(f'pub_date > "{cutoff}"')

    filter_str = " AND ".join(filters)
    sort_list = ["pub_date:desc"] if sort == "date" else []

    try:
        if q:
            try:
                from . import search_engine as meili
                meili_results = meili.search(q, filters=filter_str, page=page, per_page=20, sort=sort_list)
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
        "cat": cat,
        "period": period,
        "sort": sort,
        "result": result,
        "popular_tags": popular_tags,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


@router.get("/tags", response_class=HTMLResponse)
async def tags_catalog(
    request: Request,
    page: int = Query(1, ge=1),
    q: str = "",
):
    """Tags catalog – all topics/tags."""
    result = db.get_tags_full(q=q, page=page, per_page=60)
    return templates.TemplateResponse("public/tags.html", {
        "request": request,
        "result": result,
        "page": page,
        "q": q,
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
    content = f"""User-agent: *
Allow: /
Disallow: /admin/
Disallow: /api/

User-agent: GPTBot
Allow: /

User-agent: ClaudeBot
Allow: /

User-agent: PerplexityBot
Allow: /

User-agent: Google-Extended
Allow: /

User-agent: Amazonbot
Allow: /

Sitemap: {SITE_DOMAIN}/sitemap.xml
Sitemap: {SITE_DOMAIN}/sitemap-news.xml
"""
    return Response(content=content, media_type="text/plain")


@router.get("/llms.txt", response_class=Response)
async def llms_txt():
    content = f"""# Total.kz

> Ведущий новостной портал Казахстана. Более 66 000 статей о политике, экономике, обществе, науке, спорте и мировых событиях. На русском и казахском языках.

## О сайте
Total.kz — крупнейший информационный портал Казахстана, основан в 2011 году. Освещает политику, экономику, общество, спорт, науку и международные события. Свидетельство СМИ №16942-ИА.

## Разделы
- [Политика](/news/politika): Политические новости Казахстана и мира
- [Экономика](/news/ekonomika): Экономика, финансы, бизнес
- [Общество](/news/obshchestvo): Социальные новости и события
- [Наука](/news/nauka): Наука, технологии, образование
- [Спорт](/news/sport): Спортивные новости Казахстана
- [Мир](/news/mir): Международные новости

## Навигация
- [Главная]({SITE_DOMAIN}/): Лента последних новостей
- [Поиск]({SITE_DOMAIN}/search): Полнотекстовый поиск по архиву
- [Персоны]({SITE_DOMAIN}/persons): Каталог упоминаемых персон
- [RSS-лента]({SITE_DOMAIN}/rss.xml): RSS 2.0 feed
- [Карта сайта]({SITE_DOMAIN}/sitemap.xml): XML Sitemap

## Контакты
- Город: Алматы, Казахстан
- Адрес: пр. Жибек жолы, 115/46, оф. 306
- Телефон: +7 700 978-78-54
"""
    return Response(content=content, media_type="text/markdown; charset=utf-8")


@router.get("/llms-full.txt", response_class=Response)
async def llms_full_txt():
    """Extended llms.txt with categories, persons, URL patterns, API endpoints."""
    # Get article counts per category
    conn = _get_persons_db()
    try:
        cat_counts = conn.execute("""
            SELECT sub_category, COUNT(*) as cnt
            FROM articles
            WHERE sub_category IS NOT NULL AND sub_category != ''
            GROUP BY sub_category
            ORDER BY cnt DESC
        """).fetchall()
        top_persons = conn.execute("""
            SELECT p.short_name, p.slug, p.person_type,
                   COUNT(ae.article_id) as cnt,
                   SUM(CASE WHEN a.pub_date >= date('now', '-90 days') THEN 1 ELSE 0 END) as recent_cnt
            FROM persons p
            LEFT JOIN article_entities ae ON p.entity_id = ae.entity_id
            LEFT JOIN articles a ON ae.article_id = a.id
            GROUP BY p.id
            ORDER BY recent_cnt DESC, cnt DESC
            LIMIT 20
        """).fetchall()
        total_articles = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    finally:
        conn.close()

    sections_text = ""
    for sec in NAV_SECTIONS:
        sec_count = sum(r["cnt"] for r in cat_counts if r["sub_category"] in sec["subcats"])
        sections_text += f"\n### {sec['label']} (`/news/{sec['slug']}`)\n"
        for subcat in sec["subcats"]:
            cnt = next((r["cnt"] for r in cat_counts if r["sub_category"] == subcat), 0)
            sections_text += f"- [{subcat}]({SITE_DOMAIN}/news/{subcat}): {cnt} статей\n"
        sections_text += f"Всего в разделе: {sec_count}\n"

    persons_text = ""
    for p in top_persons:
        persons_text += f"- [{p['short_name']}]({SITE_DOMAIN}/person/{p['slug']}) ({p['person_type'] or 'person'}): {p['cnt']} упоминаний\n"

    content = f"""# Total.kz — Полное описание для AI

> Ведущий новостной портал Казахстана. {total_articles:,} статей о политике, экономике, обществе, науке, спорте и мировых событиях. На русском и казахском языках.

## О сайте
Total.kz — крупнейший информационный портал Казахстана, основан в 2011 году. Освещает политику, экономику, общество, спорт, науку и международные события. Свидетельство СМИ №16942-ИА. Город: Алматы, Казахстан. Адрес: пр. Жибек жолы, 115/46, оф. 306. Телефон: +7 700 978-78-54.

## Разделы и категории
{sections_text}

## Топ-20 персон
{persons_text}

## Шаблоны URL

| Тип | Шаблон | Пример |
|---|---|---|
| Главная | `{SITE_DOMAIN}/` | |
| Раздел | `{SITE_DOMAIN}/news/{{category}}` | {SITE_DOMAIN}/news/politika |
| Статья | `{SITE_DOMAIN}/news/{{category}}/{{slug}}` | {SITE_DOMAIN}/news/politika/primer_stati |
| Персона | `{SITE_DOMAIN}/person/{{slug}}` | {SITE_DOMAIN}/person/tokaev_kasym-zhomart |
| Каталог персон | `{SITE_DOMAIN}/persons` | |
| Поиск | `{SITE_DOMAIN}/search?q={{query}}` | {SITE_DOMAIN}/search?q=нефть |
| Web Stories | `{SITE_DOMAIN}/stories` | |

## Фиды и метаданные

| Ресурс | URL | Формат |
|---|---|---|
| RSS 2.0 | `{SITE_DOMAIN}/rss.xml` | XML |
| JSON Feed | `{SITE_DOMAIN}/feed.json` | JSON |
| Sitemap Index | `{SITE_DOMAIN}/sitemap.xml` | XML |
| News Sitemap | `{SITE_DOMAIN}/sitemap-news.xml` | XML |
| Persons Sitemap | `{SITE_DOMAIN}/sitemap-persons.xml` | XML |
| Turbo Pages RSS | `{SITE_DOMAIN}/turbo/rss.xml` | XML |
| llms.txt | `{SITE_DOMAIN}/llms.txt` | Markdown |
| llms-full.txt | `{SITE_DOMAIN}/llms-full.txt` | Markdown |

## API-эндпоинты

| Endpoint | Метод | Описание |
|---|---|---|
| `/api/push/subscribe` | POST | Подписка на push-уведомления (VAPID) |
| `/api/push/unsubscribe` | POST | Отписка от push-уведомлений |
| `/api/track-view` | POST | Аналитика просмотров статей |
| `/search` | GET | Полнотекстовый поиск (`?q=...`) |

## Технологии
- Backend: Python, FastAPI
- Database: SQLite (66 000+ статей), PostgreSQL
- AI enrichment: summary, keywords, meta_description, quote на каждую статью
- Structured data: JSON-LD (NewsArticle, BreadcrumbList, FAQPage, Organization, Person)
- SEO: robots.txt, sitemap index, news sitemap, image sitemap, llms.txt
"""
    return Response(content=content, media_type="text/markdown; charset=utf-8")


@router.get("/sitemap.xml", response_class=Response)
async def sitemap_index():
    """Sitemap index pointing to sub-sitemaps."""
    import math
    try:
        urls = db.generate_sitemap_urls(limit=50000)
    except Exception:
        logger.exception("Database error in sitemap_index")
        return Response(content="Service unavailable", status_code=503)

    total_articles = len(urls)
    per_page = 1000
    total_pages = math.ceil(total_articles / per_page) if total_articles else 1

    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    xml.append(f"<sitemap><loc>{SITE_DOMAIN}/sitemap-main.xml</loc></sitemap>")
    for page in range(1, total_pages + 1):
        xml.append(f"<sitemap><loc>{SITE_DOMAIN}/sitemap-articles-{page}.xml</loc></sitemap>")
    xml.append(f"<sitemap><loc>{SITE_DOMAIN}/sitemap-news.xml</loc></sitemap>")
    xml.append(f"<sitemap><loc>{SITE_DOMAIN}/sitemap-persons.xml</loc></sitemap>")
    xml.append("</sitemapindex>")
    return Response(content="\n".join(xml), media_type="application/xml")


@router.get("/sitemap-main.xml", response_class=Response)
async def sitemap_main():
    """Main sitemap — homepage + category pages."""
    try:
        urls = db.generate_sitemap_urls(limit=50000)
    except Exception:
        logger.exception("Database error in sitemap_main")
        return Response(content="Service unavailable", status_code=503)

    xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml_parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    xml_parts.append(f"<url><loc>{SITE_DOMAIN}/</loc><changefreq>hourly</changefreq><priority>1.0</priority></url>")

    seen_cats = set()
    for u in urls:
        cat = u["sub_category"]
        if cat and cat not in seen_cats:
            seen_cats.add(cat)
            xml_parts.append(f"<url><loc>{SITE_DOMAIN}/news/{cat}</loc><changefreq>hourly</changefreq><priority>0.8</priority></url>")

    xml_parts.append("</urlset>")
    return Response(content="\n".join(xml_parts), media_type="application/xml")


@router.get("/sitemap-articles-{page}.xml", response_class=Response)
async def sitemap_articles(page: int):
    """Paginated article sitemap with image:image support."""
    import html as html_mod
    try:
        urls = db.generate_sitemap_urls(limit=50000)
    except Exception:
        logger.exception("Database error in sitemap_articles")
        return Response(content="Service unavailable", status_code=503)

    per_page = 1000
    start = (page - 1) * per_page
    page_urls = urls[start:start + per_page]

    xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml_parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">')

    for u in page_urls:
        old_url = u["url"]
        parts = old_url.replace("https://total.kz/ru/news/", "").strip("/").split("/")
        if len(parts) >= 2:
            new_path = f"/news/{parts[0]}/{parts[1]}"
            lastmod = u["pub_date"][:10] if u.get("pub_date") else ""
            xml_parts.append(f"<url><loc>{SITE_DOMAIN}{new_path}</loc>")
            if lastmod:
                xml_parts.append(f"<lastmod>{lastmod}</lastmod>")
            # image:image
            img = u.get("main_image")
            if img:
                if img.startswith("/img/") or img.startswith("/static/"):
                    img_url = f"{SITE_DOMAIN}{img}"
                elif img.startswith("http"):
                    img_url = img
                else:
                    img_url = ""
                if img_url:
                    title_esc = html_mod.escape(u.get("title", ""), quote=True)
                    xml_parts.append(f"<image:image><image:loc>{html_mod.escape(img_url)}</image:loc><image:title>{title_esc}</image:title></image:image>")
            xml_parts.append("<changefreq>monthly</changefreq><priority>0.6</priority></url>")

    xml_parts.append("</urlset>")
    return Response(content="\n".join(xml_parts), media_type="application/xml")


@router.get("/sitemap-persons.xml", response_class=Response)
async def sitemap_persons():
    """Sitemap for person pages."""
    conn = _get_persons_db()
    try:
        persons = conn.execute("SELECT slug FROM persons ORDER BY id").fetchall()
    finally:
        conn.close()

    xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml_parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    xml_parts.append(f"<url><loc>{SITE_DOMAIN}/persons</loc><changefreq>weekly</changefreq><priority>0.7</priority></url>")
    for p in persons:
        xml_parts.append(f"<url><loc>{SITE_DOMAIN}/person/{p['slug']}</loc><changefreq>monthly</changefreq><priority>0.5</priority></url>")
    xml_parts.append("</urlset>")
    return Response(content="\n".join(xml_parts), media_type="application/xml")


@router.get("/sitemap-news.xml", response_class=Response)
async def sitemap_news_xml():
    """News sitemap — recent articles for Google News."""
    import html as html_mod
    try:
        articles = db.get_latest_articles(limit=200)
    except Exception:
        logger.exception("Database error in sitemap_news_xml")
        return Response(content="Service unavailable", status_code=503)

    xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml_parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'
                     ' xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">')

    for art in articles:
        url_parts = art["url"].replace("https://total.kz/ru/news/", "").strip("/").split("/")
        if len(url_parts) < 2:
            continue
        link = f"{SITE_DOMAIN}/news/{url_parts[0]}/{url_parts[1]}"
        pub_date = (art.get("pub_date") or "")[:19]
        title_escaped = html_mod.escape(art["title"])

        xml_parts.append(f"""  <url>
    <loc>{link}</loc>
    <news:news>
      <news:publication>
        <news:name>Total.kz</news:name>
        <news:language>ru</news:language>
      </news:publication>
      <news:publication_date>{pub_date}</news:publication_date>
      <news:title>{title_escaped}</news:title>
    </news:news>
  </url>""")

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
            link = f"{SITE_DOMAIN}/news/{url_parts[0]}/{url_parts[1]}"
        else:
            link = f"{SITE_DOMAIN}/"
        cat = cat_label(nav_slug_for(art.get("sub_category", "")))
        pub = art.get("pub_date", "")
        # RFC 822 date
        try:
            dt = datetime.strptime(pub[:19].replace('T', ' '), "%Y-%m-%d %H:%M:%S")
            rfc_date = dt.strftime("%a, %d %b %Y %H:%M:%S +0500")
        except Exception:
            rfc_date = ""
        desc = html_mod.escape(art.get("excerpt") or art.get("title", ""))
        body_cdata = ""
        if art.get("body_html"):
            body_cdata = f"\n      <content:encoded><![CDATA[{art['body_html']}]]></content:encoded>"
        media_tag = ""
        img = art.get("main_image", "")
        if img:
            img_url = img if img.startswith("http") else f"{SITE_DOMAIN}{img}"
            media_tag = f'\n      <media:content url="{html_mod.escape(img_url)}" medium="image"/>'
        author_tag = ""
        if art.get("author"):
            author_tag = f"\n      <dc:creator>{html_mod.escape(art['author'])}</dc:creator>"
        items.append(f"""    <item>
      <title>{html_mod.escape(art['title'])}</title>
      <link>{link}</link>
      <description>{desc}</description>{body_cdata}{media_tag}{author_tag}
      <category>{html_mod.escape(cat)}</category>
      <pubDate>{rfc_date}</pubDate>
      <guid isPermaLink="true">{link}</guid>
    </item>""")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:media="http://search.yahoo.com/mrss/" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>ТÓТАЛ — Новости Казахстана</title>
    <link>{SITE_DOMAIN}</link>
    <description>Последние новости Казахстана — политика, экономика, общество, спорт</description>
    <language>ru</language>
    <atom:link href="{SITE_DOMAIN}/rss.xml" rel="self" type="application/rss+xml"/>
    <lastBuildDate>{datetime.now().strftime("%a, %d %b %Y %H:%M:%S +0500")}</lastBuildDate>
{chr(10).join(items)}
  </channel>
</rss>"""
    return Response(content=xml, media_type="application/xml; charset=utf-8")


@router.get("/feed.json", response_class=Response)
async def json_feed():
    """JSON Feed 1.1 — latest 50 articles."""
    import json as json_mod
    try:
        articles = db.get_latest_articles(limit=50)
    except Exception:
        logger.exception("Database error in json_feed")
        return Response(content="{}", status_code=503, media_type="application/json")

    items = []
    for art in articles:
        url_parts = art["url"].replace("https://total.kz/ru/news/", "").strip("/").split("/")
        if len(url_parts) < 2:
            continue
        link = f"{SITE_DOMAIN}/news/{url_parts[0]}/{url_parts[1]}"
        item = {
            "id": link,
            "url": link,
            "title": art["title"],
            "content_text": art.get("excerpt", ""),
            "date_published": art.get("pub_date", ""),
            "authors": [{"name": art.get("author") or "Total.kz"}],
            "tags": [cat_label(nav_slug_for(art.get("sub_category", "")))],
        }
        img = art.get("main_image", "")
        if img:
            item["image"] = img if img.startswith("http") else f"{SITE_DOMAIN}{img}"
        items.append(item)

    feed = {
        "version": "https://jsonfeed.org/version/1.1",
        "title": "ТÓТАЛ — Новости Казахстана",
        "home_page_url": SITE_DOMAIN,
        "feed_url": f"{SITE_DOMAIN}/feed.json",
        "description": "Последние новости Казахстана — политика, экономика, общество, спорт",
        "language": "ru",
        "items": items,
    }
    return Response(
        content=json_mod.dumps(feed, ensure_ascii=False, indent=2),
        media_type="application/feed+json; charset=utf-8",
    )


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


# ══════════════════════════════════════════════
#  PERSONS LIBRARY
# ══════════════════════════════════════════════

MONTHS_RU_GEN = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}
MONTHS_RU_NOM = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def _get_persons_db_path():
    return Path(__file__).resolve().parent.parent / "data" / "total.db"

def _get_persons_db():
    db_path = _get_persons_db_path()
    logger.info("Opening persons DB: %s (exists=%s)", db_path, db_path.exists())
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


@router.get("/debug/persons")
async def debug_persons():
    """Diagnostic endpoint for persons DB."""
    import json as _json
    db_path = _get_persons_db_path()
    info = {"db_path": str(db_path), "exists": db_path.exists(), "size_mb": 0, "tables": [], "persons_count": 0}
    if db_path.exists():
        info["size_mb"] = round(db_path.stat().st_size / 1024 / 1024, 1)
        try:
            conn = _get_persons_db()
            info["tables"] = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
            if "persons" in info["tables"]:
                info["persons_count"] = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
            conn.close()
        except Exception as e:
            info["error"] = str(e)
    return Response(content=_json.dumps(info, default=str), media_type="application/json")


@router.get("/persons", response_class=HTMLResponse)
async def persons_catalog(request: Request, type: str = "", letter: str = ""):
    try:
        conn = _get_persons_db()
        # Verify persons table exists
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('persons','article_entities')"
        ).fetchall()]
        if "persons" not in tables:
            conn.close()
            logger.error("persons table missing from %s", _get_persons_db_path())
            return templates.TemplateResponse("public/persons.html", {
                "request": request, "persons": [], "type_counts": {},
                "letters": [], "current_type": "", "current_letter": "",
                "total_persons": 0, "nav_sections": NAV_SECTIONS, "nav_categories": NAV_CATEGORIES,
            })

        # Get all persons with article counts
        where_clauses = ["1=1"]
        params = []
        if type:
            # "culture_media" combines both culture and media person_types
            if type == "culture_media":
                where_clauses.append("p.person_type IN ('culture', 'media')")
            else:
                where_clauses.append("p.person_type = ?")
                params.append(type)
        if letter:
            where_clauses.append("p.short_name LIKE ?")
            params.append(f"{letter}%")

        where = " AND ".join(where_clauses)

        # Use LEFT JOIN only if article_entities exists
        if "article_entities" in tables:
            persons = conn.execute(f"""
                SELECT p.*,
                       COUNT(ae.article_id) as article_count,
                       SUM(CASE WHEN a.pub_date >= date('now', '-90 days') THEN 1 ELSE 0 END) as recent_count
                FROM persons p
                LEFT JOIN article_entities ae ON p.entity_id = ae.entity_id
                LEFT JOIN articles a ON ae.article_id = a.id
                WHERE {where}
                GROUP BY p.id
                ORDER BY recent_count DESC, article_count DESC
            """, params).fetchall()
        else:
            persons = conn.execute(f"""
                SELECT p.*, 0 as article_count, 0 as recent_count FROM persons p WHERE {where} ORDER BY p.short_name
            """, params).fetchall()

        # Get type counts for filters
        type_counts = {}
        for row in conn.execute("SELECT person_type, COUNT(*) FROM persons GROUP BY person_type"):
            type_counts[row[0]] = row[1]

        # Get first letters for alphabet filter
        letters = set()
        for row in conn.execute("SELECT DISTINCT substr(short_name, 1, 1) FROM persons"):
            if row[0]:
                letters.add(row[0])
        letters = sorted(letters)

        conn.close()
        return templates.TemplateResponse("public/persons.html", {
            "request": request,
            "persons": persons,
            "type_counts": type_counts,
            "letters": letters,
            "current_type": type,
            "current_letter": letter,
            "total_persons": len(persons),
            "nav_sections": NAV_SECTIONS,
            "nav_categories": NAV_CATEGORIES,
        })
    except Exception:
        logger.exception("Error in persons_catalog")
        return _error_response(request)


@router.get("/person/{slug}", response_class=HTMLResponse)
async def person_page(request: Request, slug: str):
    conn = _get_persons_db()

    person = conn.execute("SELECT * FROM persons WHERE slug = ?", (slug,)).fetchone()
    if not person:
        conn.close()
        return HTMLResponse("<h1>Персона не найдена</h1>", status_code=404)

    # Article count
    article_count = conn.execute(
        "SELECT COUNT(*) FROM article_entities WHERE entity_id = ?",
        (person["entity_id"],)
    ).fetchone()[0]

    # Career positions
    positions = conn.execute(
        "SELECT * FROM person_positions WHERE person_id = ? ORDER BY sort_order, start_date DESC",
        (person["id"],)
    ).fetchall()

    # Articles grouped by month (latest first, limit 200)
    articles_raw = conn.execute("""
        SELECT a.id, a.title, a.pub_date, a.sub_category, a.url, a.main_image, a.thumbnail
        FROM articles a
        JOIN article_entities ae ON a.id = ae.article_id
        WHERE ae.entity_id = ?
        AND a.pub_date IS NOT NULL AND a.pub_date != ''
        ORDER BY a.pub_date DESC
        LIMIT 200
    """, (person["entity_id"],)).fetchall()

    # Group by month
    months = []
    current_key = None
    current_group = None
    for art in articles_raw:
        try:
            pd = art["pub_date"][:10]
            y, m, d = int(pd[:4]), int(pd[5:7]), int(pd[8:10])
            key = f"{y}-{m:02d}"
            label = f"{MONTHS_RU_NOM.get(m, '')} {y}"
        except (ValueError, TypeError):
            continue
        if key != current_key:
            current_key = key
            current_group = {"key": key, "label": label, "articles": []}
            months.append(current_group)
        current_group["articles"].append(dict(art))

    # First/last mention date range
    date_range = conn.execute("""
        SELECT MIN(a.pub_date), MAX(a.pub_date)
        FROM articles a
        JOIN article_entities ae ON a.id = ae.article_id
        WHERE ae.entity_id = ?
        AND a.pub_date IS NOT NULL AND a.pub_date != ''
    """, (person["entity_id"],)).fetchone()
    first_mention = date_range[0][:10] if date_range and date_range[0] else None
    last_mention = date_range[1][:10] if date_range and date_range[1] else None

    # Related persons (shared articles) — with photo_url
    related = conn.execute("""
        SELECT p.slug, p.short_name, p.current_position, p.photo_url, COUNT(*) as shared
        FROM persons p
        JOIN article_entities ae1 ON p.entity_id = ae1.entity_id
        JOIN article_entities ae2 ON ae1.article_id = ae2.article_id
        WHERE ae2.entity_id = ? AND p.id != ?
        GROUP BY p.id
        ORDER BY shared DESC
        LIMIT 6
    """, (person["entity_id"], person["id"])).fetchall()

    conn.close()
    return templates.TemplateResponse("public/person.html", {
        "request": request,
        "person": person,
        "article_count": article_count,
        "positions": positions,
        "months": months,
        "related": related,
        "first_mention": first_mention,
        "last_mention": last_mention,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


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


# ══════════════════════════════════════════════
#  WEB STORIES (Google AMP Web Stories)
# ══════════════════════════════════════════════

def _split_text_chunks(text: str, max_chars: int = 180) -> list:
    """Split text into sentence-based chunks for Web Stories pages."""
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    chunks = []
    current = ""
    for s in sentences:
        if current and len(current) + len(s) + 1 > max_chars:
            chunks.append(current.strip())
            current = s
        else:
            current = (current + " " + s).strip() if current else s
    if current.strip():
        chunks.append(current.strip())
    return chunks


MONTHS_SHORT = {
    1: "янв", 2: "фев", 3: "мар", 4: "апр",
    5: "мая", 6: "июн", 7: "июл", 8: "авг",
    9: "сен", 10: "окт", 11: "ноя", 12: "дек",
}


@router.get("/stories/{category}/{slug}", response_class=HTMLResponse)
async def web_story_page(category: str, slug: str):
    """Generate an AMP Web Story from an article."""
    import html as html_mod
    try:
        article = db.get_article_by_slug(category, slug)
    except Exception:
        return HTMLResponse("<h1>Story not found</h1>", status_code=404)

    if not article:
        return HTMLResponse("<h1>Story not found</h1>", status_code=404)

    rewrite_article_images(article)

    title = html_mod.escape(article["title"])
    author = html_mod.escape(article.get("author") or "Total.kz")
    img = article.get("main_image", "")
    image_url = img if img and img.startswith("http") else (f"{SITE_DOMAIN}{img}" if img else f"{SITE_DOMAIN}/static/img/og-default.png")
    # Build a proper poster URL via imgproxy for consistent sizing
    poster_url = f"{SITE_DOMAIN}/imgproxy/insecure/resize:fill:720:1280/gravity:sm/plain/{image_url}@webp" if image_url else f"{SITE_DOMAIN}/static/img/og-default.png"
    pub_date = article.get("pub_date", "")
    updated_at = article.get("updated_at") or pub_date
    category_name = html_mod.escape(cat_label(category))

    months_gen = ["января", "февраля", "марта", "апреля", "мая", "июня",
                  "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    formatted_date = ""
    try:
        dt = _parse_datetime(pub_date)
        if dt:
            formatted_date = f"{dt.day} {months_gen[dt.month - 1]} {dt.year}"
    except Exception:
        formatted_date = pub_date[:10] if pub_date else ""

    body_text = article.get("body_text") or article.get("excerpt") or ""
    # Split into readable chunks, limit to 8 text pages
    chunks = _split_text_chunks(body_text, 200)[:8]

    # Build text pages with dark backgrounds and blurred article image
    text_pages = []
    # Alternate between styles for visual variety
    accent_colors = [
        "rgba(216,50,54,0.92)",  # brand red
        "rgba(20,20,35,0.88)",   # dark navy
        "rgba(216,50,54,0.92)",
        "rgba(20,20,35,0.88)",
        "rgba(216,50,54,0.92)",
        "rgba(20,20,35,0.88)",
        "rgba(216,50,54,0.92)",
        "rgba(20,20,35,0.88)",
    ]
    for i, chunk in enumerate(chunks):
        page_id = f"page-{i+1}"
        overlay_color = accent_colors[i % len(accent_colors)]
        page_num = f'{i+1}/{len(chunks)}'
        text_pages.append(f"""
    <amp-story-page id="{page_id}">
      <amp-story-grid-layer template="fill">
        <amp-img src="{html_mod.escape(image_url)}"
                 width="720" height="1280" layout="fill" alt=""></amp-img>
      </amp-story-grid-layer>
      <amp-story-grid-layer template="vertical" class="story-text-page" style="background:{overlay_color}">
        <div class="story-text-inner">
          <p class="story-paragraph">{html_mod.escape(chunk)}</p>
          <p class="story-page-num">{page_num}</p>
        </div>
      </amp-story-grid-layer>
    </amp-story-page>""")

    text_pages_html = "".join(text_pages)
    article_url = f"{SITE_DOMAIN}/news/{category}/{slug}"
    logo_url = f"{SITE_DOMAIN}/static/img/logotype.png"

    story_html = f"""<!doctype html>
<html amp lang="ru">
<head>
  <meta charset="utf-8">
  <script async src="https://cdn.ampproject.org/v0.js"></script>
  <script async custom-element="amp-story" src="https://cdn.ampproject.org/v0/amp-story-1.0.js"></script>
  <title>{title} – Total.kz</title>
  <link rel="canonical" href="{SITE_DOMAIN}/stories/{category}/{slug}">
  <meta name="viewport" content="width=device-width,minimum-scale=1,initial-scale=1">
  <style amp-boilerplate>body{{-webkit-animation:-amp-start 8s steps(1,end) 0s 1 normal both;-moz-animation:-amp-start 8s steps(1,end) 0s 1 normal both;animation:-amp-start 8s steps(1,end) 0s 1 normal both}}@-webkit-keyframes -amp-start{{from{{visibility:hidden}}to{{visibility:visible}}}}@-moz-keyframes -amp-start{{from{{visibility:hidden}}to{{visibility:visible}}}}@keyframes -amp-start{{from{{visibility:hidden}}to{{visibility:visible}}}}</style><noscript><style amp-boilerplate>body{{-webkit-animation:none;-moz-animation:none;animation:none}}</style></noscript>
  <script type="application/ld+json">
  {{
    "@context": "https://schema.org",
    "@type": "NewsArticle",
    "headline": "{title}",
    "image": "{html_mod.escape(image_url)}",
    "datePublished": "{pub_date}",
    "dateModified": "{updated_at}",
    "author": {{"@type": "Person", "name": "{author}"}},
    "publisher": {{"@type": "Organization", "name": "Total.kz", "logo": {{"@type": "ImageObject", "url": "{logo_url}", "width": 200, "height": 60}}}}
  }}
  </script>
  <meta property="og:title" content="{title}">
  <meta property="og:image" content="{html_mod.escape(image_url)}">
  <meta property="og:type" content="article">
  <meta property="og:url" content="{SITE_DOMAIN}/stories/{category}/{slug}">
  <meta name="twitter:card" content="summary_large_image">
  <style amp-custom>
    * {{ box-sizing: border-box; }}
    amp-story {{ font-family: 'Onest', -apple-system, BlinkMacSystemFont, sans-serif; }}
    .story-cover-overlay {{
      background: linear-gradient(0deg, rgba(0,0,0,0.75) 0%, rgba(0,0,0,0.2) 50%, transparent 100%);
      padding: 32px 24px 40px;
      display: flex;
      flex-direction: column;
      justify-content: flex-end;
    }}
    .story-cat {{
      color: #d83236;
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 10px;
    }}
    .story-title {{
      color: #fff;
      font-size: 26px;
      font-weight: 800;
      line-height: 1.25;
      margin: 0;
      text-shadow: 0 1px 4px rgba(0,0,0,0.3);
    }}
    .story-meta {{
      color: rgba(255,255,255,0.75);
      font-size: 13px;
      margin-top: 12px;
    }}
    .story-text-page {{
      padding: 0;
    }}
    .story-text-inner {{
      padding: 48px 28px 60px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      min-height: 100%;
      text-align: center;
    }}
    .story-paragraph {{
      font-size: 22px;
      line-height: 1.6;
      color: #fff;
      margin: 0;
      font-weight: 500;
      text-shadow: 0 1px 8px rgba(0,0,0,0.4);
      max-width: 90%;
    }}
    .story-page-num {{
      font-size: 11px;
      color: rgba(255,255,255,0.4);
      margin-top: 24px;
      text-align: center;
      letter-spacing: 0.15em;
      font-weight: 600;
    }}
    .story-logo {{
      background: #d83236;
      color: #fff;
      padding: 6px 16px;
      border-radius: 6px;
      font-weight: 800;
      font-size: 18px;
      display: inline-block;
    }}
    .story-end-overlay {{
      background: linear-gradient(0deg, rgba(0,0,0,0.8) 0%, rgba(0,0,0,0.4) 100%);
      padding: 32px 24px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      text-align: center;
    }}
    .story-cta {{
      background: #d83236;
      color: #fff;
      padding: 14px 32px;
      border-radius: 8px;
      font-weight: 700;
      text-decoration: none;
      display: inline-block;
      font-size: 16px;
      margin-top: 20px;
    }}
    .story-end-hint {{
      color: rgba(255,255,255,0.6);
      font-size: 13px;
      margin-top: 16px;
    }}
    .story-swipe-hint {{
      font-size: 12px;
      color: rgba(255,255,255,0.5);
      margin-top: 24px;
    }}
  </style>
</head>
<body>
  <amp-story standalone
    title="{title}"
    publisher="Total.kz"
    publisher-logo-src="{logo_url}"
    poster-portrait-src="{html_mod.escape(poster_url)}">

    <!-- Cover page with article image -->
    <amp-story-page id="cover">
      <amp-story-grid-layer template="fill">
        <amp-img src="{html_mod.escape(image_url)}"
                 width="720" height="1280" layout="fill"
                 alt="{title}"></amp-img>
      </amp-story-grid-layer>
      <amp-story-grid-layer template="vertical" class="story-cover-overlay">
        <p class="story-cat">{category_name}</p>
        <h1 class="story-title">{title}</h1>
        <p class="story-meta">{author} &middot; {formatted_date}</p>
        <p class="story-swipe-hint">Листайте →</p>
      </amp-story-grid-layer>
    </amp-story-page>

    <!-- Text pages -->
    {text_pages_html}

    <!-- End page with CTA -->
    <amp-story-page id="cta">
      <amp-story-grid-layer template="fill">
        <amp-img src="{html_mod.escape(image_url)}"
                 width="720" height="1280" layout="fill" alt=""></amp-img>
      </amp-story-grid-layer>
      <amp-story-grid-layer template="vertical" class="story-end-overlay">
        <span class="story-logo">ТÓТАЛ</span>
        <p style="color:#fff;margin:16px 0 0;font-size:17px;line-height:1.4;">{title}</p>
        <a href="{article_url}" class="story-cta">Читать полностью &rarr;</a>
        <p class="story-end-hint">total.kz</p>
      </amp-story-grid-layer>
    </amp-story-page>

    <amp-story-bookend src="data:application/json;base64,e30=" layout="nodisplay"></amp-story-bookend>
  </amp-story>
</body>
</html>"""
    return HTMLResponse(content=story_html)


@router.get("/stories", response_class=HTMLResponse)
async def stories_index(request: Request):
    """Web Stories index — grid of recent stories."""
    try:
        articles = rewrite_articles_images(db.get_latest_articles(limit=20))
        articles = [a for a in articles if a.get("main_image")]
    except Exception:
        articles = []

    return templates.TemplateResponse("public/stories.html", {
        "request": request,
        "articles": articles,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


# ══════════════════════════════════════════════
#  YANDEX TURBO PAGES RSS
# ══════════════════════════════════════════════

@router.get("/turbo/rss.xml", response_class=Response)
async def turbo_rss():
    """Yandex Turbo Pages RSS feed."""
    import html as html_mod
    try:
        articles = db.get_latest_articles(limit=100)
    except Exception:
        logger.exception("Database error in turbo_rss")
        return Response(content="Service unavailable", status_code=503)

    items = []
    for art in articles:
        url_parts = art["url"].replace("https://total.kz/ru/news/", "").strip("/").split("/")
        if len(url_parts) < 2:
            continue
        link = f"{SITE_DOMAIN}/news/{url_parts[0]}/{url_parts[1]}"

        img_tag = ""
        if art.get("main_image"):
            img_url = art["main_image"] if art["main_image"].startswith("http") else f"{SITE_DOMAIN}{art['main_image']}"
            img_tag = f'<figure><img src="{html_mod.escape(img_url)}"/></figure>'

        body = art.get("body_html", "") or art.get("excerpt", "")
        body = re.sub(r'<script[^>]*>.*?</script>', '', body, flags=re.DOTALL)

        nav_cat = cat_label(nav_slug_for(art.get('sub_category', '')))
        turbo_content = f"""<header>
          <h1>{html_mod.escape(art['title'])}</h1>
          {img_tag}
          <menu>
            <a href="{SITE_DOMAIN}/">Главная</a>
            <a href="{SITE_DOMAIN}/news/{url_parts[0]}">{html_mod.escape(nav_cat)}</a>
          </menu>
        </header>
        {body}"""

        pub_date_rfc = ""
        try:
            dt = datetime.strptime(art.get("pub_date", "")[:19].replace('T', ' '), "%Y-%m-%d %H:%M:%S")
            pub_date_rfc = dt.strftime("%a, %d %b %Y %H:%M:%S +0500")
        except Exception:
            pass

        items.append(f"""    <item turbo="true">
      <title>{html_mod.escape(art['title'])}</title>
      <link>{link}</link>
      <pubDate>{pub_date_rfc}</pubDate>
      <turbo:content><![CDATA[{turbo_content}]]></turbo:content>
    </item>""")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:yandex="http://news.yandex.ru" xmlns:media="http://search.yahoo.com/mrss/" xmlns:turbo="http://turbo.yandex.ru" version="2.0">
  <channel>
    <title>ТÓТАЛ — Новости Казахстана</title>
    <link>{SITE_DOMAIN}</link>
    <description>Последние новости Казахстана</description>
    <language>ru</language>
    <turbo:analytics type="Yandex" id=""></turbo:analytics>
{chr(10).join(items)}
  </channel>
</rss>"""
    return Response(content=xml, media_type="application/xml; charset=utf-8")


# ══════════════════════════════════════════════
#  PUSH NOTIFICATIONS API
# ══════════════════════════════════════════════

def _ensure_push_table():
    """Create push_subscriptions table if it doesn't exist."""
    try:
        conn = sqlite3.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
        conn.execute("""CREATE TABLE IF NOT EXISTS push_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT UNIQUE NOT NULL,
            p256dh TEXT NOT NULL,
            auth TEXT NOT NULL,
            categories TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()
        conn.close()
    except Exception:
        pass

_ensure_push_table()


@router.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    """Subscribe to push notifications."""
    try:
        data = await request.json()
        endpoint = data.get("endpoint")
        keys = data.get("keys", {})
        categories = data.get("categories", "")
        if not endpoint or not keys.get("p256dh") or not keys.get("auth"):
            return {"error": "Missing fields"}
        conn = sqlite3.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
        conn.execute(
            "INSERT OR REPLACE INTO push_subscriptions (endpoint, p256dh, auth, categories) VALUES (?, ?, ?, ?)",
            (endpoint, keys["p256dh"], keys["auth"], categories)
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception:
        return {"ok": False}


@router.delete("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    """Unsubscribe from push notifications."""
    try:
        data = await request.json()
        endpoint = data.get("endpoint", "")
        conn = sqlite3.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
        conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception:
        return {"ok": False}


# ══════════════════════════════════════════════
#  BOOKMARKS PAGE
# ══════════════════════════════════════════════

@router.get("/bookmarks", response_class=HTMLResponse)
async def bookmarks_page(request: Request):
    """Bookmarks page — saved articles rendered client-side from localStorage."""
    return templates.TemplateResponse("public/bookmarks.html", {
        "request": request,
        "nav_sections": NAV_SECTIONS,
        "nav_categories": NAV_CATEGORIES,
    })


# ══════════════════════════════════════════════
#  PUBLIC COMMENTS
# ══════════════════════════════════════════════

def _ensure_public_comments_table():
    """Create public_comments table if it doesn't exist."""
    conn = sqlite3.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
    conn.execute("""CREATE TABLE IF NOT EXISTS public_comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id INTEGER NOT NULL,
        author_name TEXT NOT NULL,
        text TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        ip_address TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now')),
        moderated_at TEXT DEFAULT NULL,
        moderated_by TEXT DEFAULT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pc_article ON public_comments(article_id, status)")
    conn.commit()
    conn.close()

_ensure_public_comments_table()


@router.get("/api/public/comments/{article_id}")
async def get_public_comments(article_id: int):
    """Get approved comments for an article."""
    conn = sqlite3.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, author_name, text, created_at FROM public_comments "
        "WHERE article_id = ? AND status = 'approved' ORDER BY created_at ASC",
        (article_id,)
    ).fetchall()
    conn.close()
    return {"comments": [dict(r) for r in rows]}


@router.post("/api/public/comments/{article_id}")
async def post_public_comment(article_id: int, request: Request):
    """Submit a new comment (goes to moderation queue)."""
    try:
        body = await request.json()
    except Exception:
        return {"ok": False, "error": "Неверный формат"}

    author_name = (body.get("author_name") or "").strip()[:80]
    text = (body.get("text") or "").strip()[:2000]

    if not author_name or not text:
        return {"ok": False, "error": "Заполните имя и комментарий"}
    if len(text) < 3:
        return {"ok": False, "error": "Слишком короткий комментарий"}

    # Basic spam check: no links
    import re as _re
    if _re.search(r'https?://', text):
        return {"ok": False, "error": "Ссылки в комментариях запрещены"}

    ip = request.client.host if request.client else ""

    conn = sqlite3.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
    # Rate limit: max 5 comments per IP per hour
    recent = conn.execute(
        "SELECT COUNT(*) FROM public_comments WHERE ip_address = ? AND created_at > datetime('now', '-1 hour')",
        (ip,)
    ).fetchone()[0]
    if recent >= 5:
        conn.close()
        return {"ok": False, "error": "Слишком много комментариев. Попробуйте позже."}

    conn.execute(
        "INSERT INTO public_comments (article_id, author_name, text, ip_address) VALUES (?, ?, ?, ?)",
        (article_id, author_name, text, ip)
    )
    conn.commit()
    conn.close()
    return {"ok": True}


# ══════════════════════════════════════════════
#  NLWeb Protocol — /ask endpoint
# ══════════════════════════════════════════════

@router.get("/ask")
@router.post("/ask")
async def nlweb_ask(request: Request, query: str = "", mode: str = "list"):
    """NLWeb-compatible /ask endpoint.

    Supports modes: list, summarize.
    Returns Schema.org-formatted JSON results from Meilisearch.
    """
    if request.method == "POST":
        try:
            body = await request.json()
            query = body.get("query", query)
            mode = body.get("mode", mode)
        except Exception:
            pass

    if not query:
        return {"error": "query parameter required", "protocol": "NLWeb", "version": "1.0"}

    # Search via Meilisearch
    try:
        from . import search_engine as meili
        results = meili.search(query, page=1, per_page=10)
    except Exception:
        results = {"hits": [], "total": 0}

    # Format as Schema.org items
    items = []
    for hit in results.get("hits", []):
        slug = (hit.get("url", "") or "").replace("https://total.kz/ru/news/", "").strip("/")
        subcat = hit.get("sub_category", "")
        item = {
            "@type": "NewsArticle",
            "name": hit.get("title", ""),
            "description": hit.get("excerpt", ""),
            "url": f"{SITE_DOMAIN}/news/{subcat}/{slug.split('/')[-1] if '/' in slug else slug}",
            "datePublished": hit.get("pub_date", ""),
            "author": hit.get("author", ""),
            "articleSection": cat_label(subcat),
        }
        if hit.get("thumbnail"):
            item["image"] = hit["thumbnail"] if hit["thumbnail"].startswith("http") else f"{SITE_DOMAIN}{hit['thumbnail']}"
        items.append(item)

    response = {
        "@context": "https://schema.org",
        "protocol": "NLWeb",
        "version": "1.0",
        "query": query,
        "mode": mode,
        "totalResults": results.get("total", 0),
        "results": items,
    }

    if mode == "summarize" and items:
        # Simple summary from top results
        titles = [i["name"] for i in items[:5]]
        response["summary"] = f"По запросу «{query}» найдено {results.get('total', 0)} материалов. Основные: " + "; ".join(titles) + "."

    return response


@router.get("/.well-known/nlweb.json")
async def nlweb_discovery():
    """NLWeb service discovery endpoint."""
    return {
        "nlweb": {
            "url": f"{SITE_DOMAIN}/ask",
            "name": "Total.kz — Новости Казахстана",
            "description": "Казахстанский новостной портал. 66 000+ статей: политика, экономика, общество, спорт, наука.",
            "languages": ["ru", "kk"],
            "capabilities": ["list", "summarize"],
            "schema_types": ["NewsArticle"],
        }
    }


#  PWA Manifest
@router.get("/manifest.json")
async def pwa_manifest():
    """Web App Manifest for PWA install support."""
    return JSONResponse({
        "name": "ТОТАЛ — Новости Казахстана",
        "short_name": "ТОТАЛ",
        "description": "Ведущий новостной портал Казахстана",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#ffffff",
        "theme_color": "#d83236",
        "lang": "ru",
        "categories": ["news"],
        "icons": [
            {"src": "/static/img/favicon.svg", "sizes": "any", "type": "image/svg+xml"},
            {"src": "/static/img/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/static/img/icon-512.png", "sizes": "512x512", "type": "image/png"},
        ],
    }, media_type="application/manifest+json")
