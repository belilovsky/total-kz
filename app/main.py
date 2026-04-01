"""FastAPI application – Total.kz v11.0 (public frontend + CMS admin)."""

import asyncio
import json
import logging
import os
import re
import time
import unicodedata
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
import calendar as _calendar_mod
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote as _url_quote, unquote as _url_unquote
from fastapi import FastAPI, Request, Query, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from brotli_asgi import BrotliMiddleware

from starlette.middleware.base import BaseHTTPMiddleware

from qazstack.core import health_router
from qazstack.content.api import content_router, SQLiteContentProvider
from .config import settings

from . import db_backend as db
from . import seo_analytics as seo
from . import search_analytics as search
from . import search_engine as meili
from . import auth
from . import workflow as wf
from .public_routes import router as public_router, NAV_SECTIONS as _NAV_SECTIONS
from .public_routes import templates as _public_templates
from .social_routes import router as social_router

logger = logging.getLogger(__name__)

MEDIA_DIR = Path(__file__).parent.parent / "data" / "media"
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}
MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10MB


# ══════════════════════════════════════════════
#  RATE LIMITING MIDDLEWARE
#  Защита дорогих эндпоинтов от злоупотреблений
# ══════════════════════════════════════════════

# Настройки лимитов: путь (prefix) → (max_requests, window_seconds)
_RATE_LIMITS: dict[str, tuple[int, int]] = {
    "/search":              (30, 60),   # 30 запросов в минуту
    "/ask":                 (20, 60),   # 20 запросов в минуту
    "/api/public/comments": (10, 60),   # 10 запросов в минуту
    "/api/public/reactions": (30, 60),  # 30 запросов в минуту
    "/api/suggest":         (60, 60),   # 60 запросов в минуту (autocomplete)
}

_rate_store: dict[str, list[float]] = defaultdict(list)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Sliding window rate limiter по IP + path prefix.
    Без внешних зависимостей — хранит timestamps в памяти.
    При перезапуске контейнера счётчики сбрасываются (приемлемо).
    """
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip() \
                    or (request.client.host if request.client else "unknown")

        for prefix, (max_req, window) in _RATE_LIMITS.items():
            if path.startswith(prefix):
                key = f"{prefix}:{client_ip}"
                now = time.monotonic()
                # Оставляем только запросы в пределах окна
                _rate_store[key] = [
                    ts for ts in _rate_store[key] if now - ts < window
                ]
                if len(_rate_store[key]) >= max_req:
                    logger.warning("Rate limit hit: %s %s", client_ip, path)
                    return JSONResponse(
                        {"error": "Слишком много запросов. Попробуйте через минуту."},
                        status_code=429,
                        headers={"Retry-After": str(window)},
                    )
                _rate_store[key].append(now)
                break

        return await call_next(request)


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
        # Vary: Save-Data for proper cache separation (lite mode)
        ct = response.headers.get("content-type", "")
        if ct.startswith("text/html"):
            existing_vary = response.headers.get("Vary", "")
            if "Save-Data" not in existing_vary:
                parts = [p.strip() for p in existing_vary.split(",") if p.strip()]
                parts.append("Save-Data")
                response.headers["Vary"] = ", ".join(parts)
        return response


class AuthMiddleware(BaseHTTPMiddleware):
    """Require authentication for /admin/* routes (except login)."""
    OPEN_PATHS = {"/admin/login"}

    async def dispatch(self, request, call_next):
        path = request.url.path.rstrip("/")
        # Only protect admin routes
        if path.startswith("/admin") and path not in self.OPEN_PATHS:
            user = auth.get_current_user(request)
            if not user:
                return RedirectResponse(url="/admin/login", status_code=302)
            # Enrich with display_name from cookie (URL-encoded for Cyrillic safety)
            user["display_name"] = _url_unquote(request.cookies.get("display_name", user.get("username", "")))
            request.state.current_user = user
        else:
            user = auth.get_current_user(request)
            if user:
                user["display_name"] = _url_unquote(request.cookies.get("display_name", user.get("username", "")))
            request.state.current_user = user
        response = await call_next(request)
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown."""
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    db.init_db()
    # Preload persons into memory (avoids SQLite query per request)
    try:
        from .public_routes import _refresh_persons_cache
        _refresh_persons_cache()
        logger.info("Persons preloaded into memory on startup")
    except Exception:
        logger.exception("Failed to preload persons on startup")
    # Start scheduled publishing loop
    from .scheduler import scheduler_loop, health_check_loop
    task = asyncio.create_task(scheduler_loop())
    # Start Telegram auto-posting loop
    from .autopost import autopost_loop
    autopost_task = asyncio.create_task(autopost_loop())
    # Start periodic health check (every 6 hours)
    health_task = asyncio.create_task(health_check_loop())
    yield
    task.cancel()
    autopost_task.cancel()
    health_task.cancel()


app = FastAPI(title="Total.kz", version="11.0.0", lifespan=lifespan)
# Middleware execution order is REVERSE of add_middleware order.
# BrotliMiddleware must be outermost (added LAST) to compress final response.
app.add_middleware(CacheControlMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(AuthMiddleware)
app.add_middleware(BrotliMiddleware, minimum_size=500, gzip_fallback=True)

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


# ── Global exception handlers ─────────────────────────────────────
from starlette.exceptions import HTTPException as StarletteHTTPException

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Custom 404/4xx error pages."""
    if exc.status_code == 404:
        try:
            nav = _NAV_SECTIONS
            return HTMLResponse(
                _public_templates.get_template("public/404.html").render(
                    request=request, nav_sections=nav, nav_categories=nav
                ),
                status_code=404,
            )
        except Exception:
            return HTMLResponse(
                "<h1>404 — Страница не найдена</h1><p><a href='/'>На главную</a></p>",
                status_code=404,
            )
    return HTMLResponse(f"Ошибка {exc.status_code}", status_code=exc.status_code)

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all: show 500.html instead of raw error text."""
    logging.error(f"Unhandled exception on {request.url}: {exc}", exc_info=True)
    try:
        nav = _NAV_SECTIONS
        return HTMLResponse(
            _public_templates.get_template("public/500.html").render(
                request=request, nav_sections=nav, nav_categories=nav
            ),
            status_code=500,
        )
    except Exception:
        return HTMLResponse(
            "<h1>500 — Внутренняя ошибка сервера</h1><p><a href='/'>На главную</a></p>",
            status_code=500,
        )


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

import math as _math
def _log_filter(value):
    """Logarithm filter for Jinja2 (tag cloud sizing)."""
    try:
        return _math.log(max(float(value), 1))
    except (ValueError, TypeError):
        return 0.0
templates.env.filters["log"] = _log_filter

# Currency rates (live from NB RK, cached 1h)
from app.currency import get_rates as _get_currency_rates, get_commodities as _get_commodities
templates.env.globals["get_currency_rates"] = _get_currency_rates
templates.env.globals["get_commodities"] = _get_commodities

# Weather (wttr.in, cached 30 min)
from app.weather import get_weather as _get_weather
templates.env.globals["get_weather"] = _get_weather


def _ctx(request: Request, **kwargs) -> dict:
    """Build template context with current_user always available."""
    user = getattr(request.state, "current_user", None)
    ctx = {"request": request, "current_user": user}

    # Build notifications for admin header (review articles + recent comments)
    notifications = []
    notification_count = 0
    if user and str(request.url.path).startswith("/admin"):
        try:
            if settings.use_postgres:
                from app.pg_queries import get_pg_session, Article, ArticleComment
                from sqlalchemy import select, func, desc
                with get_pg_session() as sess:
                    # Articles needing review
                    review_rows = sess.execute(
                        select(Article.id, Article.title, Article.updated_at)
                        .where(Article.status == "review")
                        .order_by(Article.updated_at.desc())
                        .limit(8)
                    ).all()
                    for r in review_rows:
                        title = (r.title or "Без заголовка")[:60]
                        notifications.append({
                            "url": f"/admin/article/{r.id}",
                            "title": f"На рецензии: {title}",
                            "time": str(r.updated_at)[:16] if r.updated_at else "",
                            "icon": "📝",
                            "icon_class": "notif-icon-review",
                        })
                    # Recent comments (last 5)
                    comment_rows = sess.execute(
                        select(ArticleComment.article_id, ArticleComment.username,
                               ArticleComment.comment, ArticleComment.created_at)
                        .order_by(ArticleComment.created_at.desc())
                        .limit(5)
                    ).all()
                    for c in comment_rows:
                        notifications.append({
                            "url": f"/admin/article/{c.article_id}",
                            "title": f"{c.username}: {(c.comment or '')[:50]}",
                            "time": str(c.created_at)[:16] if c.created_at else "",
                            "icon": "💬",
                            "icon_class": "notif-icon-comment",
                        })
            else:
                with db.get_db() as conn:
                    review_rows = conn.execute(
                        "SELECT id, title, updated_at FROM articles WHERE status='review' ORDER BY updated_at DESC LIMIT 8"
                    ).fetchall()
                    for r in review_rows:
                        title = (r["title"] or "Без заголовка")[:60]
                        notifications.append({
                            "url": f"/admin/article/{r['id']}",
                            "title": f"На рецензии: {title}",
                            "time": str(r["updated_at"])[:16] if r["updated_at"] else "",
                            "icon": "📝",
                            "icon_class": "notif-icon-review",
                        })
                    comment_rows = conn.execute(
                        "SELECT article_id, username, comment, created_at FROM article_comments ORDER BY created_at DESC LIMIT 5"
                    ).fetchall()
                    for c in comment_rows:
                        notifications.append({
                            "url": f"/admin/article/{c['article_id']}",
                            "title": f"{c['username']}: {(c['comment'] or '')[:50]}",
                            "time": str(c["created_at"])[:16] if c["created_at"] else "",
                            "icon": "💬",
                            "icon_class": "notif-icon-comment",
                        })
            notification_count = len(notifications)
        except Exception:
            pass  # Don't break page rendering for notification errors

    ctx["notifications"] = notifications
    ctx["notification_count"] = notification_count
    ctx.update(kwargs)
    return ctx

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


# Cyrillic → Latin transliteration for slug generation
_TRANSLIT = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
    'қ': 'q', 'ұ': 'u', 'ү': 'u', 'ғ': 'g', 'ң': 'n', 'ө': 'o',
    'і': 'i', 'ә': 'a', 'һ': 'h',
}


def _slugify(text: str) -> str:
    """Transliterate Russian/Kazakh text and create URL slug."""
    text = text.lower().strip()
    result = []
    for ch in text:
        if ch in _TRANSLIT:
            result.append(_TRANSLIT[ch])
        elif ch.isascii() and (ch.isalnum() or ch in '-_ '):
            result.append(ch)
        else:
            result.append(' ')
    slug = re.sub(r'[\s_]+', '-', ''.join(result)).strip('-')
    slug = re.sub(r'-+', '-', slug)
    return slug[:120]





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
#  HEADLESS CONTENT API (qazstack.content.api)
# ══════════════════════════════════════════════
if not settings.use_postgres:
    _content_provider = SQLiteContentProvider(db.get_db_path())
    app.include_router(
        content_router(_content_provider),
        prefix="/api/content",
        tags=["content"],
    )


# ══════════════════════════════════════════════
#  ADMIN DASHBOARD  /admin/
# ══════════════════════════════════════════════

@app.get("/admin", response_class=HTMLResponse)
@app.get("/admin/", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user:
        return RedirectResponse(url="/admin/articles", status_code=302)
    stats = db.get_stats()

    # Quick action widget counts
    qa_counts = {"review": 0, "scheduled": 0, "no_image": 0, "no_tags": 0}
    recent_articles = []
    attention_articles = []
    try:
        if settings.use_postgres:
            from app.pg_queries import get_pg_session, Article, ArticleTag
            from sqlalchemy import select, func, or_, and_
            with get_pg_session() as sess:
                qa_counts["review"] = sess.execute(
                    select(func.count()).select_from(Article).where(Article.status == "review")
                ).scalar() or 0
                qa_counts["scheduled"] = sess.execute(
                    select(func.count()).select_from(Article).where(
                        and_(Article.scheduled_at.isnot(None), Article.scheduled_at != "")
                    )
                ).scalar() or 0
                qa_counts["no_image"] = sess.execute(
                    select(func.count()).select_from(Article).where(
                        or_(Article.main_image.is_(None), Article.main_image == "")
                    ).where(Article.status != "archived")
                ).scalar() or 0
                qa_counts["no_tags"] = sess.execute(
                    select(func.count()).select_from(Article).where(
                        or_(Article.tags.is_(None), Article.tags == "", Article.tags == "[]")
                    ).where(Article.status != "archived")
                ).scalar() or 0
        else:
            with db.get_db() as conn:
                qa_counts["review"] = conn.execute("SELECT COUNT(*) FROM articles WHERE status='review'").fetchone()[0]
                qa_counts["scheduled"] = conn.execute("SELECT COUNT(*) FROM articles WHERE scheduled_at IS NOT NULL AND scheduled_at != ''").fetchone()[0]
                qa_counts["no_image"] = conn.execute("SELECT COUNT(*) FROM articles WHERE (main_image IS NULL OR main_image = '') AND status != 'archived'").fetchone()[0]
                qa_counts["no_tags"] = conn.execute("SELECT COUNT(*) FROM articles WHERE (tags IS NULL OR tags = '' OR tags = '[]') AND status != 'archived'").fetchone()[0]
    except Exception:
        pass

    # Recent articles for dashboard
    try:
        if settings.use_postgres:
            from app.pg_queries import get_pg_session, Article
            from sqlalchemy import select
            with get_pg_session() as sess:
                rows = sess.execute(
                    select(Article.id, Article.title, Article.author, Article.pub_date, Article.sub_category, Article.status)
                    .where(Article.status == "published")
                    .where(Article.pub_date.isnot(None))
                    .order_by(Article.pub_date.desc())
                    .limit(10)
                ).fetchall()
                recent_articles = [{"id": r.id, "title": r.title, "author": r.author, "pub_date": r.pub_date, "sub_category": r.sub_category, "status": r.status} for r in rows]
                rows2 = sess.execute(
                    select(Article.id, Article.title)
                    .where(Article.status == "review")
                    .order_by(Article.pub_date.desc())
                    .limit(5)
                ).fetchall()
                attention_articles = [{"id": r.id, "title": r.title} for r in rows2]
        else:
            with db.get_db() as conn:
                rows = conn.execute("""
                    SELECT id, title, author, pub_date, sub_category, status
                    FROM articles
                    WHERE status='published' AND pub_date IS NOT NULL
                    ORDER BY pub_date DESC LIMIT 10
                """).fetchall()
                recent_articles = [dict(r) for r in rows]
                rows2 = conn.execute("""
                    SELECT id, title FROM articles WHERE status='review'
                    ORDER BY pub_date DESC LIMIT 5
                """).fetchall()
                attention_articles = [dict(r) for r in rows2]
    except Exception:
        pass

    return templates.TemplateResponse("dashboard.html", _ctx(request,
        stats=stats,
        qa_counts=qa_counts,
        recent_articles=recent_articles,
        attention_articles=attention_articles,
    ))


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
    status: str = "",
    assigned_to: str = "",
    my: str = "",
    has_image: str = "",
    has_tags: str = "",
    has_enrichment: str = "",
    page: int = Query(1, ge=1),
):
    user = getattr(request.state, "current_user", None)

    # Default "Мои статьи" for journalists when no explicit filter is set
    qs = str(request.url.query)
    if (
        user
        and user.get("role") == "journalist"
        and not my
        and "my=" not in qs
        and not q and not category and not author and not status
        and not assigned_to and not tag and not entity_id
    ):
        my = "1"
    # Explicit my=0 means user chose "All" — clear the flag
    if my == "0":
        my = ""

    # "Мои статьи" filter for current user
    effective_assigned = assigned_to
    if my == "1" and user:
        effective_assigned = user.get("username", "")

    result = db.search_articles(
        query=q, category=category, author=author,
        date_from=date_from, date_to=date_to,
        tag=tag, entity_id=entity_id, status=status,
        assigned_to=effective_assigned, page=page,
    )

    # Post-filter for has_image / has_tags / has_enrichment
    # These filters work on the full result set via extra SQL queries
    if has_image or has_tags or has_enrichment:
        try:
            if settings.use_postgres:
                from app.pg_queries import get_pg_session, Article, ArticleTag, ArticleEntity
                from sqlalchemy import select, func, or_, and_, distinct
                with get_pg_session() as sess:
                    base = select(Article.id, Article.url, Article.pub_date, Article.sub_category,
                                  Article.category_label, Article.title, Article.author,
                                  Article.excerpt, Article.thumbnail, Article.main_image,
                                  Article.status, Article.updated_at, Article.assigned_to)
                    conditions = []
                    if q:
                        conditions.append(or_(Article.title.ilike(f"%{q}%"), Article.body_text.ilike(f"%{q}%")))
                    if category:
                        conditions.append(Article.sub_category == category)
                    if status:
                        conditions.append(Article.status == status)
                    if author:
                        conditions.append(Article.author == author)
                    if effective_assigned:
                        conditions.append(Article.assigned_to == effective_assigned)
                    if date_from:
                        conditions.append(Article.pub_date >= date_from)
                    if date_to:
                        conditions.append(Article.pub_date <= date_to + " 23:59:59")

                    if has_image == "1":
                        conditions.append(and_(Article.main_image.isnot(None), Article.main_image != ""))
                    elif has_image == "0":
                        conditions.append(or_(Article.main_image.is_(None), Article.main_image == ""))

                    if has_tags == "1":
                        conditions.append(and_(Article.tags.isnot(None), Article.tags != "", Article.tags != "[]"))
                    elif has_tags == "0":
                        conditions.append(or_(Article.tags.is_(None), Article.tags == "", Article.tags == "[]"))

                    if has_enrichment == "1":
                        sub = select(distinct(ArticleEntity.article_id))
                        conditions.append(Article.id.in_(sub))
                    elif has_enrichment == "0":
                        sub = select(distinct(ArticleEntity.article_id))
                        conditions.append(~Article.id.in_(sub))

                    if conditions:
                        base = base.where(*conditions)
                    total = sess.execute(select(func.count()).select_from(base.subquery())).scalar() or 0
                    per_page = 30
                    offset = (page - 1) * per_page
                    rows = sess.execute(
                        base.distinct().order_by(Article.pub_date.desc()).limit(per_page).offset(offset)
                    ).all()
                    keys = ["id", "url", "pub_date", "sub_category", "category_label",
                            "title", "author", "excerpt", "thumbnail", "main_image",
                            "status", "updated_at", "assigned_to"]
                    articles = []
                    for r in rows:
                        d = {}
                        for i, k in enumerate(keys):
                            d[k] = r[i]
                        articles.append(d)
                    pages = max(1, -(-total // per_page))
                    result = {"articles": articles, "total": total, "page": page, "per_page": per_page, "pages": pages}
        except Exception:
            pass  # Fall back to unfiltered result

    stats = db.get_stats()
    authors_list = db.get_authors()
    tags_list = db.get_tags(limit=60)

    entity_name = ""
    entity_type = ""
    if entity_id:
        ent = db.get_entity(entity_id)
        if ent:
            entity_name = ent["name"]
            entity_type = ent["entity_type"]

    # Get status counts for tabs
    if settings.use_postgres:
        status_counts = db.get_status_counts(
            username=user.get("username") if user else None
        )
        if "my" not in status_counts:
            status_counts["my"] = 0
    else:
        with db.get_db() as conn:
            status_counts = {}
            for s in ("published", "draft", "archived", "review", "ready"):
                cnt = conn.execute("SELECT COUNT(*) FROM articles WHERE status = ?", (s,)).fetchone()[0]
                status_counts[s] = cnt
            status_counts["all"] = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
            if user:
                status_counts["my"] = conn.execute(
                    "SELECT COUNT(*) FROM articles WHERE assigned_to = ?",
                    (user.get("username", ""),)
                ).fetchone()[0]
            else:
                status_counts["my"] = 0

    # Get users for bulk assign dropdown
    all_users = db.get_all_users()
    assignable_users = [u for u in all_users if u.get("is_active")]

    return templates.TemplateResponse("articles.html", _ctx(request,
        result=result,
        q=q,
        category=category,
        author=author,
        date_from=date_from,
        date_to=date_to,
        tag=tag,
        entity_id=entity_id,
        entity_name=entity_name,
        entity_type=entity_type,
        status=status,
        assigned_to=assigned_to,
        my=my,
        has_image=has_image,
        has_tags=has_tags,
        has_enrichment=has_enrichment,
        status_counts=status_counts,
        categories=stats["categories"],
        authors=authors_list,
        tags=tags_list,
        assignable_users=assignable_users,
        cat_label=cat_label,
        entity_type_label=entity_type_label,
        format_num=_format_num,
    ))


@app.get("/admin/create", response_class=HTMLResponse)
async def admin_create_article(request: Request):
    stats = db.get_stats()
    cat_slugs = [c["sub_category"] for c in stats["categories"]]
    authors = db.get_authors()
    return templates.TemplateResponse("article_create.html", _ctx(request,
        categories=cat_slugs,
        cat_label=cat_label,
        authors=authors,
        category_labels=CATEGORY_LABELS,
    ))


@app.get("/admin/article/{article_id}", response_class=HTMLResponse)
async def admin_article_detail(request: Request, article_id: int):
    article = db.get_article(article_id)
    if not article:
        return HTMLResponse("Статья не найдена", status_code=404)
    stats = db.get_stats()
    cat_slugs = [c["sub_category"] for c in stats["categories"]]
    user = getattr(request.state, "current_user", None)
    role = user.get("role", "journalist") if user else "journalist"
    current_status = article.get("status") or "published"
    workflow_actions = wf.get_available_actions(current_status, role)
    comments = wf.get_comments(article_id)
    workflow_history = wf.get_workflow_history(article_id)
    # Get journalists for assignment dropdown
    all_users = db.get_all_users()
    journalists = [u for u in all_users if u.get("is_active")]
    # Check translation status for Kazakh
    translation_status = "none"
    try:
        tr_row = db.execute_raw(
            "SELECT id FROM article_translations WHERE article_id = %s AND lang = %s",
            (article_id, "kz"),
        )
        if tr_row:
            translation_status = "translated"
    except Exception:
        pass
    return templates.TemplateResponse("article.html", _ctx(request,
        article=article,
        categories=cat_slugs,
        cat_label=cat_label,
        entity_type_label=entity_type_label,
        category_labels=CATEGORY_LABELS,
        workflow_actions=workflow_actions,
        workflow_statuses=wf.STATUSES,
        comments=comments,
        workflow_history=workflow_history,
        journalists=journalists,
        translation_status=translation_status,
    ))


@app.get("/admin/preview/{article_id}", response_class=HTMLResponse)
async def admin_preview_article(request: Request, article_id: int):
    """Preview article using the public template with a ПРЕВЬЮ banner."""
    article = db.get_article(article_id)
    if not article:
        return HTMLResponse("Статья не найдена", status_code=404)
    category = article.get("sub_category") or "other"
    from .public_routes import (
        cat_label as _cat_label, nav_slug_for as _nav_slug_for,
        estimate_reading_time as _estimate_reading_time,
        get_article_persons as _get_article_persons,
        rewrite_article_images as _rewrite_article_images,
        NAV_SECTIONS as _PREVIEW_NAV_SECTIONS,
        NAV_CATEGORIES as _PREVIEW_NAV_CATEGORIES,
    )
    _rewrite_article_images(article)
    nav_section = _nav_slug_for(category)
    article_persons = _get_article_persons(article.get("entities", []))
    return _public_templates.TemplateResponse("public/article.html", {
        "request": request,
        "article": article,
        "related": [],
        "timeline": {"prev": [], "next": []},
        "timeline_topic": "",
        "timeline_total": 0,
        "category": category,
        "category_name": _cat_label(category),
        "nav_section": nav_section,
        "nav_section_name": _cat_label(nav_section),
        "nav_sections": _PREVIEW_NAV_SECTIONS,
        "nav_categories": _PREVIEW_NAV_CATEGORIES,
        "reading_time": _estimate_reading_time(article.get("body_text", "")),
        "slug": str(article_id),
        "ticker_articles": [],
        "article_persons": article_persons,
        "popular": [],
        "is_preview": True,
    })


@app.get("/admin/content", response_class=HTMLResponse)
async def admin_content_page(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
    persons = db.get_entities(entity_type="person", limit=50)
    orgs = db.get_entities(entity_type="org", limit=50)
    locations = db.get_entities(entity_type="location", limit=50)
    tags = db.get_tags(limit=100)
    authors = db.get_authors()
    stats = db.get_stats()

    cq = seo.get_content_quality(500)
    dupes = seo.get_duplicate_titles(20)

    return templates.TemplateResponse("content.html", _ctx(request,
        persons=persons,
        orgs=orgs,
        locations=locations,
        tags=tags,
        authors=authors,
        total_articles=stats["total"],
        cq=cq,
        dupes=dupes,
        entity_type_label=entity_type_label,
    ))


@app.get("/admin/analytics", response_class=HTMLResponse)
async def admin_analytics_page(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
    gsc = search.get_search_data()
    gsc_json = json.dumps(gsc, ensure_ascii=False)

    geo = seo.get_geo_readiness()
    schema = seo.get_schema_readiness(500)
    freshness = seo.get_freshness_analysis()

    schema_json = json.dumps(schema, ensure_ascii=False)
    fresh_json = json.dumps(freshness, ensure_ascii=False)

    return templates.TemplateResponse("analytics.html", _ctx(request,
        gsc=gsc,
        gsc_json=gsc_json,
        geo=geo,
        schema=schema,
        schema_json=schema_json,
        fresh_json=fresh_json,
        cat_label=cat_label,
        umami_share_url=settings.umami_share_url,
    ))


# ══════════════════════════════════════════════
#  CMS v11: AUTH ROUTES
# ══════════════════════════════════════════════

@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    user = auth.get_current_user(request)
    if user:
        return RedirectResponse(url="/admin", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": "", "username": ""})


@app.post("/admin/login", response_class=HTMLResponse)
async def admin_login_submit(request: Request):
    form = await request.form()
    username = form.get("username", "").strip()
    password = form.get("password", "")
    user = db.get_user_by_username(username)
    if not user or not auth.check_password(password, user["password_hash"]):
        return templates.TemplateResponse("login.html", {
            "request": request, "error": "Неверное имя пользователя или пароль", "username": username,
        })
    if not user.get("is_active"):
        return templates.TemplateResponse("login.html", {
            "request": request, "error": "Аккаунт деактивирован", "username": username,
        })
    # Update last_login
    db.update_user(user["id"], {"last_login": datetime.now().isoformat(timespec="seconds")})
    # Log audit
    ip = request.client.host if request.client else ""
    db.log_audit(user["id"], user["username"], "login", "user", user["id"], "", ip)
    # Create session
    # Load display_name for session
    response = RedirectResponse(url="/admin", status_code=302)
    auth.set_session_cookie(response, user["id"], user["username"], user["role"])
    # Store display_name in a separate non-httponly cookie for UI
    response.set_cookie("display_name", _url_quote(user["display_name"], safe=""), max_age=auth.SESSION_MAX_AGE, path="/", samesite="lax")
    return response


@app.get("/admin/logout")
async def admin_logout(request: Request):
    user = auth.get_current_user(request)
    if user:
        ip = request.client.host if request.client else ""
        db.log_audit(user["user_id"], user["username"], "login", "user", user["user_id"], "logout", ip)
    response = RedirectResponse(url="/admin/login", status_code=302)
    auth.clear_session_cookie(response)
    response.delete_cookie("display_name", path="/")
    return response


# ══════════════════════════════════════════════
#  CMS v11: NEW ADMIN PAGES
# ══════════════════════════════════════════════

@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users_page(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
    users = db.get_all_users()
    return templates.TemplateResponse("users.html", _ctx(request, users=users))


@app.get("/admin/categories", response_class=HTMLResponse)
async def admin_categories_page(request: Request):
    return RedirectResponse(url="/admin/reference?tab=categories", status_code=302)


@app.get("/admin/authors", response_class=HTMLResponse)
async def admin_authors_page(request: Request, q: str = ""):
    return RedirectResponse(url="/admin/reference?tab=authors" + (f"&q={q}" if q else ""), status_code=302)


@app.get("/admin/media", response_class=HTMLResponse)
async def admin_media_page(request: Request, q: str = "", page: int = Query(1, ge=1), sort: str = "newest", media_type: str = ""):
    result = db.get_all_media(q=q, page=page, sort=sort, media_type=media_type)
    return templates.TemplateResponse("media.html", _ctx(request,
        result=result, q=q, sort=sort, media_type=media_type, format_num=_format_num))


@app.get("/admin/tags", response_class=HTMLResponse)
async def admin_tags_page(request: Request, q: str = "", page: int = Query(1, ge=1)):
    return RedirectResponse(url="/admin/reference?tab=tags" + (f"&q={q}" if q else "") + (f"&page={page}" if page > 1 else ""), status_code=302)


@app.get("/admin/entities", response_class=HTMLResponse)
async def admin_entities_page(request: Request, q: str = "", entity_type: str = "", page: int = Query(1, ge=1)):
    return RedirectResponse(url="/admin/reference?tab=entities" + (f"&q={q}" if q else "") + (f"&entity_type={entity_type}" if entity_type else "") + (f"&page={page}" if page > 1 else ""), status_code=302)


@app.get("/admin/reference", response_class=HTMLResponse)
async def admin_reference_page(
    request: Request,
    tab: str = "categories",
    q: str = "",
    page: int = Query(1, ge=1),
    min_articles: int = 50,
    entity_type: str = "",
):
    if settings.use_postgres:
        from app import pg_queries as pgq
        categories = pgq.get_all_categories()
        authors = pgq.get_all_authors_managed(q=q) if tab == "authors" else []
        tags_result = pgq.get_tags_full(q=q, page=page) if tab == "tags" else {"items": [], "total": 0, "page": 1, "pages": 1}
        entities_result = pgq.get_entities_full(q=q, entity_type=entity_type, page=page) if tab == "entities" else {"items": [], "total": 0, "page": 1, "pages": 1}

        # Get totals for tab counters
        if tab != "tags":
            try:
                from app.pg_queries import get_pg_session, ArticleTag, NerEntity
                from sqlalchemy import select, func, distinct
                with get_pg_session() as sess:
                    tags_total = sess.scalar(select(func.count(distinct(ArticleTag.tag)))) or 0
                    tags_result["total"] = tags_total
            except Exception:
                pass
        if tab != "entities":
            try:
                from app.pg_queries import get_pg_session, NerEntity
                from sqlalchemy import select, func
                with get_pg_session() as sess:
                    ent_total = sess.scalar(select(func.count()).select_from(NerEntity)) or 0
                    entities_result["total"] = ent_total
            except Exception:
                pass
        if tab != "authors":
            authors_for_count = pgq.get_all_authors_managed(q="")
        else:
            authors_for_count = authors
    else:
        categories = db.get_all_categories()
        authors = db.get_all_authors_managed(q=q) if tab == "authors" else []
        tags_result = db.get_tags_full(q=q, page=page, min_articles=min_articles) if tab == "tags" else {"items": [], "total": 0, "page": 1, "pages": 1}
        entities_result = db.get_entities_full(q=q, entity_type=entity_type, page=page) if tab == "entities" else {"items": [], "total": 0, "page": 1, "pages": 1}

        # Get totals for tab counters
        if tab != "tags":
            try:
                with db.get_db() as conn:
                    tags_total = conn.execute("SELECT COUNT(DISTINCT tag) FROM article_tags").fetchone()[0]
                    tags_result["total"] = tags_total
            except Exception:
                pass
        if tab != "entities":
            try:
                with db.get_db() as conn:
                    ent_total = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
                    entities_result["total"] = ent_total
            except Exception:
                pass
        if tab != "authors":
            authors_for_count = db.get_all_authors_managed(q="")
        else:
            authors_for_count = authors

    return templates.TemplateResponse("reference.html", _ctx(request,
        tab=tab,
        q=q,
        min_articles=min_articles,
        entity_type=entity_type,
        categories=categories,
        authors=authors if tab == "authors" else authors_for_count,
        tags_result=tags_result,
        entities_result=entities_result,
        format_num=_format_num,
    ))


@app.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings_page(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
    return templates.TemplateResponse("settings.html", _ctx(request))


# ══════════════════════════════════════════════
#  HEALTH MONITORING  /admin/health
# ══════════════════════════════════════════════

@app.get("/admin/health", response_class=HTMLResponse)
async def admin_health_page(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
    return templates.TemplateResponse("health.html", _ctx(request))


@app.post("/api/admin/health-check")
async def api_admin_health_check(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    from app.healthcheck import run_and_save
    report = await asyncio.to_thread(run_and_save)
    return JSONResponse(report)


@app.get("/api/admin/health-check/last")
async def api_admin_health_last(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    from app.healthcheck import HEALTH_DATA_PATH
    if HEALTH_DATA_PATH.exists():
        try:
            data = json.loads(HEALTH_DATA_PATH.read_text())
            return JSONResponse(data)
        except Exception:
            pass
    return JSONResponse(None)


@app.get("/admin/ui-kit", response_class=HTMLResponse)
async def admin_ui_kit_page(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user:
        return RedirectResponse(url="/admin/login", status_code=302)
    return templates.TemplateResponse("ui_kit.html", _ctx(request))


@app.get("/admin/content-analytics", response_class=HTMLResponse)
async def admin_content_analytics_page(request: Request):
    """Content Analytics Dashboard — charts and tables from article data."""
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)

    from datetime import date, timedelta
    today = date.today()
    d30 = (today - timedelta(days=30)).isoformat()
    d7 = (today - timedelta(days=7)).isoformat()

    # Top articles by views (last 30 days)
    top_views_30 = db.execute_raw_many(
        "SELECT id, title, views, pub_date, sub_category FROM articles "
        "WHERE pub_date >= %s AND status = 'published' ORDER BY views DESC LIMIT 20",
        (d30,),
    )
    # Top articles by views (last 7 days)
    top_views_7 = db.execute_raw_many(
        "SELECT id, title, views, pub_date, sub_category FROM articles "
        "WHERE pub_date >= %s AND status = 'published' ORDER BY views DESC LIMIT 20",
        (d7,),
    )
    # Articles per day (last 30 days)
    articles_per_day = db.execute_raw_many(
        "SELECT DATE(pub_date) as day, COUNT(*) as cnt FROM articles "
        "WHERE pub_date >= %s AND status = 'published' GROUP BY DATE(pub_date) ORDER BY day",
        (d30,),
    )
    # Category distribution (last 30 days)
    category_dist = db.execute_raw_many(
        "SELECT sub_category, COUNT(*) as cnt FROM articles "
        "WHERE pub_date >= %s AND status = 'published' GROUP BY sub_category ORDER BY cnt DESC",
        (d30,),
    )
    # Author productivity
    author_prod = db.execute_raw_many(
        "SELECT author, COUNT(*) as cnt, COALESCE(AVG(views), 0) as avg_views FROM articles "
        "WHERE pub_date >= %s AND status = 'published' AND author IS NOT NULL AND author != '' "
        "GROUP BY author ORDER BY cnt DESC LIMIT 20",
        (d30,),
    )

    return templates.TemplateResponse("content_analytics.html", _ctx(
        request,
        active="content-analytics",
        top_views_30=top_views_30,
        top_views_7=top_views_7,
        articles_per_day=articles_per_day,
        category_dist=category_dist,
        author_prod=author_prod,
        format_num=_format_num,
        cat_label=cat_label,
    ))


@app.get("/admin/stories", response_class=HTMLResponse)
async def admin_stories_page(request: Request, q: str = "", page: int = Query(1, ge=1)):
    result = db.get_all_stories(q=q, page=page)
    return templates.TemplateResponse("stories.html", _ctx(request,
        result=result, q=q, format_num=_format_num))


# ══════════════════════════════════════════════
#  v14: AD PLACEMENTS ADMIN
# ══════════════════════════════════════════════

@app.get("/admin/ads", response_class=HTMLResponse)
async def admin_ads_page(request: Request, page_filter: str = ""):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
    try:
        placements = db.get_all_ad_placements(page_filter=page_filter)
        stats = db.get_ad_stats()
    except Exception:
        placements = []
        stats = {"total": 0, "active": 0, "inactive": 0, "booked": 0, "available": 0}
    pages = sorted(set(p["page"] for p in placements))
    return templates.TemplateResponse("ads.html", _ctx(request,
        placements=placements, stats=stats, pages=pages,
        page_filter=page_filter, format_num=_format_num))


@app.post("/api/ads/{slot_id}/toggle")
async def api_toggle_ad(request: Request, slot_id: str):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Нет доступа"}, status_code=403)
    new_state = db.toggle_ad_placement(slot_id)
    ip = request.client.host if request.client else ""
    db.log_audit(user["user_id"], user["username"], "toggle", "ad_placement", 0,
                  f"{'Включена' if new_state else 'Отключена'} позиция {slot_id}", ip)
    return {"ok": True, "is_active": new_state}


@app.post("/api/ads/{slot_id}/update")
async def api_update_ad(request: Request, slot_id: str):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Нет доступа"}, status_code=403)
    body = await request.json()
    ok = db.update_ad_placement(slot_id, body)
    if ok:
        ip = request.client.host if request.client else ""
        db.log_audit(user["user_id"], user["username"], "update", "ad_placement", 0,
                      f"Обновлена позиция {slot_id}: {json.dumps(body, ensure_ascii=False)}", ip)
    return {"ok": ok}


@app.get("/admin/audit", response_class=HTMLResponse)
async def admin_audit_page(
    request: Request,
    user_id: int = 0,
    action: str = "",
    entity_type: str = "",
    date_from: str = "",
    date_to: str = "",
    page: int = Query(1, ge=1),
):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
    result = db.get_audit_log(
        user_id=user_id, action=action, entity_type=entity_type,
        date_from=date_from, date_to=date_to, page=page,
    )
    users = db.get_all_users()
    return templates.TemplateResponse("audit.html", _ctx(request,
        result=result, users=users,
        filter_user_id=user_id, filter_action=action,
        filter_entity_type=entity_type,
        filter_date_from=date_from, filter_date_to=date_to,
        format_num=_format_num,
    ))


# ══════════════════════════════════════════════
#  ADMIN: COMMENT MODERATION
# ══════════════════════════════════════════════

def _comments_db():
    import sqlite3 as _sq
    c = _sq.connect(str(Path(__file__).resolve().parent.parent / "data" / "total.db"))
    c.row_factory = _sq.Row
    return c


@app.get("/admin/comments", response_class=HTMLResponse)
async def admin_comments_page(request: Request, status: str = "pending", page: int = Query(1, ge=1)):
    """Comment moderation page."""
    per_page = 50
    conn = _comments_db()
    total = conn.execute("SELECT COUNT(*) FROM public_comments WHERE status = ?", (status,)).fetchone()[0]
    comments = conn.execute(
        """SELECT pc.*, a.title as article_title, a.sub_category
           FROM public_comments pc
           LEFT JOIN articles a ON a.id = pc.article_id
           WHERE pc.status = ?
           ORDER BY pc.created_at DESC
           LIMIT ? OFFSET ?""",
        (status, per_page, (page - 1) * per_page)
    ).fetchall()

    pending_count = conn.execute("SELECT COUNT(*) FROM public_comments WHERE status = 'pending'").fetchone()[0]
    conn.close()

    return templates.TemplateResponse("comments_admin.html", _ctx(request,
        comments=[dict(c) for c in comments],
        current_status=status,
        total=total,
        page=page,
        pages=max(1, (total + per_page - 1) // per_page),
        pending_count=pending_count,
        format_num=_format_num,
    ))


# ══════════════════════════════════════════════
#  DISPLAY MODE  /admin/display-mode
# ══════════════════════════════════════════════

@app.get("/admin/display-mode", response_class=HTMLResponse)
async def admin_display_mode_page(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") not in ("admin", "editor"):
        return RedirectResponse(url="/admin/articles", status_code=302)
    return templates.TemplateResponse("display_mode.html", _ctx(request))


# ══════════════════════════════════════════════
#  CONTENT CALENDAR  /admin/calendar
# ══════════════════════════════════════════════

@app.get("/admin/calendar", response_class=HTMLResponse)
async def admin_calendar_page(request: Request, year: int = 0, month: int = 0):
    user = getattr(request.state, "current_user", None)
    if not user:
        return RedirectResponse(url="/admin/login", status_code=302)

    today = datetime.now()
    if not year or not month:
        year, month = today.year, today.month
    # Clamp
    if month < 1: month, year = 12, year - 1
    if month > 12: month, year = 1, year + 1

    month_start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        month_end = f"{year + 1:04d}-01-01"
    else:
        month_end = f"{year:04d}-{month + 1:02d}-01"

    # Previous / next month
    prev_m = month - 1 if month > 1 else 12
    prev_y = year if month > 1 else year - 1
    next_m = month + 1 if month < 12 else 1
    next_y = year if month < 12 else year + 1

    # Query articles for this month
    if settings.use_postgres:
        from app.pg_database import SessionLocal
        from app.models import Article
        from sqlalchemy import func as sa_func, select as sa_select

        session = SessionLocal()
        try:
            rows = session.execute(
                sa_select(
                    sa_func.substring(Article.pub_date, 1, 10).label("day"),
                    Article.status,
                    sa_func.count().label("cnt"),
                ).where(
                    Article.pub_date.isnot(None),
                    Article.pub_date >= month_start,
                    Article.pub_date < month_end,
                ).group_by(
                    sa_func.substring(Article.pub_date, 1, 10),
                    Article.status,
                )
            ).all()
            calendar_data = {}
            for row in rows:
                day_str = str(row.day)
                if day_str not in calendar_data:
                    calendar_data[day_str] = {}
                calendar_data[day_str][row.status or "published"] = row.cnt

            # Get article details for click-to-view
            articles_rows = session.execute(
                sa_select(
                    Article.id,
                    Article.title,
                    Article.status,
                    Article.pub_date,
                ).where(
                    Article.pub_date.isnot(None),
                    Article.pub_date >= month_start,
                    Article.pub_date < month_end,
                ).order_by(Article.pub_date)
            ).all()
            articles_by_day = defaultdict(list)
            for a in articles_rows:
                day_str = str(a.pub_date)[:10]
                articles_by_day[day_str].append({
                    "id": a.id,
                    "title": a.title or "(без заголовка)",
                    "status": a.status or "published",
                    "time": str(a.pub_date)[11:16] if a.pub_date and len(str(a.pub_date)) > 10 else "",
                })
        finally:
            session.close()
    else:
        with db.get_db() as conn:
            rows = conn.execute(
                "SELECT DATE(pub_date) as day, status, COUNT(*) as cnt "
                "FROM articles WHERE pub_date >= ? AND pub_date < ? AND pub_date IS NOT NULL "
                "GROUP BY DATE(pub_date), status ORDER BY day",
                (month_start, month_end)
            ).fetchall()
            calendar_data = {}
            for row in rows:
                day_str = row["day"] if isinstance(row, dict) else row[0]
                status = row["status"] if isinstance(row, dict) else row[1]
                cnt = row["cnt"] if isinstance(row, dict) else row[2]
                if day_str not in calendar_data:
                    calendar_data[day_str] = {}
                calendar_data[day_str][status or "published"] = cnt

            articles_raw = conn.execute(
                "SELECT id, title, status, pub_date FROM articles "
                "WHERE pub_date >= ? AND pub_date < ? AND pub_date IS NOT NULL "
                "ORDER BY pub_date",
                (month_start, month_end)
            ).fetchall()
            articles_by_day = defaultdict(list)
            for a in articles_raw:
                aid = a["id"] if isinstance(a, dict) else a[0]
                title = a["title"] if isinstance(a, dict) else a[1]
                status = a["status"] if isinstance(a, dict) else a[2]
                pub = a["pub_date"] if isinstance(a, dict) else a[3]
                day_str = str(pub)[:10]
                articles_by_day[day_str].append({
                    "id": aid,
                    "title": title or "(без заголовка)",
                    "status": status or "published",
                    "time": str(pub)[11:16] if pub and len(str(pub)) > 10 else "",
                })

    # Stats
    total_published = sum(d.get("published", 0) for d in calendar_data.values())
    total_scheduled = sum(d.get("scheduled", 0) for d in calendar_data.values())
    total_draft = sum(d.get("draft", 0) for d in calendar_data.values())

    # Build calendar weeks
    cal = _calendar_mod.Calendar(firstweekday=0)  # Monday first
    month_days = cal.monthdayscalendar(year, month)

    MONTH_NAMES_RU = [
        "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
    ]

    return templates.TemplateResponse("calendar.html", _ctx(request,
        year=year,
        month=month,
        month_name=MONTH_NAMES_RU[month],
        prev_y=prev_y, prev_m=prev_m,
        next_y=next_y, next_m=next_m,
        calendar_data=json.dumps(calendar_data, ensure_ascii=False),
        articles_by_day=json.dumps(dict(articles_by_day), ensure_ascii=False),
        month_days=month_days,
        today_str=today.strftime("%Y-%m-%d"),
        total_published=total_published,
        total_scheduled=total_scheduled,
        total_draft=total_draft,
    ))


@app.post("/api/comments/{comment_id}/moderate")
async def moderate_comment(comment_id: int, request: Request):
    """Approve or reject a comment."""
    body = await request.json()
    action = body.get("action")  # 'approve' or 'reject' or 'delete'
    user = getattr(request.state, "current_user", None)
    username = user.get("username", "admin") if user else "admin"

    conn = _comments_db()
    if action == "delete":
        conn.execute("DELETE FROM public_comments WHERE id = ?", (comment_id,))
    elif action in ("approve", "reject"):
        new_status = "approved" if action == "approve" else "rejected"
        conn.execute(
            "UPDATE public_comments SET status = ?, moderated_at = datetime('now'), moderated_by = ? WHERE id = ?",
            (new_status, username, comment_id)
        )
    conn.commit()
    conn.close()
    return {"ok": True}


# ══════════════════════════════════════════════
#  CMS v11: USER API
# ══════════════════════════════════════════════

@app.post("/api/user")
async def api_create_user(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Нет доступа"}, status_code=403)
    body = await request.json()
    username = (body.get("username") or "").strip()
    password = body.get("password", "")
    if not username or not password:
        return JSONResponse({"ok": False, "error": "Имя и пароль обязательны"}, status_code=400)
    if db.get_user_by_username(username):
        return JSONResponse({"ok": False, "error": "Пользователь уже существует"}, status_code=400)
    try:
        new_id = db.create_user({
            "username": username,
            "password_hash": auth.hash_password(password),
            "display_name": body.get("display_name", username),
            "email": body.get("email", ""),
            "role": body.get("role", "journalist"),
        })
        ip = request.client.host if request.client else ""
        db.log_audit(user["user_id"], user["username"], "create", "user", new_id,
                      f"Создан пользователь @{username}", ip)
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.patch("/api/user/{user_id}")
async def api_update_user(request: Request, user_id: int):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Нет доступа"}, status_code=403)
    body = await request.json()
    updates = {}
    for k in ("email", "display_name", "role", "is_active", "avatar_url"):
        if k in body:
            updates[k] = body[k]
    if "password" in body and body["password"]:
        updates["password_hash"] = auth.hash_password(body["password"])
    try:
        db.update_user(user_id, updates)
        ip = request.client.host if request.client else ""
        db.log_audit(user["user_id"], user["username"], "update", "user", user_id, "", ip)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/user/{user_id}")
async def api_delete_user(request: Request, user_id: int):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Нет доступа"}, status_code=403)
    if user["user_id"] == user_id:
        return JSONResponse({"ok": False, "error": "Нельзя удалить себя"}, status_code=400)
    try:
        db.delete_user(user_id)
        ip = request.client.host if request.client else ""
        db.log_audit(user["user_id"], user["username"], "delete", "user", user_id, "", ip)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ══════════════════════════════════════════════
#  CMS v11: CATEGORY API
# ══════════════════════════════════════════════

@app.post("/api/category")
async def api_create_category(request: Request):
    body = await request.json()
    if not body.get("slug") or not body.get("name_ru"):
        return JSONResponse({"ok": False, "error": "Slug и название обязательны"}, status_code=400)
    try:
        new_id = db.create_category(body)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "create", "category", new_id,
                          body.get("name_ru", ""), request.client.host if request.client else "")
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.patch("/api/category/{cat_id}")
async def api_update_category(request: Request, cat_id: int):
    body = await request.json()
    try:
        db.update_category(cat_id, body)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "update", "category", cat_id,
                          "", request.client.host if request.client else "")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/category/{cat_id}")
async def api_delete_category(request: Request, cat_id: int):
    try:
        db.delete_category(cat_id)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "delete", "category", cat_id,
                          "", request.client.host if request.client else "")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ══════════════════════════════════════════════
#  CMS v11: AUTHOR API
# ══════════════════════════════════════════════

@app.post("/api/author")
async def api_create_author(request: Request):
    body = await request.json()
    if not body.get("name") or not body.get("slug"):
        return JSONResponse({"ok": False, "error": "Имя и slug обязательны"}, status_code=400)
    try:
        new_id = db.create_author_managed(body)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "create", "author", new_id,
                          body.get("name", ""), request.client.host if request.client else "")
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.patch("/api/author/{author_id}")
async def api_update_author(request: Request, author_id: int):
    body = await request.json()
    try:
        db.update_author_managed(author_id, body)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "update", "author", author_id,
                          "", request.client.host if request.client else "")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/author/{author_id}")
async def api_delete_author(request: Request, author_id: int):
    try:
        db.delete_author_managed(author_id)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "delete", "author", author_id,
                          "", request.client.host if request.client else "")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ══════════════════════════════════════════════
#  CMS v11: MEDIA API
# ══════════════════════════════════════════════

def _optimize_image(content: bytes, ext: str) -> tuple[bytes, str]:
    """Resize to max 1200px, quality 85, strip metadata. Returns (bytes, ext)."""
    if ext in (".svg", ".gif"):
        return content, ext
    try:
        from PIL import Image
        import io
        img = Image.open(io.BytesIO(content))
        # Strip EXIF/metadata by creating a clean copy
        if img.mode in ("RGBA", "P"):
            clean = Image.new("RGBA", img.size)
            clean.paste(img)
        else:
            clean = Image.new("RGB", img.size)
            clean.paste(img)
        # Resize if wider than 1200px
        max_w = 1200
        if clean.width > max_w:
            ratio = max_w / clean.width
            new_h = int(clean.height * ratio)
            clean = clean.resize((max_w, new_h), Image.LANCZOS)
        buf = io.BytesIO()
        if ext in (".png",):
            clean.save(buf, format="PNG", optimize=True)
        else:
            if clean.mode == "RGBA":
                clean = clean.convert("RGB")
            clean.save(buf, format="JPEG", quality=85, optimize=True)
            ext = ".jpg"
        return buf.getvalue(), ext
    except Exception:
        logger.warning("Image optimization failed, using original", exc_info=True)
        return content, ext


@app.post("/api/media/upload")
async def api_media_upload(request: Request, file: UploadFile = File(...)):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return JSONResponse({"ok": False, "error": "Неподдерживаемый формат файла"}, status_code=400)
    content = await file.read()
    if len(content) > MAX_UPLOAD_SIZE:
        return JSONResponse({"ok": False, "error": "Файл слишком большой (макс. 10 МБ)"}, status_code=400)
    content, ext = _optimize_image(content, ext)
    filename = f"{uuid.uuid4().hex}{ext}"
    filepath = MEDIA_DIR / filename
    filepath.write_bytes(content)
    mime = file.content_type or f"image/{ext.lstrip('.')}"
    user = getattr(request.state, "current_user", None)
    new_id = db.create_media({
        "filename": filename,
        "original_name": file.filename or filename,
        "mime_type": mime,
        "file_size": len(content),
        "url": f"/media/{filename}",
        "uploaded_by": user["user_id"] if user else None,
    })
    if user:
        db.log_audit(user["user_id"], user["username"], "create", "media", new_id,
                      file.filename or "", request.client.host if request.client else "")
    return {"ok": True, "id": new_id, "url": f"/media/{filename}"}


@app.patch("/api/media/{media_id}")
async def api_update_media(request: Request, media_id: int):
    body = await request.json()
    try:
        db.update_media(media_id, body)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/media/{media_id}")
async def api_delete_media(request: Request, media_id: int):
    item = db.delete_media(media_id)
    if not item:
        return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
    # Delete physical file
    try:
        (MEDIA_DIR / item["filename"]).unlink(missing_ok=True)
    except Exception:
        pass
    user = getattr(request.state, "current_user", None)
    if user:
        db.log_audit(user["user_id"], user["username"], "delete", "media", media_id,
                      item.get("original_name", ""), request.client.host if request.client else "")
    return {"ok": True}


@app.get("/media/{path:path}")
async def serve_media(path: str):
    """Serve uploaded media files (supports subdirectories like /media/ab/file.jpg)."""
    # Sanitize path
    if ".." in path:
        return Response(status_code=400)
    filepath = MEDIA_DIR / path
    if not filepath.exists() or not filepath.is_file():
        return Response(status_code=404)
    ext = filepath.suffix.lower()
    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml"}
    mime = mime_map.get(ext, "application/octet-stream")
    return FileResponse(
        filepath, media_type=mime,
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


# ══════════════════════════════════════════════
#  CMS v11: TAG API
# ══════════════════════════════════════════════

@app.post("/api/tag")
async def api_create_tag(request: Request):
    body = await request.json()
    tag = (body.get("tag") or "").strip()
    article_id = body.get("article_id")
    if not tag:
        return JSONResponse({"ok": False, "error": "Тег обязателен"}, status_code=400)
    if article_id:
        if settings.use_postgres:
            db.add_tag_to_article(article_id, tag)
        else:
            with db.get_db() as conn:
                conn.execute("INSERT OR IGNORE INTO article_tags (article_id, tag) VALUES (?, ?)", (article_id, tag))
                conn.commit()
    return {"ok": True}


@app.patch("/api/tag/{old_tag:path}")
async def api_rename_tag(request: Request, old_tag: str):
    body = await request.json()
    new_tag = (body.get("new_tag") or "").strip()
    if not new_tag:
        return JSONResponse({"ok": False, "error": "Новый тег обязателен"}, status_code=400)
    count = db.rename_tag(old_tag, new_tag)
    user = getattr(request.state, "current_user", None)
    if user:
        db.log_audit(user["user_id"], user["username"], "update", "tag", 0,
                      f"{old_tag} → {new_tag}", request.client.host if request.client else "")
    return {"ok": True, "updated": count}


@app.delete("/api/tag/{tag:path}")
async def api_delete_tag(request: Request, tag: str):
    count = db.delete_tag(tag)
    user = getattr(request.state, "current_user", None)
    if user:
        db.log_audit(user["user_id"], user["username"], "delete", "tag", 0,
                      tag, request.client.host if request.client else "")
    return {"ok": True, "deleted": count}


@app.post("/api/tags/merge")
async def api_merge_tags(request: Request):
    body = await request.json()
    tags = body.get("tags", [])
    target = (body.get("target") or "").strip()
    if len(tags) < 2 or not target:
        return JSONResponse({"ok": False, "error": "Нужно минимум 2 тега и целевой тег"}, status_code=400)
    count = db.merge_tags(tags, target)
    user = getattr(request.state, "current_user", None)
    if user:
        db.log_audit(user["user_id"], user["username"], "update", "tag", 0,
                      f"Объединено {len(tags)} тегов → {target}", request.client.host if request.client else "")
    return {"ok": True, "merged": count}


# ══════════════════════════════════════════════
#  CMS v11: ENTITY API
# ══════════════════════════════════════════════

@app.post("/api/entity")
async def api_create_entity(request: Request):
    body = await request.json()
    name = (body.get("name") or "").strip()
    entity_type = body.get("entity_type", "person")
    if not name:
        return JSONResponse({"ok": False, "error": "Имя обязательно"}, status_code=400)
    try:
        new_id = db.create_entity({
            "name": name,
            "short_name": body.get("short_name", ""),
            "entity_type": entity_type,
        })
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "create", "entity", new_id,
                          name, request.client.host if request.client else "")
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.patch("/api/entity/{entity_id}")
async def api_update_entity(request: Request, entity_id: int):
    body = await request.json()
    try:
        db.update_entity(entity_id, body)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "update", "entity", entity_id,
                          "", request.client.host if request.client else "")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/entity/{entity_id}")
async def api_delete_entity(request: Request, entity_id: int):
    try:
        db.delete_entity(entity_id)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "delete", "entity", entity_id,
                          "", request.client.host if request.client else "")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/entities/merge")
async def api_merge_entities(request: Request):
    body = await request.json()
    entity_ids = body.get("entity_ids", [])
    target_id = body.get("target_id")
    if len(entity_ids) < 2 or not target_id:
        return JSONResponse({"ok": False, "error": "Нужно минимум 2 сущности и целевой ID"}, status_code=400)
    count = db.merge_entities(entity_ids, target_id)
    user = getattr(request.state, "current_user", None)
    if user:
        db.log_audit(user["user_id"], user["username"], "update", "entity", target_id,
                      f"Объединено {len(entity_ids)} сущностей", request.client.host if request.client else "")
    return {"ok": True, "merged": count}


# ══════════════════════════════════════════════
#  CMS v11: STORY API
# ══════════════════════════════════════════════

@app.get("/api/story/{story_id}")
async def api_get_story(story_id: int):
    story = db.get_story(story_id)
    if not story:
        return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
    return {"ok": True, "story": story}


@app.post("/api/story")
async def api_create_story(request: Request):
    body = await request.json()
    title = (body.get("title_ru") or "").strip()
    if not title:
        return JSONResponse({"ok": False, "error": "Название обязательно"}, status_code=400)
    try:
        new_id = db.create_story(body)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "create", "story", new_id,
                          title, request.client.host if request.client else "")
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.patch("/api/story/{story_id}")
async def api_update_story(request: Request, story_id: int):
    body = await request.json()
    try:
        db.update_story(story_id, body)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "update", "story", story_id,
                          "", request.client.host if request.client else "")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/story/{story_id}")
async def api_delete_story(request: Request, story_id: int):
    try:
        db.delete_story(story_id)
        user = getattr(request.state, "current_user", None)
        if user:
            db.log_audit(user["user_id"], user["username"], "delete", "story", story_id,
                          "", request.client.host if request.client else "")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/story/{story_id}/articles")
async def api_add_to_story(request: Request, story_id: int):
    body = await request.json()
    article_id = body.get("article_id")
    if not article_id:
        return JSONResponse({"ok": False, "error": "article_id обязателен"}, status_code=400)
    try:
        db.add_article_to_story(story_id, article_id)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/story/{story_id}/articles/{article_id}")
async def api_remove_from_story(request: Request, story_id: int, article_id: int):
    try:
        db.remove_article_from_story(story_id, article_id)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ══════════════════════════════════════════════
#  CMS v11: BULK ARTICLE ACTIONS
# ══════════════════════════════════════════════

@app.post("/api/articles/bulk")
async def api_articles_bulk(request: Request):
    body = await request.json()
    article_ids = body.get("article_ids", [])
    action = body.get("action", "")
    if not article_ids:
        return JSONResponse({"ok": False, "error": "Нет выбранных статей"}, status_code=400)
    user = getattr(request.state, "current_user", None)
    ip = request.client.host if request.client else ""
    if action == "status":
        status = body.get("status", "")
        if status not in ("published", "draft", "archived", "review", "ready"):
            return JSONResponse({"ok": False, "error": "Неверный статус"}, status_code=400)
        updated = db.bulk_update_articles(article_ids, {"status": status})
        if user:
            db.log_audit(user["user_id"], user["username"], "bulk", "article", 0,
                          f"Статус → {status} для {len(article_ids)} статей", ip)
        return {"ok": True, "updated": updated}
    elif action == "category":
        cat = body.get("category", "")
        updated = db.bulk_update_articles(article_ids, {"sub_category": cat})
        if user:
            db.log_audit(user["user_id"], user["username"], "bulk", "article", 0,
                          f"Категория → {cat} для {len(article_ids)} статей", ip)
        return {"ok": True, "updated": updated}
    elif action == "delete":
        updated = db.bulk_delete_articles(article_ids)
        if user:
            db.log_audit(user["user_id"], user["username"], "bulk", "article", 0,
                          f"Архивировано {len(article_ids)} статей", ip)
        return {"ok": True, "updated": updated}
    return JSONResponse({"ok": False, "error": "Неизвестное действие"}, status_code=400)


# ══════════════════════════════════════════════
#  CMS v12: EDITORIAL WORKFLOW API
# ══════════════════════════════════════════════

@app.post("/api/article/{article_id}/workflow/{action}")
async def api_workflow_action(article_id: int, action: str, request: Request):
    """Execute a workflow transition on an article."""
    user = getattr(request.state, "current_user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "Не авторизован"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        body = {}
    comment = (body.get("comment") or "").strip()
    try:
        result = wf.execute_transition(article_id, action, user, comment)
        return result
    except wf.PermissionDeniedError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=403)
    except wf.InvalidStateError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=409)
    except wf.WorkflowError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/article/{article_id}/assign")
async def api_assign_article(article_id: int, request: Request):
    """Assign an article to a user."""
    user = getattr(request.state, "current_user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "Не авторизован"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        body = {}
    assigned_to = (body.get("assigned_to") or "").strip()
    try:
        result = wf.assign_article(article_id, assigned_to, user)
        return result
    except wf.PermissionDeniedError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=403)
    except wf.WorkflowError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/article/{article_id}/comment")
async def api_add_comment(article_id: int, request: Request):
    """Add a comment to an article."""
    user = getattr(request.state, "current_user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "Не авторизован"}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Невалидный JSON"}, status_code=400)
    comment_text = (body.get("comment") or "").strip()
    if not comment_text:
        return JSONResponse({"ok": False, "error": "Комментарий не может быть пустым"}, status_code=400)
    try:
        comment = wf.add_comment(
            article_id, user["user_id"], user["username"], user["role"], comment_text
        )
        return {"ok": True, "comment": comment}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/article/{article_id}/comments")
async def api_get_comments(article_id: int):
    """Get all comments for an article."""
    try:
        comments = wf.get_comments(article_id)
        return {"ok": True, "comments": comments}
    except Exception as e:
        return {"ok": False, "error": str(e), "comments": []}


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
    allowed = {"title", "excerpt", "sub_category", "author", "main_image", "tags",
               "body_html", "body_text", "status", "editor_note",
               "body_blocks", "scheduled_at", "focal_x", "focal_y", "assigned_to",
               "is_breaking"}
    updates = {k: v for k, v in body.items() if k in allowed}

    # If body_blocks provided, auto-generate body_html and body_text
    if "body_blocks" in updates and updates["body_blocks"]:
        try:
            updates["body_html"] = db.blocks_to_html(updates["body_blocks"])
            updates["body_text"] = db.blocks_to_text(updates["body_blocks"])
        except Exception:
            pass
    if not updates:
        return {"ok": False, "error": "Нет полей для обновления"}
    # Auto-set updated_at
    updates["updated_at"] = datetime.now().isoformat(timespec="seconds")

    # Determine revision type
    revision_type = body.get("_revision_type", "edit")

    # Build diff for revision tracking
    try:
        old_article = db.get_article(article_id)
        changes = {}
        if old_article and revision_type != "auto_save":
            for field in ("title", "excerpt", "sub_category", "author", "main_image", "status", "editor_note"):
                if field in updates:
                    old_val = old_article.get(field, "")
                    new_val = updates[field]
                    if str(old_val or "") != str(new_val or ""):
                        changes[field] = {"old": old_val, "new": new_val}
            if "tags" in updates:
                old_tags = old_article.get("tags", [])
                new_tags = updates["tags"]
                if old_tags != new_tags:
                    changes["tags"] = {"old": old_tags, "new": new_tags}
    except Exception:
        changes = {}

    user = getattr(request.state, "current_user", None)
    changed_by = user.get("username", "") if user else ""

    try:
        db.update_article(article_id, updates)
        # Record revision
        try:
            db.record_revision(article_id, changes, revision_type=revision_type, changed_by=changed_by)
        except Exception:
            pass
        # Index in Meilisearch (fire-and-forget)
        try:
            article_data = db.get_article(article_id)
            if article_data:
                meili.index_article(article_data)
        except Exception:
            pass
        # Ping WebSub when article is published
        if updates.get("status") == "published":
            try:
                from .public_routes import ping_websub_hub
                ping_websub_hub()
            except Exception:
                pass
            # Auto-translate to Kazakh (background, non-blocking)
            try:
                from .auto_translate import auto_translate_article
                asyncio.get_event_loop().run_in_executor(None, auto_translate_article, article_id)
            except Exception:
                pass
        # Audit log (skip auto_save to avoid noise)
        if user and revision_type != "auto_save":
            try:
                ip = request.client.host if request.client else ""
                action = "edit"
                details = ""
                if "status" in changes:
                    action = "status_change"
                    details = f"Статус: {changes['status'].get('old', '')} → {changes['status'].get('new', '')}"
                elif changes:
                    details = f"Изменено: {', '.join(changes.keys())}"
                db.log_audit(user["user_id"], user["username"], action, "article", article_id, details, ip)
            except Exception:
                pass
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/article")
async def api_create_article(request: Request):
    body = await request.json()
    title = (body.get("title") or "").strip()
    sub_category = (body.get("sub_category") or "").strip()
    if not title:
        return JSONResponse({"ok": False, "error": "Заголовок обязателен"}, status_code=400)
    if not sub_category:
        return JSONResponse({"ok": False, "error": "Категория обязательна"}, status_code=400)
    slug = _slugify(title)
    url = f"https://total.kz/ru/news/{sub_category}/{slug}"

    body_blocks = body.get("body_blocks", None)
    body_html = body.get("body_html", "")
    body_text = body.get("body_text", "")

    # If body_blocks provided, auto-generate body_html and body_text
    if body_blocks:
        try:
            body_html = db.blocks_to_html(body_blocks)
            body_text = db.blocks_to_text(body_blocks)
        except Exception:
            pass

    data = {
        "url": url,
        "title": title,
        "sub_category": sub_category,
        "category_label": CATEGORY_LABELS.get(sub_category, sub_category),
        "author": body.get("author", ""),
        "excerpt": body.get("excerpt", ""),
        "body_html": body_html,
        "body_text": body_text,
        "main_image": body.get("main_image", ""),
        "tags": body.get("tags", []),
        "status": body.get("status", "draft"),
        "editor_note": body.get("editor_note", ""),
        "body_blocks": body_blocks if isinstance(body_blocks, str) else (json.dumps(body_blocks) if body_blocks else None),
        "scheduled_at": body.get("scheduled_at", None),
        "focal_x": body.get("focal_x", 0.5),
        "focal_y": body.get("focal_y", 0.5),
    }
    try:
        new_id = db.create_article(data)
        # Index in Meilisearch
        try:
            data["id"] = new_id
            meili.index_article(data)
        except Exception:
            pass
        # Ping WebSub hub for real-time feed notifications
        if data.get("status") == "published":
            try:
                from .public_routes import ping_websub_hub
                ping_websub_hub()
            except Exception:
                pass
            # Auto-translate to Kazakh (background, non-blocking)
            try:
                from .auto_translate import auto_translate_article
                asyncio.get_event_loop().run_in_executor(None, auto_translate_article, new_id)
            except Exception:
                pass
        # Audit log
        user = getattr(request.state, "current_user", None)
        if user:
            ip = request.client.host if request.client else ""
            db.log_audit(user["user_id"], user["username"], "create", "article", new_id,
                          f"Создана статья: {title[:80]}", ip)
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/article/{article_id}")
async def api_delete_article(article_id: int, request: Request):
    try:
        db.update_article(article_id, {"status": "archived", "updated_at": datetime.now().isoformat(timespec="seconds")})
        # Remove from Meilisearch
        try:
            meili.delete_article(article_id)
        except Exception:
            pass
        # Audit log
        user = getattr(request.state, "current_user", None)
        if user:
            ip = request.client.host if request.client else ""
            db.log_audit(user["user_id"], user["username"], "delete", "article", article_id,
                          "Статья архивирована", ip)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/article/{article_id}/duplicate")
async def api_duplicate_article(article_id: int):
    try:
        new_id = db.duplicate_article(article_id)
        if new_id is None:
            return JSONResponse({"ok": False, "error": "Статья не найдена"}, status_code=404)
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/article/{article_id}/breaking")
async def api_toggle_breaking(article_id: int, request: Request):
    """Toggle the is_breaking flag on an article. Sends push notification when marked breaking."""
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") not in ("admin", "editor"):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    body = await request.json()
    is_breaking = bool(body.get("is_breaking", False))
    try:
        db.update_article(article_id, {
            "is_breaking": 1 if is_breaking else 0,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        })
        # Send web push notification when marked as breaking
        if is_breaking:
            article = db.get_article(article_id)
            if article:
                asyncio.create_task(_send_breaking_push(article))
        # Audit log
        ip = request.client.host if request.client else ""
        db.log_audit(
            user["user_id"], user["username"], "breaking",
            "article", article_id,
            f"Срочная новость: {'вкл' if is_breaking else 'выкл'}", ip
        )
        return {"ok": True, "is_breaking": is_breaking}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def _send_breaking_push(article: dict):
    """Send Web Push notifications for breaking news to all subscribers."""
    import sqlite3 as _sqlite3
    try:
        db_path = str(Path(__file__).parent.parent / "data" / "total.db")
        conn = _sqlite3.connect(db_path)
        conn.row_factory = _sqlite3.Row
        subs = conn.execute("SELECT endpoint, p256dh, auth FROM push_subscriptions").fetchall()
        conn.close()
    except Exception:
        subs = []

    if not subs:
        return

    vapid_private = os.environ.get("VAPID_PRIVATE_KEY", "")
    vapid_email = os.environ.get("VAPID_EMAIL", "mailto:admin@total.kz")
    if not vapid_private:
        logger.warning("VAPID_PRIVATE_KEY not set — skipping breaking news push")
        return

    title = article.get("title", "")
    excerpt = (article.get("excerpt") or "")[:200]
    article_url = article.get("url", "")
    payload = json.dumps({
        "title": f"СРОЧНО: {title}",
        "body": excerpt,
        "url": article_url,
        "icon": "/static/images/icon-192.png",
    }, ensure_ascii=False)

    try:
        from pywebpush import webpush, WebPushException
        for sub in subs:
            try:
                webpush(
                    subscription_info={
                        "endpoint": sub["endpoint"],
                        "keys": {"p256dh": sub["p256dh"], "auth": sub["auth"]},
                    },
                    data=payload,
                    vapid_private_key=vapid_private,
                    vapid_claims={"sub": vapid_email},
                    timeout=10,
                )
            except WebPushException:
                pass
            except Exception:
                pass
    except ImportError:
        logger.warning("pywebpush not installed — skipping breaking news push")


@app.get("/api/article/{article_id}/revisions")
async def api_article_revisions(article_id: int):
    try:
        revisions = db.get_revisions(article_id, limit=20)
        return {"ok": True, "revisions": revisions}
    except Exception as e:
        return {"ok": False, "error": str(e), "revisions": []}


@app.get("/api/admin/auto-post")
async def api_autopost_status(request: Request):
    """Get auto-posting status and config."""
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    from .autopost import is_autopost_enabled
    from . import social
    config = social.get_autopost_config()
    return {
        "ok": True,
        "enabled": is_autopost_enabled(),
        "telegram_configured": "telegram" in config,
        "config": {k: {"account_name": v["account_name"], "account_id": v["account_id_str"]} for k, v in config.items()},
    }


@app.post("/api/admin/auto-post")
async def api_autopost_toggle(request: Request):
    """Enable/disable auto-posting."""
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    body = await request.json()
    enabled = body.get("enabled", True)
    from .autopost import set_autopost_enabled
    set_autopost_enabled(bool(enabled))
    return {"ok": True, "enabled": bool(enabled)}


@app.post("/api/admin/auto-post/trigger")
async def api_autopost_trigger(request: Request):
    """Manually trigger one auto-posting cycle."""
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    import asyncio
    from .autopost import _do_autopost_cycle
    count = await asyncio.to_thread(_do_autopost_cycle)
    return {"ok": True, "posted": count}


@app.post("/api/admin/article/{article_id}/restore/{revision_id}")
async def api_restore_revision(request: Request, article_id: int, revision_id: int):
    user = getattr(request.state, "current_user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "Не авторизован"}, status_code=401)
    try:
        ok = db.restore_revision(article_id, revision_id)
        if not ok:
            return JSONResponse({"ok": False, "error": "Ревизия не найдена или без полного состояния"}, status_code=404)
        # Record a restore revision
        db.record_revision(article_id, {"restored_from": revision_id}, revision_type="restore", changed_by=user.get("username", ""))
        # Re-index in Meilisearch
        try:
            article_data = db.get_article(article_id)
            if article_data:
                meili.index_article(article_data)
        except Exception:
            pass
        ip = request.client.host if request.client else ""
        db.log_audit(user["user_id"], user["username"], "restore", "article", article_id,
                      f"Восстановлено из ревизии #{revision_id}", ip)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/upload")
async def api_upload():
    return JSONResponse(
        {"ok": False, "error": "Загрузка файлов будет доступна позже"},
        status_code=501,
    )

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


# ══════════════════════════════════════════════
#  NLP DATA API
# ══════════════════════════════════════════════

@app.get("/api/article/{article_id}/nlp")
async def api_article_nlp(article_id: int):
    """Return NLP extraction data for a single article."""
    row = db.execute_raw(
        "SELECT * FROM article_nlp WHERE article_id = %s", (article_id,)
    )
    if not row:
        return JSONResponse({"error": "NLP data not found"}, status_code=404)
    return row


@app.get("/api/analytics/topics")
async def api_analytics_topics(limit: int = 50):
    """Top topics by article count (from NLP extraction)."""
    rows = db.execute_raw_many("""
        SELECT topic, COUNT(*) as article_count
        FROM article_nlp, jsonb_array_elements_text(topics) AS topic
        GROUP BY topic
        ORDER BY article_count DESC
        LIMIT %s
    """, (limit,))
    return {"topics": rows, "total": len(rows)}


@app.get("/api/analytics/sentiment")
async def api_analytics_sentiment(days: int = 30):
    """Sentiment distribution over time (daily aggregation)."""
    rows = db.execute_raw_many("""
        SELECT
            DATE(a.pub_date) as date,
            n.sentiment,
            COUNT(*) as count,
            ROUND(AVG(n.sentiment_score)::numeric, 2) as avg_score
        FROM article_nlp n
        JOIN articles a ON a.id = n.article_id
        WHERE a.pub_date >= NOW() - INTERVAL '%s days'
        GROUP BY DATE(a.pub_date), n.sentiment
        ORDER BY date DESC, n.sentiment
    """, (days,))
    return {"sentiment_over_time": rows, "days": days}


# ══════════════════════════════════════════════
#  ADMIN AI ASSISTANT (GPT-4o-mini)
# ══════════════════════════════════════════════

_OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")

@app.post("/api/admin/ai-generate")
async def api_admin_ai_generate(request: Request):
    """AI-generate excerpt, meta_description, tags, or title suggestions."""
    user = getattr(request.state, "current_user", None)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    if not _OPENAI_KEY:
        return JSONResponse({"error": "OPENAI_API_KEY not configured"}, status_code=500)

    body = await request.json()
    title = (body.get("title") or "").strip()
    text = (body.get("body") or "")[:4000].strip()
    fields = body.get("fields", [])

    if not title and not text:
        return JSONResponse({"error": "Нужен заголовок или текст"}, status_code=400)

    results = {}
    import httpx as _httpx

    for field in fields:
        if field == "excerpt":
            prompt = f"Напиши краткий лид (до 200 символов) для новостной статьи.\nЗаголовок: {title}\nТекст: {text}\n\nОтвет — только текст лида, без кавычек."
        elif field == "meta_description":
            prompt = f"Напиши SEO meta description (120-155 символов) для новостной статьи.\nЗаголовок: {title}\nТекст: {text}\n\nОтвет — только текст описания."
        elif field == "tags":
            prompt = f"Предложи 5-7 тегов для новостной статьи (через запятую, без #).\nЗаголовок: {title}\nТекст: {text}\n\nОтвет — только теги через запятую."
        elif field == "titles":
            prompt = f"Предложи 3 альтернативных заголовка для новостной статьи.\nТекущий заголовок: {title}\nТекст: {text}\n\nОтвет — 3 заголовка, каждый на новой строке, без нумерации."
        elif field == "rewrite":
            prompt = f"Перепиши текст статьи, улучшив стиль и читаемость. Сохрани все факты, даты, имена и цифры без изменений. Текст должен быть на русском языке, в стиле качественного новостного портала.\n\nЗаголовок: {title}\nТекст для улучшения:\n{text}\n\nОтвет — только улучшенный текст, без пояснений."
        else:
            continue

        try:
            async with _httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {_OPENAI_KEY}"},
                    json={
                        "model": "gpt-4o-mini",
                        "messages": [
                            {"role": "system", "content": "Ты — AI-ассистент для казахстанского новостного портала Total.kz. Пиши на русском языке."},
                            {"role": "user", "content": prompt},
                        ],
                        "max_tokens": 1500 if field == "rewrite" else 300,
                        "temperature": 0.7,
                    },
                )
                data = resp.json()
                content = data["choices"][0]["message"]["content"].strip()

                if field == "tags":
                    results[field] = [t.strip() for t in content.split(",") if t.strip()]
                elif field == "titles":
                    results[field] = [t.strip().lstrip("0123456789.-) ") for t in content.split("\n") if t.strip()][:3]
                else:
                    results[field] = content
        except Exception as e:
            logger.exception("AI generate error for field %s", field)
            results[field] = f"Ошибка: {str(e)[:100]}"

    return JSONResponse({"ok": True, **results})


@app.post("/api/admin/ai-from-url")
async def api_admin_ai_from_url(request: Request):
    """Fetch URL content and extract article data using AI."""
    user = getattr(request.state, "current_user", None)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    if not _OPENAI_KEY:
        return JSONResponse({"error": "OPENAI_API_KEY not configured"}, status_code=500)

    body = await request.json()
    url = (body.get("url") or "").strip()
    if not url:
        return JSONResponse({"error": "URL обязателен"}, status_code=400)

    import httpx as _httpx
    try:
        async with _httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; TotalKZ-Bot/1.0)"})
            resp.raise_for_status()
            html = resp.text[:15000]
    except Exception as e:
        return JSONResponse({"error": f"Не удалось загрузить URL: {str(e)[:100]}"}, status_code=400)

    # Strip tags for a cleaner text extraction
    import re
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()[:5000]

    prompt = (
        "Из текста веб-страницы извлеки данные для новостной статьи. "
        "Ответь строго в формате JSON (без markdown-блока):\n"
        '{"title": "заголовок", "excerpt": "краткий лид до 200 символов", '
        '"body": "основной текст статьи, переписанный качественным журналистским стилем", '
        '"tags": ["тег1", "тег2", ...]}\n\n'
        f"Текст страницы:\n{text}"
    )

    try:
        async with _httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {_OPENAI_KEY}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": "Ты — AI-ассистент для казахстанского новостного портала Total.kz. Пиши на русском языке. Отвечай только валидным JSON."},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 2000,
                    "temperature": 0.5,
                },
            )
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            # Parse JSON from response
            import json as _json
            if content.startswith("```"):
                content = re.sub(r'^```\w*\n?', '', content)
                content = re.sub(r'\n?```$', '', content)
            result = _json.loads(content)
            return JSONResponse({"ok": True, **result})
    except Exception as e:
        logger.exception("AI from URL error")
        return JSONResponse({"error": f"Ошибка AI: {str(e)[:100]}"}, status_code=500)


# ══════════════════════════════════════════════
#  AI INSIGHTS FOR ANALYTICS
# ══════════════════════════════════════════════

_ai_insights_cache: dict = {}  # {"data": ..., "ts": float}


def _get_content_stats() -> dict:
    """Query PostgreSQL for content statistics used by AI insights."""
    if not settings.use_postgres:
        return {}
    try:
        from app.pg_queries import get_pg_session
        from app.models import Article, ArticleEntity, NerEntity
        from sqlalchemy import func, select, desc, cast
        from sqlalchemy import DateTime as SA_DateTime

        with get_pg_session() as sess:
            # Total articles
            total = sess.scalar(select(func.count(Article.id))) or 0

            # Articles published in last 7 days, by category
            seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
            thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).isoformat()

            recent_by_cat = sess.execute(
                select(
                    Article.sub_category,
                    func.count(Article.id).label("cnt"),
                ).where(Article.pub_date >= seven_days_ago)
                .group_by(Article.sub_category)
                .order_by(func.count(Article.id).desc())
            ).all()

            # Top 5 authors by article count (last 30 days)
            top_authors = sess.execute(
                select(
                    Article.author,
                    func.count(Article.id).label("cnt"),
                ).where(Article.pub_date >= thirty_days_ago)
                .where(Article.author.isnot(None))
                .group_by(Article.author)
                .order_by(func.count(Article.id).desc())
                .limit(5)
            ).all()

            # Top 10 trending entities (by sum of mention_count in last 7 days)
            trending_entities = sess.execute(
                select(
                    NerEntity.name,
                    NerEntity.entity_type,
                    func.sum(ArticleEntity.mention_count).label("mentions"),
                ).join(ArticleEntity, NerEntity.id == ArticleEntity.entity_id)
                .join(Article, Article.id == ArticleEntity.article_id)
                .where(Article.pub_date >= seven_days_ago)
                .group_by(NerEntity.name, NerEntity.entity_type)
                .order_by(func.sum(ArticleEntity.mention_count).desc())
                .limit(10)
            ).all()

            # Average articles per day (last 30 days)
            articles_30d = sess.scalar(
                select(func.count(Article.id)).where(Article.pub_date >= thirty_days_ago)
            ) or 0
            avg_per_day = round(articles_30d / 30, 1)

            # Total articles last 7 days
            articles_7d = sum(r.cnt for r in recent_by_cat)

            return {
                "total_articles": total,
                "articles_7d": articles_7d,
                "articles_30d": articles_30d,
                "avg_per_day": avg_per_day,
                "categories_7d": [{"category": r.sub_category or "без категории", "count": r.cnt} for r in recent_by_cat],
                "top_authors_30d": [{"author": r.author, "count": r.cnt} for r in top_authors],
                "trending_entities": [{"name": r.name, "type": r.entity_type, "mentions": r.mentions} for r in trending_entities],
            }
    except Exception as e:
        logger.exception("Error getting content stats for AI insights")
        return {"error": str(e)[:200]}


async def _get_umami_data() -> dict:
    """Fetch traffic data from Umami API."""
    base = settings.umami_api_url
    wid = settings.umami_website_id
    if not base or not wid:
        return {"error": "Umami not configured"}

    import httpx as _httpx
    now_ms = int(time.time() * 1000)
    seven_days_ms = now_ms - 7 * 24 * 60 * 60 * 1000

    try:
        async with _httpx.AsyncClient(timeout=15) as client:
            # Authenticate
            auth_resp = await client.post(f"{base}/api/auth/login", json={
                "username": settings.umami_username,
                "password": settings.umami_password,
            })
            token = auth_resp.json().get("token")
            if not token:
                return {"error": "Umami auth failed"}

            headers = {"Authorization": f"Bearer {token}"}
            params = {"startAt": seven_days_ms, "endAt": now_ms}

            # Fetch stats, active visitors, and top metrics in parallel
            stats_resp, active_resp, pages_resp, countries_resp, devices_resp, referrers_resp = await asyncio.gather(
                client.get(f"{base}/api/websites/{wid}/stats", headers=headers, params=params),
                client.get(f"{base}/api/websites/{wid}/active", headers=headers),
                client.get(f"{base}/api/websites/{wid}/metrics", headers=headers, params={**params, "type": "title", "limit": 10}),
                client.get(f"{base}/api/websites/{wid}/metrics", headers=headers, params={**params, "type": "country", "limit": 10}),
                client.get(f"{base}/api/websites/{wid}/metrics", headers=headers, params={**params, "type": "device", "limit": 5}),
                client.get(f"{base}/api/websites/{wid}/metrics", headers=headers, params={**params, "type": "referrer", "limit": 10}),
            )

            stats = stats_resp.json()
            active = active_resp.json()
            pages = pages_resp.json()
            countries = countries_resp.json()
            devices = devices_resp.json()
            referrers = referrers_resp.json()

            # Calculate derived metrics (Umami v3 returns flat format)
            pageviews = stats.get("pageviews", 0)
            if isinstance(pageviews, dict):
                pageviews = pageviews.get("value", 0)
            visitors = stats.get("visitors", 0)
            if isinstance(visitors, dict):
                visitors = visitors.get("value", 0)
            bounces = stats.get("bounces", 0)
            if isinstance(bounces, dict):
                bounces = bounces.get("value", 0)
            visits = stats.get("visits", 0)
            if isinstance(visits, dict):
                visits = visits.get("value", 0)
            totaltime = stats.get("totaltime", 0)
            if isinstance(totaltime, dict):
                totaltime = totaltime.get("value", 0)

            bounce_rate = f"{round(bounces / visits * 100)}%" if visits else "0%"
            avg_duration_sec = round(totaltime / visits) if visits else 0
            avg_duration = f"{avg_duration_sec // 60}m {avg_duration_sec % 60:02d}s"

            # Device shares
            total_device = sum(d.get("y", 0) for d in devices)
            mobile_count = sum(d.get("y", 0) for d in devices if d.get("x", "").lower() == "mobile")
            mobile_share = f"{round(mobile_count / total_device * 100)}%" if total_device else "0%"

            top_country = countries[0]["x"] if countries else "N/A"
            # pages may return error dict for unsupported metric types
            if isinstance(pages, dict):
                pages = []
            top_page = pages[0]["x"] if pages else "/"

            return {
                "visitors_7d": visitors,
                "pageviews_7d": pageviews,
                "bounce_rate": bounce_rate,
                "avg_duration": avg_duration,
                "active_now": active.get("visitors", 0),
                "top_page": top_page,
                "top_country": top_country,
                "mobile_share": mobile_share,
                "top_pages": [{"path": p["x"], "views": p["y"]} for p in (pages if isinstance(pages, list) else [])],

                "countries": [{"country": c["x"], "views": c["y"]} for c in (countries if isinstance(countries, list) else [])],
                "devices": [{"device": d["x"], "views": d["y"]} for d in (devices if isinstance(devices, list) else [])],
                "referrers": [{"source": r["x"], "views": r["y"]} for r in (referrers if isinstance(referrers, list) else [])],
            }
    except Exception as e:
        logger.exception("Error fetching Umami data for AI insights")
        return {"error": str(e)[:200]}


@app.get("/api/admin/ai-insights")
async def api_admin_ai_insights(request: Request, refresh: bool = False):
    """AI-powered analytics insights combining Umami, GSC, and internal data."""
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    if not _OPENAI_KEY:
        return JSONResponse({"error": "OPENAI_API_KEY not configured"}, status_code=500)

    # Check cache (1 hour)
    cache = _ai_insights_cache
    if not refresh and cache.get("data") and (time.time() - cache.get("ts", 0)) < 3600:
        return JSONResponse(cache["data"])

    # Collect data from all sources in parallel
    umami_task = _get_umami_data()
    gsc_data = search.get_search_data()
    content_stats = _get_content_stats()
    umami_data = await umami_task

    # Build stats summary for response
    stats = {}
    if not umami_data.get("error"):
        stats = {
            "visitors_7d": umami_data.get("visitors_7d", 0),
            "pageviews_7d": umami_data.get("pageviews_7d", 0),
            "bounce_rate": umami_data.get("bounce_rate", "N/A"),
            "avg_duration": umami_data.get("avg_duration", "N/A"),
            "active_now": umami_data.get("active_now", 0),
            "top_page": umami_data.get("top_page", "N/A"),
            "top_country": umami_data.get("top_country", "N/A"),
            "mobile_share": umami_data.get("mobile_share", "N/A"),
        }

    # Build prompt for GPT
    prompt_parts = [
        "Ты — AI-аналитик для казахстанского новостного портала Total.kz.",
        "Проанализируй данные и дай 6-8 конкретных, actionable инсайтов.",
        "Каждый инсайт должен быть в одной из категорий: content, seo, audience, growth.",
        "",
        "Ответь строго в формате JSON массива (без markdown):",
        '[{"category": "content|seo|audience|growth", "icon": "эмодзи", "title": "Краткий заголовок", "description": "Подробное описание на 2-3 предложения", "action": "Конкретное действие", "priority": "high|medium|low"}]',
        "",
        "═══ ДАННЫЕ ═══",
    ]

    if not umami_data.get("error"):
        prompt_parts.append(f"\n## Трафик (последние 7 дней):")
        prompt_parts.append(f"Визиты: {umami_data.get('visitors_7d', 'N/A')}, Просмотры: {umami_data.get('pageviews_7d', 'N/A')}")
        prompt_parts.append(f"Показатель отказов: {umami_data.get('bounce_rate', 'N/A')}, Среднее время: {umami_data.get('avg_duration', 'N/A')}")
        prompt_parts.append(f"Сейчас онлайн: {umami_data.get('active_now', 0)}")
        prompt_parts.append(f"Мобильные: {umami_data.get('mobile_share', 'N/A')}")
        if umami_data.get("top_pages"):
            prompt_parts.append(f"Топ страницы: {json.dumps(umami_data['top_pages'][:5], ensure_ascii=False)}")
        if umami_data.get("countries"):
            prompt_parts.append(f"Страны: {json.dumps(umami_data['countries'][:5], ensure_ascii=False)}")
        if umami_data.get("referrers"):
            prompt_parts.append(f"Реферреры: {json.dumps(umami_data['referrers'][:5], ensure_ascii=False)}")
    else:
        prompt_parts.append(f"\n## Трафик: данные недоступны ({umami_data.get('error', '')})")

    if gsc_data and gsc_data.get("totals"):
        t = gsc_data["totals"]
        prompt_parts.append(f"\n## Google Search Console:")
        prompt_parts.append(f"Клики: {t.get('clicks', 0)}, Показы: {t.get('impressions', 0)}, CTR: {t.get('avg_ctr', 0)}%, Позиция: {t.get('avg_position', 0)}")
        if gsc_data.get("top_queries"):
            top_q = gsc_data["top_queries"][:10]
            prompt_parts.append(f"Топ запросы: {json.dumps([{'query': q['query'], 'clicks': q['clicks'], 'position': q['position']} for q in top_q], ensure_ascii=False)}")
        if gsc_data.get("growth_opportunities"):
            prompt_parts.append(f"Возможности роста: {json.dumps(gsc_data['growth_opportunities'][:5], ensure_ascii=False)}")
    else:
        prompt_parts.append("\n## GSC: данные недоступны")

    if content_stats and not content_stats.get("error"):
        prompt_parts.append(f"\n## Контент:")
        prompt_parts.append(f"Всего статей: {content_stats.get('total_articles', 0)}, За 7 дней: {content_stats.get('articles_7d', 0)}, В среднем/день: {content_stats.get('avg_per_day', 0)}")
        if content_stats.get("categories_7d"):
            prompt_parts.append(f"Категории за 7д: {json.dumps(content_stats['categories_7d'][:10], ensure_ascii=False)}")
        if content_stats.get("top_authors_30d"):
            prompt_parts.append(f"Топ авторы (30д): {json.dumps(content_stats['top_authors_30d'], ensure_ascii=False)}")
        if content_stats.get("trending_entities"):
            prompt_parts.append(f"Трендовые сущности: {json.dumps(content_stats['trending_entities'][:7], ensure_ascii=False)}")

    full_prompt = "\n".join(prompt_parts)

    # Call GPT-4o-mini
    import httpx as _httpx
    try:
        async with _httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {_OPENAI_KEY}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": "Ты — AI-аналитик для новостного портала. Отвечай строго валидным JSON-массивом инсайтов. Никакого markdown, только JSON."},
                        {"role": "user", "content": full_prompt},
                    ],
                    "max_tokens": 2000,
                    "temperature": 0.7,
                },
            )
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()

            # Parse JSON — strip markdown fences if present
            if content.startswith("```"):
                content = re.sub(r'^```\w*\n?', '', content)
                content = re.sub(r'\n?```$', '', content)
            insights = json.loads(content)

            # Ensure proper structure
            icon_map = {"content": "📝", "seo": "🔍", "audience": "👥", "growth": "📈"}
            for ins in insights:
                if "icon" not in ins or not ins["icon"]:
                    ins["icon"] = icon_map.get(ins.get("category", ""), "💡")
                if "priority" not in ins:
                    ins["priority"] = "medium"

            result = {
                "ok": True,
                "generated_at": datetime.utcnow().isoformat(),
                "stats": stats,
                "insights": insights,
            }

            # Cache the result
            _ai_insights_cache["data"] = result
            _ai_insights_cache["ts"] = time.time()

            return JSONResponse(result)

    except Exception as e:
        logger.exception("AI insights generation error")
        return JSONResponse({"ok": False, "error": f"Ошибка генерации: {str(e)[:200]}"}, status_code=500)


@app.get("/api/admin/media/search")
async def api_media_search(request: Request, q: str = "", page: int = 1, per_page: int = 30):
    """Search media library for the image picker."""
    user = getattr(request.state, "current_user", None)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    result = db.get_all_media(q=q, page=page, per_page=per_page)
    return JSONResponse({"ok": True, **result})


# ══════════════════════════════════════════════
#  MEILISEARCH SEARCH API
# ══════════════════════════════════════════════

@app.get("/api/search/articles")
async def api_search_articles(q: str = "", category: str = "", page: int = 1):
    """Full-text search via Meilisearch with graceful fallback."""
    if not q:
        return {"hits": [], "total": 0, "query": ""}
    filters = f'sub_category = "{category}"' if category else ""
    try:
        return meili.search(q, filters=filters, page=page)
    except Exception:
        # Fallback to SQLite LIKE search
        result = db.search_articles(query=q, category=category, page=page)
        return {"hits": result["articles"], "total": result["total"], "query": q}


# ══════════════════════════════════════════════
#  IMGPROXY PROXY
# ══════════════════════════════════════════════

@app.get("/imgproxy/{path:path}")
async def imgproxy_proxy(path: str):
    """Forward requests to imgproxy container."""
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://imgproxy:8080/{path}", timeout=10)
            return Response(
                content=r.content,
                media_type=r.headers.get("content-type", "image/webp"),
                headers={"Cache-Control": "public, max-age=2592000"},
            )
    except Exception:
        return Response(status_code=502)


# ── Data Audit endpoint ─────────────────────

@app.get("/api/audit")
async def api_audit():
    """Full data quality audit."""
    if settings.use_postgres:
        return db.get_full_audit()
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


# ══════════════════════════════════════════════
#  SENTIMENT DASHBOARD  /admin/sentiment
# ══════════════════════════════════════════════

@app.get("/admin/sentiment", response_class=HTMLResponse)
async def admin_sentiment_page(request: Request):
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
    return templates.TemplateResponse("sentiment.html", _ctx(request))


@app.get("/api/admin/sentiment-data")
async def api_admin_sentiment_data(request: Request):
    """Return sentiment analytics data for the dashboard."""
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)

    try:
        result = await asyncio.to_thread(_query_sentiment_data)
        return JSONResponse(result)
    except Exception as e:
        logger.exception("Sentiment data error")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/admin/sentiment-govt")
async def api_admin_sentiment_govt(request: Request, filter: str = "all"):
    """Return government-mentions sentiment trend data."""
    user = getattr(request.state, "current_user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)

    try:
        result = await asyncio.to_thread(_query_govt_sentiment, filter)
        return JSONResponse(result)
    except Exception as e:
        logger.exception("Govt sentiment data error")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


def _query_sentiment_data() -> dict:
    """Query article_nlp for sentiment dashboard data (SQLite)."""
    with db.get_db() as conn:
        # Check if article_nlp table exists
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='article_nlp'"
        ).fetchone()
        if not table_check:
            return {"total": 0, "positive_count": 0, "neutral_count": 0, "negative_count": 0,
                    "pie": {"positive": 0, "neutral": 0, "negative": 0},
                    "trend": {"dates": [], "positive": [], "neutral": [], "negative": []},
                    "by_category": {"categories": [], "positive": [], "neutral": [], "negative": []},
                    "most_positive": [], "most_negative": [], "govt_trend": {"dates": [], "scores": [], "counts": []}}

        # Overall counts (last 30 days)
        counts = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN n.sentiment = 'positive' THEN 1 ELSE 0 END) as pos,
                SUM(CASE WHEN n.sentiment = 'neutral' THEN 1 ELSE 0 END) as neu,
                SUM(CASE WHEN n.sentiment = 'negative' THEN 1 ELSE 0 END) as neg
            FROM article_nlp n
            JOIN articles a ON a.id = n.article_id
            WHERE a.pub_date >= date('now', '-30 days')
              AND n.sentiment IS NOT NULL
        """).fetchone()

        total = counts[0] or 0
        pos = counts[1] or 0
        neu = counts[2] or 0
        neg = counts[3] or 0

        # Pie chart data
        pie = {"positive": pos, "neutral": neu, "negative": neg}

        # Trend: daily counts by sentiment (last 30 days)
        trend_rows = conn.execute("""
            SELECT date(a.pub_date) as day,
                SUM(CASE WHEN n.sentiment = 'positive' THEN 1 ELSE 0 END) as pos,
                SUM(CASE WHEN n.sentiment = 'neutral' THEN 1 ELSE 0 END) as neu,
                SUM(CASE WHEN n.sentiment = 'negative' THEN 1 ELSE 0 END) as neg
            FROM article_nlp n
            JOIN articles a ON a.id = n.article_id
            WHERE a.pub_date >= date('now', '-30 days')
              AND n.sentiment IS NOT NULL
            GROUP BY day
            ORDER BY day
        """).fetchall()

        trend = {
            "dates": [r[0] for r in trend_rows],
            "positive": [r[1] for r in trend_rows],
            "neutral": [r[2] for r in trend_rows],
            "negative": [r[3] for r in trend_rows],
        }

        # Sentiment by category
        cat_rows = conn.execute("""
            SELECT a.sub_category,
                SUM(CASE WHEN n.sentiment = 'positive' THEN 1 ELSE 0 END) as pos,
                SUM(CASE WHEN n.sentiment = 'neutral' THEN 1 ELSE 0 END) as neu,
                SUM(CASE WHEN n.sentiment = 'negative' THEN 1 ELSE 0 END) as neg
            FROM article_nlp n
            JOIN articles a ON a.id = n.article_id
            WHERE a.pub_date >= date('now', '-30 days')
              AND n.sentiment IS NOT NULL
              AND a.sub_category IS NOT NULL AND a.sub_category != ''
            GROUP BY a.sub_category
            ORDER BY (pos + neu + neg) DESC
            LIMIT 15
        """).fetchall()

        by_category = {
            "categories": [r[0] for r in cat_rows],
            "positive": [r[1] for r in cat_rows],
            "neutral": [r[2] for r in cat_rows],
            "negative": [r[3] for r in cat_rows],
        }

        # Most positive articles (top 10)
        most_pos = conn.execute("""
            SELECT a.id, a.title, n.sentiment, n.sentiment_score, a.sub_category
            FROM article_nlp n
            JOIN articles a ON a.id = n.article_id
            WHERE n.sentiment_score IS NOT NULL
              AND a.pub_date >= date('now', '-30 days')
            ORDER BY n.sentiment_score DESC
            LIMIT 10
        """).fetchall()

        most_positive = [
            {"id": r[0], "title": r[1], "sentiment": r[2], "sentiment_score": r[3], "category": r[4]}
            for r in most_pos
        ]

        # Most negative articles (top 10)
        most_neg = conn.execute("""
            SELECT a.id, a.title, n.sentiment, n.sentiment_score, a.sub_category
            FROM article_nlp n
            JOIN articles a ON a.id = n.article_id
            WHERE n.sentiment_score IS NOT NULL
              AND a.pub_date >= date('now', '-30 days')
            ORDER BY n.sentiment_score ASC
            LIMIT 10
        """).fetchall()

        most_negative = [
            {"id": r[0], "title": r[1], "sentiment": r[2], "sentiment_score": r[3], "category": r[4]}
            for r in most_neg
        ]

        # Government mentions trend
        govt_trend = _query_govt_sentiment("all", conn)

        return {
            "total": total,
            "positive_count": pos,
            "neutral_count": neu,
            "negative_count": neg,
            "pie": pie,
            "trend": trend,
            "by_category": by_category,
            "most_positive": most_positive,
            "most_negative": most_negative,
            "govt_trend": govt_trend,
        }


def _query_govt_sentiment(filter_type: str = "all", conn=None) -> dict:
    """Query sentiment for government-related articles."""
    close_conn = False
    if conn is None:
        conn = db.get_db()
        close_conn = True

    try:
        # Keywords for government entity filtering
        govt_keywords = {
            "all": "%",
            "president": "%президент%",
            "government": "%правительств%",
            "parliament": "%парламент%",
        }
        pattern = govt_keywords.get(filter_type, "%")

        if pattern == "%":
            # All government mentions — search for common government terms
            rows = conn.execute("""
                SELECT date(a.pub_date) as day,
                    AVG(n.sentiment_score) as avg_score,
                    COUNT(*) as cnt
                FROM article_nlp n
                JOIN articles a ON a.id = n.article_id
                WHERE a.pub_date >= date('now', '-30 days')
                  AND n.sentiment_score IS NOT NULL
                  AND (a.title LIKE '%правительств%'
                       OR a.title LIKE '%президент%'
                       OR a.title LIKE '%парламент%'
                       OR a.title LIKE '%министр%'
                       OR a.title LIKE '%Токаев%'
                       OR a.title LIKE '%Мажилис%'
                       OR a.title LIKE '%Сенат%')
                GROUP BY day
                ORDER BY day
            """).fetchall()
        else:
            rows = conn.execute("""
                SELECT date(a.pub_date) as day,
                    AVG(n.sentiment_score) as avg_score,
                    COUNT(*) as cnt
                FROM article_nlp n
                JOIN articles a ON a.id = n.article_id
                WHERE a.pub_date >= date('now', '-30 days')
                  AND n.sentiment_score IS NOT NULL
                  AND a.title LIKE ?
                GROUP BY day
                ORDER BY day
            """, (pattern,)).fetchall()

        return {
            "dates": [r[0] for r in rows],
            "scores": [round(r[1], 3) if r[1] else 0 for r in rows],
            "counts": [r[2] for r in rows],
        }
    finally:
        if close_conn:
            conn.close()
