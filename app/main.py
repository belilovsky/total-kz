"""FastAPI application – Total.kz v11.0 (public frontend + CMS admin)."""

import json
import logging
import os
import re
import unicodedata
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from urllib.parse import quote as _url_quote, unquote as _url_unquote
from fastapi import FastAPI, Request, Query, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.gzip import GZipMiddleware

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
from .public_routes import router as public_router
from .social_routes import router as social_router

logger = logging.getLogger(__name__)

MEDIA_DIR = Path(__file__).parent.parent / "data" / "media"
ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}
MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10MB


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
    yield


app = FastAPI(title="Total.kz", version="11.0.0", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CacheControlMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(AuthMiddleware)

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

# Currency rates (live from NB RK, cached 1h)
from app.currency import get_rates as _get_currency_rates
templates.env.globals["get_currency_rates"] = _get_currency_rates


def _ctx(request: Request, **kwargs) -> dict:
    """Build template context with current_user always available."""
    ctx = {"request": request, "current_user": getattr(request.state, "current_user", None)}
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
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/admin/articles", status_code=302)
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

    return templates.TemplateResponse("dashboard.html", _ctx(request,
        stats=stats,
        persons=persons,
        orgs=orgs,
        locations=locations,
        chart_months=chart_months,
        chart_counts=chart_counts,
        chart_cats=chart_cats,
        chart_cat_counts=chart_cat_counts,
        chart_cat_slugs=chart_cat_slugs,
        chart_years=chart_years,
        chart_year_counts=chart_year_counts,
        heatmap_data=heatmap_data,
        cat_labels_json=cat_labels_json,
        cat_label=cat_label,
        entity_type_label=entity_type_label,
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
    stats = db.get_stats()
    authors = db.get_authors()
    tags = db.get_tags(limit=60)

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
            user_id=user.get("user_id") if user else None
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
        status_counts=status_counts,
        categories=stats["categories"],
        authors=authors,
        tags=tags,
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
    ))


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
    categories = db.get_all_categories()
    return templates.TemplateResponse("categories.html", _ctx(request,
        categories=categories, format_num=_format_num))


@app.get("/admin/authors", response_class=HTMLResponse)
async def admin_authors_page(request: Request, q: str = ""):
    authors = db.get_all_authors_managed(q=q)
    return templates.TemplateResponse("authors_managed.html", _ctx(request,
        authors=authors, q=q, format_num=_format_num))


@app.get("/admin/media", response_class=HTMLResponse)
async def admin_media_page(request: Request, q: str = "", page: int = Query(1, ge=1)):
    result = db.get_all_media(q=q, page=page)
    return templates.TemplateResponse("media.html", _ctx(request,
        result=result, q=q, format_num=_format_num))


@app.get("/admin/tags", response_class=HTMLResponse)
async def admin_tags_page(request: Request, q: str = "", page: int = Query(1, ge=1)):
    result = db.get_tags_full(q=q, page=page)
    return templates.TemplateResponse("tags.html", _ctx(request,
        result=result, q=q, format_num=_format_num))


@app.get("/admin/entities", response_class=HTMLResponse)
async def admin_entities_page(request: Request, q: str = "", entity_type: str = "", page: int = Query(1, ge=1)):
    result = db.get_entities_full(q=q, entity_type=entity_type, page=page)
    return templates.TemplateResponse("entities_manage.html", _ctx(request,
        result=result, q=q, entity_type=entity_type, format_num=_format_num))


@app.get("/admin/stories", response_class=HTMLResponse)
async def admin_stories_page(request: Request, q: str = "", page: int = Query(1, ge=1)):
    result = db.get_all_stories(q=q, page=page)
    return templates.TemplateResponse("stories.html", _ctx(request,
        result=result, q=q, format_num=_format_num))


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

@app.post("/api/media/upload")
async def api_media_upload(request: Request, file: UploadFile = File(...)):
    ext = Path(file.filename or "").suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return JSONResponse({"ok": False, "error": "Неподдерживаемый формат файла"}, status_code=400)
    content = await file.read()
    if len(content) > MAX_UPLOAD_SIZE:
        return JSONResponse({"ok": False, "error": "Файл слишком большой (макс. 10 МБ)"}, status_code=400)
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


@app.get("/media/{filename}")
async def serve_media(filename: str):
    """Serve uploaded media files."""
    filepath = MEDIA_DIR / filename
    if not filepath.exists() or not filepath.is_file():
        return Response(status_code=404)
    ext = filepath.suffix.lower()
    mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
                ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml"}
    mime = mime_map.get(ext, "application/octet-stream")
    return Response(content=filepath.read_bytes(), media_type=mime,
                    headers={"Cache-Control": "public, max-age=2592000"})


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
               "body_blocks", "scheduled_at", "focal_x", "focal_y", "assigned_to"}
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

    try:
        db.update_article(article_id, updates)
        # Record revision
        try:
            db.record_revision(article_id, changes, revision_type=revision_type)
        except Exception:
            pass
        # Index in Meilisearch (fire-and-forget)
        try:
            article_data = db.get_article(article_id)
            if article_data:
                meili.index_article(article_data)
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
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.delete("/api/article/{article_id}")
async def api_delete_article(article_id: int):
    try:
        db.update_article(article_id, {"status": "archived", "updated_at": datetime.now().isoformat(timespec="seconds")})
        # Remove from Meilisearch
        try:
            meili.delete_article(article_id)
        except Exception:
            pass
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


@app.get("/api/article/{article_id}/revisions")
async def api_article_revisions(article_id: int):
    try:
        revisions = db.get_revisions(article_id, limit=20)
        return {"ok": True, "revisions": revisions}
    except Exception as e:
        return {"ok": False, "error": str(e), "revisions": []}


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
