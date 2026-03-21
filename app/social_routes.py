"""Admin routes for social media management."""

import json
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, Query, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from . import social
from . import database as db

router = APIRouter(prefix="/admin/social")

BASE_DIR = Path(__file__).parent
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


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def social_dashboard(request: Request):
    """Social media dashboard – overview of all accounts, stats, content plan."""
    social.init_social_db()

    accounts = social.get_all_stats_summary()
    post_stats = social.get_post_stats_summary()
    plan_stats = social.get_content_plan_stats()

    # Content plan for the next 14 days
    today = datetime.utcnow().strftime("%Y-%m-%d")
    plan_end = (datetime.utcnow() + timedelta(days=14)).strftime("%Y-%m-%d")
    content_plan = social.get_content_plan(date_from=today, date_to=plan_end)

    # Recent posts
    recent_posts = social.get_posts(limit=20)

    # Pending auto-posts
    pending = social.get_pending_auto_posts()

    return templates.TemplateResponse("social.html", {
        "request": request,
        "active": "social",
        "accounts": accounts,
        "post_stats": post_stats,
        "plan_stats": plan_stats,
        "content_plan": content_plan,
        "recent_posts": recent_posts,
        "pending_articles": pending,
        "platforms": social.PLATFORM_INFO,
        "today": today,
    })


# ── Account Management ──────────────────────────

@router.post("/account/save")
async def save_account(
    platform: str = Form(...),
    account_name: str = Form(...),
    account_url: str = Form(""),
    account_id: str = Form(""),
    api_token: str = Form(""),
    is_active: int = Form(1),
    auto_post: int = Form(0),
):
    """Create or update social account."""
    social.init_social_db()
    config = {"auto_post": bool(auto_post)}
    social.upsert_account(
        platform=platform,
        account_name=account_name,
        account_url=account_url,
        account_id=account_id,
        api_token=api_token,
        is_active=bool(is_active),
        config=config,
    )
    return RedirectResponse(url="/admin/social", status_code=303)


@router.post("/account/delete/{account_id}")
async def remove_account(account_id: int):
    social.init_social_db()
    social.delete_account(account_id)
    return RedirectResponse(url="/admin/social", status_code=303)


# ── Content Plan ────────────────────────────────

@router.post("/plan/save")
async def save_plan(
    plan_id: int = Form(None),
    platform: str = Form("all"),
    planned_date: str = Form(...),
    planned_time: str = Form(""),
    content_type: str = Form("post"),
    title: str = Form(...),
    description: str = Form(""),
    article_id: int = Form(None),
    status: str = Form("planned"),
    assigned_to: str = Form(""),
):
    social.init_social_db()
    social.upsert_content_plan(
        plan_id=plan_id if plan_id and plan_id > 0 else None,
        platform=platform,
        planned_date=planned_date,
        planned_time=planned_time,
        content_type=content_type,
        title=title,
        description=description,
        article_id=article_id if article_id and article_id > 0 else None,
        status=status,
        assigned_to=assigned_to,
    )
    return RedirectResponse(url="/admin/social", status_code=303)


@router.post("/plan/delete/{plan_id}")
async def remove_plan(plan_id: int):
    social.init_social_db()
    social.delete_content_plan(plan_id)
    return RedirectResponse(url="/admin/social", status_code=303)


@router.post("/plan/status/{plan_id}")
async def update_plan_status(plan_id: int, status: str = Form(...)):
    social.init_social_db()
    social.upsert_content_plan(plan_id=plan_id, status=status)
    return RedirectResponse(url="/admin/social", status_code=303)


# ── Manual Post ─────────────────────────────────

@router.post("/post/create")
async def create_manual_post(
    account_id: int = Form(...),
    title: str = Form(""),
    body: str = Form(...),
    article_id: int = Form(None),
    scheduled_at: str = Form(""),
):
    social.init_social_db()
    account = social.get_account(account_id)
    if not account:
        return RedirectResponse(url="/admin/social", status_code=303)

    social.create_post(
        account_id=account_id,
        platform=account["platform"],
        article_id=article_id if article_id and article_id > 0 else None,
        post_type="manual" if not scheduled_at else "scheduled",
        title=title,
        body=body,
        scheduled_at=scheduled_at if scheduled_at else None,
    )
    return RedirectResponse(url="/admin/social", status_code=303)


# ── Stats Recording (API) ──────────────────────

@router.post("/stats/record")
async def record_account_stats(
    account_id: int = Form(...),
    followers: int = Form(0),
    posts_count: int = Form(0),
    engagement_rate: float = Form(0),
    reach: int = Form(0),
):
    social.init_social_db()
    social.record_stats(
        account_id=account_id,
        followers=followers,
        posts_count=posts_count,
        engagement_rate=engagement_rate,
        reach=reach,
    )
    return RedirectResponse(url="/admin/social", status_code=303)


# ── API endpoints ──────────────────────────────

@router.get("/api/accounts")
async def api_accounts():
    social.init_social_db()
    return social.get_accounts()


@router.get("/api/posts")
async def api_posts(platform: str = "", status: str = "", limit: int = 50):
    social.init_social_db()
    return social.get_posts(platform=platform, status=status, limit=limit)


@router.get("/api/plan")
async def api_plan(date_from: str = "", date_to: str = "", platform: str = ""):
    social.init_social_db()
    return social.get_content_plan(date_from=date_from, date_to=date_to, platform=platform)


@router.get("/api/stats/{account_id}")
async def api_stats(account_id: int, days: int = 30):
    social.init_social_db()
    return social.get_stats_history(account_id, days=days)


@router.get("/api/pending")
async def api_pending():
    social.init_social_db()
    return social.get_pending_auto_posts()
