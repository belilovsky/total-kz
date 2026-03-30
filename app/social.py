"""Social media integration module for Total.kz admin dashboard.

Manages social accounts, content plans, auto-posting, and analytics.
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager

from . import database as db


# ══════════════════════════════════════════════
#  SCHEMA – social media tables
# ══════════════════════════════════════════════

SOCIAL_SCHEMA = """
-- Social media accounts linked to Total.kz
CREATE TABLE IF NOT EXISTS social_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,           -- telegram, instagram, youtube, facebook, tiktok, x, vk
    account_name TEXT NOT NULL,       -- @total_kz, Total.kz, etc.
    account_url TEXT,                 -- https://t.me/totalkz, etc.
    account_id TEXT,                  -- platform-specific ID (chat_id for Telegram, etc.)
    api_token TEXT,                   -- bot token, API key (encrypted in production)
    is_active INTEGER DEFAULT 1,
    followers_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    config TEXT DEFAULT '{}'          -- JSON: platform-specific settings
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_social_platform ON social_accounts(platform, account_name);

-- Snapshot of social account stats over time (for growth charts)
CREATE TABLE IF NOT EXISTS social_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER REFERENCES social_accounts(id),
    recorded_at TEXT DEFAULT (datetime('now')),
    followers INTEGER DEFAULT 0,
    posts_count INTEGER DEFAULT 0,
    engagement_rate REAL DEFAULT 0,   -- likes+comments / followers * 100
    reach INTEGER DEFAULT 0,          -- estimated reach
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    extra TEXT DEFAULT '{}'           -- JSON: platform-specific metrics
);

CREATE INDEX IF NOT EXISTS idx_social_stats_account ON social_stats(account_id);
CREATE INDEX IF NOT EXISTS idx_social_stats_date ON social_stats(recorded_at);

-- Posts published to social media (manual or auto)
CREATE TABLE IF NOT EXISTS social_posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER REFERENCES social_accounts(id),
    article_id INTEGER REFERENCES articles(id),  -- NULL if not linked to article
    platform TEXT NOT NULL,
    post_type TEXT DEFAULT 'auto',    -- auto, manual, scheduled
    status TEXT DEFAULT 'draft',      -- draft, scheduled, published, failed
    title TEXT,
    body TEXT,                        -- post text/caption
    media_url TEXT,                   -- image/video URL
    post_url TEXT,                    -- URL of published post
    platform_post_id TEXT,            -- ID returned by platform
    scheduled_at TEXT,                -- when to publish (NULL = immediate)
    published_at TEXT,
    likes INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    shares INTEGER DEFAULT 0,
    views INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_social_posts_status ON social_posts(status);
CREATE INDEX IF NOT EXISTS idx_social_posts_platform ON social_posts(platform);
CREATE INDEX IF NOT EXISTS idx_social_posts_article ON social_posts(article_id);
CREATE INDEX IF NOT EXISTS idx_social_posts_scheduled ON social_posts(scheduled_at);

-- Content plan entries (editorial calendar for social)
CREATE TABLE IF NOT EXISTS content_plan (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,           -- telegram, instagram, all, etc.
    planned_date TEXT NOT NULL,       -- YYYY-MM-DD
    planned_time TEXT,                -- HH:MM (optional)
    content_type TEXT DEFAULT 'post', -- post, story, reel, video, poll
    title TEXT NOT NULL,
    description TEXT,
    article_id INTEGER REFERENCES articles(id),  -- link to existing article
    status TEXT DEFAULT 'planned',    -- planned, in_progress, done, cancelled
    assigned_to TEXT,                 -- person responsible
    tags TEXT DEFAULT '[]',           -- JSON array of labels
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_content_plan_date ON content_plan(planned_date);
CREATE INDEX IF NOT EXISTS idx_content_plan_status ON content_plan(status);
CREATE INDEX IF NOT EXISTS idx_content_plan_platform ON content_plan(platform);
"""


def init_social_db():
    """Create social media tables if they don't exist."""
    with db.get_db() as conn:
        conn.executescript(SOCIAL_SCHEMA)


# ══════════════════════════════════════════════
#  PLATFORM CONFIG
# ══════════════════════════════════════════════

PLATFORM_INFO = {
    "telegram": {
        "name": "Telegram",
        "icon": "telegram",
        "color": "#26A5E4",
        "share_url": "https://t.me/share/url?url={url}&text={text}",
        "default_url": "https://t.me/",
    },
    "instagram": {
        "name": "Instagram",
        "icon": "instagram",
        "color": "#E4405F",
        "share_url": None,  # No direct share URL
        "default_url": "https://instagram.com/",
    },
    "youtube": {
        "name": "YouTube",
        "icon": "youtube",
        "color": "#FF0000",
        "share_url": None,
        "default_url": "https://youtube.com/",
    },
    "facebook": {
        "name": "Facebook",
        "icon": "facebook",
        "color": "#1877F2",
        "share_url": "https://www.facebook.com/sharer/sharer.php?u={url}",
        "default_url": "https://facebook.com/",
    },
    "tiktok": {
        "name": "TikTok",
        "icon": "tiktok",
        "color": "#000000",
        "share_url": None,
        "default_url": "https://tiktok.com/@",
    },
    "x": {
        "name": "X (Twitter)",
        "icon": "x",
        "color": "#000000",
        "share_url": "https://x.com/intent/tweet?url={url}&text={text}",
        "default_url": "https://x.com/",
    },
    "vk": {
        "name": "ВКонтакте",
        "icon": "vk",
        "color": "#0077FF",
        "share_url": "https://vk.com/share.php?url={url}&title={text}",
        "default_url": "https://vk.com/",
    },
}


# ══════════════════════════════════════════════
#  SOCIAL ACCOUNTS CRUD
# ══════════════════════════════════════════════

def get_accounts(active_only: bool = False) -> list:
    """Get all social accounts."""
    with db.get_db() as conn:
        where = "WHERE is_active = 1" if active_only else ""
        rows = conn.execute(f"""
            SELECT * FROM social_accounts {where} ORDER BY platform, account_name
        """).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["config"] = json.loads(d.get("config") or "{}")
            d["platform_info"] = PLATFORM_INFO.get(d["platform"], {})
            result.append(d)
        return result


def get_account(account_id: int) -> dict | None:
    with db.get_db() as conn:
        row = conn.execute("SELECT * FROM social_accounts WHERE id = ?", (account_id,)).fetchone()
        if row:
            d = dict(row)
            d["config"] = json.loads(d.get("config") or "{}")
            d["platform_info"] = PLATFORM_INFO.get(d["platform"], {})
            return d
        return None


def upsert_account(
    platform: str,
    account_name: str,
    account_url: str = "",
    account_id: str = "",
    api_token: str = "",
    is_active: bool = True,
    config: dict = None,
) -> int:
    """Create or update a social account. Returns account ID."""
    with db.get_db() as conn:
        config_json = json.dumps(config or {}, ensure_ascii=False)
        conn.execute("""
            INSERT INTO social_accounts (platform, account_name, account_url, account_id, api_token, is_active, config)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(platform, account_name) DO UPDATE SET
                account_url = excluded.account_url,
                account_id = CASE WHEN excluded.account_id != '' THEN excluded.account_id ELSE social_accounts.account_id END,
                api_token = CASE WHEN excluded.api_token != '' THEN excluded.api_token ELSE social_accounts.api_token END,
                is_active = excluded.is_active,
                config = excluded.config,
                updated_at = datetime('now')
        """, (platform, account_name, account_url, account_id, api_token, int(is_active), config_json))
        row = conn.execute(
            "SELECT id FROM social_accounts WHERE platform = ? AND account_name = ?",
            (platform, account_name)
        ).fetchone()
        return row[0] if row else 0


def delete_account(account_id: int):
    with db.get_db() as conn:
        conn.execute("DELETE FROM social_stats WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM social_posts WHERE account_id = ?", (account_id,))
        conn.execute("DELETE FROM social_accounts WHERE id = ?", (account_id,))


# ══════════════════════════════════════════════
#  SOCIAL STATS
# ══════════════════════════════════════════════

def record_stats(
    account_id: int,
    followers: int = 0,
    posts_count: int = 0,
    engagement_rate: float = 0,
    reach: int = 0,
    impressions: int = 0,
    clicks: int = 0,
    extra: dict = None,
):
    """Record a snapshot of social stats."""
    with db.get_db() as conn:
        conn.execute("""
            INSERT INTO social_stats (account_id, followers, posts_count, engagement_rate, reach, impressions, clicks, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (account_id, followers, posts_count, engagement_rate, reach, impressions, clicks,
              json.dumps(extra or {}, ensure_ascii=False)))
        # Also update current followers on the account
        conn.execute("UPDATE social_accounts SET followers_count = ?, updated_at = datetime('now') WHERE id = ?",
                     (followers, account_id))


def get_stats_history(account_id: int, days: int = 30) -> list:
    """Get stats snapshots for an account over the last N days."""
    with db.get_db() as conn:
        since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT * FROM social_stats
            WHERE account_id = ? AND recorded_at >= ?
            ORDER BY recorded_at
        """, (account_id, since)).fetchall()
        return [dict(r) for r in rows]


def get_all_stats_summary() -> list:
    """Get latest stats for all active accounts."""
    with db.get_db() as conn:
        rows = conn.execute("""
            SELECT sa.*, 
                   ss.followers as latest_followers,
                   ss.engagement_rate as latest_engagement,
                   ss.reach as latest_reach,
                   ss.recorded_at as stats_date
            FROM social_accounts sa
            LEFT JOIN social_stats ss ON ss.account_id = sa.id
                AND ss.id = (SELECT MAX(id) FROM social_stats WHERE account_id = sa.id)
            WHERE sa.is_active = 1
            ORDER BY sa.platform
        """).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["platform_info"] = PLATFORM_INFO.get(d["platform"], {})
            result.append(d)
        return result


# ══════════════════════════════════════════════
#  SOCIAL POSTS
# ══════════════════════════════════════════════

def create_post(
    account_id: int,
    platform: str,
    article_id: int = None,
    post_type: str = "manual",
    title: str = "",
    body: str = "",
    media_url: str = "",
    scheduled_at: str = None,
) -> int:
    """Create a new social post (draft or scheduled)."""
    status = "scheduled" if scheduled_at else "draft"
    with db.get_db() as conn:
        cur = conn.execute("""
            INSERT INTO social_posts (account_id, article_id, platform, post_type, status, title, body, media_url, scheduled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (account_id, article_id, platform, post_type, status, title, body, media_url, scheduled_at))
        return cur.lastrowid


def update_post_status(post_id: int, status: str, post_url: str = "", platform_post_id: str = "", error: str = ""):
    """Update post status after publishing."""
    with db.get_db() as conn:
        published = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") if status == "published" else None
        conn.execute("""
            UPDATE social_posts SET status = ?, post_url = ?, platform_post_id = ?,
                   published_at = COALESCE(?, published_at), error_message = ?
            WHERE id = ?
        """, (status, post_url, platform_post_id, published, error, post_id))


def get_posts(
    platform: str = "",
    status: str = "",
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """Get social posts with filters."""
    with db.get_db() as conn:
        conditions = []
        params = []
        if platform:
            conditions.append("sp.platform = ?")
            params.append(platform)
        if status:
            conditions.append("sp.status = ?")
            params.append(status)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        total = conn.execute(f"SELECT COUNT(*) FROM social_posts sp {where}", params).fetchone()[0]
        rows = conn.execute(f"""
            SELECT sp.*, sa.account_name, a.title as article_title
            FROM social_posts sp
            LEFT JOIN social_accounts sa ON sa.id = sp.account_id
            LEFT JOIN articles a ON a.id = sp.article_id
            {where}
            ORDER BY sp.created_at DESC
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()

        return {
            "posts": [dict(r) for r in rows],
            "total": total,
        }


def get_pending_auto_posts() -> list:
    """Get articles that haven't been auto-posted yet (for auto-posting cron)."""
    with db.get_db() as conn:
        rows = conn.execute("""
            SELECT a.id, a.title, a.excerpt, a.url, a.sub_category, a.main_image, a.thumbnail, a.pub_date
            FROM articles a
            LEFT JOIN social_posts sp ON sp.article_id = a.id AND sp.post_type = 'auto'
            WHERE sp.id IS NULL
                AND a.pub_date >= datetime('now', '-24 hours')
                AND a.title IS NOT NULL AND a.title != ''
            ORDER BY a.pub_date DESC
            LIMIT 10
        """).fetchall()
        return [dict(r) for r in rows]


def get_post_stats_summary() -> dict:
    """Get posting stats for dashboard."""
    with db.get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM social_posts").fetchone()[0]
        published = conn.execute("SELECT COUNT(*) FROM social_posts WHERE status = 'published'").fetchone()[0]
        scheduled = conn.execute("SELECT COUNT(*) FROM social_posts WHERE status = 'scheduled'").fetchone()[0]
        failed = conn.execute("SELECT COUNT(*) FROM social_posts WHERE status = 'failed'").fetchone()[0]

        by_platform = conn.execute("""
            SELECT platform, COUNT(*) as cnt,
                   SUM(CASE WHEN status = 'published' THEN 1 ELSE 0 END) as published_cnt,
                   SUM(likes) as total_likes, SUM(views) as total_views
            FROM social_posts
            GROUP BY platform
        """).fetchall()

        today_count = conn.execute("""
            SELECT COUNT(*) FROM social_posts
            WHERE published_at >= date('now') AND status = 'published'
        """).fetchone()[0]

        week_count = conn.execute("""
            SELECT COUNT(*) FROM social_posts
            WHERE published_at >= date('now', '-7 days') AND status = 'published'
        """).fetchone()[0]

        return {
            "total": total,
            "published": published,
            "scheduled": scheduled,
            "failed": failed,
            "today": today_count,
            "this_week": week_count,
            "by_platform": [dict(r) for r in by_platform],
        }


# ══════════════════════════════════════════════
#  CONTENT PLAN
# ══════════════════════════════════════════════

def get_content_plan(
    date_from: str = "",
    date_to: str = "",
    platform: str = "",
    status: str = "",
) -> list:
    """Get content plan entries."""
    with db.get_db() as conn:
        conditions = []
        params = []
        if date_from:
            conditions.append("planned_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("planned_date <= ?")
            params.append(date_to)
        if platform:
            conditions.append("platform = ?")
            params.append(platform)
        if status:
            conditions.append("status = ?")
            params.append(status)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        rows = conn.execute(f"""
            SELECT cp.*, a.title as article_title
            FROM content_plan cp
            LEFT JOIN articles a ON a.id = cp.article_id
            {where}
            ORDER BY cp.planned_date, cp.planned_time
        """, params).fetchall()

        result = []
        for r in rows:
            d = dict(r)
            d["tags"] = json.loads(d.get("tags") or "[]")
            result.append(d)
        return result


def upsert_content_plan(
    plan_id: int = None,
    platform: str = "all",
    planned_date: str = "",
    planned_time: str = "",
    content_type: str = "post",
    title: str = "",
    description: str = "",
    article_id: int = None,
    status: str = "planned",
    assigned_to: str = "",
    tags: list = None,
) -> int:
    """Create or update a content plan entry."""
    with db.get_db() as conn:
        tags_json = json.dumps(tags or [], ensure_ascii=False)
        if plan_id:
            conn.execute("""
                UPDATE content_plan SET
                    platform = ?, planned_date = ?, planned_time = ?, content_type = ?,
                    title = ?, description = ?, article_id = ?, status = ?,
                    assigned_to = ?, tags = ?, updated_at = datetime('now')
                WHERE id = ?
            """, (platform, planned_date, planned_time, content_type,
                  title, description, article_id, status, assigned_to, tags_json, plan_id))
            return plan_id
        else:
            cur = conn.execute("""
                INSERT INTO content_plan (platform, planned_date, planned_time, content_type,
                    title, description, article_id, status, assigned_to, tags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (platform, planned_date, planned_time, content_type,
                  title, description, article_id, status, assigned_to, tags_json))
            return cur.lastrowid


def delete_content_plan(plan_id: int):
    with db.get_db() as conn:
        conn.execute("DELETE FROM content_plan WHERE id = ?", (plan_id,))


def get_content_plan_stats() -> dict:
    """Stats for content plan dashboard."""
    with db.get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM content_plan").fetchone()[0]
        planned = conn.execute("SELECT COUNT(*) FROM content_plan WHERE status = 'planned'").fetchone()[0]
        done = conn.execute("SELECT COUNT(*) FROM content_plan WHERE status = 'done'").fetchone()[0]
        this_week = conn.execute("""
            SELECT COUNT(*) FROM content_plan
            WHERE planned_date >= date('now') AND planned_date <= date('now', '+7 days')
        """).fetchone()[0]
        overdue = conn.execute("""
            SELECT COUNT(*) FROM content_plan
            WHERE planned_date < date('now') AND status = 'planned'
        """).fetchone()[0]

        return {
            "total": total,
            "planned": planned,
            "done": done,
            "this_week": this_week,
            "overdue": overdue,
        }


# ══════════════════════════════════════════════
#  AUTO-POSTING HELPERS
# ══════════════════════════════════════════════

def format_telegram_post(article: dict, site_url: str = "https://total.kz") -> str:
    """Format article for Telegram channel post.

    Includes: bold title, 1-2 sentence excerpt, source name, link, hashtags from tags.
    """
    import re as _re
    title = article.get("title", "")
    excerpt = article.get("excerpt", "")
    url = article.get("url", "")
    source = article.get("author") or "Total.kz"
    tags = article.get("tags") or []
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            tags = []

    # Build new URL
    parts = url.replace("https://total.kz/ru/news/", "").strip("/").split("/")
    if len(parts) >= 2:
        link = f"{site_url}/news/{parts[0]}/{parts[1]}"
    else:
        link = site_url

    text = f"\U0001f4f0 <b>{title}</b>"
    if excerpt:
        short = excerpt[:200] + ("\u2026" if len(excerpt) > 200 else "")
        text += f"\n\n{short}"
    text += f"\n\n\U0001f4dd {source}"
    text += f"\n\n\U0001f449 <a href=\"{link}\">\u0427\u0438\u0442\u0430\u0442\u044c \u043d\u0430 Total.kz</a>"

    # Hashtags from tags (up to 5)
    if tags:
        hashtags = []
        for tag in tags[:5]:
            cleaned = _re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9_]', '', tag.replace(' ', '_'))
            if cleaned:
                hashtags.append(f"#{cleaned}")
        if hashtags:
            text += f"\n\n{' '.join(hashtags)}"

    return text


def send_telegram_message(bot_token: str, chat_id: str, text: str, photo_url: str = "") -> dict:
    """Send a message to a Telegram channel via Bot API.

    Returns dict with 'ok' and 'result' or 'description' on error.
    """
    import requests
    base = f"https://api.telegram.org/bot{bot_token}"

    try:
        if photo_url:
            resp = requests.post(f"{base}/sendPhoto", data={
                "chat_id": chat_id,
                "caption": text,
                "parse_mode": "HTML",
                "photo": photo_url,
            }, timeout=15)
        else:
            resp = requests.post(f"{base}/sendMessage", data={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "false",
            }, timeout=15)
        return resp.json()
    except Exception as e:
        return {"ok": False, "description": str(e)}


def get_autopost_config() -> dict:
    """Get auto-posting configuration."""
    with db.get_db() as conn:
        accounts = conn.execute("""
            SELECT * FROM social_accounts WHERE is_active = 1
        """).fetchall()
        result = {}
        for a in accounts:
            d = dict(a)
            config = json.loads(d.get("config") or "{}")
            if config.get("auto_post", False):
                result[d["platform"]] = {
                    "account_id": d["id"],
                    "account_name": d["account_name"],
                    "api_token": d["api_token"],
                    "account_id_str": d["account_id"],
                    "config": config,
                }
        return result
