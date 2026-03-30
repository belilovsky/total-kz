"""Telegram auto-posting scheduler for Total.kz.

Background async task that checks for new articles every 5 minutes
and posts them to configured Telegram channels.
"""

import asyncio
import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# Global flag to enable/disable auto-posting
_autopost_enabled = True


def is_autopost_enabled() -> bool:
    return _autopost_enabled


def set_autopost_enabled(enabled: bool):
    global _autopost_enabled
    _autopost_enabled = enabled
    logger.info("Auto-posting %s", "enabled" if enabled else "disabled")


def _do_autopost_cycle() -> int:
    """Run one cycle of auto-posting. Returns number of posts sent."""
    from app import social
    from app.public_routes import SITE_DOMAIN

    if not _autopost_enabled:
        return 0

    config = social.get_autopost_config()
    tg_config = config.get("telegram")
    if not tg_config:
        return 0

    bot_token = tg_config.get("api_token", "")
    chat_id = tg_config.get("account_id_str", "")
    account_id = tg_config.get("account_id")
    account_config = tg_config.get("config", {})

    if not bot_token or not chat_id:
        logger.debug("Telegram auto-post: missing bot_token or chat_id")
        return 0

    # Get category filter if configured
    allowed_categories = account_config.get("auto_post_categories", [])

    pending = social.get_pending_auto_posts()
    if not pending:
        return 0

    posted = 0
    for article in pending:
        # Filter by category if configured
        if allowed_categories and article.get("sub_category") not in allowed_categories:
            continue

        text = social.format_telegram_post(article, site_url=SITE_DOMAIN)

        # Get image URL for photo post
        photo_url = ""
        img = article.get("main_image") or article.get("thumbnail", "")
        if img:
            photo_url = img if img.startswith("http") else f"{SITE_DOMAIN}{img}"

        result = social.send_telegram_message(bot_token, chat_id, text, photo_url)

        if result.get("ok"):
            tg_msg = result.get("result", {})
            post_url = f"https://t.me/{chat_id.lstrip('@')}/{tg_msg.get('message_id', '')}" if chat_id.startswith("@") else ""
            social.create_post(
                account_id=account_id,
                platform="telegram",
                article_id=article["id"],
                post_type="auto",
                title=article["title"],
                body=text,
                media_url=photo_url,
            )
            social.update_post_status(
                post_id=social.get_posts(platform="telegram", status="draft", limit=1)["posts"][0]["id"]
                if social.get_posts(platform="telegram", status="draft", limit=1)["posts"]
                else 0,
                status="published",
                post_url=post_url,
                platform_post_id=str(tg_msg.get("message_id", "")),
            )
            posted += 1
            logger.info("Auto-posted article id=%d to Telegram", article["id"])
        else:
            error_msg = result.get("description", "Unknown error")
            social.create_post(
                account_id=account_id,
                platform="telegram",
                article_id=article["id"],
                post_type="auto",
                title=article["title"],
                body=text,
                media_url=photo_url,
            )
            logger.warning("Failed to auto-post article id=%d: %s", article["id"], error_msg)

        # Rate limit: max 1 message per 3 seconds
        time.sleep(3)

    # Ping WebSub after posting new content
    if posted:
        try:
            from app.public_routes import ping_websub_hub
            ping_websub_hub()
        except Exception:
            pass

    return posted


async def autopost_loop(interval: int = 300) -> None:
    """Background loop — checks for new articles every `interval` seconds (default 5 min).

    Args:
        interval: Check interval in seconds (default 300 = 5 minutes).
    """
    logger.info("Telegram auto-posting scheduler started (interval %ds)", interval)
    while True:
        try:
            if _autopost_enabled:
                count = await asyncio.to_thread(_do_autopost_cycle)
                if count:
                    logger.info("Auto-posted %d articles to Telegram", count)
        except Exception as exc:
            logger.error("Autopost: unhandled error: %s", exc, exc_info=True)
        await asyncio.sleep(interval)
