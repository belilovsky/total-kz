"""Telegram notifications for editorial team."""

import logging
import os

log = logging.getLogger("news-monitor.notifier")

# Lazy init — only import telegram if token is set
_bot = None
_chat_id = None
_enabled = False


def init_telegram() -> bool:
    """Initialize Telegram bot. Returns True if ready."""
    global _bot, _chat_id, _enabled

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not token:
        log.info("TELEGRAM_BOT_TOKEN not set — notifications disabled")
        _enabled = False
        return False

    if not chat_id:
        log.warning("TELEGRAM_CHAT_ID not set — notifications disabled")
        _enabled = False
        return False

    try:
        import telegram
        _bot = telegram.Bot(token=token)
        _chat_id = chat_id
        _enabled = True
        log.info("Telegram notifications enabled (chat_id=%s)", chat_id)
        return True
    except Exception as e:
        log.error("Failed to init Telegram bot: %s", e)
        _enabled = False
        return False


async def notify_new_article(
    article_id: int,
    title: str,
    excerpt: str,
    source_name: str,
    category: str,
) -> bool:
    """Send notification about new article to editorial chat.

    Returns True if sent, False otherwise.
    """
    if not _enabled or not _bot:
        log.debug("Telegram disabled — skipping notification for article %d", article_id)
        return False

    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        text = (
            f"📰 <b>Новая статья</b> (#{article_id})\n\n"
            f"<b>{title}</b>\n\n"
            f"{excerpt}\n\n"
            f"📌 Источник: {source_name}\n"
            f"📂 Категория: {category}"
        )

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Опубликовать", callback_data=f"pub:{article_id}"),
                InlineKeyboardButton("📝 В редактор", callback_data=f"edit:{article_id}"),
            ],
            [
                InlineKeyboardButton("❌ Пропустить", callback_data=f"skip:{article_id}"),
            ],
        ])

        await _bot.send_message(
            chat_id=_chat_id,
            text=text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
        log.info("Telegram notification sent for article %d", article_id)
        return True

    except Exception as e:
        log.error("Telegram send failed: %s", e)
        return False


def notify_sync(article_id: int, title: str, excerpt: str, source_name: str, category: str) -> bool:
    """Synchronous wrapper for notify_new_article."""
    if not _enabled:
        return False

    import asyncio

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Create a new event loop in a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(
                    asyncio.run,
                    notify_new_article(article_id, title, excerpt, source_name, category),
                ).result(timeout=10)
        else:
            return loop.run_until_complete(
                notify_new_article(article_id, title, excerpt, source_name, category)
            )
    except Exception as e:
        log.error("Telegram notify_sync failed: %s", e)
        return False
