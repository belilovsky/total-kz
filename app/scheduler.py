"""Планировщик публикаций.

Фоновый asyncio-таск, запускаемый из lifespan FastAPI.
Каждые 60 секунд проверяет статьи со статусом 'scheduled' и
scheduled_at <= now(), переводит их в 'published' и индексирует
в Meilisearch.
"""

import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def publish_scheduled_articles() -> int:
    """Опубликовать все статьи, у которых наступило scheduled_at.

    Returns:
        Количество опубликованных статей.
    """
    from . import db_backend as db
    from . import search_engine as meili

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    published_count = 0

    with db.get_db() as conn:
        rows = conn.execute(
            """
            SELECT id FROM articles
            WHERE status = 'scheduled'
              AND scheduled_at IS NOT NULL
              AND scheduled_at <= ?
            """,
            (now,),
        ).fetchall()

        for row in rows:
            article_id = row[0]
            conn.execute(
                """
                UPDATE articles
                SET status = 'published',
                    updated_at = datetime('now')
                WHERE id = ?
                """,
                (article_id,),
            )
            published_count += 1
            logger.info("Scheduler: опубликована статья id=%d", article_id)

            # Индексируем в Meilisearch (fire-and-forget, не падаем при ошибке)
            try:
                article_data = db.get_article(article_id)
                if article_data:
                    meili.index_article(article_data)
            except Exception as exc:
                logger.warning("Scheduler: ошибка индексации id=%d: %s", article_id, exc)

    return published_count


async def scheduler_loop(interval: int = 60) -> None:
    """Бесконечный цикл планировщика.

    Args:
        interval: Интервал проверки в секундах (по умолчанию 60).
    """
    logger.info("Планировщик публикаций запущен (интервал %ds)", interval)
    while True:
        try:
            count = await asyncio.to_thread(publish_scheduled_articles_sync)
            if count:
                logger.info("Scheduler: опубликовано %d статей", count)
        except Exception as exc:
            logger.error("Scheduler: необработанная ошибка: %s", exc, exc_info=True)
        await asyncio.sleep(interval)


def publish_scheduled_articles_sync() -> int:
    """Синхронная обёртка для вызова из asyncio.to_thread."""
    from . import db_backend as db
    from . import search_engine as meili

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    published_count = 0

    with db.get_db() as conn:
        rows = conn.execute(
            """
            SELECT id FROM articles
            WHERE status = 'scheduled'
              AND scheduled_at IS NOT NULL
              AND scheduled_at <= ?
            """,
            (now,),
        ).fetchall()

        for row in rows:
            article_id = row[0]
            conn.execute(
                """
                UPDATE articles
                SET status = 'published',
                    updated_at = datetime('now')
                WHERE id = ?
                """,
                (article_id,),
            )
            published_count += 1
            logger.info("Scheduler: опубликована статья id=%d", article_id)

            try:
                article_data = db.get_article(article_id)
                if article_data:
                    meili.index_article(article_data)
            except Exception as exc:
                logger.warning("Scheduler: ошибка индексации id=%d: %s", article_id, exc)

    return published_count
