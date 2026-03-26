"""Планировщик публикаций.

Фоновый asyncio-таск, запускаемый из lifespan FastAPI.
Каждые 60 секунд проверяет статьи со статусом 'scheduled' и
scheduled_at <= now(), переводит их в 'published' и индексирует
в Meilisearch.
"""

import asyncio
import logging
from datetime import datetime

from app.config import settings

logger = logging.getLogger(__name__)


def _publish_scheduled_pg() -> int:
    """Publish scheduled articles using PostgreSQL backend."""
    from app.pg_queries import get_pg_session, Article
    from app import search_engine as meili
    from app import db_backend as db
    from sqlalchemy import select

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    published_count = 0

    with get_pg_session() as session:
        rows = session.execute(
            select(Article.id).where(
                Article.status == "scheduled",
                Article.scheduled_at.isnot(None),
                Article.scheduled_at <= now,
            )
        ).all()

        for row in rows:
            article_id = row[0]
            article = session.get(Article, article_id)
            if article:
                article.status = "published"
                article.updated_at = datetime.utcnow().isoformat(timespec="seconds")
                published_count += 1
                logger.info("Scheduler: опубликована статья id=%d", article_id)

    # Index in Meilisearch after commit (fire-and-forget)
    if published_count:
        for row in rows:
            try:
                article_data = db.get_article(row[0])
                if article_data:
                    meili.index_article(article_data)
            except Exception as exc:
                logger.warning("Scheduler: ошибка индексации id=%d: %s", row[0], exc)

    return published_count


def _publish_scheduled_sqlite() -> int:
    """Publish scheduled articles using SQLite backend."""
    from app.database import get_db
    from app import search_engine as meili
    from app import db_backend as db

    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    published_count = 0

    with get_db() as conn:
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


def publish_scheduled_articles_sync() -> int:
    """Синхронная обёртка — выбирает правильный бэкенд."""
    if settings.use_postgres:
        return _publish_scheduled_pg()
    else:
        return _publish_scheduled_sqlite()


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
