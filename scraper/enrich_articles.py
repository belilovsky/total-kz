#!/usr/bin/env python3
"""
Постоянный скрипт обогащения статей через OpenAI GPT.

Обрабатывает статьи БЕЗ enrichment записи.
Генерирует: meta_description, keywords, summary, quote.

Запуск:
  python scraper/enrich_articles.py               # обработать до 100 статей
  python scraper/enrich_articles.py --batch 500    # 500 штук
  python scraper/enrich_articles.py --all          # все необогащённые
  python scraper/enrich_articles.py --since 2026-01-01  # только с указанной даты

Для Docker cron:
  python scraper/enrich_articles.py --batch 50
"""

import json
import os
import sqlite3
import sys
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path

# ── Config ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = os.environ.get("DB_PATH", str(DATA_DIR / "total.db"))
LOG_FILE = DATA_DIR / "enrich.log"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

MODEL = os.environ.get("ENRICHMENT_MODEL", "gpt-4o-mini")
MAX_RETRIES = 3
RETRY_DELAY = 5  # секунд
RATE_LIMIT_DELAY = 1.0  # задержка между запросами (секунд)

# ── Logging ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("enrich")


# ── OpenAI ──────────────────────────────────────────────
def _call_openai(title: str, text: str) -> dict | None:
    """Один вызов OpenAI API. Возвращает dict или None при ошибке."""
    import httpx

    # Обрезаем текст до ~3000 символов для экономии токенов
    truncated = text[:3000] if text else ""

    prompt = f"""Ты — редактор новостного сайта total.kz (Казахстан). Проанализируй статью и верни JSON.

Заголовок: {title}

Текст (начало):
{truncated}

Верни JSON (без markdown, только JSON):
{{
  "meta_description": "SEO-описание, 120-160 символов, на русском",
  "keywords": ["ключевое слово 1", "ключевое слово 2", ...],
  "summary": "Краткое содержание статьи, 1-2 предложения",
  "quote": "Самая яркая цитата из текста (если есть, иначе null)",
  "quote_author": "Автор цитаты (если есть, иначе null)"
}}

Правила:
- keywords: 3-6 слов/фраз, релевантных для SEO и навигации
- meta_description: информативное, привлекательное, с ключевыми словами
- summary: объективное, нейтральное, на русском
- Если цитаты нет — quote и quote_author = null"""

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 500,
        "response_format": {"type": "json_object"},
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = httpx.post(
                "https://api.openai.com/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=30,
            )
            if r.status_code == 429:
                wait = RETRY_DELAY * (attempt + 2)
                log.warning("Rate limited, ожидаю %ds...", wait)
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log.error("OpenAI HTTP %d: %s", r.status_code, r.text[:200])
                return None

            data = r.json()
            content = data["choices"][0]["message"]["content"]
            return json.loads(content)

        except (json.JSONDecodeError, KeyError) as e:
            log.error("Ошибка парсинга ответа: %s", e)
            return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                log.warning("Попытка %d: %s", attempt + 1, e)
                time.sleep(RETRY_DELAY)
            else:
                log.error("Все попытки исчерпаны: %s", e)
                return None

    return None


def get_unenriched(conn: sqlite3.Connection, limit: int, since: str = None) -> list:
    """Получить статьи без enrichment."""
    sql = """
        SELECT a.id, a.title, a.body_text, a.excerpt
        FROM articles a
        LEFT JOIN article_enrichments e ON e.article_id = a.id
        WHERE e.article_id IS NULL
          AND a.title IS NOT NULL
          AND a.title != ''
    """
    params = []
    if since:
        sql += " AND a.pub_date >= ?"
        params.append(since)
    sql += " ORDER BY a.pub_date DESC LIMIT ?"
    params.append(limit)

    return conn.execute(sql, params).fetchall()


def save_enrichment(conn: sqlite3.Connection, article_id: int, data: dict):
    """Сохранить результат обогащения."""
    keywords = data.get("keywords", [])
    if isinstance(keywords, list):
        keywords_json = json.dumps(keywords, ensure_ascii=False)
    else:
        keywords_json = json.dumps([], ensure_ascii=False)

    conn.execute(
        """INSERT OR REPLACE INTO article_enrichments
           (article_id, summary, meta_description, keywords, quote, quote_author)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            article_id,
            data.get("summary"),
            data.get("meta_description"),
            keywords_json,
            data.get("quote"),
            data.get("quote_author"),
        ),
    )


def run(batch: int = 100, since: str = None):
    """Основной цикл обогащения."""
    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY не задан. Установите переменную окружения.")
        return 0

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = get_unenriched(conn, batch, since)
    total = len(rows)

    if total == 0:
        log.info("Все статьи уже обогащены ✓")
        conn.close()
        return 0

    log.info("Начинаю обогащение: %d статей (модель: %s)", total, MODEL)

    success = 0
    errors = 0

    for i, row in enumerate(rows, 1):
        article_id = row["id"]
        title = row["title"] or ""
        text = row["body_text"] or row["excerpt"] or ""

        if not text.strip():
            log.debug("Пропуск статьи %d — нет текста", article_id)
            continue

        result = _call_openai(title, text)

        if result:
            save_enrichment(conn, article_id, result)
            conn.commit()
            success += 1
            if i % 10 == 0:
                log.info("Прогресс: %d/%d (ок: %d, ошибки: %d)", i, total, success, errors)
        else:
            errors += 1
            log.warning("Не удалось обогатить статью %d: %s", article_id, title[:60])

        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

    conn.close()

    log.info(
        "Готово: %d/%d обогащено, %d ошибок",
        success, total, errors,
    )
    return success


def count_stats():
    """Показать статистику обогащения."""
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    enriched = conn.execute("SELECT COUNT(*) FROM article_enrichments").fetchone()[0]
    conn.close()

    pct = (enriched / total * 100) if total > 0 else 0
    print(f"\nСтатьи:     {total:>8,}")
    print(f"Обогащено:  {enriched:>8,}  ({pct:.1f}%)")
    print(f"Осталось:   {total - enriched:>8,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GPT-обогащение статей total.kz")
    parser.add_argument("--batch", type=int, default=100, help="Количество статей (default: 100)")
    parser.add_argument("--all", action="store_true", help="Обработать все необогащённые")
    parser.add_argument("--since", type=str, help="Только статьи с указанной даты (YYYY-MM-DD)")
    parser.add_argument("--stats", action="store_true", help="Показать статистику")
    args = parser.parse_args()

    if args.stats:
        count_stats()
        sys.exit(0)

    batch = 999999 if args.all else args.batch
    run(batch=batch, since=args.since)
