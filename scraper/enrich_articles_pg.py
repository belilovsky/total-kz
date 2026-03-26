#!/usr/bin/env python3
"""
PostgreSQL article enrichment with async parallel workers (httpx + asyncio).

Processes unenriched articles via GPT-4o-mini with N concurrent workers.
Generates: meta_description, keywords, summary, quote, quote_author.

Usage:
  python scraper/enrich_articles_pg.py                    # all unenriched
  python scraper/enrich_articles_pg.py --batch 500        # 500 articles
  python scraper/enrich_articles_pg.py --since 2026-01-01 # from date
  python scraper/enrich_articles_pg.py --workers 20       # 20 workers
  python scraper/enrich_articles_pg.py --stats            # show stats
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

import httpx
import psycopg2
import psycopg2.extras

# ── Config ──────────────────────────────────────────────
PG_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
MODEL = os.environ.get("ENRICHMENT_MODEL", "gpt-4o-mini")

MAX_RETRIES = 4
BASE_RETRY_DELAY = 2  # seconds — exponential backoff base
CHUNK_SIZE = 1000     # fetch from PG in chunks of 1000

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOG_FILE = DATA_DIR / "enrich_pg.log"

# ── Logging ─────────────────────────────────────────────
DATA_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("enrich_pg")


# ── Prompt builder (same as enrich_articles.py) ────────
def _build_prompt(title: str, text: str) -> str:
    truncated = text[:3000] if text else ""
    return f"""Ты — редактор новостного сайта total.kz (Казахстан). Проанализируй статью и верни JSON.

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


# ── Async OpenAI caller ────────────────────────────────
async def call_openai(
    client: httpx.AsyncClient,
    title: str,
    text: str,
) -> dict | None:
    """Call OpenAI API with retries and exponential backoff."""
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": _build_prompt(title, text)}],
        "temperature": 0.3,
        "max_tokens": 500,
        "response_format": {"type": "json_object"},
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = await client.post(
                "https://api.openai.com/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=45,
            )

            if r.status_code == 429:
                # Rate limited — exponential backoff
                wait = BASE_RETRY_DELAY * (2 ** attempt)
                log.warning("Rate limited (429), waiting %ds (attempt %d/%d)...", wait, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(wait)
                continue

            if r.status_code == 402 or (r.status_code >= 400 and "billing" in r.text.lower()):
                log.error("BILLING ERROR: %s — stopping.", r.text[:300])
                raise BillingError(r.text[:300])

            if r.status_code != 200:
                log.error("OpenAI HTTP %d: %s", r.status_code, r.text[:200])
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                    continue
                return None

            data = r.json()
            content = data["choices"][0]["message"]["content"]
            return json.loads(content)

        except BillingError:
            raise
        except (json.JSONDecodeError, KeyError) as e:
            log.error("Parse error for response: %s", e)
            return None
        except httpx.TimeoutException:
            if attempt < MAX_RETRIES - 1:
                wait = BASE_RETRY_DELAY * (2 ** attempt)
                log.warning("Timeout, retry %d/%d in %ds...", attempt + 1, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
            else:
                log.error("All retries exhausted (timeout)")
                return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = BASE_RETRY_DELAY * (2 ** attempt)
                log.warning("Attempt %d error: %s — retrying in %ds", attempt + 1, e, wait)
                await asyncio.sleep(wait)
            else:
                log.error("All retries exhausted: %s", e)
                return None

    return None


class BillingError(Exception):
    pass


# ── Worker ─────────────────────────────────────────────
async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    client: httpx.AsyncClient,
    results: list,
    counters: dict,
    stop_event: asyncio.Event,
):
    """Async worker: pull articles from queue, enrich, collect results."""
    while not stop_event.is_set():
        try:
            article = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        article_id, title, body_text, excerpt = article
        text = body_text or excerpt or ""
        title = title or ""

        if not text.strip():
            counters["skipped"] += 1
            queue.task_done()
            continue

        try:
            result = await call_openai(client, title, text)
        except BillingError:
            stop_event.set()
            log.error("Worker %d: billing error — signalling all workers to stop.", worker_id)
            queue.task_done()
            break

        if result:
            results.append((article_id, result))
            counters["success"] += 1
        else:
            counters["errors"] += 1
            log.warning("Worker %d: failed article %d: %s", worker_id, article_id, title[:60])

        counters["processed"] += 1
        if counters["processed"] % 500 == 0:
            log.info(
                "Progress: %d/%d processed (success: %d, errors: %d, skipped: %d)",
                counters["processed"],
                counters["total"],
                counters["success"],
                counters["errors"],
                counters["skipped"],
            )

        queue.task_done()


# ── DB helpers ─────────────────────────────────────────
def get_pg_connection():
    """Create a new psycopg2 connection."""
    return psycopg2.connect(PG_URL)


def fetch_unenriched_chunk(conn, limit: int, offset: int, since: str = None) -> list:
    """Fetch a chunk of unenriched articles from PG."""
    sql = """
        SELECT a.id, a.title, a.body_text, a.excerpt
        FROM articles a
        LEFT JOIN article_enrichments ae ON ae.article_id = a.id
        WHERE ae.article_id IS NULL
          AND a.title IS NOT NULL
          AND a.title != ''
    """
    params = []
    if since:
        sql += " AND a.pub_date >= %s"
        params.append(since)
    sql += " ORDER BY a.pub_date DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def save_results_batch(conn, results: list):
    """Batch insert enrichment results into PG."""
    if not results:
        return

    sql = """
        INSERT INTO article_enrichments
            (article_id, summary, meta_description, keywords, quote, quote_author)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (article_id) DO NOTHING
    """
    rows = []
    for article_id, data in results:
        keywords = data.get("keywords", [])
        if not isinstance(keywords, list):
            keywords = []
        rows.append((
            article_id,
            data.get("summary"),
            data.get("meta_description"),
            psycopg2.extras.Json(keywords),
            data.get("quote"),
            data.get("quote_author"),
        ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
    conn.commit()
    log.info("Saved batch of %d enrichments to PG.", len(rows))


# ── Main async loop ────────────────────────────────────
async def run_enrichment(batch: int, since: str = None, num_workers: int = 10):
    """Main enrichment loop with async workers."""
    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY is not set.")
        return

    conn = get_pg_connection()
    start_time = time.time()

    # Count total unenriched
    count_sql = """
        SELECT COUNT(*)
        FROM articles a
        LEFT JOIN article_enrichments ae ON ae.article_id = a.id
        WHERE ae.article_id IS NULL
          AND a.title IS NOT NULL
          AND a.title != ''
    """
    count_params = []
    if since:
        count_sql = count_sql.replace(
            "AND a.title != ''",
            "AND a.title != '' AND a.pub_date >= %s"
        )
        count_params.append(since)

    with conn.cursor() as cur:
        cur.execute(count_sql, count_params)
        available = cur.fetchone()[0]

    to_process = min(batch, available) if batch else available
    log.info(
        "Found %d unenriched articles, will process %d (model: %s, workers: %d)",
        available, to_process, MODEL, num_workers,
    )

    if to_process == 0:
        log.info("All articles already enriched ✓")
        conn.close()
        return

    counters = {"processed": 0, "success": 0, "errors": 0, "skipped": 0, "total": to_process}
    stop_event = asyncio.Event()

    # Process in chunks to minimize memory
    offset = 0
    remaining = to_process

    async with httpx.AsyncClient() as client:
        while remaining > 0 and not stop_event.is_set():
            chunk_size = min(CHUNK_SIZE, remaining)
            rows = fetch_unenriched_chunk(conn, chunk_size, 0, since)
            # offset=0 because processed articles are no longer unenriched

            if not rows:
                log.info("No more unenriched articles found.")
                break

            # Fill the queue
            queue = asyncio.Queue()
            for row in rows:
                queue.put_nowait(row)

            results = []

            # Spawn workers
            workers = [
                asyncio.create_task(
                    worker(i, queue, client, results, counters, stop_event)
                )
                for i in range(min(num_workers, len(rows)))
            ]

            await asyncio.gather(*workers)

            # Save results to PG
            save_results_batch(conn, results)

            remaining -= len(rows)
            log.info(
                "Chunk done: %d/%d total processed so far.",
                counters["processed"], counters["total"],
            )

    conn.close()

    elapsed = time.time() - start_time
    # Estimate cost: ~1600 chars input ≈ ~500 tokens, 500 output tokens
    # gpt-4o-mini: $0.15/1M input + $0.60/1M output
    est_input_cost = counters["success"] * 500 * 0.15 / 1_000_000
    est_output_cost = counters["success"] * 200 * 0.60 / 1_000_000
    est_cost = est_input_cost + est_output_cost

    log.info("=" * 60)
    log.info("ENRICHMENT COMPLETE")
    log.info("  Total processed: %d", counters["processed"])
    log.info("  Success:         %d", counters["success"])
    log.info("  Errors:          %d", counters["errors"])
    log.info("  Skipped:         %d", counters["skipped"])
    log.info("  Time:            %.1fs (%.1f articles/sec)", elapsed, counters["success"] / max(elapsed, 0.1))
    log.info("  Est. cost:       $%.4f", est_cost)
    log.info("=" * 60)


def show_stats():
    """Show enrichment statistics from PG."""
    conn = get_pg_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM articles")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM article_enrichments")
        enriched = cur.fetchone()[0]
    conn.close()

    pct = (enriched / total * 100) if total > 0 else 0
    print(f"\nArticles:    {total:>8,}")
    print(f"Enriched:    {enriched:>8,}  ({pct:.1f}%)")
    print(f"Remaining:   {total - enriched:>8,}")


# ── CLI ────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PG article enrichment with async workers (total.kz)")
    parser.add_argument("--batch", type=int, default=0, help="Number of articles to process (default: all)")
    parser.add_argument("--since", type=str, help="Only articles from this date (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent workers (default: 10)")
    parser.add_argument("--stats", action="store_true", help="Show enrichment statistics")
    args = parser.parse_args()

    if args.stats:
        show_stats()
        sys.exit(0)

    batch_size = args.batch if args.batch > 0 else 999_999_999
    asyncio.run(run_enrichment(batch=batch_size, since=args.since, num_workers=args.workers))
