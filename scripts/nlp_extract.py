#!/usr/bin/env python3
"""
GPT-powered NLP extraction pipeline for Total.kz articles.

Extracts: sentiment, importance, topics, key_facts, events, money_mentions,
laws_mentioned, geo_focus, related_context, and Kazakh translations.

Usage:
  python scripts/nlp_extract.py                    # process all unprocessed
  python scripts/nlp_extract.py --batch 100        # 100 articles
  python scripts/nlp_extract.py --model gpt-4o     # use gpt-4o
  python scripts/nlp_extract.py --delay 0.3        # 300ms between requests
  python scripts/nlp_extract.py --dry-run           # test without saving
  python scripts/nlp_extract.py --stats             # show processing stats
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
DEFAULT_MODEL = "gpt-4o-mini"

MAX_RETRIES = 4
BASE_RETRY_DELAY = 2
CHUNK_SIZE = 500

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOG_FILE = DATA_DIR / "nlp_extract.log"

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
log = logging.getLogger("nlp_extract")

SYSTEM_PROMPT = "Ты — AI-аналитик казахстанского новостного портала Total.kz. Извлекай точные структурированные данные из новостей."


def _build_prompt(title: str, text: str) -> str:
    truncated = text[:3000] if text else ""
    return f"""Проанализируй казахстанскую новостную статью и извлеки структурированные данные.

Заголовок: {title}
Текст: {truncated}

Ответ строго JSON:
{{
  "sentiment": "positive|negative|neutral",
  "sentiment_score": 0.0,
  "importance": 3,
  "geo_focus": "основной город или область (если про Казахстан)",
  "topics": ["тема1", "тема2", "тема3"],
  "key_facts": ["факт1", "факт2", "факт3"],
  "events": [{{"name": "событие", "date": "дата", "type": "тип"}}],
  "money_mentions": [{{"amount": "сумма", "context": "контекст"}}],
  "laws_mentioned": [{{"name": "закон/кодекс", "article": "статья"}}],
  "related_context": "предыстория события в 1-2 предложениях",
  "topics_kz": ["тақырып1", "тақырып2"],
  "key_facts_kz": ["факт1 қазақша", "факт2 қазақша"]
}}

Правила:
- sentiment_score: float от -1.0 до 1.0 (негативная=-1, позитивная=+1)
- importance: от 1 до 5 (5=чрезвычайно важная, 1=рутинная)
- topics: 3-5 тем на русском (глубокие: коррупция, реформы, не категории сайта)
- key_facts: 3-5 ключевых фактов из статьи
- events, money_mentions, laws_mentioned: пустой массив если нет данных
- topics_kz и key_facts_kz: те же данные на казахском языке"""


class BillingError(Exception):
    pass


# ── Async OpenAI caller ────────────────────────────────
async def call_openai(
    client: httpx.AsyncClient,
    title: str,
    text: str,
    model: str,
) -> dict | None:
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_prompt(title, text)},
        ],
        "temperature": 0.2,
        "max_tokens": 1200,
        "response_format": {"type": "json_object"},
    }

    for attempt in range(MAX_RETRIES):
        try:
            r = await client.post(
                "https://api.openai.com/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=60,
            )

            if r.status_code == 429:
                wait = BASE_RETRY_DELAY * (2 ** attempt)
                log.warning("Rate limited (429), waiting %ds (attempt %d/%d)...", wait, attempt + 1, MAX_RETRIES)
                await asyncio.sleep(wait)
                continue

            if r.status_code == 402 or (r.status_code >= 400 and "billing" in r.text.lower()):
                log.error("BILLING ERROR: %s — stopping.", r.text[:300])
                raise BillingError(r.text[:300])

            if r.status_code == 401:
                log.error("AUTH ERROR (401): Invalid API key.")
                raise BillingError("Invalid API key (401)")

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
            log.error("Parse error: %s", e)
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


def _validate_result(data: dict) -> dict:
    """Validate and normalize GPT response fields."""
    valid_sentiments = {"positive", "negative", "neutral"}
    sentiment = data.get("sentiment", "neutral")
    if sentiment not in valid_sentiments:
        sentiment = "neutral"

    score = data.get("sentiment_score", 0.0)
    try:
        score = float(score)
        score = max(-1.0, min(1.0, score))
    except (TypeError, ValueError):
        score = 0.0

    importance = data.get("importance", 3)
    try:
        importance = int(importance)
        importance = max(1, min(5, importance))
    except (TypeError, ValueError):
        importance = 3

    return {
        "sentiment": sentiment,
        "sentiment_score": score,
        "importance": importance,
        "geo_focus": str(data.get("geo_focus", "") or "")[:200],
        "topics": data.get("topics") if isinstance(data.get("topics"), list) else [],
        "key_facts": data.get("key_facts") if isinstance(data.get("key_facts"), list) else [],
        "events": data.get("events") if isinstance(data.get("events"), list) else [],
        "money_mentions": data.get("money_mentions") if isinstance(data.get("money_mentions"), list) else [],
        "laws_mentioned": data.get("laws_mentioned") if isinstance(data.get("laws_mentioned"), list) else [],
        "related_context": str(data.get("related_context", "") or "")[:1000],
        "topics_kz": data.get("topics_kz") if isinstance(data.get("topics_kz"), list) else [],
        "key_facts_kz": data.get("key_facts_kz") if isinstance(data.get("key_facts_kz"), list) else [],
    }


def _save_batch(conn, results: list, model: str):
    """Save a batch of NLP results to PostgreSQL."""
    if not results:
        return
    cur = conn.cursor()
    for article_id, data in results:
        v = _validate_result(data)
        cur.execute("""
            INSERT INTO article_nlp (
                article_id, sentiment, sentiment_score, importance, geo_focus,
                topics, key_facts, events, money_mentions, laws_mentioned,
                related_context, topics_kz, key_facts_kz, model
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (article_id) DO UPDATE SET
                sentiment = EXCLUDED.sentiment,
                sentiment_score = EXCLUDED.sentiment_score,
                importance = EXCLUDED.importance,
                geo_focus = EXCLUDED.geo_focus,
                topics = EXCLUDED.topics,
                key_facts = EXCLUDED.key_facts,
                events = EXCLUDED.events,
                money_mentions = EXCLUDED.money_mentions,
                laws_mentioned = EXCLUDED.laws_mentioned,
                related_context = EXCLUDED.related_context,
                topics_kz = EXCLUDED.topics_kz,
                key_facts_kz = EXCLUDED.key_facts_kz,
                processed_at = NOW(),
                model = EXCLUDED.model
        """, (
            article_id, v["sentiment"], v["sentiment_score"], v["importance"], v["geo_focus"],
            json.dumps(v["topics"], ensure_ascii=False),
            json.dumps(v["key_facts"], ensure_ascii=False),
            json.dumps(v["events"], ensure_ascii=False),
            json.dumps(v["money_mentions"], ensure_ascii=False),
            json.dumps(v["laws_mentioned"], ensure_ascii=False),
            v["related_context"],
            json.dumps(v["topics_kz"], ensure_ascii=False),
            json.dumps(v["key_facts_kz"], ensure_ascii=False),
            model,
        ))
    conn.commit()
    cur.close()


async def process_articles(articles: list, model: str, delay: float, dry_run: bool):
    """Process articles sequentially with configurable delay."""
    results = []
    errors = 0
    processed = 0

    async with httpx.AsyncClient() as client:
        for i, art in enumerate(articles):
            article_id = art["id"]
            title = art["title"] or ""
            text = art["body_text"] or ""

            if not title and not text:
                log.warning("Skipping article %d — no title or text", article_id)
                continue

            try:
                data = await call_openai(client, title, text, model)
            except BillingError:
                log.error("Billing error — stopping processing.")
                break

            if data:
                results.append((article_id, data))
                processed += 1
                if processed % 10 == 0:
                    log.info("Processed %d/%d articles (errors: %d)", processed, len(articles), errors)
                # Save every 50 articles to avoid data loss
                if not dry_run and len(results) >= 50:
                    conn = psycopg2.connect(PG_URL)
                    try:
                        _save_batch(conn, results, model)
                        log.info("Intermediate save: %d results", len(results))
                        results = []
                    finally:
                        conn.close()
            else:
                errors += 1
                log.warning("Failed to process article %d", article_id)

            if delay > 0 and i < len(articles) - 1:
                await asyncio.sleep(delay)

    # Save remaining results
    if dry_run:
        log.info("DRY RUN — would save %d results (skipped)", len(results))
        for aid, data in results[:3]:
            log.info("  Article %d: sentiment=%s, importance=%d, topics=%s",
                     aid, data.get("sentiment"), data.get("importance", 0),
                     data.get("topics", [])[:3])
    elif results:
        conn = psycopg2.connect(PG_URL)
        try:
            _save_batch(conn, results, model)
            log.info("Final save: %d results", len(results))
        finally:
            conn.close()

    return processed, errors


def show_stats():
    """Show NLP processing statistics."""
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM articles")
    total_articles = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM article_nlp")
    processed = cur.fetchone()[0]

    cur.execute("SELECT sentiment, COUNT(*) FROM article_nlp GROUP BY sentiment ORDER BY COUNT(*) DESC")
    sentiments = cur.fetchall()

    cur.execute("SELECT importance, COUNT(*) FROM article_nlp GROUP BY importance ORDER BY importance")
    importance_dist = cur.fetchall()

    cur.execute("SELECT model, COUNT(*) FROM article_nlp GROUP BY model")
    models = cur.fetchall()

    cur.execute("SELECT MIN(processed_at), MAX(processed_at) FROM article_nlp")
    dates = cur.fetchone()

    cur.close()
    conn.close()

    print(f"\n{'='*50}")
    print(f"NLP Processing Stats")
    print(f"{'='*50}")
    print(f"Total articles:     {total_articles:,}")
    print(f"NLP processed:      {processed:,}")
    print(f"Remaining:          {total_articles - processed:,}")
    print(f"Coverage:           {processed/total_articles*100:.1f}%" if total_articles else "N/A")
    print(f"\nSentiment distribution:")
    for s, c in sentiments:
        print(f"  {s:12s} {c:,}")
    print(f"\nImportance distribution:")
    for imp, c in importance_dist:
        print(f"  Level {imp}: {c:,}")
    print(f"\nModels used:")
    for m, c in models:
        print(f"  {m}: {c:,}")
    if dates[0]:
        print(f"\nFirst processed: {dates[0]}")
        print(f"Last processed:  {dates[1]}")
    print()


def main():
    parser = argparse.ArgumentParser(description="GPT NLP extraction for articles")
    parser.add_argument("--batch", type=int, default=0, help="Max articles to process (0=all)")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL, help="OpenAI model")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests (seconds)")
    parser.add_argument("--dry-run", action="store_true", help="Process but don't save")
    parser.add_argument("--stats", action="store_true", help="Show processing stats")
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set. Export it before running.")
        sys.exit(1)

    log.info("NLP extraction starting (model=%s, batch=%s, delay=%.1fs, dry_run=%s)",
             args.model, args.batch or "all", args.delay, args.dry_run)

    # Fetch unprocessed articles
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    limit_clause = f"LIMIT {args.batch}" if args.batch > 0 else f"LIMIT {CHUNK_SIZE}"

    cur.execute(f"""
        SELECT a.id, a.title, a.body_text
        FROM articles a
        LEFT JOIN article_nlp n ON a.id = n.article_id
        WHERE n.article_id IS NULL
          AND a.body_text IS NOT NULL
          AND a.body_text != ''
        ORDER BY a.pub_date DESC
        {limit_clause}
    """)
    articles = cur.fetchall()
    cur.close()
    conn.close()

    if not articles:
        log.info("No unprocessed articles found.")
        return

    log.info("Found %d articles to process", len(articles))

    t0 = time.time()
    processed, errors = asyncio.run(
        process_articles(articles, args.model, args.delay, args.dry_run)
    )
    elapsed = time.time() - t0

    log.info("Done: %d processed, %d errors in %.1fs (%.1f articles/min)",
             processed, errors, elapsed, processed / elapsed * 60 if elapsed > 0 else 0)


if __name__ == "__main__":
    main()
