#!/bin/sh
# Unified article processing pipeline
# Runs every 2 hours from the cron container
# Steps: fetch → enrich (PG) → NER → tags → reindex

echo "$(date '+%Y-%m-%d %H:%M:%S') — Pipeline start"

# Step 1: Fetch new articles
echo "$(date '+%Y-%m-%d %H:%M:%S') — [1/5] Fetching new articles..."
cd /app && python scraper/fetch_latest.py 2>&1

# Step 2: Enrich new articles via GPT (PostgreSQL version)
echo "$(date '+%Y-%m-%d %H:%M:%S') — [2/5] Enriching articles (batch 200)..."
cd /app && python scraper/enrich_articles_pg.py --batch 150 2>&1

# Step 3: NER extraction on unprocessed articles
echo "$(date '+%Y-%m-%d %H:%M:%S') — [3/5] NER extraction (batch 500, 2 workers)..."
cd /app && python scripts/extract_entities_pg.py --batch 300 --workers 1 2>&1

# Step 3.5: Translate new articles to Kazakh
echo "$(date '+%Y-%m-%d %H:%M:%S') — [3.5/6] Translating to Kazakh (batch 50)..."
cd /app && python scripts/translate_articles_kz.py --batch 50 --workers 1 2>&1

# Step 4: Denormalize tags from enrichments
echo "$(date '+%Y-%m-%d %H:%M:%S') — [4/6] Denormalizing tags..."
cd /app && python scripts/extract_entities_pg.py --tags-only 2>&1

# Step 5: Reindex Meilisearch
echo "$(date '+%Y-%m-%d %H:%M:%S') — [5/6] Reindexing Meilisearch..."
cd /app && python scripts/reindex_meilisearch.py 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Pipeline complete"
