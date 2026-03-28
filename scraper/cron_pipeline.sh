#!/bin/sh
# Unified article processing pipeline
# Runs every 2 hours from the cron container
# Steps: fetch → enrich (PG) → NLP → NER → story-assign → translate → tags → reindex

echo "$(date '+%Y-%m-%d %H:%M:%S') — Pipeline start"

# Step 1: Fetch new articles
echo "$(date '+%Y-%m-%d %H:%M:%S') — [1/7] Fetching new articles..."
cd /app && python scraper/fetch_latest.py 2>&1

# Step 2: Enrich new articles via GPT (PostgreSQL version)
echo "$(date '+%Y-%m-%d %H:%M:%S') — [2/7] Enriching articles (batch 200)..."
cd /app && python scraper/enrich_articles_pg.py --batch 150 2>&1

# Step 2.5: NLP extraction on new articles
echo "$(date '+%Y-%m-%d %H:%M:%S') — [2.5/8] NLP extraction (batch 100)..."
cd /app && python scripts/nlp_extract.py --batch 100 --delay 0.3 2>&1

# Step 3: NER extraction on unprocessed articles
echo "$(date '+%Y-%m-%d %H:%M:%S') — [3/8] NER extraction (batch 500, 2 workers)..."
cd /app && python scripts/extract_entities_pg.py --batch 300 --workers 1 2>&1

# Step 3.5: Auto-assign new articles to stories (entity-based)
echo "$(date '+%Y-%m-%d %H:%M:%S') — [3.5/7] Auto-assigning articles to stories..."
cd /app && python scripts/cluster_stories.py --auto-assign --apply 2>&1

# Step 3.6: Translate new articles to Kazakh
echo "$(date '+%Y-%m-%d %H:%M:%S') — [3.6/7] Translating to Kazakh (batch 50)..."
cd /app && python scripts/translate_articles_kz.py --batch 50 --workers 1 2>&1

# Step 4: Denormalize tags from enrichments
echo "$(date '+%Y-%m-%d %H:%M:%S') — [4/7] Denormalizing tags..."
cd /app && python scripts/extract_entities_pg.py --tags-only 2>&1

# Step 5: Reindex Meilisearch
echo "$(date '+%Y-%m-%d %H:%M:%S') — [5/7] Reindexing Meilisearch..."
cd /app && python scripts/reindex_meilisearch.py 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Pipeline complete"
