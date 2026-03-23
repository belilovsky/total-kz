#!/bin/sh
# Cron wrapper: reindex Meilisearch after new articles imported
# Runs full reindex — safe because Meilisearch handles upserts

echo "$(date '+%Y-%m-%d %H:%M:%S') — Starting Meilisearch reindex"
cd /app && python -m scraper.reindex_meilisearch 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') — Finished reindex"
