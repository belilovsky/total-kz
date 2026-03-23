#!/bin/sh
# Cron wrapper: enrich new articles every 4 hours
# Обогащает до 50 новых статей за запуск через GPT

echo "$(date '+%Y-%m-%d %H:%M:%S') — Starting enrich_articles.py"
cd /app && python scraper/enrich_articles.py --batch 50 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') — Finished enrichment"
