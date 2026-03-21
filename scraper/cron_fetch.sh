#!/bin/sh
# Cron wrapper: fetch latest articles every 2 hours
# Runs inside the cron container, shares /app/data volume with app

echo "$(date '+%Y-%m-%d %H:%M:%S') — Starting fetch_latest.py"
cd /app && python scraper/fetch_latest.py 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') — Finished"
