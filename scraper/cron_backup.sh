#!/bin/sh
# Cron wrapper: daily database backup at ~4 AM
# SQLite → gzip → ротация (5 последних)

echo "$(date '+%Y-%m-%d %H:%M:%S') — Starting backup"
cd /app && python scraper/backup_db.py --tag daily 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') — Finished backup"
