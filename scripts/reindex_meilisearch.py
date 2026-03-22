#!/usr/bin/env python3
"""
Full Meilisearch reindex.
Run inside Docker: docker compose exec app python scripts/reindex_meilisearch.py
Or locally with MEILI_URL env var.
"""
import os
import sys
import time

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Override Meilisearch URL if env var is set (for local dev)
meili_url = os.environ.get("MEILI_URL")
if meili_url:
    import app.search_engine as se
    se.MEILI_URL = meili_url

from app.search_engine import setup_index, reindex_all

print("[reindex] Setting up Meilisearch index + settings...")
setup_index()
print("[reindex] Starting full reindex of all articles...")
t0 = time.time()
reindex_all()
elapsed = time.time() - t0
print(f"[reindex] Done in {elapsed:.1f}s")
