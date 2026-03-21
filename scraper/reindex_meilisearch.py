"""Full reindex of all articles into Meilisearch.

Usage: python -m scraper.reindex_meilisearch
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.search_engine import setup_index, reindex_all

if __name__ == "__main__":
    print("Setting up Meilisearch index...")
    setup_index()
    print("Reindexing all articles...")
    reindex_all()
    print("Done.")
