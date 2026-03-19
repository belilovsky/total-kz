"""Search Analytics module — GSC data processor for the dashboard."""

import json
from pathlib import Path

# Pre-computed GSC analysis file — check both possible locations
_parent = Path(__file__).parent
DATA_FILE = _parent / "gsc_analysis.json"
if not DATA_FILE.exists():
    DATA_FILE = _parent.parent / "gsc_analysis.json"


def get_search_data() -> dict:
    """Load pre-computed GSC analysis data."""
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "totals": {"clicks": 0, "impressions": 0, "avg_ctr": 0, "avg_position": 0},
        "brand_split": {"brand_clicks": 0, "nonbrand_clicks": 0, "brand_impressions": 0, "nonbrand_impressions": 0},
        "growth_opportunities": [],
        "quick_wins": [],
        "position_distribution": {},
        "category_stats": {},
        "top_queries": [],
        "top_pages": [],
        "devices": [],
        "countries": [],
        "daily_trends": [],
        "query_page_combos": [],
    }
