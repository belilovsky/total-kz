#!/usr/bin/env python3
"""Refresh GSC data for total.kz dashboard.

This script:
1. Fetches fresh data from Google Search Console (6 API calls)
2. Processes it into gsc_analysis.json with period comparison
3. Reports significant position changes

Designed to run as a cron job via: python refresh_gsc.py --output /path/to/gsc_analysis.json
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path


async def call_tool(source_id, tool_name, arguments):
    """Call an external tool via the external-tool CLI."""
    proc = await asyncio.create_subprocess_exec(
        "external-tool", "call", json.dumps({
            "source_id": source_id,
            "tool_name": tool_name,
            "arguments": arguments,
        }),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Tool call failed: {stderr.decode()}")
    return json.loads(stdout.decode())


BRAND_KEYWORDS = {"тотал", "total", "total.kz", "тотал кз", "total kz"}
COUNTRY_NAMES = {
    "kaz": "Казахстан", "rus": "Россия", "deu": "Германия",
    "uzb": "Узбекистан", "kgz": "Кыргызстан", "usa": "США",
    "ukr": "Украина", "tur": "Турция", "blr": "Беларусь",
    "gbr": "Великобритания", "fra": "Франция", "can": "Канада",
    "isr": "Израиль", "ita": "Италия", "nld": "Нидерланды",
    "pol": "Польша", "cze": "Чехия", "esp": "Испания",
    "aze": "Азербайджан", "tjk": "Таджикистан", "geo": "Грузия",
    "mda": "Молдова", "lva": "Латвия", "est": "Эстония",
    "ltu": "Литва", "ind": "Индия", "chn": "Китай",
    "arm": "Армения", "mng": "Монголия",
}

SITE_URL = "https://total.kz/"
SOURCE_ID = "google_search_console__pipedream"
TOOL_NAME = "google_search_console-retrieve-site-performance-data"


def is_brand(query):
    q = query.lower().strip()
    return any(bk in q for bk in BRAND_KEYWORDS)


def pct_change(current, previous):
    if previous == 0:
        return None
    return round((current - previous) / previous * 100, 1)


async def fetch_gsc_data():
    """Fetch all 6 GSC datasets."""
    today = datetime.utcnow().date()
    # GSC data has 3-day delay
    end_current = today - timedelta(days=3)
    start_current = end_current - timedelta(days=89)
    end_previous = start_current - timedelta(days=1)
    start_previous = end_previous - timedelta(days=89)

    current_start = start_current.isoformat()
    current_end = end_current.isoformat()
    prev_start = start_previous.isoformat()
    prev_end = end_previous.isoformat()

    print(f"Current period: {current_start} — {current_end}")
    print(f"Previous period: {prev_start} — {prev_end}")

    base_args = {"siteUrl": SITE_URL, "searchType": "web"}

    calls = {
        "current_queries": {**base_args, "startDate": current_start, "endDate": current_end,
                           "dimensions": ["query"], "rowLimit": 500},
        "previous_queries": {**base_args, "startDate": prev_start, "endDate": prev_end,
                            "dimensions": ["query"], "rowLimit": 500},
        "top_pages": {**base_args, "startDate": current_start, "endDate": current_end,
                      "dimensions": ["page"], "rowLimit": 50},
        "devices": {**base_args, "startDate": current_start, "endDate": current_end,
                    "dimensions": ["device"], "rowLimit": 10},
        "countries": {**base_args, "startDate": current_start, "endDate": current_end,
                      "dimensions": ["country"], "rowLimit": 20},
        "daily": {**base_args, "startDate": current_start, "endDate": current_end,
                  "dimensions": ["date"], "rowLimit": 500},
    }

    results = {}
    for name, args in calls.items():
        print(f"  Fetching {name}...")
        try:
            resp = await call_tool(SOURCE_ID, TOOL_NAME, args)
            rows = resp.get("result", resp).get("rows", [])
            results[name] = rows
            print(f"    → {len(rows)} rows")
        except Exception as e:
            print(f"    → ERROR: {e}")
            results[name] = []

    return results, current_start, current_end, prev_start, prev_end


def process_data(data, current_start, current_end, prev_start, prev_end):
    """Process raw GSC data into the analysis JSON format."""
    current_queries = data["current_queries"]
    previous_queries = data["previous_queries"]
    devices = data["devices"]
    countries = data["countries"]
    daily = data["daily"]
    top_pages = data["top_pages"]

    # Totals from devices
    total_clicks = sum(d["clicks"] for d in devices)
    total_impressions = sum(d["impressions"] for d in devices)
    avg_ctr = round(total_clicks / total_impressions * 100, 1) if total_impressions else 0
    total_impr_pos = sum(d["impressions"] * d["position"] for d in devices)
    avg_position = round(total_impr_pos / total_impressions, 1) if total_impressions else 0

    totals = {"clicks": total_clicks, "impressions": total_impressions,
              "avg_ctr": avg_ctr, "avg_position": avg_position}

    # Brand split
    brand_clicks = sum(q["clicks"] for q in current_queries if is_brand(q["keys"][0]))
    brand_impressions = sum(q["impressions"] for q in current_queries if is_brand(q["keys"][0]))

    brand_split = {
        "brand_clicks": brand_clicks,
        "nonbrand_clicks": total_clicks - brand_clicks,
        "brand_impressions": brand_impressions,
        "nonbrand_impressions": total_impressions - brand_impressions
    }

    # Growth opportunities
    growth = []
    for q in current_queries:
        query = q["keys"][0]
        if is_brand(query): continue
        if 5 <= q["position"] <= 20 and q["impressions"] >= 500:
            extra = int(q["impressions"] * 0.15 - q["clicks"])
            if extra > 0:
                growth.append({"query": query, "clicks": q["clicks"], "impressions": q["impressions"],
                               "ctr": q["ctr"], "position": q["position"], "potential_extra_clicks": extra})
    growth.sort(key=lambda x: x["potential_extra_clicks"], reverse=True)

    # Quick wins
    quick_wins = []
    for q in current_queries:
        query = q["keys"][0]
        if is_brand(query): continue
        ctr_pct = q["ctr"] * 100
        if q["position"] <= 10 and ctr_pct < 5 and q["impressions"] >= 200:
            quick_wins.append({"query": query, "clicks": q["clicks"], "impressions": q["impressions"],
                              "ctr": round(ctr_pct, 1), "position": round(q["position"], 1)})
    quick_wins.sort(key=lambda x: x["impressions"], reverse=True)

    # Position distribution
    pos_buckets = {"Топ 3 (позиции 1-3)": 0, "Топ 10 (позиции 4-10)": 0,
                   "Топ 20 (позиции 11-20)": 0, "За топ 20 (20+)": 0}
    for q in current_queries:
        p = q["position"]
        if p <= 3: pos_buckets["Топ 3 (позиции 1-3)"] += 1
        elif p <= 10: pos_buckets["Топ 10 (позиции 4-10)"] += 1
        elif p <= 20: pos_buckets["Топ 20 (позиции 11-20)"] += 1
        else: pos_buckets["За топ 20 (20+)"] += 1

    # Top queries
    top_queries = [{"query": q["keys"][0], "clicks": q["clicks"], "impressions": q["impressions"],
                    "ctr": round(q["ctr"] * 100, 1), "position": round(q["position"], 1)}
                   for q in current_queries[:50]]

    # Top pages
    top_pages_list = [{"url": p["keys"][0], "slug": p["keys"][0].replace("https://total.kz/", "").strip("/"),
                       "clicks": p["clicks"], "impressions": p["impressions"],
                       "ctr": round(p["ctr"] * 100, 1), "position": round(p["position"], 1)}
                      for p in top_pages[:50]]

    # Devices
    devices_list = [{"device": d["keys"][0], "clicks": d["clicks"], "impressions": d["impressions"],
                     "ctr": round(d["ctr"] * 100, 1), "position": round(d["position"], 1)}
                    for d in devices]

    # Countries
    countries_list = [{"country": c["keys"][0].lower(),
                       "country_name": COUNTRY_NAMES.get(c["keys"][0].lower(), c["keys"][0].upper()),
                       "clicks": c["clicks"], "impressions": c["impressions"],
                       "ctr": round(c["ctr"] * 100, 1), "position": round(c["position"], 1)}
                      for c in countries]

    # Daily trends
    daily_trends = [{"date": d["keys"][0], "clicks": d["clicks"], "impressions": d["impressions"],
                     "ctr": round(d["ctr"] * 100, 2), "position": round(d["position"], 1)}
                    for d in daily]

    # Period comparison
    prev_lookup = {q["keys"][0]: q for q in previous_queries}
    prev_clicks = sum(q["clicks"] for q in previous_queries)
    prev_impressions = sum(q["impressions"] for q in previous_queries)
    prev_avg_ctr = round(prev_clicks / prev_impressions * 100, 1) if prev_impressions else 0
    prev_avg_pos = round(sum(q["impressions"] * q["position"] for q in previous_queries) / prev_impressions, 1) if prev_impressions else 0

    top_gainers, top_losers = [], []
    for q in current_queries:
        query = q["keys"][0]
        if query in prev_lookup:
            prev = prev_lookup[query]
            delta = q["clicks"] - prev["clicks"]
            if q["clicks"] >= 20 or prev["clicks"] >= 20:
                entry = {"query": query, "current_clicks": q["clicks"], "previous_clicks": prev["clicks"],
                         "delta_clicks": delta, "current_position": round(q["position"], 1),
                         "previous_position": round(prev["position"], 1),
                         "delta_position": round(q["position"] - prev["position"], 1)}
                if delta > 0: top_gainers.append(entry)
                elif delta < 0: top_losers.append(entry)
    top_gainers.sort(key=lambda x: x["delta_clicks"], reverse=True)
    top_losers.sort(key=lambda x: x["delta_clicks"])

    period_comparison = {
        "current_period": f"{current_start} — {current_end}",
        "previous_period": f"{prev_start} — {prev_end}",
        "current": {"clicks": total_clicks, "impressions": total_impressions,
                    "avg_ctr": avg_ctr, "avg_position": avg_position},
        "previous": {"clicks": prev_clicks, "impressions": prev_impressions,
                    "avg_ctr": prev_avg_ctr, "avg_position": prev_avg_pos},
        "delta": {
            "clicks": total_clicks - prev_clicks,
            "clicks_pct": pct_change(total_clicks, prev_clicks),
            "impressions": total_impressions - prev_impressions,
            "impressions_pct": pct_change(total_impressions, prev_impressions),
            "avg_ctr": round(avg_ctr - prev_avg_ctr, 1),
            "avg_position": round(avg_position - prev_avg_pos, 1)
        },
        "top_gainers": top_gainers[:15],
        "top_losers": top_losers[:15]
    }

    return {
        "totals": totals,
        "brand_split": brand_split,
        "growth_opportunities": growth[:20],
        "quick_wins": quick_wins[:15],
        "position_distribution": pos_buckets,
        "top_queries": top_queries,
        "top_pages": top_pages_list,
        "devices": devices_list,
        "countries": countries_list,
        "daily_trends": daily_trends,
        "query_page_combos": [],
        "period_comparison": period_comparison,
        "updated_at": datetime.utcnow().isoformat() + "Z"
    }


def check_alerts(result, prev_file=None):
    """Check for significant position changes to alert on."""
    alerts = []
    pc = result.get("period_comparison", {})
    delta = pc.get("delta", {})

    # Big traffic changes
    if delta.get("clicks_pct") and abs(delta["clicks_pct"]) > 20:
        direction = "выросли" if delta["clicks_pct"] > 0 else "упали"
        alerts.append(f"Клики {direction} на {abs(delta['clicks_pct'])}% ({delta['clicks']:+,})")

    # Top losers with big drops
    for loser in pc.get("top_losers", [])[:3]:
        if loser["delta_clicks"] < -100:
            alerts.append(f"Запрос \"{loser['query']}\": {loser['delta_clicks']} кликов "
                         f"(позиция {loser['previous_position']} → {loser['current_position']})")

    return alerts


async def main():
    parser = argparse.ArgumentParser(description="Refresh GSC data for total.kz")
    parser.add_argument("--output", default="/home/user/workspace/total-kz/app/gsc_analysis.json",
                       help="Output path for gsc_analysis.json")
    args = parser.parse_args()

    print(f"=== GSC Data Refresh — {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC ===\n")

    data, cs, ce, ps, pe = await fetch_gsc_data()

    # Check if we got enough data
    if not data["devices"]:
        print("ERROR: No device data returned. Aborting.")
        sys.exit(1)

    result = process_data(data, cs, ce, ps, pe)

    # Save
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n=== Summary ===")
    print(f"Клики: {result['totals']['clicks']:,}")
    print(f"Показы: {result['totals']['impressions']:,}")
    print(f"CTR: {result['totals']['avg_ctr']}%")
    print(f"Позиция: {result['totals']['avg_position']}")
    print(f"Файл: {output}")

    # Alerts
    alerts = check_alerts(result)
    if alerts:
        print(f"\n⚠️  ALERTS:")
        for a in alerts:
            print(f"  • {a}")

    return result, alerts


if __name__ == "__main__":
    asyncio.run(main())
