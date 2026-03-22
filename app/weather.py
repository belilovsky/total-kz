"""
Weather widget for total.kz header.
Uses wttr.in (free, no API key, JSON output).
Caches 30 min to avoid rate-limiting.
"""
import httpx
import time
import logging

logger = logging.getLogger(__name__)

_cache: dict = {}
CACHE_TTL = 1800  # 30 minutes

CITIES = [
    {"name": "Астана", "query": "Astana"},
    {"name": "Алматы", "query": "Almaty"},
]


def _fetch_weather(query: str) -> dict | None:
    """Fetch current weather from wttr.in."""
    try:
        r = httpx.get(
            f"https://wttr.in/{query}?format=j1&lang=ru",
            timeout=5.0,
            headers={"User-Agent": "total.kz/1.0"},
        )
        r.raise_for_status()
        data = r.json()
        current = data["current_condition"][0]
        temp_c = current.get("temp_C", "?")
        # Weather description in Russian if available
        desc_list = current.get("lang_ru", [])
        desc = desc_list[0].get("value", "") if desc_list else current.get("weatherDesc", [{}])[0].get("value", "")
        # Weather code -> emoji
        code = int(current.get("weatherCode", 0))
        emoji = _code_to_emoji(code)
        return {"temp": temp_c, "desc": desc, "emoji": emoji}
    except Exception as e:
        logger.warning(f"Weather fetch failed for {query}: {e}")
        return None


def _code_to_emoji(code: int) -> str:
    """Map wttr.in weather codes to emoji."""
    if code == 113:
        return "☀️"
    elif code in (116,):
        return "⛅"
    elif code in (119, 122):
        return "☁️"
    elif code in (143, 248, 260):
        return "🌫️"
    elif code in (176, 263, 266, 293, 296, 299, 302, 305, 308, 311, 314, 353, 356, 359):
        return "🌧️"
    elif code in (179, 182, 185, 227, 230, 281, 284, 317, 320, 323, 326, 329, 332, 335, 338, 350, 362, 365, 368, 371, 374, 377, 392, 395):
        return "🌨️"
    elif code in (200, 386, 389):
        return "⛈️"
    else:
        return "🌤️"


def get_weather() -> list[dict]:
    """Return weather data for cities. Uses cache."""
    now = time.time()
    results = []
    for city in CITIES:
        key = city["query"]
        cached = _cache.get(key)
        if cached and (now - cached["ts"]) < CACHE_TTL:
            results.append({"city": city["name"], **cached["data"]})
            continue
        data = _fetch_weather(city["query"])
        if data:
            _cache[key] = {"ts": now, "data": data}
            results.append({"city": city["name"], **data})
        elif cached:
            # Use stale cache on failure
            results.append({"city": city["name"], **cached["data"]})
    return results
