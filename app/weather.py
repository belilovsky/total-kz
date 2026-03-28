"""
Weather widget for total.kz header.
Uses wttr.in (free, no API key, JSON output).
Caches 30 min to avoid rate-limiting.
Shows weather for major Kazakhstan cities.
"""
import httpx
import time
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

_cache: dict = {}
CACHE_TTL = 1800  # 30 minutes

# Kazakhstan is UTC+5 (Astana/most regions) or UTC+6 (Almaty)
_KZ_UTC_OFFSET = timedelta(hours=5)

# Storm weather codes from wttr.in
_STORM_CODES = {200, 386, 389}
_STORM_WIND_THRESHOLD = 50  # km/h

CITIES = [
    {"name": "Астана", "query": "Astana"},
    {"name": "Алматы", "query": "Almaty"},
    {"name": "Шымкент", "query": "Shymkent"},
    {"name": "Актау", "query": "Aktau,Kazakhstan"},
    {"name": "Караганда", "query": "Karaganda"},
    {"name": "Актобе", "query": "Aktobe"},
]


def _is_night() -> bool:
    """Check if it's nighttime in Kazakhstan (22:00-06:00 UTC+5)."""
    kz_now = datetime.now(timezone.utc) + _KZ_UTC_OFFSET
    return kz_now.hour >= 22 or kz_now.hour < 6


def _code_to_emoji(code: int, night: bool = False) -> str:
    """Map wttr.in weather codes to emoji, with night variants."""
    if code == 113:
        return "\U0001f319" if night else "\u2600\ufe0f"  # 🌙 / ☀️
    elif code in (116,):
        return "\U0001f319" if night else "\u26c5"  # 🌙 / ⛅
    elif code in (119, 122):
        return "\u2601\ufe0f"  # ☁️
    elif code in (143, 248, 260):
        return "\U0001f32b\ufe0f"  # 🌫️
    elif code in (176, 263, 266, 293, 296, 299, 302, 305, 308, 311, 314, 353, 356, 359):
        return "\U0001f327\ufe0f"  # 🌧️
    elif code in (179, 182, 185, 227, 230, 281, 284, 317, 320, 323, 326, 329, 332, 335, 338, 350, 362, 365, 368, 371, 374, 377, 392, 395):
        return "\U0001f328\ufe0f"  # 🌨️
    elif code in _STORM_CODES:
        return "\u26c8\ufe0f"  # ⛈️
    else:
        return "\U0001f324\ufe0f"  # 🌤️


def _fetch_weather(query: str) -> dict | None:
    """Fetch current weather from wttr.in with extended data."""
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
        feels_like = current.get("FeelsLikeC", temp_c)
        humidity = current.get("humidity", "?")
        wind_kmh = current.get("windspeedKmph", "0")
        wind_dir = current.get("winddir16Point", "")

        # Weather description in Russian if available
        desc_list = current.get("lang_ru", [])
        desc = desc_list[0].get("value", "") if desc_list else current.get("weatherDesc", [{}])[0].get("value", "")

        # Weather code -> emoji (with night awareness)
        code = int(current.get("weatherCode", 0))
        night = _is_night()
        emoji = _code_to_emoji(code, night)

        # Storm warning: storm weather code OR wind > 50 km/h
        try:
            wind_int = int(wind_kmh)
        except (ValueError, TypeError):
            wind_int = 0
        storm_warning = code in _STORM_CODES or wind_int > _STORM_WIND_THRESHOLD

        return {
            "temp": temp_c,
            "desc": desc,
            "emoji": emoji,
            "feels_like": feels_like,
            "humidity": humidity,
            "wind": wind_kmh,
            "wind_dir": wind_dir,
            "storm_warning": storm_warning,
        }
    except Exception as e:
        logger.warning(f"Weather fetch failed for {query}: {e}")
        return None


def get_weather(city_name: str | None = None, city_query: str | None = None) -> list[dict]:
    """Return weather data for cities. Uses cache.

    If city_name and city_query are provided, fetch weather for that city first
    and place it at the front of the results list.
    """
    now = time.time()
    results = []

    # If a specific city is requested, fetch it first
    if city_name and city_query:
        key = city_query
        cached = _cache.get(key)
        if cached and (now - cached["ts"]) < CACHE_TTL:
            results.append({"city": city_name, **cached["data"]})
        else:
            data = _fetch_weather(city_query)
            if data:
                _cache[key] = {"ts": now, "data": data}
                results.append({"city": city_name, **data})
            elif cached:
                results.append({"city": city_name, **cached["data"]})

    # Then fetch the standard cities (skip if already fetched above)
    seen_queries = {city_query} if city_query else set()
    for city in CITIES:
        if city["query"] in seen_queries:
            continue
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
            results.append({"city": city["name"], **cached["data"]})
    return results
