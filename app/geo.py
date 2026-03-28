"""Geo-personalization module for total.kz.

Detects user region from Cloudflare / proxy headers, returns
region metadata (city, weather query, entity names for article matching).
"""

import logging
from fastapi import Request
from app import db_backend as db

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
#  REGION MAP — region_code → metadata
# ══════════════════════════════════════════════

REGION_MAP: dict[str, dict] = {
    "almaty_city": {
        "city": "Алматы",
        "city_kz": "Алматы",
        "weather_query": "Almaty",
        "entity_names": ["Алматы"],
    },
    "astana_city": {
        "city": "Астана",
        "city_kz": "Астана",
        "weather_query": "Astana",
        "entity_names": ["Астана"],
    },
    "shymkent_city": {
        "city": "Шымкент",
        "city_kz": "Шымкент",
        "weather_query": "Shymkent",
        "entity_names": ["Шымкент"],
    },
    "almaty_obl": {
        "city": "Талдыкорган",
        "city_kz": "Талдықорған",
        "weather_query": "Taldykorgan",
        "entity_names": ["Алматинская область"],
    },
    "akmola": {
        "city": "Кокшетау",
        "city_kz": "Көкшетау",
        "weather_query": "Kokshetau",
        "entity_names": ["Акмолинская область"],
    },
    "aktobe": {
        "city": "Актобе",
        "city_kz": "Ақтөбе",
        "weather_query": "Aktobe",
        "entity_names": ["Актюбинская область", "Актобе"],
    },
    "atyrau": {
        "city": "Атырау",
        "city_kz": "Атырау",
        "weather_query": "Atyrau",
        "entity_names": ["Атырауская область", "Атырау"],
    },
    "vko": {
        "city": "Усть-Каменогорск",
        "city_kz": "Өскемен",
        "weather_query": "Ust-Kamenogorsk",
        "entity_names": ["Восточно-Казахстанская область", "ВКО"],
    },
    "zko": {
        "city": "Уральск",
        "city_kz": "Орал",
        "weather_query": "Uralsk,Kazakhstan",
        "entity_names": ["Западно-Казахстанская область", "ЗКО"],
    },
    "zhambyl": {
        "city": "Тараз",
        "city_kz": "Тараз",
        "weather_query": "Taraz",
        "entity_names": ["Жамбылская область", "Тараз"],
    },
    "karaganda": {
        "city": "Караганда",
        "city_kz": "Қарағанды",
        "weather_query": "Karaganda",
        "entity_names": ["Карагандинская область", "Караганда"],
    },
    "kostanay": {
        "city": "Костанай",
        "city_kz": "Қостанай",
        "weather_query": "Kostanay",
        "entity_names": ["Костанайская область", "Костанай"],
    },
    "kyzylorda": {
        "city": "Кызылорда",
        "city_kz": "Қызылорда",
        "weather_query": "Kyzylorda",
        "entity_names": ["Кызылординская область", "Кызылорда"],
    },
    "mangystau": {
        "city": "Актау",
        "city_kz": "Ақтау",
        "weather_query": "Aktau,Kazakhstan",
        "entity_names": ["Мангистауская область", "Актау"],
    },
    "pavlodar": {
        "city": "Павлодар",
        "city_kz": "Павлодар",
        "weather_query": "Pavlodar",
        "entity_names": ["Павлодарская область", "Павлодар"],
    },
    "sko": {
        "city": "Петропавловск",
        "city_kz": "Петропавл",
        "weather_query": "Petropavlovsk,Kazakhstan",
        "entity_names": ["Северо-Казахстанская область", "СКО"],
    },
    "turkestan": {
        "city": "Туркестан",
        "city_kz": "Түркістан",
        "weather_query": "Turkestan,Kazakhstan",
        "entity_names": ["Туркестанская область", "Туркестан"],
    },
    "abay": {
        "city": "Семей",
        "city_kz": "Семей",
        "weather_query": "Semey",
        "entity_names": ["Абай область", "Семей"],
    },
    "zhetisu": {
        "city": "Талдыкорган",
        "city_kz": "Талдықорған",
        "weather_query": "Taldykorgan",
        "entity_names": ["Жетісу область", "Талдыкорган"],
    },
    "ulytau": {
        "city": "Жезказган",
        "city_kz": "Жезқазған",
        "weather_query": "Jezkazgan",
        "entity_names": ["Ұлытау область", "Жезказган"],
    },
}

# Cloudflare CF-IPCity header value → region code
_CITY_TO_REGION: dict[str, str] = {
    "almaty": "almaty_city",
    "алматы": "almaty_city",
    "astana": "astana_city",
    "nur-sultan": "astana_city",
    "астана": "astana_city",
    "shymkent": "shymkent_city",
    "шымкент": "shymkent_city",
    "aktobe": "aktobe",
    "актобе": "aktobe",
    "atyrau": "atyrau",
    "атырау": "atyrau",
    "karaganda": "karaganda",
    "караганда": "karaganda",
    "kostanay": "kostanay",
    "костанай": "kostanay",
    "kyzylorda": "kyzylorda",
    "кызылорда": "kyzylorda",
    "aktau": "mangystau",
    "актау": "mangystau",
    "pavlodar": "pavlodar",
    "павлодар": "pavlodar",
    "petropavlovsk": "sko",
    "петропавловск": "sko",
    "taraz": "zhambyl",
    "тараз": "zhambyl",
    "ust-kamenogorsk": "vko",
    "oskemen": "vko",
    "усть-каменогорск": "vko",
    "uralsk": "zko",
    "oral": "zko",
    "уральск": "zko",
    "turkestan": "turkestan",
    "туркестан": "turkestan",
    "semey": "abay",
    "семей": "abay",
    "taldykorgan": "zhetisu",
    "талдыкорган": "zhetisu",
    "kokshetau": "akmola",
    "кокшетау": "akmola",
    "jezkazgan": "ulytau",
    "жезказган": "ulytau",
}

DEFAULT_REGION = "astana_city"


def detect_region(request: Request) -> dict:
    """Detect user's region from request headers.

    Priority:
    1. CF-IPCity header (Cloudflare)
    2. Fallback to default (Astana)

    Returns a region dict with city, weather_query, entity_names, and region code.
    """
    # Try Cloudflare city header
    cf_city = (request.headers.get("cf-ipcity") or "").strip().lower()
    if cf_city:
        region_code = _CITY_TO_REGION.get(cf_city)
        if region_code:
            region = REGION_MAP[region_code].copy()
            region["code"] = region_code
            return region

    # Default fallback
    region = REGION_MAP[DEFAULT_REGION].copy()
    region["code"] = DEFAULT_REGION
    return region


def get_regional_articles(entity_names: list[str], limit: int = 8) -> list[dict]:
    """Fetch articles linked to region entities (locations).

    Uses raw SQL via db_backend to query article_entities + entities tables.
    Matches on both e.name and e.normalized (lowered) for robustness.
    """
    if not entity_names:
        return []
    try:
        lowered = [n.lower() for n in entity_names]
        placeholders = ",".join(["%s"] * len(lowered))
        sql = f"""
            SELECT DISTINCT a.id, a.title, a.excerpt, a.main_image, a.pub_date,
                   a.sub_category, a.url, a.thumbnail
            FROM articles a
            JOIN article_entities ae ON a.id = ae.article_id
            JOIN entities e ON ae.entity_id = e.id
            WHERE e.entity_type = 'location'
              AND LOWER(e.name) IN ({placeholders})
              AND a.status = 'published'
            ORDER BY a.pub_date DESC
            LIMIT %s
        """
        rows = db.execute_raw_many(sql, tuple(lowered) + (limit,))
        return rows
    except Exception:
        logger.exception("Error fetching regional articles")
        return []


def get_region_label(region: dict, lang: str = "ru") -> str:
    """Return human-readable region city name."""
    if lang == "kz":
        return region.get("city_kz", region.get("city", "Астана"))
    return region.get("city", "Астана")
