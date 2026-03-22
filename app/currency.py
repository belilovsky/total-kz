"""
Курсы валют НБ РК — кэшированный запрос к nationalbank.kz
Обновляется не чаще раза в час; при ошибке отдаёт последние известные.
"""
import time, logging, xml.etree.ElementTree as ET
from datetime import datetime

import httpx

log = logging.getLogger("currency")

_cache: dict | None = None
_cache_ts: float = 0
_TTL = 3600  # 1 hour

_CODES = ("USD", "EUR", "RUB")
_FLAGS = {"USD": "🇺🇸", "EUR": "🇪🇺", "RUB": "🇷🇺"}

# Fallback if API is down
_FALLBACK = [
    {"code": "USD", "flag": "🇺🇸", "rate": "482.33", "change": "0.00", "direction": ""},
    {"code": "EUR", "flag": "🇪🇺", "rate": "557.57", "change": "0.00", "direction": ""},
    {"code": "RUB", "flag": "🇷🇺", "rate": "5.74",   "change": "0.00", "direction": ""},
]


def _fetch() -> list[dict]:
    """Fetch from NB RK XML API."""
    today = datetime.now().strftime("%d.%m.%Y")
    url = f"https://nationalbank.kz/rss/get_rates.cfm?fdate={today}"
    resp = httpx.get(url, timeout=10, follow_redirects=True)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    rates = []
    for item in root.findall("item"):
        code = item.findtext("title", "").strip()
        if code not in _CODES:
            continue
        raw_rate = item.findtext("description", "0").strip()
        quant = int(item.findtext("quant", "1").strip())
        change_val = item.findtext("change", "0").strip()
        # Rate per 1 unit
        rate_per_one = float(raw_rate) / quant if quant > 1 else float(raw_rate)
        # Direction arrow
        change_f = float(change_val) if change_val else 0.0
        if change_f > 0:
            direction = "↑"
        elif change_f < 0:
            direction = "↓"
        else:
            direction = ""
        rates.append({
            "code": code,
            "flag": _FLAGS.get(code, ""),
            "rate": f"{rate_per_one:.2f}".rstrip("0").rstrip(".") if rate_per_one != int(rate_per_one) else str(int(rate_per_one)),
            "change": change_val,
            "direction": direction,
        })
    # Ensure order: USD, EUR, RUB
    order = {c: i for i, c in enumerate(_CODES)}
    rates.sort(key=lambda r: order.get(r["code"], 99))
    return rates


def get_rates() -> list[dict]:
    """Return cached rates; refresh if stale."""
    global _cache, _cache_ts
    now = time.time()
    if _cache and (now - _cache_ts) < _TTL:
        return _cache
    try:
        rates = _fetch()
        if rates:
            _cache = rates
            _cache_ts = now
            log.info("Currency rates refreshed: %s", rates)
            return rates
    except Exception as e:
        log.warning("Currency fetch failed: %s", e)
    return _cache or _FALLBACK
