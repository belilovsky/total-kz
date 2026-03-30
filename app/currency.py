"""
Курсы валют НБ РК + Brent + Gold — кэшированный запрос.
Обновляется не чаще раза в час; при ошибке отдаёт последние известные.
"""
import time, logging, xml.etree.ElementTree as ET
from datetime import datetime

import httpx

log = logging.getLogger("currency")

_cache: dict | None = None
_cache_ts: float = 0
_TTL = 3600  # 1 hour


_CODES = ("USD", "EUR", "CNY", "RUB")
_FLAGS = {"USD": "🇺🇸", "EUR": "🇪🇺", "CNY": "🇨🇳", "RUB": "🇷🇺"}

# Fallback if API is down
_FALLBACK_RATES = [
    {"code": "USD", "flag": "🇺🇸", "rate": "482.33", "change": "0.00", "direction": ""},
    {"code": "EUR", "flag": "🇪🇺", "rate": "557.57", "change": "0.00", "direction": ""},
    {"code": "CNY", "flag": "🇨🇳", "rate": "70.05",  "change": "0.00", "direction": ""},
    {"code": "RUB", "flag": "🇷🇺", "rate": "5.74",   "change": "0.00", "direction": ""},
]
_FALLBACK_COMMODITIES = [
    {"code": "BRENT", "icon": "🛢️", "label": "Brent", "value": "$72.00", "direction": ""},
    {"code": "GOLD",  "icon": "🥇", "label": "Золото", "value": "$3 050", "direction": ""},
]


def _fetch_rates_for_date(date_str: str) -> list[dict]:
    """Fetch full rates list for a given date dd.mm.yyyy."""
    url = f"https://nationalbank.kz/rss/get_rates.cfm?fdate={date_str}"
    resp = httpx.get(url, timeout=10, follow_redirects=True)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    result = []
    for item in root.findall("item"):
        code = item.findtext("title", "").strip()
        if code not in _CODES:
            continue
        raw_rate = float(item.findtext("description", "0").strip())
        quant = int(item.findtext("quant", "1").strip())
        change_val = item.findtext("change", "0").strip()
        result.append({"code": code, "rate": raw_rate / quant if quant > 1 else raw_rate, "change": change_val})
    return result


def _fetch_rates() -> list[dict]:
    """Fetch currency rates from NB RK XML API with prev day comparison."""
    from datetime import timedelta
    today = datetime.now()
    today_str = today.strftime("%d.%m.%Y")
    # Find last business day with non-zero changes (to use on weekends)
    last_bday_changes = {}  # code -> {change, direction}
    for delta in range(1, 7):
        prev_day = today - timedelta(days=delta)
        try:
            prev_data = _fetch_rates_for_date(prev_day.strftime("%d.%m.%Y"))
            has_changes = False
            for pd in prev_data:
                cv = float(pd["change"]) if pd["change"] else 0.0
                if abs(cv) > 0.001 and pd["code"] not in last_bday_changes:
                    last_bday_changes[pd["code"]] = {
                        "change": pd["change"],
                        "direction": "↑" if cv > 0 else "↓"
                    }
                    has_changes = True
            if has_changes and len(last_bday_changes) >= len(_CODES):
                break
        except Exception:
            continue
    url = f"https://nationalbank.kz/rss/get_rates.cfm?fdate={today_str}"
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
        rate_per_one = float(raw_rate) / quant if quant > 1 else float(raw_rate)
        change_f = float(change_val) if change_val else 0.0
        if change_f > 0:
            direction = "↑"
        elif change_f < 0:
            direction = "↓"
        else:
            # Use last business day's change data on weekends/holidays
            bday = last_bday_changes.get(code)
            if bday:
                direction = bday["direction"]
                change_val = bday["change"]
            else:
                direction = ""
        # Format rate nicely
        if rate_per_one >= 10:
            rate_str = f"{rate_per_one:.2f}"
        else:
            rate_str = f"{rate_per_one:.2f}"
        rates.append({
            "code": code,
            "flag": _FLAGS.get(code, ""),
            "rate": rate_str,
            "change": change_val,
            "direction": direction,
        })
    order = {c: i for i, c in enumerate(_CODES)}
    rates.sort(key=lambda r: order.get(r["code"], 99))
    return rates


def _fetch_commodities() -> list[dict]:
    """Fetch Brent crude oil and Gold prices from Yahoo Finance."""
    results = []
    symbols = [
        ("BZ=F", "BRENT", "🛢️", "Brent"),
        ("GC=F", "GOLD", "🥇", "Золото"),
    ]
    headers = {"User-Agent": "Mozilla/5.0 (total.kz/1.0)"}
    for symbol, code, icon, label in symbols:
        try:
            resp = httpx.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2d",
                headers=headers, timeout=8,
            )
            resp.raise_for_status()
            data = resp.json()
            meta = data["chart"]["result"][0]["meta"]
            price = meta["regularMarketPrice"]
            prev = meta.get("chartPreviousClose", price)
            change = price - prev
            if change > 0.01:
                direction = "↑"
            elif change < -0.01:
                direction = "↓"
            else:
                direction = ""
            # Format price
            if price >= 1000:
                # Space as thousands separator
                whole = int(price)
                frac = int((price - whole) * 100)
                value = f"${whole:,}".replace(",", " ")
            else:
                value = f"${price:.2f}"
            results.append({
                "code": code, "icon": icon, "label": label,
                "value": value, "direction": direction,
            })
        except Exception as e:
            log.warning("Commodity fetch failed for %s: %s", code, e)
    return results


def _emergency_mode() -> bool:
    import os
    return os.getenv("EMERGENCY_MODE", "").lower() in ("1", "true", "yes")


def get_rates() -> list[dict]:
    """Return cached currency rates; refresh if stale."""
    global _cache, _cache_ts
    now = time.time()
    if _cache and (now - _cache_ts) < _TTL:
        return _cache
    # Emergency mode: skip external API, use cache or fallback
    if _emergency_mode():
        return _cache or _FALLBACK_RATES
    try:
        rates = _fetch_rates()
        if rates:
            _cache = rates
            _cache_ts = now
            log.info("Currency rates refreshed: %s", [r["code"] for r in rates])
            return rates
    except Exception as e:
        log.warning("Currency fetch failed: %s", e)
    return _cache or _FALLBACK_RATES


# Commodities cache
_comm_cache: list | None = None
_comm_cache_ts: float = 0
_COMM_TTL = 1800  # 30 min


def get_commodities() -> list[dict]:
    """Return cached commodity prices; refresh if stale."""
    global _comm_cache, _comm_cache_ts
    now = time.time()
    if _comm_cache and (now - _comm_cache_ts) < _COMM_TTL:
        return _comm_cache
    # Emergency mode: skip external API, use cache or fallback
    if _emergency_mode():
        return _comm_cache or _FALLBACK_COMMODITIES
    try:
        data = _fetch_commodities()
        if data:
            _comm_cache = data
            _comm_cache_ts = now
            return data
    except Exception as e:
        log.warning("Commodities fetch failed: %s", e)
    return _comm_cache or _FALLBACK_COMMODITIES
