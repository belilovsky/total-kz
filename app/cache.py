"""Simple in-memory TTL cache for expensive page data.

Thread-safe via a single dict + timestamp pairs.
No external dependencies.
"""

import logging
import time

logger = logging.getLogger(__name__)

_cache: dict[str, tuple[object, float]] = {}

# Default TTLs (seconds)
TTL_HOMEPAGE = 300       # 5 minutes
TTL_CATEGORY = 600       # 10 minutes
TTL_TAGS = 1800          # 30 minutes
TTL_PERSONS_LIST = 1800  # 30 minutes


def get(key: str, ttl: float | None = None) -> object | None:
    """Return cached value if it exists and hasn't expired, else None."""
    entry = _cache.get(key)
    if entry is None:
        return None
    val, ts = entry
    if ttl is not None and (time.time() - ts) >= ttl:
        return None
    return val


def set(key: str, val: object) -> None:  # noqa: A001
    """Store a value in the cache with current timestamp."""
    _cache[key] = (val, time.time())


def invalidate(key: str) -> None:
    """Remove a specific key from cache."""
    _cache.pop(key, None)


def invalidate_prefix(prefix: str) -> None:
    """Remove all keys starting with prefix."""
    to_del = [k for k in _cache if k.startswith(prefix)]
    for k in to_del:
        del _cache[k]


def invalidate_all() -> None:
    """Clear the entire cache (used on article create/update)."""
    n = len(_cache)
    _cache.clear()
    if n:
        logger.info("Cache invalidated: cleared %d entries", n)


def stats() -> dict:
    """Return cache statistics."""
    now = time.time()
    return {
        "entries": len(_cache),
        "keys": list(_cache.keys()),
        "ages": {k: round(now - ts, 1) for k, (_, ts) in _cache.items()},
    }
