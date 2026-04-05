"""Health check / self-testing module for Total.kz.

Runs a battery of tests against pages, feeds, SEO files, API endpoints,
database, and external services, then returns a structured report.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx

from app.config import settings
from app import search_engine as meili

logger = logging.getLogger(__name__)

SITE_URL = "http://localhost:8000"
HEALTH_DATA_PATH = Path(__file__).parent.parent / "data" / "health_last.json"


# ── Result helpers ───────────────────────────────────────────────

def _ok(name: str, ms: float, details: str = "") -> dict:
    return {"name": name, "status": "pass", "response_time_ms": round(ms, 1), "details": details}


def _fail(name: str, ms: float, details: str = "") -> dict:
    return {"name": name, "status": "fail", "response_time_ms": round(ms, 1), "details": details}


def _warn(name: str, ms: float, details: str = "") -> dict:
    return {"name": name, "status": "warn", "response_time_ms": round(ms, 1), "details": details}


# ── HTTP helpers ─────────────────────────────────────────────────

def _get(client: httpx.Client, path: str) -> httpx.Response:
    """GET a path on the local site."""
    return client.get(f"{SITE_URL}{path}", timeout=10, follow_redirects=True)


# ── Individual test groups ───────────────────────────────────────

def _test_pages(client: httpx.Client) -> list[dict]:
    """Test that key pages return expected HTTP status."""
    results = []
    pages = [
        ("/", 200, "Homepage"),
        ("/news/politika", 200, "Category: politika"),
        ("/news/ekonomika", 200, "Category: ekonomika"),
        ("/news/sport", 200, "Category: sport"),
        ("/search?q=test", 200, "Search page"),
        ("/persons", 200, "Persons page"),
        ("/tags", 200, "Tags page"),
        ("/nonexistent-page-test", 404, "404 page"),
        ("/kz/", 200, "KZ version"),
    ]

    for path, expected_status, label in pages:
        t0 = time.monotonic()
        try:
            r = client.get(f"{SITE_URL}{path}", timeout=10, follow_redirects=True)
            ms = (time.monotonic() - t0) * 1000
            if r.status_code == expected_status:
                results.append(_ok(f"Page: {label}", ms, f"{path} → {r.status_code}"))
            else:
                results.append(_fail(f"Page: {label}", ms,
                    f"{path} → {r.status_code} (expected {expected_status})"))
        except Exception as exc:
            ms = (time.monotonic() - t0) * 1000
            results.append(_fail(f"Page: {label}", ms, f"{path} → error: {exc}"))

    # Random article page — grab first article URL from the homepage
    t0 = time.monotonic()
    try:
        r = _get(client, "/")
        ms_home = (time.monotonic() - t0) * 1000
        # Find an article link in the HTML
        import re
        links = re.findall(r'href="(/[a-z0-9_-]+/[a-z0-9_-]+)"', r.text)
        article_links = [l for l in links if l.count("/") == 2 and not l.startswith("/admin")
                         and not l.startswith("/static") and not l.startswith("/api")]
        if article_links:
            article_path = article_links[0]
            t0 = time.monotonic()
            r2 = _get(client, article_path)
            ms = (time.monotonic() - t0) * 1000
            if r2.status_code == 200:
                results.append(_ok("Page: Random article", ms, article_path))
            else:
                results.append(_fail("Page: Random article", ms,
                    f"{article_path} → {r2.status_code}"))
        else:
            results.append(_warn("Page: Random article", ms_home, "No article links found on homepage"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("Page: Random article", ms, str(exc)))

    return results


def _test_feeds(client: httpx.Client) -> list[dict]:
    """Test RSS/JSON feeds."""
    results = []
    feeds = [
        ("/rss", "RSS feed", ["<item>"]),
        ("/feed.json", "JSON feed", None),
        ("/turbo/rss.xml", "Turbo RSS", ["turbo:content"]),
        ("/zen/rss.xml", "Zen RSS", ["content:encoded"]),
        ("/fb-ia/rss.xml", "FB IA RSS", []),
        ("/flipboard/rss.xml", "Flipboard RSS", []),
    ]

    for path, label, expected_content in feeds:
        t0 = time.monotonic()
        try:
            r = _get(client, path)
            ms = (time.monotonic() - t0) * 1000
            if r.status_code != 200:
                results.append(_fail(f"Feed: {label}", ms, f"{path} → {r.status_code}"))
                continue

            body = r.text

            # JSON feed special check
            if path == "/feed.json":
                try:
                    json.loads(body)
                    results.append(_ok(f"Feed: {label}", ms, "Valid JSON"))
                except json.JSONDecodeError:
                    results.append(_fail(f"Feed: {label}", ms, "Invalid JSON"))
                continue

            # XML feeds — basic validation
            if "<" not in body:
                results.append(_fail(f"Feed: {label}", ms, "Response is not XML"))
                continue

            # Check expected content markers
            missing = []
            if expected_content:
                for marker in expected_content:
                    if marker not in body:
                        missing.append(marker)

            if missing:
                results.append(_fail(f"Feed: {label}", ms, f"Missing: {', '.join(missing)}"))
            else:
                results.append(_ok(f"Feed: {label}", ms, f"{path} valid"))
        except Exception as exc:
            ms = (time.monotonic() - t0) * 1000
            results.append(_fail(f"Feed: {label}", ms, str(exc)))

    # RSS content integrity — check content:encoded is not empty and has no <script>
    t0 = time.monotonic()
    try:
        r = _get(client, "/rss")
        ms = (time.monotonic() - t0) * 1000
        body = r.text
        if "content:encoded" in body:
            import re
            encoded_blocks = re.findall(r"<content:encoded>(.*?)</content:encoded>", body, re.DOTALL)
            if encoded_blocks:
                non_empty = [b for b in encoded_blocks if len(b.strip()) > 10]
                if not non_empty:
                    results.append(_warn("Feed: RSS content not empty", ms, "All content:encoded blocks are empty"))
                else:
                    results.append(_ok("Feed: RSS content not empty", ms, f"{len(non_empty)} blocks with content"))

                has_script = any("<script" in b.lower() for b in encoded_blocks)
                if has_script:
                    results.append(_fail("Feed: No <script> in RSS", ms, "Found <script> tags in RSS content"))
                else:
                    results.append(_ok("Feed: No <script> in RSS", ms, "Clean"))
            else:
                results.append(_warn("Feed: RSS content not empty", ms, "No content:encoded blocks found"))
        else:
            results.append(_warn("Feed: RSS content not empty", ms, "No content:encoded in RSS"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("Feed: RSS content integrity", ms, str(exc)))

    return results


def _test_seo(client: httpx.Client) -> list[dict]:
    """Test SEO-related files."""
    results = []
    files = [
        ("/sitemap.xml", "Sitemap"),
        ("/robots.txt", "Robots.txt"),
        ("/manifest.json", "Manifest"),
    ]
    for path, label in files:
        t0 = time.monotonic()
        try:
            r = _get(client, path)
            ms = (time.monotonic() - t0) * 1000
            if r.status_code == 200:
                results.append(_ok(f"SEO: {label}", ms, f"{path} → 200"))
            else:
                results.append(_fail(f"SEO: {label}", ms, f"{path} → {r.status_code}"))
        except Exception as exc:
            ms = (time.monotonic() - t0) * 1000
            results.append(_fail(f"SEO: {label}", ms, str(exc)))
    return results


def _test_api(client: httpx.Client) -> list[dict]:
    """Test API endpoints."""
    results = []
    endpoints = [
        ("/api/display-mode", "Display mode API"),
        ("/api/suggest?q=a", "Suggest API"),
        ("/health", "Health endpoint"),
    ]
    for path, label in endpoints:
        t0 = time.monotonic()
        try:
            r = _get(client, path)
            ms = (time.monotonic() - t0) * 1000
            if r.status_code == 200:
                results.append(_ok(f"API: {label}", ms, f"{path} → 200"))
            else:
                results.append(_fail(f"API: {label}", ms, f"{path} → {r.status_code}"))
        except Exception as exc:
            ms = (time.monotonic() - t0) * 1000
            results.append(_fail(f"API: {label}", ms, str(exc)))
    return results


def _test_database() -> list[dict]:
    """Test database connectivity and basic queries."""
    from app import db_backend as db
    results = []

    # Articles count
    t0 = time.monotonic()
    try:
        if settings.use_postgres:
            from app.pg_queries import get_pg_session
            from app.models import Article
            from sqlalchemy import select, func
            with get_pg_session() as sess:
                count = sess.scalar(select(func.count()).select_from(Article))
            ms = (time.monotonic() - t0) * 1000
            if count and count > 0:
                results.append(_ok("DB: Articles count", ms, f"{count} articles"))
            else:
                results.append(_warn("DB: Articles count", ms, f"Count = {count}"))
        else:
            from app.database import get_db
            with get_db() as conn:
                count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
            ms = (time.monotonic() - t0) * 1000
            if count and count > 0:
                results.append(_ok("DB: Articles count", ms, f"{count} articles"))
            else:
                results.append(_warn("DB: Articles count", ms, f"Count = {count}"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("DB: Articles count", ms, str(exc)))

    # Latest article freshness (not older than 24h)
    t0 = time.monotonic()
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
        if settings.use_postgres:
            from app.pg_queries import get_pg_session
            from app.models import Article
            from sqlalchemy import select, desc
            with get_pg_session() as sess:
                latest = sess.execute(
                    select(Article.pub_date)
                    .where(Article.status == "published")
                    .order_by(desc(Article.pub_date))
                    .limit(1)
                ).scalar()
            ms = (time.monotonic() - t0) * 1000
            if latest and latest >= cutoff:
                results.append(_ok("DB: Latest article freshness", ms, f"Latest: {latest}"))
            elif latest:
                results.append(_warn("DB: Latest article freshness", ms,
                    f"Latest article is older than 24h: {latest}"))
            else:
                results.append(_warn("DB: Latest article freshness", ms, "No published articles found"))
        else:
            from app.database import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT pub_date FROM articles WHERE status='published' ORDER BY pub_date DESC LIMIT 1"
                ).fetchone()
            ms = (time.monotonic() - t0) * 1000
            if row and row[0] and row[0] >= cutoff:
                results.append(_ok("DB: Latest article freshness", ms, f"Latest: {row[0]}"))
            elif row and row[0]:
                results.append(_warn("DB: Latest article freshness", ms,
                    f"Latest article is older than 24h: {row[0]}"))
            else:
                results.append(_warn("DB: Latest article freshness", ms, "No published articles found"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("DB: Latest article freshness", ms, str(exc)))

    # Persons/entities count
    t0 = time.monotonic()
    try:
        if settings.use_postgres:
            from app.pg_queries import get_pg_session
            from app.models import NerEntity
            from sqlalchemy import select, func
            with get_pg_session() as sess:
                count = sess.scalar(
                    select(func.count()).select_from(NerEntity)
                    .where(NerEntity.entity_type == "person")
                )
            ms = (time.monotonic() - t0) * 1000
            if count and count > 0:
                results.append(_ok("DB: Persons count", ms, f"{count} person entities"))
            else:
                results.append(_warn("DB: Persons count", ms, f"Count = {count}"))
        else:
            from app.database import get_db
            with get_db() as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM entities WHERE entity_type='person'"
                ).fetchone()[0]
            ms = (time.monotonic() - t0) * 1000
            if count and count > 0:
                results.append(_ok("DB: Persons count", ms, f"{count} person entities"))
            else:
                results.append(_warn("DB: Persons count", ms, f"Count = {count}"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("DB: Persons count", ms, str(exc)))

    return results


def _test_services() -> list[dict]:
    """Test external service connectivity."""
    results = []

    # PostgreSQL connection
    t0 = time.monotonic()
    if settings.use_postgres:
        try:
            from app.pg_queries import get_pg_session
            from sqlalchemy import text
            with get_pg_session() as sess:
                sess.execute(text("SELECT 1"))
            ms = (time.monotonic() - t0) * 1000
            results.append(_ok("Service: PostgreSQL", ms, "Connected"))
        except Exception as exc:
            ms = (time.monotonic() - t0) * 1000
            results.append(_fail("Service: PostgreSQL", ms, str(exc)))
    else:
        try:
            from app.database import get_db
            with get_db() as conn:
                conn.execute("SELECT 1")
            ms = (time.monotonic() - t0) * 1000
            results.append(_ok("Service: SQLite", ms, "Connected"))
        except Exception as exc:
            ms = (time.monotonic() - t0) * 1000
            results.append(_fail("Service: SQLite", ms, str(exc)))

    # Meilisearch
    t0 = time.monotonic()
    try:
        r = httpx.get(
            f"{meili.MEILI_URL}/health",
            headers=meili._headers,
            timeout=5,
        )
        ms = (time.monotonic() - t0) * 1000
        if r.status_code == 200:
            # Check that the index has documents
            try:
                r2 = httpx.get(
                    f"{meili.MEILI_URL}/indexes/{meili.INDEX}/stats",
                    headers=meili._headers,
                    timeout=5,
                )
                stats = r2.json()
                doc_count = stats.get("numberOfDocuments", 0)
                if doc_count > 0:
                    results.append(_ok("Service: Meilisearch", ms, f"Healthy, {doc_count} documents"))
                else:
                    results.append(_warn("Service: Meilisearch", ms, "Healthy but index is empty"))
            except Exception:
                results.append(_ok("Service: Meilisearch", ms, "Healthy (could not check index stats)"))
        else:
            results.append(_fail("Service: Meilisearch", ms, f"Status {r.status_code}"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("Service: Meilisearch", ms, str(exc)))

    # imgproxy
    t0 = time.monotonic()
    try:
        r = httpx.get("http://imgproxy:8080/health", timeout=5)
        ms = (time.monotonic() - t0) * 1000
        if r.status_code == 200:
            results.append(_ok("Service: imgproxy", ms, "Reachable"))
        else:
            results.append(_warn("Service: imgproxy", ms, f"Status {r.status_code}"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("Service: imgproxy", ms, str(exc)))

    return results


def _test_content_integrity(client: httpx.Client) -> list[dict]:
    """Test that pages contain expected structural elements."""
    results = []

    # Homepage has shelf-section elements
    t0 = time.monotonic()
    try:
        r = _get(client, "/")
        ms = (time.monotonic() - t0) * 1000
        if "shelf-section" in r.text:
            results.append(_ok("Content: Homepage shelf-section", ms, "Found"))
        else:
            results.append(_fail("Content: Homepage shelf-section", ms, "No shelf-section elements on homepage"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("Content: Homepage shelf-section", ms, str(exc)))

    # Article page has article-body content
    t0 = time.monotonic()
    try:
        r = _get(client, "/")
        import re
        links = re.findall(r'href="(/[a-z0-9_-]+/[a-z0-9_-]+)"', r.text)
        article_links = [l for l in links if l.count("/") == 2 and not l.startswith("/admin")
                         and not l.startswith("/static") and not l.startswith("/api")]
        if article_links:
            r2 = _get(client, article_links[0])
            ms = (time.monotonic() - t0) * 1000
            if "article-body" in r2.text:
                results.append(_ok("Content: Article body", ms, f"Found in {article_links[0]}"))
            else:
                results.append(_fail("Content: Article body", ms,
                    f"No article-body in {article_links[0]}"))
        else:
            ms = (time.monotonic() - t0) * 1000
            results.append(_warn("Content: Article body", ms, "No article links found to test"))
    except Exception as exc:
        ms = (time.monotonic() - t0) * 1000
        results.append(_fail("Content: Article body", ms, str(exc)))

    return results


# ── Main entry point ─────────────────────────────────────────────

def run_all_checks() -> dict:
    """Run all health checks and return a structured report.

    Returns:
        {
            "timestamp": "2026-04-01T12:00:00",
            "duration_ms": 1234,
            "summary": {"total": 30, "passed": 28, "failed": 1, "warnings": 1},
            "results": [...]
        }
    """
    t0 = time.monotonic()
    all_results: list[dict] = []

    with httpx.Client() as client:
        all_results.extend(_test_pages(client))
        all_results.extend(_test_feeds(client))
        all_results.extend(_test_seo(client))
        all_results.extend(_test_api(client))
        all_results.extend(_test_content_integrity(client))

    all_results.extend(_test_database())
    all_results.extend(_test_services())

    duration = (time.monotonic() - t0) * 1000

    passed = sum(1 for r in all_results if r["status"] == "pass")
    failed = sum(1 for r in all_results if r["status"] == "fail")
    warnings = sum(1 for r in all_results if r["status"] == "warn")

    report = {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        "duration_ms": round(duration, 1),
        "summary": {
            "total": len(all_results),
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
        },
        "results": all_results,
    }

    return report


def run_and_save() -> dict:
    """Run checks and persist results to disk."""
    report = run_all_checks()

    try:
        HEALTH_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        HEALTH_DATA_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        logger.info("Health check saved: %d tests, %d failed, %d warnings",
                     report["summary"]["total"],
                     report["summary"]["failed"],
                     report["summary"]["warnings"])
    except Exception as exc:
        logger.error("Failed to save health check results: %s", exc)

    if report["summary"]["failed"] > 0:
        failed_names = [r["name"] for r in report["results"] if r["status"] == "fail"]
        logger.warning("Health check FAILURES: %s", ", ".join(failed_names))

    return report
