"""Meilisearch integration for full-text search."""

import json
import logging

import httpx

logger = logging.getLogger(__name__)

MEILI_URL = "http://meilisearch:7700"
MEILI_KEY = "total-kz-search-key-2026"
INDEX = "articles"

_headers = {"Authorization": f"Bearer {MEILI_KEY}", "Content-Type": "application/json"}


def index_article(article: dict):
    """Index a single article into Meilisearch."""
    tags = article.get("tags", [])
    if isinstance(tags, str):
        try:
            tags = json.loads(tags)
        except Exception:
            tags = []

    doc = {
        "id": article["id"],
        "title": article.get("title", ""),
        "excerpt": article.get("excerpt", ""),
        "body_text": (article.get("body_text") or "")[:5000],
        "author": article.get("author", ""),
        "sub_category": article.get("sub_category", ""),
        "pub_date": article.get("pub_date", ""),
        "tags": tags if isinstance(tags, list) else [],
        "status": article.get("status", "published"),
        "thumbnail": article.get("thumbnail") or article.get("main_image") or "",
    }
    try:
        httpx.post(f"{MEILI_URL}/indexes/{INDEX}/documents", json=[doc], headers=_headers, timeout=5)
    except Exception:
        logger.debug("Meilisearch unavailable for indexing article %s", article.get("id"))


def delete_article(article_id: int):
    """Remove an article from Meilisearch index."""
    try:
        httpx.delete(f"{MEILI_URL}/indexes/{INDEX}/documents/{article_id}", headers=_headers, timeout=5)
    except Exception:
        logger.debug("Meilisearch unavailable for deleting article %s", article_id)


def search(query: str, filters: str = "", page: int = 1, per_page: int = 30) -> dict:
    """Search articles via Meilisearch."""
    payload = {
        "q": query,
        "limit": per_page,
        "offset": (page - 1) * per_page,
        "attributesToHighlight": ["title", "excerpt"],
        "highlightPreTag": "<mark>",
        "highlightPostTag": "</mark>",
    }
    if filters:
        payload["filter"] = filters
    try:
        r = httpx.post(f"{MEILI_URL}/indexes/{INDEX}/search", json=payload, headers=_headers, timeout=5)
        data = r.json()
        return {
            "hits": data.get("hits", []),
            "total": data.get("estimatedTotalHits", 0),
            "query": query,
        }
    except Exception:
        return {"hits": [], "total": 0, "query": query}


def setup_index():
    """Create index with settings."""
    try:
        httpx.post(f"{MEILI_URL}/indexes", json={"uid": INDEX, "primaryKey": "id"}, headers=_headers, timeout=5)
        settings = {
            "searchableAttributes": ["title", "excerpt", "body_text", "author", "tags"],
            "filterableAttributes": ["sub_category", "status", "author", "pub_date"],
            "sortableAttributes": ["pub_date"],
            "displayedAttributes": ["id", "title", "excerpt", "author", "sub_category", "pub_date", "tags", "thumbnail", "main_image", "url", "status"],
        }
        httpx.patch(f"{MEILI_URL}/indexes/{INDEX}/settings", json=settings, headers=_headers, timeout=10)
    except Exception:
        logger.warning("Meilisearch unavailable for setup_index")


def reindex_all():
    """Bulk reindex all articles."""
    from . import db_backend as db
    from .config import settings

    if settings.use_postgres:
        from .pg_queries import get_pg_session
        from .models import Article
        from sqlalchemy import select
        with get_pg_session() as session:
            rows = session.execute(select(Article)).scalars().all()
            docs = []
            for a in rows:
                tags = []
                try:
                    tags = json.loads(a.tags or "[]")
                except Exception:
                    pass
                docs.append({
                    "id": a.id, "title": a.title or "", "excerpt": a.excerpt or "",
                    "body_text": (a.body_text or "")[:5000], "author": a.author or "",
                    "sub_category": a.sub_category or "", "pub_date": a.pub_date or "",
                    "tags": tags, "status": a.status or "published",
                    "thumbnail": a.thumbnail or a.main_image or "",
                })
    else:
        with db.get_db() as conn:
            rows = conn.execute("""
                SELECT id, title, excerpt, body_text, author, sub_category, pub_date, tags, status, thumbnail, main_image
                FROM articles
            """).fetchall()
            # Pre-load article_tags for enrichment keywords
            tag_map = {}
            try:
                tag_rows = conn.execute("SELECT article_id, tag FROM article_tags").fetchall()
                for tr in tag_rows:
                    tag_map.setdefault(tr[0], []).append(tr[1])
            except Exception:
                pass
            # Pre-load enrichment keywords
            kw_map = {}
            try:
                kw_rows = conn.execute("SELECT article_id, keywords FROM article_enrichments WHERE keywords IS NOT NULL").fetchall()
                for kr in kw_rows:
                    try:
                        kw_map[kr[0]] = json.loads(kr[1])
                    except Exception:
                        pass
            except Exception:
                pass
            docs = []
            for r in rows:
                tags = []
                try:
                    tags = json.loads(r["tags"] or "[]")
                except Exception:
                    pass
                # Merge: article.tags + article_tags + enrichment keywords
                all_tags = list(set(tags + tag_map.get(r["id"], []) + kw_map.get(r["id"], [])))
                docs.append({
                    "id": r["id"], "title": r["title"] or "", "excerpt": r["excerpt"] or "",
                    "body_text": (r["body_text"] or "")[:5000], "author": r["author"] or "",
                    "sub_category": r["sub_category"] or "", "pub_date": r["pub_date"] or "",
                    "tags": all_tags, "status": r["status"] or "published",
                    "thumbnail": r["thumbnail"] or r["main_image"] or "",
                })
    # Batch in chunks of 1000
    for i in range(0, len(docs), 1000):
        try:
            httpx.post(f"{MEILI_URL}/indexes/{INDEX}/documents", json=docs[i:i+1000], headers=_headers, timeout=30)
        except Exception:
            logger.warning("Meilisearch unavailable for reindex batch %d", i)
