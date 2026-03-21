"""SQLite database setup and models."""

import sqlite3
import json
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager

DB_PATH = Path(__file__).parent.parent / "data" / "total.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    pub_date TEXT,
    sub_category TEXT,
    category_label TEXT,
    title TEXT,
    author TEXT,
    excerpt TEXT,
    body_text TEXT,
    body_html TEXT,
    main_image TEXT,
    image_credit TEXT,
    thumbnail TEXT,
    tags TEXT,  -- JSON array
    inline_images TEXT,  -- JSON array
    imported_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_articles_pub_date ON articles(pub_date);
CREATE INDEX IF NOT EXISTS idx_articles_sub_category ON articles(sub_category);
CREATE INDEX IF NOT EXISTS idx_articles_author ON articles(author);

-- NER-сущности: персоны, организации, события, локации
CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL,  -- 'person', 'org', 'event', 'location'
    normalized TEXT,            -- нормализованное имя (для дедупликации)
    UNIQUE(normalized, entity_type)
);

CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_normalized ON entities(normalized);

-- Связь статья <-> сущность
CREATE TABLE IF NOT EXISTS article_entities (
    article_id INTEGER REFERENCES articles(id),
    entity_id INTEGER REFERENCES entities(id),
    mention_count INTEGER DEFAULT 1,
    PRIMARY KEY (article_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_ae_article ON article_entities(article_id);
CREATE INDEX IF NOT EXISTS idx_ae_entity ON article_entities(entity_id);

-- Теги (денормализованные из JSON для быстрого поиска)
CREATE TABLE IF NOT EXISTS article_tags (
    article_id INTEGER REFERENCES articles(id),
    tag TEXT NOT NULL,
    PRIMARY KEY (article_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_tags_tag ON article_tags(tag);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    phase TEXT NOT NULL,  -- 'urls', 'content', 'ner'
    status TEXT DEFAULT 'running',  -- running, completed, failed
    articles_found INTEGER DEFAULT 0,
    articles_downloaded INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    log TEXT
);

CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER REFERENCES scrape_runs(id),
    timestamp TEXT DEFAULT (datetime('now')),
    level TEXT DEFAULT 'info',  -- info, warn, error
    message TEXT
);
"""


def get_db_path():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return str(DB_PATH)


@contextmanager
def get_db():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript(SCHEMA)


# Для парсинга русских дат из date_text ("ДД месяц YYYY, HH:MM")
_RU_MONTHS = {
    "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
    "мая": "05", "июня": "06", "июля": "07", "августа": "08",
    "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12",
}

def _parse_ru_date_text(text):
    """Парсить '13 апреля 2016, 11:38' в ISO формат."""
    if not text:
        return None
    import re
    m = re.search(r'(\d{1,2})\s+(\S+)\s+(\d{4})(?:,\s*(\d{1,2}):(\d{2}))?', text)
    if m:
        day = int(m.group(1))
        month = _RU_MONTHS.get(m.group(2).lower())
        year = m.group(3)
        hour = m.group(4) or "0"
        minute = m.group(5) or "0"
        if month:
            return f"{year}-{month}-{day:02d}T{int(hour):02d}:{int(minute):02d}:00"
    return None


def import_jsonl(jsonl_path: str) -> dict:
    """Import articles from JSONL file into SQLite. Returns stats."""
    init_db()
    imported = 0
    skipped = 0
    errors = 0

    with get_db() as conn:
        with open(jsonl_path, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    art = json.loads(line)
                    tags_json = json.dumps(art.get("tags", []), ensure_ascii=False)
                    images_json = json.dumps(art.get("inline_images", []), ensure_ascii=False)

                    # Дата: из pub_date (URL/карточка), фолбэк на date_text (со страницы статьи)
                    pub_date = art.get("pub_date")
                    if not pub_date:
                        pub_date = _parse_ru_date_text(art.get("date_text", ""))

                    # Используем INSERT OR REPLACE чтобы обновлять существующие записи
                    conn.execute("""
                        INSERT INTO articles
                        (url, pub_date, sub_category, category_label, title, author,
                         excerpt, body_text, body_html, main_image, image_credit,
                         thumbnail, tags, inline_images)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(url) DO UPDATE SET
                            pub_date = COALESCE(excluded.pub_date, articles.pub_date),
                            title = excluded.title,
                            author = excluded.author,
                            excerpt = excluded.excerpt,
                            body_text = excluded.body_text,
                            body_html = excluded.body_html,
                            main_image = excluded.main_image,
                            image_credit = excluded.image_credit,
                            thumbnail = excluded.thumbnail,
                            tags = excluded.tags,
                            inline_images = excluded.inline_images,
                            imported_at = datetime('now')
                    """, (
                        art.get("url"),
                        pub_date,
                        art.get("sub_category"),
                        art.get("category_label"),
                        art.get("title"),
                        art.get("author"),
                        art.get("excerpt"),
                        art.get("body_text"),
                        art.get("body_html"),
                        art.get("main_image"),
                        art.get("image_credit"),
                        art.get("thumbnail"),
                        tags_json,
                        images_json,
                    ))
                    imported += 1
                except Exception as e:
                    errors += 1

    return {"imported": imported, "skipped": skipped, "errors": errors}


def get_stats() -> dict:
    """Get dashboard statistics."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        
        cats = conn.execute("""
            SELECT sub_category, COUNT(*) as cnt
            FROM articles GROUP BY sub_category ORDER BY cnt DESC
        """).fetchall()
        
        months = conn.execute("""
            SELECT substr(pub_date, 1, 7) as month, COUNT(*) as cnt
            FROM articles WHERE pub_date IS NOT NULL
            GROUP BY month ORDER BY month
        """).fetchall()
        
        # Годовая агрегация для sparkline
        years = conn.execute("""
            SELECT substr(pub_date, 1, 4) as year, COUNT(*) as cnt
            FROM articles WHERE pub_date IS NOT NULL
            GROUP BY year ORDER BY year
        """).fetchall()
        
        authors = conn.execute("""
            SELECT author, COUNT(*) as cnt
            FROM articles WHERE author IS NOT NULL AND author != ''
            GROUP BY author ORDER BY cnt DESC LIMIT 20
        """).fetchall()
        
        date_range = conn.execute("""
            SELECT MIN(pub_date), MAX(pub_date) FROM articles
        """).fetchone()
        
        runs = conn.execute("""
            SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT 20
        """).fetchall()

        # Среднее за последние 12 месяцев vs предыдущие 12
        avg_recent = conn.execute("""
            SELECT ROUND(AVG(cnt)) FROM (
                SELECT COUNT(*) as cnt FROM articles
                WHERE pub_date >= date('now', '-12 months')
                GROUP BY substr(pub_date, 1, 7)
            )
        """).fetchone()[0] or 0
        avg_prev = conn.execute("""
            SELECT ROUND(AVG(cnt)) FROM (
                SELECT COUNT(*) as cnt FROM articles
                WHERE pub_date >= date('now', '-24 months')
                  AND pub_date < date('now', '-12 months')
                GROUP BY substr(pub_date, 1, 7)
            )
        """).fetchone()[0] or 0

        # Статьи за текущий и прошлый месяц
        this_month = conn.execute("""
            SELECT COUNT(*) FROM articles
            WHERE substr(pub_date, 1, 7) = strftime('%Y-%m', 'now')
        """).fetchone()[0]
        last_month = conn.execute("""
            SELECT COUNT(*) FROM articles
            WHERE substr(pub_date, 1, 7) = strftime('%Y-%m', 'now', '-1 month')
        """).fetchone()[0]

        # Топ-5 категорий по году для heatmap
        cat_by_year = conn.execute("""
            SELECT sub_category, substr(pub_date, 1, 4) as year, COUNT(*) as cnt
            FROM articles WHERE pub_date IS NOT NULL
            GROUP BY sub_category, year
            ORDER BY sub_category, year
        """).fetchall()
        
        return {
            "total": total,
            "categories": [dict(r) for r in cats],
            "months": [dict(r) for r in months],
            "years": [dict(r) for r in years],
            "authors": [dict(r) for r in authors],
            "date_from": date_range[0],
            "date_to": date_range[1],
            "runs": [dict(r) for r in runs],
            "avg_recent": int(avg_recent),
            "avg_prev": int(avg_prev),
            "this_month": this_month,
            "last_month": last_month,
            "cat_by_year": [dict(r) for r in cat_by_year],
        }


def search_articles(
    query: str = "",
    category: str = "",
    author: str = "",
    date_from: str = "",
    date_to: str = "",
    tag: str = "",
    entity_id: int = 0,
    page: int = 1,
    per_page: int = 30,
) -> dict:
    """Search and paginate articles with extended filters."""
    with get_db() as conn:
        conditions = []
        params = []
        joins = []

        if query:
            conditions.append("(a.title LIKE ? OR a.body_text LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])

        if category:
            conditions.append("a.sub_category = ?")
            params.append(category)

        if author:
            conditions.append("a.author = ?")
            params.append(author)

        if date_from:
            conditions.append("a.pub_date >= ?")
            params.append(date_from)

        if date_to:
            conditions.append("a.pub_date <= ? || ' 23:59:59'")
            params.append(date_to)

        if tag:
            joins.append("JOIN article_tags at ON at.article_id = a.id")
            conditions.append("at.tag = ?")
            params.append(tag)

        if entity_id:
            joins.append("JOIN article_entities ae ON ae.article_id = a.id")
            conditions.append("ae.entity_id = ?")
            params.append(entity_id)

        join_sql = " ".join(joins)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        total = conn.execute(
            f"SELECT COUNT(DISTINCT a.id) FROM articles a {join_sql} {where}", params
        ).fetchone()[0]

        offset = (page - 1) * per_page
        rows = conn.execute(f"""
            SELECT DISTINCT a.id, a.url, a.pub_date, a.sub_category, a.category_label,
                   a.title, a.author, a.excerpt, a.thumbnail, a.main_image
            FROM articles a {join_sql} {where}
            ORDER BY a.pub_date DESC
            LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()

        return {
            "articles": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }


def get_authors() -> list:
    """Get all distinct authors sorted by article count."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT author, COUNT(*) as cnt
            FROM articles
            WHERE author IS NOT NULL AND author != ''
            GROUP BY author
            ORDER BY cnt DESC
        """).fetchall()
        return [dict(r) for r in rows]


def get_tags(limit: int = 100) -> list:
    """Get all tags sorted by usage count."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT tag, COUNT(*) as cnt
            FROM article_tags
            GROUP BY tag
            ORDER BY cnt DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_entities(entity_type: str = "", limit: int = 50) -> list:
    """Get entities sorted by mention count."""
    with get_db() as conn:
        where = "WHERE e.entity_type = ?" if entity_type else ""
        params = [entity_type] if entity_type else []
        rows = conn.execute(f"""
            SELECT e.id, e.name, e.short_name, e.entity_type, e.normalized,
                   COUNT(ae.article_id) as article_count
            FROM entities e
            JOIN article_entities ae ON ae.entity_id = e.id
            {where}
            GROUP BY e.id
            ORDER BY article_count DESC
            LIMIT ?
        """, params + [limit]).fetchall()
        return [dict(r) for r in rows]


def get_article(article_id: int) -> dict | None:
    """Get full article by ID, including entities."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM articles WHERE id = ?", (article_id,)
        ).fetchone()
        if row:
            d = dict(row)
            d["tags"] = json.loads(d.get("tags") or "[]")
            d["inline_images"] = json.loads(d.get("inline_images") or "[]")
            # Fetch entities for this article
            entities = conn.execute("""
                SELECT e.id, e.name, e.short_name, e.entity_type, ae.mention_count
                FROM entities e
                JOIN article_entities ae ON ae.entity_id = e.id
                WHERE ae.article_id = ?
                ORDER BY ae.mention_count DESC
            """, (article_id,)).fetchall()
            d["entities"] = [dict(e) for e in entities]
            return d
        return None


# ══════════════════════════════════════════════
# PUBLIC FRONTEND QUERIES
# ══════════════════════════════════════════════

def get_article_by_slug(category: str, slug: str) -> dict | None:
    """Find article by category + slug (extracted from old URL)."""
    with get_db() as conn:
        url_pattern = f"%/ru/news/{category}/{slug}"
        row = conn.execute(
            "SELECT * FROM articles WHERE url LIKE ? LIMIT 1", (url_pattern,)
        ).fetchone()
        if row:
            d = dict(row)
            d["tags"] = json.loads(d.get("tags") or "[]")
            d["inline_images"] = json.loads(d.get("inline_images") or "[]")
            entities = conn.execute("""
                SELECT e.id, e.name, e.short_name, e.entity_type, ae.mention_count
                FROM entities e
                JOIN article_entities ae ON ae.entity_id = e.id
                WHERE ae.article_id = ?
                ORDER BY ae.mention_count DESC
            """, (d["id"],)).fetchall()
            d["entities"] = [dict(e) for e in entities]
            return d
        return None


def get_latest_articles(limit: int = 20, offset: int = 0) -> list:
    """Get latest articles for homepage."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, url, pub_date, sub_category, title, author, excerpt,
                   thumbnail, main_image
            FROM articles
            ORDER BY pub_date DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()
        return [dict(r) for r in rows]


def get_latest_by_category(category: str, limit: int = 10, offset: int = 0) -> dict:
    """Get latest articles for a category with pagination."""
    with get_db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE sub_category = ?", (category,)
        ).fetchone()[0]
        rows = conn.execute("""
            SELECT id, url, pub_date, sub_category, title, author, excerpt,
                   thumbnail, main_image
            FROM articles WHERE sub_category = ?
            ORDER BY pub_date DESC
            LIMIT ? OFFSET ?
        """, (category, limit, offset)).fetchall()
        return {
            "articles": [dict(r) for r in rows],
            "total": total,
            "pages": max(1, (total + limit - 1) // limit),
        }


def get_related_articles(article_id: int, category: str, limit: int = 4) -> list:
    """Get related articles from same category, excluding current."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, url, pub_date, sub_category, title, author, excerpt,
                   thumbnail, main_image
            FROM articles
            WHERE sub_category = ? AND id != ?
            ORDER BY pub_date DESC
            LIMIT ?
        """, (category, article_id, limit)).fetchall()
        return [dict(r) for r in rows]


def get_timeline_articles(article_id: int, category: str, pub_date: str, entity_ids: list = None) -> dict:
    """Get timeline articles related by shared entities (or fallback to category)."""
    with get_db() as conn:
        if entity_ids:
            # Find articles sharing at least one entity, ordered by relevance (shared count)
            placeholders = ','.join('?' * len(entity_ids))
            prev_rows = conn.execute(f"""
                SELECT a.id, a.url, a.pub_date, a.sub_category, a.title, a.thumbnail, a.main_image,
                       COUNT(DISTINCT ae.entity_id) as shared
                FROM articles a
                JOIN article_entities ae ON ae.article_id = a.id
                WHERE ae.entity_id IN ({placeholders})
                  AND a.id != ? AND a.pub_date <= ?
                GROUP BY a.id
                ORDER BY a.pub_date DESC
                LIMIT 3
            """, (*entity_ids, article_id, pub_date)).fetchall()
            next_rows = conn.execute(f"""
                SELECT a.id, a.url, a.pub_date, a.sub_category, a.title, a.thumbnail, a.main_image,
                       COUNT(DISTINCT ae.entity_id) as shared
                FROM articles a
                JOIN article_entities ae ON ae.article_id = a.id
                WHERE ae.entity_id IN ({placeholders})
                  AND a.id != ? AND a.pub_date > ?
                GROUP BY a.id
                ORDER BY a.pub_date ASC
                LIMIT 3
            """, (*entity_ids, article_id, pub_date)).fetchall()
        else:
            # Fallback: same sub_category
            prev_rows = conn.execute("""
                SELECT id, url, pub_date, sub_category, title, thumbnail, main_image
                FROM articles
                WHERE sub_category = ? AND id != ? AND pub_date <= ?
                ORDER BY pub_date DESC
                LIMIT 3
            """, (category, article_id, pub_date)).fetchall()
            next_rows = conn.execute("""
                SELECT id, url, pub_date, sub_category, title, thumbnail, main_image
                FROM articles
                WHERE sub_category = ? AND id != ? AND pub_date > ?
                ORDER BY pub_date ASC
                LIMIT 3
            """, (category, article_id, pub_date)).fetchall()
        return {
            "prev": [dict(r) for r in prev_rows],
            "next": [dict(r) for r in next_rows],
        }


def get_related_by_entities(article_id: int, entity_ids: list, category: str, limit: int = 6) -> list:
    """Get related articles by shared entities, falling back to category."""
    with get_db() as conn:
        results = []
        if entity_ids:
            placeholders = ','.join('?' * len(entity_ids))
            rows = conn.execute(f"""
                SELECT a.id, a.url, a.pub_date, a.sub_category, a.title, a.author, a.excerpt,
                       a.thumbnail, a.main_image,
                       COUNT(DISTINCT ae.entity_id) as shared
                FROM articles a
                JOIN article_entities ae ON ae.article_id = a.id
                WHERE ae.entity_id IN ({placeholders}) AND a.id != ?
                GROUP BY a.id
                ORDER BY shared DESC, a.pub_date DESC
                LIMIT ?
            """, (*entity_ids, article_id, limit)).fetchall()
            results = [dict(r) for r in rows]
        # Fill remaining slots from same category
        if len(results) < limit:
            existing_ids = [r['id'] for r in results] + [article_id]
            placeholders2 = ','.join('?' * len(existing_ids))
            fill = conn.execute(f"""
                SELECT id, url, pub_date, sub_category, title, author, excerpt,
                       thumbnail, main_image
                FROM articles
                WHERE sub_category = ? AND id NOT IN ({placeholders2})
                ORDER BY pub_date DESC
                LIMIT ?
            """, (category, *existing_ids, limit - len(results))).fetchall()
            results.extend([dict(r) for r in fill])
        return results


def get_trending_tags(limit: int = 20) -> list:
    """Get trending tags (most used recently)."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT at.tag, COUNT(*) as cnt
            FROM article_tags at
            JOIN articles a ON a.id = at.article_id
            WHERE a.pub_date >= date('now', '-30 days')
            GROUP BY at.tag
            ORDER BY cnt DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_category_counts() -> list:
    """Get article counts per category."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT sub_category, COUNT(*) as cnt
            FROM articles
            GROUP BY sub_category
            ORDER BY cnt DESC
        """).fetchall()
        return [dict(r) for r in rows]


def get_latest_by_categories(categories: list, limit: int = 10, offset: int = 0) -> dict:
    """Get latest articles from multiple sub_categories (for grouped nav sections)."""
    if not categories:
        return {"articles": [], "total": 0, "pages": 1}
    with get_db() as conn:
        placeholders = ",".join("?" for _ in categories)
        total = conn.execute(
            f"SELECT COUNT(*) FROM articles WHERE sub_category IN ({placeholders})",
            categories
        ).fetchone()[0]
        rows = conn.execute(f"""
            SELECT id, url, pub_date, sub_category, title, author, excerpt,
                   thumbnail, main_image
            FROM articles WHERE sub_category IN ({placeholders})
            ORDER BY pub_date DESC
            LIMIT ? OFFSET ?
        """, (*categories, limit, offset)).fetchall()
        return {
            "articles": [dict(r) for r in rows],
            "total": total,
            "pages": max(1, (total + limit - 1) // limit),
        }


def get_entity(entity_id: int) -> dict | None:
    """Get entity by ID."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, name, short_name, entity_type, normalized FROM entities WHERE id = ?",
            (entity_id,)
        ).fetchone()
        return dict(row) if row else None


def get_articles_by_entity(entity_id: int, page: int = 1, per_page: int = 20) -> dict:
    """Get articles linked to an entity, paginated."""
    with get_db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM article_entities WHERE entity_id = ?",
            (entity_id,)
        ).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute("""
            SELECT a.id, a.url, a.pub_date, a.sub_category, a.title, a.author,
                   a.excerpt, a.thumbnail, a.main_image
            FROM articles a
            JOIN article_entities ae ON ae.article_id = a.id
            WHERE ae.entity_id = ?
            ORDER BY a.pub_date DESC
            LIMIT ? OFFSET ?
        """, (entity_id, per_page, offset)).fetchall()
        return {
            "articles": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }


def generate_sitemap_urls(limit: int = 50000) -> list:
    """Get URLs for sitemap generation."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT url, pub_date, sub_category
            FROM articles
            WHERE pub_date IS NOT NULL
            ORDER BY pub_date DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
