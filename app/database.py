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

CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    phase TEXT NOT NULL,  -- 'urls' or 'content'
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
                    conn.execute("""
                        INSERT OR IGNORE INTO articles
                        (url, pub_date, sub_category, category_label, title, author,
                         excerpt, body_text, body_html, main_image, image_credit,
                         thumbnail, tags, inline_images)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        art.get("url"),
                        art.get("pub_date"),
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
                        json.dumps(art.get("tags", []), ensure_ascii=False),
                        json.dumps(art.get("inline_images", []), ensure_ascii=False),
                    ))
                    if conn.total_changes:
                        imported += 1
                    else:
                        skipped += 1
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
        
        return {
            "total": total,
            "categories": [dict(r) for r in cats],
            "months": [dict(r) for r in months],
            "authors": [dict(r) for r in authors],
            "date_from": date_range[0],
            "date_to": date_range[1],
            "runs": [dict(r) for r in runs],
        }


def search_articles(query: str = "", category: str = "", page: int = 1, per_page: int = 50) -> dict:
    """Search and paginate articles."""
    with get_db() as conn:
        conditions = []
        params = []
        
        if query:
            conditions.append("(title LIKE ? OR body_text LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
        
        if category:
            conditions.append("sub_category = ?")
            params.append(category)
        
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        total = conn.execute(
            f"SELECT COUNT(*) FROM articles {where}", params
        ).fetchone()[0]
        
        offset = (page - 1) * per_page
        rows = conn.execute(f"""
            SELECT id, url, pub_date, sub_category, category_label,
                   title, author, excerpt, thumbnail, main_image
            FROM articles {where}
            ORDER BY pub_date DESC
            LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()
        
        return {
            "articles": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
        }


def get_article(article_id: int) -> dict | None:
    """Get full article by ID."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM articles WHERE id = ?", (article_id,)
        ).fetchone()
        if row:
            d = dict(row)
            d["tags"] = json.loads(d.get("tags") or "[]")
            d["inline_images"] = json.loads(d.get("inline_images") or "[]")
            return d
        return None
