"""SQLite database setup and models."""

import sqlite3
import json
import re as _re
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager

from qazstack.content import parse_ru_date, iter_jsonl

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
CREATE INDEX IF NOT EXISTS idx_articles_sub_category_pub_date ON articles(sub_category, pub_date DESC);

-- NER-сущности: персоны, организации, события, локации
CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    short_name TEXT,            -- сокращённое название (для компактного отображения)
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
        # Auto-migrate: add short_name column if missing
        entity_cols = [r[1] for r in conn.execute("PRAGMA table_info(entities)").fetchall()]
        if "short_name" not in entity_cols:
            conn.execute("ALTER TABLE entities ADD COLUMN short_name TEXT")

        # Auto-migrate: add CMS columns to articles
        article_cols = [r[1] for r in conn.execute("PRAGMA table_info(articles)").fetchall()]
        if "status" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN status TEXT DEFAULT 'published'")
        if "updated_at" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN updated_at TEXT")
        if "editor_note" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN editor_note TEXT")

        # v10.2: add body_blocks, scheduled_at, focal_x, focal_y
        if "body_blocks" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN body_blocks TEXT")
        if "scheduled_at" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN scheduled_at TEXT")
        if "focal_x" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN focal_x REAL DEFAULT 0.5")
        if "focal_y" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN focal_y REAL DEFAULT 0.5")

        # Create media table
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                original_name TEXT,
                mime_type TEXT,
                file_size INTEGER,
                url TEXT NOT NULL,
                uploaded_at TEXT DEFAULT (datetime('now')),
                uploaded_by TEXT
            );
        """)

        # Create article_revisions table for revision history
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS article_revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL REFERENCES articles(id),
                changed_at TEXT NOT NULL DEFAULT (datetime('now')),
                changed_by TEXT,
                changes_json TEXT,
                revision_type TEXT NOT NULL DEFAULT 'edit'
            );
            CREATE INDEX IF NOT EXISTS idx_revisions_article ON article_revisions(article_id, changed_at DESC);
        """)

        # ── v11: CMS Admin tables ──────────────────────
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                avatar_url TEXT DEFAULT '',
                role TEXT NOT NULL DEFAULT 'journalist',
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                last_login TEXT
            );

            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT UNIQUE NOT NULL,
                name_ru TEXT NOT NULL,
                name_kz TEXT DEFAULT '',
                parent_id INTEGER REFERENCES categories(id),
                sort_order INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                article_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_categories_slug ON categories(slug);

            CREATE TABLE IF NOT EXISTS authors_managed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                slug TEXT UNIQUE NOT NULL,
                bio TEXT DEFAULT '',
                avatar_url TEXT DEFAULT '',
                email TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1,
                article_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_authors_managed_slug ON authors_managed(slug);

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER REFERENCES users(id),
                username TEXT,
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id INTEGER,
                details TEXT,
                ip_address TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity_type, entity_id);
        """)

        # v11: add new columns to media table
        media_cols = [r[1] for r in conn.execute("PRAGMA table_info(media)").fetchall()]
        if "width" not in media_cols:
            conn.execute("ALTER TABLE media ADD COLUMN width INTEGER")
        if "height" not in media_cols:
            conn.execute("ALTER TABLE media ADD COLUMN height INTEGER")
        if "alt_text" not in media_cols:
            conn.execute("ALTER TABLE media ADD COLUMN alt_text TEXT DEFAULT ''")
        if "credit" not in media_cols:
            conn.execute("ALTER TABLE media ADD COLUMN credit TEXT DEFAULT ''")

        # v12: Workflow — article_comments table
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS article_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                role TEXT NOT NULL,
                comment TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (article_id) REFERENCES articles(id)
            );
            CREATE INDEX IF NOT EXISTS idx_comments_article ON article_comments(article_id);
        """)

        # v12: Workflow — assigned_to column on articles
        if "assigned_to" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN assigned_to TEXT")

        # Seed admin user if users table is empty
        from . import auth as _auth
        user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if user_count == 0:
            conn.execute("""
                INSERT INTO users (username, email, password_hash, display_name, role, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
            """, ("admin", "admin@total.kz", _auth.hash_password("admin"),
                  "Администратор", "admin", 1))

        # Auto-populate categories from articles.sub_category
        cat_count = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        if cat_count == 0:
            _cat_labels = {
                "vnutrennyaya_politika": "Внутренняя политика",
                "vneshnyaya_politika": "Внешняя политика",
                "politika": "Политика", "mir": "Мир",
                "bezopasnost": "Безопасность", "mneniya": "Мнения",
                "ekonomika_sobitiya": "Экономика (События)",
                "ekonomika": "Экономика", "biznes": "Бизнес",
                "finansi": "Финансы", "gossektor": "Госсектор",
                "tehno": "Технологии", "obshchestvo": "Общество",
                "obshchestvo_sobitiya": "Общество (События)",
                "proisshestviya": "Происшествия", "zhizn": "Жизнь",
                "kultura": "Культура", "religiya": "Религия",
                "den_v_istorii": "День в истории", "sport": "Спорт",
                "nauka": "Наука", "stil_zhizni": "Стиль жизни",
                "redaktsiya_tandau": "Выбор редакции", "drugoe": "Другое",
            }
            cats = conn.execute("""
                SELECT sub_category, COUNT(*) as cnt
                FROM articles WHERE sub_category IS NOT NULL AND sub_category != ''
                GROUP BY sub_category ORDER BY cnt DESC
            """).fetchall()
            for i, row in enumerate(cats):
                slug = row[0]
                name = _cat_labels.get(slug, slug)
                conn.execute("""
                    INSERT OR IGNORE INTO categories (slug, name_ru, sort_order, article_count)
                    VALUES (?, ?, ?, ?)
                """, (slug, name, i, row[1]))

        # Auto-populate authors_managed from articles.author
        author_count = conn.execute("SELECT COUNT(*) FROM authors_managed").fetchone()[0]
        if author_count == 0:
            authors_raw = conn.execute("""
                SELECT author, COUNT(*) as cnt
                FROM articles WHERE author IS NOT NULL AND author != ''
                GROUP BY author ORDER BY cnt DESC
            """).fetchall()
            for row in authors_raw:
                name = row[0]
                # Simple slug from name
                slug = name.lower().replace(" ", "-").replace(".", "")[:120]
                conn.execute("""
                    INSERT OR IGNORE INTO authors_managed (name, slug, article_count)
                    VALUES (?, ?, ?)
                """, (name, slug, row[1]))

        # ── v14: Ad Placements table ──────────────────────
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS ad_placements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                page TEXT NOT NULL,
                position TEXT NOT NULL,
                size_desktop TEXT,
                size_mobile TEXT,
                css_class TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                client_name TEXT DEFAULT '',
                client_url TEXT DEFAULT '',
                image_url TEXT DEFAULT '',
                start_date TEXT DEFAULT '',
                end_date TEXT DEFAULT '',
                impressions INTEGER DEFAULT 0,
                clicks INTEGER DEFAULT 0,
                cpm_rate REAL DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_ad_slot ON ad_placements(slot_id);
            CREATE INDEX IF NOT EXISTS idx_ad_page ON ad_placements(page);
        """)

        # Seed ad placements if table is empty
        ad_count = conn.execute("SELECT COUNT(*) FROM ad_placements").fetchone()[0]
        if ad_count == 0:
            _ad_seeds = [
                ('A1','Топ-баннер','global','Над шапкой','970×90','320×50','ad-top-banner'),
                ('A2','Под шапкой','global','Под навигацией','728×90','320×100','ad-below-header'),
                ('A3','Футер-баннер','global','Перед футером','728×90','320×50','ad-footer-banner'),
                ('H1','После hero','home','Между hero и категориями','728×90','320×100','ad-leaderboard'),
                ('H2','В ленте #1','home','После 3-й секции','100%×80','100%×80','ad-in-feed'),
                ('H3','В ленте #2','home','После 6-й секции','100%×80','100%×80','ad-in-feed'),
                ('R1','Под заголовком','article','Между заголовком и телом','728×90','320×100','ad-article-top'),
                ('R2','В тексте #1','article','После 3-го параграфа','100%×80','100%×80','ad-inline'),
                ('R3','В тексте #2','article','После 7-го параграфа','100%×80','100%×80','ad-inline'),
                ('R4','Сайдбар sticky','article','Sticky в сайдбаре','300×250','скрыт','ad-sidebar'),
                ('R5','Сайдбар #2','article','Второй блок сайдбара','300×600','скрыт','ad-sidebar-btf'),
                ('R6','После статьи','article','Между статьёй и "Читайте также"','728×90','320×100','ad-after-article'),
                ('C1','Над списком','category','После заголовка категории','728×90','320×100','ad-leaderboard'),
                ('C2','Сайдбар','category','В сайдбаре','300×250','скрыт','ad-sidebar'),
                ('S1','Над результатами','search','После поисковой формы','728×90','320×100','ad-leaderboard'),
                ('P1','Над списком','listing','После заголовка','728×90','320×100','ad-leaderboard'),
                ('PP1','Между секциями','person','Между биографией и статьями','728×90','320×100','ad-person-mid'),
            ]
            for seed in _ad_seeds:
                conn.execute("""
                    INSERT OR IGNORE INTO ad_placements
                    (slot_id, name, page, position, size_desktop, size_mobile, css_class)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, seed)


def blocks_to_html(blocks_json: str) -> str:
    """Convert Editor.js JSON blocks to HTML string."""
    blocks = json.loads(blocks_json) if isinstance(blocks_json, str) else blocks_json
    html_parts = []
    for block in blocks.get("blocks", []):
        t = block["type"]
        d = block["data"]
        if t == "header":
            lvl = d.get("level", 2)
            html_parts.append(f'<h{lvl}>{d["text"]}</h{lvl}>')
        elif t == "paragraph":
            html_parts.append(f'<p>{d["text"]}</p>')
        elif t == "list":
            tag = "ol" if d.get("style") == "ordered" else "ul"
            items = "".join(f'<li>{i.get("content","") if isinstance(i,dict) else i}</li>' for i in d.get("items", []))
            html_parts.append(f'<{tag}>{items}</{tag}>')
        elif t == "quote":
            caption = f'<cite>{d["caption"]}</cite>' if d.get("caption") else ""
            html_parts.append(f'<blockquote><p>{d["text"]}</p>{caption}</blockquote>')
        elif t == "image":
            cap = f'<figcaption>{d["caption"]}</figcaption>' if d.get("caption") else ""
            html_parts.append(f'<figure><img src="{d["file"]["url"]}" alt="{d.get("caption","")}" loading="lazy">{cap}</figure>')
        elif t == "embed":
            html_parts.append(f'<div class="embed-container"><iframe src="{d["embed"]}" frameborder="0" allowfullscreen></iframe></div>')
        elif t == "delimiter":
            html_parts.append('<hr>')
        elif t == "code":
            html_parts.append(f'<pre><code>{d["code"]}</code></pre>')
        elif t == "table":
            rows = d.get("content", [])
            with_headings = d.get("withHeadings", False)
            thtml = '<table class="article-table"><tbody>'
            for ri, row in enumerate(rows):
                thtml += '<tr>'
                for cell in row:
                    tag = 'th' if with_headings and ri == 0 else 'td'
                    thtml += f'<{tag}>{cell}</{tag}>'
                thtml += '</tr>'
            thtml += '</tbody></table>'
            html_parts.append(thtml)
        elif t == "warning":
            title = d.get("title", "")
            msg = d.get("message", "")
            html_parts.append(f'<div class="article-warning"><strong>{title}</strong><p>{msg}</p></div>')
        elif t == "infobox":
            title = d.get("title", "")
            text_html = "".join(f"<p>{p}</p>" for p in d.get("text", "").split("\n") if p.strip())
            html_parts.append(
                f'<div class="article-infobox">'
                f'<div class="article-infobox-header">'
                f'<div class="article-infobox-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg></div>'
                f'<div class="article-infobox-title">{title}</div>'
                f'</div>{text_html}</div>'
            )
        elif t == "callout":
            ctype = d.get("type", "info")
            title = d.get("title", "")
            text = d.get("text", "")
            html_parts.append(
                f'<div class="article-callout article-callout--{ctype}">'
                f'<div class="article-callout-title">{title}</div>'
                f'<p>{text}</p></div>'
            )
        elif t == "numberbox":
            items = d.get("items", [])
            stats = ""
            for it in items:
                val = it.get("value", "")
                label = it.get("label", "")
                delta = it.get("delta", "")
                delta_class = "up" if delta.startswith(("+", "↑")) else ("down" if delta.startswith(("-", "↓")) else "")
                delta_html = f'<span class="article-number-delta {delta_class}">{delta}</span>' if delta else ""
                stats += f'<div class="article-number-stat"><span class="article-number-value">{val}</span><span class="article-number-label">{label}</span>{delta_html}</div>'
            html_parts.append(f'<div class="article-number-box"><div class="article-number-box-grid">{stats}</div></div>')
    return "\n".join(html_parts)


def blocks_to_text(blocks_json: str) -> str:
    """Extract plain text from Editor.js blocks for search indexing."""
    blocks = json.loads(blocks_json) if isinstance(blocks_json, str) else blocks_json
    parts = []
    for block in blocks.get("blocks", []):
        d = block["data"]
        if "text" in d:
            parts.append(_re.sub(r'<[^>]+>', '', d["text"]))
        if "items" in d:
            for item in d["items"]:
                text = item.get("content", "") if isinstance(item, dict) else str(item)
                parts.append(_re.sub(r'<[^>]+>', '', text))
        if "code" in d:
            parts.append(d["code"])
        if block["type"] == "table":
            for row in d.get("content", []):
                parts.append(" ".join(_re.sub(r'<[^>]+>', '', c) for c in row))
        if block["type"] == "warning":
            parts.append(_re.sub(r'<[^>]+>', '', d.get("title", "")))
            parts.append(_re.sub(r'<[^>]+>', '', d.get("message", "")))
        if block["type"] in ("infobox", "callout"):
            if d.get("title"):
                parts.append(_re.sub(r'<[^>]+>', '', d["title"]))
            if d.get("text"):
                parts.append(_re.sub(r'<[^>]+>', '', d["text"]))
        if block["type"] == "numberbox":
            for it in d.get("items", []):
                if isinstance(it, dict):
                    if it.get("value"):
                        parts.append(it["value"])
                    if it.get("label"):
                        parts.append(it["label"])
    return "\n".join(parts)


# Russian date parsing is now in qazstack.content.parse_ru_date
# Kept as thin wrapper for backward compatibility
def _parse_ru_date_text(text):
    """Парсить '13 апреля 2016, 11:38' в ISO формат."""
    dt = parse_ru_date(text)
    return dt.isoformat() if dt else None


def import_jsonl(jsonl_path: str) -> dict:
    """Import articles from JSONL file into SQLite. Returns stats.

    Uses qazstack.content.iter_jsonl for parsing + date fallback.
    """
    init_db()
    imported = 0
    skipped = 0
    errors = 0

    with get_db() as conn:
        for article in iter_jsonl(jsonl_path, fix_dates=True):
            try:
                tags_json = json.dumps(article.tags, ensure_ascii=False)
                images_json = json.dumps(article.inline_images, ensure_ascii=False)
                pub_date = article.pub_date.isoformat() if article.pub_date else None

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
                    article.url,
                    pub_date,
                    article.sub_category,
                    article.category_label,
                    article.title,
                    article.author,
                    article.excerpt,
                    article.body_text,
                    article.body_html,
                    article.main_image,
                    article.image_credit,
                    article.thumbnail,
                    tags_json,
                    images_json,
                ))
                imported += 1
            except Exception:
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
    status: str = "",
    assigned_to: str = "",
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

        if status:
            conditions.append("a.status = ?")
            params.append(status)

        if author:
            conditions.append("a.author = ?")
            params.append(author)

        if assigned_to:
            conditions.append("a.assigned_to = ?")
            params.append(assigned_to)

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
                   a.title, a.author, a.excerpt, a.thumbnail, a.main_image,
                   a.status, a.updated_at, a.assigned_to
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


def update_article(article_id: int, updates: dict) -> None:
    """Update article fields."""
    allowed = {"title", "excerpt", "sub_category", "author", "main_image",
               "body_html", "body_text", "status", "editor_note", "updated_at",
               "body_blocks", "scheduled_at", "focal_x", "focal_y", "assigned_to"}
    with get_db() as conn:
        # Separate tags (JSON) from scalar fields
        tags = updates.pop("tags", None)
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if filtered:
            cols = []
            vals = []
            for k, v in filtered.items():
                cols.append(f"{k} = ?")
                vals.append(v)
            vals.append(article_id)
            conn.execute(f"UPDATE articles SET {', '.join(cols)} WHERE id = ?", vals)
        if tags is not None:
            conn.execute("UPDATE articles SET tags = ? WHERE id = ?", (json.dumps(tags, ensure_ascii=False), article_id))
        conn.commit()


def create_article(data: dict) -> int:
    """Create a new article. Returns the new article ID."""
    with get_db() as conn:
        tags_json = json.dumps(data.get("tags", []), ensure_ascii=False)
        now = datetime.now().isoformat(timespec="seconds")
        row = conn.execute("""
            INSERT INTO articles (url, pub_date, sub_category, category_label, title, author,
                                  excerpt, body_text, body_html, main_image, tags, status,
                                  updated_at, editor_note, imported_at,
                                  body_blocks, scheduled_at, focal_x, focal_y)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("url", ""),
            data.get("pub_date", now),
            data.get("sub_category", ""),
            data.get("category_label", ""),
            data.get("title", ""),
            data.get("author", ""),
            data.get("excerpt", ""),
            data.get("body_text", ""),
            data.get("body_html", ""),
            data.get("main_image", ""),
            tags_json,
            data.get("status", "draft"),
            now,
            data.get("editor_note", ""),
            now,
            data.get("body_blocks", None),
            data.get("scheduled_at", None),
            data.get("focal_x", 0.5),
            data.get("focal_y", 0.5),
        ))
        conn.commit()
        return row.lastrowid


def record_revision(article_id: int, changes: dict, revision_type: str = "edit", changed_by: str = "") -> None:
    """Record a revision for an article."""
    with get_db() as conn:
        conn.execute("""
            INSERT INTO article_revisions (article_id, changed_at, changed_by, changes_json, revision_type)
            VALUES (?, datetime('now'), ?, ?, ?)
        """, (article_id, changed_by, json.dumps(changes, ensure_ascii=False), revision_type))
        conn.commit()


def get_revisions(article_id: int, limit: int = 20) -> list:
    """Get revision history for an article."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, article_id, changed_at, changed_by, changes_json, revision_type
            FROM article_revisions
            WHERE article_id = ?
            ORDER BY changed_at DESC
            LIMIT ?
        """, (article_id, limit)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["changes"] = json.loads(d.get("changes_json") or "{}")
            del d["changes_json"]
            result.append(d)
        return result


def restore_revision(article_id: int, revision_id: int) -> bool:
    """Restore an article to a previous revision (stub for SQLite backend)."""
    return False


def duplicate_article(article_id: int) -> int | None:
    """Duplicate an article. Returns new article ID or None."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        now = datetime.now().isoformat(timespec="seconds")
        import random
        import string
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        old_url = d.get("url", "")
        new_url = old_url.rstrip("/") + "-copy-" + suffix if old_url else f"https://total.kz/copy-{suffix}"
        new_title = "Копия: " + (d.get("title") or "")
        tags_val = d.get("tags") or "[]"

        result = conn.execute("""
            INSERT INTO articles (url, pub_date, sub_category, category_label, title, author,
                                  excerpt, body_text, body_html, main_image, image_credit,
                                  thumbnail, tags, inline_images, status, updated_at, editor_note, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?)
        """, (
            new_url,
            d.get("pub_date"),
            d.get("sub_category", ""),
            d.get("category_label", ""),
            new_title,
            d.get("author", ""),
            d.get("excerpt", ""),
            d.get("body_text", ""),
            d.get("body_html", ""),
            d.get("main_image", ""),
            d.get("image_credit", ""),
            d.get("thumbnail", ""),
            tags_val,
            d.get("inline_images") or "[]",
            now,
            d.get("editor_note", ""),
            now,
        ))
        conn.commit()
        return result.lastrowid


def _load_enrichment(conn, article_id: int) -> dict | None:
    """Load GPT enrichment data for an article. Returns None gracefully if table missing."""
    try:
        row = conn.execute("""
            SELECT summary, meta_description, keywords, quote, quote_author
            FROM article_enrichments WHERE article_id = ?
        """, (article_id,)).fetchone()
        if row:
            d = dict(row)
            d["keywords"] = json.loads(d.get("keywords") or "[]")
            return d
    except Exception:
        pass
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
            d["enrichment"] = _load_enrichment(conn, d["id"])
            return d
        return None


def get_latest_articles(limit: int = 20, offset: int = 0) -> list:
    """Get latest articles for homepage."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, url, pub_date, sub_category, title, author, excerpt,
                   thumbnail, main_image, COALESCE(views, 0) as views
            FROM articles
            WHERE pub_date IS NOT NULL AND pub_date != ''
            ORDER BY pub_date DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()
        articles = [dict(r) for r in rows]
        # Backfill empty excerpts with GPT summaries
        try:
            ids = [a["id"] for a in articles if not a.get("excerpt")]
            if ids:
                placeholders = ','.join('?' * len(ids))
                sums = conn.execute(f"""
                    SELECT article_id, summary FROM article_enrichments
                    WHERE article_id IN ({placeholders})
                """, ids).fetchall()
                smap = {r["article_id"]: r["summary"] for r in sums}
                for a in articles:
                    if not a.get("excerpt") and a["id"] in smap:
                        a["excerpt"] = smap[a["id"]]
        except Exception:
            pass
        return articles


def get_latest_by_category(category: str, limit: int = 10, offset: int = 0) -> dict:
    """Get latest articles for a category with pagination."""
    with get_db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE sub_category = ?", (category,)
        ).fetchone()[0]
        rows = conn.execute("""
            SELECT id, url, pub_date, sub_category, title, author, excerpt,
                   thumbnail, main_image, COALESCE(views, 0) as views
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


def get_story_timeline(article_id: int, pub_date: str) -> dict | None:
    """Get story-based timeline for an article.
    Returns {story_title, prev: [...], next: [...]} or None if no story found.
    Only returns results for stories with 2+ articles (confidence >= 0.3).
    """
    try:
        return _get_story_timeline_inner(article_id, pub_date)
    except Exception:
        return None


def _get_story_timeline_inner(article_id: int, pub_date: str) -> dict | None:
    with get_db() as conn:
        story_row = conn.execute("""
            SELECT s.id, s.title_ru, s.article_count
            FROM article_stories as2
            JOIN stories s ON s.id = as2.story_id
            WHERE as2.article_id = ? AND as2.confidence >= 0.3
            ORDER BY as2.confidence DESC
            LIMIT 1
        """, (article_id,)).fetchone()

        if not story_row or story_row["article_count"] < 2:
            return None

        story_id = story_row["id"]
        story_title = story_row["title_ru"]

        # Previous articles in this story (older)
        prev_rows = conn.execute("""
            SELECT a.id, a.url, a.pub_date, a.sub_category, a.title, a.thumbnail, a.main_image
            FROM articles a
            JOIN article_stories as2 ON as2.article_id = a.id
            WHERE as2.story_id = ? AND a.id != ? AND a.pub_date <= ?
              AND as2.confidence >= 0.3
            ORDER BY a.pub_date DESC
            LIMIT 5
        """, (story_id, article_id, pub_date)).fetchall()

        # Next articles in this story (newer)
        next_rows = conn.execute("""
            SELECT a.id, a.url, a.pub_date, a.sub_category, a.title, a.thumbnail, a.main_image
            FROM articles a
            JOIN article_stories as2 ON as2.article_id = a.id
            WHERE as2.story_id = ? AND a.id != ? AND a.pub_date > ?
              AND as2.confidence >= 0.3
            ORDER BY a.pub_date ASC
            LIMIT 5
        """, (story_id, article_id, pub_date)).fetchall()

        return {
            "story_title": story_title,
            "total_articles": story_row["article_count"],
            "prev": [dict(r) for r in prev_rows],
            "next": [dict(r) for r in next_rows],
        }


def get_related_by_entities(article_id: int, entity_ids: list, category: str, limit: int = 6) -> list:
    """Get related articles using multi-signal ranking.

    Scoring layers (combined with weights):
    1. FTS5 BM25 on title+excerpt+keywords — works for ALL articles
    2. Shared entities — strong topical signal
    3. Shared tags — complementary signal
    4. Same category boost
    5. Recency decay — prefer recent but don't exclude old

    Falls back to simpler methods if FTS5 is unavailable.
    """
    with get_db() as conn:
        # Check if FTS5 index exists
        fts_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='articles_fts'"
        ).fetchone()

        if fts_exists:
            return _get_related_smart(conn, article_id, entity_ids, category, limit)
        else:
            return _get_related_legacy(conn, article_id, entity_ids, category, limit)


def _get_related_smart(conn, article_id: int, entity_ids: list, category: str, limit: int = 6) -> list:
    """Multi-signal related articles using FTS5 + entities + tags."""
    import logging
    logger = logging.getLogger(__name__)

    # Get current article's title for FTS query
    cur = conn.execute(
        "SELECT title, sub_category FROM articles WHERE id = ?", (article_id,)
    ).fetchone()
    if not cur:
        return []

    title = cur[0] or ""
    # Clean title for FTS5 query: remove punctuation, keep words
    import re
    words = re.findall(r'[\w]+', title, re.UNICODE)
    # Use top 6 words (skip very short ones) for broader matching
    query_words = [w for w in words if len(w) > 2][:8]
    if not query_words:
        query_words = words[:5]
    fts_query = ' OR '.join(query_words) if query_words else None

    candidates = {}  # article_id -> score dict

    # --- Signal 1: FTS5 BM25 (title similarity) ---
    if fts_query:
        try:
            fts_rows = conn.execute("""
                SELECT f.rowid, f.title, f.rank,
                       a.url, a.pub_date, a.sub_category, a.author, a.excerpt,
                       a.thumbnail, a.main_image
                FROM articles_fts f
                JOIN articles a ON a.id = f.rowid
                WHERE articles_fts MATCH ? AND f.rowid != ?
                ORDER BY f.rank
                LIMIT 30
            """, (fts_query, article_id)).fetchall()

            for i, row in enumerate(fts_rows):
                aid = row[0]
                # BM25 rank is negative (more negative = better match)
                bm25_score = -row[2]  # Make positive
                normalized = min(bm25_score / 15.0, 1.0)  # Normalize to ~0-1
                candidates[aid] = {
                    'id': aid, 'title': row[1], 'url': row[3],
                    'pub_date': row[4], 'sub_category': row[5],
                    'author': row[6], 'excerpt': row[7],
                    'thumbnail': row[8], 'main_image': row[9],
                    'fts_score': normalized * 3.0,  # Weight: 3x
                    'entity_score': 0.0,
                    'tag_score': 0.0,
                    'cat_score': 0.0,
                }
        except Exception as e:
            logger.warning("FTS5 query failed: %s", e)

    # --- Signal 2: Shared entities ---
    if entity_ids:
        placeholders = ','.join('?' * len(entity_ids))
        ent_rows = conn.execute(f"""
            SELECT a.id, a.url, a.pub_date, a.sub_category, a.title, a.author,
                   a.excerpt, a.thumbnail, a.main_image,
                   COUNT(DISTINCT ae.entity_id) as shared
            FROM articles a
            JOIN article_entities ae ON ae.article_id = a.id
            WHERE ae.entity_id IN ({placeholders}) AND a.id != ?
            GROUP BY a.id
            ORDER BY shared DESC
            LIMIT 20
        """, (*entity_ids, article_id)).fetchall()

        max_shared = max((r[9] for r in ent_rows), default=1)
        for row in ent_rows:
            aid = row[0]
            entity_normalized = row[9] / max(max_shared, 1)
            if aid in candidates:
                candidates[aid]['entity_score'] = entity_normalized * 4.0  # Weight: 4x
            else:
                candidates[aid] = {
                    'id': aid, 'title': row[4], 'url': row[1],
                    'pub_date': row[2], 'sub_category': row[3],
                    'author': row[5], 'excerpt': row[6],
                    'thumbnail': row[7], 'main_image': row[8],
                    'fts_score': 0.0,
                    'entity_score': entity_normalized * 4.0,
                    'tag_score': 0.0,
                    'cat_score': 0.0,
                }

    # --- Signal 3: Shared tags ---
    tag_rows = conn.execute("""
        SELECT a.id, a.url, a.pub_date, a.sub_category, a.title, a.author,
               a.excerpt, a.thumbnail, a.main_image,
               COUNT(DISTINCT t2.tag) as shared_tags
        FROM article_tags t1
        JOIN article_tags t2 ON t1.tag = t2.tag AND t2.article_id != t1.article_id
        JOIN articles a ON a.id = t2.article_id
        WHERE t1.article_id = ?
        GROUP BY a.id
        ORDER BY shared_tags DESC
        LIMIT 15
    """, (article_id,)).fetchall()

    max_tags = max((r[9] for r in tag_rows), default=1)
    for row in tag_rows:
        aid = row[0]
        tag_normalized = row[9] / max(max_tags, 1)
        if aid in candidates:
            candidates[aid]['tag_score'] = tag_normalized * 2.0  # Weight: 2x
        else:
            candidates[aid] = {
                'id': aid, 'title': row[4], 'url': row[1],
                'pub_date': row[2], 'sub_category': row[3],
                'author': row[5], 'excerpt': row[6],
                'thumbnail': row[7], 'main_image': row[8],
                'fts_score': 0.0,
                'entity_score': 0.0,
                'tag_score': tag_normalized * 2.0,
                'cat_score': 0.0,
            }

    # --- Signal 4: Category boost ---
    for aid, c in candidates.items():
        if c['sub_category'] == category:
            c['cat_score'] = 1.0  # Weight: 1x

    # --- Final ranking ---
    for c in candidates.values():
        c['total_score'] = (
            c['fts_score'] + c['entity_score'] +
            c['tag_score'] + c['cat_score']
        )

    ranked = sorted(candidates.values(), key=lambda x: x['total_score'], reverse=True)

    # Convert to standard format
    results = []
    for c in ranked[:limit]:
        results.append({
            'id': c['id'], 'url': c['url'], 'pub_date': c['pub_date'],
            'sub_category': c['sub_category'], 'title': c['title'],
            'author': c['author'], 'excerpt': c['excerpt'],
            'thumbnail': c['thumbnail'], 'main_image': c['main_image'],
        })

    # Fill remaining slots from category if needed
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


def _get_related_legacy(conn, article_id: int, entity_ids: list, category: str, limit: int = 6) -> list:
    """Original entity-based fallback (no FTS5 index)."""
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


def get_category_highlights_batch(nav_sections: list, per_section: int = 3) -> dict:
    """Fetch top N articles per nav section for homepage highlights.

    Returns: dict mapping section_slug -> list[dict]
    """
    result = {}
    with get_db() as conn:
        for section in nav_sections:
            subcats = section.get("subcats", [])
            if not subcats:
                continue
            placeholders = ",".join("?" for _ in subcats)
            rows = conn.execute(f"""
                SELECT id, url, pub_date, sub_category, title, author,
                       excerpt, thumbnail, main_image, COALESCE(views, 0) as views
                FROM articles
                WHERE sub_category IN ({placeholders})
                ORDER BY
                    CASE WHEN main_image IS NOT NULL AND main_image != '' THEN 0 ELSE 1 END,
                    pub_date DESC
                LIMIT ?
            """, (*subcats, per_section)).fetchall()
            if rows:
                result[section["slug"]] = [dict(r) for r in rows]
    return result


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
                   thumbnail, main_image, COALESCE(views, 0) as views
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
            SELECT url, pub_date, sub_category, main_image, title
            FROM articles
            WHERE pub_date IS NOT NULL
            ORDER BY pub_date DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]


# ══════════════════════════════════════════════
# CMS v11 – Users, Categories, Authors, Media,
#            Audit, Stories, Tags, Entities
# ══════════════════════════════════════════════

# ── Users ────────────────────────────────────

def get_user_by_username(username: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return dict(row) if row else None


def get_user(user_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return dict(row) if row else None


def get_all_users() -> list:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]


def create_user(data: dict) -> int:
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO users (username, email, password_hash, display_name, role, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (data["username"], data.get("email", ""), data["password_hash"],
              data["display_name"], data.get("role", "journalist"), data.get("is_active", 1)))
        conn.commit()
        return row.lastrowid


def update_user(user_id: int, updates: dict) -> None:
    allowed = {"email", "display_name", "role", "is_active", "avatar_url", "password_hash", "last_login"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_db() as conn:
        cols = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [user_id]
        conn.execute(f"UPDATE users SET {cols} WHERE id = ?", vals)
        conn.commit()


def delete_user(user_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()


# ── Categories ───────────────────────────────

def get_all_categories() -> list:
    with get_db() as conn:
        rows = conn.execute("""
            SELECT c.*, (SELECT COUNT(*) FROM articles WHERE sub_category = c.slug) as live_count
            FROM categories c ORDER BY c.sort_order, c.name_ru
        """).fetchall()
        return [dict(r) for r in rows]


def get_category(cat_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM categories WHERE id = ?", (cat_id,)).fetchone()
        return dict(row) if row else None


def create_category(data: dict) -> int:
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO categories (slug, name_ru, name_kz, parent_id, sort_order, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (data["slug"], data["name_ru"], data.get("name_kz", ""),
              data.get("parent_id"), data.get("sort_order", 0), data.get("is_active", 1)))
        conn.commit()
        return row.lastrowid


def update_category(cat_id: int, updates: dict) -> None:
    allowed = {"name_ru", "name_kz", "slug", "parent_id", "sort_order", "is_active"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_db() as conn:
        cols = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [cat_id]
        conn.execute(f"UPDATE categories SET {cols} WHERE id = ?", vals)
        conn.commit()


def delete_category(cat_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM categories WHERE id = ?", (cat_id,))
        conn.commit()


# ── Authors Managed ──────────────────────────

def get_all_authors_managed(q: str = "") -> list:
    with get_db() as conn:
        if q:
            rows = conn.execute("""
                SELECT am.*, (SELECT COUNT(*) FROM articles WHERE author = am.name) as live_count,
                       (SELECT MAX(pub_date) FROM articles WHERE author = am.name) as last_published
                FROM authors_managed am WHERE am.name LIKE ? ORDER BY am.article_count DESC
            """, (f"%{q}%",)).fetchall()
        else:
            rows = conn.execute("""
                SELECT am.*, (SELECT COUNT(*) FROM articles WHERE author = am.name) as live_count,
                       (SELECT MAX(pub_date) FROM articles WHERE author = am.name) as last_published
                FROM authors_managed am ORDER BY am.article_count DESC
            """).fetchall()
        return [dict(r) for r in rows]


def get_author_managed(author_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM authors_managed WHERE id = ?", (author_id,)).fetchone()
        return dict(row) if row else None


def create_author_managed(data: dict) -> int:
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO authors_managed (name, slug, bio, avatar_url, email, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (data["name"], data["slug"], data.get("bio", ""),
              data.get("avatar_url", ""), data.get("email", ""), data.get("is_active", 1)))
        conn.commit()
        return row.lastrowid


def update_author_managed(author_id: int, updates: dict) -> None:
    allowed = {"name", "slug", "bio", "avatar_url", "email", "is_active"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_db() as conn:
        cols = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [author_id]
        conn.execute(f"UPDATE authors_managed SET {cols} WHERE id = ?", vals)
        conn.commit()


def delete_author_managed(author_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM authors_managed WHERE id = ?", (author_id,))
        conn.commit()


# ── Media Library ────────────────────────────

def get_all_media(q: str = "", page: int = 1, per_page: int = 30, sort: str = "newest", media_type: str = "") -> dict:
    with get_db() as conn:
        conditions = []
        params = []
        if q:
            conditions.append("(m.original_name LIKE ? OR m.alt_text LIKE ?)")
            params.extend([f"%{q}%", f"%{q}%"])
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        total = conn.execute(f"SELECT COUNT(*) FROM media m {where}", params).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute(f"""
            SELECT * FROM media m {where} ORDER BY m.uploaded_at DESC LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()
        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }


def create_media(data: dict) -> int:
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO media (filename, original_name, mime_type, file_size, url,
                               width, height, alt_text, credit, uploaded_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (data["filename"], data["original_name"], data["mime_type"],
              data["file_size"], data["url"], data.get("width"), data.get("height"),
              data.get("alt_text", ""), data.get("credit", ""), data.get("uploaded_by")))
        conn.commit()
        return row.lastrowid


def update_media(media_id: int, updates: dict) -> None:
    allowed = {"alt_text", "credit", "original_name"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_db() as conn:
        cols = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [media_id]
        conn.execute(f"UPDATE media SET {cols} WHERE id = ?", vals)
        conn.commit()


def delete_media(media_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM media WHERE id = ?", (media_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        conn.execute("DELETE FROM media WHERE id = ?", (media_id,))
        conn.commit()
        return d


# ── Tags CRUD ────────────────────────────────

def get_tags_full(q: str = "", page: int = 1, per_page: int = 50, min_articles: int = 0) -> dict:
    with get_db() as conn:
        conditions = []
        params = []
        if q:
            conditions.append("at.tag LIKE ?")
            params.append(f"%{q}%")
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        having = f"HAVING article_count >= {int(min_articles)}" if min_articles > 0 else ""
        total = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT at.tag, COUNT(*) as article_count
                FROM article_tags at {where}
                GROUP BY at.tag {having}
            )
        """, params).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute(f"""
            SELECT at.tag, COUNT(*) as article_count
            FROM article_tags at {where}
            GROUP BY at.tag {having}
            ORDER BY article_count DESC
            LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()
        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }


def rename_tag(old_tag: str, new_tag: str) -> int:
    with get_db() as conn:
        cur = conn.execute("UPDATE article_tags SET tag = ? WHERE tag = ?", (new_tag, old_tag))
        conn.execute("UPDATE articles SET tags = REPLACE(tags, ?, ?) WHERE tags LIKE ?",
                      (f'"{old_tag}"', f'"{new_tag}"', f'%{old_tag}%'))
        conn.commit()
        return cur.rowcount


def merge_tags(tags: list, target_tag: str) -> int:
    """Merge multiple tags into one target tag."""
    total = 0
    with get_db() as conn:
        for tag in tags:
            if tag == target_tag:
                continue
            # Get articles with the source tag
            rows = conn.execute("SELECT article_id FROM article_tags WHERE tag = ?", (tag,)).fetchall()
            for row in rows:
                # Try inserting target tag, ignore if already exists
                try:
                    conn.execute("INSERT OR IGNORE INTO article_tags (article_id, tag) VALUES (?, ?)",
                                 (row[0], target_tag))
                except Exception:
                    pass
            # Delete old tag
            cur = conn.execute("DELETE FROM article_tags WHERE tag = ?", (tag,))
            total += cur.rowcount
            # Update JSON tags field
            conn.execute("UPDATE articles SET tags = REPLACE(tags, ?, ?) WHERE tags LIKE ?",
                          (f'"{tag}"', f'"{target_tag}"', f'%{tag}%'))
        conn.commit()
    return total


def delete_tag(tag: str) -> int:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM article_tags WHERE tag = ?", (tag,))
        conn.commit()
        return cur.rowcount


# ── Entities CRUD ────────────────────────────

def get_entities_full(q: str = "", entity_type: str = "", page: int = 1, per_page: int = 50) -> dict:
    with get_db() as conn:
        conditions = []
        params = []
        if q:
            conditions.append("e.name LIKE ?")
            params.append(f"%{q}%")
        if entity_type:
            conditions.append("e.entity_type = ?")
            params.append(entity_type)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        total = conn.execute(f"SELECT COUNT(*) FROM entities e {where}", params).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute(f"""
            SELECT e.*, COUNT(ae.article_id) as article_count
            FROM entities e
            LEFT JOIN article_entities ae ON ae.entity_id = e.id
            {where}
            GROUP BY e.id
            ORDER BY article_count DESC
            LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()
        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }


def create_entity(data: dict) -> int:
    with get_db() as conn:
        name = data["name"]
        normalized = data.get("normalized", name.lower().strip())
        row = conn.execute("""
            INSERT INTO entities (name, short_name, entity_type, normalized)
            VALUES (?, ?, ?, ?)
        """, (name, data.get("short_name", ""), data["entity_type"], normalized))
        conn.commit()
        return row.lastrowid


def update_entity(entity_id: int, updates: dict) -> None:
    allowed = {"name", "short_name", "entity_type", "normalized"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_db() as conn:
        cols = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [entity_id]
        conn.execute(f"UPDATE entities SET {cols} WHERE id = ?", vals)
        conn.commit()


def delete_entity(entity_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (entity_id,))
        conn.execute("DELETE FROM entities WHERE id = ?", (entity_id,))
        conn.commit()


def merge_entities(entity_ids: list, target_id: int) -> int:
    total = 0
    with get_db() as conn:
        for eid in entity_ids:
            if eid == target_id:
                continue
            rows = conn.execute("SELECT article_id, mention_count FROM article_entities WHERE entity_id = ?",
                                 (eid,)).fetchall()
            for row in rows:
                existing = conn.execute(
                    "SELECT mention_count FROM article_entities WHERE article_id = ? AND entity_id = ?",
                    (row[0], target_id)).fetchone()
                if existing:
                    conn.execute("""
                        UPDATE article_entities SET mention_count = mention_count + ?
                        WHERE article_id = ? AND entity_id = ?
                    """, (row[1], row[0], target_id))
                else:
                    conn.execute("""
                        INSERT INTO article_entities (article_id, entity_id, mention_count)
                        VALUES (?, ?, ?)
                    """, (row[0], target_id, row[1]))
            conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (eid,))
            conn.execute("DELETE FROM entities WHERE id = ?", (eid,))
            total += 1
        conn.commit()
    return total


# ── Stories ──────────────────────────────────

def get_all_stories(q: str = "", page: int = 1, per_page: int = 30) -> dict:
    with get_db() as conn:
        try:
            conn.execute("SELECT 1 FROM stories LIMIT 1")
        except Exception:
            return {"items": [], "total": 0, "page": 1, "pages": 1}
        conditions = []
        params = []
        if q:
            conditions.append("s.title_ru LIKE ?")
            params.append(f"%{q}%")
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        total = conn.execute(f"SELECT COUNT(*) FROM stories s {where}", params).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute(f"""
            SELECT s.*,
                   (SELECT COUNT(*) FROM story_articles sa WHERE sa.story_id = s.id) as article_count,
                   (SELECT MIN(a2.pub_date) FROM story_articles sa2 JOIN articles a2 ON a2.id = sa2.article_id WHERE sa2.story_id = s.id) as first_date,
                   (SELECT MAX(a3.pub_date) FROM story_articles sa3 JOIN articles a3 ON a3.id = sa3.article_id WHERE sa3.story_id = s.id) as last_date
            FROM stories s {where}
            ORDER BY s.id DESC LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()
        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }


def get_story(story_id: int) -> dict | None:
    with get_db() as conn:
        try:
            row = conn.execute("SELECT * FROM stories WHERE id = ?", (story_id,)).fetchone()
            if not row:
                return None
            d = dict(row)
            articles = conn.execute("""
                SELECT a.id, a.title, a.pub_date, a.sub_category, a.thumbnail, a.main_image,
                       a.author, a.excerpt, ars.confidence
                FROM article_stories ars
                JOIN articles a ON a.id = ars.article_id
                WHERE ars.story_id = ?
                ORDER BY a.pub_date ASC
            """, (story_id,)).fetchall()
            d["articles"] = [dict(a) for a in articles]
            return d
        except Exception:
            return None


def create_story(data: dict) -> int:
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO stories (title_ru, description, article_count)
            VALUES (?, ?, 0)
        """, (data["title_ru"], data.get("description", "")))
        conn.commit()
        return row.lastrowid


def update_story(story_id: int, updates: dict) -> None:
    allowed = {"title_ru", "description"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_db() as conn:
        cols = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [story_id]
        conn.execute(f"UPDATE stories SET {cols} WHERE id = ?", vals)
        conn.commit()


def delete_story(story_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM article_stories WHERE story_id = ?", (story_id,))
        conn.execute("DELETE FROM stories WHERE id = ?", (story_id,))
        conn.commit()


def add_article_to_story(story_id: int, article_id: int) -> None:
    with get_db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO article_stories (story_id, article_id, confidence)
            VALUES (?, ?, 1.0)
        """, (story_id, article_id))
        conn.execute("UPDATE stories SET article_count = (SELECT COUNT(*) FROM article_stories WHERE story_id = ?) WHERE id = ?",
                      (story_id, story_id))
        conn.commit()


def remove_article_from_story(story_id: int, article_id: int) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM article_stories WHERE story_id = ? AND article_id = ?",
                      (story_id, article_id))
        conn.execute("UPDATE stories SET article_count = (SELECT COUNT(*) FROM article_stories WHERE story_id = ?) WHERE id = ?",
                      (story_id, story_id))
        conn.commit()


# ── Audit Log ────────────────────────────────

def log_audit(user_id: int, username: str, action: str, entity_type: str,
              entity_id: int = None, details: str = "", ip_address: str = "") -> None:
    with get_db() as conn:
        conn.execute("""
            INSERT INTO audit_log (user_id, username, action, entity_type, entity_id, details, ip_address)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, username, action, entity_type, entity_id, details, ip_address))
        conn.commit()


def get_audit_log(user_id: int = 0, action: str = "", entity_type: str = "",
                  date_from: str = "", date_to: str = "",
                  page: int = 1, per_page: int = 50) -> dict:
    with get_db() as conn:
        conditions = []
        params = []
        if user_id:
            conditions.append("al.user_id = ?")
            params.append(user_id)
        if action:
            conditions.append("al.action = ?")
            params.append(action)
        if entity_type:
            conditions.append("al.entity_type = ?")
            params.append(entity_type)
        if date_from:
            conditions.append("al.created_at >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("al.created_at <= ? || ' 23:59:59'")
            params.append(date_to)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        total = conn.execute(f"SELECT COUNT(*) FROM audit_log al {where}", params).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute(f"""
            SELECT al.* FROM audit_log al {where}
            ORDER BY al.created_at DESC LIMIT ? OFFSET ?
        """, params + [per_page, offset]).fetchall()
        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page),
        }


# ── Bulk operations on articles ──────────────

def bulk_update_articles(article_ids: list, updates: dict) -> int:
    """Bulk update status/category for multiple articles."""
    if not article_ids:
        return 0
    allowed = {"status", "sub_category"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return 0
    with get_db() as conn:
        placeholders = ",".join("?" * len(article_ids))
        cols = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + article_ids
        cur = conn.execute(f"UPDATE articles SET {cols}, updated_at = datetime('now') WHERE id IN ({placeholders})", vals)
        conn.commit()
        return cur.rowcount


def bulk_delete_articles(article_ids: list) -> int:
    if not article_ids:
        return 0
    with get_db() as conn:
        placeholders = ",".join("?" * len(article_ids))
        cur = conn.execute(f"UPDATE articles SET status = 'archived', updated_at = datetime('now') WHERE id IN ({placeholders})",
                            article_ids)
        conn.commit()
        return cur.rowcount


# ══════════════════════════════════════════════
#  v14: AD PLACEMENTS CRUD
# ══════════════════════════════════════════════

def get_all_ad_placements(page_filter: str = "") -> list:
    """Get all ad placements, optionally filtered by page."""
    with get_db() as conn:
        if page_filter:
            rows = conn.execute(
                "SELECT * FROM ad_placements WHERE page = ? ORDER BY slot_id",
                (page_filter,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM ad_placements ORDER BY slot_id"
            ).fetchall()
        return [dict(r) for r in rows]


def get_ad_placement(slot_id: str) -> dict | None:
    """Get a single ad placement by slot_id."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM ad_placements WHERE slot_id = ?", (slot_id,)
        ).fetchone()
        return dict(row) if row else None


def toggle_ad_placement(slot_id: str) -> bool:
    """Toggle is_active for an ad placement. Returns new state."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT is_active FROM ad_placements WHERE slot_id = ?", (slot_id,)
        ).fetchone()
        if not row:
            return False
        new_state = 0 if row[0] else 1
        conn.execute(
            "UPDATE ad_placements SET is_active = ?, updated_at = datetime('now') WHERE slot_id = ?",
            (new_state, slot_id),
        )
        conn.commit()
        return bool(new_state)


def update_ad_placement(slot_id: str, data: dict) -> bool:
    """Update ad placement fields."""
    allowed = {
        'name', 'client_name', 'client_url', 'image_url',
        'start_date', 'end_date', 'cpm_rate', 'notes', 'is_active',
    }
    filtered = {k: v for k, v in data.items() if k in allowed}
    if not filtered:
        return False
    with get_db() as conn:
        cols = ", ".join(f"{k} = ?" for k in filtered)
        vals = list(filtered.values()) + [slot_id]
        conn.execute(
            f"UPDATE ad_placements SET {cols}, updated_at = datetime('now') WHERE slot_id = ?",
            vals,
        )
        conn.commit()
        return True


def get_ad_stats() -> dict:
    """Get summary stats for ad placements."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM ad_placements").fetchone()[0]
        active = conn.execute("SELECT COUNT(*) FROM ad_placements WHERE is_active = 1").fetchone()[0]
        booked = conn.execute("SELECT COUNT(*) FROM ad_placements WHERE client_name IS NOT NULL AND client_name != ''").fetchone()[0]
        return {'total': total, 'active': active, 'inactive': total - active, 'booked': booked, 'available': total - booked}
