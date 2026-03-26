"""PostgreSQL ORM queries for total-kz.

Drop-in replacement for app/database.py.  Same function signatures,
identical return types — uses SQLAlchemy 2.0 ORM instead of raw SQLite.

Switch backends by changing one import line:
    from app import database as db   →   from app import pg_queries as db
"""

import json
import random
import re as _re
import string
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import cast, delete as sa_delete
from sqlalchemy import distinct, func, or_, select, text, update as sa_update
from sqlalchemy import DateTime as SA_DateTime
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.models import (
    Article,
    ArticleComment,
    ArticleEnrichment,
    ArticleEntity,
    ArticleRevision,
    ArticleStory,
    ArticleTag,
    AuditLog,
    AuthorManaged,
    Base,
    CMSCategory,
    Media,
    NerEntity,
    ScrapeLog,
    ScrapeRun,
    Story,
    User,
)
from app.pg_database import SessionLocal, engine
from qazstack.content import iter_jsonl, parse_ru_date


# ═══════════════════════════════════════════════
# Session helper
# ═══════════════════════════════════════════════

@contextmanager
def get_pg_session():
    """Context manager that mirrors the SQLite ``get_db()`` contract."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ═══════════════════════════════════════════════
# Utility – copied verbatim from database.py
# ═══════════════════════════════════════════════

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
                txt = item.get("content", "") if isinstance(item, dict) else str(item)
                parts.append(_re.sub(r'<[^>]+>', '', txt))
        if "code" in d:
            parts.append(d["code"])
        if block["type"] == "table":
            for row in d.get("content", []):
                parts.append(" ".join(_re.sub(r'<[^>]+>', '', c) for c in row))
        if block["type"] == "warning":
            parts.append(_re.sub(r'<[^>]+>', '', d.get("title", "")))
            parts.append(_re.sub(r'<[^>]+>', '', d.get("message", "")))
    return "\n".join(parts)


def _parse_ru_date_text(text_val):
    """Парсить '13 апреля 2016, 11:38' в ISO формат."""
    dt = parse_ru_date(text_val)
    return dt.isoformat() if dt else None


# ═══════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════

_ARTICLE_COLUMNS = [
    "id", "url", "pub_date", "sub_category", "category_label", "title",
    "author", "excerpt", "body_text", "body_html", "main_image",
    "image_credit", "thumbnail", "tags", "inline_images", "imported_at",
    "status", "updated_at", "editor_note", "body_blocks", "scheduled_at",
    "focal_x", "focal_y", "assigned_to",
]

_ARTICLE_LIST_COLUMNS = [
    Article.id, Article.url, Article.pub_date, Article.sub_category,
    Article.title, Article.author, Article.excerpt, Article.thumbnail,
    Article.main_image, func.coalesce(Article.views, 0).label("views"),
]

_ARTICLE_SEARCH_COLUMNS = [
    Article.id, Article.url, Article.pub_date, Article.sub_category,
    Article.category_label, Article.title, Article.author, Article.excerpt,
    Article.thumbnail, Article.main_image, Article.status, Article.updated_at,
    Article.assigned_to,
]


def _article_to_dict(a: Article) -> dict:
    """Convert an Article ORM object to a dict matching the SQLite dict(row)."""
    d = {}
    for col in _ARTICLE_COLUMNS:
        d[col] = getattr(a, col, None)
    # JSONB columns are already native dicts/lists — normalise None → []
    if d["tags"] is None:
        d["tags"] = []
    if d["inline_images"] is None:
        d["inline_images"] = []
    return d


def _row_to_dict(row, keys: list[str]) -> dict:
    """Convert a SQLAlchemy Row (from .execute()) to a plain dict."""
    return dict(zip(keys, row))


def _load_enrichment(db: Session, article_id: int) -> dict | None:
    """Load GPT enrichment data for an article.  None if missing."""
    try:
        enr = db.get(ArticleEnrichment, article_id)
        if enr:
            return {
                "summary": enr.summary,
                "meta_description": enr.meta_description,
                "keywords": enr.keywords if enr.keywords else [],
                "quote": enr.quote,
                "quote_author": enr.quote_author,
            }
    except Exception:
        pass
    return None


def _paginate(total: int, per_page: int) -> int:
    return max(1, (total + per_page - 1) // per_page)


# ═══════════════════════════════════════════════
# Init / Import
# ═══════════════════════════════════════════════

_CAT_LABELS = {
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


def init_db() -> None:
    """Create all tables and seed initial data."""
    Base.metadata.create_all(engine)

    with get_pg_session() as db:
        # Seed admin user if users table is empty
        user_count = db.scalar(select(func.count()).select_from(User))
        if user_count == 0:
            from app import auth as _auth
            admin = User(
                username="admin",
                email="admin@total.kz",
                password_hash=_auth.hash_password("admin"),
                display_name="Администратор",
                role="admin",
                is_active=True,
            )
            db.add(admin)
            db.flush()

        # Auto-populate categories from articles.sub_category
        cat_count = db.scalar(select(func.count()).select_from(CMSCategory))
        if cat_count == 0:
            cats = db.execute(
                select(Article.sub_category, func.count().label("cnt"))
                .where(Article.sub_category.isnot(None), Article.sub_category != "")
                .group_by(Article.sub_category)
                .order_by(func.count().desc())
            ).all()
            for i, (slug, cnt) in enumerate(cats):
                name = _CAT_LABELS.get(slug, slug)
                db.add(CMSCategory(slug=slug, name_ru=name, sort_order=i, article_count=cnt))
            db.flush()

        # Auto-populate authors_managed from articles.author
        author_count = db.scalar(select(func.count()).select_from(AuthorManaged))
        if author_count == 0:
            authors_raw = db.execute(
                select(Article.author, func.count().label("cnt"))
                .where(Article.author.isnot(None), Article.author != "")
                .group_by(Article.author)
                .order_by(func.count().desc())
            ).all()
            for name, cnt in authors_raw:
                slug = name.lower().replace(" ", "-").replace(".", "")[:120]
                db.add(AuthorManaged(name=name, slug=slug, article_count=cnt))
            db.flush()


def import_jsonl(jsonl_path: str) -> dict:
    """Import articles from JSONL file into PostgreSQL.  Returns stats."""
    init_db()
    imported = 0
    skipped = 0
    errors = 0

    with get_pg_session() as db:
        for article in iter_jsonl(jsonl_path, fix_dates=True):
            try:
                pub_date = article.pub_date.isoformat() if article.pub_date else None
                tags_val = article.tags if article.tags else []
                images_val = article.inline_images if article.inline_images else []

                stmt = pg_insert(Article).values(
                    url=article.url,
                    pub_date=pub_date,
                    sub_category=article.sub_category,
                    category_label=article.category_label,
                    title=article.title,
                    author=article.author,
                    excerpt=article.excerpt,
                    body_text=article.body_text,
                    body_html=article.body_html,
                    main_image=article.main_image,
                    image_credit=article.image_credit,
                    thumbnail=article.thumbnail,
                    tags=tags_val,
                    inline_images=images_val,
                ).on_conflict_do_update(
                    index_elements=["url"],
                    set_={
                        "pub_date": func.coalesce(pg_insert(Article).excluded.pub_date, Article.pub_date),
                        "title": pg_insert(Article).excluded.title,
                        "author": pg_insert(Article).excluded.author,
                        "excerpt": pg_insert(Article).excluded.excerpt,
                        "body_text": pg_insert(Article).excluded.body_text,
                        "body_html": pg_insert(Article).excluded.body_html,
                        "main_image": pg_insert(Article).excluded.main_image,
                        "image_credit": pg_insert(Article).excluded.image_credit,
                        "thumbnail": pg_insert(Article).excluded.thumbnail,
                        "tags": pg_insert(Article).excluded.tags,
                        "inline_images": pg_insert(Article).excluded.inline_images,
                        "imported_at": func.now(),
                    },
                )
                db.execute(stmt)
                imported += 1
            except Exception:
                errors += 1
        db.flush()

    return {"imported": imported, "skipped": skipped, "errors": errors}


# ═══════════════════════════════════════════════
# Dashboard / Stats
# ═══════════════════════════════════════════════

def get_stats() -> dict:
    """Get dashboard statistics."""
    with get_pg_session() as db:
        total = db.scalar(select(func.count()).select_from(Article))

        cats = db.execute(
            select(Article.sub_category, func.count().label("cnt"))
            .group_by(Article.sub_category)
            .order_by(func.count().desc())
        ).all()

        month_col = func.substring(Article.pub_date, 1, 7).label("month")
        months = db.execute(
            select(month_col, func.count().label("cnt"))
            .where(Article.pub_date.isnot(None))
            .group_by(month_col)
            .order_by(month_col)
        ).all()

        year_col = func.substring(Article.pub_date, 1, 4).label("year")
        years = db.execute(
            select(year_col, func.count().label("cnt"))
            .where(Article.pub_date.isnot(None))
            .group_by(year_col)
            .order_by(year_col)
        ).all()

        authors = db.execute(
            select(Article.author, func.count().label("cnt"))
            .where(Article.author.isnot(None), Article.author != "")
            .group_by(Article.author)
            .order_by(func.count().desc())
            .limit(20)
        ).all()

        date_range = db.execute(
            select(func.min(Article.pub_date), func.max(Article.pub_date))
        ).one()

        runs = db.execute(
            select(ScrapeRun)
            .order_by(ScrapeRun.started_at.desc())
            .limit(20)
        ).scalars().all()

        # Avg recent 12 months vs previous 12
        # pub_date is text, so cast to timestamp for comparison
        _pub_ts = cast(Article.pub_date, SA_DateTime)
        _12m = text("CURRENT_DATE - INTERVAL '12 months'")
        _24m = text("CURRENT_DATE - INTERVAL '24 months'")

        avg_recent = db.scalar(
            select(func.round(func.avg(text("cnt")))).select_from(
                select(func.count().label("cnt"))
                .where(Article.pub_date.isnot(None), _pub_ts >= _12m)
                .group_by(func.substring(Article.pub_date, 1, 7))
                .subquery()
            )
        ) or 0

        avg_prev = db.scalar(
            select(func.round(func.avg(text("cnt")))).select_from(
                select(func.count().label("cnt"))
                .where(Article.pub_date.isnot(None), _pub_ts >= _24m, _pub_ts < _12m)
                .group_by(func.substring(Article.pub_date, 1, 7))
                .subquery()
            )
        ) or 0

        this_month_str = datetime.now().strftime("%Y-%m")
        this_month = db.scalar(
            select(func.count())
            .where(func.substring(Article.pub_date, 1, 7) == this_month_str)
        )

        # Previous month
        now = datetime.now()
        if now.month == 1:
            prev_m = f"{now.year - 1}-12"
        else:
            prev_m = f"{now.year}-{now.month - 1:02d}"
        last_month = db.scalar(
            select(func.count())
            .where(func.substring(Article.pub_date, 1, 7) == prev_m)
        )

        cat_by_year = db.execute(
            select(
                Article.sub_category,
                func.substring(Article.pub_date, 1, 4).label("year"),
                func.count().label("cnt"),
            )
            .where(Article.pub_date.isnot(None))
            .group_by(Article.sub_category, text("year"))
            .order_by(Article.sub_category, text("year"))
        ).all()

        def _run_to_dict(r: ScrapeRun) -> dict:
            return {
                "id": r.id, "started_at": r.started_at, "finished_at": r.finished_at,
                "phase": r.phase, "status": r.status, "articles_found": r.articles_found,
                "articles_downloaded": r.articles_downloaded, "errors": r.errors, "log": r.log,
            }

        return {
            "total": total,
            "categories": [{"sub_category": c, "cnt": n} for c, n in cats],
            "months": [{"month": m, "cnt": n} for m, n in months],
            "years": [{"year": y, "cnt": n} for y, n in years],
            "authors": [{"author": a, "cnt": n} for a, n in authors],
            "date_from": date_range[0],
            "date_to": date_range[1],
            "runs": [_run_to_dict(r) for r in runs],
            "avg_recent": int(avg_recent),
            "avg_prev": int(avg_prev),
            "this_month": this_month,
            "last_month": last_month,
            "cat_by_year": [{"sub_category": c, "year": y, "cnt": n} for c, y, n in cat_by_year],
        }


# ═══════════════════════════════════════════════
# Article CRUD
# ═══════════════════════════════════════════════

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
    with get_pg_session() as db:
        base = select(Article)
        count_q = select(func.count(distinct(Article.id)))

        conditions = []
        if query:
            conditions.append(or_(
                Article.title.ilike(f"%{query}%"),
                Article.body_text.ilike(f"%{query}%"),
            ))
        if category:
            conditions.append(Article.sub_category == category)
        if status:
            conditions.append(Article.status == status)
        if author:
            conditions.append(Article.author == author)
        if assigned_to:
            conditions.append(Article.assigned_to == assigned_to)
        if date_from:
            conditions.append(Article.pub_date >= date_from)
        if date_to:
            conditions.append(Article.pub_date <= date_to + " 23:59:59")

        if tag:
            base = base.join(ArticleTag, ArticleTag.article_id == Article.id)
            count_q = count_q.join(ArticleTag, ArticleTag.article_id == Article.id)
            conditions.append(ArticleTag.tag == tag)
        if entity_id:
            base = base.join(ArticleEntity, ArticleEntity.article_id == Article.id)
            count_q = count_q.join(ArticleEntity, ArticleEntity.article_id == Article.id)
            conditions.append(ArticleEntity.entity_id == entity_id)

        if conditions:
            base = base.where(*conditions)
            count_q = count_q.where(*conditions)

        total = db.scalar(count_q)
        offset = (page - 1) * per_page

        rows = db.execute(
            base.with_only_columns(*_ARTICLE_SEARCH_COLUMNS)
            .distinct()
            .order_by(Article.pub_date.desc())
            .limit(per_page)
            .offset(offset)
        ).all()

        keys = ["id", "url", "pub_date", "sub_category", "category_label",
                "title", "author", "excerpt", "thumbnail", "main_image",
                "status", "updated_at", "assigned_to"]
        return {
            "articles": [_row_to_dict(r, keys) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": _paginate(total, per_page),
        }


def get_authors() -> list:
    """Get all distinct authors sorted by article count."""
    with get_pg_session() as db:
        rows = db.execute(
            select(Article.author, func.count().label("cnt"))
            .where(Article.author.isnot(None), Article.author != "")
            .group_by(Article.author)
            .order_by(func.count().desc())
        ).all()
        return [{"author": a, "cnt": n} for a, n in rows]


def get_tags(limit: int = 100) -> list:
    """Get all tags sorted by usage count."""
    with get_pg_session() as db:
        rows = db.execute(
            select(ArticleTag.tag, func.count().label("cnt"))
            .group_by(ArticleTag.tag)
            .order_by(func.count().desc())
            .limit(limit)
        ).all()
        return [{"tag": t, "cnt": n} for t, n in rows]


def get_entities(entity_type: str = "", limit: int = 50) -> list:
    """Get entities sorted by mention count."""
    with get_pg_session() as db:
        q = (
            select(
                NerEntity.id, NerEntity.name, NerEntity.short_name,
                NerEntity.entity_type, NerEntity.normalized,
                func.count(ArticleEntity.article_id).label("article_count"),
            )
            .join(ArticleEntity, ArticleEntity.entity_id == NerEntity.id)
            .group_by(NerEntity.id)
            .order_by(text("article_count DESC"))
            .limit(limit)
        )
        if entity_type:
            q = q.where(NerEntity.entity_type == entity_type)
        rows = db.execute(q).all()
        return [
            {"id": r[0], "name": r[1], "short_name": r[2],
             "entity_type": r[3], "normalized": r[4], "article_count": r[5]}
            for r in rows
        ]


def get_article(article_id: int) -> dict | None:
    """Get full article by ID, including entities."""
    with get_pg_session() as db:
        article = db.get(Article, article_id)
        if not article:
            return None
        d = _article_to_dict(article)
        # Entities
        rows = db.execute(
            select(NerEntity.id, NerEntity.name, NerEntity.short_name,
                   NerEntity.entity_type, ArticleEntity.mention_count)
            .join(ArticleEntity, ArticleEntity.entity_id == NerEntity.id)
            .where(ArticleEntity.article_id == article_id)
            .order_by(ArticleEntity.mention_count.desc())
        ).all()
        d["entities"] = [
            {"id": r[0], "name": r[1], "short_name": r[2],
             "entity_type": r[3], "mention_count": r[4]}
            for r in rows
        ]
        return d


def update_article(article_id: int, updates: dict) -> None:
    """Update article fields."""
    allowed = {
        "title", "excerpt", "sub_category", "author", "main_image",
        "body_html", "body_text", "status", "editor_note", "updated_at",
        "body_blocks", "scheduled_at", "focal_x", "focal_y", "assigned_to",
    }
    with get_pg_session() as db:
        tags = updates.pop("tags", None)
        filtered = {k: v for k, v in updates.items() if k in allowed}
        if filtered or tags is not None:
            article = db.get(Article, article_id)
            if article:
                for k, v in filtered.items():
                    setattr(article, k, v)
                if tags is not None:
                    article.tags = tags
    # Invalidate page caches on article change
    try:
        from app import cache
        cache.invalidate_all()
    except Exception:
        pass


def create_article(data: dict) -> int:
    """Create a new article.  Returns the new article ID."""
    with get_pg_session() as db:
        now = datetime.now().isoformat(timespec="seconds")
        tags_val = data.get("tags", [])
        if isinstance(tags_val, str):
            tags_val = json.loads(tags_val)
        body_blocks_val = data.get("body_blocks")
        if isinstance(body_blocks_val, str):
            body_blocks_val = json.loads(body_blocks_val) if body_blocks_val else None
        article = Article(
            url=data.get("url", ""),
            pub_date=data.get("pub_date", now),
            sub_category=data.get("sub_category", ""),
            category_label=data.get("category_label", ""),
            title=data.get("title", ""),
            author=data.get("author", ""),
            excerpt=data.get("excerpt", ""),
            body_text=data.get("body_text", ""),
            body_html=data.get("body_html", ""),
            main_image=data.get("main_image", ""),
            tags=tags_val,
            status=data.get("status", "draft"),
            updated_at=now,
            editor_note=data.get("editor_note", ""),
            imported_at=now,
            body_blocks=body_blocks_val,
            scheduled_at=data.get("scheduled_at"),
            focal_x=data.get("focal_x", 0.5),
            focal_y=data.get("focal_y", 0.5),
        )
        db.add(article)
        db.flush()
        new_id = article.id
    # Invalidate page caches on new article
    try:
        from app import cache
        cache.invalidate_all()
    except Exception:
        pass
    return new_id


def record_revision(article_id: int, changes: dict, revision_type: str = "edit", changed_by: str = "") -> None:
    """Record a revision for an article, storing full article state as JSON."""
    with get_pg_session() as db:
        # Get current full article state before saving
        article = db.get(Article, article_id)
        full_state = {}
        if article:
            for field in ("title", "excerpt", "sub_category", "author", "main_image",
                          "body_html", "body_text", "status", "editor_note",
                          "body_blocks", "scheduled_at", "focal_x", "focal_y",
                          "image_credit", "assigned_to"):
                val = getattr(article, field, None)
                full_state[field] = val
            full_state["tags"] = article.tags
        revision_data = {"changes": changes, "full_state": full_state}
        rev = ArticleRevision(
            article_id=article_id,
            changed_by=changed_by,
            changes_json=json.dumps(revision_data, ensure_ascii=False),
            revision_type=revision_type,
        )
        db.add(rev)
        db.flush()
        # Limit to 20 revisions per article – delete oldest
        count = db.scalar(
            select(func.count()).select_from(ArticleRevision)
            .where(ArticleRevision.article_id == article_id)
        )
        if count > 20:
            oldest = db.execute(
                select(ArticleRevision.id)
                .where(ArticleRevision.article_id == article_id)
                .order_by(ArticleRevision.changed_at.asc())
                .limit(count - 20)
            ).scalars().all()
            if oldest:
                db.execute(
                    sa_delete(ArticleRevision).where(ArticleRevision.id.in_(oldest))
                )


def get_revisions(article_id: int, limit: int = 20) -> list:
    """Get revision history for an article."""
    with get_pg_session() as db:
        rows = db.execute(
            select(ArticleRevision)
            .where(ArticleRevision.article_id == article_id)
            .order_by(ArticleRevision.changed_at.desc())
            .limit(limit)
        ).scalars().all()
        result = []
        for r in rows:
            raw = json.loads(r.changes_json or "{}")
            # Support both old format (flat changes) and new format (changes + full_state)
            if "full_state" in raw:
                changes = raw.get("changes", {})
                has_full_state = True
            else:
                changes = raw
                has_full_state = False
            d = {
                "id": r.id, "article_id": r.article_id,
                "changed_at": r.changed_at, "changed_by": r.changed_by,
                "revision_type": r.revision_type,
                "changes": changes,
                "has_full_state": has_full_state,
            }
            result.append(d)
        return result


def restore_revision(article_id: int, revision_id: int) -> bool:
    """Restore an article to a previous revision's full state."""
    with get_pg_session() as db:
        rev = db.get(ArticleRevision, revision_id)
        if not rev or rev.article_id != article_id:
            return False
        raw = json.loads(rev.changes_json or "{}")
        full_state = raw.get("full_state")
        if not full_state:
            return False
        article = db.get(Article, article_id)
        if not article:
            return False
        allowed = {"title", "excerpt", "sub_category", "author", "main_image",
                   "body_html", "body_text", "status", "editor_note",
                   "body_blocks", "scheduled_at", "focal_x", "focal_y",
                   "image_credit", "assigned_to"}
        for field in allowed:
            if field in full_state:
                setattr(article, field, full_state[field])
        if "tags" in full_state:
            article.tags = full_state["tags"]
        article.updated_at = datetime.now().isoformat(timespec="seconds")
    # Invalidate caches
    try:
        from app import cache
        cache.invalidate_all()
    except Exception:
        pass
    return True


def duplicate_article(article_id: int) -> int | None:
    """Duplicate an article.  Returns new article ID or None."""
    with get_pg_session() as db:
        orig = db.get(Article, article_id)
        if not orig:
            return None
        now = datetime.now().isoformat(timespec="seconds")
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        old_url = orig.url or ""
        new_url = old_url.rstrip("/") + "-copy-" + suffix if old_url else f"https://total.kz/copy-{suffix}"
        new_title = "Копия: " + (orig.title or "")
        dup = Article(
            url=new_url,
            pub_date=orig.pub_date,
            sub_category=orig.sub_category or "",
            category_label=orig.category_label or "",
            title=new_title,
            author=orig.author or "",
            excerpt=orig.excerpt or "",
            body_text=orig.body_text or "",
            body_html=orig.body_html or "",
            main_image=orig.main_image or "",
            image_credit=orig.image_credit or "",
            thumbnail=orig.thumbnail or "",
            tags=orig.tags if orig.tags else [],
            inline_images=orig.inline_images if orig.inline_images else [],
            status="draft",
            updated_at=now,
            editor_note=orig.editor_note or "",
            imported_at=now,
        )
        db.add(dup)
        db.flush()
        return dup.id


def bulk_update_articles(article_ids: list, updates: dict) -> int:
    """Bulk update status/category for multiple articles."""
    if not article_ids:
        return 0
    allowed = {"status", "sub_category"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return 0
    with get_pg_session() as db:
        filtered["updated_at"] = datetime.now().isoformat(timespec="seconds")
        result = db.execute(
            sa_update(Article)
            .where(Article.id.in_(article_ids))
            .values(**filtered)
        )
        return result.rowcount


def bulk_delete_articles(article_ids: list) -> int:
    """Soft-delete (archive) multiple articles."""
    if not article_ids:
        return 0
    with get_pg_session() as db:
        result = db.execute(
            sa_update(Article)
            .where(Article.id.in_(article_ids))
            .values(status="archived", updated_at=datetime.now().isoformat(timespec="seconds"))
        )
        return result.rowcount


# ═══════════════════════════════════════════════
# PUBLIC FRONTEND QUERIES
# ═══════════════════════════════════════════════

def get_article_by_slug(category: str, slug: str) -> dict | None:
    """Find article by category + slug (extracted from old URL)."""
    with get_pg_session() as db:
        url_pattern = f"%/ru/news/{category}/{slug}"
        article = db.execute(
            select(Article).where(Article.url.like(url_pattern)).limit(1)
        ).scalar_one_or_none()
        if not article:
            return None
        d = _article_to_dict(article)
        # Entities
        rows = db.execute(
            select(NerEntity.id, NerEntity.name, NerEntity.short_name,
                   NerEntity.entity_type, ArticleEntity.mention_count)
            .join(ArticleEntity, ArticleEntity.entity_id == NerEntity.id)
            .where(ArticleEntity.article_id == article.id)
            .order_by(ArticleEntity.mention_count.desc())
        ).all()
        d["entities"] = [
            {"id": r[0], "name": r[1], "short_name": r[2],
             "entity_type": r[3], "mention_count": r[4]}
            for r in rows
        ]
        d["enrichment"] = _load_enrichment(db, article.id)
        return d


def get_latest_articles(limit: int = 20, offset: int = 0) -> list:
    """Get latest articles for homepage."""
    with get_pg_session() as db:
        rows = db.execute(
            select(*_ARTICLE_LIST_COLUMNS)
            .order_by(Article.pub_date.desc())
            .limit(limit).offset(offset)
        ).all()
        keys = ["id", "url", "pub_date", "sub_category", "title", "author",
                "excerpt", "thumbnail", "main_image", "views"]
        articles = [_row_to_dict(r, keys) for r in rows]
        # Backfill empty excerpts with GPT summaries
        try:
            ids = [a["id"] for a in articles if not a.get("excerpt")]
            if ids:
                sums = db.execute(
                    select(ArticleEnrichment.article_id, ArticleEnrichment.summary)
                    .where(ArticleEnrichment.article_id.in_(ids))
                ).all()
                smap = {r[0]: r[1] for r in sums}
                for a in articles:
                    if not a.get("excerpt") and a["id"] in smap:
                        a["excerpt"] = smap[a["id"]]
        except Exception:
            pass
        return articles


def get_latest_by_category(category: str, limit: int = 10, offset: int = 0) -> dict:
    """Get latest articles for a category with pagination."""
    with get_pg_session() as db:
        total = db.scalar(
            select(func.count()).where(Article.sub_category == category)
        )
        rows = db.execute(
            select(*_ARTICLE_LIST_COLUMNS)
            .where(Article.sub_category == category)
            .order_by(Article.pub_date.desc())
            .limit(limit).offset(offset)
        ).all()
        keys = ["id", "url", "pub_date", "sub_category", "title", "author",
                "excerpt", "thumbnail", "main_image", "views"]
        return {
            "articles": [_row_to_dict(r, keys) for r in rows],
            "total": total,
            "pages": _paginate(total, limit),
        }


def get_related_articles(article_id: int, category: str, limit: int = 4) -> list:
    """Get related articles from same category, excluding current."""
    with get_pg_session() as db:
        rows = db.execute(
            select(*_ARTICLE_LIST_COLUMNS)
            .where(Article.sub_category == category, Article.id != article_id)
            .order_by(Article.pub_date.desc())
            .limit(limit)
        ).all()
        keys = ["id", "url", "pub_date", "sub_category", "title", "author",
                "excerpt", "thumbnail", "main_image", "views"]
        return [_row_to_dict(r, keys) for r in rows]


def get_timeline_articles(article_id: int, category: str, pub_date: str, entity_ids: list = None) -> dict:
    """Get timeline articles related by shared entities (or fallback to category)."""
    _timeline_cols = [
        Article.id, Article.url, Article.pub_date, Article.sub_category,
        Article.title, Article.thumbnail, Article.main_image,
    ]
    _keys = ["id", "url", "pub_date", "sub_category", "title", "thumbnail", "main_image"]

    with get_pg_session() as db:
        if entity_ids:
            shared = func.count(distinct(ArticleEntity.entity_id)).label("shared")
            base = (
                select(*_timeline_cols, shared)
                .join(ArticleEntity, ArticleEntity.article_id == Article.id)
                .where(ArticleEntity.entity_id.in_(entity_ids), Article.id != article_id)
                .group_by(Article.id)
            )
            prev_rows = db.execute(
                base.where(Article.pub_date <= pub_date)
                .order_by(Article.pub_date.desc()).limit(3)
            ).all()
            next_rows = db.execute(
                base.where(Article.pub_date > pub_date)
                .order_by(Article.pub_date.asc()).limit(3)
            ).all()
            # Drop the 'shared' column from dicts
            _keys_ext = _keys + ["shared"]
            return {
                "prev": [_row_to_dict(r, _keys_ext) for r in prev_rows],
                "next": [_row_to_dict(r, _keys_ext) for r in next_rows],
            }
        else:
            base = (
                select(*_timeline_cols)
                .where(Article.sub_category == category, Article.id != article_id)
            )
            prev_rows = db.execute(
                base.where(Article.pub_date <= pub_date)
                .order_by(Article.pub_date.desc()).limit(3)
            ).all()
            next_rows = db.execute(
                base.where(Article.pub_date > pub_date)
                .order_by(Article.pub_date.asc()).limit(3)
            ).all()
            return {
                "prev": [_row_to_dict(r, _keys) for r in prev_rows],
                "next": [_row_to_dict(r, _keys) for r in next_rows],
            }


def get_story_timeline(article_id: int, pub_date: str) -> dict | None:
    """Get story-based timeline for an article."""
    try:
        return _get_story_timeline_inner(article_id, pub_date)
    except Exception:
        return None


def _get_story_timeline_inner(article_id: int, pub_date: str) -> dict | None:
    with get_pg_session() as db:
        story_row = db.execute(
            select(Story.id, Story.title_ru, Story.article_count)
            .join(ArticleStory, ArticleStory.story_id == Story.id)
            .where(ArticleStory.article_id == article_id, ArticleStory.confidence >= 0.3)
            .order_by(ArticleStory.confidence.desc())
            .limit(1)
        ).one_or_none()

        if not story_row or story_row[2] < 2:
            return None

        story_id, story_title, art_count = story_row

        _cols = [Article.id, Article.url, Article.pub_date, Article.sub_category,
                 Article.title, Article.thumbnail, Article.main_image]
        _keys = ["id", "url", "pub_date", "sub_category", "title", "thumbnail", "main_image"]

        prev_rows = db.execute(
            select(*_cols)
            .join(ArticleStory, ArticleStory.article_id == Article.id)
            .where(
                ArticleStory.story_id == story_id,
                Article.id != article_id,
                Article.pub_date <= pub_date,
                ArticleStory.confidence >= 0.3,
            )
            .order_by(Article.pub_date.desc()).limit(5)
        ).all()

        next_rows = db.execute(
            select(*_cols)
            .join(ArticleStory, ArticleStory.article_id == Article.id)
            .where(
                ArticleStory.story_id == story_id,
                Article.id != article_id,
                Article.pub_date > pub_date,
                ArticleStory.confidence >= 0.3,
            )
            .order_by(Article.pub_date.asc()).limit(5)
        ).all()

        return {
            "story_title": story_title,
            "total_articles": art_count,
            "prev": [_row_to_dict(r, _keys) for r in prev_rows],
            "next": [_row_to_dict(r, _keys) for r in next_rows],
        }


def get_related_by_entities(article_id: int, entity_ids: list, category: str, limit: int = 6) -> list:
    """Get related articles by shared entities, falling back to category."""
    with get_pg_session() as db:
        results = []
        keys_ext = ["id", "url", "pub_date", "sub_category", "title", "author",
                     "excerpt", "thumbnail", "main_image", "shared"]
        keys_base = keys_ext[:-1]

        if entity_ids:
            shared = func.count(distinct(ArticleEntity.entity_id)).label("shared")
            rows = db.execute(
                select(
                    Article.id, Article.url, Article.pub_date, Article.sub_category,
                    Article.title, Article.author, Article.excerpt,
                    Article.thumbnail, Article.main_image, shared,
                )
                .join(ArticleEntity, ArticleEntity.article_id == Article.id)
                .where(ArticleEntity.entity_id.in_(entity_ids), Article.id != article_id)
                .group_by(Article.id)
                .order_by(shared.desc(), Article.pub_date.desc())
                .limit(limit)
            ).all()
            results = [_row_to_dict(r, keys_ext) for r in rows]

        # Fill remaining slots from same category
        if len(results) < limit:
            existing_ids = [r["id"] for r in results] + [article_id]
            fill = db.execute(
                select(
                    Article.id, Article.url, Article.pub_date, Article.sub_category,
                    Article.title, Article.author, Article.excerpt,
                    Article.thumbnail, Article.main_image,
                )
                .where(Article.sub_category == category, Article.id.notin_(existing_ids))
                .order_by(Article.pub_date.desc())
                .limit(limit - len(results))
            ).all()
            results.extend([_row_to_dict(r, keys_base) for r in fill])
        return results


def get_trending_tags(limit: int = 20) -> list:
    """Get trending tags (most used recently)."""
    with get_pg_session() as db:
        _30d = text("to_char(CURRENT_DATE - INTERVAL '30 days', 'YYYY-MM-DD')")
        rows = db.execute(
            select(ArticleTag.tag, func.count().label("cnt"))
            .join(Article, Article.id == ArticleTag.article_id)
            .where(Article.pub_date >= _30d)
            .group_by(ArticleTag.tag)
            .order_by(func.count().desc())
            .limit(limit)
        ).all()
        return [{"tag": t, "cnt": n} for t, n in rows]


def get_category_counts() -> list:
    """Get article counts per category."""
    with get_pg_session() as db:
        rows = db.execute(
            select(Article.sub_category, func.count().label("cnt"))
            .group_by(Article.sub_category)
            .order_by(func.count().desc())
        ).all()
        return [{"sub_category": c, "cnt": n} for c, n in rows]


def get_latest_by_categories(categories: list, limit: int = 10, offset: int = 0) -> dict:
    """Get latest articles from multiple sub_categories."""
    if not categories:
        return {"articles": [], "total": 0, "pages": 1}
    with get_pg_session() as db:
        total = db.scalar(
            select(func.count()).where(Article.sub_category.in_(categories))
        )
        rows = db.execute(
            select(*_ARTICLE_LIST_COLUMNS)
            .where(Article.sub_category.in_(categories))
            .order_by(Article.pub_date.desc())
            .limit(limit).offset(offset)
        ).all()
        keys = ["id", "url", "pub_date", "sub_category", "title", "author",
                "excerpt", "thumbnail", "main_image", "views"]
        return {
            "articles": [_row_to_dict(r, keys) for r in rows],
            "total": total,
            "pages": _paginate(total, limit),
        }


def get_category_highlights_batch(nav_sections: list, per_section: int = 3) -> dict:
    """Fetch top N articles per nav section in ONE query using window function.

    Returns: dict mapping section_slug -> list[dict]
    """
    # Collect all subcats with section mapping
    all_subcats = []
    subcat_to_section = {}
    for section in nav_sections:
        for sc in section["subcats"]:
            all_subcats.append(sc)
            subcat_to_section[sc] = section["slug"]
    if not all_subcats:
        return {}

    with get_pg_session() as db:
        rows = db.execute(
            text("""
                SELECT sub_category, id, url, pub_date, title, author,
                       excerpt, thumbnail, main_image, COALESCE(views, 0) as views
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY sub_category ORDER BY pub_date DESC
                    ) as rn
                    FROM articles
                    WHERE sub_category = ANY(:subcats)
                ) t
                WHERE rn <= :per_section
                ORDER BY sub_category, pub_date DESC
            """),
            {"subcats": all_subcats, "per_section": per_section}
        ).all()

    keys = ["sub_category", "id", "url", "pub_date", "title", "author",
            "excerpt", "thumbnail", "main_image", "views"]
    # Group by nav section
    result = {}
    for row in rows:
        d = dict(zip(keys, row))
        section_slug = subcat_to_section.get(d["sub_category"])
        if section_slug:
            result.setdefault(section_slug, []).append(d)
    return result


def popular_in_category(subcats: list, limit: int = 5) -> list:
    """Top articles by views in the given sub_categories."""
    if not subcats:
        return []
    with get_pg_session() as db:
        rows = db.execute(
            select(
                Article.id, Article.url, Article.title,
                Article.pub_date, func.coalesce(Article.views, 0).label("views"),
            )
            .where(Article.sub_category.in_(subcats))
            .order_by(func.coalesce(Article.views, 0).desc(), Article.pub_date.desc())
            .limit(limit)
        ).all()
        keys = ["id", "url", "title", "pub_date", "views"]
        return [_row_to_dict(r, keys) for r in rows]


def trending_tags_for_category(subcats: list, limit: int = 15) -> list:
    """Top tags from article_enrichments for articles in given sub_categories."""
    if not subcats:
        return []
    with get_pg_session() as db:
        rows = db.execute(
            text("""
                SELECT tag, COUNT(*) as cnt
                FROM article_enrichments ae
                JOIN articles a ON a.id = ae.article_id
                CROSS JOIN LATERAL jsonb_array_elements_text(ae.keywords) AS tag
                WHERE a.sub_category = ANY(:subcats)
                  AND ae.keywords IS NOT NULL
                GROUP BY tag
                ORDER BY cnt DESC
                LIMIT :lim
            """),
            {"subcats": subcats, "lim": limit}
        ).all()
        return [{"tag": r[0], "count": r[1]} for r in rows]


def get_entity(entity_id: int) -> dict | None:
    """Get entity by ID."""
    with get_pg_session() as db:
        e = db.get(NerEntity, entity_id)
        if not e:
            return None
        return {
            "id": e.id, "name": e.name, "short_name": e.short_name,
            "entity_type": e.entity_type, "normalized": e.normalized,
        }


def get_articles_by_entity(entity_id: int, page: int = 1, per_page: int = 20) -> dict:
    """Get articles linked to an entity, paginated."""
    with get_pg_session() as db:
        total = db.scalar(
            select(func.count()).select_from(ArticleEntity)
            .where(ArticleEntity.entity_id == entity_id)
        )
        offset = (page - 1) * per_page
        rows = db.execute(
            select(
                Article.id, Article.url, Article.pub_date, Article.sub_category,
                Article.title, Article.author, Article.excerpt,
                Article.thumbnail, Article.main_image,
            )
            .join(ArticleEntity, ArticleEntity.article_id == Article.id)
            .where(ArticleEntity.entity_id == entity_id)
            .order_by(Article.pub_date.desc())
            .limit(per_page).offset(offset)
        ).all()
        keys = ["id", "url", "pub_date", "sub_category", "title", "author",
                "excerpt", "thumbnail", "main_image", "views"]
        return {
            "articles": [_row_to_dict(r, keys) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": _paginate(total, per_page),
        }


def generate_sitemap_urls(limit: int = 50000) -> list:
    """Get URLs for sitemap generation."""
    with get_pg_session() as db:
        rows = db.execute(
            select(Article.url, Article.pub_date, Article.sub_category)
            .where(Article.pub_date.isnot(None))
            .order_by(Article.pub_date.desc())
            .limit(limit)
        ).all()
        return [{"url": r[0], "pub_date": r[1], "sub_category": r[2]} for r in rows]


# ═══════════════════════════════════════════════
# CMS v11 – Users
# ═══════════════════════════════════════════════

def _user_to_dict(u: User) -> dict:
    return {
        "id": u.id, "username": u.username, "email": u.email,
        "password_hash": u.password_hash, "display_name": u.display_name,
        "avatar_url": u.avatar_url, "role": u.role, "is_active": u.is_active,
        "created_at": u.created_at, "last_login": u.last_login,
    }


def get_user_by_username(username: str) -> dict | None:
    with get_pg_session() as db:
        u = db.execute(
            select(User).where(User.username == username)
        ).scalar_one_or_none()
        return _user_to_dict(u) if u else None


def get_user(user_id: int) -> dict | None:
    with get_pg_session() as db:
        u = db.get(User, user_id)
        return _user_to_dict(u) if u else None


def get_all_users() -> list:
    with get_pg_session() as db:
        rows = db.execute(
            select(User).order_by(User.created_at.desc())
        ).scalars().all()
        return [_user_to_dict(u) for u in rows]


def create_user(data: dict) -> int:
    with get_pg_session() as db:
        u = User(
            username=data["username"],
            email=data.get("email", ""),
            password_hash=data["password_hash"],
            display_name=data["display_name"],
            role=data.get("role", "journalist"),
            is_active=data.get("is_active", 1),
        )
        db.add(u)
        db.flush()
        return u.id


def update_user(user_id: int, updates: dict) -> None:
    allowed = {"email", "display_name", "role", "is_active", "avatar_url", "password_hash", "last_login"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_pg_session() as db:
        db.execute(sa_update(User).where(User.id == user_id).values(**filtered))


def delete_user(user_id: int) -> None:
    with get_pg_session() as db:
        db.execute(sa_delete(User).where(User.id == user_id))


# ═══════════════════════════════════════════════
# CMS v11 – Categories
# ═══════════════════════════════════════════════

def _cat_to_dict(c: CMSCategory) -> dict:
    return {
        "id": c.id, "slug": c.slug, "name_ru": c.name_ru, "name_kz": c.name_kz,
        "parent_id": c.parent_id, "sort_order": c.sort_order, "is_active": c.is_active,
        "article_count": c.article_count, "created_at": c.created_at,
    }


def get_all_categories() -> list:
    with get_pg_session() as db:
        live_count = (
            select(func.count())
            .where(Article.sub_category == CMSCategory.slug)
            .correlate(CMSCategory)
            .scalar_subquery()
        ).label("live_count")
        rows = db.execute(
            select(CMSCategory, live_count)
            .order_by(CMSCategory.sort_order, CMSCategory.name_ru)
        ).all()
        result = []
        for cat, lc in rows:
            d = _cat_to_dict(cat)
            d["live_count"] = lc
            result.append(d)
        return result


def get_category(cat_id: int) -> dict | None:
    with get_pg_session() as db:
        c = db.get(CMSCategory, cat_id)
        return _cat_to_dict(c) if c else None


def create_category(data: dict) -> int:
    with get_pg_session() as db:
        c = CMSCategory(
            slug=data["slug"],
            name_ru=data["name_ru"],
            name_kz=data.get("name_kz", ""),
            parent_id=data.get("parent_id"),
            sort_order=data.get("sort_order", 0),
            is_active=data.get("is_active", 1),
        )
        db.add(c)
        db.flush()
        return c.id


def update_category(cat_id: int, updates: dict) -> None:
    allowed = {"name_ru", "name_kz", "slug", "parent_id", "sort_order", "is_active"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_pg_session() as db:
        db.execute(sa_update(CMSCategory).where(CMSCategory.id == cat_id).values(**filtered))


def delete_category(cat_id: int) -> None:
    with get_pg_session() as db:
        db.execute(sa_delete(CMSCategory).where(CMSCategory.id == cat_id))


# ═══════════════════════════════════════════════
# CMS v11 – Authors Managed
# ═══════════════════════════════════════════════

def _author_to_dict(a: AuthorManaged) -> dict:
    return {
        "id": a.id, "name": a.name, "slug": a.slug, "bio": a.bio,
        "avatar_url": a.avatar_url, "email": a.email, "is_active": a.is_active,
        "article_count": a.article_count, "created_at": a.created_at,
    }


def get_all_authors_managed(q: str = "") -> list:
    with get_pg_session() as db:
        live_count = (
            select(func.count())
            .where(Article.author == AuthorManaged.name)
            .correlate(AuthorManaged)
            .scalar_subquery()
        ).label("live_count")
        last_pub = (
            select(func.max(Article.pub_date))
            .where(Article.author == AuthorManaged.name)
            .correlate(AuthorManaged)
            .scalar_subquery()
        ).label("last_published")

        stmt = (
            select(AuthorManaged, live_count, last_pub)
            .order_by(AuthorManaged.article_count.desc())
        )
        if q:
            stmt = stmt.where(AuthorManaged.name.ilike(f"%{q}%"))

        rows = db.execute(stmt).all()
        result = []
        for am, lc, lp in rows:
            d = _author_to_dict(am)
            d["live_count"] = lc
            d["last_published"] = lp
            result.append(d)
        return result


def get_author_managed(author_id: int) -> dict | None:
    with get_pg_session() as db:
        a = db.get(AuthorManaged, author_id)
        return _author_to_dict(a) if a else None


def create_author_managed(data: dict) -> int:
    with get_pg_session() as db:
        a = AuthorManaged(
            name=data["name"],
            slug=data["slug"],
            bio=data.get("bio", ""),
            avatar_url=data.get("avatar_url", ""),
            email=data.get("email", ""),
            is_active=data.get("is_active", 1),
        )
        db.add(a)
        db.flush()
        return a.id


def update_author_managed(author_id: int, updates: dict) -> None:
    allowed = {"name", "slug", "bio", "avatar_url", "email", "is_active"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_pg_session() as db:
        db.execute(sa_update(AuthorManaged).where(AuthorManaged.id == author_id).values(**filtered))


def delete_author_managed(author_id: int) -> None:
    with get_pg_session() as db:
        db.execute(sa_delete(AuthorManaged).where(AuthorManaged.id == author_id))


# ═══════════════════════════════════════════════
# CMS v11 – Media Library
# ═══════════════════════════════════════════════

def _media_to_dict(m: Media) -> dict:
    return {
        "id": m.id, "filename": m.filename, "original_name": m.original_name,
        "mime_type": m.mime_type, "file_size": m.file_size, "url": m.url,
        "uploaded_at": m.uploaded_at, "uploaded_by": m.uploaded_by,
        "width": m.width, "height": m.height,
        "alt_text": m.alt_text, "credit": m.credit,
    }


def get_all_media(q: str = "", page: int = 1, per_page: int = 30, sort: str = "newest", media_type: str = "") -> dict:
    with get_pg_session() as db:
        stmt = select(Media)
        count_q = select(func.count()).select_from(Media)
        if q:
            cond = or_(
                Media.original_name.ilike(f"%{q}%"),
                Media.alt_text.ilike(f"%{q}%"),
            )
            stmt = stmt.where(cond)
            count_q = count_q.where(cond)
        if media_type == "images":
            img_cond = Media.mime_type.ilike("image/%")
            stmt = stmt.where(img_cond)
            count_q = count_q.where(img_cond)
        elif media_type == "documents":
            doc_cond = ~Media.mime_type.ilike("image/%")
            stmt = stmt.where(doc_cond)
            count_q = count_q.where(doc_cond)
        total = db.scalar(count_q)
        if sort == "oldest":
            order = Media.uploaded_at.asc()
        elif sort == "largest":
            order = Media.file_size.desc()
        else:
            order = Media.uploaded_at.desc()
        offset = (page - 1) * per_page
        rows = db.execute(
            stmt.order_by(order).limit(per_page).offset(offset)
        ).scalars().all()
        return {
            "items": [_media_to_dict(m) for m in rows],
            "total": total,
            "page": page,
            "pages": _paginate(total, per_page),
        }


def create_media(data: dict) -> int:
    with get_pg_session() as db:
        m = Media(
            filename=data["filename"],
            original_name=data["original_name"],
            mime_type=data["mime_type"],
            file_size=data["file_size"],
            url=data["url"],
            width=data.get("width"),
            height=data.get("height"),
            alt_text=data.get("alt_text", ""),
            credit=data.get("credit", ""),
            uploaded_by=data.get("uploaded_by"),
        )
        db.add(m)
        db.flush()
        return m.id


def update_media(media_id: int, updates: dict) -> None:
    allowed = {"alt_text", "credit", "original_name"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_pg_session() as db:
        db.execute(sa_update(Media).where(Media.id == media_id).values(**filtered))


def delete_media(media_id: int) -> dict | None:
    with get_pg_session() as db:
        m = db.get(Media, media_id)
        if not m:
            return None
        d = _media_to_dict(m)
        db.delete(m)
        return d


# ═══════════════════════════════════════════════
# Tags CRUD
# ═══════════════════════════════════════════════

def get_popular_tags(limit: int = 1000, min_articles: int = 5) -> list:
    """Tags by article count (for tag cloud). Only tags with min_articles+ articles."""
    with get_pg_session() as db:
        rows = db.execute(
            select(ArticleTag.tag, func.count().label("article_count"))
            .group_by(ArticleTag.tag)
            .having(func.count() >= min_articles)
            .order_by(func.count().desc())
            .limit(limit)
        ).all()
        return [{"tag": t, "article_count": n} for t, n in rows]


def get_tags_by_letter(min_count: int = 3) -> dict:
    """All tags with min_count+ articles, grouped by first letter."""
    with get_pg_session() as db:
        rows = db.execute(
            select(ArticleTag.tag, func.count().label("article_count"))
            .group_by(ArticleTag.tag)
            .having(func.count() >= min_count)
            .order_by(ArticleTag.tag)
        ).all()
        alpha = {}
        for t, n in rows:
            letter = t[0].upper() if t else "#"
            alpha.setdefault(letter, []).append({"tag": t, "article_count": n})
        return alpha


def get_tags_full(q: str = "", page: int = 1, per_page: int = 50) -> dict:
    with get_pg_session() as db:
        base = select(ArticleTag.tag)
        if q:
            base = base.where(ArticleTag.tag.ilike(f"%{q}%"))

        total = db.scalar(
            select(func.count(distinct(ArticleTag.tag))).where(
                ArticleTag.tag.ilike(f"%{q}%") if q else True
            )
        )
        offset = (page - 1) * per_page
        rows = db.execute(
            select(ArticleTag.tag, func.count().label("article_count"))
            .where(ArticleTag.tag.ilike(f"%{q}%") if q else True)
            .group_by(ArticleTag.tag)
            .order_by(func.count().desc())
            .limit(per_page).offset(offset)
        ).all()
        return {
            "items": [{"tag": t, "article_count": n} for t, n in rows],
            "total": total,
            "page": page,
            "pages": _paginate(total, per_page),
        }


def rename_tag(old_tag: str, new_tag: str) -> int:
    with get_pg_session() as db:
        result = db.execute(
            sa_update(ArticleTag).where(ArticleTag.tag == old_tag).values(tag=new_tag)
        )
        # Update JSONB tags field in articles — replace old_tag with new_tag
        db.execute(
            sa_update(Article)
            .where(Article.tags.cast(text("text")).like(f"%{old_tag}%"))
            .values(tags=func.replace(Article.tags.cast(text("text")), f'"{old_tag}"', f'"{new_tag}"').cast(Article.tags.type))
        )
        return result.rowcount


def merge_tags(tags: list, target_tag: str) -> int:
    """Merge multiple tags into one target tag."""
    total = 0
    with get_pg_session() as db:
        for tag in tags:
            if tag == target_tag:
                continue
            # Get article_ids with the source tag
            rows = db.execute(
                select(ArticleTag.article_id).where(ArticleTag.tag == tag)
            ).all()
            for (aid,) in rows:
                # Insert target tag if not already present
                stmt = pg_insert(ArticleTag).values(
                    article_id=aid, tag=target_tag
                ).on_conflict_do_nothing()
                db.execute(stmt)
            # Delete old tag
            result = db.execute(
                sa_delete(ArticleTag).where(ArticleTag.tag == tag)
            )
            total += result.rowcount
            # Update JSONB tags in articles
            db.execute(
                sa_update(Article)
                .where(Article.tags.cast(text("text")).like(f"%{tag}%"))
                .values(tags=func.replace(Article.tags.cast(text("text")), f'"{tag}"', f'"{target_tag}"').cast(Article.tags.type))
            )
    return total


def delete_tag(tag: str) -> int:
    with get_pg_session() as db:
        result = db.execute(
            sa_delete(ArticleTag).where(ArticleTag.tag == tag)
        )
        return result.rowcount


# ═══════════════════════════════════════════════
# Entities CRUD
# ═══════════════════════════════════════════════

def get_entities_full(q: str = "", entity_type: str = "", page: int = 1, per_page: int = 50) -> dict:
    with get_pg_session() as db:
        conditions = []
        if q:
            conditions.append(NerEntity.name.ilike(f"%{q}%"))
        if entity_type:
            conditions.append(NerEntity.entity_type == entity_type)

        count_q = select(func.count()).select_from(NerEntity)
        if conditions:
            count_q = count_q.where(*conditions)
        total = db.scalar(count_q)

        offset = (page - 1) * per_page
        stmt = (
            select(
                NerEntity.id, NerEntity.name, NerEntity.short_name,
                NerEntity.entity_type, NerEntity.normalized,
                func.count(ArticleEntity.article_id).label("article_count"),
            )
            .outerjoin(ArticleEntity, ArticleEntity.entity_id == NerEntity.id)
            .group_by(NerEntity.id)
            .order_by(text("article_count DESC"))
            .limit(per_page).offset(offset)
        )
        if conditions:
            stmt = stmt.where(*conditions)

        rows = db.execute(stmt).all()
        return {
            "items": [
                {"id": r[0], "name": r[1], "short_name": r[2],
                 "entity_type": r[3], "normalized": r[4], "article_count": r[5]}
                for r in rows
            ],
            "total": total,
            "page": page,
            "pages": _paginate(total, per_page),
        }


def create_entity(data: dict) -> int:
    with get_pg_session() as db:
        name = data["name"]
        normalized = data.get("normalized", name.lower().strip())
        e = NerEntity(
            name=name,
            short_name=data.get("short_name", ""),
            entity_type=data["entity_type"],
            normalized=normalized,
        )
        db.add(e)
        db.flush()
        return e.id


def update_entity(entity_id: int, updates: dict) -> None:
    allowed = {"name", "short_name", "entity_type", "normalized"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_pg_session() as db:
        db.execute(sa_update(NerEntity).where(NerEntity.id == entity_id).values(**filtered))


def delete_entity(entity_id: int) -> None:
    with get_pg_session() as db:
        db.execute(sa_delete(ArticleEntity).where(ArticleEntity.entity_id == entity_id))
        db.execute(sa_delete(NerEntity).where(NerEntity.id == entity_id))


def merge_entities(entity_ids: list, target_id: int) -> int:
    total = 0
    with get_pg_session() as db:
        for eid in entity_ids:
            if eid == target_id:
                continue
            # Get article linkages for source entity
            rows = db.execute(
                select(ArticleEntity.article_id, ArticleEntity.mention_count)
                .where(ArticleEntity.entity_id == eid)
            ).all()
            for aid, mc in rows:
                existing = db.execute(
                    select(ArticleEntity.mention_count)
                    .where(ArticleEntity.article_id == aid, ArticleEntity.entity_id == target_id)
                ).scalar_one_or_none()
                if existing is not None:
                    db.execute(
                        sa_update(ArticleEntity)
                        .where(ArticleEntity.article_id == aid, ArticleEntity.entity_id == target_id)
                        .values(mention_count=ArticleEntity.mention_count + mc)
                    )
                else:
                    db.add(ArticleEntity(article_id=aid, entity_id=target_id, mention_count=mc))
                    db.flush()
            # Delete source entity and its linkages
            db.execute(sa_delete(ArticleEntity).where(ArticleEntity.entity_id == eid))
            db.execute(sa_delete(NerEntity).where(NerEntity.id == eid))
            total += 1
    return total


# ═══════════════════════════════════════════════
# Stories
# ═══════════════════════════════════════════════

def get_all_stories(q: str = "", page: int = 1, per_page: int = 30) -> dict:
    with get_pg_session() as db:
        try:
            # Check if stories table has data
            db.execute(select(Story.id).limit(1)).first()
        except Exception:
            return {"items": [], "total": 0, "page": 1, "pages": 1}

        conditions = []
        if q:
            conditions.append(Story.title_ru.ilike(f"%{q}%"))

        count_q = select(func.count()).select_from(Story)
        if conditions:
            count_q = count_q.where(*conditions)
        total = db.scalar(count_q)

        # article_count, first_date, last_date via subqueries on article_stories
        art_count_sq = (
            select(func.count())
            .select_from(ArticleStory)
            .where(ArticleStory.story_id == Story.id)
            .correlate(Story)
            .scalar_subquery()
        ).label("article_count")
        first_date_sq = (
            select(func.min(Article.pub_date))
            .join(ArticleStory, ArticleStory.article_id == Article.id)
            .where(ArticleStory.story_id == Story.id)
            .correlate(Story)
            .scalar_subquery()
        ).label("first_date")
        last_date_sq = (
            select(func.max(Article.pub_date))
            .join(ArticleStory, ArticleStory.article_id == Article.id)
            .where(ArticleStory.story_id == Story.id)
            .correlate(Story)
            .scalar_subquery()
        ).label("last_date")

        offset = (page - 1) * per_page
        stmt = (
            select(Story, art_count_sq, first_date_sq, last_date_sq)
            .order_by(Story.id.desc())
            .limit(per_page).offset(offset)
        )
        if conditions:
            stmt = stmt.where(*conditions)

        rows = db.execute(stmt).all()
        items = []
        for s, ac, fd, ld in rows:
            d = {
                "id": s.id, "slug": s.slug, "title_ru": s.title_ru,
                "description": s.description, "article_count": ac or s.article_count,
                "first_date": fd or s.first_date, "last_date": ld or s.last_date,
                "created_at": s.created_at,
            }
            items.append(d)
        return {
            "items": items,
            "total": total,
            "page": page,
            "pages": _paginate(total, per_page),
        }


def get_story(story_id: int) -> dict | None:
    with get_pg_session() as db:
        try:
            s = db.get(Story, story_id)
            if not s:
                return None
            d = {
                "id": s.id, "slug": s.slug, "title_ru": s.title_ru,
                "description": s.description, "article_count": s.article_count,
                "first_date": s.first_date, "last_date": s.last_date,
                "created_at": s.created_at,
            }
            arts = db.execute(
                select(
                    Article.id, Article.title, Article.pub_date, Article.sub_category,
                    Article.thumbnail, Article.main_image, Article.author,
                    Article.excerpt, ArticleStory.confidence,
                )
                .join(ArticleStory, ArticleStory.article_id == Article.id)
                .where(ArticleStory.story_id == story_id)
                .order_by(Article.pub_date.asc())
            ).all()
            d["articles"] = [
                {"id": r[0], "title": r[1], "pub_date": r[2], "sub_category": r[3],
                 "thumbnail": r[4], "main_image": r[5], "author": r[6],
                 "excerpt": r[7], "confidence": r[8]}
                for r in arts
            ]
            return d
        except Exception:
            return None


def create_story(data: dict) -> int:
    with get_pg_session() as db:
        s = Story(
            title_ru=data["title_ru"],
            description=data.get("description", ""),
            slug=data.get("slug", ""),
            article_count=0,
        )
        db.add(s)
        db.flush()
        return s.id


def update_story(story_id: int, updates: dict) -> None:
    allowed = {"title_ru", "description"}
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return
    with get_pg_session() as db:
        db.execute(sa_update(Story).where(Story.id == story_id).values(**filtered))


def delete_story(story_id: int) -> None:
    with get_pg_session() as db:
        db.execute(sa_delete(ArticleStory).where(ArticleStory.story_id == story_id))
        db.execute(sa_delete(Story).where(Story.id == story_id))


def add_article_to_story(story_id: int, article_id: int) -> None:
    with get_pg_session() as db:
        stmt = pg_insert(ArticleStory).values(
            story_id=story_id, article_id=article_id, confidence=1.0,
        ).on_conflict_do_nothing()
        db.execute(stmt)
        # Update article_count
        cnt = db.scalar(
            select(func.count()).select_from(ArticleStory)
            .where(ArticleStory.story_id == story_id)
        )
        db.execute(
            sa_update(Story).where(Story.id == story_id).values(article_count=cnt)
        )


def remove_article_from_story(story_id: int, article_id: int) -> None:
    with get_pg_session() as db:
        db.execute(
            sa_delete(ArticleStory)
            .where(ArticleStory.story_id == story_id, ArticleStory.article_id == article_id)
        )
        cnt = db.scalar(
            select(func.count()).select_from(ArticleStory)
            .where(ArticleStory.story_id == story_id)
        )
        db.execute(
            sa_update(Story).where(Story.id == story_id).values(article_count=cnt)
        )


# ═══════════════════════════════════════════════
# Audit Log
# ═══════════════════════════════════════════════

def log_audit(user_id: int, username: str, action: str, entity_type: str,
              entity_id: int = None, details: str = "", ip_address: str = "") -> None:
    with get_pg_session() as db:
        db.add(AuditLog(
            user_id=user_id,
            username=username,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            details=details,
            ip_address=ip_address,
        ))


def get_audit_log(user_id: int = 0, action: str = "", entity_type: str = "",
                  date_from: str = "", date_to: str = "",
                  page: int = 1, per_page: int = 50) -> dict:
    with get_pg_session() as db:
        conditions = []
        if user_id:
            conditions.append(AuditLog.user_id == user_id)
        if action:
            conditions.append(AuditLog.action == action)
        if entity_type:
            conditions.append(AuditLog.entity_type == entity_type)
        if date_from:
            conditions.append(AuditLog.created_at >= date_from)
        if date_to:
            conditions.append(AuditLog.created_at <= date_to + " 23:59:59")

        count_q = select(func.count()).select_from(AuditLog)
        if conditions:
            count_q = count_q.where(*conditions)
        total = db.scalar(count_q)

        offset = (page - 1) * per_page
        stmt = (
            select(AuditLog)
            .order_by(AuditLog.created_at.desc())
            .limit(per_page).offset(offset)
        )
        if conditions:
            stmt = stmt.where(*conditions)

        rows = db.execute(stmt).scalars().all()
        items = []
        for al in rows:
            items.append({
                "id": al.id, "user_id": al.user_id, "username": al.username,
                "action": al.action, "entity_type": al.entity_type,
                "entity_id": al.entity_id, "details": al.details,
                "ip_address": al.ip_address, "created_at": al.created_at,
            })
        return {
            "items": items,
            "total": total,
            "page": page,
            "pages": _paginate(total, per_page),
        }


# ═══════════════════════════════════════════════
# Extra helpers (replace raw SQL in main.py)
# ═══════════════════════════════════════════════

def get_status_counts(user_id: int | None = None, username: str | None = None) -> dict:
    """Article counts grouped by status + optional user assignment count."""
    with get_pg_session() as db:
        counts: dict[str, int] = {}
        for s in ("published", "draft", "archived", "review", "ready"):
            counts[s] = db.execute(
                select(func.count()).select_from(Article).where(Article.status == s)
            ).scalar() or 0
        counts["all"] = db.execute(
            select(func.count()).select_from(Article)
        ).scalar() or 0
        # assigned_to stores username (text), not user_id
        assigned_val = username or (str(user_id) if user_id else None)
        if assigned_val:
            counts["my"] = db.execute(
                select(func.count()).select_from(Article).where(Article.assigned_to == assigned_val)
            ).scalar() or 0
        return counts


def add_tag_to_article(article_id: int, tag: str) -> None:
    """Link a tag to an article (idempotent)."""
    with get_pg_session() as db:
        stmt = pg_insert(ArticleTag).values(
            article_id=article_id, tag=tag
        ).on_conflict_do_nothing()
        db.execute(stmt)


def get_full_audit() -> dict:
    """Full data-quality audit (PG version of /api/audit)."""
    from collections import defaultdict

    with get_pg_session() as db:
        total = db.execute(select(func.count()).select_from(Article)).scalar() or 0
        if total == 0:
            return {"total_articles": 0}

        # 1. Content completeness
        fields_check = {}
        text_fields = [
            "title", "body_text", "body_html", "excerpt", "author",
            "main_image", "thumbnail", "image_credit", "sub_category", "category_label",
        ]
        for field in text_fields:
            col = getattr(Article, field, None)
            if col is None:
                continue
            empty = db.execute(
                select(func.count()).select_from(Article).where(
                    or_(col.is_(None), col == "")
                )
            ).scalar() or 0
            fields_check[field] = {"empty": empty, "pct": round(empty / total * 100, 1)}

        no_tags = db.execute(
            select(func.count()).select_from(Article).where(
                or_(Article.tags.is_(None), Article.tags == "", Article.tags == "[]")
            )
        ).scalar() or 0
        fields_check["tags_json"] = {"empty": no_tags, "pct": round(no_tags / total * 100, 1)}

        no_inline = db.execute(
            select(func.count()).select_from(Article).where(
                or_(Article.inline_images.is_(None), Article.inline_images == "", Article.inline_images == "[]")
            )
        ).scalar() or 0
        fields_check["inline_images_json"] = {"empty": no_inline, "pct": round(no_inline / total * 100, 1)}

        # 2. Body text stats
        avg_body = db.execute(
            select(func.avg(func.length(Article.body_text))).where(
                Article.body_text.isnot(None), Article.body_text != ""
            )
        ).scalar() or 0
        short_body = db.execute(
            select(func.count()).select_from(Article).where(
                func.length(Article.body_text) < 100,
                Article.body_text.isnot(None),
                Article.body_text != "",
            )
        ).scalar() or 0

        # 3. Monthly distribution
        rows = db.execute(
            select(
                func.to_char(Article.pub_date, "YYYY-MM").label("month"),
                func.count().label("cnt"),
            ).where(Article.pub_date.isnot(None))
            .group_by(text("1")).order_by(text("1"))
        ).all()
        monthly = [{"month": r.month, "count": r.cnt} for r in rows]
        gaps = [m for m in monthly if m["count"] < 200]

        # 4. Categories
        cats = db.execute(
            select(Article.sub_category, func.count().label("cnt"))
            .group_by(Article.sub_category)
            .order_by(func.count().desc())
        ).all()
        categories = [{"name": r.sub_category or "(empty)", "count": r.cnt} for r in cats]

        # 5. Authors top-30
        authors_q = db.execute(
            select(Article.author, func.count().label("cnt"))
            .where(Article.author.isnot(None), Article.author != "")
            .group_by(Article.author)
            .order_by(func.count().desc())
            .limit(30)
        ).all()
        author_list = [{"name": r.author, "count": r.cnt} for r in authors_q]
        unique_authors = db.execute(
            select(func.count(distinct(Article.author))).where(
                Article.author.isnot(None), Article.author != ""
            )
        ).scalar() or 0

        # 6. Tags audit
        total_tag_links = db.execute(
            select(func.count()).select_from(ArticleTag)
        ).scalar() or 0
        unique_tags = db.execute(
            select(func.count(distinct(ArticleTag.tag)))
        ).scalar() or 0
        articles_with_tags = db.execute(
            select(func.count(distinct(ArticleTag.article_id)))
        ).scalar() or 0

        top_tags_q = db.execute(
            select(ArticleTag.tag, func.count().label("cnt"))
            .group_by(ArticleTag.tag)
            .order_by(func.count().desc())
            .limit(50)
        ).all()
        tag_list = [{"tag": r.tag, "count": r.cnt} for r in top_tags_q]

        all_tags_raw = db.execute(
            select(ArticleTag.tag, func.count().label("cnt"))
            .group_by(ArticleTag.tag)
        ).all()
        tag_groups: dict[str, list] = defaultdict(list)
        for t in all_tags_raw:
            tag_groups[t.tag.lower().strip()].append({"tag": t.tag, "count": t.cnt})
        tag_dupes = []
        for lower, variants in tag_groups.items():
            if len(variants) > 1:
                tag_dupes.append({"normalized": lower, "variants": variants})
        tag_dupes.sort(key=lambda x: -sum(v["count"] for v in x["variants"]))

        # Latin-only tags (no Cyrillic)
        latin_tags_q = db.execute(
            select(ArticleTag.tag, func.count().label("cnt"))
            .where(~ArticleTag.tag.op("~")("[а-яА-ЯёЁ]"), ArticleTag.tag != "")
            .group_by(ArticleTag.tag)
            .order_by(func.count().desc())
            .limit(30)
        ).all()
        latin_tag_list = [{"tag": r.tag, "count": r.cnt} for r in latin_tags_q]

        # 7. Entities audit
        total_entities = db.execute(
            select(func.count()).select_from(NerEntity)
        ).scalar() or 0
        total_entity_links = db.execute(
            select(func.count()).select_from(ArticleEntity)
        ).scalar() or 0
        entity_types = db.execute(
            select(NerEntity.entity_type, func.count())
            .group_by(NerEntity.entity_type)
        ).all()
        entity_type_counts = {r[0]: r[1] for r in entity_types}

        top_entities: dict[str, list] = {}
        for etype in ["person", "org", "location", "event"]:
            top = db.execute(
                select(
                    NerEntity.name, NerEntity.normalized,
                    func.count(ArticleEntity.article_id).label("cnt"),
                ).join(ArticleEntity, ArticleEntity.entity_id == NerEntity.id)
                .where(NerEntity.entity_type == etype)
                .group_by(NerEntity.id, NerEntity.name, NerEntity.normalized)
                .order_by(func.count(ArticleEntity.article_id).desc())
                .limit(20)
            ).all()
            top_entities[etype] = [
                {"name": r.name, "normalized": r.normalized, "articles": r.cnt}
                for r in top
            ]

        entity_dupes = db.execute(
            select(
                NerEntity.normalized,
                NerEntity.entity_type,
                func.string_agg(NerEntity.name, " | ").label("names"),
                func.count().label("cnt"),
            ).group_by(NerEntity.normalized, NerEntity.entity_type)
            .having(func.count() > 1)
            .order_by(func.count().desc())
            .limit(30)
        ).all()
        entity_dupe_list = [
            {"normalized": r.normalized, "type": r.entity_type,
             "names": r.names, "variant_count": r.cnt}
            for r in entity_dupes
        ]

        orphan_entities = db.execute(
            select(func.count()).select_from(NerEntity)
            .outerjoin(ArticleEntity, ArticleEntity.entity_id == NerEntity.id)
            .where(ArticleEntity.article_id.is_(None))
        ).scalar() or 0

        return {
            "total_articles": total,
            "content_completeness": fields_check,
            "body_text_avg_length": int(avg_body or 0),
            "body_text_short_count": short_body,
            "monthly_gaps": gaps,
            "categories": categories,
            "authors": {"top_30": author_list, "unique_count": unique_authors},
            "tags": {
                "total_links": total_tag_links,
                "unique": unique_tags,
                "articles_with_tags": articles_with_tags,
                "top_50": tag_list,
                "case_duplicates": tag_dupes[:30],
                "latin_only": latin_tag_list,
            },
            "entities": {
                "total": total_entities,
                "total_links": total_entity_links,
                "by_type": entity_type_counts,
                "top_by_type": top_entities,
                "duplicates": entity_dupe_list,
                "orphans": orphan_entities,
            },
        }


# ─── View tracking ──────────────────────────────────────────

def suggest_articles(query: str, limit: int = 7) -> list:
    """Fast autocomplete: returns up to `limit` article title suggestions."""
    if len(query) < 2:
        return []
    with get_pg_session() as db:
        rows = db.execute(
            select(Article.title, Article.sub_category, Article.pub_date, Article.url)
            .where(Article.title.ilike(f"%{query}%"))
            .order_by(Article.pub_date.desc())
            .limit(limit)
        ).all()
        return [{"title": r[0], "sub_category": r[1], "pub_date": r[2], "url": r[3]} for r in rows]


def track_view(article_id: int) -> int:
    """Increment view count for an article. Returns new count."""
    with get_pg_session() as db:
        db.execute(
            sa_update(Article)
            .where(Article.id == article_id)
            .values(views=func.coalesce(Article.views, 0) + 1)
        )
        db.commit()
        row = db.execute(
            select(Article.views).where(Article.id == article_id)
        ).scalar()
        return row or 0
