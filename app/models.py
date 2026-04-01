"""SQLAlchemy 2.0 ORM models for total-kz (PostgreSQL).

Phase 1: Mirror of the existing SQLite schema with proper types.
These models are used by Alembic for migrations and will eventually
replace the raw-SQL database.py layer.
"""

from datetime import datetime

from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ── Articles ─────────────────────────────────────────────


class Article(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    pub_date: Mapped[str | None] = mapped_column(Text)
    sub_category: Mapped[str | None] = mapped_column(Text)
    category_label: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(Text)
    author: Mapped[str | None] = mapped_column(Text)
    excerpt: Mapped[str | None] = mapped_column(Text)
    body_text: Mapped[str | None] = mapped_column(Text)
    body_html: Mapped[str | None] = mapped_column(Text)
    main_image: Mapped[str | None] = mapped_column(Text)
    image_credit: Mapped[str | None] = mapped_column(Text)
    thumbnail: Mapped[str | None] = mapped_column(Text)
    tags: Mapped[dict | None] = mapped_column(JSONB)
    inline_images: Mapped[dict | None] = mapped_column(JSONB)
    imported_at: Mapped[str | None] = mapped_column(
        Text, server_default=func.now()
    )

    # CMS columns (v10+)
    status: Mapped[str | None] = mapped_column(Text, server_default="published")
    updated_at: Mapped[str | None] = mapped_column(Text)
    editor_note: Mapped[str | None] = mapped_column(Text)

    # v10.2 — Editor.js blocks
    body_blocks: Mapped[dict | None] = mapped_column(JSONB)
    scheduled_at: Mapped[str | None] = mapped_column(Text)
    focal_x: Mapped[float | None] = mapped_column(Float, server_default="0.5")
    focal_y: Mapped[float | None] = mapped_column(Float, server_default="0.5")

    # v11 — View tracking
    views: Mapped[int | None] = mapped_column(Integer, server_default="0")

    # v12 — Workflow
    assigned_to: Mapped[str | None] = mapped_column(Text)

    # v15.1 — Breaking news flag
    is_breaking: Mapped[bool] = mapped_column(Integer, server_default="0")

    # Relationships
    entities: Mapped[list["ArticleEntity"]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )
    article_tags: Mapped[list["ArticleTag"]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )
    revisions: Mapped[list["ArticleRevision"]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )
    comments: Mapped[list["ArticleComment"]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )
    article_stories: Mapped[list["ArticleStory"]] = relationship(
        back_populates="article", cascade="all, delete-orphan"
    )
    enrichment: Mapped["ArticleEnrichment | None"] = relationship(
        back_populates="article", uselist=False, cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_articles_pub_date", "pub_date"),
        Index("idx_articles_sub_category", "sub_category"),
        Index("idx_articles_author", "author"),
        Index("idx_articles_sub_category_pub_date", "sub_category", pub_date.desc()),
    )


# ── NER Entities ─────────────────────────────────────────


class NerEntity(Base):
    __tablename__ = "entities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    short_name: Mapped[str | None] = mapped_column(Text)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    normalized: Mapped[str | None] = mapped_column(Text)

    articles: Mapped[list["ArticleEntity"]] = relationship(
        back_populates="entity", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("normalized", "entity_type", name="uq_entities_normalized_type"),
        Index("idx_entities_type", "entity_type"),
        Index("idx_entities_normalized", "normalized"),
    )


# ── Article ↔ Entity (many-to-many) ─────────────────────


class ArticleEntity(Base):
    __tablename__ = "article_entities"

    article_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("articles.id"), primary_key=True
    )
    entity_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("entities.id"), primary_key=True
    )
    mention_count: Mapped[int] = mapped_column(Integer, server_default="1")

    article: Mapped["Article"] = relationship(back_populates="entities")
    entity: Mapped["NerEntity"] = relationship(back_populates="articles")

    __table_args__ = (
        Index("idx_ae_article", "article_id"),
        Index("idx_ae_entity", "entity_id"),
    )


# ── Article Tags ─────────────────────────────────────────


class ArticleTag(Base):
    __tablename__ = "article_tags"

    article_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("articles.id"), primary_key=True
    )
    tag: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)

    article: Mapped["Article"] = relationship(back_populates="article_tags")

    __table_args__ = (
        Index("idx_tags_tag", "tag"),
    )


# ── Scrape Runs ──────────────────────────────────────────


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[str] = mapped_column(Text, nullable=False)
    finished_at: Mapped[str | None] = mapped_column(Text)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, server_default="running")
    articles_found: Mapped[int] = mapped_column(Integer, server_default="0")
    articles_downloaded: Mapped[int] = mapped_column(Integer, server_default="0")
    errors: Mapped[int] = mapped_column(Integer, server_default="0")
    log: Mapped[str | None] = mapped_column(Text)

    logs: Mapped[list["ScrapeLog"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


# ── Scrape Log ───────────────────────────────────────────


class ScrapeLog(Base):
    __tablename__ = "scrape_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("scrape_runs.id")
    )
    timestamp: Mapped[str | None] = mapped_column(Text, server_default=func.now())
    level: Mapped[str] = mapped_column(Text, server_default="info")
    message: Mapped[str | None] = mapped_column(Text)

    run: Mapped["ScrapeRun | None"] = relationship(back_populates="logs")


# ── Media ────────────────────────────────────────────────


class Media(Base):
    __tablename__ = "media"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    original_name: Mapped[str | None] = mapped_column(Text)
    mime_type: Mapped[str | None] = mapped_column(Text)
    file_size: Mapped[int | None] = mapped_column(Integer)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    uploaded_at: Mapped[str | None] = mapped_column(Text, server_default=func.now())
    uploaded_by: Mapped[str | None] = mapped_column(Text)
    # v11
    width: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)
    alt_text: Mapped[str | None] = mapped_column(Text, server_default="")
    credit: Mapped[str | None] = mapped_column(Text, server_default="")


# ── Article Revisions ────────────────────────────────────


class ArticleRevision(Base):
    __tablename__ = "article_revisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    article_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("articles.id"), nullable=False
    )
    changed_at: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=func.now()
    )
    changed_by: Mapped[str | None] = mapped_column(Text)
    changes_json: Mapped[str | None] = mapped_column(Text)
    revision_type: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="edit"
    )

    article: Mapped["Article"] = relationship(back_populates="revisions")

    __table_args__ = (
        Index("idx_revisions_article", "article_id", changed_at.desc()),
    )


# ── Users ────────────────────────────────────────────────


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    email: Mapped[str | None] = mapped_column(Text, unique=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    avatar_url: Mapped[str | None] = mapped_column(Text, server_default="")
    role: Mapped[str] = mapped_column(Text, nullable=False, server_default="journalist")
    is_active: Mapped[bool] = mapped_column(Integer, server_default="1")
    created_at: Mapped[str | None] = mapped_column(Text, server_default=func.now())
    last_login: Mapped[str | None] = mapped_column(Text)

    audit_entries: Mapped[list["AuditLog"]] = relationship(back_populates="user")


# ── CMS Categories ───────────────────────────────────────


class CMSCategory(Base):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    name_ru: Mapped[str] = mapped_column(Text, nullable=False)
    name_kz: Mapped[str | None] = mapped_column(Text, server_default="")
    parent_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("categories.id")
    )
    sort_order: Mapped[int] = mapped_column(Integer, server_default="0")
    is_active: Mapped[bool] = mapped_column(Integer, server_default="1")
    article_count: Mapped[int] = mapped_column(Integer, server_default="0")
    created_at: Mapped[str | None] = mapped_column(Text, server_default=func.now())

    parent: Mapped["CMSCategory | None"] = relationship(
        remote_side="CMSCategory.id", back_populates="children"
    )
    children: Mapped[list["CMSCategory"]] = relationship(back_populates="parent")

    __table_args__ = (
        Index("idx_categories_slug", "slug"),
    )


# ── Managed Authors ──────────────────────────────────────


class AuthorManaged(Base):
    __tablename__ = "authors_managed"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    bio: Mapped[str | None] = mapped_column(Text, server_default="")
    avatar_url: Mapped[str | None] = mapped_column(Text, server_default="")
    email: Mapped[str | None] = mapped_column(Text, server_default="")
    is_active: Mapped[bool] = mapped_column(Integer, server_default="1")
    article_count: Mapped[int] = mapped_column(Integer, server_default="0")
    created_at: Mapped[str | None] = mapped_column(Text, server_default=func.now())

    __table_args__ = (
        Index("idx_authors_managed_slug", "slug"),
    )


# ── Audit Log ────────────────────────────────────────────


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id")
    )
    username: Mapped[str | None] = mapped_column(Text)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[int | None] = mapped_column(Integer)
    details: Mapped[str | None] = mapped_column(Text)
    ip_address: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str | None] = mapped_column(Text, server_default=func.now())

    user: Mapped["User | None"] = relationship(back_populates="audit_entries")

    __table_args__ = (
        Index("idx_audit_created", created_at.desc()),
        Index("idx_audit_entity", "entity_type", "entity_id"),
    )


# ── Article Comments (Workflow v12) ──────────────────────


class ArticleComment(Base):
    __tablename__ = "article_comments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    article_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("articles.id"), nullable=False
    )
    user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    comment: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str | None] = mapped_column(Text, server_default=func.now())

    article: Mapped["Article"] = relationship(back_populates="comments")

    __table_args__ = (
        Index("idx_comments_article", "article_id"),
    )


# ── Stories ──────────────────────────────────────────────


class Story(Base):
    __tablename__ = "stories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    title_ru: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, server_default="")
    article_count: Mapped[int] = mapped_column(Integer, server_default="0")
    first_date: Mapped[str | None] = mapped_column(Text)
    last_date: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str | None] = mapped_column(Text, server_default=func.now())

    article_stories: Mapped[list["ArticleStory"]] = relationship(
        back_populates="story", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_stories_slug", "slug"),
    )


# ── Article ↔ Story (many-to-many) ──────────────────────


class ArticleStory(Base):
    __tablename__ = "article_stories"

    article_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("articles.id"), primary_key=True
    )
    story_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("stories.id"), primary_key=True
    )
    confidence: Mapped[float] = mapped_column(Float, server_default="0.5")

    article: Mapped["Article"] = relationship(back_populates="article_stories")
    story: Mapped["Story"] = relationship(back_populates="article_stories")

    __table_args__ = (
        Index("idx_article_stories_story", "story_id"),
        Index("idx_article_stories_article", "article_id"),
    )


# ── Article Enrichments (GPT summaries) ─────────────────


class ArticleEnrichment(Base):
    __tablename__ = "article_enrichments"

    article_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("articles.id"), primary_key=True
    )
    summary: Mapped[str | None] = mapped_column(Text)
    meta_description: Mapped[str | None] = mapped_column(Text)
    keywords: Mapped[dict | None] = mapped_column(JSONB)
    quote: Mapped[str | None] = mapped_column(Text)
    quote_author: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str | None] = mapped_column(Text, server_default=func.now())

    article: Mapped["Article"] = relationship(back_populates="enrichment")

    __table_args__ = (
        Index("idx_enrichments_article", "article_id"),
    )
