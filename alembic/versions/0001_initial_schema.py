"""Initial schema – all 16 tables from SQLAlchemy models.

Revision ID: 0001
Revises: (none)
Create Date: 2026-03-22
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Independent tables (no FK) ─────────────────────────

    op.create_table(
        "articles",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("url", sa.Text, unique=True, nullable=False),
        sa.Column("pub_date", sa.Text),
        sa.Column("sub_category", sa.Text),
        sa.Column("category_label", sa.Text),
        sa.Column("title", sa.Text),
        sa.Column("author", sa.Text),
        sa.Column("excerpt", sa.Text),
        sa.Column("body_text", sa.Text),
        sa.Column("body_html", sa.Text),
        sa.Column("main_image", sa.Text),
        sa.Column("image_credit", sa.Text),
        sa.Column("thumbnail", sa.Text),
        sa.Column("tags", postgresql.JSONB),
        sa.Column("inline_images", postgresql.JSONB),
        sa.Column("imported_at", sa.Text, server_default=sa.func.now()),
        sa.Column("status", sa.Text, server_default="published"),
        sa.Column("updated_at", sa.Text),
        sa.Column("editor_note", sa.Text),
        sa.Column("body_blocks", postgresql.JSONB),
        sa.Column("scheduled_at", sa.Text),
        sa.Column("focal_x", sa.Float, server_default="0.5"),
        sa.Column("focal_y", sa.Float, server_default="0.5"),
        sa.Column("assigned_to", sa.Text),
    )
    op.create_index("idx_articles_pub_date", "articles", ["pub_date"])
    op.create_index("idx_articles_sub_category", "articles", ["sub_category"])
    op.create_index("idx_articles_author", "articles", ["author"])
    op.create_index(
        "idx_articles_sub_category_pub_date",
        "articles",
        ["sub_category", sa.text("pub_date DESC")],
    )

    op.create_table(
        "entities",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("short_name", sa.Text),
        sa.Column("entity_type", sa.Text, nullable=False),
        sa.Column("normalized", sa.Text),
        sa.UniqueConstraint("normalized", "entity_type", name="uq_entities_normalized_type"),
    )
    op.create_index("idx_entities_type", "entities", ["entity_type"])
    op.create_index("idx_entities_normalized", "entities", ["normalized"])

    op.create_table(
        "scrape_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("started_at", sa.Text, nullable=False),
        sa.Column("finished_at", sa.Text),
        sa.Column("phase", sa.Text, nullable=False),
        sa.Column("status", sa.Text, server_default="running", nullable=False),
        sa.Column("articles_found", sa.Integer, server_default="0", nullable=False),
        sa.Column("articles_downloaded", sa.Integer, server_default="0", nullable=False),
        sa.Column("errors", sa.Integer, server_default="0", nullable=False),
        sa.Column("log", sa.Text),
    )

    op.create_table(
        "media",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("filename", sa.Text, nullable=False),
        sa.Column("original_name", sa.Text),
        sa.Column("mime_type", sa.Text),
        sa.Column("file_size", sa.Integer),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column("uploaded_at", sa.Text, server_default=sa.func.now()),
        sa.Column("uploaded_by", sa.Text),
        sa.Column("width", sa.Integer),
        sa.Column("height", sa.Integer),
        sa.Column("alt_text", sa.Text, server_default=""),
        sa.Column("credit", sa.Text, server_default=""),
    )

    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("username", sa.Text, unique=True, nullable=False),
        sa.Column("email", sa.Text, unique=True),
        sa.Column("password_hash", sa.Text, nullable=False),
        sa.Column("display_name", sa.Text, nullable=False),
        sa.Column("avatar_url", sa.Text, server_default=""),
        sa.Column("role", sa.Text, server_default="journalist", nullable=False),
        sa.Column("is_active", sa.Integer, server_default="1", nullable=False),
        sa.Column("created_at", sa.Text, server_default=sa.func.now()),
        sa.Column("last_login", sa.Text),
    )

    op.create_table(
        "categories",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("slug", sa.Text, unique=True, nullable=False),
        sa.Column("name_ru", sa.Text, nullable=False),
        sa.Column("name_kz", sa.Text, server_default=""),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("categories.id")),
        sa.Column("sort_order", sa.Integer, server_default="0", nullable=False),
        sa.Column("is_active", sa.Integer, server_default="1", nullable=False),
        sa.Column("article_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("created_at", sa.Text, server_default=sa.func.now()),
    )
    op.create_index("idx_categories_slug", "categories", ["slug"])

    op.create_table(
        "authors_managed",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text, unique=True, nullable=False),
        sa.Column("slug", sa.Text, unique=True, nullable=False),
        sa.Column("bio", sa.Text, server_default=""),
        sa.Column("avatar_url", sa.Text, server_default=""),
        sa.Column("email", sa.Text, server_default=""),
        sa.Column("is_active", sa.Integer, server_default="1", nullable=False),
        sa.Column("article_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("created_at", sa.Text, server_default=sa.func.now()),
    )
    op.create_index("idx_authors_managed_slug", "authors_managed", ["slug"])

    op.create_table(
        "stories",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("slug", sa.Text, unique=True, nullable=False),
        sa.Column("title_ru", sa.Text, nullable=False),
        sa.Column("description", sa.Text, server_default=""),
        sa.Column("article_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("first_date", sa.Text),
        sa.Column("last_date", sa.Text),
        sa.Column("created_at", sa.Text, server_default=sa.func.now()),
    )
    op.create_index("idx_stories_slug", "stories", ["slug"])

    # ── Dependent tables (FK) ──────────────────────────────

    op.create_table(
        "article_entities",
        sa.Column("article_id", sa.Integer, sa.ForeignKey("articles.id"), primary_key=True),
        sa.Column("entity_id", sa.Integer, sa.ForeignKey("entities.id"), primary_key=True),
        sa.Column("mention_count", sa.Integer, server_default="1", nullable=False),
    )
    op.create_index("idx_ae_article", "article_entities", ["article_id"])
    op.create_index("idx_ae_entity", "article_entities", ["entity_id"])

    op.create_table(
        "article_tags",
        sa.Column("article_id", sa.Integer, sa.ForeignKey("articles.id"), primary_key=True),
        sa.Column("tag", sa.Text, primary_key=True, nullable=False),
    )
    op.create_index("idx_tags_tag", "article_tags", ["tag"])

    op.create_table(
        "article_revisions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("article_id", sa.Integer, sa.ForeignKey("articles.id"), nullable=False),
        sa.Column("changed_at", sa.Text, server_default=sa.func.now(), nullable=False),
        sa.Column("changed_by", sa.Text),
        sa.Column("changes_json", sa.Text),
        sa.Column("revision_type", sa.Text, server_default="edit", nullable=False),
    )
    op.create_index(
        "idx_revisions_article",
        "article_revisions",
        ["article_id", sa.text("changed_at DESC")],
    )

    op.create_table(
        "article_comments",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("article_id", sa.Integer, sa.ForeignKey("articles.id"), nullable=False),
        sa.Column("user_id", sa.Integer, nullable=False),
        sa.Column("username", sa.Text, nullable=False),
        sa.Column("role", sa.Text, nullable=False),
        sa.Column("comment", sa.Text, nullable=False),
        sa.Column("created_at", sa.Text, server_default=sa.func.now()),
    )
    op.create_index("idx_comments_article", "article_comments", ["article_id"])

    op.create_table(
        "article_stories",
        sa.Column("article_id", sa.Integer, sa.ForeignKey("articles.id"), primary_key=True),
        sa.Column("story_id", sa.Integer, sa.ForeignKey("stories.id"), primary_key=True),
        sa.Column("confidence", sa.Float, server_default="0.5", nullable=False),
    )
    op.create_index("idx_article_stories_story", "article_stories", ["story_id"])
    op.create_index("idx_article_stories_article", "article_stories", ["article_id"])

    op.create_table(
        "article_enrichments",
        sa.Column("article_id", sa.Integer, sa.ForeignKey("articles.id"), primary_key=True),
        sa.Column("summary", sa.Text),
        sa.Column("meta_description", sa.Text),
        sa.Column("keywords", postgresql.JSONB),
        sa.Column("quote", sa.Text),
        sa.Column("quote_author", sa.Text),
        sa.Column("created_at", sa.Text, server_default=sa.func.now()),
    )
    op.create_index("idx_enrichments_article", "article_enrichments", ["article_id"])

    op.create_table(
        "scrape_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Integer, sa.ForeignKey("scrape_runs.id")),
        sa.Column("timestamp", sa.Text, server_default=sa.func.now()),
        sa.Column("level", sa.Text, server_default="info", nullable=False),
        sa.Column("message", sa.Text),
    )

    op.create_table(
        "audit_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
        sa.Column("username", sa.Text),
        sa.Column("action", sa.Text, nullable=False),
        sa.Column("entity_type", sa.Text, nullable=False),
        sa.Column("entity_id", sa.Integer),
        sa.Column("details", sa.Text),
        sa.Column("ip_address", sa.Text),
        sa.Column("created_at", sa.Text, server_default=sa.func.now()),
    )
    op.create_index("idx_audit_created", "audit_log", [sa.text("created_at DESC")])
    op.create_index("idx_audit_entity", "audit_log", ["entity_type", "entity_id"])


def downgrade() -> None:
    # Drop in reverse dependency order
    op.drop_table("audit_log")
    op.drop_table("scrape_log")
    op.drop_table("article_enrichments")
    op.drop_table("article_stories")
    op.drop_table("article_comments")
    op.drop_table("article_revisions")
    op.drop_table("article_tags")
    op.drop_table("article_entities")
    op.drop_table("stories")
    op.drop_table("authors_managed")
    op.drop_table("categories")
    op.drop_table("users")
    op.drop_table("media")
    op.drop_table("scrape_runs")
    op.drop_table("entities")
    op.drop_table("articles")
