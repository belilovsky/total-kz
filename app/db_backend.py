"""Database backend selector for total-kz.

Import this module instead of database / pg_queries directly.
Controlled by USE_POSTGRES env var / settings.use_postgres.

    from app import db_backend as db

All public functions of database.py / pg_queries.py are
transparently re-exported.
"""

from app.config import settings

if settings.use_postgres:
    from app.pg_queries import *      # noqa: F401,F403
    from app.pg_queries import (      # noqa: F401  – explicit for IDE hints
        init_db, import_jsonl, get_stats, search_articles,
        get_authors, get_tags, get_entities, get_article,
        update_article, create_article, record_revision, get_revisions,
        duplicate_article, bulk_update_articles, bulk_delete_articles,
        get_article_by_slug, get_latest_articles, get_latest_by_category,
        get_related_articles, get_timeline_articles, get_story_timeline,
        get_related_by_entities, get_trending_tags, get_category_counts,
        get_latest_by_categories, get_entity, get_articles_by_entity,
        generate_sitemap_urls, get_user_by_username, get_user, get_all_users,
        create_user, update_user, delete_user, get_all_categories,
        get_category, create_category, update_category, delete_category,
        get_all_authors_managed, get_author_managed, create_author_managed,
        update_author_managed, delete_author_managed, get_all_media,
        create_media, update_media, delete_media, get_tags_full,
        rename_tag, merge_tags, delete_tag, get_entities_full,
        create_entity, update_entity, delete_entity, merge_entities,
        get_all_stories, get_story, create_story, update_story, delete_story,
        add_article_to_story, remove_article_from_story, log_audit,
        get_audit_log, blocks_to_html, blocks_to_text,
        get_status_counts, add_tag_to_article, get_full_audit,
        suggest_articles, track_view,
    )

    # PG mode doesn't have get_db_path / get_db — these are SQLite-only
    _BACKEND = "postgresql"
else:
    from app.database import *        # noqa: F401,F403
    from app.database import (        # noqa: F401
        init_db, import_jsonl, get_stats, search_articles,
        get_authors, get_tags, get_entities, get_article,
        update_article, create_article, record_revision, get_revisions,
        duplicate_article, bulk_update_articles, bulk_delete_articles,
        get_article_by_slug, get_latest_articles, get_latest_by_category,
        get_related_articles, get_timeline_articles, get_story_timeline,
        get_related_by_entities, get_trending_tags, get_category_counts,
        get_latest_by_categories, get_entity, get_articles_by_entity,
        generate_sitemap_urls, get_user_by_username, get_user, get_all_users,
        create_user, update_user, delete_user, get_all_categories,
        get_category, create_category, update_category, delete_category,
        get_all_authors_managed, get_author_managed, create_author_managed,
        update_author_managed, delete_author_managed, get_all_media,
        create_media, update_media, delete_media, get_tags_full,
        rename_tag, merge_tags, delete_tag, get_entities_full,
        create_entity, update_entity, delete_entity, merge_entities,
        get_all_stories, get_story, create_story, update_story, delete_story,
        add_article_to_story, remove_article_from_story, log_audit,
        get_audit_log, blocks_to_html, blocks_to_text,
        get_db_path, get_db,
        get_all_ad_placements, get_ad_placement, toggle_ad_placement,
        update_ad_placement, get_ad_stats,
    )
    _BACKEND = "sqlite"
