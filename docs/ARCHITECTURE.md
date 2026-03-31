# Architecture — Total.kz

## System Overview

Total.kz is a full-stack news aggregation platform with automated content pipeline, NLP enrichment, and multi-platform distribution.

## Data Flow

```
External Sources (RSS feeds)
        │
        ▼
┌─────────────────┐     ┌──────────────────┐
│  news-monitor   │────▶│  PostgreSQL 16    │
│  (real-time)    │     │  (pgvector)       │
└─────────────────┘     └────────┬─────────┘
                                 │
┌─────────────────┐              │
│  cron pipeline  │──────────────┤
│  (every 2h)     │              │
│  fetch → enrich │              │
│  → NER → tags   │              ▼
│  → reindex      │     ┌──────────────────┐
└─────────────────┘     │  Meilisearch     │
                        │  (full-text)     │
                        └──────────────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │  FastAPI app     │
                        │  public + admin  │
                        └────────┬─────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
              Web Browser   RSS Feeds   Telegram
              (PWA)         (6 feeds)   (auto-post)
```

## Database Schema (Key Tables)

### articles
Primary content table with ~200K+ articles.
- `id`, `url`, `pub_date`, `title`, `excerpt`, `body_html`, `body_text`
- `sub_category`, `category_label`, `author`, `source`
- `main_image`, `thumbnail`, `image_credit`
- `status` (draft/review/published), `views`
- `tags` (JSONB), `assigned_to`, `editor_note`

### article_enrichments
GPT-generated metadata (1:1 with articles).
- `summary`, `meta_description`, `keywords` (array)
- `quote`, `key_facts`, `key_facts_kz`
- `sentiment` (positive/negative/neutral), `topics`

### ner_entities
Named entities extracted by Natasha NER.
- `name`, `short_name`, `slug`, `entity_type` (person/org/location)
- `photo_url`, `current_position`, `career` (JSONB)
- `biography`, `biography_kz`

### article_entities
Many-to-many: articles ↔ entities.
- `article_id`, `entity_id`, `mention_count`

### stories
Story clusters (timelines).
- `title`, `topic`, `summary`, `auto_generated`
- Articles linked via `story_articles` junction table

### public_comments
User comments on articles.
- `article_id`, `author_name`, `author_email`, `text`
- `status` (pending/approved/rejected), `ip_address`

### social_accounts
Connected social media accounts.
- `platform` (telegram/instagram/youtube/facebook/tiktok/x/vk)
- `account_name`, `config` (JSONB with tokens, channel IDs)

## Template Architecture

Two Jinja2 environments:
1. **Admin** (`main.py`) — `app/templates/` (21 templates, `base.html`)
2. **Public** (`public_routes.py`) — `app/templates/public/` (19 templates, `public/base.html`)

### Template Globals (Public)
- `site_domain`, `nav_sections`, `nav_section_names`
- `format_date()`, `format_date_full()`, `format_date_day()`
- `article_url()`, `article_url_i18n()`, `imgproxy_url()`
- `cat_label()`, `nav_slug_for()`, `reading_time_minutes()`
- `get_display_mode()` — current display mode
- `lang`, `lang_prefix` — language context

## Caching Strategy

| Layer | TTL | Scope |
|-------|-----|-------|
| Nginx | 120s (max-age) + 300s (stale-while-revalidate) | All pages |
| In-memory (Python) | 300s | Homepage, category pages |
| CSS versioning | `?v=N` (manual bump) | Static assets |
| Meilisearch | Reindexed every 2h via cron | Search index |

## Security

- Admin authentication: bcrypt-hashed passwords, session cookies
- CSRF: not applicable (API uses JSON, no form submissions from external origins)
- XSS: Jinja2 auto-escaping enabled, `|safe` used only for `body_html`
- Rate limiting: per-IP for views, comments, reactions
- HSTS + CSP headers via Nginx
- Docker: ports bound to 127.0.0.1 (no external exposure)

## Multi-Language

- Russian: default (`/news/...`)
- Kazakh: `/kz/news/...` — machine-translated via GPT
- Language detection: URL prefix (`/kz/`)
- KZ-specific: separate sitemap, translated enrichments (`key_facts_kz`, `biography_kz`)
