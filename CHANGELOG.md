# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [2.4.0] — 2026-03-31

### Added
- **Platform integration:** RSS feeds for Yandex Zen (`/zen/rss.xml`), Facebook Instant Articles (`/fb-ia/rss.xml`), Flipboard (`/flipboard/rss.xml`)
- **WebSub/PubSubHubbub:** real-time feed notifications, auto-ping on article publish
- **Telegram auto-posting:** background scheduler (`autopost.py`), admin API (`/api/admin/auto-post`), rate limiting, category filtering
- **Feed discovery:** 5 `<link rel="alternate">` tags in `<head>` for auto-detection
- **`get_latest_articles_full()`** — database function returning articles with `body_html` for feed syndication

### Changed
- Yandex Turbo RSS (`/turbo/rss.xml`) now includes full `body_html` instead of excerpts only
- Main RSS (`/rss`) now includes `content:encoded` with full article body
- Google News structured data: added `dateModified`, `isAccessibleForFree`, `speakable`
- Telegram Instant View: added `tg:site_verification` meta placeholder, optimized article HTML structure

### Fixed
- Article page: removed duplicate lead/excerpt paragraph (only body text lead remains)
- Article page: fixed large empty gap between content and timeline (`align-items:start` on grid)
- Persons sidebar: deduplicated names (e.g., "Жибек Куламбаева" + "Куламбаева" + "Жибек" → only full name shown)
- Renamed "Сообщить" button to "Поделиться темой", moved to bottom of sidebar

## [2.3.0] — 2026-03-30

### Added
- **Display modes:** 3 admin-controlled modes — Mourning (grayscale, muted), Holiday (9 KZ holidays with decorations), Evening (positive content priority after 20:00)
- Display mode admin UI (`/admin/settings` → Режим оформления)
- API endpoints: `GET /api/display-mode`, `POST /api/admin/display-mode`
- JSON config storage (`display_mode.json`)

## [2.2.0] — 2026-03-30

### Added
- **Lite Mode:** frontend toggle (lightning button), auto-detect 2G/3G, compact hero, deferred images

### Fixed
- Responsive overflow on Galaxy Fold (280px), iPad (768/1024px)
- Engage-bar, currency-strip, mobile-menu-drawer overflow issues

## [2.1.0] — 2026-03-29

### Added
- 15th anniversary badge next to logo (2011–2026)
- Unified reactions + share card on article page (replacing 3-block layout)

### Changed
- Currency bar: hide change values by default, show on hover
- Floating share bar replaced with inline buttons

### Fixed
- Move lead directly under title
- Currency triangles on weekends
- Key facts gradient
- Larger article title
- Mobile meta row and touch targets

## [2.0.0] — 2026-03-28

### Added
- **Kazakh language version** (`/kz/...` routes) with full localization
- **Geo-personalization:** regional content based on user location
- **Smart recommendations** and reading history
- **Public comments** with moderation system
- **Entity-based story clustering** — auto-assign articles to story timelines
- **NLP extraction pipeline** (GPT-powered key facts, summaries, sentiment)
- **NER extraction** (Natasha) — persons, organizations, locations
- **Organization pages** (`/organizations`, `/organization/{name}`)

### Changed
- Homepage redesign v14.5 with hero slider, category highlights
- Article page redesign with NLP facts, reactions, timeline, sidebar
- Category pages redesign with card grid layout
- Person pages redesign with tabs, collapsible timeline, hero stats
- Footer redesign — logo+nav top, legal+socials bottom
- Admin panel v15 — simplified sidebar, compact dashboard

## [1.5.0] — 2026-03-22

### Added
- **News monitor service** — real-time RSS monitoring, GPT rewrite, Telegram alerts
- **AI-powered analytics** — Umami + GSC + content insights
- **Media library** with article versioning and restore
- **Admin AI assistant** for content workflows
- **Content calendar** for editorial planning

### Changed
- Admin mobile adaptation
- Unified cron pipeline (fetch → enrich → NER → tags → reindex)
- Performance: in-memory cache, batch queries, Brotli compression

### Fixed
- Server hardening (HSTS, CSP, Docker port binding)
- Search 503 on empty query
- Person 500 errors (sqlite3.Row conversion)

## [1.2.0] — 2026-03-17

### Added
- **Web Stories** (`/stories/{category}/{slug}`)
- **Sitemaps:** main, articles (paginated), persons, news, KZ
- **RSS feeds:** `/rss`, `/rss/{category}`, `/feed.json`
- **robots.txt**, **llms.txt** for crawler guidance
- **imgproxy** integration for image optimization (WebP, resize)
- **PostgreSQL migration** from SQLite (with SQLite fallback)

### Changed
- Tag cloud redesign with alphabetical browsing
- Category pages: uniform cards, 16:9 images

## [1.0.0] — 2026-03-10

### Added
- Initial release of Total.kz news portal
- News article scraping and storage
- Dashboard with statistics and charts
- Entity extraction and tagging
- GSC search analytics integration
- Docker Compose deployment
- Admin panel with CRUD for articles, categories, tags, users
