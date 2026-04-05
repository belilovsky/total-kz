# Архитектура Total.kz

## Сервисы Docker

| Сервис | Образ | Порт | Назначение |
|--------|-------|------|------------|
| total_kz_app | total-kz-app:latest | 3847 | FastAPI (Python) — основное приложение |
| total_kz_db | pgvector/pgvector:pg16 | 5437 | PostgreSQL — статьи, NLP, entities |
| total_kz_imgproxy | darthsim/imgproxy:v3.27 | 8080 (internal) | Оптимизация изображений |
| total_kz_meilisearch | getmeili/meilisearch | internal | Полнотекстовый поиск |
| total_kz_cron | total-kz-app | — | Фоновые задачи (healthcheck, backup) |
| total_kz_monitor | total-kz-monitor | — | Мониторинг источников новостей |
| total_kz_umami | ghcr.io/umami-software/umami | 3000 | Аналитика посещаемости |
| total_kz_umami_db | postgres:15 | — | БД для Umami |

## Разделение данных

### PostgreSQL (187K+ статей)
- `articles` — 187K статей (title, body_html, pub_date, main_image, image_credit, url, sub_category)
- `article_entities` — 1.5M связей статья↔сущность
- `article_nlp` — 12K записей (sentiment, key_facts, quote)
- `entities` — 300K (person: 145K, org: 112K, location: 43K)
- `article_tags`, `article_stories`, `article_translations`

### SQLite (data/total.db) — справочники и кэш
- `persons` — 10K (>5 упоминаний), slug + short_name + mention_count
- `organizations` — 10K
- `article_entities` — 1.5M (синхронизировано из PG для person/org routes)
- `article_comments`, `audit_log`, `person_positions`

**Синхронизация PG→SQLite:** `create_persons.py` и `sync_ae2.py`

## Конвейер изображений

1. **Локальные** (`/media/xx/hash.jpg`) → app serve_media route → прямая отдача
2. **Внешние** (`https://...`) → `imgproxy_url()` → `/imgproxy/insecure/resize:fit:{w}:0/plain/{url}@webp`
3. **imgproxy** требует DNS (8.8.8.8, 1.1.1.1) и `ALLOWED_SOURCES=https://,http://`
4. `imgproxy_url()` делает `html.unescape()` для исправления `&amp;` в URL из БД

## CSS-лейаут

- **CSS версия:** v34.0 (`base.html` → `public.min.css?v=34.0`)
- **Статья:** `article-layout` = CSS grid `1fr 300px`. Sidebar в `article-layout` (НЕ в `article-body-grid`). Hero — full-width над grid.
- **body_html:** Jinja-фильтр `balance_divs` удаляет orphan closing tags (div, blockquote, figure, section и др.)
- **Главная:** hero + live-feed (max 600px, scroll), 4 shelf-cards, category shelves, feed-duo (Последние + Популярные)
- **Категория:** uniform 3-col grid + 300px sidebar

## Навигация (NAV_SECTIONS)

| Slug | Label | Subcats |
|------|-------|---------|
| politika | Политика | vnutrennyaya_politika, vneshnyaya_politika, gossektor |
| ekonomika | Экономика | ekonomika_sobitiya, finansi, biznes |
| obshchestvo | Общество | obshchestvo_sobitiya, obshchestvo, zhizn, proisshestviya, bezopasnost, stil_zhizni, religiya, kultura, mneniya |
| zakon | Закон | proisshestviya, bezopasnost *(remapped — нет оригинальных zakon subcats)* |
| nauka | Наука и Техно | tehno, nauka |
| mir | Мир | mir |
| sport | Спорт | sport |
