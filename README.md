# Total.kz Dashboard

Панель управления контентом Total.kz — сбор, хранение и просмотр новостных статей.

## Возможности

- **Дашборд** — статистика, графики по месяцам и категориям, топ авторов
- **Статьи** — поиск, фильтрация по категориям, пагинация, просмотр полного текста
- **История парсинга** — логи всех запусков сбора контента
- **Парсер** — автоматический сбор новых статей с total.kz
- **API** — JSON-эндпоинты для интеграций

## Стек

- **Backend:** FastAPI + Jinja2 + SQLite → PostgreSQL
- **Frontend:** HTML/CSS (dark/light theme), без фреймворков
- **Поиск:** Meilisearch
- **Редактор:** Editor.js (блочный)
- **Деплой:** Docker Compose + GitHub Actions CI/CD

## Быстрый старт

### Docker (рекомендуется)

```bash
docker compose up -d --build
```

### Деплой

Автодеплой настроен через GitHub Actions — каждый push в `main` автоматически деплоится на сервер.

Ручной деплой на VPS:
```bash
cd /opt/total-kz && git pull && docker compose up -d --build
```

### Логи

```bash
docker compose logs -f app
```

### Переиндексация поиска

```bash
docker compose exec app python -m app.search_engine reindex
```
