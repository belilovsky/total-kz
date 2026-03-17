# Total.kz Dashboard

Панель управления контентом Total.kz — сбор, хранение и просмотр новостных статей.

## Возможности

- **Дашборд** — статистика, графики по месяцам и категориям, топ авторов
- **Статьи** — поиск, фильтрация по категориям, пагинация, просмотр полного текста
- **История парсинга** — логи всех запусков сбора контента
- **API** — JSON-эндпоинты для интеграций

## Стек

- **Backend:** FastAPI + Jinja2 + SQLite
- **Frontend:** HTML/CSS (dark theme), Chart.js
- **Деплой:** Docker Compose

## Быстрый старт

### Локально

```bash
pip install -r requirements.txt

# Импорт данных (поместите articles.jsonl в data/)
python scraper/import_data.py

# Запуск
uvicorn app.main:app --reload --port 8000
```

### Docker

```bash
docker compose up -d --build

# Импорт данных
docker compose exec web python scraper/import_data.py
```

### VPS (Hostinger)

```bash
curl -sL https://raw.githubusercontent.com/belilovsky/total-kz/main/setup.sh | bash
```

## Парсинг

```bash
# 1. Сбор URL-адресов
python scraper/scrape_urls.py

# 2. Загрузка контента
python scraper/download_content.py

# 3. Импорт в базу
python scraper/import_data.py
```

## API

- `GET /api/stats` — общая статистика
- `GET /api/articles?q=...&category=...&page=1` — список статей
- `GET /api/article/{id}` — полная статья
