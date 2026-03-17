# Total.kz Dashboard

Панель управления контентом Total.kz — сбор, хранение и просмотр новостных статей.

## Возможности

- **Дашборд** — статистика, графики по месяцам и категориям, топ авторов
- **Статьи** — поиск, фильтрация по категориям, пагинация, просмотр полного текста
- **История парсинга** — логи всех запусков сбора контента
- **Парсер** — автоматический сбор новых статей с total.kz
- **API** — JSON-эндпоинты для интеграций

## Стек

- **Backend:** FastAPI + Jinja2 + SQLite
- **Frontend:** HTML/CSS (dark theme), Chart.js
- **Парсер:** requests + BeautifulSoup (сбор URL) + httpx + selectolax (контент)
- **Деплой:** Docker Compose

## Быстрый старт

### Docker (рекомендуется)

```bash
docker compose up -d --build

# Импорт данных (если articles.jsonl в data/)
docker compose exec web python scraper/import_data.py
```

### Локально

```bash
pip install -r requirements.txt
python scraper/import_data.py
uvicorn app.main:app --reload --port 3847
```

### VPS (Hostinger)

```bash
curl -sL https://raw.githubusercontent.com/belilovsky/total-kz/main/setup.sh | bash
```

## Парсер

### Сбор новых статей

```bash
# Собрать URL за последний год (по умолчанию)
docker compose exec web python scraper/scrape_urls.py

# Собрать URL за последние 30 дней
docker compose exec web python scraper/scrape_urls.py --days 30

# Собрать URL начиная с конкретной даты
docker compose exec web python scraper/scrape_urls.py --since 2024-01-01
```

### Загрузка контента

```bash
# Скачать контент для всех собранных URL
docker compose exec web python scraper/download_content.py

# С автоматическим импортом в БД
docker compose exec web python scraper/download_content.py --import-db

# Больше параллельных запросов (по умолчанию 15)
docker compose exec web python scraper/download_content.py --workers 20
```

### Импорт в базу

```bash
docker compose exec web python scraper/import_data.py
```

### Полный цикл обновления

```bash
# 1. Собрать новые URL
docker compose exec web python scraper/scrape_urls.py --days 7

# 2. Скачать контент и импортировать
docker compose exec web python scraper/download_content.py --import-db
```

## API

- `GET /api/stats` — общая статистика
- `GET /api/articles?q=...&category=...&page=1` — список статей
- `GET /api/article/{id}` — полная статья
