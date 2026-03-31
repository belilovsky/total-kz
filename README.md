# Total.kz — Новостной агрегатор Казахстана

[![Deploy](https://img.shields.io/badge/deploy-VPS-brightgreen)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Total.kz** — мультиязычный новостной портал, агрегирующий и обогащающий новости Казахстана. Автоматический сбор, NLP-обработка, NER-извлечение персон и организаций, интерактивные сюжетные линии, мультиплатформенная дистрибуция.

🌐 **Продакшен:** [https://total.qdev.run](https://total.qdev.run)

---

## Содержание

- [Архитектура](#архитектура)
- [Стек технологий](#стек-технологий)
- [Быстрый старт](#быстрый-старт)
- [Структура проекта](#структура-проекта)
- [Публичный сайт](#публичный-сайт)
- [Админ-панель](#админ-панель)
- [RSS-фиды и платформенная дистрибуция](#rss-фиды-и-платформенная-дистрибуция)
- [SEO и поисковая оптимизация](#seo-и-поисковая-оптимизация)
- [Режимы отображения](#режимы-отображения)
- [Авто-постинг и социальные сети](#авто-постинг-и-социальные-сети)
- [API](#api)
- [Деплой](#деплой)
- [Конфигурация](#конфигурация)

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                        Nginx (reverse proxy)                │
│                   SSL termination, caching (120s)           │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                     Docker Compose                          │
│                                                             │
│  ┌─────────────┐  ┌──────────┐  ┌───────────┐             │
│  │  app (8000) │  │ cron     │  │ monitor   │             │
│  │  FastAPI    │  │ pipeline │  │ real-time  │             │
│  │  + Jinja2   │  │ 2h cycle │  │ news feed │             │
│  └──────┬──────┘  └────┬─────┘  └─────┬─────┘             │
│         │              │              │                     │
│  ┌──────▼──────────────▼──────────────▼─────┐              │
│  │        PostgreSQL 16 (pgvector)          │              │
│  └──────────────────────────────────────────┘              │
│                                                             │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────┐          │
│  │ Meilisearch │  │  imgproxy    │  │  Umami   │          │
│  │ full-text   │  │  image CDN   │  │ analytics│          │
│  └─────────────┘  └──────────────┘  └──────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### Контейнеры

| Контейнер | Назначение | Порт |
|-----------|-----------|------|
| `total_kz_app` | FastAPI приложение | 8000 |
| `total_kz_cron` | Пайплайн сбора: fetch → enrich → NER → tags → reindex (каждые 2ч) |  |
| `total_kz_monitor` | Мониторинг новостных лент в реальном времени |  |
| `total_kz_db` | PostgreSQL 16 с pgvector | 5432 |
| `total_kz_meilisearch` | Полнотекстовый поиск | 7700 |
| `total_kz_imgproxy` | Проксирование и оптимизация изображений (WebP) | 8080 |
| `total_kz_umami` | Веб-аналитика | 3000 |

---

## Стек технологий

- **Backend:** Python 3.12, FastAPI, Jinja2, SQLAlchemy 2.0, Alembic
- **Database:** PostgreSQL 16 (pgvector), SQLite fallback
- **Search:** Meilisearch v1.12
- **NLP:** Natasha (NER), pymorphy3 (морфология), OpenAI (обогащение)
- **Frontend:** Vanilla HTML/CSS/JS, без фреймворков. Тёмная/светлая тема, адаптив
- **Images:** imgproxy v3.27 (автоматическая конвертация в WebP, ресайз)
- **Analytics:** Umami (self-hosted)
- **Deploy:** Docker Compose, Nginx, GitHub Actions CI/CD

---

## Быстрый старт

### Требования

- Docker 24+ и Docker Compose v2
- 4 GB RAM минимум (рекомендуется 8 GB)
- GitHub Personal Access Token (для приватного пакета `qazstack`)

### Запуск

```bash
# 1. Клонировать
git clone https://github.com/belilovsky/total-kz.git
cd total-kz

# 2. Настроить переменные окружения
cp .env.example .env
# Отредактировать .env: DB_PASSWORD, GITHUB_TOKEN, OPENAI_API_KEY

# 3. Запустить
docker compose up -d --build

# 4. Проверить
curl http://localhost:3847/health
```

### Полезные команды

```bash
# Логи приложения
docker compose logs -f app

# Переиндексация поиска
docker compose exec app python -m app.search_engine reindex

# Миграции БД
docker compose exec app alembic upgrade head

# Бэкап PostgreSQL
docker compose exec db pg_dump -U total_kz total_kz > backup.sql
```

---

## Структура проекта

```
total-kz/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app + админ-маршруты (104 route)
│   ├── public_routes.py     # Публичные маршруты (58 routes)
│   ├── social_routes.py     # Соц.сети маршруты
│   ├── config.py            # Конфигурация (env → Settings)
│   ├── models.py            # SQLAlchemy ORM модели
│   ├── database.py          # SQLite backend
│   ├── pg_queries.py        # PostgreSQL backend
│   ├── pg_database.py       # PG connection management
│   ├── db_backend.py        # Автоматический выбор backend
│   ├── cache.py             # In-memory кэширование (TTL 5 мин)
│   ├── auth.py              # Аутентификация (bcrypt)
│   ├── autopost.py          # Авто-постинг в Telegram
│   ├── social.py            # Форматирование постов, Telegram Bot API
│   ├── scheduler.py         # Планировщик (публикация отложенных, WebSub ping)
│   ├── search_engine.py     # Meilisearch интеграция
│   ├── search_analytics.py  # Аналитика поиска (GSC)
│   ├── seo_analytics.py     # SEO метрики
│   ├── currency.py          # Курсы валют
│   ├── geo.py               # Гео-детекция регионов
│   ├── static/
│   │   ├── css/
│   │   │   ├── public.css       # Основные стили (~195KB)
│   │   │   └── public.min.css   # Минифицированные стили
│   │   ├── js/
│   │   └── img/
│   └── templates/
│       ├── public/              # Публичные шаблоны (19 файлов)
│       │   ├── base.html        # Базовый layout (meta, feeds, PWA)
│       │   ├── home.html        # Главная страница
│       │   ├── article.html     # Страница статьи
│       │   ├── category.html    # Категория
│       │   ├── search.html      # Поиск
│       │   ├── person.html      # Страница персоны
│       │   └── ...
│       └── *.html               # Админ-шаблоны (21 файл)
├── scraper/                     # Скрипты сбора новостей
├── scripts/                     # Утилиты (миграции, бэкапы)
├── services/
│   └── news-monitor/            # Real-time мониторинг лент
├── alembic/                     # Миграции БД
├── data/                        # Данные (volume mount)
├── deploy/                      # Конфигурации деплоя
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Публичный сайт

### Страницы

| URL | Описание |
|-----|----------|
| `/` | Главная — hero-слайдер, категории, тренды, курсы валют |
| `/news/{category}` | Лента категории с пагинацией |
| `/news/{category}/{slug}` | Статья — NLP-факты, реакции, сюжет, комментарии |
| `/search?q=...` | Полнотекстовый поиск (Meilisearch) |
| `/persons` | Все персоны (NER) |
| `/person/{slug}` | Профиль персоны — упоминания, хронология |
| `/organizations` | Все организации |
| `/tags` | Облако тегов |
| `/tag/{name}` | Статьи по тегу |
| `/stories` | AMP-stories |
| `/bookmarks` | Закладки пользователя (localStorage) |

### Категории (NAV_SECTIONS)

`politika` · `ekonomika` · `obshchestvo` · `zakon` · `nauka` · `mir` · `sport`

### Функции

- **NLP-обогащение:** ключевые факты, саммари, мета-описания, цитаты (OpenAI)
- **NER:** извлечение персон, организаций, локаций (Natasha)
- **Сюжетные линии:** автоматическое связывание статей в хронологии
- **Реакции:** 👍💡😄😢😡 с анонимным голосованием
- **Комментарии:** модерируемые комментарии к статьям
- **PWA:** Service Worker, manifest.json, офлайн-режим
- **Lite Mode:** облегчённый режим для медленного интернета (2G/3G)
- **Мультиязычность:** русский + казахский (`/kz/...`)
- **Адаптивность:** 280px (Galaxy Fold) → 2560px (4K), полная поддержка мобильных

---

## Админ-панель

**URL:** `/admin` (требует авторизацию)

| Раздел | Описание |
|--------|----------|
| Дашборд | Статистика, графики по месяцам/категориям, топ авторов |
| Статьи | CRUD, поиск, фильтрация, bulk-операции, workflow (draft→review→published) |
| Контент | Редактор статей (Editor.js) |
| Категории | Управление рубриками |
| Теги | Управление тегами, merge дублей |
| Персоны/Организации | NER-сущности, редактирование, объединение |
| Медиа | Загрузка и управление изображениями |
| Авторы | Управление авторами |
| Сюжеты | Ручное создание сюжетных линий |
| Комментарии | Модерация комментариев |
| Соц.сети | Подключение аккаунтов, планирование постов |
| Реклама | Управление рекламными слотами |
| Аналитика | Umami embed, GSC-аналитика |
| Настройки | Режим отображения, пользователи, общие настройки |
| Календарь | Редакционный календарь |

---

## RSS-фиды и платформенная дистрибуция

### Фиды

| Эндпоинт | Платформа | Описание |
|----------|-----------|----------|
| `/rss` | Универсальный | RSS 2.0, 50 статей, `content:encoded` с полным HTML |
| `/rss/{category}` | По категориям | RSS 2.0 для конкретной рубрики |
| `/feed.json` | JSON Feed | JSON Feed 1.1 |
| `/turbo/rss.xml` | Яндекс Турбо | Turbo Pages RSS, полный `turbo:content` |
| `/zen/rss.xml` | Яндекс Дзен | RSS с `content:encoded`, 50 статей |
| `/fb-ia/rss.xml` | Facebook | Instant Articles RSS с полной IA-разметкой |
| `/flipboard/rss.xml` | Flipboard | RSS с `media:content` (1200×675) |

### Мгновенные просмотры

- **Telegram Instant View:** мета-тег `tg:site_verification`, структура HTML оптимизирована для IV-шаблонов
- **WebSub/PubSubHubbub:** все RSS-фиды содержат `<link rel="hub">`, автоматический ping при публикации

### Feed Discovery

В `<head>` каждой страницы — 5 `<link rel="alternate">` тегов для автообнаружения фидов.

---

## SEO и поисковая оптимизация

### Sitemap

| URL | Содержимое |
|-----|-----------|
| `/sitemap.xml` | Индексный sitemap |
| `/sitemap-main.xml` | Основные страницы |
| `/sitemap-articles-{page}.xml` | Статьи (пагинированный) |
| `/sitemap-persons.xml` | Страницы персон |
| `/sitemap-news.xml` | Google News Sitemap |
| `/kz/sitemap.xml` | Казахская версия |

### Structured Data (JSON-LD)

- `NewsArticle` с `dateModified`, `isAccessibleForFree`, `speakable`
- `BreadcrumbList` навигация
- `FAQPage` (если есть ключевые факты)
- `Organization` publisher

### Мета-теги

- Open Graph (article type, image с размерами, published_time, author, section, tags)
- Twitter Cards (summary_large_image)
- Telegram (`tg:site_name`, `tg:site_verification`)

### Прочее

- `/robots.txt` — динамический, с Sitemap
- `/llms.txt`, `/llms-full.txt` — для LLM-краулеров
- `/.well-known/nlweb.json` — NLWeb protocol

---

## Режимы отображения

Три режима, управляемых из админки (`/admin/settings` → Режим оформления):

| Режим | Описание |
|-------|----------|
| **Нормальный** | Стандартное отображение |
| **Траур** | Grayscale фильтр, приглушённые тона, баннер |
| **Праздник** | Праздничные декорации для 9 государственных праздников КЗ |
| **Вечерний** | Приоритизация позитивного контента после 20:00 |

**API:** `GET /api/display-mode` · `POST /api/admin/display-mode`

---

## Авто-постинг и социальные сети

### Авто-постинг в Telegram

Модуль `autopost.py` — фоновый процесс, проверяющий новые статьи каждые 5 минут.

- Настройка: `/admin/social` → добавить Telegram аккаунт с `auto_post: true`
- Управление: `GET/POST /api/admin/auto-post`
- Rate limit: 1 сообщение / 3 секунды
- Фильтрация по категориям
- Трекинг опубликованных (избежание дублей)

### Социальные аккаунты

Поддерживаемые платформы: Telegram, Instagram, YouTube, Facebook, TikTok, X (Twitter), VK.

Управление: `/admin/social` — подключение аккаунтов, планирование постов, статистика.

---

## API

### Публичные

| Метод | Endpoint | Описание |
|-------|----------|----------|
| GET | `/api/feed?offset=N` | Подгрузка ленты (HTML-фрагмент) |
| GET | `/api/recommendations` | Рекомендации (HTML-фрагмент) |
| GET | `/api/suggest?q=...` | Автодополнение поиска |
| POST | `/api/view/{id}` | Инкремент просмотров |
| GET | `/api/public/comments/{id}` | Комментарии к статье |
| POST | `/api/public/comments/{id}` | Добавить комментарий |
| GET | `/api/public/reactions/{id}` | Реакции к статье |
| POST | `/api/public/reactions/{id}` | Добавить реакцию |
| POST | `/api/push/subscribe` | Web Push подписка |
| DELETE | `/api/push/unsubscribe` | Web Push отписка |
| GET | `/api/display-mode` | Текущий режим отображения |
| GET | `/ask` | AI-ассистент (NLWeb) |

### Админские (требуют авторизацию)

| Метод | Endpoint | Описание |
|-------|----------|----------|
| POST | `/api/admin/display-mode` | Сменить режим отображения |
| GET/POST | `/api/admin/auto-post` | Управление авто-постингом |
| POST | `/api/article/{id}/workflow/{action}` | Workflow статьи |
| POST | `/api/article/{id}/assign` | Назначить редактора |
| POST | `/api/articles/bulk` | Массовые операции |
| POST | `/api/media/upload` | Загрузка медиа |
| POST | `/api/user` | CRUD пользователей |
| POST | `/api/category` | CRUD категорий |
| POST | `/api/tag` | CRUD тегов |
| POST | `/api/entity` | CRUD сущностей |
| POST | `/api/story` | CRUD сюжетов |
| POST | `/api/comments/{id}/moderate` | Модерация комментария |
| POST | `/api/ads/{slot}/toggle` | Вкл/выкл рекламного слота |

---

## Деплой

### Продакшен (VPS)

```
Сервер: srv1380923 (62.72.32.112)
Путь:   /opt/total-kz
Стек:   Docker Compose → Nginx reverse proxy → Let's Encrypt SSL
Домен:  total.qdev.run
```

### Процесс деплоя

1. **Автоматический:** Push в `main` → GitHub Actions → docker compose up
2. **Ручной:**
```bash
# На VPS
cd /opt/total-kz
git pull origin main
docker compose up -d --build
```

### Горячий деплой (без пересборки)

```bash
# Копировать файлы напрямую в контейнер
docker cp app/public_routes.py total_kz_app:/app/app/public_routes.py
docker stop total_kz_app && sleep 2 && docker start total_kz_app
```

> **Важно:** `docker stop` + `docker start` (не `restart`) для очистки Jinja bytecode кэша.

### Кэширование

- **Nginx:** `max-age=120, stale-while-revalidate=300` — изменения видны через ~2 мин
- **In-memory:** Homepage кэшируется 5 мин (`cache.TTL_HOMEPAGE = 300`)
- **CSS:** версионирование через `?v=N` (текущая v20.0)

---

## Конфигурация

### Переменные окружения (.env)

| Переменная | Описание | Обязательна |
|-----------|----------|-------------|
| `DB_PASSWORD` | Пароль PostgreSQL | ✅ |
| `GITHUB_TOKEN` | GitHub PAT (для qazstack) | ✅ |
| `OPENAI_API_KEY` | OpenAI API ключ (обогащение) | Для NLP |
| `APP_PORT` | Порт приложения (default: 3847) | |
| `USE_POSTGRES` | Использовать PostgreSQL (default: false) | |
| `PG_DATABASE_URL` | PostgreSQL connection string | Если USE_POSTGRES |
| `MEILI_MASTER_KEY` | Ключ Meilisearch | |
| `SITE_DOMAIN` | Домен сайта | |
| `UMAMI_WEBSITE_ID` | ID сайта в Umami | Для аналитики |

---

## Лицензия

[MIT](LICENSE)
