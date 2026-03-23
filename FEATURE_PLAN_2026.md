# Total.kz — План развития 2026

## Приоритет 1: SEO / GEO (Generative Engine Optimization)

### 1.1 Усиление structured data (JSON-LD)
- [x] NewsArticle + BreadcrumbList — уже есть
- [ ] Добавить multiple image formats (1x1, 4x3, 16x9) в NewsArticle
- [x] Добавить `articleSection`, `keywords`, `wordCount`, `articleBody` (summary)
- [x] Добавить `author.url` — ссылка на /persons/{id}
- [x] FAQ schema на страницах с enrichment
- [x] Organization schema — полная версия с sameAs (соцсети)
- [x] Person schema на /persons/{id} страницах
- [ ] ProfilePage schema на авторских страницах

### 1.2 Мета-теги и Open Graph
- [x] og:type article + og:title + og:description + og:image — есть
- [x] Добавить article:published_time, article:modified_time, article:author, article:section, article:tag
- [x] Twitter Card metadata (twitter:title, twitter:description, twitter:image, twitter:creator)
- [ ] og:image:width, og:image:height (1200x630)

### 1.3 robots.txt + AI-краулеры
- [x] Разрешить GPTBot, ClaudeBot, PerplexityBot, Google-Extended
- [x] Добавить ссылку на llms.txt

### 1.4 llms.txt — файл для AI-движков
- [x] Создать /llms.txt — описание сайта, структура, ключевые разделы
- [x] Создать /llms-full.txt — расширенная версия

### 1.5 Sitemap усиление
- [x] Добавить news:news namespace для Google News
- [x] Sitemap index (основной + news sitemap)
- [x] image:image в sitemap для статей с фото

### 1.6 Контентные GEO-оптимизации
- [x] Структура статей: TL;DR блок вверху (из enrichment.summary)
- [x] FAQ секция автоматически из enrichment
- [x] Цитаты экспертов (enrichment.quote) — цитатные карточки
- [x] "Ключевые факты" — bullet-блок из enrichment.keywords

## Приоритет 2: Instant Articles / Быстрый просмотр

### 2.1 Telegram Instant View
- [x] Адаптировать HTML структуру под IV шаблон
- [x] Добавить tg:site_verification мета-тег
- [x] Создать IV шаблон в Telegram IV Editor
- [ ] Связать с Telegram-каналом Total.kz

### 2.2 Google Web Stories
- [x] Автоматическая генерация Web Stories из топ-статей
- [x] AMP-совместимый формат
- [x] Метаданные для Google Discover

### 2.3 RSS-расширения для быстрого просмотра
- [x] Полный контент в RSS (content:encoded)
- [x] Медиа-вложения (enclosure для главной картинки)
- [x] Категории по разделам
- [x] JSON Feed (/feed.json) — альтернативный формат

## Приоритет 3: UX-фичи 2025-2026 трендов

### 3.1 Web Push уведомления
- [x] Service Worker для push
- [x] Подписка по категориям / Breaking news
- [x] Управление в настройках

### 3.2 AI-фичи для читателей
- [x] TL;DR / Краткое содержание (из enrichment)
- [x] "Ключевые факты" — блок bullet-points
- [x] Время чтения — уже есть
- [ ] Аудио-версия статей (TTS) — будущее

### 3.3 Улучшение навигации и поиска
- [ ] Trending/Popular tags — облако тегов
- [ ] "Читаемое сейчас" — сайдбар с популярными за 24ч
- [ ] Улучшенный search с фильтрами по категории и дате
- [x] Закладки (localstorage) — "Сохранить статью"

### 3.4 Performance / Core Web Vitals
- [ ] Brotli сжатие
- [ ] HTTP/2 Server Push для критических ресурсов
- [x] Image lazy loading с native loading="lazy" — уже есть
- [x] Preconnect для внешних ресурсов — уже есть
- [ ] Cache headers для статики

## Приоритет 4: Будущие интеграции
- [ ] NLWeb protocol (Microsoft) — /ask endpoint
- [ ] ActivityPub — RSS → Mastodon
- [x] Turbo Pages (Yandex) — для ru-аудитории
- [ ] Apple News Format
