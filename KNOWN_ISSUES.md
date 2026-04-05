# Известные проблемы и обходные пути

## 1. Docker Compose zombie-контейнер
**Проблема:** compose помнит контейнер `d7d4c4dfe2d7` который не существует. `docker compose up -d` падает на app.
**Обход:** запускать app вручную:
```bash
docker run -d --name total_kz_app --restart unless-stopped \
  --env-file /opt/total-kz/.env -e SITE_DOMAIN=https://total.qdev.run \
  -p 127.0.0.1:3847:8000 \
  -v /opt/total-kz/data:/app/data -v /opt/total-kz/app:/app/app \
  --network total_kz_net \
  --health-cmd "curl -f http://localhost:8000/health" \
  --health-interval 30s --health-timeout 10s --health-start-period 15s \
  total-kz-app:latest
```

## 2. Категория «Закон» — нет контента
**Проблема:** в БД нет статей с `sub_category` = zakon/pravo/zakonodatelstvo.
**Обход:** remapped на `proisshestviya + bezopasnost` в NAV_SECTIONS.

## 3. body_html — незакрытые теги
**Проблема:** HTML из total.kz содержит orphan `</div>`, `</blockquote>` и т.д.
**Обход:** Jinja-фильтр `balance_divs` в `public_routes.py`. При появлении нового типа тега — добавить в список.

## 4. 24 статьи без изображений
**Проблема:** Google News redirect URLs, og:image недоступен.
**Статус:** не критично, менее 0.01% статей.

## 5. KZ версия — контент не переведён
**Проблема:** UI на казахском, но заголовки/текст статей на русском.
**Решение:** модуль `auto_translate.py` готов, нужен GPT API ключ в .env (`OPENAI_API_KEY`).

## 6. imgproxy URL с &amp;
**Проблема:** в БД некоторые image URL содержат `&amp;` вместо `&`.
**Обход:** `imgproxy_url()` делает `html.unescape()`. Также почищено 205 записей в БД.

## 7. SQLite синхронизация
**Проблема:** persons/organizations/article_entities в SQLite — копия из PG.
**Обновление:** `python3 create_persons.py` и `python3 sync_ae2.py` на VPS.

## 8. Sentiment SQL
**Проблема:** `pub_date` — TEXT, не timestamp. Нужен cast `pub_date::timestamp`.
**Статус:** исправлено в `_query_sentiment_data` и `/api/analytics/sentiment`.

## 9. После ребута VPS
Все контейнеры total-kz нужно запустить вручную:
```bash
cd /opt/total-kz
docker compose up -d db imgproxy meilisearch cron umami_db umami
# Подключить к сети
for c in total_kz_db total_kz_imgproxy total_kz_meilisearch total_kz_cron total_kz_umami total_kz_umami_db; do
  docker network connect total_kz_net $c --alias $(echo $c | sed "s/total_kz_//")
done
# Запустить app
docker run -d --name total_kz_app ... (см. п.1)
```
