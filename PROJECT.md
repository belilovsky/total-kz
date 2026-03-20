# total-kz — Проектная документация

**Проект:** Total.kz — новостной портал 2026
**Стек:** Python + FastAPI + PostgreSQL + Docker
**Язык документа:** русский

## 1. Обзор проекта

### Цель
Панель управления контентом Total.kz: сбор, хранение, просмотр новостных статей.

### Структура

- `app/` — FastAPI-приложение
- `data/` — данные
- `scraper/` — парсер статей

## 2. Инфраструктура

- **База данных:** PostgreSQL
- **Контейнеризация:** Docker + docker-compose

## 3. Запуск

```bash
cp .env.example .env
docker compose up -d --build
```
