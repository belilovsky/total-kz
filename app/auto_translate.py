"""Auto-translate articles to Kazakh using GPT on publish.

Background task — does not block the publish action.
Uses the same GPT model/prompt as scripts/translate_articles_kz.py.
"""

import json
import logging
import os
import re

import httpx

from app import db_backend as db

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

SYSTEM_PROMPT = (
    "Ты — профессиональный переводчик с русского на казахский язык "
    "для казахстанского новостного портала Total.kz.\n\n"
    "Требования к переводу:\n"
    "1. Используй литературный казахский язык (әдеби қазақ тілі), не разговорный\n"
    "2. Имена собственные людей НЕ переводи — оставь как есть "
    "(Касым-Жомарт Токаев, Аида Балаева)\n"
    "3. Казахстанские названия используй на казахском "
    "(ВКО→ШҚО, ЗКО→БҚО, СКО→СҚО, Восточный Казахстан→Шығыс Қазақстан, "
    "Министерство внутренних дел→Ішкі істер министрлігі)\n"
    "4. Сохрани ВСЮ HTML-разметку (<p>, <a href=\"...\">, <blockquote>, "
    "<strong>, <em>) — переводи ТОЛЬКО текст внутри тегов\n"
    "5. НЕ переводи: URL-адреса, числа, даты в формате цифр\n"
    "6. Перевод должен быть естественным и читаемым, не дословным\n"
    "7. Ответ — СТРОГО JSON без пояснений:\n"
    '{"title_kz": "...", "excerpt_kz": "...", "body_html_kz": "..."}'
)


def _has_translation(article_id: int) -> bool:
    """Check if article already has a Kazakh translation."""
    try:
        row = db.execute_raw(
            "SELECT id FROM article_translations WHERE article_id = %s AND lang = %s",
            (article_id, "kz"),
        )
        return row is not None
    except Exception:
        return False


def _translate_via_gpt(title: str, excerpt: str, body_html: str) -> dict | None:
    """Call GPT to translate article fields. Returns parsed JSON or None."""
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY not set — skipping auto-translation")
        return None

    body_html = (body_html or "")[:12000]
    body_html = body_html.replace('\x00', '')
    user_msg = f"Заголовок: {title}\nЛид: {excerpt}\nТекст: {body_html}"

    try:
        resp = httpx.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": 0.3,
                "max_tokens": 4096,
                "response_format": {"type": "json_object"},
            },
            timeout=90,
        )
        data = resp.json()

        if "error" in data:
            logger.error("GPT translation API error: %s", data["error"].get("message", ""))
            return None

        content = data["choices"][0]["message"]["content"]
        return json.loads(content)

    except json.JSONDecodeError:
        try:
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception:
            pass
        return None
    except Exception as e:
        logger.error("GPT translation error: %s", e)
        return None


def _save_translation(article_id: int, data: dict):
    """Save translation to article_translations table."""
    body_html = data.get("body_html_kz", "")
    body_text = re.sub(r'<[^>]+>', ' ', body_html).strip()
    body_text = re.sub(r'\s+', ' ', body_text)

    from app.config import settings
    if settings.use_postgres:
        from app.pg_database import SessionLocal
        from sqlalchemy import text
        session = SessionLocal()
        try:
            session.execute(text(
                "INSERT INTO article_translations "
                "(article_id, lang, title, excerpt, body_html, body_text) "
                "VALUES (:aid, 'kz', :title, :excerpt, :body_html, :body_text) "
                "ON CONFLICT (article_id, lang) DO UPDATE SET "
                "title = EXCLUDED.title, excerpt = EXCLUDED.excerpt, "
                "body_html = EXCLUDED.body_html, body_text = EXCLUDED.body_text, "
                "translated_at = NOW()"
            ), {
                "aid": article_id,
                "title": data.get("title_kz", ""),
                "excerpt": data.get("excerpt_kz", ""),
                "body_html": body_html,
                "body_text": body_text,
            })
            session.commit()
        finally:
            session.close()
    else:
        from app.database import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO article_translations "
                "(article_id, lang, title, excerpt, body_html, body_text) "
                "VALUES (?, 'kz', ?, ?, ?, ?)",
                (article_id, data.get("title_kz", ""),
                 data.get("excerpt_kz", ""), body_html, body_text),
            )
            conn.commit()


def auto_translate_article(article_id: int):
    """Translate an article to Kazakh. Meant to run in a background thread."""
    try:
        if _has_translation(article_id):
            logger.info("Article %d already translated, skipping", article_id)
            return

        article = db.get_article(article_id)
        if not article:
            return

        title = article.get("title", "")
        excerpt = article.get("excerpt", "")
        body_html = article.get("body_html", "")

        if not title:
            return

        logger.info("Auto-translating article %d: %s", article_id, title[:60])
        data = _translate_via_gpt(title, excerpt, body_html)
        if data:
            _save_translation(article_id, data)
            logger.info("Article %d translated successfully", article_id)
        else:
            logger.warning("Translation failed for article %d", article_id)
    except Exception as e:
        logger.error("Auto-translate error for article %d: %s", article_id, e)
