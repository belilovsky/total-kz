"""GPT-4o-mini rewriter: translates and adapts articles for Total.kz style."""

import json
import logging
import os

from openai import OpenAI

log = logging.getLogger("news-monitor.rewriter")

SYSTEM_PROMPT = """Ты — редактор казахстанского новостного портала Total.kz.
Твоя задача — переписать новость в редакционном стиле Total.kz.

Правила:
1. Пиши на русском языке, грамотно и лаконично
2. Заголовок — цепляющий, информативный, до 100 символов
3. Лид (excerpt) — 1-2 предложения, суть новости
4. Текст статьи — 3-6 абзацев, от самого важного к деталям (перевёрнутая пирамида)
5. Не добавляй отсебятину — только факты из оригинала
6. Для иностранных источников — адаптируй для казахстанской аудитории
7. Укажи 3-5 тегов (через запятую)
8. Определи категорию: политика, экономика, общество, мир, спорт, технологии, культура

Ответ строго в JSON:
{
  "title": "Заголовок статьи",
  "excerpt": "Лид статьи",
  "body_html": "<p>Текст статьи в HTML</p>",
  "tags": ["тег1", "тег2", "тег3"],
  "category": "категория"
}"""


def rewrite_article(
    original_title: str,
    original_body: str,
    source_name: str,
    source_lang: str,
    needs_rewrite: bool = True,
) -> dict | None:
    """Rewrite/translate an article using GPT-4o-mini.

    Returns dict with: title, excerpt, body_html, tags, category
    Returns None on failure.
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        log.error("OPENAI_API_KEY not set — cannot rewrite")
        return None

    client = OpenAI(api_key=api_key)

    if needs_rewrite:
        user_msg = (
            f"Источник: {source_name} ({source_lang})\n\n"
            f"Заголовок: {original_title}\n\n"
            f"Текст:\n{original_body[:4000]}"
        )
    else:
        # Russian source — just restyle, don't translate
        user_msg = (
            f"Источник: {source_name}\n\n"
            f"Перепиши в стиле Total.kz (не переводи, только адаптируй стиль):\n\n"
            f"Заголовок: {original_title}\n\n"
            f"Текст:\n{original_body[:4000]}"
        )

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.3,
            max_tokens=2000,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content
        result = json.loads(raw)

        # Validate required fields
        for field in ("title", "excerpt", "body_html", "tags", "category"):
            if field not in result:
                log.error("GPT response missing field: %s", field)
                return None

        log.info("Rewritten: %s", result["title"][:80])
        return result

    except Exception as e:
        log.error("GPT rewrite failed: %s", e)
        return None
