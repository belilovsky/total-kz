#!/usr/bin/env python3
"""
NER-извлечение сущностей из статей Total.kz.
Использует Natasha (русскоязычный NLP) для извлечения:
- Персон (PER)
- Организаций (ORG)
- Локаций (LOC)

Также денормализует теги из JSON-поля в таблицу article_tags.

Запуск:
    python scraper/extract_entities.py              # обработать все статьи
    python scraper/extract_entities.py --batch 1000 # по 1000 за раз
    python scraper/extract_entities.py --tags-only   # только теги, без NER
    python scraper/extract_entities.py --reprocess   # переобработать все (сброс)
"""
import json
import re
import sys
import argparse
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.database import get_db, init_db

try:
    # pymorphy2 сломан на Python 3.12+ (pkg_resources)
    # Подменяем на pymorphy3
    import pymorphy3
    sys.modules['pymorphy2'] = pymorphy3
    sys.modules['pymorphy2.analyzer'] = pymorphy3.analyzer
    sys.modules['pymorphy2.tagset'] = pymorphy3.tagset
    sys.modules['pymorphy2.shapes'] = pymorphy3.shapes

    from natasha import (
        Segmenter, MorphVocab,
        NewsEmbedding, NewsMorphTagger, NewsNERTagger,
        NamesExtractor, Doc,
    )
    HAS_NATASHA = True
except ImportError:
    HAS_NATASHA = False
    print("⚠ Natasha не установлена. Запустите: pip install natasha pymorphy3 pymorphy3-dicts-ru")
    print("  Будут обработаны только теги.")


# ═══════════════════════════════════════════════════════════════════
# ЧЁРНЫЙ СПИСОК — шумные сущности, которые NER ошибочно распознаёт
# ═══════════════════════════════════════════════════════════════════
BLACKLIST_EXACT = {
    # Сайт / бренд
    "total.kz", "total", "тотал", "тотал.kz",
    # Общие слова, ложно распознанные как сущности
    "казахстанец", "казахстанцы", "казахстанка",
    "президент", "министр", "премьер", "депутат", "аким",
    "мажилис", "сенат", "правительство",
    "фото", "видео", "источник", "редакция", "автор",
    "тенге", "доллар", "рубль", "евро",
    "нур-султан",  # устаревшее, дубликат Астаны
}

# Паттерны — если normalized содержит эти подстроки, сущность отбрасывается
BLACKLIST_PATTERNS = [
    r"^https?://",      # URL-ы
    r"^www\.",          # URL-ы
    r"\.kz$",           # домены
    r"\.ru$",           # домены
    r"\.com$",          # домены
    r"^\d+$",           # числа
    r"^[а-яё]$",        # одиночные буквы
]

# Минимальная длина имени для каждого типа
MIN_NAME_LENGTH = {
    "person": 4,   # "Ли" — слишком коротко, ложные срабатывания
    "org": 2,
    "location": 2,
}

# Паттерн для очистки мусора в начале и конце строки
# Ловит обрезки слов, прилипшие к реальному имени
GARBAGE_PREFIX_RE = re.compile(
    r'^[а-яёa-z]{1,4}(?=[А-ЯЁA-Z])'  # 1-4 строчных символа перед заглавной
)
GARBAGE_SUFFIX_RE = re.compile(
    r'[а-яёa-z]{1,3}$'  # 1-3 строчных символа в конце (после пробела — не трогаем)
)

# Валидный формат имени персоны: минимум 2 слова, каждое с заглавной
PERSON_NAME_RE = re.compile(r'^[А-ЯЁA-Z][а-яёa-z]+\s+[А-ЯЁA-Z][а-яёa-z]+')


def normalize_name(name: str) -> str:
    """Нормализация имени для дедупликации."""
    return " ".join(name.strip().split()).lower()


def clean_entity_name(raw_name: str, entity_type: str) -> str | None:
    """
    Очищает и валидирует имя сущности.
    Возвращает очищенное имя или None, если сущность мусорная.
    """
    if not raw_name:
        return None

    name = raw_name.strip()

    # Удаляем кавычки и скобки по краям
    name = name.strip('«»""\'()[]{}')
    name = name.strip()

    if not name:
        return None

    # Проверяем чёрный список (точное совпадение)
    if normalize_name(name) in BLACKLIST_EXACT:
        return None

    # Проверяем паттерны чёрного списка
    norm = normalize_name(name)
    for pattern in BLACKLIST_PATTERNS:
        if re.search(pattern, norm):
            return None

    # Чистим мусорные префиксы: "делАрман" → "Арман"
    # Только если в имени есть переход строчная→заглавная без пробела
    if re.search(r'[а-яёa-z][А-ЯЁA-Z]', name):
        # Находим первую заглавную после строчной
        match = re.search(r'[А-ЯЁA-Z]', name)
        if match:
            prefix = name[:match.start()]
            # Если перед заглавной нет пробела, а есть строчные — это мусор
            if prefix and not prefix[-1].isspace() and prefix[-1].islower():
                name = name[match.start():]

    # Проверяем минимальную длину
    min_len = MIN_NAME_LENGTH.get(entity_type, 2)
    if len(name) < min_len:
        return None

    # Для персон — проверяем формат: минимум Имя Фамилия
    if entity_type == "person":
        words = name.split()
        if len(words) < 2:
            return None
        # Каждое слово должно начинаться с заглавной
        if not all(w[0].isupper() for w in words if w):
            # Попробуем title case
            name = name.title()
            words = name.split()
            if not all(w[0].isupper() for w in words if w):
                return None

    # Финальная проверка — не слишком длинное
    if len(name) > 100:
        return None

    return name


def is_blacklisted(normalized: str) -> bool:
    """Проверка — шумная ли это сущность."""
    if normalized in BLACKLIST_EXACT:
        return True
    for pattern in BLACKLIST_PATTERNS:
        if re.search(pattern, normalized):
            return True
    return False


def extract_tags(conn):
    """Денормализовать теги из JSON-поля articles.tags в article_tags."""
    print("\n═══ Денормализация тегов ═══")

    # Сколько уже обработано
    existing = conn.execute("SELECT COUNT(DISTINCT article_id) FROM article_tags").fetchone()[0]
    print(f"  Уже обработано статей: {existing}")

    # Получаем статьи с тегами, которых ещё нет в article_tags
    rows = conn.execute("""
        SELECT a.id, a.tags FROM articles a
        WHERE a.tags IS NOT NULL AND a.tags != '[]' AND a.tags != ''
        AND a.id NOT IN (SELECT DISTINCT article_id FROM article_tags)
    """).fetchall()

    print(f"  Статей для обработки: {len(rows)}")
    count = 0

    for row in rows:
        try:
            tags = json.loads(row[1])
            for tag in tags:
                tag = tag.strip()
                if tag:
                    conn.execute(
                        "INSERT OR IGNORE INTO article_tags (article_id, tag) VALUES (?, ?)",
                        (row[0], tag)
                    )
                    count += 1
        except (json.JSONDecodeError, TypeError):
            continue

        if count % 5000 == 0 and count > 0:
            conn.commit()
            print(f"  ... {count} тегов", flush=True)

    conn.commit()
    print(f"  Готово: {count} тегов добавлено")
    return count


def extract_ner(conn, batch_size=500):
    """Извлечь NER-сущности из текстов статей."""
    if not HAS_NATASHA:
        print("⚠ Natasha не установлена — NER пропущен")
        return 0

    print("\n═══ NER-извлечение сущностей ═══")

    # Инициализация Natasha
    segmenter = Segmenter()
    morph_vocab = MorphVocab()
    emb = NewsEmbedding()
    morph_tagger = NewsMorphTagger(emb)
    ner_tagger = NewsNERTagger(emb)
    names_extractor = NamesExtractor(morph_vocab)

    # Какие статьи уже обработаны
    processed_ids = set()
    rows = conn.execute("SELECT DISTINCT article_id FROM article_entities").fetchall()
    for r in rows:
        processed_ids.add(r[0])
    print(f"  Уже обработано: {len(processed_ids)} статей")

    # Получаем необработанные
    articles = conn.execute("""
        SELECT id, title, body_text FROM articles
        WHERE body_text IS NOT NULL AND body_text != ''
        AND id NOT IN (SELECT DISTINCT article_id FROM article_entities)
        ORDER BY id
    """).fetchall()

    total = len(articles)
    print(f"  Статей для обработки: {total}")

    if total == 0:
        return 0

    entity_cache = {}  # (normalized, type) -> entity_id
    total_entities = 0
    errors = 0
    filtered_out = 0

    for i, art in enumerate(articles):
        art_id, title, body = art[0], art[1] or "", art[2] or ""
        text = f"{title}. {body}"[:5000]  # ограничиваем для скорости

        try:
            doc = Doc(text)
            doc.segment(segmenter)
            doc.tag_morph(morph_tagger)
            doc.tag_ner(ner_tagger)

            # Нормализуем спаны
            for span in doc.spans:
                span.normalize(morph_vocab)

            # Для персон — дополнительно извлекаем имена
            for span in doc.spans:
                if span.type == "PER":
                    span.extract(names_extractor)

            # Считаем упоминания
            mentions = {}  # (clean_name, type) -> (count, display_name)
            for span in doc.spans:
                etype_map = {"PER": "person", "ORG": "org", "LOC": "location"}
                etype = etype_map.get(span.type)
                if not etype:
                    continue

                # Приоритет: span.normal (нормализованная форма Natasha)
                raw_name = span.normal or span.text
                if not raw_name:
                    continue

                # Очищаем и валидируем
                clean = clean_entity_name(raw_name, etype)
                if not clean:
                    filtered_out += 1
                    continue

                norm = normalize_name(clean)
                if not norm or len(norm) < 2:
                    filtered_out += 1
                    continue

                # Проверяем чёрный список по нормализованному
                if is_blacklisted(norm):
                    filtered_out += 1
                    continue

                key = (norm, etype)
                if key in mentions:
                    mentions[key] = (mentions[key][0] + 1, mentions[key][1])
                else:
                    mentions[key] = (1, clean)

            # Записываем в БД
            for (norm, etype), (count, display_name) in mentions.items():
                # Получаем или создаём entity
                cache_key = (norm, etype)
                if cache_key in entity_cache:
                    eid = entity_cache[cache_key]
                else:
                    # Пробуем найти
                    row = conn.execute(
                        "SELECT id FROM entities WHERE normalized = ? AND entity_type = ?",
                        (norm, etype)
                    ).fetchone()
                    if row:
                        eid = row[0]
                    else:
                        cursor = conn.execute(
                            "INSERT OR IGNORE INTO entities (name, entity_type, normalized) VALUES (?, ?, ?)",
                            (display_name, etype, norm)
                        )
                        if cursor.lastrowid:
                            eid = cursor.lastrowid
                        else:
                            row = conn.execute(
                                "SELECT id FROM entities WHERE normalized = ? AND entity_type = ?",
                                (norm, etype)
                            ).fetchone()
                            eid = row[0] if row else None

                    if eid:
                        entity_cache[cache_key] = eid

                if eid:
                    conn.execute(
                        "INSERT OR IGNORE INTO article_entities (article_id, entity_id, mention_count) VALUES (?, ?, ?)",
                        (art_id, eid, count)
                    )
                    total_entities += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  ⚠ Ошибка статьи {art_id}: {e}")

        # Прогресс
        if (i + 1) % batch_size == 0:
            conn.commit()
            pct = (i + 1) / total * 100
            print(f"  [{i+1}/{total}] {pct:.1f}% | сущностей: {total_entities} | уникальных: {len(entity_cache)} | отфильтровано: {filtered_out} | ошибок: {errors}", flush=True)

    conn.commit()
    print(f"\n  Готово: {total_entities} связей, {len(entity_cache)} уникальных сущностей, {filtered_out} отфильтровано, {errors} ошибок")
    return total_entities


def main():
    parser = argparse.ArgumentParser(description="NER-извлечение из статей Total.kz")
    parser.add_argument("--batch", type=int, default=500, help="Размер батча для коммитов")
    parser.add_argument("--tags-only", action="store_true", help="Только теги, без NER")
    parser.add_argument("--reprocess", action="store_true", help="Переобработать все (сбросить NER-данные)")
    args = parser.parse_args()

    init_db()

    if args.reprocess:
        print("⚠ Сброс NER-данных...")
        with get_db() as conn:
            conn.execute("DELETE FROM article_entities")
            conn.execute("DELETE FROM entities")
            print("  Удалено всё из entities и article_entities")

    # Логируем запуск
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_runs (started_at, phase, status) VALUES (?, 'ner', 'running')",
            (datetime.now().isoformat(),)
        )
        run_id = cursor.lastrowid

    try:
        with get_db() as conn:
            tag_count = extract_tags(conn)

            if not args.tags_only:
                ner_count = extract_ner(conn, batch_size=args.batch)
            else:
                ner_count = 0

        with get_db() as conn:
            conn.execute(
                "UPDATE scrape_runs SET finished_at=?, status='completed', articles_found=?, articles_downloaded=? WHERE id=?",
                (datetime.now().isoformat(), ner_count, tag_count, run_id)
            )

        print(f"\n{'='*60}")
        print(f"  ГОТОВО: {tag_count} тегов, {ner_count} NER-связей")
        print(f"{'='*60}")

    except Exception as e:
        with get_db() as conn:
            conn.execute(
                "UPDATE scrape_runs SET finished_at=?, status='failed', log=? WHERE id=?",
                (datetime.now().isoformat(), str(e), run_id)
            )
        raise


if __name__ == "__main__":
    main()
