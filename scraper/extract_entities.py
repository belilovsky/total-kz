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
"""
import json
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


def normalize_name(name: str) -> str:
    """Нормализация имени для дедупликации."""
    return " ".join(name.strip().split()).lower()


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

            # Считаем упоминания
            mentions = {}  # (normalized, type) -> count
            for span in doc.spans:
                etype_map = {"PER": "person", "ORG": "org", "LOC": "location"}
                etype = etype_map.get(span.type)
                if not etype:
                    continue

                name = span.normal or span.text
                if not name or len(name) < 2:
                    continue

                norm = normalize_name(name)
                if len(norm) < 2:
                    continue

                key = (norm, etype)
                mentions[key] = mentions.get(key, 0) + 1

            # Записываем в БД
            for (norm, etype), count in mentions.items():
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
                        # Создаём — используем оригинальное имя (с заглавных)
                        display_name = norm.title() if etype == "person" else norm.capitalize()
                        # Восстановим оригинальное написание из span
                        for span in doc.spans:
                            sn = span.normal or span.text
                            if normalize_name(sn) == norm:
                                display_name = sn
                                break
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
            print(f"  [{i+1}/{total}] {pct:.1f}% | сущностей: {total_entities} | уникальных: {len(entity_cache)} | ошибок: {errors}", flush=True)

    conn.commit()
    print(f"\n  Готово: {total_entities} связей, {len(entity_cache)} уникальных сущностей, {errors} ошибок")
    return total_entities


def main():
    parser = argparse.ArgumentParser(description="NER-извлечение из статей Total.kz")
    parser.add_argument("--batch", type=int, default=500, help="Размер батча для коммитов")
    parser.add_argument("--tags-only", action="store_true", help="Только теги, без NER")
    args = parser.parse_args()

    init_db()

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
