#!/usr/bin/env python3
"""
Очистка мусорных NER-сущностей в базе данных Total.kz.

Удаляет:
- Сущности из чёрного списка (Total.kz и т.д.)
- Сущности с мусорными слипшимися словами ("государстваКасым-Жомарт Токаевподписал")
- URL-ы и числа, ошибочно распознанные как сущности

Переименовывает:
- "делАрман Исетов" → "Арман Исетов" (мусорные префиксы)
- "наТАСС" → "ТАСС"
- "здравоохраненияАжар Гиният" → "Ажар Гиният"

Запуск:
    python scraper/cleanup_entities.py              # показать что будет удалено
    python scraper/cleanup_entities.py --apply       # применить очистку
    python scraper/cleanup_entities.py --apply --fix  # исправить + удалить
"""
import re
import sys
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.database import get_db, init_db

# Импортируем правила из основного скрипта
from scraper.extract_entities import (
    BLACKLIST_EXACT, BLACKLIST_PATTERNS, MIN_NAME_LENGTH,
    clean_entity_name, normalize_name, is_blacklisted,
)


def analyze_entities(conn):
    """Анализ мусорных сущностей."""
    entities = conn.execute("""
        SELECT e.id, e.name, e.entity_type, e.normalized,
               COUNT(ae.article_id) as article_count
        FROM entities e
        LEFT JOIN article_entities ae ON ae.entity_id = e.id
        GROUP BY e.id
        ORDER BY article_count DESC
    """).fetchall()

    to_delete = []       # (id, name, etype, art_count, reason)
    to_rename = []       # (id, old_name, new_name, new_normalized, etype, art_count)
    ok_count = 0

    for ent in entities:
        eid, name, etype, norm, art_count = ent[0], ent[1], ent[2], ent[3], ent[4]

        # 1. Проверяем чёрный список
        if is_blacklisted(norm):
            to_delete.append((eid, name, etype, art_count, "чёрный список"))
            continue

        # 2. Проверяем clean_entity_name – если возвращает None, удаляем
        cleaned = clean_entity_name(name, etype)
        if cleaned is None:
            to_delete.append((eid, name, etype, art_count, "не прошёл валидацию"))
            continue

        # 3. Если cleaned отличается от name – переименовываем
        if cleaned != name:
            new_norm = normalize_name(cleaned)
            to_rename.append((eid, name, cleaned, new_norm, etype, art_count))
            continue

        ok_count += 1

    return to_delete, to_rename, ok_count


def run_cleanup(apply=False, fix=False):
    """Запуск анализа и (опционально) очистки."""
    init_db()

    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        print(f"\n═══ Анализ сущностей ({total} всего) ═══\n")

        to_delete, to_rename, ok_count = analyze_entities(conn)

        # Отчёт: удаление
        if to_delete:
            print(f"🗑  К УДАЛЕНИЮ: {len(to_delete)} сущностей")
            print("-" * 70)
            for eid, name, etype, art_count, reason in to_delete[:30]:
                print(f"  [{etype:8s}] {name:40s} ({art_count:4d} ст.) – {reason}")
            if len(to_delete) > 30:
                print(f"  ... и ещё {len(to_delete) - 30}")
            print()

        # Отчёт: переименование
        if to_rename:
            print(f"✏️  К ПЕРЕИМЕНОВАНИЮ: {len(to_rename)} сущностей")
            print("-" * 70)
            for eid, old, new, new_norm, etype, art_count in to_rename[:30]:
                print(f"  [{etype:8s}] «{old}» → «{new}» ({art_count} ст.)")
            if len(to_rename) > 30:
                print(f"  ... и ещё {len(to_rename) - 30}")
            print()

        print(f"✓  Валидных: {ok_count}")
        print(f"   Итого: {ok_count} ок + {len(to_rename)} переим. + {len(to_delete)} удал. = {ok_count + len(to_rename) + len(to_delete)}")

        if not apply:
            print(f"\n⚠  Это предварительный просмотр. Для применения добавьте --apply")
            return

        # Применяем удаление
        deleted = 0
        for eid, name, etype, art_count, reason in to_delete:
            conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (eid,))
            conn.execute("DELETE FROM entities WHERE id = ?", (eid,))
            deleted += 1

        print(f"\n🗑  Удалено: {deleted} сущностей и их связей")

        # Применяем переименование
        if fix:
            renamed = 0
            merged = 0
            for eid, old, new, new_norm, etype, art_count in to_rename:
                # Проверяем – может entity с таким normalized уже есть
                existing = conn.execute(
                    "SELECT id FROM entities WHERE normalized = ? AND entity_type = ? AND id != ?",
                    (new_norm, etype, eid)
                ).fetchone()

                if existing:
                    # Мержим: переносим article_entities на существующую сущность
                    target_id = existing[0]
                    conn.execute("""
                        INSERT OR IGNORE INTO article_entities (article_id, entity_id, mention_count)
                        SELECT ae.article_id, ?, ae.mention_count
                        FROM article_entities ae
                        WHERE ae.entity_id = ?
                        AND ae.article_id NOT IN (
                            SELECT article_id FROM article_entities WHERE entity_id = ?
                        )
                    """, (target_id, eid, target_id))
                    # Удаляем старую сущность
                    conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (eid,))
                    conn.execute("DELETE FROM entities WHERE id = ?", (eid,))
                    merged += 1
                else:
                    # Просто переименовываем
                    conn.execute(
                        "UPDATE entities SET name = ?, normalized = ? WHERE id = ?",
                        (new, new_norm, eid)
                    )
                    renamed += 1

            print(f"✏️  Переименовано: {renamed}, объединено: {merged}")

        conn.commit()
        print(f"\n✓  Очистка завершена")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Очистка NER-сущностей")
    parser.add_argument("--apply", action="store_true", help="Применить изменения (по умолчанию – только просмотр)")
    parser.add_argument("--fix", action="store_true", help="Также переименовать кривые сущности (+ --apply)")
    args = parser.parse_args()

    run_cleanup(apply=args.apply, fix=args.fix)
