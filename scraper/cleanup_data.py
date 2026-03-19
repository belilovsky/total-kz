#!/usr/bin/env python3
"""
Очистка и нормализация данных total.kz.
Объединяет дубликаты тегов, персон, организаций и локаций.

Запуск:
    python scraper/cleanup_data.py --dry-run    # только показать что будет сделано
    python scraper/cleanup_data.py              # применить все исправления
"""
import sys
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from app.database import get_db


# ═══════════════════════════════════════════════════════
# 1. ТЕГИ: объединение дубликатов по регистру
# ═══════════════════════════════════════════════════════
TAG_MERGES = {
    # wrong → correct
    "казахстан": "Казахстан",
    "алматы": "Алматы",
    "дтп": "ДТП",
    "whatsapp": "WhatsApp",
    "facebook": "Facebook",
}


def fix_tags(conn, dry_run=False):
    """Объединить теги-дубликаты (разный регистр → один правильный)."""
    print("\n═══ 1. ТЕГИ: объединение дубликатов ═══")
    total_fixed = 0

    for wrong, correct in TAG_MERGES.items():
        # Сколько статей с неправильным тегом
        count = conn.execute(
            "SELECT COUNT(*) FROM article_tags WHERE tag = ?", (wrong,)
        ).fetchone()[0]

        if count == 0:
            print(f"  ✓ '{wrong}' → '{correct}': нечего исправлять")
            continue

        print(f"  {'[DRY]' if dry_run else '→'} '{wrong}' → '{correct}': {count} записей")

        if not dry_run:
            # Для каждой статьи с неправильным тегом:
            # если уже есть правильный тег — удаляем неправильный
            # если нет — переименовываем
            rows = conn.execute(
                "SELECT article_id FROM article_tags WHERE tag = ?", (wrong,)
            ).fetchall()

            for row in rows:
                article_id = row[0]
                has_correct = conn.execute(
                    "SELECT 1 FROM article_tags WHERE article_id = ? AND tag = ?",
                    (article_id, correct)
                ).fetchone()

                if has_correct:
                    conn.execute(
                        "DELETE FROM article_tags WHERE article_id = ? AND tag = ?",
                        (article_id, wrong)
                    )
                else:
                    conn.execute(
                        "UPDATE article_tags SET tag = ? WHERE article_id = ? AND tag = ?",
                        (correct, article_id, wrong)
                    )

            # Также обновляем JSON в articles.tags
            # (не критично, но для консистентности)

        total_fixed += count

    print(f"  Итого: {total_fixed} тегов {'будет исправлено' if dry_run else 'исправлено'}")
    return total_fixed


# ═══════════════════════════════════════════════════════
# 2. СУЩНОСТИ: маппинг дубликатов
# ═══════════════════════════════════════════════════════

# Формат: (entity_type, from_normalized, to_normalized, to_name)
# from будет объединён в to
ENTITY_MERGES = [
    # --- Персоны ---
    ("person", "токаев", "касым-жомарт токаев", "Касым-Жомарт Токаев"),
    ("person", "назарбаев", "нурсултан назарбаев", "Нурсултан Назарбаев"),
    ("person", "путин", "владимир путин", "Владимир Путин"),
    ("person", "трамп", "дональд трамп", "Дональд Трамп"),

    # --- Организации ---
    ("org", "иаtotal.kz", "иа total.kz", "ИА Total.kz"),
    ("org", "нацбанк", "национальный банк", "Национальный банк"),

    # --- Локации: аббревиатуры → полное название ---
    ("location", "рк", "казахстан", "Казахстан"),
    ("location", "республика казахстан", "казахстан", "Казахстан"),
    ("location", "рф", "россия", "Россия"),
    ("location", "российская федерация", "россия", "Россия"),
    ("location", "соединенные штаты", "сша", "США"),
    ("location", "америка", "сша", "США"),

    # --- Локации: Астана / Нур-Султан (оставляем Астана — текущее название) ---
    ("location", "нур-султан", "астана", "Астана"),
    ("location", "нур-султана", "астана", "Астана"),

    # --- Локации: области — формы с окончаниями → именительный падеж ---
    ("location", "северо-казахстанской", "северо-казахстанская область", "Северо-Казахстанская область"),
    ("location", "туркестанской", "туркестанская область", "Туркестанская область"),
    ("location", "восточно-казахстанской", "восточно-казахстанская область", "Восточно-Казахстанская область"),
    ("location", "западно-казахстанской", "западно-казахстанская область", "Западно-Казахстанская область"),
    ("location", "восточный казахстан", "восточно-казахстанская область", "Восточно-Казахстанская область"),
    ("location", "восточно-казахстанская", "восточно-казахстанская область", "Восточно-Казахстанская область"),
    ("location", "западно-казахстанская", "западно-казахстанская область", "Западно-Казахстанская область"),
]


def merge_entity(conn, entity_type, from_norm, to_norm, to_name, dry_run=False):
    """Объединить сущность from → to. Перенести все связи article_entities."""

    from_entity = conn.execute(
        "SELECT id, name FROM entities WHERE normalized = ? AND entity_type = ?",
        (from_norm, entity_type)
    ).fetchone()

    to_entity = conn.execute(
        "SELECT id, name FROM entities WHERE normalized = ? AND entity_type = ?",
        (to_norm, entity_type)
    ).fetchone()

    if not from_entity:
        print(f"  ✓ [{entity_type}] '{from_norm}': не найден, пропуск")
        return 0

    from_id = from_entity[0]
    from_name = from_entity[1]

    # Считаем связи
    link_count = conn.execute(
        "SELECT COUNT(*) FROM article_entities WHERE entity_id = ?", (from_id,)
    ).fetchone()[0]

    if link_count == 0:
        print(f"  ✓ [{entity_type}] '{from_name}': нет связей, удаляю")
        if not dry_run:
            conn.execute("DELETE FROM entities WHERE id = ?", (from_id,))
        return 0

    if not to_entity:
        # Целевая сущность не существует — просто переименовываем
        print(f"  {'[DRY]' if dry_run else '→'} [{entity_type}] '{from_name}' → '{to_name}' (переименование, {link_count} связей)")
        if not dry_run:
            conn.execute(
                "UPDATE entities SET name = ?, normalized = ? WHERE id = ?",
                (to_name, to_norm, from_id)
            )
        return link_count

    to_id = to_entity[0]

    print(f"  {'[DRY]' if dry_run else '→'} [{entity_type}] '{from_name}' → '{to_name}' (слияние, {link_count} связей)")

    if not dry_run:
        # Переносим связи: для каждой связи from → to
        links = conn.execute(
            "SELECT article_id, mention_count FROM article_entities WHERE entity_id = ?",
            (from_id,)
        ).fetchall()

        for article_id, mention_count in links:
            # Проверяем, есть ли уже связь с целевой сущностью
            existing = conn.execute(
                "SELECT mention_count FROM article_entities WHERE article_id = ? AND entity_id = ?",
                (article_id, to_id)
            ).fetchone()

            if existing:
                # Суммируем mention_count
                conn.execute(
                    "UPDATE article_entities SET mention_count = mention_count + ? WHERE article_id = ? AND entity_id = ?",
                    (mention_count, article_id, to_id)
                )
            else:
                # Создаём новую связь
                conn.execute(
                    "INSERT INTO article_entities (article_id, entity_id, mention_count) VALUES (?, ?, ?)",
                    (article_id, to_id, mention_count)
                )

        # Удаляем старые связи и сущность
        conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (from_id,))
        conn.execute("DELETE FROM entities WHERE id = ?", (from_id,))

    return link_count


def fix_entities(conn, dry_run=False):
    """Объединить все дубликаты сущностей."""
    print("\n═══ 2. СУЩНОСТИ: объединение дубликатов ═══")
    total_fixed = 0

    for entity_type, from_norm, to_norm, to_name in ENTITY_MERGES:
        fixed = merge_entity(conn, entity_type, from_norm, to_norm, to_name, dry_run)
        total_fixed += fixed

    print(f"\n  Итого: {total_fixed} связей {'будет перенесено' if dry_run else 'перенесено'}")
    return total_fixed


# ═══════════════════════════════════════════════════════
# 3. УДАЛЕНИЕ СИРОТ (сущности без статей)
# ═══════════════════════════════════════════════════════
def cleanup_orphans(conn, dry_run=False):
    """Удалить сущности без связей с статьями."""
    print("\n═══ 3. УДАЛЕНИЕ СИРОТ ═══")

    orphans = conn.execute("""
        SELECT e.id, e.name, e.entity_type FROM entities e
        LEFT JOIN article_entities ae ON ae.entity_id = e.id
        WHERE ae.article_id IS NULL
    """).fetchall()

    print(f"  {'[DRY]' if dry_run else '→'} Сущностей без статей: {len(orphans)}")

    if not dry_run and orphans:
        orphan_ids = [o[0] for o in orphans]
        # Удаляем пачками
        for i in range(0, len(orphan_ids), 500):
            batch = orphan_ids[i:i+500]
            placeholders = ",".join("?" * len(batch))
            conn.execute(f"DELETE FROM entities WHERE id IN ({placeholders})", batch)

    return len(orphans)


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Очистка данных total.kz")
    parser.add_argument("--dry-run", action="store_true", help="Только показать что будет сделано")
    args = parser.parse_args()

    print("=" * 60)
    print(f"  ОЧИСТКА ДАННЫХ total.kz {'(DRY RUN)' if args.dry_run else ''}")
    print("=" * 60)

    with get_db() as conn:
        # Статистика ДО
        tag_count = conn.execute("SELECT COUNT(DISTINCT tag) FROM article_tags").fetchone()[0]
        entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        print(f"\nДО: {tag_count} уникальных тегов, {entity_count} сущностей")

        fix_tags(conn, args.dry_run)
        fix_entities(conn, args.dry_run)
        cleanup_orphans(conn, args.dry_run)

        if not args.dry_run:
            # Статистика ПОСЛЕ
            tag_count2 = conn.execute("SELECT COUNT(DISTINCT tag) FROM article_tags").fetchone()[0]
            entity_count2 = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            print(f"\nПОСЛЕ: {tag_count2} уникальных тегов, {entity_count2} сущностей")
            print(f"Удалено: {tag_count - tag_count2} тегов, {entity_count - entity_count2} сущностей")

    print("\n" + "=" * 60)
    print(f"  {'DRY RUN завершён' if args.dry_run else 'ГОТОВО'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
