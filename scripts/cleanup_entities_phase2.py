#!/usr/bin/env python3
"""
Entity cleanup Phase 2:
1. Merge ambiguous surnames where the dominant fullname is >70%
2. Clean up broken NER (concatenated words like "Досаевв", "Ашимбаевтакже")
3. Merge morphological variants of top persons
"""
import sqlite3
import os
import sys
import re

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'total.db')


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def merge_entities(conn, source_id, target_id, source_name, target_name, dry_run=False):
    if source_id == target_id:
        return 0
    check = conn.execute("SELECT id FROM entities WHERE id = ?", (source_id,)).fetchone()
    if not check:
        return 0

    moved = conn.execute("""
        SELECT COUNT(*) FROM article_entities 
        WHERE entity_id = ? 
        AND article_id NOT IN (SELECT article_id FROM article_entities WHERE entity_id = ?)
    """, (source_id, target_id)).fetchone()[0]

    if dry_run:
        total = conn.execute("SELECT COUNT(*) FROM article_entities WHERE entity_id=?", (source_id,)).fetchone()[0]
        print(f"  MERGE: [{source_id}] '{source_name}' ({total}) → [{target_id}] '{target_name}' (+{moved})")
        return moved

    # Update mention counts for overlapping
    conn.execute("""
        UPDATE article_entities 
        SET mention_count = mention_count + COALESCE(
            (SELECT mention_count FROM article_entities ae2 
             WHERE ae2.entity_id = ? AND ae2.article_id = article_entities.article_id), 0
        )
        WHERE entity_id = ? 
        AND article_id IN (SELECT article_id FROM article_entities WHERE entity_id = ?)
    """, (source_id, target_id, source_id))

    # Move non-overlapping
    conn.execute("""
        UPDATE article_entities SET entity_id = ? 
        WHERE entity_id = ? 
        AND article_id NOT IN (SELECT article_id FROM article_entities WHERE entity_id = ?)
    """, (target_id, source_id, target_id))

    # Delete remaining source links
    conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (source_id,))
    conn.execute("DELETE FROM entities WHERE id = ?", (source_id,))

    print(f"  ✓ MERGED: [{source_id}] '{source_name}' → [{target_id}] '{target_name}' (+{moved})")
    return moved


def delete_entity(conn, entity_id, name, dry_run=False):
    arts = conn.execute("SELECT COUNT(*) FROM article_entities WHERE entity_id=?", (entity_id,)).fetchone()[0]
    if dry_run:
        print(f"  DELETE: [{entity_id}] '{name}' ({arts} links)")
        return
    conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (entity_id,))
    conn.execute("DELETE FROM entities WHERE id = ?", (entity_id,))
    print(f"  ✓ DELETED: [{entity_id}] '{name}' ({arts} links)")


def run_phase2(dry_run=True):
    conn = get_conn()
    stats = {'merged': 0, 'deleted': 0}

    print(f"\n{'='*60}")
    print(f"PHASE 2 CLEANUP {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
    print(f"{'='*60}")

    # ── 1. Ambiguous surname merges (manually verified) ──────────────
    print(f"\n── 1. Merge ambiguous surnames (verified) ──")
    
    # These are ambiguous but the dominant person is obvious from context
    AMBIGUOUS_MERGES = [
        # (surname, target_fullname) – verified by article count dominance
        ('Сапаров', 'Айдарбек Сапаров'),
        ('Ашимбаев', 'Маулен Ашимбаев'),
        ('Карабаев', 'Марат Карабаев'),
        ('Досаев', 'Ерболат Досаев'),
        ('Байден', 'Джо Байден'),
        ('Нурбек', 'Саясат Нурбек'),
        ('Бейсембаев', 'Гани Бейсембаев'),
        ('Жакупова', 'Светлана Жакупова'),
        ('Нагаспаев', 'Ерсайын Нагаспаев'),
        ('Аймагамбетов', 'Асхат Аймагамбетов'),
        ('Сауранбаев', 'Нурлан Сауранбаев'),
        ('Адилов', 'Санжар Адилов'),
        ('Аринов', 'Чингис Аринов'),
        ('Баркулов', 'Марат Баркулов'),
        ('Пономарев', 'Сергей Пономарев'),
        ('Сатыбалды', 'Дархан Сатыбалды'),
        ('Сарсембаев', 'Ерлан Сарсембаев'),
        ('Койгельдиев', 'Галымжан Койгельдиев'),
        ('Мбаппе', 'Килиан Мбаппе'),
        ('Байден', 'Джо Байден'),
        ('Мадиев', 'Жаслан Мадиев'),
        ('Бозумбаев', 'Канат Бозумбаев'),
        ('Базарбек', 'Бакытжан Базарбек'),
        ('Байбазаров', 'Нурлан Байбазаров'),
        ('Бишимбаев', 'Куандык Бишимбаев'),
        ('Шарлапаев', 'Канат Шарлапаев'),
        ('Султанов', 'Бахытжан Султанов'),  # if exists
        ('Байжанов', 'Ербол Байжанов'),  # if exists
        ('Лукин', 'Андрей Лукин'),
    ]

    for surname, fullname in AMBIGUOUS_MERGES:
        source = conn.execute(
            "SELECT id FROM entities WHERE name=? AND entity_type='person'", (surname,)
        ).fetchone()
        target = conn.execute(
            "SELECT id FROM entities WHERE name=? AND entity_type='person'", (fullname,)
        ).fetchone()
        if source and target:
            merge_entities(conn, source[0], target[0], surname, fullname, dry_run)
            stats['merged'] += 1

    if not dry_run:
        conn.commit()

    # ── 2. Merge Сулейменов variants ─────────────────────────────────
    # Сулейменов is truly ambiguous (Тимур and others), so we DON'T merge the bare surname
    # But we DO merge the female variants
    print(f"\n── 2. Merge female/morphological variants ──")
    
    FEMALE_MORPH_MERGES = [
        # Сулейменова variants → Жулдыз Сулейменова (dominant female)
        ('Сулейменова', 'Жулдыз Сулейменова'),
        ('Жулдыз Сулейменовой', 'Жулдыз Сулейменова'),
        ('Жулдыз Сулейменову', 'Жулдыз Сулейменова'),
        ('Сулейменов Жулдыз Досбергеновна', 'Жулдыз Сулейменова'),
        # Досаев variants
        ('Досаева', 'Ерболат Досаев'),
        ('Ерболата Досаева', 'Ерболат Досаев'),
        ('Ерболату Досаеву', 'Ерболат Досаев'),
        ('Досаеву', 'Ерболат Досаев'),
        ('Досаев Ерболат Аскарбекович', 'Ерболат Досаев'),
        # Сапаров variants
        ('Айдарбек Сапарова', 'Айдарбек Сапаров'),
        # Ашимбаев variants
        ('Маулен Ашимбаева', 'Маулен Ашимбаев'),
        # Карабаев variants
        ('Марат Карабаевна', 'Марат Карабаев'),
        # Мбаппе variants
        ('Килиана Мбаппе', 'Килиан Мбаппе'),
        ('Килиану Мбаппе', 'Килиан Мбаппе'),
        ('Килиане Мбаппе', 'Килиан Мбаппе'),
        # Сатыбалды variants
        ('Дархана Сатыбалды', 'Дархан Сатыбалды'),
        # Нурбек variants
        ('Саясата Нурбек', 'Саясат Нурбек'),
        # Нукенова
        ('Салтанат Нукеновой', 'Салтанат Нукенова'),
        # Баркулов
        ('Марата Баркулов', 'Марат Баркулов'),
        # Адилов
        ('МВДСанжар Адилов', 'Санжар Адилов'),
        # Байден
        ('Джозеф Байден', 'Джо Байден'),
        # Галымжайн vs Галымжан (typo)
        ('Галымжайн Койгельдиев', 'Галымжан Койгельдиев'),
        # Ерсаин vs Ерсайын (spelling variant)
        ('Ерсаин Нагаспаев', 'Ерсайын Нагаспаев'),
    ]

    for source_name, target_name in FEMALE_MORPH_MERGES:
        source = conn.execute(
            "SELECT id FROM entities WHERE name=? AND entity_type='person'", (source_name,)
        ).fetchone()
        target = conn.execute(
            "SELECT id FROM entities WHERE name=? AND entity_type='person'", (target_name,)
        ).fetchone()
        if source and target:
            merge_entities(conn, source[0], target[0], source_name, target_name, dry_run)
            stats['merged'] += 1

    if not dry_run:
        conn.commit()

    # ── 3. Clean broken NER (concatenated text) ──────────────────────
    print(f"\n── 3. Delete broken NER entries (concatenated text) ──")
    
    # Find entities whose name contains a known person name concatenated with other text
    broken = conn.execute("""
        SELECT id, name FROM entities 
        WHERE entity_type = 'person'
        AND (
            name LIKE '%проинформировал%'
            OR name LIKE '%заявлял%'
            OR name LIKE '%также%'
            OR name LIKE '%торговля%'
            OR name LIKE '%реформа%'
            OR name LIKE '%журналисты%'
            OR name LIKE '%освобожден%'
            OR name LIKE '%ознакомил%'
            OR name LIKE '%жалобы%'
            OR name LIKE '%усилен%'
            OR name LIKE '%связывались%'
            OR name LIKE '%Досаевв%'
            OR name LIKE '%Досаевво%'
            OR name LIKE '%Досаеван%'
            OR name LIKE '%Байденупо%'
            OR name LIKE '%Ашимбаевтакже%'
            OR name LIKE 'СШАДжо%'
            OR name LIKE 'Аким Алматы%журнал%'
            OR name LIKE '%Байденаза%'
            OR name LIKE '%Досаевозн%'
            OR name LIKE '%Досаева%силен%'
            OR name LIKE '%Областимырз%'
            OR name LIKE '%Областиданиал%'
            OR name LIKE 'Байжанов Ерлан Сапарович%'
            OR name LIKE 'РК%' AND LENGTH(name) > 15
            OR name LIKE 'Шымкент%' AND LENGTH(name) > 15
            OR name LIKE 'Жетісу С.%'
            OR name LIKE 'Костаная %'
            OR name LIKE 'Баркулов%Мангис%'
            OR name LIKE 'Дмитрий Бивол Екатерина'
            OR name LIKE 'Замену Хави'
            OR name LIKE 'Аким Семей'
            OR name LIKE 'Замакима%'
            OR name LIKE '%Бишимбаевалейла%'
            OR name LIKE 'Мухин Александр'
            OR name LIKE 'Халметова Диана'
            OR name LIKE 'Абдикаримов Дархан'
            OR name LIKE 'Есендикова Айнура'
            OR name LIKE 'Баймолда Бейбит'
            OR (name LIKE 'Ж.Сулейменов')
            OR (name LIKE 'Сулейменовым Д.С')
            OR (name LIKE '%.%' AND LENGTH(name) < 15 AND entity_type='person')
        )
    """).fetchall()

    for eid, name in broken:
        delete_entity(conn, eid, name, dry_run)
        stats['deleted'] += 1

    if not dry_run:
        conn.commit()

    # ── 4. Delete various garbage entities ───────────────────────────
    print(f"\n── 4. Delete remaining garbage ──")
    
    GARBAGE_NAMES = [
        'Экс-аким Алматы Досаев', 'Фейковый Досаев',
        'Нейросеть Илона Маска', 'Псевдосотрудник Антикора',
        'Аким Дархан Сатыбалды', 'Дархан Амангельдиевич Сатыбалды',
        'Мажилисмен Азат Перуашев',
        'Шымкента Галымжан Шарипов',
    ]

    for name in GARBAGE_NAMES:
        row = conn.execute("SELECT id FROM entities WHERE name=?", (name,)).fetchone()
        if row:
            delete_entity(conn, row[0], name, dry_run)
            stats['deleted'] += 1

    if not dry_run:
        conn.commit()

    # ── 5. Clean orphans ─────────────────────────────────────────────
    print(f"\n── 5. Orphan cleanup ──")
    orphans = conn.execute("""
        SELECT COUNT(*) FROM entities 
        WHERE NOT EXISTS (SELECT 1 FROM article_entities WHERE entity_id = entities.id)
    """).fetchone()[0]
    
    if not dry_run and orphans > 0:
        conn.execute("""
            DELETE FROM entities WHERE id NOT IN (SELECT DISTINCT entity_id FROM article_entities)
        """)
        conn.commit()
    
    print(f"  {'Would delete' if dry_run else '✓ Deleted'} {orphans} orphan entities")
    stats['deleted'] += orphans

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"PHASE 2 SUMMARY {'(DRY RUN)' if dry_run else '(COMPLETED)'}")
    print(f"{'='*60}")
    print(f"  Merged: {stats['merged']}")
    print(f"  Deleted: {stats['deleted']}")
    
    remaining = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    single = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person' AND name NOT LIKE '% %'").fetchone()[0]
    print(f"  Total entities: {remaining}")
    print(f"  Single-word persons: {single}")
    
    conn.close()


if __name__ == '__main__':
    dry = '--execute' not in sys.argv
    if dry:
        print("DRY RUN mode. Use --execute to apply.")
    run_phase2(dry_run=dry)
