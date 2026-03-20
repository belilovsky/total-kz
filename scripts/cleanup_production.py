#!/usr/bin/env python3
"""
Production entity cleanup for total.kz
Works on the full DB (312K entities, 185K articles, 1.5M links).
Run on server: python scripts/cleanup_production.py --execute

Handles ALL known problems:
1. Surname → full name merges (auto + manual)
2. Morphological variants (Куандыка → Куандык)
3. Not-a-person: Акорда, Эксклюзив, Казгидромет, Елбасы, etc.
4. Authors masquerading as entities (Азамат Галеев, Ирина Ярунина, etc.)
5. Broken NER: "РККуандык", "Досаевв", initials "А.Б."
6. Org/location misclassified as person
"""
import sqlite3
import os
import sys
import shutil
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'total.db')
BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'backups')


def backup_db():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = os.path.join(BACKUP_DIR, f'total_{ts}.db')
    print(f"Creating backup... ", end='', flush=True)
    shutil.copy2(DB_PATH, backup_path)
    size_mb = os.path.getsize(backup_path) / 1024 / 1024
    print(f"✓ {backup_path} ({size_mb:.0f} MB)")
    return backup_path


def merge(conn, source_id, target_id, dry_run=False):
    """Merge source → target: move links, delete source."""
    if source_id == target_id:
        return 0
    check = conn.execute("SELECT id FROM entities WHERE id=?", (source_id,)).fetchone()
    if not check:
        return 0

    moved = conn.execute("""
        SELECT COUNT(*) FROM article_entities 
        WHERE entity_id=? AND article_id NOT IN 
        (SELECT article_id FROM article_entities WHERE entity_id=?)
    """, (source_id, target_id)).fetchone()[0]

    if dry_run:
        return moved

    # Boost mention_count for overlapping articles
    conn.execute("""
        UPDATE article_entities SET mention_count = mention_count + COALESCE(
            (SELECT mention_count FROM article_entities ae2 
             WHERE ae2.entity_id=? AND ae2.article_id=article_entities.article_id), 0)
        WHERE entity_id=? AND article_id IN 
        (SELECT article_id FROM article_entities WHERE entity_id=?)
    """, (source_id, target_id, source_id))

    # Move non-overlapping
    conn.execute("""
        UPDATE article_entities SET entity_id=? 
        WHERE entity_id=? AND article_id NOT IN 
        (SELECT article_id FROM article_entities WHERE entity_id=?)
    """, (target_id, source_id, target_id))

    # Delete source links + entity
    conn.execute("DELETE FROM article_entities WHERE entity_id=?", (source_id,))
    conn.execute("DELETE FROM entities WHERE id=?", (source_id,))
    return moved


def delete_entity(conn, entity_id, dry_run=False):
    if not conn.execute("SELECT id FROM entities WHERE id=?", (entity_id,)).fetchone():
        return
    if not dry_run:
        conn.execute("DELETE FROM article_entities WHERE entity_id=?", (entity_id,))
        conn.execute("DELETE FROM entities WHERE id=?", (entity_id,))


def find(conn, name, etype='person'):
    return conn.execute(
        "SELECT id FROM entities WHERE name=? AND entity_type=?", (name, etype)
    ).fetchone()


def run(dry_run=True):
    if not dry_run:
        backup_db()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-100000")  # 100MB cache for big DB

    stats = {'merged': 0, 'deleted': 0, 'reclassified': 0, 'articles_moved': 0}

    # ── Get initial counts ────────────────────────────────────────────
    total_before = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    persons_before = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person'").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"PRODUCTION CLEANUP {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
    print(f"{'='*60}")
    print(f"Entities: {total_before}, Persons: {persons_before}")

    # ══════════════════════════════════════════════════════════════════
    # STEP 1: DELETE GARBAGE (not real entities)
    # ══════════════════════════════════════════════════════════════════
    print(f"\n── Step 1: Delete garbage entities ──")

    # 1a. Known garbage names
    GARBAGE_NAMES = [
        'Эксклюзив', 'Елбасы', 'ЗРАБатр',
    ]
    for name in GARBAGE_NAMES:
        row = find(conn, name, 'person')
        if row:
            delete_entity(conn, row[0], dry_run)
            stats['deleted'] += 1

    # 1b. Authors/journalists → delete (they're not news entities)
    AUTHORS = [
        'Азамат Галеев', 'Ирина Ярунина', 'Алексей Байтеев',
    ]
    for name in AUTHORS:
        row = find(conn, name, 'person')
        if row:
            delete_entity(conn, row[0], dry_run)
            stats['deleted'] += 1

    # 1c. Broken NER patterns (bulk delete)
    broken_count = conn.execute("""
        SELECT COUNT(*) FROM entities WHERE entity_type='person' AND (
            name LIKE 'РК%' AND LENGTH(name) > 15
            OR name LIKE '%проинформировал%'
            OR name LIKE '%заявлял%'
            OR name LIKE '%также%' AND LENGTH(name) > 20
            OR name LIKE '%освобожден%'
            OR name LIKE '%ознакомил%'
            OR name LIKE '%реформа%' AND LENGTH(name) > 20
            OR name LIKE '%журналист%' AND LENGTH(name) > 25
            OR name LIKE '%усилен%' AND LENGTH(name) > 15
            OR name LIKE '%связывались%'
            OR name LIKE '%жалобы%' AND LENGTH(name) > 15
            OR name LIKE 'Экс-Министр%' AND LENGTH(name) > 20
            OR name LIKE 'Премьер-Министр%' AND LENGTH(name) > 25
            OR name LIKE '%Област%' AND name LIKE '%\u043c%' AND LENGTH(name) > 30
            OR name LIKE '%всегда%' AND LENGTH(name) > 20
            OR name LIKE '%условиявозвращ%'
            OR name LIKE 'СШАДжо%'
            OR name LIKE '%Досаевв%' OR name LIKE '%Досаевво%' OR name LIKE '%Досаеван%'
            OR name LIKE '%Байденупо%' OR name LIKE '%Байденаза%'
            OR name LIKE '%Ашимбаевтакже%'
            OR name LIKE '%Бишимбаевалейла%'
            OR name LIKE 'Племянник %' OR name LIKE 'Внук %'
            OR name LIKE 'Псевдосотрудник %' OR name LIKE 'Фейковый %'
            OR name LIKE '%Назарбаеваи' OR name LIKE '%Назарбаевых' OR name LIKE '%Назарбаевыми'
        )
    """).fetchone()[0]
    
    if not dry_run:
        conn.execute("""
            DELETE FROM article_entities WHERE entity_id IN (
                SELECT id FROM entities WHERE entity_type='person' AND (
                    name LIKE 'РК%' AND LENGTH(name) > 15
                    OR name LIKE '%проинформировал%'
                    OR name LIKE '%заявлял%'
                    OR name LIKE '%также%' AND LENGTH(name) > 20
                    OR name LIKE '%освобожден%'
                    OR name LIKE '%ознакомил%'
                    OR name LIKE '%реформа%' AND LENGTH(name) > 20
                    OR name LIKE '%журналист%' AND LENGTH(name) > 25
                    OR name LIKE '%усилен%' AND LENGTH(name) > 15
                    OR name LIKE '%связывались%'
                    OR name LIKE '%жалобы%' AND LENGTH(name) > 15
                    OR name LIKE 'Экс-Министр%' AND LENGTH(name) > 20
                    OR name LIKE 'Премьер-Министр%' AND LENGTH(name) > 25
                    OR name LIKE '%Област%' AND name LIKE '%\u043c%' AND LENGTH(name) > 30
                    OR name LIKE '%всегда%' AND LENGTH(name) > 20
                    OR name LIKE '%условиявозвращ%'
                    OR name LIKE 'СШАДжо%'
                    OR name LIKE '%Досаевв%' OR name LIKE '%Досаевво%' OR name LIKE '%Досаеван%'
                    OR name LIKE '%Байденупо%' OR name LIKE '%Байденаза%'
                    OR name LIKE '%Ашимбаевтакже%'
                    OR name LIKE '%Бишимбаевалейла%'
                    OR name LIKE 'Племянник %' OR name LIKE 'Внук %'
                    OR name LIKE 'Псевдосотрудник %' OR name LIKE 'Фейковый %'
                    OR name LIKE '%Назарбаеваи' OR name LIKE '%Назарбаевых' OR name LIKE '%Назарбаевыми'
                )
            )
        """)
        conn.execute("""
            DELETE FROM entities WHERE entity_type='person' AND (
                name LIKE 'РК%' AND LENGTH(name) > 15
                OR name LIKE '%проинформировал%'
                OR name LIKE '%заявлял%'
                OR name LIKE '%также%' AND LENGTH(name) > 20
                OR name LIKE '%освобожден%'
                OR name LIKE '%ознакомил%'
                OR name LIKE '%реформа%' AND LENGTH(name) > 20
                OR name LIKE '%журналист%' AND LENGTH(name) > 25
                OR name LIKE '%усилен%' AND LENGTH(name) > 15
                OR name LIKE '%связывались%'
                OR name LIKE '%жалобы%' AND LENGTH(name) > 15
                OR name LIKE 'Экс-Министр%' AND LENGTH(name) > 20
                OR name LIKE 'Премьер-Министр%' AND LENGTH(name) > 25
                OR name LIKE '%Област%' AND name LIKE '%\u043c%' AND LENGTH(name) > 30
                OR name LIKE '%всегда%' AND LENGTH(name) > 20
                OR name LIKE '%условиявозвращ%'
                OR name LIKE 'СШАДжо%'
                OR name LIKE '%Досаевв%' OR name LIKE '%Досаевво%' OR name LIKE '%Досаеван%'
                OR name LIKE '%Байденупо%' OR name LIKE '%Байденаза%'
                OR name LIKE '%Ашимбаевтакже%'
                OR name LIKE '%Бишимбаевалейла%'
                OR name LIKE 'Племянник %' OR name LIKE 'Внук %'
                OR name LIKE 'Псевдосотрудник %' OR name LIKE 'Фейковый %'
                OR name LIKE '%Назарбаеваи' OR name LIKE '%Назарбаевых' OR name LIKE '%Назарбаевыми'
            )
        """)
    print(f"  Broken NER patterns: {broken_count}")
    stats['deleted'] += broken_count

    # 1d. Initials-style entities ("А.Б.", "К.Н.", "Н. Назарбаев", etc.)
    initials_count = conn.execute("""
        SELECT COUNT(*) FROM entities WHERE entity_type='person' AND (
            (name GLOB '*[А-Я].[А-Я].' AND LENGTH(name) < 8)
            OR (name GLOB '*[А-Я].[А-Я].[А-Я].' AND LENGTH(name) < 10)
            OR (name GLOB '[А-Я]. [А-Я].' AND LENGTH(name) < 8)
            OR (name GLOB '[А-Я].[А-Я]' AND LENGTH(name) < 5)
            OR (name GLOB '[А-Я]. *' AND LENGTH(name) < 20 AND name NOT LIKE '% %а%')
            OR (name GLOB '*.*)' AND LENGTH(name) < 6)
            OR name = 'Ф.И.О.'
            OR (LENGTH(name) <= 3 AND name GLOB '[А-Я].*')
        )
    """).fetchone()[0]
    
    if not dry_run:
        conn.execute("""
            DELETE FROM article_entities WHERE entity_id IN (
                SELECT id FROM entities WHERE entity_type='person' AND (
                    (name GLOB '*[А-Я].[А-Я].' AND LENGTH(name) < 8)
                    OR (name GLOB '*[А-Я].[А-Я].[А-Я].' AND LENGTH(name) < 10)
                    OR (name GLOB '[А-Я]. [А-Я].' AND LENGTH(name) < 8)
                    OR (name GLOB '[А-Я].[А-Я]' AND LENGTH(name) < 5)
                    OR (name GLOB '[А-Я]. *' AND LENGTH(name) < 20 AND name NOT LIKE '% %а%')
                    OR (name GLOB '*.*)' AND LENGTH(name) < 6)
                    OR name = 'Ф.И.О.'
                    OR (LENGTH(name) <= 3 AND name GLOB '[А-Я].*')
                )
            )
        """)
        conn.execute("""
            DELETE FROM entities WHERE entity_type='person' AND (
                (name GLOB '*[А-Я].[А-Я].' AND LENGTH(name) < 8)
                OR (name GLOB '*[А-Я].[А-Я].[А-Я].' AND LENGTH(name) < 10)
                OR (name GLOB '[А-Я]. [А-Я].' AND LENGTH(name) < 8)
                OR (name GLOB '[А-Я].[А-Я]' AND LENGTH(name) < 5)
                OR (name GLOB '[А-Я]. *' AND LENGTH(name) < 20 AND name NOT LIKE '% %а%')
                OR (name GLOB '*.*)' AND LENGTH(name) < 6)
                OR name = 'Ф.И.О.'
                OR (LENGTH(name) <= 3 AND name GLOB '[А-Я].*')
            )
        """)
    print(f"  Initials-style entities: {initials_count}")
    stats['deleted'] += initials_count

    if not dry_run:
        conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # STEP 2: RECLASSIFY (person → org/location)
    # ══════════════════════════════════════════════════════════════════
    print(f"\n── Step 2: Reclassify mistyped entities ──")

    TO_ORG = [
        'Акорда', 'Акорды', 'Антикора', 'Нацфонд', 'Казгидромет', 'Казком',
        'Миннацэкономики', 'Минторговли', 'Европульс', 'Минцифры', 'Госаудит',
        'Миннауки', 'Минпром', 'Зампремьера', 'Акиматы', 'Минобороны',
    ]
    TO_LOCATION = [
        'Нур-Султан', 'Улытау', 'Мангистау', 'Жетісу', 'Аксу', 'Семей',
        'Туркестанской', 'Улытауской', 'Жетысуской', 'Абайской',
        'Сырдарьинской', 'Джизакской', 'Тобол-Торгайской',
        'Акмолинская', 'Актюбинская', 'Алматинская', 'Жамбылская',
        'Костанайская', 'Кызылорды', 'Талдыкорган', 'Кульсары',
    ]

    for name in TO_ORG:
        row = find(conn, name, 'person')
        if row and not dry_run:
            conn.execute("UPDATE entities SET entity_type='org' WHERE id=?", (row[0],))
            stats['reclassified'] += 1
        elif row:
            stats['reclassified'] += 1

    for name in TO_LOCATION:
        row = find(conn, name, 'person')
        if row and not dry_run:
            conn.execute("UPDATE entities SET entity_type='location' WHERE id=?", (row[0],))
            stats['reclassified'] += 1
        elif row:
            stats['reclassified'] += 1

    if not dry_run:
        conn.commit()
    print(f"  Reclassified: {stats['reclassified']}")

    # ══════════════════════════════════════════════════════════════════
    # STEP 3: MERGE MORPHOLOGICAL VARIANTS
    # ══════════════════════════════════════════════════════════════════
    print(f"\n── Step 3: Merge morphological variants ──")

    MORPH_MERGES = [
        ('Куандыка Бишимбаев', 'Куандык Бишимбаев'),
        ('Куандыком Бишимбаев', 'Куандык Бишимбаев'),
        ('Каната Бозумбаев', 'Канат Бозумбаев'),
        ('Илона Маска', 'Илон Маск'),
        ('Экс-министр Бишимбаев', 'Куандык Бишимбаев'),
        ('Зампремьера Бозумбаев', 'Канат Бозумбаев'),
        ('Мажилисмен Бакытжан Базарбек', 'Бакытжан Базарбек'),
        ('Бахытжан Базарбек', 'Бакытжан Базарбек'),
        ('Жаслана Мадиев', 'Жаслан Мадиев'),
        ('Нукеновой', 'Салтанат Нукенова'),
        ('Салтанат Нукеновой', 'Салтанат Нукенова'),
        ('Дархана Сатыбалды', 'Дархан Сатыбалды'),
        ('Ерболата Досаева', 'Ерболат Досаев'),
        ('Ерболату Досаеву', 'Ерболат Досаев'),
        ('Маулен Ашимбаева', 'Маулен Ашимбаев'),
        ('Джозеф Байден', 'Джо Байден'),
        ('Килиана Мбаппе', 'Килиан Мбаппе'),
        ('Килиану Мбаппе', 'Килиан Мбаппе'),
        ('Килиане Мбаппе', 'Килиан Мбаппе'),
        ('Н. Назарбаев', 'Нурсултан Назарбаев'),
        ('Нурсултан Абишевич Назарбаев', 'Нурсултан Назарбаев'),
        ('Маска', 'Илон Маск'),
        ('Канат Бисембаевич Шарлапаев', 'Канат Шарлапаев'),
    ]

    for src_name, tgt_name in MORPH_MERGES:
        src = find(conn, src_name, 'person')
        tgt = find(conn, tgt_name, 'person')
        if src and tgt:
            moved = merge(conn, src[0], tgt[0], dry_run)
            stats['merged'] += 1
            stats['articles_moved'] += moved

    if not dry_run:
        conn.commit()
    print(f"  Morphological merges: {stats['merged']}")

    # ══════════════════════════════════════════════════════════════════
    # STEP 4: AUTO-MERGE SURNAMES → FULL NAMES
    # ══════════════════════════════════════════════════════════════════
    print(f"\n── Step 4: Auto-merge surnames → full names ──")

    SKIP = {
        'Антикора', 'Нацфонд', 'Улытау', 'Мангистау', 'Миннацэкономики',
        'Минторговли', 'Европульс', 'Акорды', 'Акорда', 'Казгидромет',
        'Казком', 'Нур-Султан', 'Жетісу', 'Аксу', 'Семей', 'Семея',
        'Кашаган', 'Диана', 'Александр', 'Екатерина', 'Майкл', 'Кайрат',
        'Карина', 'Шерзат', 'Шерзата', 'Гарри', 'Тайсон', 'Хави', 'Елбасы',
        'Эксклюзив', 'Наурыз', 'Салтанат', 'Наталья', 'Елена',
    }

    candidates = conn.execute("""
        SELECT s.id, s.name,
               (SELECT COUNT(*) FROM article_entities WHERE entity_id=s.id) as s_arts
        FROM entities s
        WHERE s.entity_type='person' AND s.name NOT LIKE '% %'
          AND LENGTH(s.name) > 3
        ORDER BY s_arts DESC
    """).fetchall()

    merge_count = 0
    for sid, surname, s_arts in candidates:
        if not conn.execute("SELECT id FROM entities WHERE id=?", (sid,)).fetchone():
            continue
        if surname in SKIP:
            continue

        matches = conn.execute("""
            SELECT id, name,
                   (SELECT COUNT(*) FROM article_entities WHERE entity_id=id) as arts
            FROM entities
            WHERE entity_type='person'
              AND LOWER(name) LIKE '% ' || LOWER(?)
              AND name LIKE '% %'
              AND name NOT LIKE '% % % %'
              AND name NOT LIKE 'Экс-%'
              AND name NOT LIKE 'Зампремьер%'
              AND name NOT LIKE 'Мажилисмен%'
              AND name NOT LIKE 'РК%'
              AND name NOT LIKE 'Псевдо%'
              AND name NOT LIKE 'Фейков%'
              AND name NOT LIKE 'Аким %'
              AND name NOT LIKE 'Замаким%'
              AND name NOT LIKE 'Маслихат%'
              AND name NOT LIKE 'Месторожд%'
              AND name NOT LIKE 'Нейросеть%'
              AND name NOT LIKE 'Брат %'
              AND name NOT LIKE 'Племянник%'
              AND name NOT LIKE 'Внук%'
              AND id != ?
        """, (surname, sid)).fetchall()

        if len(matches) == 1:
            tid, tname, t_arts = matches[0]
            moved = merge(conn, sid, tid, dry_run)
            stats['merged'] += 1
            stats['articles_moved'] += moved
            merge_count += 1

    if not dry_run:
        conn.commit()
    print(f"  Unambiguous surname merges: {merge_count}")

    # ══════════════════════════════════════════════════════════════════
    # STEP 5: VERIFIED AMBIGUOUS MERGES
    # ══════════════════════════════════════════════════════════════════
    print(f"\n── Step 5: Verified ambiguous merges ──")

    AMBIGUOUS = [
        ('Ахметов', 'Серик Ахметов'),
        ('Аблязов', 'Мухтар Аблязов'),
        ('Головкин', 'Геннадий Головкин'),
        ('Порошенко', 'Петр Порошенко'),
        ('Челах', 'Владислав Челах'),
        ('Обама', 'Барак Обама'),
        ('Досаев', 'Ерболат Досаев'),
        ('Бишимбаев', 'Куандык Бишимбаев'),
        ('Шарлапаев', 'Канат Шарлапаев'),
        ('Сапаров', 'Айдарбек Сапаров'),
        ('Ашимбаев', 'Маулен Ашимбаев'),
        ('Карабаев', 'Марат Карабаев'),
        ('Байден', 'Джо Байден'),
        ('Базарбек', 'Бакытжан Базарбек'),
        ('Бозумбаев', 'Канат Бозумбаев'),
        ('Мадиев', 'Жаслан Мадиев'),
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
        ('Лукин', 'Андрей Лукин'),
        ('Коскина', None),  # Check who she merges to — she's a journalist
    ]

    amb_count = 0
    for surname, fullname in AMBIGUOUS:
        if fullname is None:
            continue
        src = find(conn, surname, 'person')
        tgt = find(conn, fullname, 'person')
        if src and tgt:
            moved = merge(conn, src[0], tgt[0], dry_run)
            stats['merged'] += 1
            stats['articles_moved'] += moved
            amb_count += 1

    if not dry_run:
        conn.commit()
    print(f"  Ambiguous merges (verified): {amb_count}")

    # ══════════════════════════════════════════════════════════════════
    # STEP 6: CLEAN UP ORPHANS
    # ══════════════════════════════════════════════════════════════════
    print(f"\n── Step 6: Orphan cleanup ──")
    orphans = conn.execute("""
        SELECT COUNT(*) FROM entities 
        WHERE NOT EXISTS (SELECT 1 FROM article_entities WHERE entity_id=entities.id)
    """).fetchone()[0]

    if not dry_run and orphans > 0:
        conn.execute("""
            DELETE FROM entities WHERE id NOT IN 
            (SELECT DISTINCT entity_id FROM article_entities)
        """)
        conn.commit()
    print(f"  Orphans: {orphans}")
    stats['deleted'] += orphans

    # ══════════════════════════════════════════════════════════════════
    # SUMMARY
    # ══════════════════════════════════════════════════════════════════
    total_after = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    persons_after = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person'").fetchone()[0]
    single_after = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person' AND name NOT LIKE '% %'").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"SUMMARY {'(DRY RUN)' if dry_run else '(COMPLETED)'}")
    print(f"{'='*60}")
    print(f"  Merged:        {stats['merged']}")
    print(f"  Articles moved: {stats['articles_moved']}")
    print(f"  Deleted:       {stats['deleted']}")
    print(f"  Reclassified:  {stats['reclassified']}")
    print(f"  ──────────────────────────")
    print(f"  Before: {total_before} entities, {persons_before} persons")
    print(f"  After:  {total_after} entities, {persons_after} persons")
    print(f"  Single-word persons: {single_after}")

    conn.close()


if __name__ == '__main__':
    dry = '--execute' not in sys.argv
    if dry:
        print("DRY RUN. Use --execute to apply changes.")
    run(dry_run=dry)
