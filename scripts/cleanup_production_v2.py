#!/usr/bin/env python3
"""
Production entity cleanup v2 — OPTIMIZED for large DBs (312K+ entities).
Key optimization: batch SQL instead of per-entity subqueries.

Run: python scripts/cleanup_production_v2.py --execute
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
    bp = os.path.join(BACKUP_DIR, f'total_{ts}.db')
    print(f"Backup... ", end='', flush=True)
    shutil.copy2(DB_PATH, bp)
    print(f"✓ ({os.path.getsize(bp)/1024/1024:.0f} MB)")


def merge(conn, source_id, target_id):
    """Merge source→target: move links, delete source. Returns 1 if merged, 0 if skipped."""
    if source_id == target_id:
        return 0
    # Verify both exist
    if not conn.execute("SELECT id FROM entities WHERE id=?", (source_id,)).fetchone():
        return 0
    if not conn.execute("SELECT id FROM entities WHERE id=?", (target_id,)).fetchone():
        return 0
    # Move non-overlapping links
    conn.execute("""
        UPDATE article_entities SET entity_id=?
        WHERE entity_id=? AND article_id NOT IN
        (SELECT article_id FROM article_entities WHERE entity_id=?)
    """, (target_id, source_id, target_id))
    # Delete remaining source links (overlaps)
    conn.execute("DELETE FROM article_entities WHERE entity_id=?", (source_id,))
    # Delete source entity
    conn.execute("DELETE FROM entities WHERE id=?", (source_id,))
    return 1


def run(dry_run=True):
    if not dry_run:
        backup_db()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-200000")
    conn.execute("PRAGMA temp_store=MEMORY")

    total_b = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    persons_b = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person'").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"PRODUCTION CLEANUP v2 {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
    print(f"{'='*60}")
    print(f"Entities: {total_b:,}, Persons: {persons_b:,}\n")

    stats = {'merged': 0, 'deleted': 0, 'reclassified': 0}

    # ═══════════════════════════════════════════════════════════════
    # STEP 1: DELETE GARBAGE (batch SQL)
    # ═══════════════════════════════════════════════════════════════
    print("── Step 1: Delete garbage ──")

    # 1a. Known garbage names
    GARBAGE = ['Эксклюзив', 'Елбасы', 'ЗРАБатр', 'Александр', 'COVID-19']
    AUTHORS = ['Азамат Галеев', 'Ирина Ярунина', 'Алексей Байтеев']
    all_garbage = GARBAGE + AUTHORS
    placeholders = ','.join('?' * len(all_garbage))

    cnt = conn.execute(f"""
        SELECT COUNT(*) FROM entities
        WHERE entity_type='person' AND name IN ({placeholders})
    """, all_garbage).fetchone()[0]
    print(f"  Named garbage (person): {cnt}")

    if not dry_run:
        conn.execute(f"""
            DELETE FROM article_entities WHERE entity_id IN
            (SELECT id FROM entities WHERE entity_type='person' AND name IN ({placeholders}))
        """, all_garbage)
        conn.execute(f"""
            DELETE FROM entities WHERE entity_type='person' AND name IN ({placeholders})
        """, all_garbage)
    stats['deleted'] += cnt

    # 1a-2. Garbage in orgs
    ORG_GARBAGE = ['ТОО', 'COVID-19', 'Мой город']
    ph_org = ','.join('?' * len(ORG_GARBAGE))
    cnt_og = conn.execute(f"""
        SELECT COUNT(*) FROM entities
        WHERE entity_type='org' AND name IN ({ph_org})
    """, ORG_GARBAGE).fetchone()[0]
    print(f"  Named garbage (org): {cnt_og}")
    if not dry_run:
        conn.execute(f"DELETE FROM article_entities WHERE entity_id IN (SELECT id FROM entities WHERE entity_type='org' AND name IN ({ph_org}))", ORG_GARBAGE)
        conn.execute(f"DELETE FROM entities WHERE entity_type='org' AND name IN ({ph_org})", ORG_GARBAGE)
    stats['deleted'] += cnt_og

    # 1a-3. Delete any-type garbage (COVID-19 may be in any type)
    ANY_GARBAGE = ['COVID-19']
    ph_any = ','.join('?' * len(ANY_GARBAGE))
    cnt_any = conn.execute(f"SELECT COUNT(*) FROM entities WHERE name IN ({ph_any})", ANY_GARBAGE).fetchone()[0]
    print(f"  Named garbage (any type): {cnt_any}")
    if not dry_run:
        conn.execute(f"DELETE FROM article_entities WHERE entity_id IN (SELECT id FROM entities WHERE name IN ({ph_any}))", ANY_GARBAGE)
        conn.execute(f"DELETE FROM entities WHERE name IN ({ph_any})", ANY_GARBAGE)
    stats['deleted'] += cnt_any

    # 1b. Broken NER patterns (batch)
    BROKEN_WHERE = """
        entity_type='person' AND (
            (name LIKE 'РК%' AND LENGTH(name) > 15)
            OR name LIKE '%проинформировал%'
            OR name LIKE '%заявлял%'
            OR (name LIKE '%также%' AND LENGTH(name) > 20)
            OR name LIKE '%освобожден%'
            OR name LIKE '%ознакомил%'
            OR (name LIKE '%реформа%' AND LENGTH(name) > 20)
            OR (name LIKE '%журналист%' AND LENGTH(name) > 25)
            OR (name LIKE '%усилен%' AND LENGTH(name) > 15)
            OR name LIKE '%связывались%'
            OR (name LIKE '%жалобы%' AND LENGTH(name) > 15)
            OR (name LIKE 'Экс-Министр%' AND LENGTH(name) > 20)
            OR (name LIKE 'Премьер-Министр%' AND LENGTH(name) > 25)
            OR name LIKE '%условиявозвращ%'
            OR name LIKE 'СШАДжо%'
            OR name LIKE '%Досаевв%' OR name LIKE '%Досаевво%' OR name LIKE '%Досаеван%'
            OR name LIKE '%Байденупо%' OR name LIKE '%Байденаза%'
            OR name LIKE '%Ашимбаевтакже%'
            OR name LIKE '%Бишимбаевалейла%'
            OR name LIKE 'Племянник %' OR name LIKE 'Внук %'
            OR name LIKE 'Псевдосотрудник %' OR name LIKE 'Фейковый %'
            OR name LIKE '%Назарбаеваи' OR name LIKE '%Назарбаевых'
            OR name LIKE '%Назарбаевыми'
            OR name LIKE '%всегда%' AND LENGTH(name) > 20
            OR name LIKE 'Аким Алматы%'
            OR name LIKE 'Замакима%'
        )
    """
    cnt = conn.execute(f"SELECT COUNT(*) FROM entities WHERE {BROKEN_WHERE}").fetchone()[0]
    print(f"  Broken NER: {cnt}")
    if not dry_run:
        conn.execute(f"DELETE FROM article_entities WHERE entity_id IN (SELECT id FROM entities WHERE {BROKEN_WHERE})")
        conn.execute(f"DELETE FROM entities WHERE {BROKEN_WHERE}")
    stats['deleted'] += cnt

    # 1c. Initials (А.Б., К.Н., Н. Назарбаев, etc.)
    INITIALS_WHERE = """
        entity_type='person' AND (
            (LENGTH(name) <= 4 AND name LIKE '%.%')
            OR (LENGTH(name) <= 6 AND name LIKE '%.%.%')
            OR (LENGTH(name) <= 8 AND name LIKE '%.%.%.%')
            OR name = 'Ф.И.О.'
            OR (LENGTH(name) <= 3)
        )
    """
    cnt = conn.execute(f"SELECT COUNT(*) FROM entities WHERE {INITIALS_WHERE}").fetchone()[0]
    print(f"  Initials/short: {cnt}")
    if not dry_run:
        conn.execute(f"DELETE FROM article_entities WHERE entity_id IN (SELECT id FROM entities WHERE {INITIALS_WHERE})")
        conn.execute(f"DELETE FROM entities WHERE {INITIALS_WHERE}")
    stats['deleted'] += cnt

    if not dry_run:
        conn.commit()
        print("  ✓ Committed step 1")

    # ═══════════════════════════════════════════════════════════════
    # STEP 2: RECLASSIFY
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 2: Reclassify ──")

    TO_ORG = ['Акорда','Акорды','Антикора','Нацфонд','Казгидромет','Казком',
              'Миннацэкономики','Минторговли','Европульс','Минцифры','Госаудит',
              'Миннауки','Минпром','Зампремьера','Акиматы','Минобороны']

    # Reclassify location → org
    LOC_TO_ORG = ['Евросоюз']
    TO_LOC = ['Нур-Султан','Улытау','Мангистау','Жетісу','Аксу','Семей',
              'Туркестанской','Улытауской','Жетысуской','Абайской',
              'Сырдарьинской','Джизакской','Тобол-Торгайской',
              'Акмолинская','Актюбинская','Алматинская','Жамбылская',
              'Костанайская','Кызылорды','Талдыкорган','Кульсары']

    # Reclassify location→org
    for names, old_type, new_type in [(LOC_TO_ORG, 'location', 'org')]:
        ph = ','.join('?' * len(names))
        rows = conn.execute(f"SELECT id, name, normalized FROM entities WHERE entity_type=? AND name IN ({ph})", [old_type] + names).fetchall()
        for eid, ename, enorm in rows:
            existing = conn.execute(
                "SELECT id FROM entities WHERE normalized=? AND entity_type=? AND id!=?",
                (enorm, new_type, eid)
            ).fetchone()
            if existing:
                if not dry_run:
                    merge(conn, eid, existing[0])
            else:
                if not dry_run:
                    conn.execute("UPDATE entities SET entity_type=? WHERE id=?", (new_type, eid))
            stats['reclassified'] += 1
        print(f"  {old_type}→{new_type}: {len(rows)} ({', '.join(names)})")

    # Reclassify person→org and person→location
    for names, new_type in [(TO_ORG, 'org'), (TO_LOC, 'location')]:
        ph = ','.join('?' * len(names))
        rows = conn.execute(f"SELECT id, name, normalized FROM entities WHERE entity_type='person' AND name IN ({ph})", names).fetchall()
        for eid, ename, enorm in rows:
            # Check if target type already has this normalized name
            existing = conn.execute(
                "SELECT id FROM entities WHERE normalized=? AND entity_type=? AND id!=?",
                (enorm, new_type, eid)
            ).fetchone()
            if existing:
                # Merge into existing entity of correct type
                if not dry_run:
                    merge(conn, eid, existing[0])
            else:
                if not dry_run:
                    conn.execute("UPDATE entities SET entity_type=? WHERE id=?", (new_type, eid))
            stats['reclassified'] += 1

    if not dry_run:
        conn.commit()
    print(f"  Reclassified: {stats['reclassified']}")

    # ═══════════════════════════════════════════════════════════════
    # STEP 3: MORPHOLOGICAL MERGES (manual list)
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 3: Morphological merges ──")

    MORPH = [
        ('Куандыка Бишимбаев','Куандык Бишимбаев'),
        ('Куандыком Бишимбаев','Куандык Бишимбаев'),
        ('Каната Бозумбаев','Канат Бозумбаев'),
        ('Илона Маска','Илон Маск'),
        ('Экс-министр Бишимбаев','Куандык Бишимбаев'),
        ('Зампремьера Бозумбаев','Канат Бозумбаев'),
        ('Мажилисмен Бакытжан Базарбек','Бакытжан Базарбек'),
        ('Бахытжан Базарбек','Бакытжан Базарбек'),
        ('Жаслана Мадиев','Жаслан Мадиев'),
        ('Нукеновой','Салтанат Нукенова'),
        ('Салтанат Нукеновой','Салтанат Нукенова'),
        ('Дархана Сатыбалды','Дархан Сатыбалды'),
        ('Ерболата Досаева','Ерболат Досаев'),
        ('Ерболату Досаеву','Ерболат Досаев'),
        ('Маулен Ашимбаева','Маулен Ашимбаев'),
        ('Джозеф Байден','Джо Байден'),
        ('Килиана Мбаппе','Килиан Мбаппе'),
        ('Килиану Мбаппе','Килиан Мбаппе'),
        ('Килиане Мбаппе','Килиан Мбаппе'),
        ('Н. Назарбаев','Нурсултан Назарбаев'),
        ('Нурсултан Абишевич Назарбаев','Нурсултан Назарбаев'),
        ('Маска','Илон Маск'),
        ('Канат Бисембаевич Шарлапаев','Канат Шарлапаев'),
        ('Шерзата','Шерзат'),
        ('Марата Баркулов','Марат Баркулов'),
        ('Саясата Нурбек','Саясат Нурбек'),
        ('МВДСанжар Адилов','Санжар Адилов'),
        ('Галымжайн Койгельдиев','Галымжан Койгельдиев'),
        ('Ерсаин Нагаспаев','Ерсайын Нагаспаев'),
        ('Марат Карабаевна','Марат Карабаев'),
        ('Айдарбек Сапарова','Айдарбек Сапаров'),
        # v2.1 additions
        ('Сы Цзиньпин','Си Цзиньпин'),
    ]

    m_cnt = 0
    for sn, tn in MORPH:
        s = conn.execute("SELECT id FROM entities WHERE name=? AND entity_type='person'", (sn,)).fetchone()
        t = conn.execute("SELECT id FROM entities WHERE name=? AND entity_type='person'", (tn,)).fetchone()
        if s and t and not dry_run:
            merge(conn, s[0], t[0])
            m_cnt += 1
        elif s and t:
            m_cnt += 1
    stats['merged'] += m_cnt
    if not dry_run:
        conn.commit()
    print(f"  Morphological: {m_cnt}")

    # ═══════════════════════════════════════════════════════════════
    # STEP 4: AUTO-MERGE SURNAMES → FULL NAMES (OPTIMIZED!)
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 4: Auto-merge surnames (optimized) ──")

    SKIP = {
        'Антикора','Нацфонд','Улытау','Мангистау','Миннацэкономики',
        'Минторговли','Европульс','Акорды','Акорда','Казгидромет',
        'Казком','Нур-Султан','Жетісу','Аксу','Семей','Семея',
        'Кашаган','Диана','Александр','Екатерина','Майкл','Кайрат',
        'Карина','Шерзат','Шерзата','Гарри','Тайсон','Хави','Елбасы',
        'Эксклюзив','Наурыз','Салтанат','Наталья','Елена','Минцифры',
        'Госаудит','Миннауки','Минпром','Акиматы',
    }

    # KEY OPTIMIZATION: build lookup of all two-word person names
    # and index them by surname (last word)
    print("  Building fullname index...", flush=True)

    # Get all "Имя Фамилия" entities (2 words, no garbage prefixes)
    fullnames = conn.execute("""
        SELECT e.id, e.name, e.normalized,
               (SELECT COUNT(*) FROM article_entities WHERE entity_id=e.id) as arts
        FROM entities e
        WHERE e.entity_type='person'
          AND e.name LIKE '% %'
          AND e.name NOT LIKE '% % % %'
          AND e.name NOT LIKE 'Экс-%'
          AND e.name NOT LIKE 'Зампремьер%'
          AND e.name NOT LIKE 'Мажилисмен%'
          AND e.name NOT LIKE 'РК%'
          AND e.name NOT LIKE 'Псевдо%'
          AND e.name NOT LIKE 'Фейков%'
          AND e.name NOT LIKE 'Аким %'
          AND e.name NOT LIKE 'Замаким%'
          AND e.name NOT LIKE 'Маслихат%'
          AND e.name NOT LIKE 'Месторожд%'
          AND e.name NOT LIKE 'Нейросеть%'
          AND e.name NOT LIKE 'Брат %'
          AND e.name NOT LIKE 'Племянник%'
          AND e.name NOT LIKE 'Внук%'
    """).fetchall()

    # Index by lowercase surname
    from collections import defaultdict
    surname_index = defaultdict(list)
    for eid, name, norm, arts in fullnames:
        parts = name.strip().split()
        if len(parts) >= 2:
            surname_lower = parts[-1].lower()
            surname_index[surname_lower].append((eid, name, arts))

    print(f"  Indexed {len(fullnames):,} fullnames, {len(surname_index):,} unique surnames")

    # Get all single-word person entities
    singles = conn.execute("""
        SELECT e.id, e.name,
               (SELECT COUNT(*) FROM article_entities WHERE entity_id=e.id) as arts
        FROM entities e
        WHERE e.entity_type='person'
          AND e.name NOT LIKE '% %'
          AND LENGTH(e.name) > 3
        ORDER BY arts DESC
    """).fetchall()

    print(f"  Processing {len(singles):,} single-word entities...")

    merge_count = 0
    for sid, surname, s_arts in singles:
        if surname in SKIP:
            continue
        
        key = surname.lower()
        matches = surname_index.get(key, [])

        if len(matches) == 1:
            tid, tname, t_arts = matches[0]
            if not dry_run:
                merge(conn, sid, tid)
            merge_count += 1
        # Skip ambiguous (multiple matches)

    stats['merged'] += merge_count
    if not dry_run:
        conn.commit()
    print(f"  Unambiguous merges: {merge_count}")

    # ═══════════════════════════════════════════════════════════════
    # STEP 5: VERIFIED AMBIGUOUS MERGES
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 5: Verified ambiguous merges ──")

    AMBIGUOUS = [
        ('Ахметов','Серик Ахметов'),
        ('Аблязов','Мухтар Аблязов'),
        ('Головкин','Геннадий Головкин'),
        ('Порошенко','Петр Порошенко'),
        ('Челах','Владислав Челах'),
        ('Обама','Барак Обама'),
        ('Досаев','Ерболат Досаев'),
        ('Бишимбаев','Куандык Бишимбаев'),
        ('Шарлапаев','Канат Шарлапаев'),
        ('Сапаров','Айдарбек Сапаров'),
        ('Ашимбаев','Маулен Ашимбаев'),
        ('Карабаев','Марат Карабаев'),
        ('Байден','Джо Байден'),
        ('Базарбек','Бакытжан Базарбек'),
        ('Бозумбаев','Канат Бозумбаев'),
        ('Мадиев','Жаслан Мадиев'),
        ('Нурбек','Саясат Нурбек'),
        ('Бейсембаев','Гани Бейсембаев'),
        ('Жакупова','Светлана Жакупова'),
        ('Нагаспаев','Ерсайын Нагаспаев'),
        ('Аймагамбетов','Асхат Аймагамбетов'),
        ('Сауранбаев','Нурлан Сауранбаев'),
        ('Адилов','Санжар Адилов'),
        ('Аринов','Чингис Аринов'),
        ('Баркулов','Марат Баркулов'),
        ('Пономарев','Сергей Пономарев'),
        ('Сатыбалды','Дархан Сатыбалды'),
        ('Сарсембаев','Ерлан Сарсембаев'),
        ('Койгельдиев','Галымжан Койгельдиев'),
        ('Мбаппе','Килиан Мбаппе'),
        ('Лукин','Андрей Лукин'),
        ('Байбазаров','Нурлан Байбазаров'),
        # v2.1 additions
        ('Янукович','Виктор Янукович'),
    ]

    a_cnt = 0
    for sn, tn in AMBIGUOUS:
        s = conn.execute("SELECT id FROM entities WHERE name=? AND entity_type='person'", (sn,)).fetchone()
        t = conn.execute("SELECT id FROM entities WHERE name=? AND entity_type='person'", (tn,)).fetchone()
        if s and t:
            if not dry_run:
                merge(conn, s[0], t[0])
            a_cnt += 1
    stats['merged'] += a_cnt
    if not dry_run:
        conn.commit()
    print(f"  Verified ambiguous: {a_cnt}")

    # ═══════════════════════════════════════════════════════════════
    # STEP 6: ORPHAN CLEANUP
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 6: Orphan cleanup ──")
    orphans = conn.execute("""
        SELECT COUNT(*) FROM entities
        WHERE NOT EXISTS (SELECT 1 FROM article_entities WHERE entity_id=entities.id)
    """).fetchone()[0]
    if not dry_run and orphans:
        conn.execute("DELETE FROM entities WHERE id NOT IN (SELECT DISTINCT entity_id FROM article_entities)")
        conn.commit()
    print(f"  Orphans: {orphans:,}")
    stats['deleted'] += orphans

    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    total_a = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    persons_a = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person'").fetchone()[0]
    single_a = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person' AND name NOT LIKE '% %'").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"SUMMARY {'(DRY RUN)' if dry_run else '(COMPLETED)'}")
    print(f"{'='*60}")
    print(f"  Merged:       {stats['merged']:,}")
    print(f"  Deleted:      {stats['deleted']:,}")
    print(f"  Reclassified: {stats['reclassified']}")
    print(f"  ──────────────────────────")
    print(f"  Before: {total_b:,} entities, {persons_b:,} persons")
    print(f"  After:  {total_a:,} entities, {persons_a:,} persons")
    print(f"  Single-word persons remaining: {single_a:,}")

    conn.close()


if __name__ == '__main__':
    dry = '--execute' not in sys.argv
    if dry:
        print("DRY RUN. Use --execute to apply.")
    run(dry_run=dry)
