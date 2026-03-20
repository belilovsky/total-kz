#!/usr/bin/env python3
"""
Entity cleanup v3 — Org dedup + Person names fix.
Fixes: duplicate orgs (short/full name), broken grammar, garbage, single-word persons.

Run: python scripts/cleanup_v3_entities.py              # dry run
     python scripts/cleanup_v3_entities.py --execute     # apply
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


def find_entity(conn, name, entity_type=None):
    """Find entity by name (case-insensitive). Returns (id, name, type) or None."""
    if entity_type:
        row = conn.execute(
            "SELECT id, name, entity_type FROM entities WHERE normalized=? AND entity_type=?",
            (name.lower().strip(), entity_type)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id, name, entity_type FROM entities WHERE normalized=?",
            (name.lower().strip(),)
        ).fetchone()
    return row


def get_link_count(conn, entity_id):
    return conn.execute("SELECT COUNT(*) FROM article_entities WHERE entity_id=?", (entity_id,)).fetchone()[0]


def merge_into(conn, source_name, target_id, dry_run, stats, source_type=None):
    """Find entity by name and merge it into target_id."""
    row = find_entity(conn, source_name, source_type)
    if not row:
        return
    source_id = row[0]
    if source_id == target_id:
        return
    cnt = get_link_count(conn, source_id)
    print(f"    merge «{row[1]}» ({cnt} links) → target #{target_id}")
    if not dry_run:
        # Move non-overlapping links
        conn.execute("""
            UPDATE article_entities SET entity_id=?
            WHERE entity_id=? AND article_id NOT IN
            (SELECT article_id FROM article_entities WHERE entity_id=?)
        """, (target_id, source_id, target_id))
        # Delete remaining (overlaps)
        conn.execute("DELETE FROM article_entities WHERE entity_id=?", (source_id,))
        conn.execute("DELETE FROM entities WHERE id=?", (source_id,))
    stats['merged'] += 1


def rename_entity(conn, name, new_name, entity_type=None, dry_run=True, stats=None):
    """Rename an entity (update name + normalized)."""
    row = find_entity(conn, name, entity_type)
    if not row:
        return None
    cnt = get_link_count(conn, row[0])
    print(f"    rename «{row[1]}» → «{new_name}» ({cnt} links)")
    if not dry_run:
        # Check if target normalized already exists
        existing = find_entity(conn, new_name, row[2])
        if existing and existing[0] != row[0]:
            # Target name exists — merge instead
            print(f"      → target exists (#{existing[0]}), merging instead")
            merge_into(conn, name, existing[0], dry_run, stats, entity_type)
            return existing[0]
        conn.execute(
            "UPDATE entities SET name=?, normalized=? WHERE id=?",
            (new_name, new_name.lower().strip(), row[0])
        )
    if stats:
        stats['renamed'] += 1
    return row[0]


def delete_entity(conn, name, entity_type=None, dry_run=True, stats=None):
    """Delete entity and all its article links."""
    row = find_entity(conn, name, entity_type)
    if not row:
        return
    cnt = get_link_count(conn, row[0])
    print(f"    delete «{row[1]}» ({row[2]}, {cnt} links)")
    if not dry_run:
        conn.execute("DELETE FROM article_entities WHERE entity_id=?", (row[0],))
        conn.execute("DELETE FROM entities WHERE id=?", (row[0],))
    if stats:
        stats['deleted'] += 1


def reclassify_entity(conn, name, old_type, new_type, dry_run=True, stats=None):
    """Change entity_type."""
    row = find_entity(conn, name, old_type)
    if not row:
        return
    cnt = get_link_count(conn, row[0])
    print(f"    reclassify «{row[1]}» {old_type}→{new_type} ({cnt} links)")
    if not dry_run:
        # Check if same normalized+new_type already exists
        existing = find_entity(conn, name, new_type)
        if existing:
            print(f"      → target type exists (#{existing[0]}), merging")
            conn.execute("""
                UPDATE article_entities SET entity_id=?
                WHERE entity_id=? AND article_id NOT IN
                (SELECT article_id FROM article_entities WHERE entity_id=?)
            """, (existing[0], row[0], existing[0]))
            conn.execute("DELETE FROM article_entities WHERE entity_id=?", (row[0],))
            conn.execute("DELETE FROM entities WHERE id=?", (row[0],))
        else:
            conn.execute(
                "UPDATE entities SET entity_type=? WHERE id=?",
                (new_type, row[0])
            )
    if stats:
        stats['reclassified'] += 1


def run(dry_run=True):
    if not dry_run:
        backup_db()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-200000")
    conn.execute("PRAGMA temp_store=MEMORY")

    total_b = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"ENTITY CLEANUP v3 {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
    print(f"{'='*60}")
    print(f"Entities before: {total_b:,}\n")

    stats = {'merged': 0, 'deleted': 0, 'renamed': 0, 'reclassified': 0}

    # ═══════════════════════════════════════════════════════════════
    # STEP 1: MERGE DUPLICATE ORGANIZATIONS
    # ═══════════════════════════════════════════════════════════════
    print("── Step 1: Merge duplicate orgs ──")

    # Each tuple: (canonical_name, [aliases_to_merge])
    ORG_MERGES = [
        # National Bank
        ("Национальный банк РК", [
            "Национальный банк",
            "Национальный банк Казахстана",
        ]),
        # Prosecutor General
        ("Генеральная прокуратура РК", [
            "Генпрокуратура",
            "Генеральная прокуратура",
        ]),
        # Health Ministry
        ("Минздрав РК", [
            "Минздрав",
            "Министерство здравоохранение",
        ]),
        # Interior Ministry
        ("МВД РК", [
            "МВД",
            "Министерство внутренние дела",
        ]),
        # Foreign Ministry
        ("МИД РК", [
            "МИД",
            "Министерство иностранные дела",
        ]),
        # Defense Ministry
        ("Минобороны РК", [
            "Минобороны",
            "Министерство оборона",
        ]),
        # Finance Ministry
        ("Минфин РК", [
            "Минфин",
            "Министерство финансов",
        ]),
        # Agriculture Ministry
        ("Минсельхоз РК", [
            "Минсельхоз",
            "Министерство сельское хозяйство",
            "МСХ",
        ]),
        # Energy Ministry
        ("Минэнерго РК", [
            "Минэнерго",
            "Министерство энергетика",
        ]),
        # KNB
        ("КНБ РК", [
            "КНБ",
            "Комитет национальная безопасность",
        ]),
        # Statistics Bureau
        ("Бюро нацстатистики РК", [
            "Бюро национальная статистика",
            "Бюро нацстатистики",
        ]),
        # Kazhydromet
        ("Казгидромет", [
            "РГП «Казгидромет»",
            "Казгидромет",
        ]),
        # Labor Ministry
        ("Минтруда РК", [
            "Минтруда",
            "Министерство труд и социальная защита население",
            "Министерство труд и соцзащита",
        ]),
        # Anti-corruption
        ("Антикор РК", [
            "Агентство по противодействие коррупция",
            "Агентство РК по противодействие коррупция",
            "Антикоррупционная служба",
            "Антикоре",
        ]),
        # Emergency Ministry
        ("МЧС РК", [
            "МЧС",
            "Министерство по чрезвычайные ситуациям",
        ]),
        # Education Ministry
        ("Минпросвещения РК", [
            "Минпросвещения",
            "Министерство просвещение",
        ]),
        # Justice Ministry
        ("Минюст РК", [
            "Минюст",
            "Министерство юстиция",
        ]),
        # State Revenue Committee
        ("КГД МФ РК", [
            "КГД",
            "Комитет государственные доходы",
        ]),
        # Transport Ministry
        ("Минтранс РК", [
            "Министерство транспорт",
        ]),
        # Industry Ministry
        ("Минпром РК", [
            "Министерство промышленность и строительство",
        ]),
        # Trade Ministry
        ("Минторговли РК", [
            "Министерство торговля и интеграция",
        ]),
        # Tourism & Sports Ministry
        ("Минтуризма РК", [
            "Министерство туризм и спорт",
        ]),
        # Culture Ministry
        ("Минкультуры РК", [
            "Министерство культура и информация",
        ]),
        # Ecology Ministry
        ("Минэкологии РК", [
            "Министерство экология и природные ресурсы",
        ]),
        # Economy Ministry
        ("Миннацэкономики РК", [
            "Министерство национальная экономика",
        ]),
        # Water Ministry
        ("Минводресурсов РК", [
            "Министерство водные ресурсы и ирригации",
        ]),
        # Competition agency
        ("АЗРК", [
            "Агентство по защита и развитие конкуренция",
            "АЗРК",
        ]),
        # 24.kg news
        ("24.kg", [
            "ИА 24.kg",
            "ИА24.kg",
        ]),
        # Armed Forces into Defense Ministry
        ("Вооружённые силы РК", [
            "Вооруженные силы",
        ]),
        # AFM — rename
        ("АФМ РК", [
            "АФМ",
        ]),
        # AFRR — rename
        ("АРРФР РК", [
            "АРРФР",
        ]),
        # SCC — rename
        ("СЦК", [
            "СЦК",
        ]),
    ]

    for canonical, aliases in ORG_MERGES:
        # Find or create the canonical entity — first alias that exists becomes the target
        target_id = None
        target_row = find_entity(conn, canonical, 'org')
        if target_row:
            target_id = target_row[0]

        if not target_id:
            # Use first existing alias as target
            for alias in aliases:
                row = find_entity(conn, alias, 'org')
                if row:
                    target_id = row[0]
                    break

        if not target_id:
            continue  # None exist in this DB

        # Rename target to canonical if different
        target_row = conn.execute("SELECT name FROM entities WHERE id=?", (target_id,)).fetchone()
        if target_row and target_row[0] != canonical:
            # Check if canonical normalized already exists with same type
            existing = find_entity(conn, canonical, 'org')
            if existing and existing[0] != target_id:
                # Merge target into existing canonical
                print(f"  «{canonical}» already exists (#{existing[0]}), merging #{target_id} into it")
                if not dry_run:
                    conn.execute("""
                        UPDATE article_entities SET entity_id=?
                        WHERE entity_id=? AND article_id NOT IN
                        (SELECT article_id FROM article_entities WHERE entity_id=?)
                    """, (existing[0], target_id, existing[0]))
                    conn.execute("DELETE FROM article_entities WHERE entity_id=?", (target_id,))
                    conn.execute("DELETE FROM entities WHERE id=?", (target_id,))
                target_id = existing[0]
            else:
                cnt = get_link_count(conn, target_id)
                print(f"  rename «{target_row[0]}» → «{canonical}» (#{target_id}, {cnt} links)")
                if not dry_run:
                    conn.execute(
                        "UPDATE entities SET name=?, normalized=? WHERE id=?",
                        (canonical, canonical.lower().strip(), target_id)
                    )
                stats['renamed'] += 1

        # Merge all aliases into target
        for alias in aliases:
            row = find_entity(conn, alias, 'org')
            if row and row[0] != target_id:
                merge_into(conn, alias, target_id, dry_run, stats, 'org')

    # ═══════════════════════════════════════════════════════════════
    # STEP 2: DELETE GARBAGE ORGS
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 2: Delete garbage orgs ──")

    GARBAGE_ORGS = [
        'ТОО', 'Telegram-канал', 'ИА', 'Мой город',
        'АПК',   # too generic
        'ТЭЦ',   # too generic
        'АЭС',   # too generic
        'ДП',    # too generic (Департамент полиции — abbreviation is ambiguous)
        'УВД',   # too generic
        'ДВД',   # too generic
        'ДЧС',   # too generic — merged into МЧС above
        'ИВС',   # Изолятор временного содержания — not an org
        'КВИ',   # Коронавирусная инфекция — not an org
    ]
    for name in GARBAGE_ORGS:
        delete_entity(conn, name, 'org', dry_run, stats)

    # ═══════════════════════════════════════════════════════════════
    # STEP 3: RENAME SPECIFIC ORGS
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 3: Rename orgs ──")

    RENAMES = [
        ("Высочайшая аудиторская палата", "Высшая аудиторская палата РК"),
        ("ВАП", "Высшая аудиторская палата РК"),  # may also exist as abbreviation
        ("Совет Безопасность", "Совет Безопасности ООН"),
        ("Реал", "Реал Мадрид"),
        ("ВВС", "BBC"),
        ("Лада", "LADA"),
        ("Экспресс К", "Экспресс К (газета)"),
        ("Акорды", "Акорда"),
        ("Казахстанская фондовая биржа", "KASE"),
        ("Таможенный союз", "Таможенный союз ЕАЭС"),
        ("Народный банк", "Халык банк"),
        ("Сбербанк", "Сбербанк России"),
        ("Госдума", "Госдума РФ"),
        ("ГКНБ", "ГКНБ Кыргызстана"),
    ]
    for old_name, new_name in RENAMES:
        rename_entity(conn, old_name, new_name, 'org', dry_run, stats)

    # ═══════════════════════════════════════════════════════════════
    # STEP 4: RECLASSIFY MISTYPED ENTITIES
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 4: Reclassify entities ──")

    RECLASSIFY = [
        # Org → Location
        ("Акорда", "org", "location"),
        ("Барселона", "org", "location"),
    ]
    for name, old_type, new_type in RECLASSIFY:
        reclassify_entity(conn, name, old_type, new_type, dry_run, stats)

    # ═══════════════════════════════════════════════════════════════
    # STEP 5: CLEAN UP PERSON NAMES
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 5: Fix person names ──")

    # 5a. Delete single-word person entities with < 20 links (too ambiguous)
    print("  5a. Delete ambiguous single-word person names...")
    single_word = conn.execute("""
        SELECT e.id, e.name, COUNT(ae.article_id) as cnt
        FROM entities e
        JOIN article_entities ae ON ae.entity_id = e.id
        WHERE e.entity_type = 'person'
          AND e.name NOT LIKE '% %'
          AND e.name NOT LIKE '%.%'
        GROUP BY e.id
        HAVING cnt < 20
        ORDER BY cnt DESC
    """).fetchall()
    print(f"    Found {len(single_word)} single-word persons with < 20 links")
    for eid, ename, cnt in single_word[:10]:
        print(f"      sample: «{ename}» ({cnt} links)")
    if len(single_word) > 10:
        print(f"      ... and {len(single_word)-10} more")

    if not dry_run:
        for eid, ename, cnt in single_word:
            conn.execute("DELETE FROM article_entities WHERE entity_id=?", (eid,))
            conn.execute("DELETE FROM entities WHERE id=?", (eid,))
    stats['deleted'] += len(single_word)

    # 5b. Known single-word merges (important people where surname-only duplicate exists)
    PERSON_MERGES = [
        # Surname-only → Full name
        ("Сулейменов", "Тимур Сулейменов"),
        ("Байжанов", None),  # delete — ambiguous
        ("Султанов", None),  # delete — ambiguous
        ("Алиев", "Ильхам Алиев"),
        ("Шерзат", "Шерзат Полат"),  # contextually this is likely Шерзат Полат
    ]
    for surname, full_name in PERSON_MERGES:
        if full_name is None:
            delete_entity(conn, surname, 'person', dry_run, stats)
        else:
            target = find_entity(conn, full_name, 'person')
            if target:
                merge_into(conn, surname, target[0], dry_run, stats, 'person')
            else:
                # No target, just rename
                rename_entity(conn, surname, full_name, 'person', dry_run, stats)

    # 5c. Delete single-word persons with >= 20 links (handle individually)
    single_big = conn.execute("""
        SELECT e.id, e.name, COUNT(ae.article_id) as cnt
        FROM entities e
        JOIN article_entities ae ON ae.entity_id = e.id
        WHERE e.entity_type = 'person'
          AND e.name NOT LIKE '% %'
          AND e.name NOT LIKE '%.%'
        GROUP BY e.id
        HAVING cnt >= 20
        ORDER BY cnt DESC
    """).fetchall()
    if single_big:
        print(f"\n  5c. Single-word persons with >= 20 links (deleting — too ambiguous):")
        for eid, ename, cnt in single_big:
            print(f"      «{ename}» ({cnt} links)")
            if not dry_run:
                conn.execute("DELETE FROM article_entities WHERE entity_id=?", (eid,))
                conn.execute("DELETE FROM entities WHERE id=?", (eid,))
            stats['deleted'] += 1

    # ═══════════════════════════════════════════════════════════════
    # STEP 6: DELETE LOCATION/PERSON MISMATCHES IN ORG
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 6: Clean up media source orgs ──")

    # Media outlets are OK to keep as orgs, but let's make sure they're properly named
    MEDIA_RENAMES = [
        ("Газета.ru", "Газета.ру"),
        ("Газета.uz", "Газета.уз"),
    ]
    for old_name, new_name in MEDIA_RENAMES:
        rename_entity(conn, old_name, new_name, 'org', dry_run, stats)

    # Delete pure media source duplicates/aggregators — these are citation sources, not entities
    MEDIA_DELETE = [
        'Livesport', 'Otyrar', 'Upl', 'Hi-tech', 'Economist',
        'Championat', 'Energyprom', 'Finprom', 'YK-news', 'Bild',
        'Stopfake', 'Transfermarkt',
    ]
    for name in MEDIA_DELETE:
        delete_entity(conn, name, 'org', dry_run, stats)

    # ═══════════════════════════════════════════════════════════════
    # COMMIT
    # ═══════════════════════════════════════════════════════════════
    if not dry_run:
        conn.commit()

    total_a = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"  Merged:       {stats['merged']}")
    print(f"  Deleted:      {stats['deleted']}")
    print(f"  Renamed:      {stats['renamed']}")
    print(f"  Reclassified: {stats['reclassified']}")
    print(f"  Entities:     {total_b:,} → {total_a:,} ({total_b - total_a:,} removed)")
    print(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    execute = "--execute" in sys.argv
    run(dry_run=not execute)
