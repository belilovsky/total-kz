#!/usr/bin/env python3
"""
Entity cleanup script for total.kz
Handles: merging duplicates, reclassifying garbage, deleting broken NER entries.

Operations:
1. MERGE: surname → full name (move article_entities links, delete old)
2. MERGE: morphological variants → canonical form
3. RECLASSIFY: non-person entities wrongly typed as 'person' → correct type
4. DELETE: broken NER garbage entries
5. MERGE: "Н. Назарбаев" style → "Нурсултан Назарбаев"

All operations are idempotent and safe (uses transactions).
"""
import sqlite3
import os
import sys
import shutil
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'total.db')
BACKUP_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'backups')


def backup_db():
    """Create timestamped backup before any changes."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = os.path.join(BACKUP_DIR, f'total_{ts}.db')
    shutil.copy2(DB_PATH, backup_path)
    size_mb = os.path.getsize(backup_path) / 1024 / 1024
    print(f"✓ Backup created: {backup_path} ({size_mb:.1f} MB)")
    return backup_path


def merge_entities(conn, source_id, target_id, source_name, target_name, dry_run=False):
    """
    Merge source entity into target:
    1. Move all article_entities from source → target (skip duplicates, sum mention_count)
    2. Delete source entity
    """
    if source_id == target_id:
        return 0

    # Count articles that will be moved
    moved = conn.execute("""
        SELECT COUNT(*) FROM article_entities 
        WHERE entity_id = ? 
        AND article_id NOT IN (SELECT article_id FROM article_entities WHERE entity_id = ?)
    """, (source_id, target_id)).fetchone()[0]

    dupes = conn.execute("""
        SELECT COUNT(*) FROM article_entities 
        WHERE entity_id = ? 
        AND article_id IN (SELECT article_id FROM article_entities WHERE entity_id = ?)
    """, (source_id, target_id)).fetchone()[0]

    total_source = conn.execute(
        "SELECT COUNT(*) FROM article_entities WHERE entity_id = ?", (source_id,)
    ).fetchone()[0]

    if dry_run:
        print(f"  MERGE: [{source_id}] '{source_name}' ({total_source} arts) → [{target_id}] '{target_name}' "
              f"(+{moved} new, {dupes} dupes skipped)")
        return moved

    # For overlapping articles: add mention_count from source to target
    conn.execute("""
        UPDATE article_entities 
        SET mention_count = mention_count + COALESCE(
            (SELECT mention_count FROM article_entities ae2 
             WHERE ae2.entity_id = ? AND ae2.article_id = article_entities.article_id), 0
        )
        WHERE entity_id = ? 
        AND article_id IN (SELECT article_id FROM article_entities WHERE entity_id = ?)
    """, (source_id, target_id, source_id))

    # Move non-overlapping articles
    conn.execute("""
        UPDATE article_entities SET entity_id = ? 
        WHERE entity_id = ? 
        AND article_id NOT IN (SELECT article_id FROM article_entities WHERE entity_id = ?)
    """, (target_id, source_id, target_id))

    # Delete remaining source links (the overlapping ones)
    conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (source_id,))

    # Delete source entity
    conn.execute("DELETE FROM entities WHERE id = ?", (source_id,))

    print(f"  ✓ MERGED: [{source_id}] '{source_name}' → [{target_id}] '{target_name}' "
          f"(+{moved} articles, {dupes} dupes merged)")
    return moved


def reclassify_entity(conn, entity_id, name, new_type, dry_run=False):
    """Change entity_type for misclassified entities."""
    if dry_run:
        print(f"  RECLASSIFY: [{entity_id}] '{name}' → type='{new_type}'")
        return
    
    # Check if there's already an entity with same normalized name and new type
    existing = conn.execute("""
        SELECT id, name FROM entities 
        WHERE normalized = (SELECT normalized FROM entities WHERE id = ?) 
        AND entity_type = ? AND id != ?
    """, (entity_id, new_type, entity_id)).fetchone()
    
    if existing:
        # Merge into existing entity of the correct type
        merge_entities(conn, entity_id, existing[0], name, existing[1])
        print(f"  ✓ RECLASSIFIED+MERGED: [{entity_id}] '{name}' → [{existing[0]}] '{existing[1]}' (type={new_type})")
    else:
        conn.execute("UPDATE entities SET entity_type = ? WHERE id = ?", (new_type, entity_id))
        print(f"  ✓ RECLASSIFIED: [{entity_id}] '{name}' → type='{new_type}'")


def delete_entity(conn, entity_id, name, dry_run=False):
    """Delete entity and all its article links."""
    arts = conn.execute(
        "SELECT COUNT(*) FROM article_entities WHERE entity_id = ?", (entity_id,)
    ).fetchone()[0]
    
    if dry_run:
        print(f"  DELETE: [{entity_id}] '{name}' ({arts} article links)")
        return
    
    conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (entity_id,))
    conn.execute("DELETE FROM entities WHERE id = ?", (entity_id,))
    print(f"  ✓ DELETED: [{entity_id}] '{name}' ({arts} article links removed)")


def find_entity(conn, name, entity_type='person'):
    """Find entity by exact name."""
    row = conn.execute(
        "SELECT id, name FROM entities WHERE name = ? AND entity_type = ?",
        (name, entity_type)
    ).fetchone()
    return row


def find_entity_by_id(conn, eid):
    """Find entity by ID."""
    row = conn.execute(
        "SELECT id, name, entity_type FROM entities WHERE id = ?", (eid,)
    ).fetchone()
    return row


def get_article_count(conn, entity_id):
    """Get number of articles for an entity."""
    return conn.execute(
        "SELECT COUNT(*) FROM article_entities WHERE entity_id = ?", (entity_id,)
    ).fetchone()[0]


# ──────────────────────────────────────────────────────────────────────
# MERGE RULES
# ──────────────────────────────────────────────────────────────────────

# 1. Surname → Full name (unambiguous — only ONE full-name match)
#    Auto-detected: finds all single-word person entities that match
#    exactly one "Имя Фамилия" entity

# 2. Manual merge pairs for known ambiguous cases or special forms
MANUAL_MERGES = {
    # Morphological variants → canonical
    # "Куандыка Бишимбаев" → "Куандык Бишимбаев"
    # "Каната Бозумбаев" → "Канат Бозумбаев"  
    # "Илона Маска" → "Илон Маск"
    # "Куандыком Бишимбаев" → "Куандык Бишимбаев"
    # "Салтанат Нукеновой" → "Салтанат Нукенова" (if exists, or fix name)
    # "Шерзата" → "Шерзат" (need to check context)
    # "Экс-министр Бишимбаев" → "Куандык Бишимбаев"
    # "РККуандык Бишимбаев" → "Куандык Бишимбаев"
    # "Зампремьера Бозумбаев" → "Канат Бозумбаев"
    # "Мажилисмен Бакытжан Базарбек" → "Бакытжан Базарбек"
    # "Жомарт Токаев Айбек Смадияров" → split? delete?
    # These will be resolved by name below
}

# Entities to RECLASSIFY (person → org/location)
RECLASSIFY_TO_ORG = [
    'Акорды', 'Акорда', 'Антикора', 'Нацфонд', 'Казгидромет', 'Казком',
    'Миннацэкономики', 'Минторговли', 'Европульс',
]

RECLASSIFY_TO_LOCATION = [
    'Улытау', 'Мангистау', 'Нур-Султан',
    'Улытауской', 'Жетысуской', 'Абайской', 'Сырдарьинской',
    'Джизакской', 'Тобол-Торгайской', 'Туркестанской',
    'Жетісу',
]

# Entities to DELETE (broken NER, garbage)
DELETE_PATTERNS = [
    'Эксклюзив',
    'ЗРАБатр',
    'Премьер-Министромолжасомбектенов',
    'Премьер-Министрасерикажумангарин',
    'Масимоваи Премьер-Министрааскар Мамина',
    'Туркестанской Областимырзасеитов М.',
    'Восточно-Казахстанской Областиданиал Ахметов',
    'Экс-Министракуандыкабишимбаев',
    'Атырауской Областиайсулукельдыгалиев',
]

# Manual morphological merges: source_name → target_name
MORPH_MERGES = [
    ('Куандыка Бишимбаев', 'Куандык Бишимбаев'),
    ('Куандыком Бишимбаев', 'Куандык Бишимбаев'),
    ('Каната Бозумбаев', 'Канат Бозумбаев'),
    ('Илона Маска', 'Илон Маск'),
    ('Нукеновой', 'Салтанат Нукенова'),  # check if target exists
    ('Шерзата', 'Шерзат'),
    ('Экс-министр Бишимбаев', 'Куандык Бишимбаев'),
    ('РККуандык Бишимбаев', 'Куандык Бишимбаев'),
    ('Зампремьера Бозумбаев', 'Канат Бозумбаев'),
    ('Мажилисмен Бакытжан Базарбек', 'Бакытжан Базарбек'),
    ('Жомарт Токаев Айбек Смадияров', 'Айбек Смадияров'),
    ('Канат Бисембаевич Шарлапаев', 'Канат Шарлапаев'),
    ('Бахытжан Базарбек', 'Бакытжан Базарбек'),  # spelling variant
    ('Жаслана Мадиев', 'Жаслан Мадиев'),
    ('Маска', 'Илон Маск'),
]


def run_cleanup(dry_run=True):
    """Execute all cleanup operations."""
    
    if not dry_run:
        backup_db()
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    
    stats = {'merged': 0, 'reclassified': 0, 'deleted': 0, 'articles_moved': 0}
    
    print(f"\n{'='*60}")
    print(f"ENTITY CLEANUP {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
    print(f"{'='*60}")
    
    # ── Step 1: Delete garbage entries ────────────────────────────────
    print(f"\n── Step 1: Delete garbage entities ──")
    for name in DELETE_PATTERNS:
        row = find_entity(conn, name, 'person')
        if row:
            delete_entity(conn, row[0], row[1], dry_run)
            stats['deleted'] += 1
        else:
            # Try any entity_type
            row = conn.execute("SELECT id, name FROM entities WHERE name = ?", (name,)).fetchone()
            if row:
                delete_entity(conn, row[0], row[1], dry_run)
                stats['deleted'] += 1
    
    # Also delete all entities where name looks like broken NER (contains "Области" + lowercase)
    broken = conn.execute("""
        SELECT id, name FROM entities 
        WHERE entity_type = 'person' 
        AND (name LIKE '%Области%' OR name LIKE 'Премьер-Министр%' OR name LIKE 'Экс-Министр%')
        AND LENGTH(name) > 30
    """).fetchall()
    for row in broken:
        delete_entity(conn, row[0], row[1], dry_run)
        stats['deleted'] += 1
    
    if not dry_run:
        conn.commit()
    
    # ── Step 2: Reclassify mistyped entities ──────────────────────────
    print(f"\n── Step 2: Reclassify mistyped entities ──")
    for name in RECLASSIFY_TO_ORG:
        row = find_entity(conn, name, 'person')
        if row:
            reclassify_entity(conn, row[0], row[1], 'org', dry_run)
            stats['reclassified'] += 1
    
    for name in RECLASSIFY_TO_LOCATION:
        row = find_entity(conn, name, 'person')
        if row:
            reclassify_entity(conn, row[0], row[1], 'location', dry_run)
            stats['reclassified'] += 1
    
    if not dry_run:
        conn.commit()
    
    # ── Step 3: Manual morphological merges ───────────────────────────
    print(f"\n── Step 3: Morphological and manual merges ──")
    for source_name, target_name in MORPH_MERGES:
        source = find_entity(conn, source_name, 'person')
        target = find_entity(conn, target_name, 'person')
        if source and target:
            arts = merge_entities(conn, source[0], target[0], source[1], target[1], dry_run)
            stats['merged'] += 1
            stats['articles_moved'] += arts
        elif source and not target:
            print(f"  ⚠ Target not found: '{target_name}' (source '{source_name}' exists)")
        # else: source already merged or doesn't exist
    
    if not dry_run:
        conn.commit()
    
    # ── Step 4: Auto-merge single-word surnames → full names ──────────
    print(f"\n── Step 4: Auto-merge surnames → full names (unambiguous only) ──")
    
    # Find all single-word person names that have EXACTLY ONE full-name match
    # Filter out names that are clearly not surnames (orgs, locations, etc.)
    SKIP_SURNAMES = {
        'Антикора', 'Нацфонд', 'Улытау', 'Мангистау', 'Миннацэкономики',
        'Минторговли', 'Европульс', 'Акорды', 'Акорда', 'Казгидромет',
        'Казком', 'Нур-Султан', 'Жетісу', 'Аксу', 'Семей', 'Семея',
        'Кашаган', 'Диана', 'Александр', 'Екатерина', 'Майкл', 'Кайрат',
        'Карина', 'Шерзат', 'Шерзата', 'Гарри', 'Тайсон', 'Хави',
        'Блиновскую',  # morphological form, not surname
    }
    
    candidates = conn.execute("""
        SELECT s.id, s.name,
               (SELECT COUNT(*) FROM article_entities WHERE entity_id = s.id) as s_arts
        FROM entities s
        WHERE s.entity_type = 'person'
          AND s.name NOT LIKE '% %'
          AND LENGTH(s.name) > 3
        ORDER BY s_arts DESC
    """).fetchall()
    
    for surname_id, surname, s_arts in candidates:
        # Check entity still exists (might have been deleted/merged above)
        if not find_entity_by_id(conn, surname_id):
            continue
        
        # Skip known non-surname entries
        if surname in SKIP_SURNAMES:
            continue
            
        # Find all full-name matches — only proper "Имя Фамилия" patterns
        matches = conn.execute("""
            SELECT id, name, 
                   (SELECT COUNT(*) FROM article_entities WHERE entity_id = id) as arts
            FROM entities 
            WHERE entity_type = 'person' 
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
              AND id != ?
        """, (surname, surname_id)).fetchall()
        
        if len(matches) == 1:
            # Unambiguous: exactly one full name matches
            target_id, target_name, t_arts = matches[0]
            arts = merge_entities(conn, surname_id, target_id, surname, target_name, dry_run)
            stats['merged'] += 1
            stats['articles_moved'] += arts
        elif len(matches) > 1 and s_arts >= 5:
            # Ambiguous: pick the one with most articles as likely match
            # But only if it has >70% of total full-name articles
            total_fullname_arts = sum(m[2] for m in matches)
            best = max(matches, key=lambda m: m[2])
            if best[2] > total_fullname_arts * 0.7 and best[2] > 5:
                if dry_run:
                    alts = ', '.join(f"'{m[1]}'({m[2]})" for m in matches if m[0] != best[0])
                    print(f"  AMBIGUOUS but likely: '{surname}'({s_arts}) → '{best[1]}'({best[2]}) [also: {alts}]")
                # Don't auto-merge ambiguous in this pass
    
    if not dry_run:
        conn.commit()
    
    # ── Step 5: Clean up orphan entities (no articles) ────────────────
    print(f"\n── Step 5: Clean up orphan entities ──")
    orphans = conn.execute("""
        SELECT COUNT(*) FROM entities e
        WHERE NOT EXISTS (SELECT 1 FROM article_entities ae WHERE ae.entity_id = e.id)
    """).fetchone()[0]
    
    if not dry_run and orphans > 0:
        conn.execute("""
            DELETE FROM entities WHERE id NOT IN (SELECT DISTINCT entity_id FROM article_entities)
        """)
        conn.commit()
    print(f"  {'Would delete' if dry_run else '✓ Deleted'} {orphans} orphan entities (no article links)")
    stats['deleted'] += orphans
    
    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"SUMMARY {'(DRY RUN)' if dry_run else '(COMPLETED)'}")
    print(f"{'='*60}")
    print(f"  Entities merged:       {stats['merged']}")
    print(f"  Articles moved:        {stats['articles_moved']}")
    print(f"  Entities reclassified: {stats['reclassified']}")
    print(f"  Entities deleted:      {stats['deleted']}")
    
    remaining = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    remaining_persons = conn.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person'").fetchone()[0]
    single_word = conn.execute("""
        SELECT COUNT(*) FROM entities WHERE entity_type='person' AND name NOT LIKE '% %'
    """).fetchone()[0]
    
    print(f"\n  Entities remaining:    {remaining}")
    print(f"  Persons remaining:     {remaining_persons}")
    print(f"  Single-word persons:   {single_word}")
    
    conn.close()
    return stats


if __name__ == '__main__':
    dry = '--execute' not in sys.argv
    if dry:
        print("Running in DRY RUN mode. Use --execute to apply changes.")
    run_cleanup(dry_run=dry)
