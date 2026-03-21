"""
Normalize government entity names in total.kz database.
1. Add short_name column to entities table
2. Update name to proper full official names
3. Set short_name for compact display
4. Merge duplicates (re-link article_entities, delete old entity)
5. Clean up garbage entries (parser bugs like "МВДКуандык Алпыс")
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "total.db"

# ─── CANONICAL GOVERNMENT ENTITIES ───
# Format: (current_id, new_name, short_name, merge_ids_into_this)
# merge_ids = list of entity IDs whose article links should be re-pointed to current_id
GOV_ENTITIES = [
    # Силовые
    (22690, "Министерство внутренних дел РК", "МВД", [40137, 47414, 39196]),
    (22704, "Министерство иностранных дел РК", "МИД", [41005]),
    (22907, "Министерство по чрезвычайным ситуациям РК", "МЧС", [32272, 38662, 41031]),
    (22844, "Министерство обороны РК", "Минобороны", []),
    (23232, "Комитет национальной безопасности РК", "КНБ", [44633]),
    (22815, "Вооружённые силы РК", "ВС РК", []),
    
    # Правоохранительные / суд
    (22725, "Генеральная прокуратура РК", "Генпрокуратура", [38362]),
    (23120, "Верховный суд РК", "Верховный суд", []),
    (25082, "Агентство по противодействию коррупции РК", "Антикор", [27060, 30226, 41010]),
    (23316, "Агентство по финансовому мониторингу РК", "АФМ", []),
    
    # Финансы / экономика
    (22838, "Министерство финансов РК", "Минфин", [27854]),
    (29046, "Национальный банк РК", "Нацбанк", [37718]),
    (24274, "Министерство национальной экономики РК", "Миннацэкономики", [25418, 25704]),
    (23579, "Национальный фонд РК", "Нацфонд", [42013, 41823, 28166]),
    
    # Социальные
    (22870, "Министерство здравоохранения РК", "Минздрав", [24961]),
    (23025, "Министерство труда и социальной защиты населения РК", "Минтруда", [23026]),
    (23038, "Министерство просвещения РК", "Минпросвещения", []),
    (24385, "Министерство науки и высшего образования РК", "МНВО", [27579]),
    
    # Отраслевые
    (23438, "Министерство сельского хозяйства РК", "Минсельхоз", [39938]),
    (23252, "Министерство энергетики РК", "Минэнерго", []),
    (22931, "Министерство промышленности и строительства РК", "Минпром", [22928, 39688]),
    (23921, "Министерство культуры и информации РК", "Минкультуры", [24253]),
    (23935, "Министерство юстиции РК", "Минюст", []),
    (22916, "Министерство транспорта РК", "Минтранс", []),
    (23044, "Министерство экологии и природных ресурсов РК", "Минэкологии", []),
    (26147, "Министерство торговли и интеграции РК", "Минторговли", [23441, 30523]),
    (22642, "Министерство водных ресурсов и ирригации РК", "Минводресурсов", []),
    (23505, "Министерство туризма и спорта РК", "Минтуризма", []),
    
    # Прочие госструктуры
    (23530, "Администрация Президента РК", "АП РК", []),
    (23511, "Служба центральных коммуникаций", "СЦК", []),
    (23938, "Министерство цифрового развития РК", "МЦРИАП", []),
    (23936, "Высшая аудиторская палата РК", "ВАП", [39919]),
    (23295, "Бюро национальной статистики РК", "Бюро нацстатистики", [42145]),
    (24432, "Агентство по защите и развитию конкуренции РК", "АЗРК", [27965, 41756, 42738]),
    (25600, "Агентство по регулированию и развитию финансового рынка РК", "АРРФР", [24353, 25603]),
]


def run_migration(dry_run=False):
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    
    # Step 1: Add short_name column if not exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(entities)").fetchall()]
    if "short_name" not in cols:
        print("Adding short_name column to entities table...")
        if not dry_run:
            conn.execute("ALTER TABLE entities ADD COLUMN short_name TEXT")
            conn.commit()
        print("  Done.")
    else:
        print("short_name column already exists.")
    
    total_merged = 0
    total_renamed = 0
    total_deleted = 0
    
    for canonical_id, new_name, short_name, merge_ids in GOV_ENTITIES:
        # Check if canonical entity exists
        row = conn.execute("SELECT id, name FROM entities WHERE id = ?", (canonical_id,)).fetchone()
        if not row:
            print(f"  WARNING: Canonical entity {canonical_id} not found, skipping.")
            continue
        
        old_name = row[1]
        
        # Step 2: Rename canonical entity
        if old_name != new_name:
            print(f"  RENAME: {old_name} → {new_name}")
            if not dry_run:
                conn.execute(
                    "UPDATE entities SET name = ?, short_name = ?, normalized = ? WHERE id = ?",
                    (new_name, short_name, new_name.lower(), canonical_id)
                )
            total_renamed += 1
        else:
            # Just set short_name
            if not dry_run:
                conn.execute("UPDATE entities SET short_name = ? WHERE id = ?", (short_name, canonical_id))
        
        # Step 3: Merge duplicates
        for dup_id in merge_ids:
            dup_row = conn.execute("SELECT id, name FROM entities WHERE id = ?", (dup_id,)).fetchone()
            if not dup_row:
                continue
            
            dup_article_count = conn.execute(
                "SELECT count(*) FROM article_entities WHERE entity_id = ?", (dup_id,)
            ).fetchone()[0]
            
            print(f"  MERGE: {dup_row[1]} (id={dup_id}, {dup_article_count} articles) → {new_name} (id={canonical_id})")
            
            if not dry_run:
                # Re-link articles: update entity_id, ignore conflicts (article already linked to canonical)
                conn.execute("""
                    INSERT OR IGNORE INTO article_entities (article_id, entity_id)
                    SELECT article_id, ? FROM article_entities WHERE entity_id = ?
                """, (canonical_id, dup_id))
                
                # Delete old links
                conn.execute("DELETE FROM article_entities WHERE entity_id = ?", (dup_id,))
                
                # Delete old entity
                conn.execute("DELETE FROM entities WHERE id = ?", (dup_id,))
            
            total_merged += dup_article_count
            total_deleted += 1
    
    if not dry_run:
        conn.commit()
    
    # Step 4: Report garbage entities that should be reviewed
    print("\n=== GARBAGE ENTITIES (parser bugs, likely need manual review) ===")
    garbage = conn.execute("""
        SELECT e.id, e.name, count(ae.article_id) as cnt
        FROM entities e
        LEFT JOIN article_entities ae ON ae.entity_id = e.id
        WHERE e.entity_type = 'org'
        AND (e.name LIKE '%window%' OR e.name LIKE '%сообщал%' OR e.name LIKE '%поручение%'
             OR e.name GLOB '*[A-Z][A-Z][A-Z][А-Я]*' OR e.name GLOB '*[А-Я][а-я][a-z]*')
        GROUP BY e.id
        ORDER BY cnt DESC
    """).fetchall()
    for g in garbage:
        print(f"  {g[0]:>6} | {g[1]:50s} | {g[2]} articles")
    
    conn.close()
    
    print(f"\n=== SUMMARY ===")
    print(f"  Renamed: {total_renamed}")
    print(f"  Merged: {total_deleted} duplicate entities ({total_merged} article links reassigned)")
    print(f"  {'DRY RUN — no changes saved' if dry_run else 'Changes committed.'}")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
    if dry_run:
        print("=== DRY RUN MODE ===\n")
    run_migration(dry_run=dry_run)
