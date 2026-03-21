"""
Production migration: normalize government entity names.
Works by name matching (not IDs), safe for any DB instance.
Run: docker compose exec app python scripts/migrate_prod_entities.py
"""
import sqlite3, sys

DB_PATH = "data/total.db"

# (search_patterns, canonical_name, short_name)
MAPPINGS = [
    (["МВД РК", "МВД Казахстана", "Министерство внутренних дел РК"], "Министерство внутренних дел РК", "МВД"),
    (["МИД РК", "МИД Казахстана", "Министерство иностранных дел РК"], "Министерство иностранных дел РК", "МИД"),
    (["МЧС РК", "МЧС Казахстана", "Министерство по чрезвычайным ситуациям РК"], "Министерство по чрезвычайным ситуациям РК", "МЧС"),
    (["Минобороны РК", "Минобороны", "Министерство обороны РК"], "Министерство обороны РК", "Минобороны"),
    (["КНБ РК", "КНБ Казахстана", "КНБ", "Комитет национальной безопасности РК"], "Комитет национальной безопасности РК", "КНБ"),
    (["Генеральная прокуратура РК", "Генпрокуратура РК", "Генпрокуратура"], "Генеральная прокуратура РК", "Генпрокуратура"),
    (["Верховный суд РК", "Верховный Суд РК"], "Верховный суд РК", "Верховный суд"),
    (["Антикор", "Агентство по противодействию коррупции РК"], "Агентство по противодействию коррупции РК", "Антикор"),
    (["АФМ", "Агентство по финансовому мониторингу РК"], "Агентство по финансовому мониторингу РК", "АФМ"),
    (["Минфин РК", "Минфин", "Министерство финансов РК"], "Министерство финансов РК", "Минфин"),
    (["Нацбанк РК", "Нацбанк", "Национальный банк РК", "Национальный Банк РК"], "Национальный банк РК", "Нацбанк"),
    (["Миннацэкономики", "Министерство национальной экономики РК", "МНЭ РК"], "Министерство национальной экономики РК", "Миннацэкономики"),
    (["Нацфонд РК", "Нацфонд", "Национальный фонд РК"], "Национальный фонд РК", "Нацфонд"),
    (["Минздрав РК", "Минздрав", "Министерство здравоохранения РК"], "Министерство здравоохранения РК", "Минздрав"),
    (["Минтруда", "Министерство труда и социальной защиты населения РК", "МТСЗН"], "Министерство труда и социальной защиты населения РК", "Минтруда"),
    (["Минпросвещения", "Министерство просвещения РК"], "Министерство просвещения РК", "Минпросвещения"),
    (["МНВО", "Министерство науки и высшего образования РК"], "Министерство науки и высшего образования РК", "МНВО"),
    (["Минсельхоз РК", "Минсельхоз", "Министерство сельского хозяйства РК"], "Министерство сельского хозяйства РК", "Минсельхоз"),
    (["Минэнерго РК", "Минэнерго", "Министерство энергетики РК"], "Министерство энергетики РК", "Минэнерго"),
    (["Минпром", "Министерство промышленности и строительства РК", "МИИР РК"], "Министерство промышленности и строительства РК", "Минпром"),
    (["Минкультуры", "Министерство культуры и информации РК", "МКИ РК"], "Министерство культуры и информации РК", "Минкультуры"),
    (["Минюст РК", "Минюст", "Министерство юстиции РК"], "Министерство юстиции РК", "Минюст"),
    (["Минтранс РК", "Минтранс", "Министерство транспорта РК"], "Министерство транспорта РК", "Минтранс"),
    (["Минэкологии", "Министерство экологии и природных ресурсов РК"], "Министерство экологии и природных ресурсов РК", "Минэкологии"),
    (["Минторговли", "Министерство торговли и интеграции РК", "МТИ РК"], "Министерство торговли и интеграции РК", "Минторговли"),
    (["Минводресурсов", "Министерство водных ресурсов и ирригации РК"], "Министерство водных ресурсов и ирригации РК", "Минводресурсов"),
    (["Минтуризма", "Министерство туризма и спорта РК"], "Министерство туризма и спорта РК", "Минтуризма"),
    (["Администрация Президента РК", "АП РК"], "Администрация Президента РК", "АП РК"),
    (["Служба центральных коммуникаций", "СЦК"], "Служба центральных коммуникаций", "СЦК"),
    (["МЦРИАП", "Министерство цифрового развития РК"], "Министерство цифрового развития РК", "МЦРИАП"),
    (["ВАП", "Высшая аудиторская палата РК"], "Высшая аудиторская палата РК", "ВАП"),
    (["Бюро нацстатистики", "Бюро национальной статистики РК", "БНС"], "Бюро национальной статистики РК", "Бюро нацстатистики"),
    (["АЗРК", "Агентство по защите и развитию конкуренции РК"], "Агентство по защите и развитию конкуренции РК", "АЗРК"),
    (["АРРФР", "Агентство по регулированию и развитию финансового рынка РК"], "Агентство по регулированию и развитию финансового рынка РК", "АРРФР"),
    (["Вооружённые силы РК", "ВС РК"], "Вооружённые силы РК", "ВС РК"),
]


def run():
    dry = "--dry-run" in sys.argv
    if dry:
        print("=== DRY RUN ===\n")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # Ensure column exists
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(entities)").fetchall()]
    if "short_name" not in cols:
        print("Adding short_name column...")
        if not dry:
            conn.execute("ALTER TABLE entities ADD COLUMN short_name TEXT")
            conn.commit()

    total_renamed = 0
    total_merged = 0

    for patterns, canonical, short in MAPPINGS:
        # Find all matching entities
        placeholders = ",".join("?" * len(patterns))
        rows = conn.execute(
            f"SELECT id, name FROM entities WHERE name IN ({placeholders})",
            patterns,
        ).fetchall()

        if not rows:
            print(f"  SKIP: no match for {canonical}")
            continue

        # Pick canonical: prefer exact match, else first by ID
        canon_row = None
        for r in rows:
            if r["name"] == canonical:
                canon_row = r
                break
        if not canon_row:
            canon_row = min(rows, key=lambda r: r["id"])  # oldest = most links

        canon_id = canon_row["id"]
        dups = [r for r in rows if r["id"] != canon_id]

        # Rename + set short_name
        if canon_row["name"] != canonical:
            print(f"  RENAME: {canon_row['name']} → {canonical}")
            total_renamed += 1
        if not dry:
            conn.execute(
                "UPDATE entities SET name=?, short_name=?, normalized=? WHERE id=?",
                (canonical, short, canonical.lower(), canon_id),
            )

        # Merge duplicates
        for dup in dups:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM article_entities WHERE entity_id=?",
                (dup["id"],),
            ).fetchone()[0]
            print(f"  MERGE: {dup['name']} (id={dup['id']}, {cnt} articles) → {canonical} (id={canon_id})")
            if not dry:
                conn.execute(
                    "INSERT OR IGNORE INTO article_entities (article_id, entity_id, mention_count) "
                    "SELECT article_id, ?, mention_count FROM article_entities WHERE entity_id=?",
                    (canon_id, dup["id"]),
                )
                conn.execute("DELETE FROM article_entities WHERE entity_id=?", (dup["id"],))
                conn.execute("DELETE FROM entities WHERE id=?", (dup["id"],))
            total_merged += 1

    # Clean garbage entities — ONLY parser concatenation bugs
    # Be precise: only match patterns like "МВДКуандык" (org+name stuck together)
    # or "сообщал" suffix stuck to names, or "window" suffix from JS bugs
    garbage = conn.execute("""
        SELECT id, name, (SELECT COUNT(*) FROM article_entities WHERE entity_id=e.id) as cnt
        FROM entities e
        WHERE name LIKE '%window'
          OR name LIKE '%поручение'
          OR name GLOB '*[А-Я][а-я]*сообщал*'
          OR name GLOB 'МВД[А-Я]*'
          OR name GLOB 'МИД[А-Я]*'
          OR name GLOB 'МЧС[А-Я]*'
          OR name GLOB 'СМИ[а-я]*'
          OR name GLOB 'РК[А-Я][а-я]*'
          OR name GLOB 'ЦИК[А-Я]*'
        ORDER BY cnt DESC
    """).fetchall()

    if garbage:
        print(f"\n  GARBAGE cleanup: {len(garbage)} entities")
        for g in garbage:
            clean = g["name"].replace("window", "").strip()
            # Try to find clean version
            clean_row = conn.execute("SELECT id FROM entities WHERE name=?", (clean,)).fetchone()
            if clean_row and not dry:
                conn.execute(
                    "INSERT OR IGNORE INTO article_entities (article_id, entity_id, mention_count) "
                    "SELECT article_id, ?, mention_count FROM article_entities WHERE entity_id=?",
                    (clean_row["id"], g["id"]),
                )
            if not dry:
                conn.execute("DELETE FROM article_entities WHERE entity_id=?", (g["id"],))
                conn.execute("DELETE FROM entities WHERE id=?", (g["id"],))
            print(f"    DEL: {g['name']} ({g['cnt']} articles)")

    if not dry:
        conn.commit()

    print(f"\n=== DONE: {total_renamed} renamed, {total_merged} merged, {len(garbage)} garbage cleaned ===")
    if dry:
        print("(dry run — no changes saved)")

    # Verify
    with_short = conn.execute(
        "SELECT COUNT(*) FROM entities WHERE short_name IS NOT NULL AND short_name != ''"
    ).fetchone()[0]
    print(f"Entities with short_name: {with_short}")
    conn.close()


if __name__ == "__main__":
    run()
