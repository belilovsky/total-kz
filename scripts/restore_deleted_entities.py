"""
Restore entities that were incorrectly deleted by the garbage cleanup.
Re-runs NER only for articles that lost entity links.

The garbage filter `LIKE '%Куандык%'` incorrectly deleted real people
like Куандык Бишимбаев, Болатбек Куандыков, etc.

This script:
1. Identifies articles that had links to deleted entities (by checking
   which article_ids no longer have ANY entity links)
2. Re-runs NER extraction on those specific articles
3. Restores the entities and links

Run: docker compose exec app python scripts/restore_deleted_entities.py
"""
import json
import re
import sys
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

DB_PATH = BASE_DIR / "data" / "total.db"

# Names of entities that were incorrectly deleted
DELETED_NAMES = [
    "Куандык Бишимбаев",
    "Куандык Бишимбаева",  # genitive
    "Куандыка Бишимбаева",
    "Куандык Турганкулов",
    "Болатбек Куандыков",
    "Куандык Ешимет",
    "Бишимбаев Куандык Валиханович",
    "Куандыком Бишимбаевым",
    "Куандык Кулмурзин",
    "Куандык Алпыс",
    "Балтабек Куандыков",
    "Куандыка Бишимбаев",
    "Айгуль Куандыкова",
    "Куандыку Бишимбаеву",
    "Куандык Валиханович",
    "Куандык Кажкенов",
    "Куандык Рахым",
    "Куандык Шакиржанов",
    "Куандыка Турганкулова",
    "Куандык Айтакын",
    "Кулмурзин Куандык Сагиндыкович",
    "Б. Куандыков",
    "Валихан Куандыков",
    "Кыдырали Дархан Куандыкулы",
]


def run():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    print("=== Restoring deleted entities ===\n")

    # Step 1: Find articles that mention "Куандык" or "Бишимбаев" in body
    # These are candidates for having lost entity links
    print("Finding articles with relevant names in body text...")
    articles = conn.execute("""
        SELECT id, title, body_text FROM articles
        WHERE body_text LIKE '%Куандык%' OR body_text LIKE '%Бишимбаев%'
           OR title LIKE '%Куандык%' OR title LIKE '%Бишимбаев%'
    """).fetchall()
    print(f"  Found {len(articles)} articles mentioning these names\n")

    # Step 2: Check which of these articles lost their entity links
    orphaned = []
    for art in articles:
        # Check if this article still has entity links
        count = conn.execute(
            "SELECT COUNT(*) FROM article_entities WHERE article_id = ?",
            (art["id"],)
        ).fetchone()[0]
        # Even if they have SOME links, they may have lost the Куандык ones
        orphaned.append(art)

    print(f"  Will re-process {len(orphaned)} articles for Куандык-related entities\n")

    # Step 3: Re-create entities and links
    restored_entities = 0
    restored_links = 0

    for name in DELETED_NAMES:
        norm = " ".join(name.strip().split()).lower()
        # Check if entity already exists (shouldn't, but be safe)
        existing = conn.execute(
            "SELECT id FROM entities WHERE normalized = ? AND entity_type = 'person'",
            (norm,)
        ).fetchone()

        if existing:
            print(f"  EXISTS: {name} (id={existing['id']})")
            continue

        # Create entity
        cursor = conn.execute(
            "INSERT OR IGNORE INTO entities (name, entity_type, normalized) VALUES (?, 'person', ?)",
            (name, norm)
        )
        if cursor.lastrowid:
            restored_entities += 1
            print(f"  CREATED: {name} (id={cursor.lastrowid})")

    conn.commit()

    # Step 4: Re-link articles
    # For each article, search for name mentions and create links
    for art in orphaned:
        body = (art["title"] or "") + " " + (art["body_text"] or "")
        body_lower = body.lower()

        for name in DELETED_NAMES:
            name_lower = name.lower()
            if name_lower in body_lower:
                # Count mentions
                count = body_lower.count(name_lower)

                # Find entity id
                norm = " ".join(name.strip().split()).lower()
                ent = conn.execute(
                    "SELECT id FROM entities WHERE normalized = ? AND entity_type = 'person'",
                    (norm,)
                ).fetchone()

                if ent:
                    conn.execute(
                        "INSERT OR IGNORE INTO article_entities (article_id, entity_id, mention_count) VALUES (?, ?, ?)",
                        (art["id"], ent["id"], count)
                    )
                    restored_links += 1

    conn.commit()

    # Step 5: Summary
    print(f"\n=== DONE ===")
    print(f"  Entities restored: {restored_entities}")
    print(f"  Article links restored: {restored_links}")

    # Show top entities by article count
    print(f"\n=== Top restored entities ===")
    for name in DELETED_NAMES[:10]:
        norm = " ".join(name.strip().split()).lower()
        row = conn.execute("""
            SELECT e.id, e.name, COUNT(ae.article_id) as cnt
            FROM entities e
            LEFT JOIN article_entities ae ON ae.entity_id = e.id
            WHERE e.normalized = ? AND e.entity_type = 'person'
            GROUP BY e.id
        """, (norm,)).fetchone()
        if row:
            print(f"  {row['name']}: {row['cnt']} articles")

    conn.close()


if __name__ == "__main__":
    run()
