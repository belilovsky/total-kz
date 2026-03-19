"""Normalize author names — fix typos, merge duplicates.

Run: python scraper/normalize_authors.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "total.db"

# ── Canonical author names ──────────────────────────────────
# Mapping: wrong_name → correct_name
# We keep only single-author entries for normalization.
# Multi-author entries (e.g. "Тулеубек Габбасов, Айнур Коскина") are left as-is.

AUTHOR_MAP = {
    # Диас Калиакпаров — all typo variants
    "Диас Каликпаров": "Диас Калиакпаров",
    "Диас Клиакпаров": "Диас Калиакпаров",
    "Диас Калиапаров": "Диас Калиакпаров",
    "Диас Кадиакпаров": "Диас Калиакпаров",
    "Диас Калиаакпаров": "Диас Калиакпаров",
    "Диас Калиакпров": "Диас Калиакпаров",
    "Диас  Калиакпаров": "Диас Калиакпаров",  # double space
    "Диас Кааликпаров": "Диас Калиакпаров",
    "Диас Калиакапаров": "Диас Калиакпаров",
    "Диас Калиакпаро": "Диас Калиакпаров",     # truncated
    "Диас Калиакпарова": "Диас Калиакпаров",   # wrong ending
    "Диас Каликапаров": "Диас Калиакпаров",
    "Диас Каликпров": "Диас Калиакпаров",
    "Дис Калиакпаров": "Диас Калиакпаров",     # missing letter

    # Тулеубек Габбасов — all typo variants
    "Тулеубек  Габбасов": "Тулеубек Габбасов",  # double space
    "Тулеубек Габбассов": "Тулеубек Габбасов",  # extra с
    "Туелеубек Габбасов": "Тулеубек Габбасов",
    "Туелубек Габбасов": "Тулеубек Габбасов",
    "Тулеубек Габббасов": "Тулеубек Габбасов",  # triple б
    "Тулебек Габбасов": "Тулеубек Габбасов",    # missing у
    "Тулеубек Габбасо": "Тулеубек Габбасов",    # truncated
    "Тулеубек Габбсов": "Тулеубек Габбасов",    # missing а
    "Тулеуебек Габбасов": "Тулеубек Габбасов",
    "Тулеуьбек Габбасов": "Тулеубек Габбасов",  # ь instead of nothing
    "Тулуекбек Габбасов": "Тулеубек Габбасов",
    "Туоеубек Габбасов": "Тулеубек Габбасов",   # о instead of л
}


def normalize():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print(f"Database: {DB_PATH}")
    print(f"Mapping: {len(AUTHOR_MAP)} typo variants → correct names\n")

    total_updated = 0
    for wrong, correct in AUTHOR_MAP.items():
        cursor.execute(
            "UPDATE articles SET author = ? WHERE author = ?",
            (correct, wrong),
        )
        count = cursor.rowcount
        if count > 0:
            print(f"  {wrong!r:40s} → {correct!r:30s}  ({count} статей)")
            total_updated += count

    conn.commit()
    print(f"\nГотово: обновлено {total_updated} статей")

    # Show final author list
    authors = cursor.execute("""
        SELECT author, COUNT(*) as cnt
        FROM articles
        WHERE author IS NOT NULL AND author != ''
        GROUP BY author ORDER BY cnt DESC
    """).fetchall()
    print(f"\nАвторы после нормализации ({len(authors)}):")
    for a, c in authors:
        print(f"  [{c:5d}] {a}")

    conn.close()


if __name__ == "__main__":
    normalize()
