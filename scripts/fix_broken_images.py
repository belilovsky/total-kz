#!/usr/bin/env python3
"""
Fix broken article images in PostgreSQL.

Finds articles where main_image is NULL, empty, or malformed (doesn't start
with 'http'), and copies a valid thumbnail URL into main_image where available.

Usage:
    python scripts/fix_broken_images.py              # dry-run (default)
    python scripts/fix_broken_images.py --apply       # actually update rows
"""
import argparse
import os
import sys

import psycopg2
import psycopg2.extras

PG_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)


def main():
    parser = argparse.ArgumentParser(description="Fix broken article images")
    parser.add_argument("--apply", action="store_true", help="Apply fixes (default is dry-run)")
    args = parser.parse_args()

    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Find articles with broken main_image
    cur.execute("""
        SELECT id, title, main_image, thumbnail
        FROM articles
        WHERE (main_image IS NULL OR main_image = '' OR main_image NOT LIKE 'http%%')
          AND status = 'published'
        ORDER BY id
    """)
    broken = cur.fetchall()
    print(f"Found {len(broken)} articles with missing/broken main_image")

    fixable = []
    unfixable = []

    for row in broken:
        thumb = row["thumbnail"]
        if thumb and thumb.startswith("http"):
            fixable.append(row)
        else:
            unfixable.append(row)

    print(f"  Fixable (thumbnail available): {len(fixable)}")
    print(f"  Unfixable (no valid thumbnail): {len(unfixable)}")

    if fixable and args.apply:
        print("\nApplying fixes...")
        for row in fixable:
            cur.execute(
                "UPDATE articles SET main_image = %s WHERE id = %s",
                (row["thumbnail"], row["id"]),
            )
        conn.commit()
        print(f"Fixed {len(fixable)} articles (copied thumbnail -> main_image)")
    elif fixable:
        print("\nDry-run mode. Sample fixable articles:")
        for row in fixable[:10]:
            print(f"  id={row['id']}: thumb={row['thumbnail'][:80]}...")
        print("Run with --apply to update the database.")

    if unfixable:
        print(f"\nUnfixable articles (first 10):")
        for row in unfixable[:10]:
            print(f"  id={row['id']}: {row['title'][:60]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
