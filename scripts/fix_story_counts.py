#!/usr/bin/env python3
"""
Recalculate stories.article_count from article_stories and clean up stale stories.

Actions:
  1. Recalculate article_count for all stories
  2. Delete stories with 0 articles (orphaned)
  3. Optionally merge duplicate stories (same/very similar title_ru)

Usage:
    python scripts/fix_story_counts.py                    # dry-run
    python scripts/fix_story_counts.py --apply            # apply changes
    python scripts/fix_story_counts.py --apply --merge    # also merge duplicates
"""
import argparse
import os
import re
import sys

import psycopg2
import psycopg2.extras

PG_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)


def normalize_title(title: str) -> str:
    """Normalize a title for fuzzy matching: lowercase, strip punctuation, collapse spaces."""
    t = title.lower().strip()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t)
    return t


def main():
    parser = argparse.ArgumentParser(description="Fix story article_count & cleanup")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--merge", action="store_true", help="Also merge duplicate stories")
    args = parser.parse_args()

    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # ── Step 1: Recalculate article_count ──
    print("Step 1: Recalculating article_count for all stories...")
    cur.execute("""
        SELECT s.id, s.title_ru, s.article_count AS old_count,
               COALESCE(ac.cnt, 0) AS real_count
        FROM stories s
        LEFT JOIN (
            SELECT story_id, COUNT(*) AS cnt
            FROM article_stories
            GROUP BY story_id
        ) ac ON ac.story_id = s.id
    """)
    rows = cur.fetchall()

    mismatched = [(r["id"], r["old_count"], r["real_count"]) for r in rows if r["old_count"] != r["real_count"]]
    print(f"  Total stories: {len(rows)}")
    print(f"  Mismatched counts: {len(mismatched)}")

    if mismatched and args.apply:
        cur.execute("""
            UPDATE stories s SET article_count = (
                SELECT COUNT(*) FROM article_stories ast WHERE ast.story_id = s.id
            )
        """)
        print(f"  Updated {len(mismatched)} story counts")

    # ── Step 2: Delete stories with 0 articles ──
    print("\nStep 2: Finding stories with 0 articles...")
    zero_stories = [r for r in rows if r["real_count"] == 0]
    print(f"  Stories with 0 articles: {len(zero_stories)}")

    if zero_stories and args.apply:
        zero_ids = [r["id"] for r in zero_stories]
        cur.execute("DELETE FROM article_stories WHERE story_id = ANY(%s)", (zero_ids,))
        cur.execute("DELETE FROM stories WHERE id = ANY(%s)", (zero_ids,))
        print(f"  Deleted {len(zero_ids)} orphaned stories")

    single_stories = [r for r in rows if r["real_count"] == 1]
    print(f"  Stories with 1 article: {len(single_stories)} (kept — may grow)")

    # ── Step 3: Merge duplicates ──
    if args.merge:
        print("\nStep 3: Finding duplicate stories by similar title...")
        cur.execute("SELECT id, title_ru FROM stories ORDER BY id")
        all_stories = cur.fetchall()

        groups = {}
        for s in all_stories:
            key = normalize_title(s["title_ru"])
            groups.setdefault(key, []).append(s["id"])

        dupes = {k: v for k, v in groups.items() if len(v) > 1}
        print(f"  Duplicate groups found: {len(dupes)}")

        merged = 0
        for norm_title, ids in dupes.items():
            # Keep the story with the most articles
            cur.execute("""
                SELECT s.id, COALESCE(ac.cnt, 0) AS cnt
                FROM stories s
                LEFT JOIN (
                    SELECT story_id, COUNT(*) AS cnt FROM article_stories GROUP BY story_id
                ) ac ON ac.story_id = s.id
                WHERE s.id = ANY(%s)
                ORDER BY cnt DESC
            """, (ids,))
            ranked = cur.fetchall()
            keep_id = ranked[0]["id"]
            remove_ids = [r["id"] for r in ranked[1:]]

            if args.apply:
                # Move article links to the keeper
                for rid in remove_ids:
                    cur.execute("""
                        UPDATE article_stories SET story_id = %s
                        WHERE story_id = %s
                          AND article_id NOT IN (
                              SELECT article_id FROM article_stories WHERE story_id = %s
                          )
                    """, (keep_id, rid, keep_id))
                    cur.execute("DELETE FROM article_stories WHERE story_id = %s", (rid,))
                    cur.execute("DELETE FROM stories WHERE id = %s", (rid,))
                merged += len(remove_ids)

        if args.apply:
            print(f"  Merged {merged} duplicate stories")
            # Recalculate counts after merge
            cur.execute("""
                UPDATE stories s SET article_count = (
                    SELECT COUNT(*) FROM article_stories ast WHERE ast.story_id = s.id
                )
            """)
            print("  Recalculated counts after merge")
        else:
            total_removable = sum(len(v) - 1 for v in dupes.values())
            print(f"  Would merge {total_removable} duplicate stories (dry-run)")
            for norm_title, ids in list(dupes.items())[:5]:
                print(f"    '{norm_title[:60]}' — {len(ids)} copies")

    if args.apply:
        conn.commit()
        print("\nAll changes committed.")
    else:
        conn.rollback()
        print("\nDry-run complete. Run with --apply to execute changes.")

    # ── Summary ──
    cur.execute("SELECT COUNT(*) FROM stories")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM stories WHERE article_count > 1")
    multi = cur.fetchone()[0]
    print(f"\nCurrent state: {total} stories, {multi} with 2+ articles")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
