#!/usr/bin/env python3
"""
Smart story clustering: merge single-article stories that share entities.

Algorithm:
  1. Load all stories with their articles' entity IDs
  2. Stories sharing 3+ entity IDs → auto-merge (same topic)
  3. Stories sharing 2 entities + same category → auto-merge
  4. For remaining single-article stories, assign to existing multi-article
     stories if they share 2+ entities
  5. Delete empty stories after merge
  6. Recalculate article_count, first_date, last_date

Merge logic:
  - Keep story with the most articles as "parent"
  - Move all article_stories links from child → parent
  - Delete child stories

Usage:
    python scripts/cluster_stories.py                 # dry-run
    python scripts/cluster_stories.py --apply         # apply merges
    python scripts/cluster_stories.py --apply --verbose
"""
import argparse
import os
import sys
from collections import defaultdict
from itertools import combinations

import psycopg2
import psycopg2.extras

PG_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)


# ── Data Loading ────────────────────────────────────────────────


def load_story_entities(cur):
    """Load entity_ids grouped by story_id, plus article categories.

    Returns:
        story_entities: {story_id: set(entity_ids)}
        story_info: {story_id: {title, article_count, article_ids}}
        story_categories: {story_id: set(categories)}
    """
    # Story metadata
    cur.execute("""
        SELECT s.id, s.title_ru, s.article_count,
               array_agg(DISTINCT ast.article_id) AS article_ids
        FROM stories s
        JOIN article_stories ast ON ast.story_id = s.id
        GROUP BY s.id
    """)
    story_info = {}
    for row in cur.fetchall():
        story_info[row["id"]] = {
            "title": row["title_ru"],
            "article_count": row["article_count"] or 0,
            "article_ids": set(row["article_ids"] or []),
        }

    # Entity IDs per story (via article_stories → article_entities)
    cur.execute("""
        SELECT ast.story_id, ae.entity_id
        FROM article_stories ast
        JOIN article_entities ae ON ae.article_id = ast.article_id
    """)
    story_entities = defaultdict(set)
    for row in cur.fetchall():
        story_entities[row["story_id"]].add(row["entity_id"])

    # Categories per story (via articles)
    cur.execute("""
        SELECT ast.story_id, a.sub_category
        FROM article_stories ast
        JOIN articles a ON a.id = ast.article_id
        WHERE a.sub_category IS NOT NULL AND a.sub_category != ''
    """)
    story_categories = defaultdict(set)
    for row in cur.fetchall():
        story_categories[row["story_id"]].add(row["sub_category"])

    return story_entities, story_info, story_categories


# ── Clustering ──────────────────────────────────────────────────


def find_merge_groups(story_entities, story_info, story_categories, verbose=False):
    """Find groups of stories that should be merged.

    Rules:
      - 3+ shared entities → auto-merge
      - 2 shared entities + overlapping category → auto-merge

    Returns list of sets, each set = group of story_ids to merge.
    """
    # Only consider stories that have entities
    story_ids = [sid for sid in story_info if story_entities.get(sid)]

    # Build adjacency: which stories should merge?
    merge_edges = []  # (story_a, story_b)

    for sid_a, sid_b in combinations(story_ids, 2):
        ents_a = story_entities[sid_a]
        ents_b = story_entities[sid_b]
        shared = ents_a & ents_b

        if len(shared) >= 3:
            merge_edges.append((sid_a, sid_b))
            if verbose:
                print(f"  3+ entities: story {sid_a} + {sid_b} "
                      f"({len(shared)} shared)")
        elif len(shared) >= 2:
            cats_a = story_categories.get(sid_a, set())
            cats_b = story_categories.get(sid_b, set())
            if cats_a & cats_b:
                merge_edges.append((sid_a, sid_b))
                if verbose:
                    print(f"  2 entities + category: story {sid_a} + {sid_b} "
                          f"(shared cats: {cats_a & cats_b})")

    # Build connected components from edges (union-find)
    parent = {}

    def find(x):
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for a, b in merge_edges:
        union(a, b)

    # Collect groups
    groups = defaultdict(set)
    for sid in story_ids:
        root = find(sid)
        if root in parent or sid in parent:
            groups[find(sid)].add(sid)

    # Only return groups with 2+ stories
    return [g for g in groups.values() if len(g) >= 2]


# ── Merge Execution ─────────────────────────────────────────────


def merge_story_group(cur, group, story_info, dry_run=True, verbose=False):
    """Merge a group of stories: keep the one with the most articles.

    Returns (keep_id, removed_ids, articles_moved).
    """
    # Sort by article_count descending to find the parent
    ranked = sorted(group, key=lambda sid: story_info.get(sid, {}).get("article_count", 0),
                    reverse=True)
    keep_id = ranked[0]
    remove_ids = ranked[1:]

    keep_title = story_info.get(keep_id, {}).get("title", "?")
    total_moved = 0

    if verbose or dry_run:
        print(f"  MERGE → keep story {keep_id} ({keep_title[:60]})")
        for rid in remove_ids:
            rtitle = story_info.get(rid, {}).get("title", "?")
            rcount = story_info.get(rid, {}).get("article_count", 0)
            print(f"    absorb story {rid} ({rtitle[:50]}) [{rcount} articles]")

    if not dry_run:
        for rid in remove_ids:
            # Move article links that don't already exist on parent
            cur.execute("""
                UPDATE article_stories SET story_id = %s
                WHERE story_id = %s
                  AND article_id NOT IN (
                      SELECT article_id FROM article_stories WHERE story_id = %s
                  )
            """, (keep_id, rid, keep_id))
            total_moved += cur.rowcount

            # Delete remaining links (duplicates)
            cur.execute("DELETE FROM article_stories WHERE story_id = %s", (rid,))

            # Delete the child story
            cur.execute("DELETE FROM stories WHERE id = %s", (rid,))

    return keep_id, remove_ids, total_moved


# ── Assign orphan articles to existing stories ──────────────────


def assign_orphans_to_stories(cur, story_entities, story_info, dry_run=True, verbose=False):
    """For articles not in any story, check if they share 2+ entities
    with an existing multi-article story. If so, add them.

    Returns count of articles assigned.
    """
    # Find articles not linked to any story
    cur.execute("""
        SELECT a.id, a.sub_category,
               array_agg(ae.entity_id) AS entity_ids
        FROM articles a
        JOIN article_entities ae ON ae.article_id = a.id
        LEFT JOIN article_stories ast ON ast.article_id = a.id
        WHERE ast.article_id IS NULL
        GROUP BY a.id
    """)
    orphan_articles = []
    for row in cur.fetchall():
        orphan_articles.append({
            "id": row["id"],
            "category": row["sub_category"] or "",
            "entity_ids": set(row["entity_ids"] or []),
        })

    if verbose:
        print(f"  Orphan articles (not in any story): {len(orphan_articles)}")

    # Refresh story_entities from DB for current state
    cur.execute("""
        SELECT ast.story_id, array_agg(DISTINCT ae.entity_id) AS entity_ids
        FROM article_stories ast
        JOIN article_entities ae ON ae.article_id = ast.article_id
        GROUP BY ast.story_id
    """)
    current_story_ents = {}
    for row in cur.fetchall():
        current_story_ents[row["story_id"]] = set(row["entity_ids"] or [])

    assigned = 0
    for art in orphan_articles:
        if not art["entity_ids"]:
            continue

        best_story = None
        best_overlap = 0

        for sid, s_ents in current_story_ents.items():
            overlap = len(art["entity_ids"] & s_ents)
            if overlap >= 2 and overlap > best_overlap:
                best_overlap = overlap
                best_story = sid

        if best_story:
            if verbose:
                print(f"    article {art['id']} → story {best_story} "
                      f"({best_overlap} shared entities)")
            if not dry_run:
                cur.execute("""
                    INSERT INTO article_stories (article_id, story_id, confidence)
                    VALUES (%s, %s, 0.8)
                    ON CONFLICT (article_id, story_id) DO NOTHING
                """, (art["id"], best_story))
            assigned += 1

    return assigned


# ── Auto-create stories for new articles ────────────────────────


def auto_assign_new_articles(cur, dry_run=True, verbose=False):
    """For new articles with entities but no story:
    - If they share 2+ entities with an existing story → assign
    - Otherwise → create a new story from article title

    This is the function called from cron_pipeline.sh.
    Returns (assigned, created).
    """
    # Articles with entities but no story
    cur.execute("""
        SELECT a.id, a.title, a.sub_category, a.pub_date,
               array_agg(ae.entity_id) AS entity_ids
        FROM articles a
        JOIN article_entities ae ON ae.article_id = a.id
        LEFT JOIN article_stories ast ON ast.article_id = a.id
        WHERE ast.article_id IS NULL
        GROUP BY a.id
    """)
    new_articles = cur.fetchall()

    if not new_articles:
        print("  No unassigned articles with entities.")
        return 0, 0

    # Current story entities
    cur.execute("""
        SELECT ast.story_id, array_agg(DISTINCT ae.entity_id) AS entity_ids
        FROM article_stories ast
        JOIN article_entities ae ON ae.article_id = ast.article_id
        GROUP BY ast.story_id
    """)
    story_ent_map = {}
    for row in cur.fetchall():
        story_ent_map[row["story_id"]] = set(row["entity_ids"] or [])

    assigned = 0
    created = 0

    for art in new_articles:
        art_entities = set(art["entity_ids"] or [])
        if not art_entities:
            continue

        # Find best matching story
        best_story = None
        best_overlap = 0
        for sid, s_ents in story_ent_map.items():
            overlap = len(art_entities & s_ents)
            if overlap >= 2 and overlap > best_overlap:
                best_overlap = overlap
                best_story = sid

        if best_story:
            if verbose:
                print(f"    article {art['id']} → existing story {best_story} "
                      f"({best_overlap} shared entities)")
            if not dry_run:
                cur.execute("""
                    INSERT INTO article_stories (article_id, story_id, confidence)
                    VALUES (%s, %s, 0.8)
                    ON CONFLICT (article_id, story_id) DO NOTHING
                """, (art["id"], best_story))
            assigned += 1
        else:
            # Create new story from article title
            title = art["title"] or "Без названия"
            slug = _make_slug(title, art["id"])
            if verbose:
                print(f"    article {art['id']} → NEW story: {title[:60]}")
            if not dry_run:
                cur.execute("""
                    INSERT INTO stories (slug, title_ru, article_count, first_date, last_date)
                    VALUES (%s, %s, 1, %s, %s)
                    RETURNING id
                """, (slug, title, art["pub_date"], art["pub_date"]))
                new_story_id = cur.fetchone()["id"]
                cur.execute("""
                    INSERT INTO article_stories (article_id, story_id, confidence)
                    VALUES (%s, %s, 0.7)
                    ON CONFLICT (article_id, story_id) DO NOTHING
                """, (art["id"], new_story_id))
                # Track for subsequent articles in same batch
                story_ent_map[new_story_id] = art_entities
            created += 1

    return assigned, created


def _make_slug(title: str, article_id: int) -> str:
    """Generate a URL-safe slug from a Russian title."""
    import re
    # Simple transliteration for slug
    translit_map = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
        'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k',
        'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r',
        'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
        'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '',
        'э': 'e', 'ю': 'yu', 'я': 'ya',
    }
    slug = title.lower().strip()
    result = []
    for ch in slug:
        if ch in translit_map:
            result.append(translit_map[ch])
        elif ch.isascii() and ch.isalnum():
            result.append(ch)
        elif ch in (' ', '-', '_'):
            result.append('-')
    slug = '-'.join(filter(None, ''.join(result).split('-')))
    # Truncate and add article_id for uniqueness
    slug = slug[:80]
    return f"{slug}-{article_id}"


# ── Stats Recalculation ─────────────────────────────────────────


def recalculate_story_stats(cur):
    """Recalculate article_count, first_date, last_date for all stories."""
    cur.execute("""
        UPDATE stories s SET
            article_count = sub.cnt,
            first_date = sub.min_date,
            last_date = sub.max_date
        FROM (
            SELECT ast.story_id,
                   COUNT(*) AS cnt,
                   MIN(a.pub_date) AS min_date,
                   MAX(a.pub_date) AS max_date
            FROM article_stories ast
            JOIN articles a ON a.id = ast.article_id
            GROUP BY ast.story_id
        ) sub
        WHERE s.id = sub.story_id
    """)
    return cur.rowcount


def delete_empty_stories(cur):
    """Delete stories with no articles linked."""
    cur.execute("""
        DELETE FROM stories
        WHERE id NOT IN (SELECT DISTINCT story_id FROM article_stories)
    """)
    return cur.rowcount


# ── Main ────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Smart story clustering via entity overlap"
    )
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Show what would be done (default)")
    parser.add_argument("--apply", action="store_true",
                        help="Apply changes to the database")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print detailed merge decisions")
    parser.add_argument("--auto-assign", action="store_true",
                        help="Auto-assign new articles to stories (for pipeline)")
    args = parser.parse_args()

    dry_run = not args.apply

    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # ── Baseline stats ──
    cur.execute("SELECT COUNT(*) AS total FROM stories")
    total_before = cur.fetchone()["total"]
    cur.execute("SELECT COUNT(*) AS cnt FROM stories WHERE article_count <= 1")
    singles_before = cur.fetchone()["cnt"]
    print(f"Before: {total_before} stories, {singles_before} with ≤1 article")
    print()

    # ── Auto-assign mode (for pipeline) ──
    if args.auto_assign:
        print("=== Auto-assigning new articles to stories ===")
        assigned, created = auto_assign_new_articles(
            cur, dry_run=dry_run, verbose=args.verbose
        )
        print(f"  Assigned to existing: {assigned}")
        print(f"  New stories created: {created}")

        if not dry_run:
            recalculate_story_stats(cur)
            deleted = delete_empty_stories(cur)
            if deleted:
                print(f"  Cleaned up {deleted} empty stories")
            conn.commit()
            print("  Changes committed.")
        else:
            conn.rollback()
            print("  Dry-run — no changes made.")
        cur.close()
        conn.close()
        return

    # ── Step 1: Load data ──
    print("=== Step 1: Loading story entities ===")
    story_entities, story_info, story_categories = load_story_entities(cur)
    stories_with_ents = len([s for s in story_info if story_entities.get(s)])
    print(f"  Stories with entities: {stories_with_ents}")
    print()

    # ── Step 2: Find merge groups ──
    print("=== Step 2: Finding merge groups (entity overlap) ===")
    groups = find_merge_groups(
        story_entities, story_info, story_categories, verbose=args.verbose
    )
    total_merges = sum(len(g) - 1 for g in groups)
    print(f"  Merge groups found: {len(groups)} ({total_merges} stories to absorb)")
    print()

    # ── Step 3: Execute merges ──
    if groups:
        print("=== Step 3: Merging stories ===")
        total_removed = 0
        total_articles_moved = 0
        for i, group in enumerate(groups):
            if args.verbose or dry_run:
                print(f"\n  Group {i+1}/{len(groups)} ({len(group)} stories):")
            keep_id, removed, moved = merge_story_group(
                cur, group, story_info, dry_run=dry_run, verbose=args.verbose
            )
            total_removed += len(removed)
            total_articles_moved += moved

        print(f"\n  Stories absorbed: {total_removed}")
        if not dry_run:
            print(f"  Articles moved: {total_articles_moved}")
    else:
        print("No merge groups found — stories are already well-separated.")

    # ── Step 4: Assign orphan articles ──
    print()
    print("=== Step 4: Assigning orphan articles to stories ===")
    assigned = assign_orphans_to_stories(
        cur, story_entities, story_info, dry_run=dry_run, verbose=args.verbose
    )
    print(f"  Orphan articles assigned: {assigned}")

    # ── Step 5: Cleanup ──
    print()
    print("=== Step 5: Cleanup ===")
    if not dry_run:
        updated = recalculate_story_stats(cur)
        print(f"  Recalculated stats for {updated} stories")
        deleted = delete_empty_stories(cur)
        print(f"  Deleted {deleted} empty stories")
        conn.commit()
        print("  All changes committed.")
    else:
        conn.rollback()
        print("  Dry-run — no changes made. Run with --apply to execute.")

    # ── Final stats ──
    cur.execute("SELECT COUNT(*) AS total FROM stories")
    total_after = cur.fetchone()["total"]
    cur.execute("SELECT COUNT(*) AS cnt FROM stories WHERE article_count > 1")
    multi_after = cur.fetchone()["cnt"]
    print(f"\nAfter: {total_after} stories, {multi_after} with 2+ articles")

    if dry_run:
        reduction = total_merges
        print(f"Estimated reduction: ~{reduction} fewer stories after merge")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
