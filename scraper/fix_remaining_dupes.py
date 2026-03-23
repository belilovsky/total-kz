#!/usr/bin/env python3
"""
Fix remaining duplicate photos across ALL Tokayev articles.
For older articles where exact date match isn't possible,
use any available unused Akorda photo.

Usage:
    python fix_remaining_dupes.py --dry-run
    python fix_remaining_dupes.py --apply
"""

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

DB = Path(__file__).parent.parent / "data" / "total.db"
DRY_RUN = '--apply' not in sys.argv


def main():
    mode = "DRY RUN" if DRY_RUN else "APPLYING"
    print(f"=== {mode} ===\n")
    
    conn = sqlite3.connect(str(DB))
    
    # Get all Tokayev articles with duplicate images
    dupes_raw = conn.execute("""
        SELECT main_image, COUNT(*) as cnt
        FROM articles 
        WHERE (title LIKE '%Токаев%' OR title LIKE '%токаев%' OR title LIKE '%Тоқаев%')
          AND main_image IS NOT NULL AND main_image != ''
        GROUP BY main_image HAVING cnt > 1
        ORDER BY cnt DESC
    """).fetchall()
    
    print(f"Duplicate image groups: {len(dupes_raw)}")
    total_dupes = sum(r[1] for r in dupes_raw)
    print(f"Total articles with duplicate images: {total_dupes}")
    
    # Get pool of unused full-res Akorda photos
    pool = conn.execute("""
        SELECT ap.id, ap.photo_url, ae.event_date, ae.title
        FROM akorda_photos ap
        JOIN akorda_events ae ON ae.id = ap.event_id
        WHERE ap.photo_url LIKE '%uploadMedia%'
          AND ap.used_in_article_id IS NULL
        ORDER BY ae.event_date DESC
    """).fetchall()
    print(f"Available Akorda photos pool: {len(pool)}")
    
    pool_idx = 0
    changes = []
    
    for img, cnt in dupes_raw:
        # Get all articles with this image
        articles = conn.execute("""
            SELECT id, title, pub_date FROM articles 
            WHERE main_image = ? 
            ORDER BY pub_date DESC
        """, (img,)).fetchall()
        
        # Keep newest article's photo, replace the rest
        keeper = articles[0]
        
        for a in articles[1:]:
            art_id, art_title, art_date = a
            date_str = art_date[:10] if art_date else ''
            
            # Try to find Akorda photo for same date first
            best = None
            if date_str:
                try:
                    d = datetime.strptime(date_str, '%Y-%m-%d')
                    for delta in [0, -1, 1, -2, 2]:
                        check = (d + timedelta(days=delta)).strftime('%Y-%m-%d')
                        row = conn.execute("""
                            SELECT ap.id, ap.photo_url, ae.title
                            FROM akorda_photos ap
                            JOIN akorda_events ae ON ae.id = ap.event_id
                            WHERE ae.event_date = ?
                              AND ap.photo_url LIKE '%uploadMedia%'
                              AND ap.used_in_article_id IS NULL
                            LIMIT 1
                        """, (check,)).fetchone()
                        if row:
                            best = row
                            break
                except:
                    pass
            
            # Fallback: take any photo from pool
            if not best:
                while pool_idx < len(pool):
                    p = pool[pool_idx]
                    pool_idx += 1
                    # Verify still unused
                    check = conn.execute("SELECT used_in_article_id FROM akorda_photos WHERE id = ?", (p[0],)).fetchone()
                    if check and check[0] is None:
                        best = (p[0], p[1], p[3])
                        break
            
            if best:
                changes.append({
                    'article_id': art_id,
                    'article_title': art_title[:55],
                    'date': date_str,
                    'old_image': img[:70],
                    'new_image': best[1],
                    'akorda_id': best[0],
                    'akorda_event': best[2][:50] if best[2] else '',
                    'group_size': cnt,
                    'keeper_id': keeper[0]
                })
                
                if not DRY_RUN:
                    conn.execute("UPDATE akorda_photos SET used_in_article_id = ? WHERE id = ?",
                                (art_id, best[0]))
    
    print(f"\nReplacements ready: {len(changes)}")
    
    # Show changes
    for c in changes[:20]:
        is_full = '✓' if 'uploadMedia' in c['new_image'] else '○'
        print(f"  {is_full} [{c['article_id']}] {c['date']} | {c['article_title']}")
        print(f"    Группа: {c['group_size']}x, оставляем [{c['keeper_id']}]")
        if c['akorda_event']:
            print(f"    Акорда: {c['akorda_event']}")
    
    if len(changes) > 20:
        print(f"  ... и ещё {len(changes) - 20}")
    
    if not DRY_RUN:
        for c in changes:
            conn.execute("""
                UPDATE articles SET main_image = ?, image_credit = 'Фото: akorda.kz'
                WHERE id = ?
            """, (c['new_image'], c['article_id']))
        conn.commit()
        print(f"\n✓ Применено: {len(changes)} замен фото")
    else:
        print(f"\n→ Готово: {len(changes)} замен. Запустите с --apply")
    
    # Verify: any remaining dupes?
    remaining = conn.execute("""
        SELECT main_image, COUNT(*) as cnt
        FROM articles 
        WHERE (title LIKE '%Токаев%' OR title LIKE '%токаев%')
          AND main_image IS NOT NULL AND main_image != ''
        GROUP BY main_image HAVING cnt > 1
    """).fetchall()
    
    if DRY_RUN:
        # In dry run, subtract expected fixes
        fixed_articles = set(c['article_id'] for c in changes)
        # Re-check
        still_duped = 0
        for img, cnt in remaining:
            arts = conn.execute("SELECT id FROM articles WHERE main_image = ?", (img,)).fetchall()
            unfixed = [a[0] for a in arts if a[0] not in fixed_articles]
            if len(unfixed) > 1:
                still_duped += 1
        print(f"\nПосле применения останется дублей: ~{still_duped}")
    else:
        print(f"\nОставшиеся группы дублей: {len(remaining)}")
    
    conn.close()


if __name__ == '__main__':
    main()
