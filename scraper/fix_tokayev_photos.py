#!/usr/bin/env python3
"""
Fix Tokayev article photos:
1. Set image_credit = 'Фото: akorda.kz' where missing (for Tokayev articles)
2. Find duplicate photos across articles and replace with unique Akorda photos
   - Prefer full-res gallery photos (/uploadMedia/)
   - Fallback to cover photos (non-template, non-thumbnail)
3. Match articles to Akorda events by date, assign best photo per article

Usage:
    python fix_tokayev_photos.py --dry-run    # Preview changes only
    python fix_tokayev_photos.py --apply      # Apply changes
"""

import sqlite3
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

DB = Path(__file__).parent.parent / "data" / "total.db"
DRY_RUN = '--apply' not in sys.argv


def get_tokayev_articles(conn):
    rows = conn.execute("""
        SELECT id, title, main_image, image_credit, pub_date, thumbnail
        FROM articles 
        WHERE title LIKE '%Токаев%' OR title LIKE '%токаев%' 
            OR title LIKE '%Тоқаев%'
        ORDER BY pub_date DESC
    """).fetchall()
    return [{'id': r[0], 'title': r[1], 'main_image': r[2], 'image_credit': r[3], 
             'pub_date': r[4], 'thumbnail': r[5]} for r in rows]


def fix_credits(conn, articles):
    """Set image_credit for Tokayev articles that are missing it."""
    count = 0
    for a in articles:
        if a['image_credit'] and a['image_credit'].strip():
            continue
        if not a['main_image']:
            continue
        count += 1
        if not DRY_RUN:
            conn.execute("UPDATE articles SET image_credit = 'Фото: akorda.kz' WHERE id = ?", (a['id'],))
    return count


def find_duplicate_photos(articles):
    """Find articles sharing the same main_image."""
    img_to_articles = defaultdict(list)
    for a in articles:
        img = a['main_image']
        if img and img.strip():
            img_norm = img.replace('https://total.kz', '').strip()
            img_to_articles[img_norm].append(a)
    return {k: v for k, v in img_to_articles.items() if len(v) > 1}


def get_best_akorda_photo(conn, date_str, used_photos):
    """Get best available Akorda photo for a date (±2 days range).
    Prefers: full-res gallery > cover (non-template) > medium thumb
    Excludes: templates, already-used photos, _smallThumb, _miniThumb
    """
    try:
        d = datetime.strptime(date_str, '%Y-%m-%d')
    except:
        return None
    
    # Try exact date first, then expand ±1, ±2 days
    for delta in [0, -1, 1, -2, 2]:
        check_date = (d + timedelta(days=delta)).strftime('%Y-%m-%d')
        
        # Priority 1: Full-res gallery photos
        rows = conn.execute("""
            SELECT ap.id, ap.photo_url, ae.title 
            FROM akorda_photos ap
            JOIN akorda_events ae ON ae.id = ap.event_id
            WHERE ae.event_date = ? 
              AND ap.photo_url LIKE '%uploadMedia%'
              AND ap.used_in_article_id IS NULL
            ORDER BY ap.id
        """, (check_date,)).fetchall()
        
        for r in rows:
            if r[1] not in used_photos:
                return {'id': r[0], 'url': r[1], 'event_title': r[2], 'date': check_date}
        
        # Priority 2: Cover photos (non-template, non-thumb)
        rows = conn.execute("""
            SELECT ap.id, ap.photo_url, ae.title 
            FROM akorda_photos ap
            JOIN akorda_events ae ON ae.id = ap.event_id
            WHERE ae.event_date = ?
              AND ap.photo_url NOT LIKE '%template%'
              AND ap.photo_url NOT LIKE '%mediumThumb%'
              AND ap.photo_url NOT LIKE '%smallThumb%'
              AND ap.photo_url LIKE '%.jpg' 
              AND ap.used_in_article_id IS NULL
            ORDER BY ap.is_cover DESC, ap.id
        """, (check_date,)).fetchall()
        
        for r in rows:
            if r[1] not in used_photos:
                return {'id': r[0], 'url': r[1], 'event_title': r[2], 'date': check_date}
    
    return None


def resolve_duplicates(conn, articles, dupes):
    """For each duplicate group, assign unique Akorda photos where possible."""
    changes = []
    used_photos = set()
    
    for img_norm, group in sorted(dupes.items(), key=lambda x: -len(x[1])):
        if len(group) < 2:
            continue
        
        # Sort by date (newest first) — newest keeps its photo
        group_sorted = sorted(group, key=lambda a: a.get('pub_date', '') or '', reverse=True)
        keeper = group_sorted[0]
        
        for a in group_sorted[1:]:
            date = a['pub_date'][:10] if a.get('pub_date') else ''
            if not date:
                continue
            
            photo = get_best_akorda_photo(conn, date, used_photos)
            if photo:
                used_photos.add(photo['url'])
                changes.append({
                    'article_id': a['id'],
                    'article_title': a['title'][:60],
                    'article_date': date,
                    'old_image': a['main_image'],
                    'new_image': photo['url'],
                    'akorda_photo_id': photo['id'],
                    'akorda_event': photo['event_title'][:50],
                    'reason': f"Duplicate (group of {len(group)}, keep newest [{keeper['id']}])"
                })
                
                if not DRY_RUN:
                    conn.execute("UPDATE akorda_photos SET used_in_article_id = ? WHERE id = ?",
                                (a['id'], photo['id']))
    
    return changes


def main():
    mode = "DRY RUN" if DRY_RUN else "APPLYING CHANGES"
    print(f"=== {mode} ===\n")
    
    conn = sqlite3.connect(str(DB))
    
    try:
        articles = get_tokayev_articles(conn)
        print(f"Tokayev articles: {len(articles)}")
        
        # 1. Fix credits
        credit_fixes = fix_credits(conn, articles)
        print(f"\n1. КОПИРАЙТЫ: {credit_fixes} статей получат 'Фото: akorda.kz'")
        
        # 2. Find duplicates
        dupes = find_duplicate_photos(articles)
        dupe_articles = sum(len(v) for v in dupes.values())
        print(f"\n2. ДУБЛИ: {len(dupes)} уникальных фото повторяются в {dupe_articles} статьях")
        
        for img, group in sorted(dupes.items(), key=lambda x: -len(x[1]))[:5]:
            print(f"  {len(group)}x | {group[0]['title'][:55]}")
            for a in group[1:3]:
                print(f"       └─ [{a['id']}] {a['title'][:50]}")
            if len(group) > 3:
                print(f"       └─ ... и ещё {len(group)-3}")
        
        # 3. Resolve duplicates
        changes = resolve_duplicates(conn, articles, dupes)
        print(f"\n3. ЗАМЕНЫ: {len(changes)} фото можно заменить на оригиналы с Акорды")
        
        full_res = sum(1 for c in changes if 'uploadMedia' in c['new_image'])
        print(f"   Из них полноразмерные (uploadMedia): {full_res}")
        
        for c in changes[:10]:
            is_full = '✓' if 'uploadMedia' in c['new_image'] else '○'
            print(f"  {is_full} [{c['article_id']}] {c['article_title']}")
            print(f"    Дата: {c['article_date']} | Акорда: {c['akorda_event']}")
        
        if not DRY_RUN:
            for c in changes:
                conn.execute("""
                    UPDATE articles SET main_image = ?, image_credit = 'Фото: akorda.kz'
                    WHERE id = ?
                """, (c['new_image'], c['article_id']))
            conn.commit()
            print(f"\n✓ Применено: {credit_fixes} копирайтов + {len(changes)} замен фото")
        else:
            print(f"\n→ Готово к применению: {credit_fixes} копирайтов + {len(changes)} замен")
            print("  Запустите с --apply для применения")
        
        # Overall picture
        print("\n=== ИТОГО ===")
        all_no_credit = sum(1 for a in articles if not (a['image_credit'] or '').strip() and a['main_image'])
        print(f"Статей без копирайта: {all_no_credit} → {max(0, all_no_credit - credit_fixes)} после")
        print(f"Групп дублей: {len(dupes)}")
        print(f"Можно заменить с Акорды: {len(changes)}")
        print(f"Осталось дублей (нет фото Акорды): {len(dupes) - len(set(c['article_id'] for c in changes))}")
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()
