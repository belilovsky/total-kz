#!/usr/bin/env python3
"""
Typography migration for total.kz:
1. Replace all em-dash (–, U+2014) with en-dash (–, U+2013)
2. Fix nested quotation marks:
   - Outer level: «ёлочки» (guillemets)
   - Inner level (nested inside «»): "лапки" (U+201C / U+201D)
   
Usage:
  python scripts/migrate_typography.py          # dry-run (show stats only)
  python scripts/migrate_typography.py --apply  # apply changes to DB
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "total.db"

MDASH = "\u2014"  # –
NDASH = "\u2013"  # –
LEFT_GUILL = "«"
RIGHT_GUILL = "»"
LEFT_DOUBLE = "\u201c"   # "
RIGHT_DOUBLE = "\u201d"  # "


def fix_dashes(text: str) -> str:
    """Replace all em-dashes with en-dashes."""
    return text.replace(MDASH, NDASH)


def fix_quotes(text: str) -> str:
    """Fix nested quotation marks.
    
    Rules:
    - Outer quotes stay as «ёлочки»
    - Inner quotes (nested «» inside outer «») become \u201cлапки\u201d
    - Straight quotes \" inside «» are converted to \u201cлапки\u201d (paired)
    """
    if LEFT_GUILL not in text:
        return text
    
    # Step 1: Convert nested «» to \u201c\u201d
    result = []
    depth = 0
    i = 0
    
    while i < len(text):
        ch = text[i]
        
        if ch == LEFT_GUILL:
            depth += 1
            if depth == 1:
                result.append(LEFT_GUILL)
            else:
                result.append(LEFT_DOUBLE)
            i += 1
            
        elif ch == RIGHT_GUILL:
            if depth > 1:
                result.append(RIGHT_DOUBLE)
                depth -= 1
            elif depth == 1:
                result.append(RIGHT_GUILL)
                depth -= 1
            else:
                result.append(ch)
            i += 1
            
        else:
            result.append(ch)
            i += 1
    
    text = "".join(result)
    
    # Step 2: Convert paired straight quotes inside «» to \u201c\u201d
    # Find all «...» spans and convert " pairs inside them
    if '"' in text:
        text = _fix_straight_quotes_in_guillemets(text)
    
    return text


def _fix_straight_quotes_in_guillemets(text: str) -> str:
    """Convert paired straight quotes inside «» to \u201c\u201d."""
    result = []
    depth = 0
    straight_open = False  # tracks if we're inside an opening straight quote
    i = 0
    
    while i < len(text):
        ch = text[i]
        
        if ch == LEFT_GUILL:
            depth += 1
            result.append(ch)
        elif ch == RIGHT_GUILL:
            depth = max(0, depth - 1)
            result.append(ch)
        elif ch == '"' and depth >= 1:
            if not straight_open:
                result.append(LEFT_DOUBLE)
                straight_open = True
            else:
                result.append(RIGHT_DOUBLE)
                straight_open = False
        else:
            result.append(ch)
        i += 1
    
    return "".join(result)


def fix_html_quotes(html: str) -> str:
    """Fix quotes in HTML body while preserving tags and attributes.
    
    We only process text nodes, not attribute values.
    """
    if LEFT_GUILL not in html and '"' not in html:
        return fix_dashes(html)  # just dashes
    
    # Split HTML into tags and text segments
    # Process only text segments for quote fixing
    parts = re.split(r'(<[^>]+>)', html)
    result = []
    
    for part in parts:
        if part.startswith('<'):
            # HTML tag – only fix dashes, leave quotes in attributes alone
            result.append(fix_dashes(part))
        else:
            # Text node – fix both dashes and quotes
            result.append(fix_quotes(fix_dashes(part)))
    
    return "".join(result)


def process_text(text: str) -> str:
    """Process plain text: fix dashes and quotes."""
    if not text:
        return text
    text = fix_dashes(text)
    text = fix_quotes(text)
    return text


def process_html(html: str) -> str:
    """Process HTML: fix dashes and quotes (only in text nodes)."""
    if not html:
        return html
    return fix_html_quotes(html)


def test_suite():
    """Run tests to verify the transformations."""
    print("=== RUNNING TESTS ===\n")
    
    tests = [
        # (input, expected, description)
        
        # Dash tests
        (f"реформы затронут все сферы {MDASH} от законов", 
         f"реформы затронут все сферы {NDASH} от законов",
         "mdash → ndash in running text"),
        
        (f"вод», {MDASH} говорится в сообщении",
         f"вод», {NDASH} говорится в сообщении",
         "mdash → ndash after quote attribution"),
        
        # Quote tests – nested guillemets
        ("«Пиррова победа»",
         "«Пиррова победа»",
         "single-level «» stays as is"),
        
        ("«выражение «Пиррова победа» это»",
         "«выражение \u201cПиррова победа\u201d это»",
         "nested «» → "" inside"),
        
        ("«внешний «средний «глубокий» уровень» конец»",
         "«внешний \u201cсредний \u201cглубокий\u201d уровень\u201d конец»",
         "triple nested – all inner become """),
         
        # Mixed: dash + quotes
        (f"«тенденция набирает обороты и «под шумок» пытаются», {MDASH} заявил",
         f"«тенденция набирает обороты и \u201cпод шумок\u201d пытаются», {NDASH} заявил",
         "combined dash + nested quotes"),
         
        # Straight quotes inside guillemets
        ('«компания "Рога и Копыта" объявила»',
         '«компания \u201cРога и Копыта\u201d объявила»',
         'straight " inside «» → ""'),

        # No guillemets – leave straight quotes alone
        ('Он сказал "привет"',
         'Он сказал "привет"',
         'straight " without «» – untouched'),
        
        # Edge: empty
        ("", "", "empty string"),
        
        # Edge: no special chars
        ("просто текст", "просто текст", "plain text"),
    ]
    
    passed = 0
    failed = 0
    
    for inp, expected, desc in tests:
        result = process_text(inp)
        if result == expected:
            print(f"  ✓ {desc}")
            passed += 1
        else:
            print(f"  ✗ {desc}")
            print(f"    input:    {repr(inp)}")
            print(f"    expected: {repr(expected)}")
            print(f"    got:      {repr(result)}")
            failed += 1
    
    # HTML test
    html_in = f'<p>«текст «вложенный» конец» {MDASH} автор</p>'
    html_exp = f'<p>«текст \u201cвложенный\u201d конец» {NDASH} автор</p>'
    html_out = process_html(html_in)
    if html_out == html_exp:
        print(f"  ✓ HTML: nested quotes in tags + mdash")
        passed += 1
    else:
        print(f"  ✗ HTML: nested quotes in tags + mdash")
        print(f"    expected: {repr(html_exp)}")
        print(f"    got:      {repr(html_out)}")
        failed += 1
    
    # HTML test: don't break attributes
    html_in2 = '<a href="https://example.com" class="link">«текст «внутри»»</a>'
    html_out2 = process_html(html_in2)
    assert 'href="https://example.com"' in html_out2, f"Attribute broken: {html_out2}"
    assert 'class="link"' in html_out2, f"Attribute broken: {html_out2}"
    print(f"  ✓ HTML: attributes preserved")
    passed += 1
    
    print(f"\n  Results: {passed} passed, {failed} failed\n")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="Typography migration for total.kz")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--skip-tests", action="store_true", help="Skip test suite")
    args = parser.parse_args()
    
    if not args.skip_tests:
        if not test_suite():
            print("Tests failed! Aborting.")
            sys.exit(1)
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    # Stats before
    print("=== PRE-MIGRATION STATS ===")
    mdash_text = conn.execute(f"SELECT COUNT(*) FROM articles WHERE body_text LIKE '%{MDASH}%'").fetchone()[0]
    mdash_html = conn.execute(f"SELECT COUNT(*) FROM articles WHERE body_html LIKE '%{MDASH}%'").fetchone()[0]
    mdash_title = conn.execute(f"SELECT COUNT(*) FROM articles WHERE title LIKE '%{MDASH}%'").fetchone()[0]
    nested_q = conn.execute(f"SELECT COUNT(*) FROM articles WHERE body_text LIKE '%{LEFT_GUILL}%{LEFT_GUILL}%{RIGHT_GUILL}%{RIGHT_GUILL}%'").fetchone()[0]
    
    print(f"  Articles with mdash in body_text: {mdash_text}")
    print(f"  Articles with mdash in body_html: {mdash_html}")
    print(f"  Articles with mdash in title: {mdash_title}")
    print(f"  Articles with nested «« »» guillemets: {nested_q}")
    
    if not args.apply:
        print("\n  DRY RUN – no changes made. Use --apply to execute.\n")
        
        # Show some examples of what would change
        print("=== SAMPLE TRANSFORMATIONS ===")
        rows = conn.execute(f"""
            SELECT id, title, substr(body_text, 1, 500) as body_sample
            FROM articles 
            WHERE body_text LIKE '%{LEFT_GUILL}%{LEFT_GUILL}%{RIGHT_GUILL}%{RIGHT_GUILL}%'
            LIMIT 5
        """).fetchall()
        
        for r in rows:
            body = r["body_sample"]
            new_body = process_text(body)
            if body != new_body:
                # Find first difference
                for j in range(min(len(body), len(new_body))):
                    if body[j] != new_body[j]:
                        start = max(0, j - 30)
                        end = min(len(body), j + 50)
                        print(f"\n  ID {r['id']}:")
                        print(f"    BEFORE: ...{body[start:end]}...")
                        end2 = min(len(new_body), j + 50)
                        print(f"    AFTER:  ...{new_body[start:end2]}...")
                        break
        
        conn.close()
        return
    
    # Apply changes
    print("\n=== APPLYING MIGRATION ===")
    
    rows = conn.execute("SELECT id, title, body_text, body_html, excerpt FROM articles").fetchall()
    
    updated = 0
    for r in rows:
        art_id = r["id"]
        new_title = process_text(r["title"] or "")
        new_body_text = process_text(r["body_text"] or "")
        new_body_html = process_html(r["body_html"] or "")
        new_excerpt = process_text(r["excerpt"] or "")
        
        changed = (
            new_title != (r["title"] or "") or
            new_body_text != (r["body_text"] or "") or
            new_body_html != (r["body_html"] or "") or
            new_excerpt != (r["excerpt"] or "")
        )
        
        if changed:
            conn.execute("""
                UPDATE articles 
                SET title = ?, body_text = ?, body_html = ?, excerpt = ?
                WHERE id = ?
            """, (new_title, new_body_text, new_body_html, new_excerpt, art_id))
            updated += 1
    
    conn.commit()
    
    # Post-migration stats
    print(f"\n  Updated {updated} articles out of {len(rows)}")
    
    print("\n=== POST-MIGRATION STATS ===")
    mdash_text = conn.execute(f"SELECT COUNT(*) FROM articles WHERE body_text LIKE '%{MDASH}%'").fetchone()[0]
    mdash_html = conn.execute(f"SELECT COUNT(*) FROM articles WHERE body_html LIKE '%{MDASH}%'").fetchone()[0]
    mdash_title = conn.execute(f"SELECT COUNT(*) FROM articles WHERE title LIKE '%{MDASH}%'").fetchone()[0]
    nested_q = conn.execute(f"SELECT COUNT(*) FROM articles WHERE body_text LIKE '%{LEFT_GUILL}%{LEFT_GUILL}%{RIGHT_GUILL}%{RIGHT_GUILL}%'").fetchone()[0]
    ndash_text = conn.execute(f"SELECT COUNT(*) FROM articles WHERE body_text LIKE '%{NDASH}%'").fetchone()[0]
    
    print(f"  Articles with mdash remaining in body_text: {mdash_text}")
    print(f"  Articles with mdash remaining in body_html: {mdash_html}")
    print(f"  Articles with mdash remaining in title: {mdash_title}")
    print(f"  Articles with nested «« »» remaining: {nested_q}")
    print(f"  Articles with ndash in body_text: {ndash_text}")
    
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
