#!/usr/bin/env python3
"""
Content cleanup & SEO polish for all articles in PostgreSQL.

Four phases, all SQL/regex-based (no API calls):
  Phase 1: Clean body_html — remove comments, script/style/iframe, adfox, &nbsp;, entities, whitespace
  Phase 2: Clean titles — double spaces, strip, remove "total.kz"
  Phase 3: Fix excerpts — generate from body_text if missing, trim long ones
  Phase 4: SEO polish — use enrichment.summary as excerpt where available

Idempotent — safe to run multiple times.
Run: docker exec total_kz_app python scripts/content_cleanup.py
"""

import os
import re
import sys
import time

PG_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)

BATCH = 1000
PROGRESS_EVERY = 5000


def get_conn():
    import psycopg2
    return psycopg2.connect(PG_URL)


# ── Phase 1: Clean body_html ─────────────────────────────


def phase1_clean_body_html(conn):
    """Remove HTML comments, script/style/iframe, adfox, &nbsp;, decode entities, collapse whitespace."""
    cur = conn.cursor()
    results = {}

    # 1a. Remove HTML comments <!-- ... --> from body_html
    cur.execute("""
        UPDATE articles
        SET body_html = regexp_replace(body_html, '<!--.*?-->', '', 'gs')
        WHERE body_html ~ '<!--.*?-->'
    """)
    results["html_comments"] = cur.rowcount
    conn.commit()
    print(f"  1a. HTML comments removed: {results['html_comments']}")

    # 1b. Remove <script>...</script> tags and contents
    cur.execute("""
        UPDATE articles
        SET body_html = regexp_replace(body_html, '<script[^>]*>.*?</script>', '', 'gsi')
        WHERE body_html ~* '<script'
    """)
    results["script_tags"] = cur.rowcount
    conn.commit()
    print(f"  1b. Script tags removed: {results['script_tags']}")

    # 1c. Remove <style>...</style> tags and contents
    cur.execute("""
        UPDATE articles
        SET body_html = regexp_replace(body_html, '<style[^>]*>.*?</style>', '', 'gsi')
        WHERE body_html ~* '<style'
    """)
    results["style_tags"] = cur.rowcount
    conn.commit()
    print(f"  1c. Style tags removed: {results['style_tags']}")

    # 1d. Remove <iframe>...</iframe> tags (including self-closing)
    cur.execute("""
        UPDATE articles
        SET body_html = regexp_replace(body_html, '<iframe[^>]*>.*?</iframe>', '', 'gsi')
        WHERE body_html ~* '<iframe'
    """)
    results["iframe_tags"] = cur.rowcount
    conn.commit()
    print(f"  1d. Iframe tags removed: {results['iframe_tags']}")

    # 1e. Remove adfox div containers: <div class="...adfox...">...</div>
    cur.execute("""
        UPDATE articles
        SET body_html = regexp_replace(body_html, '<div[^>]*(?:adfox|adserver)[^>]*>.*?</div>', '', 'gsi')
        WHERE body_html ~* 'adfox|adserver'
    """)
    results["adfox_ads"] = cur.rowcount
    conn.commit()
    print(f"  1e. Adfox/adserver divs removed: {results['adfox_ads']}")

    # 1f. Replace &nbsp; with regular space in body_html
    cur.execute("""
        UPDATE articles
        SET body_html = replace(body_html, '&nbsp;', ' ')
        WHERE body_html LIKE '%&nbsp;%'
    """)
    results["nbsp_html"] = cur.rowcount
    conn.commit()
    print(f"  1f. &nbsp; replaced in body_html: {results['nbsp_html']}")

    # 1g. Decode HTML entities in body_text: &amp; -> &, &lt; -> <, &gt; -> >, &quot; -> "
    cur.execute("""
        UPDATE articles
        SET body_text = replace(replace(replace(replace(replace(
            body_text,
            '&amp;', '&'),
            '&lt;', '<'),
            '&gt;', '>'),
            '&quot;', '"'),
            '&nbsp;', ' ')
        WHERE body_text IS NOT NULL
          AND (body_text LIKE '%&amp;%'
            OR body_text LIKE '%&lt;%'
            OR body_text LIKE '%&gt;%'
            OR body_text LIKE '%&quot;%'
            OR body_text LIKE '%&nbsp;%')
    """)
    results["entities_text"] = cur.rowcount
    conn.commit()
    print(f"  1g. HTML entities decoded in body_text: {results['entities_text']}")

    # 1h. Collapse multiple whitespace in body_text (multiple spaces -> single)
    cur.execute("""
        UPDATE articles
        SET body_text = regexp_replace(body_text, '[ \t]{2,}', ' ', 'g')
        WHERE body_text ~ '[ \t]{2,}'
    """)
    results["whitespace_collapsed"] = cur.rowcount
    conn.commit()
    print(f"  1h. Whitespace collapsed in body_text: {results['whitespace_collapsed']}")

    # 1i. Strip leading/trailing whitespace from body_text
    cur.execute("""
        UPDATE articles
        SET body_text = btrim(body_text)
        WHERE body_text IS NOT NULL
          AND (body_text != btrim(body_text))
    """)
    results["trimmed_text"] = cur.rowcount
    conn.commit()
    print(f"  1i. Body_text trimmed: {results['trimmed_text']}")

    cur.close()
    return results


# ── Phase 2: Clean titles ────────────────────────────────


def phase2_clean_titles(conn):
    """Remove double spaces, strip whitespace, remove 'total.kz' from titles."""
    cur = conn.cursor()
    results = {}

    # 2a. Remove double spaces in titles
    cur.execute("""
        UPDATE articles
        SET title = regexp_replace(title, ' {2,}', ' ', 'g')
        WHERE title ~ ' {2,}'
    """)
    results["double_spaces"] = cur.rowcount
    conn.commit()
    print(f"  2a. Double spaces in titles: {results['double_spaces']}")

    # 2b. Strip trailing/leading whitespace
    cur.execute("""
        UPDATE articles
        SET title = btrim(title)
        WHERE title IS NOT NULL AND title != btrim(title)
    """)
    results["trimmed"] = cur.rowcount
    conn.commit()
    print(f"  2b. Titles trimmed: {results['trimmed']}")

    # 2c. Remove "total.kz" / "Total.kz" / "| total.kz" / "- total.kz" from titles
    cur.execute("""
        UPDATE articles
        SET title = btrim(regexp_replace(title, '[\s\-–|]*[Tt]otal\.kz\s*$', '', 'g'))
        WHERE title ~* 'total\.kz'
    """)
    results["total_kz_removed"] = cur.rowcount
    conn.commit()
    print(f"  2c. 'total.kz' removed from titles: {results['total_kz_removed']}")

    # 2d. Capitalize first letter if lowercase (Russian/Kazakh text)
    cur.execute("""
        UPDATE articles
        SET title = upper(left(title, 1)) || substring(title from 2)
        WHERE title IS NOT NULL
          AND length(title) > 0
          AND left(title, 1) ~ '[a-zа-яәғқңөұүһі]'
    """)
    results["capitalized"] = cur.rowcount
    conn.commit()
    print(f"  2d. Titles capitalized: {results['capitalized']}")

    cur.close()
    return results


# ── Phase 3: Fix excerpts ────────────────────────────────


def phase3_fix_excerpts(conn):
    """Generate excerpt from body_text for articles without one. Trim long excerpts."""
    cur = conn.cursor()
    results = {}

    # 3a. Generate excerpt from body_text for articles without excerpt
    # Take first ~160 chars at sentence boundary (look for . ? ! followed by space)
    cur.execute("""
        UPDATE articles
        SET excerpt = CASE
            WHEN length(body_text) <= 160 THEN body_text
            WHEN position('.' IN substring(body_text from 1 for 200)) > 30
                THEN left(body_text, position('.' IN substring(body_text from 1 for 200)))
            WHEN position('!' IN substring(body_text from 1 for 200)) > 30
                THEN left(body_text, position('!' IN substring(body_text from 1 for 200)))
            WHEN position('?' IN substring(body_text from 1 for 200)) > 30
                THEN left(body_text, position('?' IN substring(body_text from 1 for 200)))
            ELSE left(body_text, 160)
        END
        WHERE (excerpt IS NULL OR btrim(excerpt) = '')
          AND body_text IS NOT NULL
          AND btrim(body_text) != ''
    """)
    results["generated"] = cur.rowcount
    conn.commit()
    print(f"  3a. Excerpts generated from body_text: {results['generated']}")

    # 3b. Trim long excerpts to 200 chars at word boundary
    cur.execute("""
        UPDATE articles
        SET excerpt = CASE
            WHEN position(' ' IN reverse(left(excerpt, 200))) > 0
                THEN left(excerpt, 200 - position(' ' IN reverse(left(excerpt, 200)))) || '…'
            ELSE left(excerpt, 200) || '…'
        END
        WHERE excerpt IS NOT NULL
          AND length(excerpt) > 200
    """)
    results["trimmed"] = cur.rowcount
    conn.commit()
    print(f"  3b. Long excerpts trimmed: {results['trimmed']}")

    # 3c. Strip whitespace from excerpts
    cur.execute("""
        UPDATE articles
        SET excerpt = btrim(excerpt)
        WHERE excerpt IS NOT NULL AND excerpt != btrim(excerpt)
    """)
    results["stripped"] = cur.rowcount
    conn.commit()
    print(f"  3c. Excerpts stripped: {results['stripped']}")

    cur.close()
    return results


# ── Phase 4: SEO polish ──────────────────────────────────


def phase4_seo_polish(conn):
    """Use enrichment.summary as excerpt where available and excerpt is missing."""
    cur = conn.cursor()
    results = {}

    # 4a. Use enrichment.summary as excerpt where article has no excerpt
    cur.execute("""
        UPDATE articles a
        SET excerpt = e.summary
        FROM article_enrichments e
        WHERE e.article_id = a.id
          AND e.summary IS NOT NULL
          AND btrim(e.summary) != ''
          AND (a.excerpt IS NULL OR btrim(a.excerpt) = '')
    """)
    results["summary_as_excerpt"] = cur.rowcount
    conn.commit()
    print(f"  4a. Enrichment summary used as excerpt: {results['summary_as_excerpt']}")

    # 4b. Use meta_description as excerpt for remaining articles without excerpt
    cur.execute("""
        UPDATE articles a
        SET excerpt = e.meta_description
        FROM article_enrichments e
        WHERE e.article_id = a.id
          AND e.meta_description IS NOT NULL
          AND btrim(e.meta_description) != ''
          AND (a.excerpt IS NULL OR btrim(a.excerpt) = '')
    """)
    results["meta_as_excerpt"] = cur.rowcount
    conn.commit()
    print(f"  4b. Meta description used as excerpt: {results['meta_as_excerpt']}")

    # 4c. Final count of articles still without excerpt
    cur.execute("""
        SELECT COUNT(*) FROM articles
        WHERE excerpt IS NULL OR btrim(excerpt) = ''
    """)
    results["still_no_excerpt"] = cur.fetchone()[0]
    print(f"  4c. Articles still without excerpt: {results['still_no_excerpt']}")

    cur.close()
    return results


# ── Main ─────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("Content Cleanup & SEO Polish")
    print("=" * 60)

    conn = get_conn()
    cur = conn.cursor()

    # Pre-flight counts
    cur.execute("SELECT COUNT(*) FROM articles")
    total = cur.fetchone()[0]
    print(f"\nTotal articles: {total:,}")

    cur.execute("SELECT COUNT(*) FROM articles WHERE body_html ~* '<script'")
    scripts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM articles WHERE excerpt IS NULL OR btrim(excerpt) = ''")
    no_excerpt = cur.fetchone()[0]
    print(f"Articles with <script> tags: {scripts:,}")
    print(f"Articles without excerpt: {no_excerpt:,}")
    cur.close()

    all_results = {}
    t0 = time.time()

    # Phase 1
    print(f"\n{'─' * 40}")
    print("Phase 1: Clean body_html")
    print(f"{'─' * 40}")
    t1 = time.time()
    all_results["phase1"] = phase1_clean_body_html(conn)
    print(f"  Phase 1 done in {time.time() - t1:.1f}s")

    # Phase 2
    print(f"\n{'─' * 40}")
    print("Phase 2: Clean titles")
    print(f"{'─' * 40}")
    t2 = time.time()
    all_results["phase2"] = phase2_clean_titles(conn)
    print(f"  Phase 2 done in {time.time() - t2:.1f}s")

    # Phase 3
    print(f"\n{'─' * 40}")
    print("Phase 3: Fix excerpts")
    print(f"{'─' * 40}")
    t3 = time.time()
    all_results["phase3"] = phase3_fix_excerpts(conn)
    print(f"  Phase 3 done in {time.time() - t3:.1f}s")

    # Phase 4
    print(f"\n{'─' * 40}")
    print("Phase 4: SEO polish")
    print(f"{'─' * 40}")
    t4 = time.time()
    all_results["phase4"] = phase4_seo_polish(conn)
    print(f"  Phase 4 done in {time.time() - t4:.1f}s")

    conn.close()

    # Summary
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"ALL DONE in {elapsed:.1f}s")
    print(f"{'=' * 60}")

    print("\nPhase 1 (body_html cleanup):")
    for k, v in all_results["phase1"].items():
        print(f"  {k}: {v:,}")

    print("\nPhase 2 (title cleanup):")
    for k, v in all_results["phase2"].items():
        print(f"  {k}: {v:,}")

    print("\nPhase 3 (excerpt fix):")
    for k, v in all_results["phase3"].items():
        print(f"  {k}: {v:,}")

    print("\nPhase 4 (SEO polish):")
    for k, v in all_results["phase4"].items():
        print(f"  {k}: {v:,}")


if __name__ == "__main__":
    main()
