#!/usr/bin/env python3
"""
Clean body_html in articles table.

Two formats exist:
1. Old (cs-article): Full page HTML from total.kz with <article class="cs-article">,
   ads, scripts, duplicated title/meta. Content is inside <section class="article__post post">.
2. New (article__post__body): Cleaner but still has ad comments, scripts.
   Content is inside <div class="article__post__body">.

This script extracts clean content paragraphs and rebuilds body_html and body_text.

Run: python scripts/clean_body_html.py [--dry-run] [--limit N]
"""

import sqlite3
import re
import sys
from pathlib import Path
from bs4 import BeautifulSoup, Comment

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"

# Tags we want to keep
ALLOWED_TAGS = {
    "p", "blockquote", "h2", "h3", "h4", "h5", "h6",
    "ul", "ol", "li", "strong", "em", "b", "i", "a", "br",
    "figure", "figcaption", "img", "iframe",
    "table", "thead", "tbody", "tr", "th", "td",
}

# Tags to remove entirely (with all children)
REMOVE_TAGS = {"script", "style", "noscript", "svg", "button", "form", "input"}

# Classes that indicate ad/junk content
AD_CLASSES = {"adserver", "adfox", "sharethis", "share-buttons", "article__stats",
              "article__meta", "article__title", "icon-list", "custom-share-style",
              "gray-text"}

# Image credit patterns
IMAGE_CREDIT_RE = re.compile(r"^(Фото|Фото:|Источник фото|Photo|Иллюстрат)", re.IGNORECASE)


def is_ad_element(tag):
    """Check if element is an ad/junk."""
    if not tag.name:
        return False
    # Check ID
    tag_id = tag.get("id", "")
    if "adfox" in tag_id or "adserver" in tag_id:
        return True
    # Check classes
    classes = set(tag.get("class", []))
    if classes & AD_CLASSES:
        return True
    return False


def clean_html(raw_html: str) -> tuple[str, str]:
    """
    Extract clean HTML and plain text from raw body_html.
    Returns (clean_html, clean_text).
    """
    if not raw_html or not raw_html.strip():
        return "", ""

    soup = BeautifulSoup(raw_html, "html.parser")

    # Determine format and find content root
    content_root = None

    # Format 1: cs-article — find the post section
    post_section = soup.select_one("section.article__post, section.post, .article__post.post")
    if post_section:
        content_root = post_section
    else:
        # Format 2: article__post__body
        body_div = soup.select_one("div.article__post__body, .article__post__body")
        if body_div:
            content_root = body_div
        else:
            # Fallback: use the whole thing
            content_root = soup

    # Remove all scripts, styles, and ad elements
    for tag in content_root.find_all(REMOVE_TAGS):
        tag.decompose()

    # Remove HTML comments (ad markers)
    for comment in content_root.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    # Remove ad elements by class/id
    for tag in content_root.find_all(True):
        if is_ad_element(tag):
            tag.decompose()

    # Remove the main image div (already stored in main_image field)
    for div in content_root.select(".post__image, .article__image, .post__image_author"):
        div.decompose()

    # Remove share buttons, meta info
    for div in content_root.select(".sharethis-inline-share-buttons, .article__stats, .article__meta, .article__title, .flex-right"):
        div.decompose()

    # Remove h1 (duplicate title)
    for h1 in content_root.find_all("h1"):
        h1.decompose()

    # Now extract clean content blocks
    clean_parts = []

    def process_element(el):
        """Recursively process and clean an element."""
        if el.name in REMOVE_TAGS:
            return None

        if is_ad_element(el):
            return None

        # If it's a tag we want to keep
        if el.name in ALLOWED_TAGS:
            # Clean attributes (keep only href, src, alt, class for blockquotes)
            keep_attrs = {}
            if el.name == "a" and el.get("href"):
                keep_attrs["href"] = el["href"]
                if el.get("target"):
                    keep_attrs["target"] = el["target"]
            elif el.name == "img" and el.get("src"):
                keep_attrs["src"] = el["src"]
                if el.get("alt"):
                    keep_attrs["alt"] = el["alt"]
            elif el.name == "iframe" and el.get("src"):
                keep_attrs["src"] = el["src"]
                keep_attrs["width"] = el.get("width", "100%")
                keep_attrs["height"] = el.get("height", "400")
                keep_attrs["allowfullscreen"] = ""
            el.attrs = keep_attrs

            # Recursively clean children
            for child in list(el.children):
                if hasattr(child, "name") and child.name:
                    result = process_element(child)
                    if result is None:
                        child.decompose()

            # Skip empty paragraphs
            text = el.get_text(strip=True)
            if el.name == "p" and not text:
                return None

            # Skip image credit lines that are already in metadata
            if el.name == "p" and IMAGE_CREDIT_RE.match(text):
                # Keep it but we'll note it
                pass

            return el

        # For container divs, extract their children
        if el.name in ("div", "section", "article", "span", "main"):
            results = []
            for child in list(el.children):
                if hasattr(child, "name") and child.name:
                    result = process_element(child)
                    if result:
                        results.append(result)
                elif isinstance(child, str) and child.strip():
                    # Loose text — wrap in <p>
                    text = child.strip()
                    if len(text) > 10:
                        results.append(f"<p>{text}</p>")
            return results

        return None

    # Process all top-level children
    for child in list(content_root.children):
        if hasattr(child, "name") and child.name:
            result = process_element(child)
            if result is None:
                continue
            if isinstance(result, list):
                for item in result:
                    if isinstance(item, str):
                        clean_parts.append(item)
                    else:
                        clean_parts.append(str(item))
            else:
                clean_parts.append(str(result))
        elif isinstance(child, str) and child.strip() and len(child.strip()) > 10:
            clean_parts.append(f"<p>{child.strip()}</p>")

    # Join and clean up
    clean = "\n".join(clean_parts)

    # Remove excessive whitespace
    clean = re.sub(r"\n{3,}", "\n\n", clean)
    clean = clean.strip()

    # Generate clean plain text
    text_soup = BeautifulSoup(clean, "html.parser")
    clean_text = text_soup.get_text("\n\n", strip=True)

    # Remove duplicate consecutive paragraphs (sometimes happens)
    lines = clean_text.split("\n\n")
    deduped = []
    for line in lines:
        if not deduped or line.strip() != deduped[-1].strip():
            deduped.append(line)
    clean_text = "\n\n".join(deduped)

    return clean, clean_text


def main():
    dry_run = "--dry-run" in sys.argv
    limit = None
    for i, arg in enumerate(sys.argv):
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])

    print(f"Database: {DB_PATH}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # Count articles
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    dirty = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE body_html LIKE '%<script%' OR body_html LIKE '%adserver%' OR body_html LIKE '%cs-article%'"
    ).fetchone()[0]
    print(f"Total articles: {total}, dirty: {dirty}")

    # Fetch articles to clean
    query = "SELECT id, body_html FROM articles WHERE body_html IS NOT NULL AND body_html != ''"
    if limit:
        query += f" LIMIT {limit}"

    articles = conn.execute(query).fetchall()
    print(f"Processing {len(articles)} articles...")

    updated = 0
    errors = 0
    empty_after = 0

    for i, art in enumerate(articles):
        try:
            clean, text = clean_html(art["body_html"])

            if not clean or len(clean) < 20:
                empty_after += 1
                if i < 5:
                    print(f"  [{art['id']}] WARNING: empty after cleaning (orig len={len(art['body_html'])})")
                continue

            if not dry_run:
                conn.execute(
                    "UPDATE articles SET body_html = ?, body_text = ? WHERE id = ?",
                    (clean, text, art["id"])
                )
            updated += 1

            if i < 3 or (i < 20 and i % 5 == 0):
                orig_len = len(art["body_html"])
                new_len = len(clean)
                ratio = new_len / orig_len * 100 if orig_len > 0 else 0
                print(f"  [{art['id']}] {orig_len:,} -> {new_len:,} chars ({ratio:.0f}%)")

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [{art['id']}] ERROR: {e}")

        if (i + 1) % 2000 == 0:
            print(f"  ... processed {i + 1}/{len(articles)}")
            if not dry_run:
                conn.commit()

    if not dry_run:
        conn.commit()

    conn.close()

    print(f"\nDone!")
    print(f"  Updated: {updated}")
    print(f"  Empty after cleaning: {empty_after}")
    print(f"  Errors: {errors}")
    if dry_run:
        print("  (DRY RUN — no changes written)")


if __name__ == "__main__":
    main()
