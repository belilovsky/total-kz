#!/usr/bin/env python3
"""
Clean body_html in articles table.

Multiple body_html formats exist in the database:
1. Full page HTML from total.kz wrapped in <article class="cs-article">,
   with nested divs, ads, scripts, share buttons, metadata.
   Actual article text is inside <div class="article__post__body">.
2. Older format with <section class="article__post post"> containing the content.
3. Already-clean HTML (just <p>, <blockquote> etc.) — left as-is.

The script finds the content root using prioritized selectors, strips all
wrapper/junk elements, and keeps only clean content tags.

Run: python scripts/clean_body_html.py [--dry-run] [--limit N] [--test]
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

    # Priority 1: Find .article__post__body directly (class-only selector)
    # This is where the actual article text lives in total.kz HTML
    body_div = soup.select_one(".article__post__body")
    if body_div:
        content_root = body_div
    else:
        # Priority 2: Look inside article.cs-article for .article__post__body
        cs_article = soup.select_one("article.cs-article")
        if cs_article:
            body_div = cs_article.select_one(".article__post__body")
            if body_div:
                content_root = body_div
            else:
                # cs-article exists but no .article__post__body inside —
                # use the cs-article itself as root
                content_root = cs_article
        else:
            # Priority 3: Older format with section.article__post
            post_section = soup.select_one("section.article__post, .article__post.post")
            if post_section:
                content_root = post_section
            else:
                # Priority 4: Look inside .cs-flex-container
                flex_container = soup.select_one(".cs-flex-container")
                if flex_container:
                    content_root = flex_container
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


def test_clean_html():
    """Verify cleaning works on a sample of dirty HTML from the production server."""
    dirty_html = """
    <article class="cs-article">
      <div class="container">
        <h1 class="article__title">Заголовок статьи <span class="icon-list">icon</span></h1>
        <div class="article__meta iv_date">
          <span class="gray-text meta__date">01.01.2025</span>
          <span class="gray-text">Автор</span>
          <div class="flex-right article__stats gray-text">
            <div class="custom-share-style">
              <span class="gray-text">Поделиться</span>
              <div class="sharethis-inline-share-buttons right">share</div>
            </div>
          </div>
        </div>
        <div class="cs-flex-container">
          <div class="post__image"><img src="photo.jpg"></div>
          <div class="adserver_1 adserver">
            <script>var adConfig = {};</script>
            <!-- Площадка: total.kz -->
          </div>
          <div class="article__post__body">
            <p>Первый параграф статьи с текстом.</p>
            <p>Второй параграф с <strong>жирным</strong> и <em>курсивом</em>.</p>
            <blockquote>Цитата из источника.</blockquote>
            <p>Третий параграф с <a href="https://example.com">ссылкой</a>.</p>
          </div>
        </div>
      </div>
    </article>
    """

    clean, text = clean_html(dirty_html)

    # Verify no junk remains
    assert "cs-article" not in clean, f"cs-article wrapper not removed: {clean[:200]}"
    assert "adserver" not in clean, f"Ad element not removed: {clean[:200]}"
    assert "article__meta" not in clean, f"Meta not removed: {clean[:200]}"
    assert "article__title" not in clean, f"Title not removed: {clean[:200]}"
    assert "sharethis" not in clean, f"Share buttons not removed: {clean[:200]}"
    assert "<script" not in clean, f"Script not removed: {clean[:200]}"
    assert "icon-list" not in clean, f"Icon list not removed: {clean[:200]}"
    assert "post__image" not in clean, f"Post image not removed: {clean[:200]}"
    assert "<h1" not in clean, f"H1 not removed: {clean[:200]}"

    # Verify content IS preserved
    assert "Первый параграф" in clean, f"First paragraph missing: {clean[:200]}"
    assert "Второй параграф" in clean, f"Second paragraph missing: {clean[:200]}"
    assert "<strong>жирным</strong>" in clean, f"Strong tag missing: {clean[:200]}"
    assert "<em>курсивом</em>" in clean, f"Em tag missing: {clean[:200]}"
    assert "<blockquote>" in clean, f"Blockquote missing: {clean[:200]}"
    assert 'href="https://example.com"' in clean, f"Link missing: {clean[:200]}"
    assert "Третий параграф" in clean, f"Third paragraph missing: {clean[:200]}"

    # Verify significant size reduction
    ratio = len(clean) / len(dirty_html)
    assert ratio < 0.5, f"Insufficient cleaning: ratio={ratio:.2f}, clean_len={len(clean)}, orig_len={len(dirty_html)}"

    # Verify plain text
    assert "Первый параграф" in text, f"First paragraph missing from text: {text[:200]}"
    assert "<" not in text, f"HTML tags in plain text: {text[:200]}"

    # Test idempotency: cleaning already-clean HTML should not break it
    clean2, text2 = clean_html(clean)
    assert "Первый параграф" in clean2, f"Idempotency failed — content lost after second clean: {clean2[:200]}"
    assert "Второй параграф" in clean2, f"Idempotency failed — content lost: {clean2[:200]}"

    # Test already-clean HTML (Format C)
    already_clean = "<p>Simple article text.</p><p>Second paragraph.</p>"
    clean3, text3 = clean_html(already_clean)
    assert "Simple article text" in clean3, f"Already-clean HTML broken: {clean3[:200]}"
    assert "Second paragraph" in clean3, f"Already-clean HTML lost content: {clean3[:200]}"

    print("All tests passed!")
    print(f"  Dirty HTML: {len(dirty_html):,} chars")
    print(f"  Clean HTML: {len(clean):,} chars ({ratio:.0%} of original)")
    print(f"  Clean text: {len(text):,} chars")
    print(f"  Clean HTML output:\n{clean}")


if __name__ == "__main__":
    if "--test" in sys.argv:
        test_clean_html()
    else:
        main()
