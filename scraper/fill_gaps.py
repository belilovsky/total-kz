#!/usr/bin/env python3
"""Fill data gaps in articles: extract tags from HTML, restore images.

Run: python fill_gaps.py [--db path/to/total.db] [--dry-run] [--limit N]

What it does:
1. For articles with body_html but no tags → extract tags from HTML content
   - Parses <meta name="keywords">, <meta property="article:tag">
   - Parses tag links like <a href="...tag/..."> patterns common on total.kz
   - Falls back to extracting from visible keyword/tag containers
2. For articles with body_html but no main_image → extract first <img> from HTML
3. For articles with body_html but no inline_images → extract all <img src> from HTML
4. Populates article_tags table for newly discovered tags
"""

import argparse
import json
import re
import sqlite3
import sys
from html.parser import HTMLParser
from pathlib import Path

DEFAULT_DB = "/opt/total-kz/data/total.db"


class TagExtractor(HTMLParser):
    """Extract tags and images from HTML content."""

    def __init__(self):
        super().__init__()
        self.tags = []
        self.images = []
        self.meta_keywords = []
        self.in_tag_container = False
        self.tag_depth = 0
        self.current_tag_text = ""
        # Track tag-like link patterns
        self.in_tag_link = False
        self.tag_link_text = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)

        # Meta tags with keywords
        if tag == "meta":
            name = (attrs_dict.get("name") or "").lower()
            prop = (attrs_dict.get("property") or "").lower()
            content = attrs_dict.get("content", "")
            if name == "keywords" and content:
                self.meta_keywords.extend(
                    [k.strip() for k in content.split(",") if k.strip()]
                )
            elif prop in ("article:tag", "og:tag") and content:
                self.meta_keywords.append(content.strip())

        # Images
        if tag == "img":
            src = attrs_dict.get("src") or attrs_dict.get("data-src") or ""
            if src and not src.startswith("data:"):
                # Normalize
                if src.startswith("//"):
                    src = "https:" + src
                self.images.append(src)

        # Source tag (in <picture>)
        if tag == "source":
            srcset = attrs_dict.get("srcset", "")
            if srcset:
                # Take first URL from srcset
                first_src = srcset.split(",")[0].strip().split(" ")[0]
                if first_src and not first_src.startswith("data:"):
                    if first_src.startswith("//"):
                        first_src = "https:" + first_src
                    self.images.append(first_src)

        # Tag links (common patterns in news sites)
        if tag == "a":
            href = attrs_dict.get("href", "")
            cls = attrs_dict.get("class", "")
            # Check for tag-related links
            if any(p in href.lower() for p in ["/tag/", "/tags/", "tag=", "/tema/"]):
                self.in_tag_link = True
                self.tag_link_text = ""
            elif any(
                c in cls.lower()
                for c in ["tag", "keyword", "label", "topic", "badge"]
            ):
                self.in_tag_link = True
                self.tag_link_text = ""

        # Tag containers (divs/sections with tag-related classes)
        if tag in ("div", "section", "ul", "span"):
            cls = attrs_dict.get("class", "")
            if any(
                c in cls.lower()
                for c in ["tags", "keywords", "article-tags", "post-tags"]
            ):
                self.in_tag_container = True
                self.tag_depth = 0

        if self.in_tag_container:
            self.tag_depth += 1

    def handle_endtag(self, tag):
        if self.in_tag_link and tag == "a":
            text = self.tag_link_text.strip()
            if text and len(text) < 100:
                self.tags.append(text)
            self.in_tag_link = False
            self.tag_link_text = ""

        if self.in_tag_container:
            self.tag_depth -= 1
            if self.tag_depth <= 0:
                self.in_tag_container = False

    def handle_data(self, data):
        if self.in_tag_link:
            self.tag_link_text += data

    def get_tags(self):
        """Return deduplicated list of extracted tags."""
        all_tags = self.meta_keywords + self.tags
        seen = set()
        result = []
        for t in all_tags:
            t = t.strip().strip("#").strip()
            if not t or len(t) < 2 or len(t) > 120:
                continue
            normalized = t.lower()
            if normalized not in seen:
                seen.add(normalized)
                result.append(t)
        return result

    def get_images(self):
        """Return deduplicated list of image URLs."""
        seen = set()
        result = []
        for img in self.images:
            if img not in seen:
                seen.add(img)
                result.append(img)
        return result


def extract_tags_regex(html: str) -> list:
    """Fallback: extract tags using regex patterns common in total.kz HTML."""
    tags = []

    # Pattern 1: meta keywords
    for m in re.finditer(
        r'<meta\s+name=["\']keywords["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    ):
        for t in m.group(1).split(","):
            t = t.strip()
            if t:
                tags.append(t)

    # Pattern 2: article:tag meta
    for m in re.finditer(
        r'<meta\s+property=["\']article:tag["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    ):
        tags.append(m.group(1).strip())

    # Pattern 3: links with /tag/ in href
    for m in re.finditer(r'<a[^>]*href=["\'][^"\']*\/tag\/[^"\']*["\'][^>]*>([^<]+)</a>', html, re.IGNORECASE):
        t = m.group(1).strip()
        if t and len(t) < 100:
            tags.append(t)

    # Pattern 4: data-tag attributes
    for m in re.finditer(r'data-tag=["\']([^"\']+)["\']', html, re.IGNORECASE):
        tags.append(m.group(1).strip())

    # Deduplicate
    seen = set()
    result = []
    for t in tags:
        t = t.strip().strip("#")
        if t and len(t) >= 2 and t.lower() not in seen:
            seen.add(t.lower())
            result.append(t)
    return result


def extract_images_regex(html: str) -> list:
    """Extract image URLs from HTML using regex."""
    images = []
    # img src
    for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']', html, re.IGNORECASE):
        src = m.group(1)
        if not src.startswith("data:"):
            if src.startswith("//"):
                src = "https:" + src
            images.append(src)

    # data-src (lazy loading)
    for m in re.finditer(
        r'<img[^>]+data-src=["\']([^"\']+)["\']', html, re.IGNORECASE
    ):
        src = m.group(1)
        if not src.startswith("data:"):
            if src.startswith("//"):
                src = "https:" + src
            images.append(src)

    # Deduplicate
    seen = set()
    result = []
    for img in images:
        if img not in seen:
            seen.add(img)
            result.append(img)
    return result


def process_article(html: str):
    """Extract tags and images from article HTML.

    Returns: (tags: list[str], images: list[str])
    """
    tags = []
    images = []

    # Try HTMLParser first
    try:
        parser = TagExtractor()
        parser.feed(html)
        tags = parser.get_tags()
        images = parser.get_images()
    except Exception:
        pass

    # Supplement with regex if needed
    if not tags:
        tags = extract_tags_regex(html)

    if not images:
        images = extract_images_regex(html)

    return tags, images


def main():
    parser = argparse.ArgumentParser(description="Fill data gaps in total.kz DB")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print stats without modifying DB"
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="Process only N articles (0 = all)"
    )
    parser.add_argument(
        "--batch", type=int, default=1000, help="Batch size for commits"
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Stats
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    no_tags = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE (tags IS NULL OR tags = '' OR tags = '[]') AND body_html IS NOT NULL AND body_html != ''"
    ).fetchone()[0]
    no_main_image = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE (main_image IS NULL OR main_image = '') AND body_html IS NOT NULL AND body_html != ''"
    ).fetchone()[0]
    no_inline = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE (inline_images IS NULL OR inline_images = '' OR inline_images = '[]') AND body_html IS NOT NULL AND body_html != ''"
    ).fetchone()[0]

    print(f"=== Total.kz Gap Filler ===")
    print(f"Database: {db_path}")
    print(f"Total articles: {total:,}")
    print(f"Articles without tags (with HTML): {no_tags:,}")
    print(f"Articles without main_image (with HTML): {no_main_image:,}")
    print(f"Articles without inline_images (with HTML): {no_inline:,}")
    print()

    if args.dry_run:
        # Sample analysis
        print("=== DRY RUN: Sampling 100 articles ===")
        rows = conn.execute("""
            SELECT id, body_html FROM articles
            WHERE body_html IS NOT NULL AND body_html != ''
            AND (tags IS NULL OR tags = '' OR tags = '[]')
            LIMIT 100
        """).fetchall()

        tags_found = 0
        images_found = 0
        for row in rows:
            tags, images = process_article(row["body_html"])
            if tags:
                tags_found += 1
                print(f"  Article {row['id']}: {len(tags)} tags: {tags[:5]}")
            if images:
                images_found += 1

        print(f"\nSample results:")
        print(f"  Tags found in {tags_found}/100 articles")
        print(f"  Images found in {images_found}/100 articles")
        conn.close()
        return

    # Process articles
    print("=== Processing articles ===")

    # Get articles needing tags or images
    limit_clause = f"LIMIT {args.limit}" if args.limit else ""
    rows = conn.execute(f"""
        SELECT id, body_html, tags, main_image, inline_images
        FROM articles
        WHERE body_html IS NOT NULL AND body_html != ''
        AND (
            (tags IS NULL OR tags = '' OR tags = '[]')
            OR (main_image IS NULL OR main_image = '')
            OR (inline_images IS NULL OR inline_images = '' OR inline_images = '[]')
        )
        {limit_clause}
    """).fetchall()

    print(f"Articles to process: {len(rows):,}")

    tags_updated = 0
    main_image_updated = 0
    inline_images_updated = 0
    tags_inserted = 0
    batch_count = 0

    for i, row in enumerate(rows):
        article_id = row["id"]
        html = row["body_html"]
        current_tags = row["tags"] or "[]"
        current_main = row["main_image"] or ""
        current_inline = row["inline_images"] or "[]"

        tags, images = process_article(html)

        # Update tags if empty
        has_tags = current_tags not in ("", "[]", None)
        if not has_tags and tags:
            tags_json = json.dumps(tags, ensure_ascii=False)
            conn.execute(
                "UPDATE articles SET tags = ? WHERE id = ?", (tags_json, article_id)
            )
            # Also insert into article_tags table
            for tag in tags:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO article_tags (article_id, tag) VALUES (?, ?)",
                        (article_id, tag),
                    )
                    tags_inserted += 1
                except Exception:
                    pass
            tags_updated += 1

        # Update main_image if empty
        if not current_main and images:
            conn.execute(
                "UPDATE articles SET main_image = ? WHERE id = ?",
                (images[0], article_id),
            )
            main_image_updated += 1

        # Update inline_images if empty
        has_inline = current_inline not in ("", "[]", None)
        if not has_inline and images:
            images_json = json.dumps(images, ensure_ascii=False)
            conn.execute(
                "UPDATE articles SET inline_images = ? WHERE id = ?",
                (images_json, article_id),
            )
            inline_images_updated += 1

        batch_count += 1
        if batch_count >= args.batch:
            conn.commit()
            batch_count = 0
            progress = (i + 1) / len(rows) * 100
            print(
                f"  Progress: {i + 1:,}/{len(rows):,} ({progress:.1f}%) — "
                f"tags: +{tags_updated:,}, images: +{main_image_updated:,}, "
                f"inline: +{inline_images_updated:,}"
            )

    conn.commit()

    print(f"\n=== Results ===")
    print(f"Tags restored: {tags_updated:,} articles")
    print(f"Tag links created: {tags_inserted:,}")
    print(f"Main images restored: {main_image_updated:,} articles")
    print(f"Inline images restored: {inline_images_updated:,} articles")

    # Updated stats
    no_tags_after = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE (tags IS NULL OR tags = '' OR tags = '[]')"
    ).fetchone()[0]
    no_main_after = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE (main_image IS NULL OR main_image = '')"
    ).fetchone()[0]

    print(f"\nRemaining gaps:")
    print(f"  Without tags: {no_tags_after:,} (was {no_tags:,})")
    print(f"  Without main_image: {no_main_after:,} (was {no_main_image:,})")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
