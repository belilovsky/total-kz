#!/usr/bin/env python3
"""Import articles from JSONL files into PostgreSQL.

Reads articles.jsonl (primary, ~260K lines, 2.3GB) and articles_full.jsonl
(~112K lines, 22 unique URLs not in the primary file) from the data/ directory,
deduplicates by URL (keeps the version with longer body_html), and inserts
into the PostgreSQL articles table.

Two-pass approach for memory efficiency:
  Pass 1 — Scan both files, build a map of url → (file_index, byte_offset, body_html_len).
            For duplicate URLs, keep the entry with the longest body_html.
  Pass 2 — Group winning entries by file, seek to each offset, parse, and insert in batches.

Usage:
    python -m scripts.import_jsonl_to_pg
    python -m scripts.import_jsonl_to_pg --data-dir /opt/total-kz/data
    python -m scripts.import_jsonl_to_pg --dry-run

Connection: uses PG_DATABASE_URL env var or defaults to
    postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

# Default connection for running inside Docker (db service)
DEFAULT_PG_URL = "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz"

# Columns to insert (order matters — must match VALUES placeholders)
INSERT_COLUMNS = [
    "url",
    "pub_date",
    "sub_category",
    "category_label",
    "title",
    "author",
    "excerpt",
    "body_text",
    "body_html",
    "main_image",
    "image_credit",
    "thumbnail",
    "tags",
    "inline_images",
    "status",
    "imported_at",
]

BATCH_SIZE = 1000
PROGRESS_EVERY = 5000


def build_insert_sql() -> str:
    """Build INSERT SQL with now() for imported_at."""
    placeholders = []
    for col in INSERT_COLUMNS:
        if col == "imported_at":
            placeholders.append("now()")
        else:
            placeholders.append("%s")
    return (
        f"INSERT INTO articles ({', '.join(INSERT_COLUMNS)}) "
        f"VALUES ({', '.join(placeholders)}) "
        f"ON CONFLICT (url) DO NOTHING"
    )


def article_to_row(article: dict) -> tuple:
    """Convert a parsed JSONL article dict to a parameter tuple.

    Returns values for all INSERT_COLUMNS except imported_at (handled by now() in SQL).
    """
    tags = article.get("tags")
    inline_images = article.get("inline_images")

    return (
        article.get("url"),
        article.get("pub_date"),
        article.get("sub_category"),
        article.get("category_label"),
        article.get("title"),
        article.get("author"),
        article.get("excerpt"),
        article.get("body_text"),
        article.get("body_html"),
        article.get("main_image"),
        article.get("image_credit"),
        article.get("thumbnail"),
        psycopg2.extras.Json(tags) if tags is not None else None,
        psycopg2.extras.Json(inline_images) if inline_images is not None else None,
        "published",
    )


# ── Pass 1: Scan & Dedup ────────────────────────────────────────────────────

def scan_file(file_path: Path, file_index: int, url_map: dict) -> tuple[int, int]:
    """Scan a JSONL file and update url_map with best (file_index, byte_offset, body_html_len).

    Opens in binary mode so f.tell() returns reliable byte offsets for seeking later.
    Returns (lines_scanned, new_unique_urls_added).
    """
    lines = 0
    new_urls = 0

    with open(file_path, "rb") as f:
        while True:
            byte_offset = f.tell()
            raw_line = f.readline()
            if not raw_line:
                break

            lines += 1
            try:
                line = raw_line.decode("utf-8").strip()
            except UnicodeDecodeError:
                continue
            if not line:
                continue

            try:
                article = json.loads(line)
            except json.JSONDecodeError:
                continue

            url = article.get("url")
            if not url:
                continue

            body_html = article.get("body_html") or ""
            html_len = len(body_html)

            if url in url_map:
                # Keep version with longer body_html
                if html_len > url_map[url][2]:
                    url_map[url] = (file_index, byte_offset, html_len)
            else:
                url_map[url] = (file_index, byte_offset, html_len)
                new_urls += 1

            if lines % PROGRESS_EVERY == 0:
                print(
                    f"    Scanning {file_path.name}: {lines:,} lines, "
                    f"{len(url_map):,} unique URLs"
                )

    return lines, new_urls


# ── Pass 2: Read winners & Insert ────────────────────────────────────────────

def read_line_at_offset(file_handle, offset: int) -> dict | None:
    """Seek to a byte offset in a binary-mode file and parse the JSON line."""
    file_handle.seek(offset)
    raw_line = file_handle.readline()
    if not raw_line:
        return None
    try:
        return json.loads(raw_line.decode("utf-8").strip())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def flush_batch(cursor, conn, batch: list[tuple], insert_sql: str) -> int:
    """Execute a batch insert. Returns the number of rows inserted."""
    if not batch:
        return 0
    cursor.executemany(insert_sql, batch)
    conn.commit()
    # executemany rowcount with ON CONFLICT DO NOTHING:
    # psycopg2 returns the rowcount of the last statement in executemany,
    # which is not reliable for total count. We'll track separately.
    return len(batch)


def main():
    parser = argparse.ArgumentParser(
        description="Import articles from JSONL files into PostgreSQL"
    )
    parser.add_argument(
        "--data-dir",
        default=str(Path(__file__).resolve().parent.parent / "data"),
        help="Directory containing articles.jsonl and articles_full.jsonl",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and dedup only, don't insert into PG",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    files = []
    primary = data_dir / "articles.jsonl"
    secondary = data_dir / "articles_full.jsonl"

    if not primary.exists():
        print(f"ERROR: Primary file not found: {primary}")
        sys.exit(1)
    files.append(primary)

    if secondary.exists():
        files.append(secondary)
    else:
        print(f"NOTE: Secondary file not found: {secondary} — continuing with primary only")

    pg_url = os.environ.get("PG_DATABASE_URL", DEFAULT_PG_URL)
    print(f"Data directory : {data_dir}")
    print(f"Primary file   : {primary} ({'exists' if primary.exists() else 'MISSING'})")
    print(f"Secondary file : {secondary} ({'exists' if secondary.exists() else 'not found'})")
    print(f"PostgreSQL     : {pg_url.split('@')[0]}@***")
    print()

    # ── Pass 1: Scan & Dedup ─────────────────────────────────────────────
    print("=" * 60)
    print("Pass 1: Scanning JSONL files and deduplicating by URL")
    print("=" * 60)
    t0 = time.time()

    # url_map: url → (file_index, byte_offset, body_html_len)
    url_map: dict[str, tuple[int, int, int]] = {}
    total_lines = 0

    for i, fpath in enumerate(files):
        print(f"\n  Scanning {fpath.name} (file {i})...")
        lines, new_urls = scan_file(fpath, i, url_map)
        total_lines += lines
        print(f"  ✓ {fpath.name}: {lines:,} lines, {new_urls:,} new unique URLs")

    elapsed = time.time() - t0
    print(f"\n  Dedup complete: {total_lines:,} lines → {len(url_map):,} unique articles ({elapsed:.1f}s)")

    if args.dry_run:
        print("\nDry run — skipping database insert.")
        return

    # ── Connect to PG and get baseline count ─────────────────────────────
    conn = psycopg2.connect(pg_url)
    conn.autocommit = False
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM articles")
    count_before = cursor.fetchone()[0]
    print(f"\n  Articles in PG before import: {count_before:,}")

    # ── Pass 2: Read winning articles & Insert ───────────────────────────
    print("\n" + "=" * 60)
    print("Pass 2: Inserting articles into PostgreSQL")
    print("=" * 60)
    t1 = time.time()

    insert_sql = build_insert_sql()

    # Group winners by file_index for sequential reading
    by_file: dict[int, list[tuple[str, int]]] = {}
    for url, (file_idx, offset, _html_len) in url_map.items():
        by_file.setdefault(file_idx, []).append((url, offset))

    # Sort each file's entries by byte offset for sequential I/O
    for file_idx in by_file:
        by_file[file_idx].sort(key=lambda x: x[1])

    processed = 0
    batch: list[tuple] = []
    total_unique = len(url_map)

    for file_idx, entries in sorted(by_file.items()):
        fpath = files[file_idx]
        print(f"\n  Reading winners from {fpath.name} ({len(entries):,} articles)...")

        with open(fpath, "rb") as f:
            for url, offset in entries:
                article = read_line_at_offset(f, offset)
                if article is None:
                    continue

                row = article_to_row(article)
                batch.append(row)
                processed += 1

                if len(batch) >= BATCH_SIZE:
                    flush_batch(cursor, conn, batch, insert_sql)
                    batch = []

                if processed % PROGRESS_EVERY == 0:
                    elapsed = time.time() - t1
                    rate = processed / elapsed if elapsed > 0 else 0
                    print(
                        f"    Processed {processed:,} / {total_unique:,} "
                        f"({processed * 100 // total_unique}%) — {rate:.0f} articles/s"
                    )

    # Flush remaining
    if batch:
        flush_batch(cursor, conn, batch, insert_sql)

    elapsed_insert = time.time() - t1

    # ── Get final count ──────────────────────────────────────────────────
    cursor.execute("SELECT COUNT(*) FROM articles")
    count_after = cursor.fetchone()[0]
    new_inserts = count_after - count_before
    skipped = total_unique - new_inserts

    print(f"\n  Insert phase complete ({elapsed_insert:.1f}s)")

    # ── Reset sequence ───────────────────────────────────────────────────
    print("\nResetting articles_id_seq...")
    cursor.execute(
        "SELECT setval('articles_id_seq', "
        "COALESCE((SELECT MAX(id) FROM articles), 0) + 1, false)"
    )
    new_seq_val = cursor.fetchone()[0]
    conn.commit()
    print(f"  ✓ articles_id_seq set to {new_seq_val}")

    cursor.close()
    conn.close()

    # ── Summary ──────────────────────────────────────────────────────────
    total_elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print("Import Summary")
    print("=" * 60)
    print(f"  JSONL lines read       : {total_lines:,}")
    print(f"  Unique URLs (deduped)  : {total_unique:,}")
    print(f"  Articles in PG before  : {count_before:,}")
    print(f"  Articles in PG after   : {count_after:,}")
    print(f"  New inserts            : {new_inserts:,}")
    print(f"  Skipped (already in PG): {skipped:,}")
    print(f"  Total time             : {total_elapsed:.1f}s")
    print("=" * 60)
    print("\nDone. Remember to reindex Meilisearch separately if needed.")


if __name__ == "__main__":
    main()
