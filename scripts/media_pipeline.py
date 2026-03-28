#!/usr/bin/env python3
"""
Media Pipeline — download, optimize, deduplicate, index all article images.

Phase 1: Download + Optimize
- Collect all unique image URLs from articles.main_image
- Skip generic placeholders
- Download from total.kz/storage/... 
- Optimize: convert to JPG, max 800px, quality 82, strip metadata
- Deduplicate by perceptual hash
- Update media table with metadata (file_size, width, height, hash, local_path)
- Rewrite articles.main_image to local /media/... paths

Phase 2 (separate): GPT-4o-mini ALT text generation

Usage:
  python scripts/media_pipeline.py [--workers 3] [--batch 500] [--skip-download] [--skip-optimize] [--dry-run]
"""

import argparse
import hashlib
import io
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
import psycopg2
from psycopg2.extras import execute_batch
from PIL import Image

# ── Config ──────────────────────────────────────────────────────
DB_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)
MEDIA_DIR = Path("/app/data/media")
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

# Image optimization settings
MAX_DIMENSION = 800        # max width or height
JPEG_QUALITY = 82          # quality for JPEG compression
PLACEHOLDER_URL = "https://total.kz/img/custom_image_resize_w_830_h_465.jpg"

# Download settings
MAX_WORKERS = 3            # concurrent downloads (gentle on VPS)
DOWNLOAD_TIMEOUT = 20.0    # seconds per image
BATCH_SIZE = 500           # commit every N images
RETRY_COUNT = 2
DELAY_BETWEEN = 0.1        # seconds between downloads per worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/data/media_pipeline.log"),
    ],
)
log = logging.getLogger("media_pipeline")


def get_db():
    return psycopg2.connect(DB_URL)


def ensure_media_columns(conn):
    """Add columns to media table if they don't exist."""
    cur = conn.cursor()
    columns = {
        "local_path": "TEXT",
        "original_url": "TEXT",
        "phash": "TEXT",
        "optimized": "BOOLEAN DEFAULT FALSE",
        "download_status": "TEXT DEFAULT 'pending'",  # pending, ok, failed, duplicate
    }
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='media'"
    )
    existing = {r[0] for r in cur.fetchall()}
    for col, typ in columns.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE media ADD COLUMN {col} {typ}")
            log.info(f"Added column media.{col}")
    
    # Add index on original_url for fast lookups
    cur.execute("""
        SELECT 1 FROM pg_indexes WHERE indexname = 'idx_media_original_url'
    """)
    if not cur.fetchone():
        cur.execute("CREATE INDEX idx_media_original_url ON media(original_url)")
        log.info("Created index idx_media_original_url")
    
    # Add index on phash for dedup
    cur.execute("""
        SELECT 1 FROM pg_indexes WHERE indexname = 'idx_media_phash'
    """)
    if not cur.fetchone():
        cur.execute("CREATE INDEX idx_media_phash ON media(phash)")
        log.info("Created index idx_media_phash")
    
    conn.commit()


def collect_unique_urls(conn):
    """Get all unique image URLs from articles that need downloading."""
    cur = conn.cursor()
    
    # Get unique main_image URLs that point to total.kz/storage
    cur.execute("""
        SELECT DISTINCT main_image 
        FROM articles 
        WHERE main_image IS NOT NULL 
          AND main_image != ''
          AND main_image LIKE 'https://total.kz/storage/%'
    """)
    urls = {r[0] for r in cur.fetchall()}
    log.info(f"Found {len(urls)} unique storage URLs in articles.main_image")
    
    # Also collect from media table (may have additional URLs)
    cur.execute("""
        SELECT DISTINCT url FROM media 
        WHERE url LIKE 'https://total.kz/storage/%'
    """)
    media_urls = {r[0] for r in cur.fetchall()}
    urls.update(media_urls)
    log.info(f"Total unique URLs including media table: {len(urls)}")
    
    # Exclude already downloaded
    cur.execute("""
        SELECT original_url FROM media 
        WHERE download_status = 'ok' AND local_path IS NOT NULL
    """)
    done = {r[0] for r in cur.fetchall()}
    
    # Also check existing cache
    pending = set()
    for url in urls:
        if url in done:
            continue
        pending.add(url)
    
    log.info(f"Already downloaded: {len(done)}, pending: {len(pending)}")
    return list(pending)


def url_to_local_path(url: str) -> Path:
    """Convert a total.kz/storage URL to a local path.
    
    https://total.kz/storage/ab/abcdef123_resize_w_830_h_465.jpg
    → /app/data/media/ab/abcdef123_resize_w_830_h_465.jpg
    """
    # Extract path after /storage/
    path = url.split("/storage/", 1)[-1]
    return MEDIA_DIR / path


def compute_phash(img: Image.Image, hash_size=8) -> str:
    """Compute perceptual hash of an image."""
    # Resize to hash_size+1 x hash_size, grayscale
    small = img.resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS).convert("L")
    pixels = list(small.getdata())
    
    # Compute difference hash (dHash)
    bits = []
    for row in range(hash_size):
        for col in range(hash_size):
            idx = row * (hash_size + 1) + col
            bits.append(1 if pixels[idx] < pixels[idx + 1] else 0)
    
    # Convert to hex string
    hash_int = 0
    for bit in bits:
        hash_int = (hash_int << 1) | bit
    return f"{hash_int:016x}"


def download_and_optimize(url: str, client: httpx.Client) -> dict:
    """Download an image, optimize it, and return metadata."""
    result = {
        "url": url,
        "status": "failed",
        "local_path": None,
        "file_size": None,
        "width": None,
        "height": None,
        "phash": None,
        "mime_type": None,
        "error": None,
    }
    
    local_path = url_to_local_path(url)
    
    # Check if file already exists on disk (from img_cache)
    cache_path = Path("/app/data/img_cache") / url.split("/storage/", 1)[-1]
    
    try:
        raw_bytes = None
        
        if cache_path.exists():
            # Use cached version
            raw_bytes = cache_path.read_bytes()
        elif local_path.exists():
            # Already downloaded
            raw_bytes = local_path.read_bytes()
        else:
            # Download from origin
            for attempt in range(RETRY_COUNT + 1):
                try:
                    resp = client.get(url, timeout=DOWNLOAD_TIMEOUT)
                    if resp.status_code == 200:
                        raw_bytes = resp.content
                        break
                    elif resp.status_code == 404:
                        result["status"] = "not_found"
                        result["error"] = "404"
                        return result
                except (httpx.TimeoutException, httpx.ConnectError) as e:
                    if attempt < RETRY_COUNT:
                        time.sleep(1)
                    else:
                        result["error"] = str(e)[:100]
                        return result
            
            if raw_bytes is None:
                result["error"] = f"HTTP {resp.status_code}"
                return result
            
            time.sleep(DELAY_BETWEEN)
        
        # Open with PIL
        img = Image.open(io.BytesIO(raw_bytes))
        
        # Convert to RGB (handle RGBA, P, etc.)
        if img.mode in ("RGBA", "LA", "P"):
            background = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            background.paste(img, mask=img.split()[-1] if "A" in img.mode else None)
            img = background
        elif img.mode != "RGB":
            img = img.convert("RGB")
        
        # Compute phash before resize
        phash = compute_phash(img)
        
        # Resize if larger than MAX_DIMENSION
        w, h = img.size
        if max(w, h) > MAX_DIMENSION:
            ratio = MAX_DIMENSION / max(w, h)
            new_w = int(w * ratio)
            new_h = int(h * ratio)
            img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
        
        final_w, final_h = img.size
        
        # Save as optimized JPEG
        local_path.parent.mkdir(parents=True, exist_ok=True)
        # Force .jpg extension
        jpg_path = local_path.with_suffix(".jpg")
        
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
        optimized_bytes = buf.getvalue()
        
        jpg_path.write_bytes(optimized_bytes)
        
        result.update({
            "status": "ok",
            "local_path": str(jpg_path.relative_to(MEDIA_DIR)),
            "file_size": len(optimized_bytes),
            "width": final_w,
            "height": final_h,
            "phash": phash,
            "mime_type": "image/jpeg",
        })
        
    except Exception as e:
        result["error"] = str(e)[:200]
        log.debug(f"Error processing {url}: {e}")
    
    return result


def process_batch(urls: list, args) -> dict:
    """Download and optimize a batch of images."""
    stats = {"ok": 0, "failed": 0, "not_found": 0, "duplicate": 0, "skipped": 0}
    results = []
    
    client = httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": "TotalKZ-MediaPipeline/1.0"},
    )
    
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {}
            for url in urls:
                f = executor.submit(download_and_optimize, url, client)
                futures[f] = url
            
            for f in as_completed(futures):
                result = f.result()
                results.append(result)
                stats[result["status"]] = stats.get(result["status"], 0) + 1
    finally:
        client.close()
    
    return results, stats


def save_results_to_db(conn, results: list):
    """Update media table with download results."""
    cur = conn.cursor()
    
    for r in results:
        if r["status"] not in ("ok", "not_found"):
            continue
        
        if r["status"] == "ok":
            # Check if media record exists for this URL
            cur.execute("SELECT id FROM media WHERE url = %s LIMIT 1", (r["url"],))
            existing = cur.fetchone()
            
            if existing:
                # Update existing record
                cur.execute("""
                    UPDATE media SET 
                        local_path = %s, file_size = %s, width = %s, height = %s,
                        phash = %s, mime_type = %s, optimized = TRUE, 
                        download_status = 'ok', original_url = %s
                    WHERE id = %s
                """, (
                    r["local_path"], r["file_size"], r["width"], r["height"],
                    r["phash"], r["mime_type"], r["url"], existing[0],
                ))
            else:
                # Insert new record
                filename = r["local_path"].split("/")[-1] if r["local_path"] else ""
                cur.execute("""
                    INSERT INTO media (filename, url, original_url, local_path, 
                        file_size, width, height, phash, mime_type, optimized,
                        download_status, uploaded_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, 'ok', NOW())
                """, (
                    filename, r["url"], r["url"], r["local_path"],
                    r["file_size"], r["width"], r["height"], r["phash"],
                    r["mime_type"],
                ))
        elif r["status"] == "not_found":
            cur.execute("""
                UPDATE media SET download_status = 'not_found' 
                WHERE url = %s
            """, (r["url"],))
    
    conn.commit()


def rewrite_article_urls(conn):
    """Rewrite articles.main_image from total.kz URLs to local /media/ paths."""
    cur = conn.cursor()
    
    # Build mapping: original_url → local_path
    cur.execute("""
        SELECT original_url, local_path FROM media 
        WHERE download_status = 'ok' AND local_path IS NOT NULL
    """)
    url_map = {}
    for url, local_path in cur.fetchall():
        url_map[url] = f"/media/{local_path}"
    
    log.info(f"URL mapping has {len(url_map)} entries")
    
    if not url_map:
        log.warning("No URL mappings found — skipping article rewrite")
        return 0
    
    # Update articles in batches
    updated = 0
    batch_size = 1000
    
    cur.execute("""
        SELECT id, main_image FROM articles 
        WHERE main_image LIKE 'https://total.kz/storage/%'
    """)
    
    updates = []
    for article_id, main_image in cur.fetchall():
        new_url = url_map.get(main_image)
        if new_url:
            updates.append((new_url, article_id))
    
    if updates:
        execute_batch(
            cur,
            "UPDATE articles SET main_image = %s WHERE id = %s",
            updates,
            page_size=batch_size,
        )
        updated = len(updates)
        conn.commit()
    
    log.info(f"Rewrote {updated} article image URLs to local paths")
    return updated


def deduplicate_media(conn):
    """Find and mark duplicate images by perceptual hash."""
    cur = conn.cursor()
    
    # Find phashes with multiple files
    cur.execute("""
        SELECT phash, count(*) as cnt 
        FROM media 
        WHERE phash IS NOT NULL AND download_status = 'ok'
        GROUP BY phash 
        HAVING count(*) > 1
        ORDER BY cnt DESC
    """)
    
    dup_hashes = cur.fetchall()
    total_dupes = sum(cnt - 1 for _, cnt in dup_hashes)
    log.info(f"Found {len(dup_hashes)} duplicate groups, {total_dupes} redundant files")
    
    # For each group, keep the one with smallest file_size, mark rest as duplicate
    removed_bytes = 0
    for phash, cnt in dup_hashes:
        cur.execute("""
            SELECT id, local_path, file_size FROM media 
            WHERE phash = %s AND download_status = 'ok'
            ORDER BY file_size ASC NULLS LAST
        """, (phash,))
        rows = cur.fetchall()
        
        # Keep first (smallest), mark rest
        keep_id = rows[0][0]
        for mid, local_path, fsize in rows[1:]:
            cur.execute(
                "UPDATE media SET download_status = 'duplicate' WHERE id = %s",
                (mid,),
            )
            # Don't delete file yet — just mark
            if fsize:
                removed_bytes += fsize
    
    conn.commit()
    log.info(f"Marked {total_dupes} duplicates (potential savings: {removed_bytes/1024/1024:.1f} MB)")
    return total_dupes


def print_stats(conn):
    """Print current media pipeline statistics."""
    cur = conn.cursor()
    
    cur.execute("SELECT download_status, count(*) FROM media GROUP BY download_status")
    status_counts = dict(cur.fetchall())
    
    cur.execute("SELECT count(*) FROM media WHERE optimized = TRUE")
    optimized = cur.fetchone()[0]
    
    cur.execute("SELECT SUM(file_size) FROM media WHERE download_status = 'ok'")
    total_size = cur.fetchone()[0] or 0
    
    cur.execute("SELECT count(*) FROM articles WHERE main_image LIKE '/media/%'")
    local_articles = cur.fetchone()[0]
    
    cur.execute("SELECT count(*) FROM articles WHERE main_image LIKE 'https://total.kz/%'")
    remote_articles = cur.fetchone()[0]
    
    log.info("=" * 50)
    log.info("MEDIA PIPELINE STATS")
    log.info(f"  Media records by status: {status_counts}")
    log.info(f"  Optimized: {optimized}")
    log.info(f"  Total local size: {total_size / 1024 / 1024:.1f} MB")
    log.info(f"  Articles with local images: {local_articles}")
    log.info(f"  Articles still pointing to total.kz: {remote_articles}")
    log.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Media Pipeline for total.kz")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--batch", type=int, default=BATCH_SIZE)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-optimize", action="store_true")
    parser.add_argument("--skip-rewrite", action="store_true")
    parser.add_argument("--skip-dedup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Limit URLs to process (0=all)")
    args = parser.parse_args()
    
    conn = get_db()
    
    # Step 0: Ensure schema
    log.info("Ensuring media table columns...")
    ensure_media_columns(conn)
    
    if not args.skip_download:
        # Step 1: Collect URLs
        log.info("Collecting unique image URLs...")
        urls = collect_unique_urls(conn)
        
        if args.limit > 0:
            urls = urls[:args.limit]
        
        if not urls:
            log.info("No new images to download")
        else:
            log.info(f"Processing {len(urls)} images with {args.workers} workers...")
            
            # Process in batches
            total_stats = {"ok": 0, "failed": 0, "not_found": 0}
            
            for i in range(0, len(urls), args.batch):
                batch = urls[i : i + args.batch]
                batch_num = i // args.batch + 1
                total_batches = (len(urls) + args.batch - 1) // args.batch
                
                log.info(f"Batch {batch_num}/{total_batches}: processing {len(batch)} images...")
                
                if args.dry_run:
                    log.info("  [DRY RUN] would download and optimize")
                    continue
                
                results, stats = process_batch(batch, args)
                
                # Save to DB
                save_results_to_db(conn, results)
                
                for k, v in stats.items():
                    total_stats[k] = total_stats.get(k, 0) + v
                
                log.info(
                    f"  Batch done: ok={stats.get('ok',0)}, "
                    f"failed={stats.get('failed',0)}, "
                    f"not_found={stats.get('not_found',0)}"
                )
                log.info(
                    f"  Running total: ok={total_stats.get('ok',0)}, "
                    f"failed={total_stats.get('failed',0)}, "
                    f"not_found={total_stats.get('not_found',0)}"
                )
            
            log.info(f"Download complete: {total_stats}")
    
    # Step 2: Migrate existing img_cache to media
    log.info("Checking img_cache for already-cached images...")
    migrate_img_cache(conn)
    
    # Step 3: Deduplicate
    if not args.skip_dedup:
        log.info("Deduplicating by perceptual hash...")
        deduplicate_media(conn)
    
    # Step 4: Rewrite article URLs
    if not args.skip_rewrite:
        log.info("Rewriting article image URLs to local paths...")
        rewrite_article_urls(conn)
    
    # Final stats
    print_stats(conn)
    conn.close()


def migrate_img_cache(conn):
    """Move already-cached images from img_cache to media dir and register."""
    cache_dir = Path("/app/data/img_cache")
    if not cache_dir.exists():
        return
    
    cur = conn.cursor()
    
    # Get already-registered local paths
    cur.execute("SELECT original_url FROM media WHERE download_status = 'ok'")
    already_done = {r[0] for r in cur.fetchall()}
    
    migrated = 0
    batch = []
    
    for subdir in sorted(cache_dir.iterdir()):
        if not subdir.is_dir():
            continue
        for img_file in subdir.iterdir():
            if not img_file.is_file():
                continue
            
            # Reconstruct original URL
            rel = img_file.relative_to(cache_dir)
            original_url = f"https://total.kz/storage/{rel}"
            
            if original_url in already_done:
                continue
            
            try:
                # Read and optimize
                raw = img_file.read_bytes()
                img = Image.open(io.BytesIO(raw))
                
                if img.mode in ("RGBA", "LA", "P"):
                    bg = Image.new("RGB", img.size, (255, 255, 255))
                    if img.mode == "P":
                        img = img.convert("RGBA")
                    bg.paste(img, mask=img.split()[-1] if "A" in img.mode else None)
                    img = bg
                elif img.mode != "RGB":
                    img = img.convert("RGB")
                
                phash = compute_phash(img)
                
                w, h = img.size
                if max(w, h) > MAX_DIMENSION:
                    ratio = MAX_DIMENSION / max(w, h)
                    img = img.resize((int(w * ratio), int(h * ratio)), Image.Resampling.LANCZOS)
                
                final_w, final_h = img.size
                
                # Save to media dir
                dest = MEDIA_DIR / str(rel)
                dest = dest.with_suffix(".jpg")
                dest.parent.mkdir(parents=True, exist_ok=True)
                
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
                opt_bytes = buf.getvalue()
                dest.write_bytes(opt_bytes)
                
                local_rel = str(dest.relative_to(MEDIA_DIR))
                filename = dest.name
                
                batch.append((
                    filename, original_url, original_url, local_rel,
                    len(opt_bytes), final_w, final_h, phash, "image/jpeg",
                ))
                migrated += 1
                
                if len(batch) >= 500:
                    execute_batch(
                        cur,
                        """INSERT INTO media (filename, url, original_url, local_path,
                            file_size, width, height, phash, mime_type, optimized,
                            download_status, uploaded_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, 'ok', NOW())
                        ON CONFLICT DO NOTHING""",
                        batch,
                        page_size=500,
                    )
                    conn.commit()
                    log.info(f"  Migrated {migrated} from img_cache...")
                    batch = []
                    
            except Exception as e:
                log.debug(f"Skip cache file {img_file}: {e}")
                continue
    
    if batch:
        execute_batch(
            cur,
            """INSERT INTO media (filename, url, original_url, local_path,
                file_size, width, height, phash, mime_type, optimized,
                download_status, uploaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, 'ok', NOW())
            ON CONFLICT DO NOTHING""",
            batch,
            page_size=500,
        )
        conn.commit()
    
    log.info(f"Migrated {migrated} images from img_cache to media/")


if __name__ == "__main__":
    main()
