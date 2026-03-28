#!/usr/bin/env python3
"""
Generate ALT text + description for media images using GPT-4o-mini vision.

For each image in the media table that has a local file but no alt_text,
sends the image to GPT-4o-mini and generates:
- alt_text: short accessible description (RU) for <img alt="">
- description: longer description for SEO / image search

Usage:
  python scripts/media_alt_generate.py [--batch 50] [--delay 0.5] [--limit 0] [--dry-run]
"""

import argparse
import base64
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import httpx
import psycopg2

DB_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
MEDIA_DIR = Path("/app/data/media")

BATCH_SIZE = 50
DELAY = 0.5  # seconds between API calls
MAX_IMAGE_SIZE = 512 * 1024  # 512KB max for vision API (resize if larger)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/data/media_alt_gen.log"),
    ],
)
log = logging.getLogger("media_alt")


def get_db():
    return psycopg2.connect(DB_URL)


def ensure_columns(conn):
    """Add description column if missing."""
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='media'"
    )
    existing = {r[0] for r in cur.fetchall()}
    if "description" not in existing:
        cur.execute("ALTER TABLE media ADD COLUMN description TEXT")
        log.info("Added column media.description")
    if "alt_generated" not in existing:
        cur.execute("ALTER TABLE media ADD COLUMN alt_generated BOOLEAN DEFAULT FALSE")
        log.info("Added column media.alt_generated")
    conn.commit()


def get_pending_images(conn, limit=0):
    """Get media records that need ALT text."""
    cur = conn.cursor()
    q = """
        SELECT m.id, m.local_path, m.filename, a.title, a.excerpt
        FROM media m
        LEFT JOIN articles a ON a.main_image = '/media/' || m.local_path
        WHERE m.download_status = 'ok' 
          AND m.local_path IS NOT NULL
          AND (m.alt_generated IS NULL OR m.alt_generated = FALSE)
        ORDER BY m.id
    """
    if limit > 0:
        q += f" LIMIT {limit}"
    cur.execute(q)
    return cur.fetchall()


def encode_image_base64(filepath: Path, max_dim=512) -> str | None:
    """Read and resize image, return base64."""
    if not filepath.exists():
        return None
    
    from PIL import Image
    
    img = Image.open(filepath)
    w, h = img.size
    
    # Resize for API (save tokens)
    if max(w, h) > max_dim:
        ratio = max_dim / max(w, h)
        img = img.resize((int(w * ratio), int(h * ratio)), Image.Resampling.LANCZOS)
    
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=70)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def generate_alt(image_b64: str, article_title: str | None, client: httpx.Client) -> dict:
    """Call GPT-4o-mini vision to generate ALT + description."""
    
    context = ""
    if article_title:
        context = f"\nКонтекст: это изображение из новостной статьи «{article_title}»."
    
    resp = client.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json={
            "model": "gpt-4o-mini",
            "messages": [
                {
                    "role": "system",
                    "content": "Ты — помощник для казахстанского новостного портала. Генерируй описания изображений на русском языке.",
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": f"Опиши это изображение для новостного портала.{context}\n\nОтветь в JSON формате:\n{{\"alt\": \"Краткое описание для атрибута alt (10-20 слов)\", \"description\": \"Подробное описание для SEO (30-60 слов)\", \"tags\": [\"тег1\", \"тег2\"]}}",
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_b64}",
                                "detail": "low",
                            },
                        },
                    ],
                },
            ],
            "max_tokens": 300,
            "temperature": 0.3,
        },
        timeout=30.0,
    )
    
    if resp.status_code != 200:
        raise Exception(f"API error {resp.status_code}: {resp.text[:200]}")
    
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    
    # Parse JSON from response
    # Try to extract JSON even if wrapped in markdown
    if "```" in content:
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
    
    try:
        result = json.loads(content.strip())
    except json.JSONDecodeError:
        # Fallback: use raw text as alt
        result = {"alt": content.strip()[:200], "description": "", "tags": []}
    
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=BATCH_SIZE)
    parser.add_argument("--delay", type=float, default=DELAY)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    
    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set")
        sys.exit(1)
    
    conn = get_db()
    ensure_columns(conn)
    
    images = get_pending_images(conn, args.limit)
    log.info(f"Found {len(images)} images needing ALT text")
    
    if not images:
        return
    
    client = httpx.Client(timeout=30.0)
    cur = conn.cursor()
    
    ok = 0
    errors = 0
    
    try:
        for i, (mid, local_path, filename, title, excerpt) in enumerate(images):
            filepath = MEDIA_DIR / local_path
            
            if args.dry_run:
                log.info(f"[DRY RUN] Would process {mid}: {filename}")
                continue
            
            try:
                b64 = encode_image_base64(filepath)
                if not b64:
                    log.warning(f"Cannot read {filepath}")
                    errors += 1
                    continue
                
                result = generate_alt(b64, title, client)
                
                alt = result.get("alt", "")[:500]
                desc = result.get("description", "")[:1000]
                tags = result.get("tags", [])
                
                cur.execute("""
                    UPDATE media SET 
                        alt_text = %s,
                        description = %s,
                        alt_generated = TRUE
                    WHERE id = %s
                """, (alt, desc, mid))
                
                ok += 1
                
                if ok % args.batch == 0:
                    conn.commit()
                    log.info(f"Progress: {ok}/{len(images)} (errors: {errors})")
                
                time.sleep(args.delay)
                
            except Exception as e:
                errors += 1
                log.warning(f"Error processing {mid}: {e}")
                if "rate_limit" in str(e).lower():
                    log.info("Rate limited, sleeping 30s...")
                    time.sleep(30)
                continue
    
    finally:
        conn.commit()
        client.close()
    
    log.info(f"Done: {ok} generated, {errors} errors out of {len(images)} total")
    conn.close()


if __name__ == "__main__":
    main()
