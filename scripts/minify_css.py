#!/usr/bin/env python3
"""Minify public.css → public.min.css using regex-based compression.

No external dependencies. Run from project root:
    python scripts/minify_css.py
"""

import hashlib
import re
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "app" / "static" / "css" / "public.css"
DST = SRC.with_suffix(".min.css")


def minify_css(css: str) -> str:
    """Remove comments, whitespace, and redundant characters."""
    # Remove multi-line comments
    css = re.sub(r'/\*[\s\S]*?\*/', '', css)
    # Remove single-line comments (only at start of line or after semicolon)
    css = re.sub(r'(?<=;)\s*//[^\n]*', '', css)
    # Collapse whitespace
    css = re.sub(r'\s+', ' ', css)
    # Remove spaces around selectors/braces/colons/semicolons
    css = re.sub(r'\s*{\s*', '{', css)
    css = re.sub(r'\s*}\s*', '}', css)
    css = re.sub(r'\s*;\s*', ';', css)
    css = re.sub(r'\s*:\s*', ':', css)
    css = re.sub(r'\s*,\s*', ',', css)
    # Remove trailing semicolons before closing braces
    css = css.replace(';}', '}')
    # Remove leading/trailing whitespace
    css = css.strip()
    return css


def main():
    if not SRC.exists():
        print(f"ERROR: {SRC} not found", file=sys.stderr)
        sys.exit(1)

    raw = SRC.read_text(encoding="utf-8")
    minified = minify_css(raw)

    DST.write_text(minified, encoding="utf-8")

    raw_kb = len(raw.encode()) / 1024
    min_kb = len(minified.encode()) / 1024
    ratio = (1 - min_kb / raw_kb) * 100

    # Compute content hash for cache-busting
    content_hash = hashlib.md5(minified.encode()).hexdigest()[:8]

    print(f"Minified: {SRC.name} ({raw_kb:.1f}KB) → {DST.name} ({min_kb:.1f}KB) — {ratio:.1f}% smaller")
    print(f"Content hash: {content_hash}")
    print(f"Suggested version tag: ?v={content_hash}")


if __name__ == "__main__":
    main()
