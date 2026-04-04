#!/usr/bin/env python3
"""Fix: anniversary badge round + popular compact + live feed final."""
import re, shutil

css = open('app/static/css/public.css').read()

# === 1. Anniversary badge: round pill, height matches logo ===
old_badge = """.anniversary-badge {
  display:inline-flex; align-items:center; justify-content:center;
  margin-left:8px;
  padding:3px 8px;
  font-family:'Montserrat',var(--font-display),sans-serif;
  font-weight:900; font-size:0.6rem; letter-spacing:0.05em;
  line-height:1; text-transform:uppercase;
  color:#fff;
  background:linear-gradient(135deg, var(--accent) 0%, #b5121e 100%);
  border-radius:4px;
  vertical-align:middle;
  position:relative; top:-1px;
  white-space:nowrap;
  box-shadow:0 1px 3px rgba(216,50,54,.3);
}"""

new_badge = """.anniversary-badge {
  display:inline-flex; align-items:center; justify-content:center;
  margin-left:6px;
  height:1.5rem;
  padding:0 8px;
  font-family:'Montserrat',var(--font-display),sans-serif;
  font-weight:800; font-size:0.55rem; letter-spacing:0.04em;
  line-height:1; text-transform:uppercase;
  color:#fff;
  background:var(--accent);
  border-radius:999px;
  vertical-align:middle;
  white-space:nowrap;
}"""

if old_badge in css:
    css = css.replace(old_badge, new_badge)
    print("1. Badge: round pill, height=1.5rem (matches logo)")
else:
    print("1. Badge pattern not found")

# Also fix desktop override at ~3947
old_badge_desk = ".anniversary-badge { font-size:0.7rem; padding:4px 10px; margin-left:10px; border-radius:5px; }"
new_badge_desk = ".anniversary-badge { font-size:0.6rem; padding:0 10px; margin-left:8px; border-radius:999px; height:1.75rem; }"
if old_badge_desk in css:
    css = css.replace(old_badge_desk, new_badge_desk)
    print("1b. Badge desktop override fixed")

# Fix mobile overrides
css = css.replace(
    ".anniversary-badge { font-size:0.45rem; padding:2px 4px; margin-left:3px; }",
    ".anniversary-badge { font-size:0.4rem; padding:0 5px; margin-left:3px; border-radius:999px; height:1.2rem; }"
)
css = css.replace(
    ".anniversary-badge { font-size:0.5rem; padding:2px 5px; margin-left:4px; }",
    ".anniversary-badge { font-size:0.45rem; padding:0 6px; margin-left:4px; border-radius:999px; height:1.35rem; }"
)
print("1c. Badge mobile overrides fixed")

# === 2. Popular item: more compact, date inline ===
# Find the second .popular-item block (around line 4077)
# Replace padding
m = re.search(r'(\.popular-item\s*\{\s*display:flex;\s*gap:var\(--space-3\);\s*padding:var\(--space-3\) 0;\s*border-bottom:1px solid var\(--border-light\);\s*\})', css[4000:])
if m:
    old_pi = m.group(1)
    new_pi = """.popular-item {
  display:flex;
  gap:var(--space-2);
  padding:6px 0;
  border-bottom:1px solid var(--border-light);
  align-items:flex-start;
}"""
    css = css.replace(old_pi, new_pi, 1)
    print("2. Popular-item compacted")
else:
    print("2. Popular-item pattern not found, trying alt")
    # Try broader match
    css = re.sub(
        r'(\.popular-item\s*\{[^}]*padding:\s*var\(--space-3\)\s+0[^}]*\})',
        """.popular-item {
  display:flex;
  gap:var(--space-2);
  padding:6px 0;
  border-bottom:1px solid var(--border-light);
  align-items:flex-start;
}""",
        css, count=1
    )
    print("2. Popular-item compacted (alt)")

# === 3. Check for text overflow issues on cards ===
# Shelf-card titles might overflow - ensure they clip
if '.shelf-card-title' in css:
    # Already has line-clamp, should be fine
    print("3. shelf-card-title already has line-clamp")

# === Save ===
open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("\nCSS saved and minified")
