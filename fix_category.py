#!/usr/bin/env python3
"""Redesign category page: compact hero, clean grid, proper sidebar."""
import shutil

# === CSS fixes ===
css = open('app/static/css/public.css').read()

# 1. Fix cat-top hero — make it shorter
# Find .cat-top-lead and limit height
cat_top_lead_fix = """
.cat-top-lead {
  display:block;
  position:relative;
  border-radius:var(--radius-lg);
  overflow:hidden;
  text-decoration:none;
  color:#fff;
  max-height:350px;
}
.cat-top-lead img {
  width:100%;
  height:350px;
  object-fit:cover;
  display:block;
}
"""

# 2. Fix cat2-layout grid — proper 2 column with sidebar
cat2_layout_fix = """
.cat2-layout {
  display:grid;
  grid-template-columns:1fr 300px;
  gap:var(--space-5);
  margin-top:var(--space-6);
}
.cat2-grid {
  display:grid;
  grid-template-columns:repeat(3, 1fr);
  gap:var(--space-4);
}
.cat2-sidebar {
  align-self:start;
  position:sticky;
  top:5rem;
}
"""

# 3. Shelf-row (4 cards under hero) — proper sizing
shelf_row_fix = """
.shelf-row {
  display:grid;
  grid-template-columns:repeat(4, 1fr);
  gap:var(--space-4);
}
"""

# Find and replace existing rules
import re

# Replace cat-top-lead
m = re.search(r'\.cat-top-lead\s*\{[^}]+\}', css)
if m:
    css = css.replace(m.group(), '.cat-top-lead { display:block; position:relative; border-radius:var(--radius-lg); overflow:hidden; text-decoration:none; color:#fff; max-height:350px; }')
    print("1. Fixed cat-top-lead height")

# Replace cat-top-lead img
m2 = re.search(r'\.cat-top-lead\s+img\s*\{[^}]+\}', css)
if m2:
    css = css.replace(m2.group(), '.cat-top-lead img { width:100%; height:350px; object-fit:cover; display:block; }')
    print("2. Fixed cat-top-lead img height")

# Replace cat2-layout
m3 = re.search(r'\.cat2-layout\s*\{[^}]+\}', css)
if m3:
    css = css.replace(m3.group(), '.cat2-layout { display:grid; grid-template-columns:1fr 300px; gap:var(--space-5); margin-top:var(--space-6); }')
    print("3. Fixed cat2-layout grid")

# Replace cat2-grid
m4 = re.search(r'\.cat2-grid\s*\{[^}]+\}', css)
if m4:
    css = css.replace(m4.group(), '.cat2-grid { display:grid; grid-template-columns:repeat(3, 1fr); gap:var(--space-4); }')
    print("4. Fixed cat2-grid 3-column")

# Replace cat2-sidebar
m5 = re.search(r'\.cat2-sidebar\s*\{[^}]+\}', css)
if m5:
    css = css.replace(m5.group(), '.cat2-sidebar { align-self:start; }')
    print("5. Fixed cat2-sidebar")

# Add sticky inner for sidebar
if '.cat2-sidebar-inner' not in css:
    css += '\n.cat2-sidebar > .cat2-sb-block:first-child { position:sticky; top:5rem; }\n'

# Fix shelf-row to be proper grid
m6 = re.search(r'\.shelf-row\s*\{[^}]+\}', css)
if m6:
    css = css.replace(m6.group(), '.shelf-row { display:grid; grid-template-columns:repeat(4, 1fr); gap:var(--space-4); }')
    print("6. Fixed shelf-row grid")

# Mobile: cat2-layout single column
mobile_cat2 = '\n@media (max-width:900px) { .cat2-layout { grid-template-columns:1fr; } .cat2-grid { grid-template-columns:repeat(2, 1fr); } .shelf-row { grid-template-columns:repeat(2, 1fr); } .cat-top-lead, .cat-top-lead img { max-height:250px; height:250px; } }\n'
if '.cat2-layout { grid-template-columns:1fr; }' not in css:
    css += mobile_cat2
    print("7. Added mobile overrides")

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("CSS saved")
