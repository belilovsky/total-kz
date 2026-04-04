#!/usr/bin/env python3
"""Fix: 1) dropdown z-index, 2) live-feed height, 3) shelf-card text size."""
import shutil

css = open('app/static/css/public.css').read()

# 1. Dropdown z-index: raise above currency bar
css = css.replace(
    '.nav-dropdown__menu {\n  display: none;\n  position: absolute;\n  top: 100%;\n  left: 50%;\n  transform: translateX(-50%);\n  min-width: 180px;\n  background: var(--surface);\n  border: 1px solid var(--border-light);\n  border-radius: var(--radius-lg);\n  box-shadow: var(--shadow-md);\n  padding: var(--space-2) 0;\n  z-index: 200;',
    '.nav-dropdown__menu {\n  display: none;\n  position: absolute;\n  top: 100%;\n  left: 50%;\n  transform: translateX(-50%);\n  min-width: 180px;\n  background: var(--surface);\n  border: 1px solid var(--border-light);\n  border-radius: var(--radius-lg);\n  box-shadow: var(--shadow-md);\n  padding: var(--space-2) 0;\n  z-index: 500;'
)
print("1. Dropdown z-index: 200 -> 500")

# 2. Live-feed: increase max-height from 520px to match hero height
css = css.replace(
    'max-height: 520px;',
    'max-height: 600px;'
)
print("2. Live-feed max-height: 520 -> 600px")

# 3. Shelf-card title: smaller font for homepage 4-card row
# Find shelf-card-title and reduce font
import re
m = re.search(r'\.shelf-card-title\s*\{([^}]+)\}', css[3900:3920])
if m:
    old_title = m.group()
    print(f"Found shelf-card-title: {old_title[:60]}")

# Better approach: shelf-card-title should clamp to 3 lines with smaller text
# The base rule
old_sct = """.shelf-card-title {
  font-size: var(--text-sm);
  font-weight: 600;
  line-height: 1.35;
  color: var(--text);
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
  padding: var(--space-2) 0;
}"""

new_sct = """.shelf-card-title {
  font-size: 13px;
  font-weight: 600;
  line-height: 1.35;
  color: var(--text);
  display: -webkit-box;
  -webkit-line-clamp: 4;
  -webkit-box-orient: vertical;
  overflow: hidden;
  padding: var(--space-2) 0;
}"""

if old_sct in css:
    css = css.replace(old_sct, new_sct)
    print("3. Shelf-card-title: smaller font (13px), 4 lines clamp")
else:
    # Try to find and update
    m2 = re.search(r'(\.shelf-card-title\s*\{[^}]+\})', css[3800:4000])
    if m2:
        old = m2.group(1)
        # Just change font-size and line-clamp
        new = old.replace('var(--text-sm)', '13px')
        new = new.replace('-webkit-line-clamp: 3', '-webkit-line-clamp: 4')
        css = css[:3800] + css[3800:4000].replace(old, new) + css[4000:]
        print("3. Shelf-card-title: updated via regex")
    else:
        print("3. Could not find shelf-card-title")

# Bump version
base = open('app/templates/public/base.html').read()
base = base.replace('public.min.css?v=32.1', 'public.min.css?v=32.2')
open('app/templates/public/base.html', 'w').write(base)

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("Done, CSS v32.2")
