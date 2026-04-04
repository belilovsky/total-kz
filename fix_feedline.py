#!/usr/bin/env python3
"""Remove divider line above ПОСЛЕДНИЕ НОВОСТИ and add spacing."""
import shutil

css = open('app/static/css/public.css').read()

# Remove border-top and the red accent bar from feed-duo
old = """.feed-duo {
  display:grid;
  grid-template-columns:1fr;
  gap:var(--space-8);
  padding-top:var(--space-8);
  border-top:1px solid var(--border);
  position:relative;
}
.feed-duo::before {
  content:''; position:absolute; top:-1px; left:0; width:64px; height:3px;
  background:var(--accent); border-radius:0 0 2px 2px;
}"""

new = """.feed-duo {
  display:grid;
  grid-template-columns:1fr;
  gap:var(--space-8);
  padding-top:var(--space-10);
}"""

if old in css:
    css = css.replace(old, new)
    print("1. Removed feed-duo border-top and accent bar, added more top padding")
else:
    print("1. Pattern not found")

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("Saved")
