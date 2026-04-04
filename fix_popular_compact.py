#!/usr/bin/env python3
"""Hide popular dates for compact layout."""
import shutil

css = open('app/static/css/public.css').read()

# Hide popular-meta (date) to save vertical space
old = """.popular-meta {
  font-size:11px;
  color:var(--text-muted);
  white-space:nowrap;
}"""
new = """.popular-meta {
  display:none;
}"""

if old in css:
    css = css.replace(old, new)
    print("1. Hidden popular date")
else:
    print("1. popular-meta not found")

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("Saved")
