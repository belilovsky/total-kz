#!/usr/bin/env python3
"""Fix homepage design: live feed header, popular compact, misc spacing."""

# === 1. HOME TEMPLATE: Remove live feed header, keep inner scroll ===
html = open('app/templates/public/home.html').read()

# Remove the header div (dot + title) from live-feed
old_header = '''      <div class="live-feed__header">
        <span class="live-feed__dot"></span>
        <h2 class="live-feed__title">{{ t('homepage.live_feed') if t is defined else 'Лента новостей' }}</h2>
      </div>'''
html = html.replace(old_header, '')
print("1. Removed live-feed header")

# Remove date from popular section (compact mode)
old_pop = '''            <span class="popular-meta">{{ format_date_day(art.pub_date) }}</span>'''
new_pop = '''            <span class="popular-meta">{{ format_date_short(art.pub_date) }}</span>'''
html = html.replace(old_pop, new_pop)
print("2. Shortened popular date format")

open('app/templates/public/home.html', 'w').write(html)

# === 2. CSS: compact popular, live feed scroll fix ===
css = open('app/static/css/public.css').read()

# Fix popular item - make more compact
old_pop_css = '''.popular-item {
  display:flex;
  gap:var(--space-3);
  padding:var(--space-3) 0;
  border-bottom:1px solid var(--border-light);
}'''
new_pop_css = '''.popular-item {
  display:flex;
  gap:var(--space-2);
  padding:var(--space-2) 0;
  border-bottom:1px solid var(--border-light);
  align-items:flex-start;
}'''
if old_pop_css in css:
    css = css.replace(old_pop_css, new_pop_css)
    print("3. Compacted popular-item padding")

# Make popular-meta (date) inline with title, smaller
old_pop_meta = '''.popular-meta {'''
# Find and replace the full block
import re
m = re.search(r'\.popular-meta\s*\{[^}]+\}', css[4000:])
if m:
    old_meta_full = m.group()
    new_meta_full = '''.popular-meta {
  font-size:11px;
  color:var(--text-muted);
  white-space:nowrap;
}'''
    css = css.replace(old_meta_full, new_meta_full, 1)
    print("4. Compacted popular-meta")

# Make popular-body use flex row (title + date inline)
old_pop_body = '''.popular-body {'''
m2 = re.search(r'\.popular-body\s*\{[^}]+\}', css[4000:])
if m2:
    old_body_full = m2.group()
    new_body_full = '''.popular-body {
  flex:1;
  min-width:0;
}'''
    css = css.replace(old_body_full, new_body_full, 1)
    print("5. Fixed popular-body")

# Make popular-title smaller line-height
old_pop_title = re.search(r'(\.popular-title\s*\{[^}]+\})', css[4050:])
if old_pop_title:
    old_title = old_pop_title.group()
    new_title = '''.popular-title {
  display:block;
  font-size:var(--text-xs);
  font-weight:600;
  line-height:1.3;
  color:var(--text);
  text-decoration:none;
}'''
    css = css.replace(old_title, new_title, 1)
    print("6. Compacted popular-title")

# Popular num - smaller
old_num = re.search(r'\.popular-num\s*\{[^}]+\}', css[3300:3400])
if old_num:
    old_num_full = css[3300:3400][old_num.start():old_num.end()]
    # Don't replace this one, it might be different context

# Live feed: ensure inner scroll with fixed height
# The live-feed already has overflow-y:auto and max-height:100%
# Just need to make sure the container constrains height
# Add a specific max-height to live-feed__list
old_feed_list = '''.live-feed__list {
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  overflow-y: auto;
}'''
new_feed_list = '''.live-feed__list {
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  overflow-y: auto;
  scrollbar-width: thin;
  scrollbar-color: var(--border-light) transparent;
}'''
if old_feed_list in css:
    css = css.replace(old_feed_list, new_feed_list)
    print("7. Added thin scrollbar to live-feed")

# Remove the live-feed header styles since we removed the header
# (keep them for backwards compat, they won't hurt)

# Fix live feed items - slightly more compact
old_feed_item = '''.live-feed__item {
  display: flex;
  gap: 10px;
  padding: 8px 4px;
  text-decoration: none;
  border-bottom: 1px solid var(--border-light);
  transition: background var(--transition);
}'''
new_feed_item = '''.live-feed__item {
  display: flex;
  gap: 8px;
  padding: 6px 4px;
  text-decoration: none;
  border-bottom: 1px solid var(--border-light);
  transition: background var(--transition);
}'''
if old_feed_item in css:
    css = css.replace(old_feed_item, new_feed_item)
    print("8. Compacted live-feed items")

open('app/static/css/public.css', 'w').write(css)
# Copy to min
import shutil
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')

# Bump CSS version
base = open('app/templates/public/base.html').read()
base = base.replace('public.min.css?v=31.2', 'public.min.css?v=31.3')
open('app/templates/public/base.html', 'w').write(base)
print("9. CSS version bumped to 31.3")

print("\nDone!")
