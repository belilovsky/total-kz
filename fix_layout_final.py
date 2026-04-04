#!/usr/bin/env python3
"""
Final fix: hero image beside sidebar using article-layout grid.
No moving HTML elements - pure CSS approach.

Strategy:
1. Make article-layout a 2-column grid (was block)
2. Header stays full-width (grid-column: 1/-1)
3. Hero image goes to column 1 only (remove article-full-width)
4. Ad stays full-width
5. article-body-grid becomes single-column (no longer needs its own grid)
6. Sidebar pulled out of body-grid into article-layout column 2
"""
import shutil

# === STEP 1: Template changes ===
html = open('app/templates/public/article.html').read()

# Remove article-full-width from hero
html = html.replace(
    'class="article-hero-image article-hero article-full-width"',
    'class="article-hero-image article-hero"'
)
print("1. Hero: removed article-full-width")

# Move sidebar OUT of article-body-grid, into article-layout directly
# Find sidebar opening
sidebar_start = html.find('<!-- Sidebar: TOC')
if sidebar_start < 0:
    sidebar_start = html.find('<aside class="article-sidebar">')

# Find body-grid closing
bodygrid_close = html.find('</div>{# close article-body-grid #}')

if sidebar_start > 0 and bodygrid_close > 0:
    # Extract sidebar block (from comment to </aside>)
    sidebar_aside_start = html.find('<aside', sidebar_start)
    aside_end = html.find('</aside>', sidebar_aside_start)
    sidebar_end = aside_end + len('</aside>')
    
    sidebar_block = html[sidebar_start:sidebar_end]
    
    # Remove sidebar from body-grid
    html = html[:sidebar_start] + html[sidebar_end:]
    
    # Find the new position of body-grid close (shifted after removal)
    bodygrid_close_new = html.find('</div>{# close article-body-grid #}')
    insert_pos = bodygrid_close_new + len('</div>{# close article-body-grid #}')
    
    # Insert sidebar after body-grid close (directly in article-layout)
    html = html[:insert_pos] + '\n    ' + sidebar_block + '\n' + html[insert_pos:]
    print("2. Sidebar moved out of body-grid into article-layout")
else:
    print("2. Could not find sidebar or body-grid markers")

open('app/templates/public/article.html', 'w').write(html)

# === STEP 2: CSS changes ===
css = open('app/static/css/public.css').read()

# Make article-layout a 2-column grid on desktop
old_layout = '.article-layout { display:block; }'
new_layout = '.article-layout { display:grid; grid-template-columns:1fr 300px; gap:var(--space-5); }'
css = css.replace(old_layout, new_layout)
print("3. article-layout: grid 1fr 300px")

# Add full-width for header, ad; hero in col 1
if '.article-full-width { grid-column: 1 / -1; }' not in css:
    idx = css.find('.article-layout {', css.find('@media (min-width:1100px)'))
    end = css.find('}', idx) + 1
    rules = """
.article-full-width { grid-column: 1 / -1; }
.article-hero-image { grid-column: 1; border-radius: var(--radius-lg); overflow: hidden; }
.article-hero-image img { max-height: 450px; object-fit: cover; width: 100%; display: block; }
.article-body-grid { grid-column: 1; }
.article-layout > .article-sidebar { grid-column: 2; grid-row: 3 / -1; align-self: start; }
.article-sidebar-inner { position: sticky; top: 5rem; }
"""
    css = css[:end] + rules + css[end:]
    print("4. Added grid column rules")

# Remove the old body-grid styles since body-grid is now single-column
old_bodygrid = '.article-body-grid { display:grid; grid-template-columns:1fr 280px; gap:var(--space-6); }'
new_bodygrid = '.article-body-grid { display:block; }'
css = css.replace(old_bodygrid, new_bodygrid)
print("5. body-grid: now single-column block")

# Mobile: article-layout back to block
# Already handled by existing mobile media queries

# Bump CSS version
css_ver_old = 'public.min.css?v=31.4'
css_ver_new = 'public.min.css?v=32.0'

base = open('app/templates/public/base.html').read()
base = base.replace(css_ver_old, css_ver_new)
open('app/templates/public/base.html', 'w').write(base)

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("6. CSS saved, version 32.0")
print("Done!")
