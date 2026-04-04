#!/usr/bin/env python3
"""
Revert hero image to outside article-body-grid (full-width).
Use negative margin-top on sidebar to pull it up to hero level.
"""
import shutil

html = open('app/templates/public/article.html').read()

# Current: hero is INSIDE article-body-grid, before article-content
# Need to move it BEFORE article-body-grid (after ad-placeholder)

# Find the hero block inside body-grid
hero_comment = '{# ── Hero image / placeholder ── #}'
hero_start = html.find(hero_comment)
if hero_start < 0:
    print("Hero comment not found")
    exit(1)

# Find end of hero block ({% endif %} after </figure>)
hero_endif = html.find('{% endif %}', html.find('</figure>', hero_start))
hero_end = hero_endif + len('{% endif %}')

hero_block = html[hero_start:hero_end]
print(f"Found hero block: {len(hero_block)} chars")

# Remove hero from inside body-grid
html = html[:hero_start] + html[hero_end:]
print("Removed hero from body-grid")

# Insert hero BEFORE article-body-grid (after ad-placeholder)
bodygrid_marker = '<div class="article-body-grid">'
bodygrid_pos = html.find(bodygrid_marker)
if bodygrid_pos < 0:
    print("ERROR: article-body-grid not found")
    exit(1)

# Add article-full-width back to hero
hero_block = hero_block.replace(
    'class="article-hero-image article-hero"',
    'class="article-hero-image article-hero article-full-width"'
)

# Insert before body-grid
html = html[:bodygrid_pos] + '    ' + hero_block + '\n' + html[bodygrid_pos:]
print("Inserted hero before body-grid (full-width)")

open('app/templates/public/article.html', 'w').write(html)

# === Fix CSS: revert hero to full-width, sidebar sticky from content start ===
css = open('app/static/css/public.css').read()

# Remove the hero grid-column rules we added
old_hero_css = """
.article-hero-image {
  grid-column: 1;
  margin: 0;
  border-radius: var(--radius-lg);
  overflow: hidden;
}
.article-hero-image img {
  width: 100%;
  height: auto;
  display: block;
  max-height: 450px;
  object-fit: cover;
}
.article-body-grid > .article-content { grid-column:1; }
.article-body-grid > .article-sidebar { grid-column:2; grid-row:1 / -1; align-self:start; }
"""

new_hero_css = """
.article-hero-image {
  margin: 0 0 var(--space-4) 0;
}
.article-hero-image img {
  width: 100%;
  height: auto;
  display: block;
  max-height: 500px;
  object-fit: cover;
  border-radius: var(--radius-lg);
}
"""

css = css.replace(old_hero_css, new_hero_css)

# Make sure article-full-width rule exists
if '.article-full-width' not in css:
    # Add it back
    idx = css.find('.article-layout {')
    if idx >= 0:
        end = css.find('}', idx) + 1
        css = css[:end] + '\n.article-full-width { grid-column: 1 / -1; }\n' + css[end:]
        print("Added .article-full-width rule back")

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("CSS fixed")
print("Done!")
