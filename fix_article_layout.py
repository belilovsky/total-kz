#!/usr/bin/env python3
"""Move hero image inside article-body-grid so sidebar starts at photo level."""

html = open('app/templates/public/article.html').read()

# Current structure:
# <figure class="article-hero-image article-hero article-full-width">...</figure>
# <div class="ad-placeholder ...">...</div>
# <div class="article-body-grid">
# <div class="article-content">

# Target: move figure INSIDE article-body-grid, before article-content
# and remove article-full-width from figure

# Step 1: Remove article-full-width from hero figure
old_hero = 'class="article-hero-image article-hero article-full-width"'
new_hero = 'class="article-hero-image article-hero"'
html = html.replace(old_hero, new_hero)
print("1. Removed article-full-width from hero image")

# Step 2: Move the hero block inside article-body-grid
# Find the hero block and the body-grid opening
# The hero is between </header> and ad-placeholder
# We need to cut it from current position and paste it after article-body-grid opening

# Find hero figure block
hero_start_marker = '{# ── Hero image / placeholder ── #}'
hero_end_marker = '<!-- ═══ AD: UNDER HERO (R1) ═══ -->'

hero_start = html.find(hero_start_marker)
hero_end = html.find(hero_end_marker)

if hero_start < 0 or hero_end < 0:
    print("ERROR: Could not find hero markers")
    exit(1)

hero_block = html[hero_start:hero_end].strip()
print(f"2. Found hero block ({len(hero_block)} chars)")

# Remove hero block from current position (leave a newline)
html = html[:hero_start] + '\n    ' + html[hero_end:]
print("3. Removed hero from original position")

# Insert hero block inside article-body-grid, before article-content
bodygrid_marker = '<div class="article-body-grid">'
bodygrid_pos = html.find(bodygrid_marker)
if bodygrid_pos < 0:
    print("ERROR: Could not find article-body-grid")
    exit(1)

insert_pos = bodygrid_pos + len(bodygrid_marker)
html = html[:insert_pos] + '\n    ' + hero_block + '\n' + html[insert_pos:]
print("4. Inserted hero inside article-body-grid")

open('app/templates/public/article.html', 'w').write(html)

# Now fix CSS: hero inside grid should span only column 1
import shutil
css = open('app/static/css/public.css').read()

# Add CSS for hero inside grid
hero_css = """
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
"""

# Find article-body-grid CSS and add after it
idx = css.find('.article-body-grid {')
end = css.find('}', idx) + 1
css = css[:end] + hero_css + css[end:]

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("5. Added hero CSS for grid column 1")
print("Done!")
