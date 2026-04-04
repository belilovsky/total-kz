#!/usr/bin/env python3
"""Fix category page: remove separate hero/shelf-row, single uniform grid."""
import shutil

html = open('app/templates/public/category.html').read()

# Remove the entire cat-top section (hero + shelf-row) and the ad
# Replace with a single cat2-layout that starts from articles[0]

# Find the cat-top block start
cat_top_start = html.find('{# ═══ TOP STORIES')
# Find the cat2-grid start (where articles[5:] begins)
old_grid_start = html.find("{% for art in articles[5:] %}")

if cat_top_start > 0 and old_grid_start > 0:
    # Replace articles[5:] with articles to include all
    html = html.replace("{% for art in articles[5:] %}", "{% for art in articles %}")
    html = html.replace("{% if articles|length > 4 %}", "{% if articles|length > 0 %}")
    
    # Remove cat-top block entirely (from TOP STORIES comment to the ad placeholder end)
    ad_end_marker = '<div class="ad-placeholder ad-leaderboard"'
    ad_end = html.find(ad_end_marker, cat_top_start)
    # Find end of ad div
    ad_div_end = html.find('</div>', ad_end) + len('</div>')
    
    # Remove from cat-top start to ad end
    html = html[:cat_top_start] + html[ad_div_end:]
    print("Removed cat-top and shelf-row")
else:
    print(f"Markers not found: cat_top={cat_top_start}, grid={old_grid_start}")

open('app/templates/public/category.html', 'w').write(html)
print("Template saved")

# CSS: make cat2-grid work well as uniform grid
css = open('app/static/css/public.css').read()

# Already have cat2-grid: repeat(3, 1fr) and cat2-layout: 1fr 300px
# Just need to make sure shelf-card images have consistent aspect ratio
if '.cat2-grid .shelf-card-img' not in css:
    css += '\n.cat2-grid .shelf-card-img { aspect-ratio:16/10; }\n'
    css += '.cat2-grid .shelf-card-img img { width:100%; height:100%; object-fit:cover; }\n'
    print("Added consistent card aspect ratio")

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')

# Bump version
base = open('app/templates/public/base.html').read()
base = base.replace('public.min.css?v=32.0', 'public.min.css?v=32.1')
open('app/templates/public/base.html', 'w').write(base)

print("Done!")
