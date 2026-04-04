#!/usr/bin/env python3
"""Fix article grid columns: content in col 1, sidebar in col 2."""
import shutil

css = open('app/static/css/public.css').read()

# Hero stays in column 1 (alongside sidebar in column 2) - that's correct
# But content needs explicit column 1, sidebar needs explicit column 2

# Add explicit grid assignments
# Find the article-body-grid CSS section
idx = css.find('.article-body-grid {')
if idx < 0:
    print("ERROR: article-body-grid not found")
    exit(1)

# Find the closing brace
end = css.find('}', idx) + 1

# Check what's after it
after = css[end:end+200]
print(f"After body-grid: {after[:100]}")

# Remove any existing article-content/sidebar rules we added before
css = css.replace('.article-content { min-width:0; grid-column:1; }', '.article-content { min-width:0; }')

# Now add proper rules after the hero image CSS block
hero_end = css.find('.article-hero-image img {')
hero_block_end = css.find('}', hero_end) + 1

insert = """
.article-body-grid > .article-content { grid-column:1; }
.article-body-grid > .article-sidebar { grid-column:2; align-self:start; }
"""

css = css[:hero_block_end] + insert + css[hero_block_end:]
print("Added explicit grid-column rules")

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("Done")
