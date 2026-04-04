#!/usr/bin/env python3
"""Fix 6 article page issues at once."""
import re, shutil

# === 1. TEMPLATE: article.html ===
html = open('app/templates/public/article.html').read()

# 1) Remove article title from breadcrumbs
# Find: <span>{{ article.title[:60] }}...</span> or similar in breadcrumbs
# Replace breadcrumb article title with just category
bc_pattern = r'(<span class="sep">›</span>\s*\n\s*<span>)\{\{ article\.title[^}]*\}\}(</span>)'
html = re.sub(bc_pattern, '', html)
# Also try simpler patterns
if 'article.title' in html.split('breadcrumbs')[0] if 'breadcrumbs' in html else '':
    pass
# Actually find the breadcrumb block
bc_start = html.find('class="breadcrumbs"')
if bc_start > 0:
    bc_end = html.find('</nav>', bc_start)
    bc_block = html[bc_start:bc_end]
    # Find last <span> with article.title
    last_sep = bc_block.rfind('<span class="sep">')
    if last_sep > 0 and 'article.title' in bc_block[last_sep:]:
        # Remove from last separator to end of block
        new_bc = bc_block[:last_sep].rstrip()
        html = html[:bc_start] + new_bc + html[bc_start + len(bc_block):]
        print("1. Removed article title from breadcrumbs")
    else:
        print("1. Breadcrumb title pattern not found in block")
else:
    print("1. Breadcrumbs not found")

# 2) Add image credit overlay on photo
# Find article-hero-image figure and add credit
hero_fig = html.find('<figure class="article-hero-image')
if hero_fig > 0:
    # Find </figure> after hero
    fig_end = html.find('</figure>', hero_fig)
    # Insert credit overlay before </figure>
    credit_html = '''
      {% if article.image_credit %}
      <span class="hero-credit">{{ article.image_credit }}</span>
      {% endif %}'''
    if 'hero-credit' not in html:
        html = html[:fig_end] + credit_html + '\n    ' + html[fig_end:]
        print("2. Added image credit overlay")
    else:
        print("2. Credit already exists")

# 3) Remove drop cap (буквица) - find dropcap CSS class usage
# The dropcap is added via CSS ::first-letter or a class
# Check for first-letter in template or special span
if 'dropcap' in html or 'drop-cap' in html:
    html = html.replace('class="dropcap"', '')
    html = html.replace('class="drop-cap"', '')
    print("3. Removed dropcap class from template")
else:
    print("3. No dropcap class in template (CSS-only)")

# 4) Change "Сейчас читают" from 5 to 10 items
# Find the popular_in_sidebar or sidebar popular limit
old_pop5 = 'popular_articles[:5]'
old_pop5b = 'popular[:5]'
if old_pop5 in html:
    html = html.replace(old_pop5, 'popular_articles[:10]')
    print("4. Сейчас читают: 5 -> 10")
elif old_pop5b in html:
    html = html.replace(old_pop5b, 'popular[:10]')
    print("4. Сейчас читают: 5 -> 10")
else:
    print("4. Popular limit not found in template, checking route")

open('app/templates/public/article.html', 'w').write(html)

# Also check route for popular limit
routes = open('app/public_routes.py').read()
if 'popular_articles=popular_articles[:5]' in routes:
    routes = routes.replace('popular_articles=popular_articles[:5]', 'popular_articles=popular_articles[:10]')
    open('app/public_routes.py', 'w').write(routes)
    print("4b. Route: popular 5 -> 10")
elif 'LIMIT 5' in routes and 'popular' in routes:
    # Find the popular query
    pop_query_match = re.search(r"(popular.*?LIMIT\s+)5", routes, re.DOTALL)
    if pop_query_match:
        print("4c. Found popular LIMIT 5 in route")

# === 2. CSS fixes ===
css = open('app/static/css/public.css').read()

# 3) Remove drop cap styles
dropcap_patterns = [
    r'\.article-body\s*>\s*p:first-of-type::first-letter\s*\{[^}]+\}',
    r'\.dropcap[^{]*\{[^}]+\}',
    r'\.drop-cap[^{]*\{[^}]+\}',
    r'\.article-body\s+p:first-child::first-letter\s*\{[^}]+\}',
]
for pat in dropcap_patterns:
    m = re.search(pat, css)
    if m:
        css = css.replace(m.group(), '/* dropcap removed */')
        print(f"3b. Removed dropcap CSS: {m.group()[:50]}...")

# 2) Hero credit overlay style
if '.hero-credit' not in css:
    credit_css = """
.hero-credit {
  position:absolute;
  bottom:8px;
  right:12px;
  font-size:10px;
  color:rgba(255,255,255,.7);
  background:rgba(0,0,0,.4);
  padding:2px 8px;
  border-radius:4px;
  z-index:2;
}
"""
    # Insert after article-hero-image rule
    idx = css.find('.article-hero-image img {')
    end = css.find('}', idx) + 1
    css = css[:end] + credit_css + css[end:]
    print("2b. Added hero-credit CSS")

# Also make sure figure is position:relative for the overlay
if 'article-hero-image' in css:
    old_hero = '.article-hero-image {\n  margin: 0 0 var(--space-4) 0;\n}'
    new_hero = '.article-hero-image {\n  margin: 0 0 var(--space-4) 0;\n  position:relative;\n}'
    css = css.replace(old_hero, new_hero)
    print("2c. Added position:relative to hero figure")

# 5) Gap after tags before chronology - find and reduce
# Look for article-keywords margin-bottom or gap
if 'article-keywords' in css:
    m_kw = re.search(r'\.article-keywords\s*\{([^}]+)\}', css)
    if m_kw and 'margin-bottom' in m_kw.group():
        old_kw = m_kw.group()
        new_kw = re.sub(r'margin-bottom:[^;]+;', 'margin-bottom:var(--space-4);', old_kw)
        css = css.replace(old_kw, new_kw)
        print("5. Reduced keywords margin-bottom")

# Also check engage-bar margin
m_eb = re.search(r'\.engage-bar\s*\{([^}]+)\}', css)
if m_eb and 'margin' in m_eb.group():
    old_eb = m_eb.group()
    new_eb = re.sub(r'margin-bottom:\s*var\(--space-\d+\)', 'margin-bottom:var(--space-3)', old_eb)
    css = css.replace(old_eb, new_eb)
    print("5b. Reduced engage-bar margin")

# Bump version
base = open('app/templates/public/base.html').read()
base = base.replace('public.min.css?v=32.2', 'public.min.css?v=33.0')
open('app/templates/public/base.html', 'w').write(base)

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("\nAll done! CSS v33.0")
