#!/usr/bin/env python3
"""Fix audit issues batch 1: Sport images, Zakon categories, Sport section, API suggest."""
import re, shutil

# === 1. Fix imgproxy_url to decode HTML entities (fixes Sport + other broken images) ===
routes = open('app/public_routes.py').read()

old_imgproxy = '''def imgproxy_url(source_url: str, width: int = 800) -> str:
    """Generate imgproxy URL for an image."""
    if not source_url:
        return ""
    # Decode HTML entities (e.g. &amp; -> &)
    from html import unescape
    source_url = unescape(source_url)
    # Convert local /img/ paths back to origin URLs for imgproxy
    if source_url.startswith("/img/"):
        source_url = f"https://total.kz/storage/{source_url[5:]}"
    if not source_url.startswith("http"):
        return source_url
    return f"/imgproxy/insecure/resize:fit:{width}:0/plain/{source_url}@webp"'''

# Check if already fixed
if 'unescape' in routes.split('def imgproxy_url')[1][:300]:
    print("1. imgproxy_url already has unescape")
else:
    old = '''def imgproxy_url(source_url: str, width: int = 800) -> str:
    """Generate imgproxy URL for an image."""
    if not source_url:
        return ""
    # Convert local /img/ paths back to origin URLs for imgproxy
    if source_url.startswith("/img/"):
        source_url = f"https://total.kz/storage/{source_url[5:]}"
    if not source_url.startswith("http"):
        return source_url
    return f"/imgproxy/insecure/resize:fit:{width}:0/plain/{source_url}@webp"'''
    
    new = '''def imgproxy_url(source_url: str, width: int = 800) -> str:
    """Generate imgproxy URL for an image."""
    from html import unescape as _unescape
    if not source_url:
        return ""
    source_url = _unescape(source_url)  # fix &amp; in URLs from DB
    # Convert local /img/ paths back to origin URLs for imgproxy
    if source_url.startswith("/img/"):
        source_url = f"https://total.kz/storage/{source_url[5:]}"
    if not source_url.startswith("http"):
        return source_url
    return f"/imgproxy/insecure/resize:fit:{width}:0/plain/{source_url}@webp"'''
    
    if old in routes:
        routes = routes.replace(old, new)
        print("1. Fixed imgproxy_url: added HTML unescape")
    else:
        print("1. imgproxy_url pattern not found")

# === 2. Also fix HTML entities in DB directly ===
# Will do via SQL after this script

# === 3. Fix API Suggest (422 error) ===
# Find the suggest endpoint
suggest_match = re.search(r'@app\.(get|post)\(["\'].*?suggest.*?["\']\)', routes)
if suggest_match:
    print(f"4. Found suggest endpoint: {suggest_match.group()}")
else:
    print("4. Suggest endpoint not found - checking...")
    # Search more broadly
    if 'suggest' in routes:
        idx = routes.find('suggest')
        print(f"   Found 'suggest' at pos {idx}: ...{routes[idx-20:idx+80]}...")

# === 4. Fix "Читайте также" to exclude articles without images ===
# Find the related articles query and add WHERE main_image IS NOT NULL
read_also = routes.find('ЧИТАЙТЕ ТАКЖЕ')
if read_also < 0:
    read_also = routes.find('read_also')
if read_also < 0:
    read_also = routes.find('related_articles')
if read_also > 0:
    print(f"7. Found related articles at pos {read_also}")

open('app/public_routes.py', 'w').write(routes)

# === 5. Fix homepage Zakon section + add Sport section ===
home = open('app/templates/public/home.html').read()

# Check how categories are rendered
zakon_match = home.find('zakon')
if zakon_match > 0:
    print(f"2. Found zakon in home template at pos {zakon_match}")
else:
    print("2. 'zakon' not found in home template - checking category sections...")
    # Find category section pattern
    cat_sections = re.findall(r'cat_sections|category_sections|sections\[', home)
    print(f"   Found patterns: {cat_sections}")

print("\nBatch 1 script done. Manual fixes needed for suggest API and category sections.")
