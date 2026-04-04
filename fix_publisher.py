#!/usr/bin/env python3
"""Update publisher.py with improved og:image extraction + credit."""

c = open('services/news-monitor/publisher.py').read()

# Replace the extract_og_image function
old = '''def extract_og_image(url: str) -> str | None:
    """Extract og:image from source article."""
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Total.kz Bot"})
        match = re.search(r'<meta\\s+property="og:image"\\s+content="([^"]+)"', resp.text)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None'''

new = '''def extract_og_image(url: str) -> tuple:
    """Extract og:image and credit from source article."""
    from urllib.parse import urlparse
    UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": UA}, allow_redirects=True)
        html = resp.text[:80000]
        image = None
        for pat in [
            r"""property=["']og:image["'][^>]*?content=["']([^"']+)["']""",
            r"""content=["']([^"']+)["'][^>]*?property=["']og:image["']""",
            r"""name=["']twitter:image["'][^>]*?content=["']([^"']+)["']""",
        ]:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                image = m.group(1)
                if image.startswith("//"):
                    image = "https:" + image
                break
        if not image:
            return None, None
        credit = None
        cm = re.search(r"""property=["']og:site_name["'][^>]*?content=["']([^"']+)["']""", html)
        if cm:
            credit = cm.group(1).strip()
        if not credit:
            credit = urlparse(url).netloc.replace("www.", "")
        return image, "\\u0424\\u043e\\u0442\\u043e: " + credit
    except Exception:
        return None, None'''

if old in c:
    c = c.replace(old, new)
    print("1. Replaced extract_og_image function")
else:
    print("1. Function pattern not found")

# Update the call site
old_call = "main_image = extract_og_image(original_url)"
new_call = "main_image, image_credit = extract_og_image(original_url)"
if old_call in c:
    c = c.replace(old_call, new_call)
    print("2. Updated call site")

# Add image_credit to INSERT
if "image_credit" not in c.split("INSERT INTO")[1][:200] if "INSERT INTO" in c else "":
    # Add image_credit column to INSERT
    old_insert = "author, status, pub_date, imported_at, editor_note, main_image)"
    new_insert = "author, status, pub_date, imported_at, editor_note, main_image, image_credit)"
    c = c.replace(old_insert, new_insert)
    
    old_values = "(%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)"
    new_values = "(%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s)"
    c = c.replace(old_values, new_values)
    
    # Add image_credit to values tuple - find the main_image value and add after
    old_vals_end = """                main_image,
            ),"""
    new_vals_end = """                main_image,
                image_credit,
            ),"""
    c = c.replace(old_vals_end, new_vals_end)
    print("3. Added image_credit to INSERT")

open('services/news-monitor/publisher.py', 'w').write(c)
print("Done!")
