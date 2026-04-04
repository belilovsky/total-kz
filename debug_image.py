#!/usr/bin/env python3
import requests, re

url = "https://www.aljazeera.com/news/2026/4/4/iran-war-what-is-happening-on-day-36-of-us-israeli-attacks?traffic_source=rss"
headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

try:
    resp = requests.get(url, timeout=15, headers=headers, allow_redirects=True)
    print(f"Status: {resp.status_code}")
    print(f"Length: {len(resp.text)}")
    html = resp.text[:50000]
    
    # Search for og:image
    idx = html.find('og:image')
    if idx >= 0:
        print(f"og:image at position {idx}")
        print(f"Context: {html[idx-50:idx+200]}")
    else:
        print("No og:image found")
    
    # Try patterns
    patterns = [
        r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
        r'property="og:image"\s+content="([^"]+)"',
        r"property='og:image'\s+content='([^']+)'",
    ]
    for i, pat in enumerate(patterns):
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            print(f"Pattern {i} matched: {m.group(1)[:100]}")
            break
    else:
        print("No pattern matched")
        
except Exception as e:
    print(f"Error: {e}")
