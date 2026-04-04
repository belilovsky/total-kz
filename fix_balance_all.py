#!/usr/bin/env python3
"""
Fix balance_divs filter to handle ALL block-level tags, not just div.
Also strip orphan closing tags that have no matching opener.
"""

content = open('app/public_routes.py').read()

old_func = '''def _balance_divs(html):
    """Remove trailing extra </div> tags from scraped body_html."""
    import re
    if not html:
        return html
    opens = len(re.findall(r'<div\\b', html))
    closes = len(re.findall(r'</div>', html))
    if closes > opens:
        for _ in range(closes - opens):
            idx = html.rfind('</div>')
            if idx >= 0:
                html = html[:idx] + html[idx+6:]
    return html'''

new_func = '''def _balance_divs(html):
    """Remove orphan closing tags from scraped body_html to prevent layout breaks."""
    import re
    if not html:
        return html
    # Fix all block-level tags that might break the grid layout
    for tag in ['div', 'blockquote', 'section', 'article', 'aside', 'figure', 'header', 'footer', 'nav', 'main']:
        opens = len(re.findall(rf'<{tag}\\b', html, re.IGNORECASE))
        closes = len(re.findall(rf'</{tag}>', html, re.IGNORECASE))
        if closes > opens:
            # Remove extra closing tags from the end
            diff = closes - opens
            for _ in range(diff):
                idx = html.rfind(f'</{tag}>')
                if idx >= 0:
                    html = html[:idx] + html[idx+len(f'</{tag}>'):]
        elif opens > closes:
            # Add missing closing tags at the end
            diff = opens - closes
            html += f'</{tag}>' * diff
    return html'''

if old_func in content:
    content = content.replace(old_func, new_func)
    open('app/public_routes.py', 'w').write(content)
    print("Updated _balance_divs to handle all block tags")
else:
    print("Old function not found")
    # Show what's there
    idx = content.find('def _balance_divs')
    if idx >= 0:
        print(content[idx:idx+500])
    else:
        print("_balance_divs not found at all")
