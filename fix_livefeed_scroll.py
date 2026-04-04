#!/usr/bin/env python3
"""Fix live-feed scroll on all viewports."""
import shutil

css = open('app/static/css/public.css').read()

# 1. Desktop: live-feed needs a fixed max-height for scroll to work
# The container has overflow-y:auto and max-height:100%, but 100%
# of parent is unconstrained. Use a calc based on hero height.
old_feed = """.live-feed {
  background: var(--surface);
  border: 1px solid var(--border-light);
  border-radius: var(--radius-lg);
  padding: var(--space-3);
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  min-height: 0;
  max-height: 100%;
}"""

new_feed = """.live-feed {
  background: var(--surface);
  border: 1px solid var(--border-light);
  border-radius: var(--radius-lg);
  padding: var(--space-3);
  overflow: hidden;
  display: flex;
  flex-direction: column;
  min-height: 0;
  max-height: 520px;
}"""

if old_feed in css:
    css = css.replace(old_feed, new_feed)
    print("1. Fixed desktop live-feed max-height to 520px")
else:
    print("1. Desktop live-feed pattern not found")

# 2. Make live-feed__list scroll within
old_list = """.live-feed__list {
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  overflow-y: auto;
  scrollbar-width: thin;
  scrollbar-color: var(--border-light) transparent;
}"""

new_list = """.live-feed__list {
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  overflow-y: auto;
  -webkit-overflow-scrolling: touch;
  scrollbar-width: thin;
  scrollbar-color: var(--border-light) transparent;
}"""

if old_list in css:
    css = css.replace(old_list, new_list)
    print("2. Added touch scrolling to feed list")
else:
    print("2. Feed list pattern not found")

# 3. Mobile: enforce 400px max-height (enough for ~8 items)
old_mobile = """  .live-feed {
    max-width: 100%;
    max-height: 300px;
  }"""

new_mobile = """  .live-feed {
    max-width: 100%;
    max-height: 400px;
    overflow: hidden;
  }"""

if old_mobile in css:
    css = css.replace(old_mobile, new_mobile)
    print("3. Fixed mobile live-feed max-height to 400px")
else:
    print("3. Mobile live-feed pattern not found")

open('app/static/css/public.css', 'w').write(css)
shutil.copy('app/static/css/public.css', 'app/static/css/public.min.css')
print("CSS saved")
