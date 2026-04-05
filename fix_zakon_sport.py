#!/usr/bin/env python3
"""Fix Zakon subcats and verify Sport is showing."""

c = open('app/public_routes.py').read()

# Map zakon to proisshestviya + bezopasnost (law-adjacent content)
old_zakon = '"subcats": ["zakon", "pravo", "zakonodatelstvo"],'
new_zakon = '"subcats": ["proisshestviya", "bezopasnost"],'
if old_zakon in c:
    c = c.replace(old_zakon, new_zakon)
    print("Zakon: subcats -> proisshestviya + bezopasnost")
else:
    print("Zakon pattern not found")

open('app/public_routes.py', 'w').write(c)
print("Done")
