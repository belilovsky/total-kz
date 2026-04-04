#!/usr/bin/env python3
"""Create persons table in SQLite from PG entities."""
import sqlite3
import subprocess
import json
import re

# Get person entities from PG
result = subprocess.run([
    'docker', 'exec', 'total_kz_db', 'psql', '-U', 'total_kz', '-d', 'total_kz',
    '-t', '-A', '-c',
    "SELECT json_agg(row_to_json(e)) FROM (SELECT id, name, short_name, normalized, mention_count FROM entities WHERE entity_type='person' ORDER BY mention_count DESC LIMIT 50000) e;"
], capture_output=True, text=True)

data = json.loads(result.stdout.strip())
print(f'Got {len(data)} persons from PG')

conn = sqlite3.connect('data/total.db')

# Drop and recreate
conn.execute('DROP TABLE IF EXISTS persons')
conn.execute('''CREATE TABLE persons (
    id INTEGER PRIMARY KEY,
    slug TEXT UNIQUE,
    short_name TEXT,
    name TEXT,
    person_type TEXT DEFAULT 'person',
    photo_url TEXT,
    current_position TEXT,
    bio TEXT,
    entity_id INTEGER,
    normalized TEXT,
    mention_count INTEGER DEFAULT 0
)''')

# Transliterate for slug
tr = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'shch',
    'ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
    'ә':'a','ғ':'g','қ':'q','ң':'n','ө':'o','ұ':'u','ү':'u','һ':'h','і':'i'
}

def make_slug(name):
    if not name:
        return ''
    s = name.lower()
    out = ''
    for c in s:
        out += tr.get(c, c)
    out = re.sub(r'[^a-z0-9]+', '-', out).strip('-')
    return out

inserted = 0
seen_slugs = set()
for p in data:
    slug = make_slug(p.get('normalized') or p.get('short_name') or p.get('name', ''))
    if not slug or slug in seen_slugs:
        continue
    seen_slugs.add(slug)
    conn.execute(
        'INSERT OR IGNORE INTO persons (id, slug, short_name, name, entity_id, normalized, mention_count) VALUES (?,?,?,?,?,?,?)',
        (p['id'], slug, p.get('short_name') or p['name'], p['name'], p['id'], p.get('normalized'), p.get('mention_count', 0))
    )
    inserted += 1

conn.commit()
total = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
print(f'Inserted {inserted} persons, total: {total}')

# Also create organizations table
conn.execute('DROP TABLE IF EXISTS organizations')
conn.execute('''CREATE TABLE organizations (
    id INTEGER PRIMARY KEY,
    slug TEXT UNIQUE,
    short_name TEXT,
    name TEXT,
    org_type TEXT DEFAULT 'organization',
    entity_id INTEGER,
    normalized TEXT,
    mention_count INTEGER DEFAULT 0
)''')

# Get orgs from PG
result2 = subprocess.run([
    'docker', 'exec', 'total_kz_db', 'psql', '-U', 'total_kz', '-d', 'total_kz',
    '-t', '-A', '-c',
    "SELECT json_agg(row_to_json(e)) FROM (SELECT id, name, short_name, normalized, mention_count FROM entities WHERE entity_type='org' ORDER BY mention_count DESC LIMIT 50000) e;"
], capture_output=True, text=True)

org_data = json.loads(result2.stdout.strip())
print(f'Got {len(org_data)} orgs from PG')

seen_slugs2 = set()
inserted2 = 0
for o in org_data:
    slug = make_slug(o.get('normalized') or o.get('short_name') or o.get('name', ''))
    if not slug or slug in seen_slugs2:
        continue
    seen_slugs2.add(slug)
    conn.execute(
        'INSERT OR IGNORE INTO organizations (id, slug, short_name, name, entity_id, normalized, mention_count) VALUES (?,?,?,?,?,?,?)',
        (o['id'], slug, o.get('short_name') or o['name'], o['name'], o['id'], o.get('normalized'), o.get('mention_count', 0))
    )
    inserted2 += 1

conn.commit()
total2 = conn.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
print(f'Inserted {inserted2} orgs, total: {total2}')

# Add views column if missing
try:
    conn.execute("ALTER TABLE articles ADD COLUMN views INTEGER DEFAULT 0")
    print("Added views column")
except:
    pass

# Create indexes
conn.execute('CREATE INDEX IF NOT EXISTS idx_persons_slug ON persons(slug)')
conn.execute('CREATE INDEX IF NOT EXISTS idx_persons_short_name ON persons(short_name)')
conn.execute('CREATE INDEX IF NOT EXISTS idx_orgs_slug ON organizations(slug)')
conn.commit()
conn.close()
print('Done!')
