#!/usr/bin/env python3
"""Sync article_entities from PG to SQLite for person/org pages."""
import sqlite3
import subprocess
import json

# Get article_entities from PG (only person/org entities)
print("Fetching article_entities from PG...")
result = subprocess.run([
    'docker', 'exec', 'total_kz_db', 'psql', '-U', 'total_kz', '-d', 'total_kz',
    '-t', '-A', '-c',
    """SELECT count(*) FROM article_entities;"""
], capture_output=True, text=True)
total = int(result.stdout.strip())
print(f"Total article_entities in PG: {total}")

conn = sqlite3.connect("data/total.db")

# Recreate article_entities table
conn.execute("DROP TABLE IF EXISTS article_entities")
conn.execute("""CREATE TABLE article_entities (
    id INTEGER PRIMARY KEY,
    article_id INTEGER,
    entity_id INTEGER,
    entity_type TEXT,
    mention_count INTEGER DEFAULT 1
)""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_ae_article ON article_entities(article_id)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_ae_entity ON article_entities(entity_id)")

# Fetch in batches
batch_size = 50000
offset = 0
total_inserted = 0

while offset < total:
    print(f"Fetching batch at offset {offset}...")
    result = subprocess.run([
        'docker', 'exec', 'total_kz_db', 'psql', '-U', 'total_kz', '-d', 'total_kz',
        '-t', '-A', '-F', '|', '-c',
        f"SELECT id, article_id, entity_id FROM article_entities ORDER BY id LIMIT {batch_size} OFFSET {offset};"
    ], capture_output=True, text=True)
    
    rows = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split('|')
        if len(parts) >= 3:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2])))
    
    if not rows:
        break
    
    conn.executemany(
        "INSERT OR IGNORE INTO article_entities (id, article_id, entity_id) VALUES (?,?,?)",
        rows
    )
    conn.commit()
    total_inserted += len(rows)
    offset += batch_size
    print(f"  Inserted {len(rows)}, total: {total_inserted}")

print(f"Done! Total article_entities in SQLite: {conn.execute('SELECT COUNT(*) FROM article_entities').fetchone()[0]}")
conn.close()
