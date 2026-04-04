#!/usr/bin/env python3
"""Sync article_entities from PG to SQLite."""
import sqlite3
import subprocess

conn = sqlite3.connect("data/total.db")

conn.execute("DROP TABLE IF EXISTS article_entities")
conn.execute("""CREATE TABLE article_entities (
    article_id INTEGER,
    entity_id INTEGER,
    mention_count INTEGER DEFAULT 1,
    PRIMARY KEY (article_id, entity_id)
)""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_ae_article ON article_entities(article_id)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_ae_entity ON article_entities(entity_id)")

# Use COPY for speed
print("Exporting from PG...")
result = subprocess.run([
    'docker', 'exec', 'total_kz_db', 'psql', '-U', 'total_kz', '-d', 'total_kz',
    '-t', '-A', '-F', '\t', '-c',
    "SELECT article_id, entity_id, mention_count FROM article_entities;"
], capture_output=True, text=True)

rows = []
for line in result.stdout.strip().split('\n'):
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) >= 3:
        try:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2])))
        except ValueError:
            continue

print(f"Parsed {len(rows)} rows")

# Insert in bulk
batch = 10000
for i in range(0, len(rows), batch):
    conn.executemany(
        "INSERT OR IGNORE INTO article_entities (article_id, entity_id, mention_count) VALUES (?,?,?)",
        rows[i:i+batch]
    )
    if i % 100000 == 0:
        print(f"  Inserted {i + min(batch, len(rows)-i)}...")

conn.commit()
total = conn.execute("SELECT COUNT(*) FROM article_entities").fetchone()[0]
print(f"Done! Total in SQLite: {total}")
conn.close()
