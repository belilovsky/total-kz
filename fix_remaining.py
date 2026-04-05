#!/usr/bin/env python3
"""Fix remaining issues found during self-check."""
import sqlite3

# === 1. Create all missing SQLite tables ===
conn = sqlite3.connect('data/total.db')

tables = """
CREATE TABLE IF NOT EXISTS article_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    parent_id INTEGER,
    author_name TEXT,
    author_email TEXT,
    body TEXT,
    status TEXT DEFAULT 'pending',
    ip_address TEXT,
    user_agent TEXT,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_ac_article ON article_comments(article_id);

CREATE TABLE IF NOT EXISTS article_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    title TEXT,
    body_html TEXT,
    editor_id INTEGER,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    slug TEXT UNIQUE,
    parent_id INTEGER,
    sort_order INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS authors_managed (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    slug TEXT UNIQUE,
    email TEXT,
    bio TEXT,
    photo_url TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT,
    url TEXT,
    mime_type TEXT,
    size INTEGER,
    width INTEGER,
    height INTEGER,
    alt_text TEXT,
    article_id INTEGER,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT,
    started_at TEXT,
    finished_at TEXT,
    articles_found INTEGER DEFAULT 0,
    articles_new INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    url TEXT,
    title TEXT,
    status TEXT,
    error TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    username TEXT,
    action TEXT,
    object_type TEXT,
    object_id INTEGER,
    details TEXT,
    ip_address TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    email TEXT,
    password_hash TEXT,
    role TEXT DEFAULT 'editor',
    is_active INTEGER DEFAULT 1,
    created_at TEXT,
    last_login TEXT
);
"""

conn.executescript(tables)
conn.commit()

# Verify
all_tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print(f"SQLite tables: {len(all_tables)}")
for t in ['article_comments', 'article_revisions', 'categories', 'authors_managed', 'media', 'audit_log', 'users']:
    if t in all_tables:
        count = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
        print(f"  {t}: {count} rows")
    else:
        print(f"  {t}: MISSING!")

conn.close()

# === 2. Add /admin/references redirect to /admin/categories ===
main = open('app/main.py').read()

if '/admin/references' not in main:
    # Find a good place to add the redirect
    idx = main.find('@app.get("/admin/categories"')
    if idx > 0:
        redirect_code = '''
@app.get("/admin/references")
async def admin_references_redirect():
    """Redirect /admin/references to /admin/categories."""
    from starlette.responses import RedirectResponse
    return RedirectResponse(url="/admin/categories", status_code=301)

'''
        main = main[:idx] + redirect_code + main[idx:]
        open('app/main.py', 'w').write(main)
        print("\n2. Added /admin/references -> /admin/categories redirect")
    else:
        print("\n2. /admin/categories route not found")
else:
    print("\n2. /admin/references already exists")

print("\nDone!")
