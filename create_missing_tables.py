#!/usr/bin/env python3
"""Create missing tables in SQLite."""
import sqlite3

conn = sqlite3.connect("data/total.db")

conn.executescript("""
CREATE TABLE IF NOT EXISTS person_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER,
    position TEXT,
    organization TEXT,
    start_date TEXT,
    end_date TEXT,
    is_current INTEGER DEFAULT 0,
    source_url TEXT
);
CREATE INDEX IF NOT EXISTS idx_pp_person ON person_positions(person_id);

CREATE TABLE IF NOT EXISTS public_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    author_name TEXT,
    author_email TEXT,
    body TEXT,
    status TEXT DEFAULT 'pending',
    parent_id INTEGER,
    created_at TEXT,
    ip_address TEXT,
    user_agent TEXT
);
CREATE INDEX IF NOT EXISTS idx_comments_article ON public_comments(article_id);

CREATE TABLE IF NOT EXISTS push_subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT UNIQUE,
    keys_json TEXT,
    created_at TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS article_reactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    reaction_type TEXT,
    ip_hash TEXT,
    created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_reactions_article ON article_reactions(article_id);

CREATE TABLE IF NOT EXISTS article_translations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER UNIQUE,
    title_kz TEXT,
    excerpt_kz TEXT,
    body_html_kz TEXT,
    translated_at TEXT
);

CREATE TABLE IF NOT EXISTS article_nlp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER UNIQUE,
    sentiment TEXT,
    key_facts TEXT,
    key_facts_kz TEXT,
    quote TEXT,
    quote_author TEXT,
    summary TEXT,
    processed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_nlp_article ON article_nlp(article_id);
""")

conn.close()
print("All missing tables created")
