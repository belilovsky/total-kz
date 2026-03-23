#!/usr/bin/env python3
"""
Find zakon.kz doc_ids for persons by searching Google.
Uses search_web-equivalent approach via httpx.
"""
import sqlite3, json, re, sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"

# Known doc_ids from manual research
KNOWN_IDS = {
    "Касым-Жомарт Токаев": "30100479",
    "Олжас Бектенов": "37121274",
    "Нурсултан Назарбаев": "30009100",
    "Серик Жумангарин": "37124432",
    "Алихан Смаилов": "36766940",
    "Тимур Сулейменов": "36283756",
    "Ерболат Досаев": "33024264",
    "Канат Бозумбаев": "33024290",
    "Марат Карабаев": "37121267",
    "Аида Балаева": "38282356",
    "Алмасадам Саткалиев": "37121270",
    "Канат Шарлапаев": "38282349",
    "Айдарбек Сапаров": "37121262",
    "Жаслан Мадиев": "38283124",
    "Акмарал Альназарова": "38282362",
    "Ерлан Аккенженов": "38283190",
    "Маулен Ашимбаев": "33024286",
    "Нуржан Нуржигитов": "38282365",
    "Гани Бейсембаев": "38283183",
    "Садыр Жапаров": None,  # Kyrgyz president, no KZ resume
    "Шавкат Мирзиеев": None,  # Uzbek president
    "Владимир Путин": None,  # Russian president
}

def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    persons = conn.execute("""
        SELECT id, short_name, full_name, zakon_doc_id 
        FROM persons 
        WHERE person_type = 'government'
        ORDER BY id
    """).fetchall()
    
    updated = 0
    for p in persons:
        name = p["short_name"]
        if p["zakon_doc_id"]:
            continue  # already has ID
        
        doc_id = KNOWN_IDS.get(name)
        if doc_id:
            conn.execute("UPDATE persons SET zakon_doc_id = ?, updated_at = datetime('now') WHERE id = ?",
                        (doc_id, p["id"]))
            print(f"  [+] {name} → doc_id={doc_id}")
            updated += 1
        elif doc_id is None and name in KNOWN_IDS:
            print(f"  [-] {name} — no KZ resume (foreign leader)")
        else:
            print(f"  [?] {name} — need to find doc_id")
    
    conn.commit()
    conn.close()
    print(f"\nUpdated {updated} persons with doc_ids")


if __name__ == "__main__":
    main()
