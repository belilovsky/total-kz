#!/usr/bin/env python3
"""
Fetch biographical data for top persons from online.zakon.kz
Searches for person by name → extracts resume → saves to DB.

Usage:
  python scraper/fetch_zakon_bios.py           # process all government persons without zakon data
  python scraper/fetch_zakon_bios.py --limit 5  # process only 5
"""
import sqlite3, json, re, sys, time, argparse
from pathlib import Path
import httpx
from selectolax.parser import HTMLParser

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"
BASE_URL = "https://online.zakon.kz"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru,en;q=0.9",
}

def search_person(name: str, client: httpx.Client) -> list[dict]:
    """Search zakon.kz for a person by name, return list of results."""
    # Use the search API
    url = f"{BASE_URL}/api/search/person"
    params = {"q": name, "limit": 5}
    try:
        r = client.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return data
    except Exception:
        pass
    
    # Fallback: try the general search
    try:
        url2 = f"{BASE_URL}/mcl/search?q={name}&type=person"
        r2 = client.get(url2, timeout=15, follow_redirects=True)
        if r2.status_code == 200:
            tree = HTMLParser(r2.text)
            results = []
            for link in tree.css("a[href*='doc_id=']"):
                href = link.attributes.get("href", "")
                match = re.search(r'doc_id=(\d+)', href)
                if match:
                    results.append({
                        "doc_id": match.group(1),
                        "title": link.text(strip=True),
                    })
            return results
    except Exception:
        pass
    return []


def fetch_bio_page(doc_id: str, client: httpx.Client) -> str:
    """Fetch the biography page and return raw HTML."""
    url = f"{BASE_URL}/Document/?doc_id={doc_id}"
    r = client.get(url, timeout=30, follow_redirects=True)
    return r.text if r.status_code == 200 else ""


def parse_bio(html: str) -> dict:
    """Parse biography from zakon.kz HTML page. Returns structured data."""
    tree = HTMLParser(html)
    
    result = {
        "full_name": "",
        "birth_date": "",
        "birth_place": "",
        "education": [],
        "career": [],
        "awards": [],
        "languages": "",
        "bio_summary": "",
        "photo_url": "",
    }
    
    # Try to find the resume content - zakon.kz uses table-based layout
    # Look for key markers in text
    text = tree.body.text() if tree.body else ""
    
    # Extract photo
    for img in tree.css("img"):
        src = img.attributes.get("src", "")
        if "photo" in src.lower() or "person" in src.lower() or "/uploads/" in src:
            if not src.startswith("http"):
                src = BASE_URL + src
            result["photo_url"] = src
            break
    
    # Parse structured sections from text
    lines = text.split("\n")
    lines = [l.strip() for l in lines if l.strip()]
    
    current_section = ""
    for line in lines:
        line_lower = line.lower()
        
        # Detect sections
        if "дата рождения" in line_lower:
            current_section = "birth"
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', line)
            if date_match:
                result["birth_date"] = date_match.group(1)
            continue
        elif "место рождения" in line_lower:
            current_section = "birthplace"
            place = line.split(":", 1)[-1].strip() if ":" in line else ""
            if place:
                result["birth_place"] = place
            continue
        elif "образование" in line_lower and ("специальность" in line_lower or "квалификация" in line_lower or len(line) < 60):
            current_section = "education"
            continue
        elif "трудовой стаж" in line_lower or "трудовая деятельность" in line_lower:
            current_section = "career"
            continue
        elif "владение языками" in line_lower or "языки" in line_lower and len(line) < 40:
            current_section = "languages"
            lang_val = line.split(":", 1)[-1].strip() if ":" in line else ""
            if lang_val:
                result["languages"] = lang_val
            continue
        elif "награды" in line_lower or "государственные и международные награды" in line_lower:
            current_section = "awards"
            continue
        elif "научные" in line_lower and "степен" in line_lower:
            current_section = "science"
            continue
        elif "выборные должности" in line_lower or "депутатство" in line_lower:
            current_section = "elected"
            continue
        elif "партийная" in line_lower:
            current_section = "party"
            continue
        elif "прочие должности" in line_lower:
            current_section = "other_positions"
            continue
        elif "последняя должность" in line_lower:
            current_section = "current_pos"
            pos = line.split(":", 1)[-1].strip() if ":" in line else ""
            if pos:
                result["current_position"] = pos
            continue
        
        # Collect data based on section
        if current_section == "education" and line.startswith("•"):
            result["education"].append(line.lstrip("• ").strip())
        elif current_section == "career" and line.startswith("•"):
            result["career"].append(line.lstrip("• ").strip())
        elif current_section == "awards" and (line.startswith("•") or line.startswith("Орден")):
            result["awards"].append(line.lstrip("• ").strip())
        elif current_section == "languages" and not result["languages"]:
            result["languages"] = line.lstrip("• ").strip()
        elif current_section == "birthplace" and not result["birth_place"]:
            result["birth_place"] = line.lstrip("• ").strip()
    
    # Extract full name from first non-empty significant line
    for line in lines:
        if len(line) > 5 and len(line) < 80 and not any(x in line.lower() for x in ["сервис", "юрист", "бухгалтер", "zakon", "online", "параграф"]):
            # Check if it looks like a name (2-4 words, Cyrillic)
            words = line.split()
            if 2 <= len(words) <= 5 and all(re.match(r'^[А-ЯЁа-яёІіҚқҒғӘәҮүҰұҺһӨөҢңЫы\-]+$', w) for w in words):
                result["full_name"] = line
                break
    
    return result


def update_person_db(person_id: int, bio: dict, doc_id: str, conn: sqlite3.Connection):
    """Update person record with extracted bio data."""
    updates = {}
    
    if bio.get("birth_date"):
        # Convert DD.MM.YYYY to YYYY-MM-DD
        parts = bio["birth_date"].split(".")
        if len(parts) == 3:
            updates["birth_date"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
    
    if bio.get("birth_place"):
        updates["birth_place"] = bio["birth_place"]
    
    if bio.get("photo_url"):
        updates["photo_url"] = bio["photo_url"]
    
    if bio.get("languages"):
        updates["languages"] = bio["languages"]
    
    if bio.get("education"):
        updates["education"] = "; ".join(bio["education"])
    
    if bio.get("awards"):
        updates["awards"] = "; ".join(bio["awards"])
    
    if bio.get("full_name"):
        updates["full_name"] = bio["full_name"]
    
    updates["zakon_doc_id"] = doc_id
    updates["updated_at"] = "datetime('now')"
    
    if updates:
        set_parts = []
        values = []
        for k, v in updates.items():
            if k == "updated_at":
                set_parts.append(f"{k} = datetime('now')")
            else:
                set_parts.append(f"{k} = ?")
                values.append(v)
        values.append(person_id)
        
        sql = f"UPDATE persons SET {', '.join(set_parts)} WHERE id = ?"
        conn.execute(sql, values)
    
    # Insert career positions
    if bio.get("career"):
        # Clear existing positions from zakon source
        conn.execute("DELETE FROM person_positions WHERE person_id = ? AND source = 'zakon'", (person_id,))
        
        for i, entry in enumerate(bio["career"]):
            # Parse career entry: "Position (dates)" or "Org: position (dates)"
            pos_title = entry
            org = ""
            start = ""
            end = ""
            
            # Try to extract dates
            date_match = re.search(r'\(([^)]+)\)', entry)
            if date_match:
                dates_str = date_match.group(1)
                pos_title = entry[:date_match.start()].strip().rstrip(",")
                
                # Parse date ranges
                if "–" in dates_str or "-" in dates_str:
                    parts = re.split(r'[–\-]', dates_str)
                    start = parts[0].strip()
                    end = parts[1].strip() if len(parts) > 1 else ""
                elif dates_str.strip().startswith("с "):
                    start = dates_str.strip()[2:]
                else:
                    start = dates_str.strip()
            
            # Split org and position
            if ":" in pos_title:
                org, pos_title = pos_title.split(":", 1)
                org = org.strip()
                pos_title = pos_title.strip()
            
            conn.execute("""
                INSERT INTO person_positions (person_id, position_title, organization, start_date, end_date, sort_order, source)
                VALUES (?, ?, ?, ?, ?, ?, 'zakon')
            """, (person_id, pos_title, org, start, end, i))
    
    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--person-id", type=int, default=0)
    args = parser.parse_args()
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    # Check if person_positions has 'source' column
    cols = [r[1] for r in conn.execute("PRAGMA table_info(person_positions)").fetchall()]
    if "source" not in cols:
        conn.execute("ALTER TABLE person_positions ADD COLUMN source TEXT DEFAULT 'seed'")
        conn.commit()
        print("Added 'source' column to person_positions")
    
    # Get persons to process
    if args.person_id:
        persons = conn.execute("SELECT * FROM persons WHERE id = ?", (args.person_id,)).fetchall()
    else:
        persons = conn.execute("""
            SELECT p.*, COUNT(ae.article_id) as ac
            FROM persons p
            LEFT JOIN article_entities ae ON p.entity_id = ae.entity_id
            WHERE p.person_type = 'government'
            AND (p.zakon_doc_id IS NULL OR p.zakon_doc_id = '')
            AND p.birth_date IS NULL
            GROUP BY p.id
            ORDER BY ac DESC
        """).fetchall()
    
    if args.limit:
        persons = persons[:args.limit]
    
    print(f"Processing {len(persons)} persons...")
    
    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30)
    
    success = 0
    failed = 0
    
    for p in persons:
        name = p["short_name"] or p["full_name"]
        print(f"\n{'='*60}")
        print(f"[{p['id']}] {name} ({p['current_position'] or 'no position'})")
        
        # Search on zakon.kz
        results = search_person(name, client)
        
        if not results:
            # Try with full_name if different
            if p["full_name"] and p["full_name"] != name:
                results = search_person(p["full_name"], client)
        
        if results:
            # Pick best match
            doc_id = results[0].get("doc_id", "")
            print(f"  Found doc_id: {doc_id}")
            
            if doc_id:
                html = fetch_bio_page(doc_id, client)
                if html:
                    bio = parse_bio(html)
                    print(f"  Birth: {bio.get('birth_date', '?')}, Career entries: {len(bio.get('career', []))}")
                    update_person_db(p["id"], bio, doc_id, conn)
                    success += 1
                else:
                    print(f"  Failed to fetch page")
                    failed += 1
            else:
                print(f"  No doc_id in results")
                failed += 1
        else:
            print(f"  Not found on zakon.kz")
            failed += 1
        
        time.sleep(1)  # Be polite
    
    client.close()
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"Done: {success} updated, {failed} failed/not found")


if __name__ == "__main__":
    main()
