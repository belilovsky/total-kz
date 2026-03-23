#!/usr/bin/env python3
"""
Import biographical data from zakon.kz wide_browse CSV into persons DB.
Also imports Tokayev data from the browser_task JSON.
"""
import csv, sqlite3, re, json
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"
CSV_PATH = "/home/user/workspace/wide/browse_results_mn32rhsk.csv"
TOKAYEV_JSON = "/home/user/workspace/tool_calls/browser_task/output_mn31xveb.json"


def clean_citations(text: str) -> str:
    """Remove markdown citations like [Source](url) from text."""
    if not text:
        return ""
    # Remove [text](url) patterns
    text = re.sub(r'\[[^\]]*\]\([^)]*\)', '', text)
    return text.strip()


def extract_doc_id(val: str) -> str:
    """Extract numeric doc_id from possibly citation-laden string."""
    val = clean_citations(val)
    match = re.match(r'(\d+)', val.strip())
    return match.group(1) if match else val.strip()


def parse_birth_date(date_str: str) -> str:
    """Convert DD.MM.YYYY to YYYY-MM-DD."""
    date_str = clean_citations(date_str).strip()
    m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return date_str


def parse_career(career_str: str) -> list[dict]:
    """Parse career string into list of position dicts."""
    career_str = clean_citations(career_str)
    if not career_str:
        return []
    
    positions = []
    # Split by | and group into triples (years|position|org) or by semicolons
    # The format is inconsistent, try multiple approaches
    
    # First try splitting by semicolons (between entries)
    entries = re.split(r'\s*;\s*', career_str)
    
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        
        # Try to split by | within entry
        parts = entry.split('|')
        
        if len(parts) >= 2:
            years = parts[0].strip()
            position = parts[1].strip()
            org = parts[2].strip() if len(parts) > 2 else ""
            
            # Parse start/end dates from years
            start_date = ""
            end_date = ""
            if years:
                # Handle "с DD.MM.YYYY" or "с YYYY"
                years_clean = years.replace("с ", "").strip()
                if "-" in years_clean or "–" in years_clean:
                    date_parts = re.split(r'[-–]', years_clean, 1)
                    start_date = date_parts[0].strip()
                    end_date = date_parts[1].strip() if len(date_parts) > 1 else ""
                elif years.startswith("с "):
                    start_date = years_clean
                else:
                    start_date = years_clean
            
            positions.append({
                "position_title": position,
                "organization": org,
                "start_date": start_date,
                "end_date": end_date,
            })
        else:
            # Can't parse, store as-is
            positions.append({
                "position_title": entry,
                "organization": "",
                "start_date": "",
                "end_date": "",
            })
    
    return positions


def clean_photo_url(url: str) -> str:
    """Clean photo URL, extract actual URL from markdown links."""
    if not url:
        return ""
    # Extract URL from markdown link
    m = re.search(r'https?://[^\s\)]+/mapi/document/\?doc_id=\d+', url)
    if m:
        return m.group(0)
    m = re.search(r'(https?://[^\s\)\]]+)', url)
    if m:
        return m.group(1)
    return clean_citations(url)


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    # Ensure source column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(person_positions)").fetchall()]
    if "source" not in cols:
        conn.execute("ALTER TABLE person_positions ADD COLUMN source TEXT DEFAULT 'seed'")
        conn.commit()
    
    # Build mapping: doc_id → person_id
    persons = conn.execute("SELECT id, short_name, zakon_doc_id FROM persons WHERE zakon_doc_id IS NOT NULL AND zakon_doc_id != ''").fetchall()
    doc_to_person = {p["zakon_doc_id"]: p["id"] for p in persons}
    
    print(f"Found {len(doc_to_person)} persons with doc_ids")
    
    # Process Tokayev separately (from JSON)
    tokayev_path = Path(TOKAYEV_JSON)
    if tokayev_path.exists():
        with open(tokayev_path) as f:
            tok = json.load(f)
        
        tokayev_id = doc_to_person.get("30100479")
        if tokayev_id:
            birth_iso = parse_birth_date(tok.get("birth_date", ""))
            conn.execute("""
                UPDATE persons SET 
                    full_name = ?, birth_date = ?, birth_place = ?,
                    languages = ?, education = ?, awards = ?,
                    bio_summary = ?, updated_at = datetime('now')
                WHERE id = ?
            """, (
                tok.get("full_name", ""),
                birth_iso,
                tok.get("birth_place", ""),
                tok.get("languages", ""),
                "; ".join(tok.get("education", [])),
                "; ".join(tok.get("awards", [])),
                "Казахстанский дипломат и государственный деятель. Кандидат исторических наук, доктор политических наук. Владеет пятью языками.",
                tokayev_id,
            ))
            
            # Insert career
            conn.execute("DELETE FROM person_positions WHERE person_id = ? AND source = 'zakon'", (tokayev_id,))
            for i, pos in enumerate(tok.get("career", [])):
                conn.execute("""
                    INSERT INTO person_positions (person_id, position_title, organization, start_date, end_date, sort_order, source)
                    VALUES (?, ?, ?, ?, ?, ?, 'zakon')
                """, (tokayev_id, pos.get("position", ""), pos.get("organization", ""),
                      pos.get("years", "").split("–")[0].strip() if "–" in pos.get("years", "") else pos.get("years", ""),
                      pos.get("years", "").split("–")[1].strip() if "–" in pos.get("years", "") else "",
                      i))
            print(f"  [+] Токаев: {len(tok.get('career', []))} positions imported")
    
    # Process CSV
    csv_path = Path(CSV_PATH)
    if not csv_path.exists():
        print(f"CSV not found: {CSV_PATH}")
        return
    
    updated = 0
    with open(csv_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc_id = extract_doc_id(row.get("Doc ID", ""))
            if not doc_id or doc_id not in doc_to_person:
                entity_url = row.get("entity", "")
                # Try extracting doc_id from entity URL
                m = re.search(r'doc_id=(\d+)', entity_url)
                if m:
                    doc_id = m.group(1)
            
            if not doc_id or doc_id not in doc_to_person:
                print(f"  [?] Skipping: doc_id={doc_id}")
                continue
            
            person_id = doc_to_person[doc_id]
            name = clean_citations(row.get("Full Name", ""))
            
            birth_date = parse_birth_date(row.get("Birth Date", ""))
            birth_place = clean_citations(row.get("Birth Place", ""))
            languages = clean_citations(row.get("Languages", ""))
            education = clean_citations(row.get("Education (semicolon separated)", ""))
            awards = clean_citations(row.get("Awards", ""))
            current_pos = clean_citations(row.get("Current Position", ""))
            photo_url = clean_photo_url(row.get("Photo URL", ""))
            
            # Update person record
            conn.execute("""
                UPDATE persons SET
                    full_name = COALESCE(NULLIF(?, ''), full_name),
                    birth_date = COALESCE(NULLIF(?, ''), birth_date),
                    birth_place = COALESCE(NULLIF(?, ''), birth_place),
                    languages = COALESCE(NULLIF(?, ''), languages),
                    education = COALESCE(NULLIF(?, ''), education),
                    awards = COALESCE(NULLIF(?, ''), awards),
                    current_position = COALESCE(NULLIF(?, ''), current_position),
                    photo_url = COALESCE(NULLIF(?, ''), photo_url),
                    updated_at = datetime('now')
                WHERE id = ?
            """, (name, birth_date, birth_place, languages, education, awards, current_pos, photo_url, person_id))
            
            # Parse and insert career positions
            career_str = row.get("Career History (pipe separated entries: years|position|org)", "")
            positions = parse_career(career_str)
            
            if positions:
                conn.execute("DELETE FROM person_positions WHERE person_id = ? AND source = 'zakon'", (person_id,))
                for i, pos in enumerate(positions):
                    conn.execute("""
                        INSERT INTO person_positions (person_id, position_title, organization, start_date, end_date, sort_order, source)
                        VALUES (?, ?, ?, ?, ?, ?, 'zakon')
                    """, (person_id, pos["position_title"], pos["organization"], pos["start_date"], pos["end_date"], i))
            
            print(f"  [+] {name}: birth={birth_date}, positions={len(positions)}, photo={'yes' if photo_url else 'no'}")
            updated += 1
    
    conn.commit()
    
    # Print stats
    total_persons = conn.execute("SELECT COUNT(*) FROM persons WHERE birth_date IS NOT NULL AND birth_date != ''").fetchone()[0]
    total_positions = conn.execute("SELECT COUNT(*) FROM person_positions WHERE source = 'zakon'").fetchone()[0]
    
    conn.close()
    print(f"\nDone: {updated} persons updated from CSV + Tokayev from JSON")
    print(f"Total persons with birth dates: {total_persons}")
    print(f"Total career positions (zakon source): {total_positions}")


if __name__ == "__main__":
    main()
