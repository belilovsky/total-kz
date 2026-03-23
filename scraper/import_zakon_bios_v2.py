#!/usr/bin/env python3
"""
Import biographical data from zakon.kz wide_browse CSV into persons DB. v2.
Fixed: career parsing for pipe-separated triplets.
"""
import csv, sqlite3, re, json
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "total.db"
CSV_PATH = "/home/user/workspace/wide/browse_results_mn32rhsk.csv"
TOKAYEV_JSON = "/home/user/workspace/tool_calls/browser_task/output_mn31xveb.json"


def clean_citations(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'\[[^\]]*\]\([^)]*\)', '', text)
    return text.strip()


def extract_doc_id(val: str) -> str:
    val = clean_citations(val)
    match = re.match(r'(\d+)', val.strip())
    return match.group(1) if match else val.strip()


def parse_birth_date(date_str: str) -> str:
    date_str = clean_citations(date_str).strip()
    m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return date_str


def looks_like_date(s: str) -> bool:
    """Check if string looks like a date/year range."""
    s = s.strip()
    if not s:
        return False
    # Matches: YYYY, MM.YYYY, DD.MM.YYYY, YYYY-YYYY, "с YYYY", etc.
    return bool(re.match(r'^(с\s+)?\d{1,2}[\.\-/]?\d{0,4}[\.\-/]?\d{0,4}', s))


def parse_career_pipes(career_str: str) -> list[dict]:
    """Parse career string with pipe-separated triplets: years|position|org|years|position|org|..."""
    career_str = clean_citations(career_str)
    if not career_str:
        return []
    
    # First try: split by | and group into triplets
    parts = [p.strip() for p in career_str.split('|')]
    
    positions = []
    i = 0
    while i < len(parts):
        # Find next date-like part
        if looks_like_date(parts[i]):
            years = parts[i]
            position = parts[i+1] if i+1 < len(parts) else ""
            org = parts[i+2] if i+2 < len(parts) else ""
            
            # Check if org is actually a date (next entry), which means no org for current
            if org and looks_like_date(org):
                org = ""
                i += 2
            else:
                i += 3
            
            # Parse dates
            start_date, end_date = parse_date_range(years)
            
            positions.append({
                "position_title": position,
                "organization": org,
                "start_date": start_date,
                "end_date": end_date,
            })
        elif parts[i]:
            # Non-date starting part — could be "position|org" without dates
            position = parts[i]
            org = parts[i+1] if i+1 < len(parts) and not looks_like_date(parts[i+1] if i+1 < len(parts) else "") else ""
            if org:
                i += 2
            else:
                i += 1
            
            positions.append({
                "position_title": position,
                "organization": org,
                "start_date": "",
                "end_date": "",
            })
        else:
            i += 1
    
    return positions


def parse_date_range(years: str) -> tuple:
    """Parse a date range string into (start, end)."""
    years = years.strip()
    start_date = ""
    end_date = ""
    
    # Remove "с "
    is_ongoing = years.startswith("с ")
    years_clean = years[2:].strip() if is_ongoing else years
    
    # Split on dash
    if re.search(r'[-–]', years_clean):
        date_parts = re.split(r'[-–]', years_clean, maxsplit=1)
        start_date = date_parts[0].strip()
        end_date = date_parts[1].strip() if len(date_parts) > 1 and not is_ongoing else ""
    else:
        start_date = years_clean
    
    return start_date, end_date


def clean_photo_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r'https?://[^\s\)\]]+/mapi/document/\?doc_id=\d+', url)
    if m:
        return m.group(0)
    m = re.search(r'(https?://[^\s\)\]]+)', url)
    if m:
        return m.group(1)
    return clean_citations(url)


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    
    # Ensure source column
    cols = [r[1] for r in conn.execute("PRAGMA table_info(person_positions)").fetchall()]
    if "source" not in cols:
        conn.execute("ALTER TABLE person_positions ADD COLUMN source TEXT DEFAULT 'seed'")
        conn.commit()
    
    doc_to_person = {}
    for p in conn.execute("SELECT id, short_name, zakon_doc_id FROM persons WHERE zakon_doc_id IS NOT NULL AND zakon_doc_id != ''").fetchall():
        doc_to_person[p["zakon_doc_id"]] = {"id": p["id"], "name": p["short_name"]}
    
    print(f"Found {len(doc_to_person)} persons with doc_ids")
    
    # Import Tokayev from JSON
    tok_path = Path(TOKAYEV_JSON)
    if tok_path.exists():
        with open(tok_path) as f:
            tok = json.load(f)
        
        tokayev_id = doc_to_person.get("30100479", {}).get("id")
        if tokayev_id:
            birth_iso = parse_birth_date(tok.get("birth_date", ""))
            conn.execute("""
                UPDATE persons SET full_name=?, birth_date=?, birth_place=?, languages=?,
                    education=?, awards=?, bio_summary=?, updated_at=datetime('now')
                WHERE id=?
            """, (
                tok.get("full_name", ""), birth_iso, tok.get("birth_place", ""),
                tok.get("languages", ""),
                "; ".join(tok.get("education", [])), "; ".join(tok.get("awards", [])),
                "Казахстанский дипломат и государственный деятель. Кандидат исторических наук, доктор политических наук. Владеет пятью языками.",
                tokayev_id,
            ))
            conn.execute("DELETE FROM person_positions WHERE person_id=? AND source='zakon'", (tokayev_id,))
            for i, pos in enumerate(tok.get("career", [])):
                yrs = pos.get("years", "")
                start, end = parse_date_range(yrs)
                conn.execute("""
                    INSERT INTO person_positions (person_id, position_title, organization, start_date, end_date, sort_order, source)
                    VALUES (?, ?, ?, ?, ?, ?, 'zakon')
                """, (tokayev_id, pos.get("position", ""), pos.get("organization", ""), start, end, i))
            print(f"  [+] Токаев: {len(tok.get('career', []))} positions")
    
    # Process CSV
    updated = 0
    with open(CSV_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc_id = extract_doc_id(row.get("Doc ID", ""))
            if not doc_id or doc_id not in doc_to_person:
                m = re.search(r'doc_id=(\d+)', row.get("entity", ""))
                if m:
                    doc_id = m.group(1)
            
            if not doc_id or doc_id not in doc_to_person:
                continue
            
            person_id = doc_to_person[doc_id]["id"]
            person_name = doc_to_person[doc_id]["name"]
            
            name = clean_citations(row.get("Full Name", ""))
            birth_date = parse_birth_date(row.get("Birth Date", ""))
            birth_place = clean_citations(row.get("Birth Place", ""))
            languages = clean_citations(row.get("Languages", ""))
            education = clean_citations(row.get("Education (semicolon separated)", ""))
            awards = clean_citations(row.get("Awards", ""))
            current_pos = clean_citations(row.get("Current Position", ""))
            photo_url = clean_photo_url(row.get("Photo URL", ""))
            
            conn.execute("""
                UPDATE persons SET
                    full_name=COALESCE(NULLIF(?,''),full_name),
                    birth_date=COALESCE(NULLIF(?,''),birth_date),
                    birth_place=COALESCE(NULLIF(?,''),birth_place),
                    languages=COALESCE(NULLIF(?,''),languages),
                    education=COALESCE(NULLIF(?,''),education),
                    awards=COALESCE(NULLIF(?,''),awards),
                    current_position=COALESCE(NULLIF(?,''),current_position),
                    photo_url=COALESCE(NULLIF(?,''),photo_url),
                    updated_at=datetime('now')
                WHERE id=?
            """, (name, birth_date, birth_place, languages, education, awards, current_pos, photo_url, person_id))
            
            # Career
            career_str = row.get("Career History (pipe separated entries: years|position|org)", "")
            positions = parse_career_pipes(career_str)
            
            if positions:
                conn.execute("DELETE FROM person_positions WHERE person_id=? AND source='zakon'", (person_id,))
                for i, pos in enumerate(positions):
                    conn.execute("""
                        INSERT INTO person_positions (person_id, position_title, organization, start_date, end_date, sort_order, source)
                        VALUES (?, ?, ?, ?, ?, ?, 'zakon')
                    """, (person_id, pos["position_title"], pos["organization"], pos["start_date"], pos["end_date"], i))
            
            print(f"  [+] {person_name}: birth={birth_date}, positions={len(positions)}, photo={'yes' if photo_url else 'no'}")
            updated += 1
    
    conn.commit()
    
    total_with_birth = conn.execute("SELECT COUNT(*) FROM persons WHERE birth_date IS NOT NULL AND birth_date!=''").fetchone()[0]
    total_positions = conn.execute("SELECT COUNT(*) FROM person_positions WHERE source='zakon'").fetchone()[0]
    conn.close()
    
    print(f"\nDone: {updated} persons updated")
    print(f"Persons with birth dates: {total_with_birth}")
    print(f"Career positions (zakon): {total_positions}")


if __name__ == "__main__":
    main()
