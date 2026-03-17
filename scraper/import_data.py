"""Import existing JSONL data into SQLite database."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from app.database import import_jsonl, init_db

DATA_DIR = Path(__file__).parent.parent / "data"
ARTICLES_FILE = DATA_DIR / "articles.jsonl"


def main():
    init_db()
    
    if not ARTICLES_FILE.exists():
        print(f"File not found: {ARTICLES_FILE}")
        print("Place articles.jsonl in the data/ directory first.")
        sys.exit(1)
    
    print(f"Importing from {ARTICLES_FILE}...")
    result = import_jsonl(str(ARTICLES_FILE))
    print(f"Done! Imported: {result['imported']}, Skipped: {result['skipped']}, Errors: {result['errors']}")


if __name__ == "__main__":
    main()
