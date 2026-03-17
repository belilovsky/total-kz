#!/usr/bin/env python3
"""
Импорт статей из JSONL в базу данных SQLite.

Запуск:
    python scraper/import_data.py                              # из стандартного файла
    python scraper/import_data.py --file data/my_articles.jsonl  # из другого файла
"""
import sys
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from app.database import import_jsonl, init_db

DATA_DIR = BASE_DIR / "data"
DEFAULT_FILE = DATA_DIR / "articles.jsonl"


def main():
    parser = argparse.ArgumentParser(description="Импорт статей в базу данных")
    parser.add_argument("--file", type=str, default=str(DEFAULT_FILE), help="Путь к JSONL файлу")
    args = parser.parse_args()

    init_db()

    if not Path(args.file).exists():
        print(f"Файл не найден: {args.file}")
        print("Поместите articles.jsonl в директорию data/ или укажите путь через --file")
        sys.exit(1)

    print(f"Импортирую из {args.file}...")
    result = import_jsonl(args.file)
    print(f"Готово! Импортировано: {result['imported']}, пропущено: {result['skipped']}, ошибок: {result['errors']}")


if __name__ == "__main__":
    main()
