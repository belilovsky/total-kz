#!/usr/bin/env python3
"""
Бэкап SQLite базы total.kz.

Три уровня:
  1. Локальный бэкап в data/backups/ (с ротацией — хранит 5 последних)
  2. Сжатый .gz архив для долгосрочного хранения
  3. Опционально: выгрузка на Google Drive через коннектор

Запуск:
  python scraper/backup_db.py                # стандартный бэкап
  python scraper/backup_db.py --tag pre-import  # бэкап с пометкой
  python scraper/backup_db.py --list         # показать существующие бэкапы
"""
import sqlite3
import shutil
import gzip
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
BACKUP_DIR = DATA_DIR / "backups"
DB_PATH = os.environ.get("DB_PATH", str(DATA_DIR / "total.db"))
MAX_BACKUPS = 5


def backup(tag=""):
    """Создать бэкап базы данных."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag_suffix = f"_{tag}" if tag else ""
    backup_name = f"total_{timestamp}{tag_suffix}.db"
    backup_gz = f"total_{timestamp}{tag_suffix}.db.gz"

    backup_path = BACKUP_DIR / backup_name
    backup_gz_path = BACKUP_DIR / backup_gz

    db_path = Path(DB_PATH)
    if not db_path.exists():
        print(f"❌ БД не найдена: {DB_PATH}")
        return None

    # Используем SQLite backup API — безопасно даже при активной записи
    print(f"Создаю бэкап: {backup_name}")
    src = sqlite3.connect(DB_PATH)
    dst = sqlite3.connect(str(backup_path))
    src.backup(dst)
    dst.close()
    src.close()

    # Считаем статьи в бэкапе для верификации
    verify = sqlite3.connect(str(backup_path))
    count = verify.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    verify.close()

    size_mb = backup_path.stat().st_size / 1024 / 1024

    # Сжимаем
    print(f"Сжимаю → {backup_gz}")
    with open(backup_path, "rb") as f_in:
        with gzip.open(backup_gz_path, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)

    gz_size_mb = backup_gz_path.stat().st_size / 1024 / 1024

    # Удаляем несжатую копию
    backup_path.unlink()

    print(f"✅ Бэкап: {backup_gz} | {count:,} статей | {gz_size_mb:.1f} MB (сжатый)")

    # Ротация — удаляем старые
    backups = sorted(BACKUP_DIR.glob("total_*.db.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
    if len(backups) > MAX_BACKUPS:
        for old in backups[MAX_BACKUPS:]:
            print(f"  🗑 Удаляю старый: {old.name}")
            old.unlink()

    return str(backup_gz_path)


def list_backups():
    """Показать существующие бэкапы."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backups = sorted(BACKUP_DIR.glob("total_*.db.gz"), key=lambda p: p.stat().st_mtime, reverse=True)

    if not backups:
        print("Бэкапов нет.")
        return

    print(f"\n{'='*60}")
    print(f"  БЭКАПЫ ({BACKUP_DIR})")
    print(f"{'='*60}")
    for b in backups:
        size_mb = b.stat().st_size / 1024 / 1024
        mtime = datetime.fromtimestamp(b.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        print(f"  {b.name:50s} {size_mb:>7.1f} MB  {mtime}")
    print()


def restore(backup_file):
    """Восстановить из бэкапа."""
    path = Path(backup_file)
    if not path.exists():
        # Ищем в BACKUP_DIR
        path = BACKUP_DIR / backup_file
    if not path.exists():
        print(f"❌ Файл не найден: {backup_file}")
        return False

    db_path = Path(DB_PATH)

    # Сначала бэкап текущей
    print("Делаю бэкап текущей БД перед восстановлением...")
    backup(tag="pre-restore")

    if path.suffix == ".gz":
        print(f"Распаковываю {path.name}...")
        with gzip.open(path, "rb") as f_in:
            with open(db_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    else:
        shutil.copy2(path, db_path)

    # Верификация
    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    conn.close()

    print(f"✅ Восстановлено: {count:,} статей")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Бэкап SQLite total.kz")
    parser.add_argument("--tag", type=str, default="", help="Пометка бэкапа")
    parser.add_argument("--list", action="store_true", help="Показать бэкапы")
    parser.add_argument("--restore", type=str, help="Восстановить из файла")
    args = parser.parse_args()

    if args.list:
        list_backups()
    elif args.restore:
        restore(args.restore)
    else:
        backup(tag=args.tag)
