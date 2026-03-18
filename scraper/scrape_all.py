#!/usr/bin/env python3
"""
Полный пайплайн: сбор URL → скачивание контента → импорт в БД.
Один скрипт, один запуск, всё автоматически.

Запуск:
    python scraper/scrape_all.py --since 2023-03-17   # собрать всё с марта 2023
    python scraper/scrape_all.py --days 365            # за последний год
    python scraper/scrape_all.py                       # за последний год (по умолчанию)
"""
import subprocess
import sys
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def run_step(name, cmd):
    """Запустить шаг пайплайна."""
    print(f"\n{'='*60}")
    print(f"  ШАГ: {name}")
    print(f"{'='*60}\n")

    result = subprocess.run(
        cmd,
        cwd=str(BASE_DIR),
        # Пробрасываем вывод в реальном времени
    )

    if result.returncode != 0:
        print(f"\n  ❌ Ошибка на шаге: {name}")
        sys.exit(1)

    print(f"\n  ✓ {name} — завершено")


def main():
    parser = argparse.ArgumentParser(description="Полный сбор total.kz: URL → контент → БД")
    parser.add_argument("--days", type=int, default=365, help="За сколько дней (по умолчанию 365)")
    parser.add_argument("--since", type=str, help="С какой даты (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=15, help="Параллельных загрузок контента (по умолчанию 15)")
    args = parser.parse_args()

    # Шаг 1: Сбор URL
    scrape_cmd = [sys.executable, "scraper/scrape_urls.py"]
    if args.since:
        scrape_cmd += ["--since", args.since]
    else:
        scrape_cmd += ["--days", str(args.days)]

    run_step("Сбор URL статей", scrape_cmd)

    # Шаг 2: Скачивание контента + импорт в БД
    download_cmd = [
        sys.executable, "scraper/download_content.py",
        "--workers", str(args.workers),
        "--import-db",
    ]

    run_step("Скачивание контента и импорт в БД", download_cmd)

    print(f"\n{'='*60}")
    print(f"  ✓ ВСЁ ГОТОВО")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
