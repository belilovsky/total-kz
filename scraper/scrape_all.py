#!/usr/bin/env python3
"""
Полный пайплайн: сбор URL → скачивание контента → импорт в БД → NER-разметка.
Один скрипт, один запуск, всё автоматически.

Запуск:
    python scraper/scrape_all.py --since 2021-01-01   # собрать всё с 2021
    python scraper/scrape_all.py --days 365            # за последний год
    python scraper/scrape_all.py                       # за последний год (по умолчанию)
    python scraper/scrape_all.py --skip-ner            # без NER-разметки
    python scraper/scrape_all.py --ner-only            # только NER (без скрейпинга)
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
    parser = argparse.ArgumentParser(description="Полный сбор total.kz: URL → контент → БД → NER")
    parser.add_argument("--days", type=int, default=365, help="За сколько дней (по умолчанию 365)")
    parser.add_argument("--since", type=str, help="С какой даты (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=15, help="Параллельных загрузок контента (по умолчанию 15)")
    parser.add_argument("--skip-ner", action="store_true", help="Пропустить NER-разметку")
    parser.add_argument("--ner-only", action="store_true", help="Только NER (пропустить сбор и загрузку)")
    parser.add_argument("--ner-batch", type=int, default=500, help="Размер NER-батча (по умолчанию 500)")
    parser.add_argument("--force-scrape", action="store_true", help="Принудительный пересбор URL (игнорировать oldest_known)")
    args = parser.parse_args()

    if not args.ner_only:
        # Шаг 1: Сбор URL
        scrape_cmd = [sys.executable, "scraper/scrape_urls.py"]
        if args.since:
            scrape_cmd += ["--since", args.since]
        else:
            scrape_cmd += ["--days", str(args.days)]
        if args.force_scrape:
            scrape_cmd += ["--force"]

        run_step("Сбор URL статей", scrape_cmd)

        # Шаг 2: Скачивание контента + импорт в БД
        download_cmd = [
            sys.executable, "scraper/download_content.py",
            "--workers", str(args.workers),
            "--import-db",
        ]

        run_step("Скачивание контента и импорт в БД", download_cmd)

    if not args.skip_ner:
        # Шаг 3: NER-разметка (теги + сущности)
        ner_cmd = [
            sys.executable, "scraper/extract_entities.py",
            "--batch", str(args.ner_batch),
        ]

        run_step("NER-разметка сущностей и тегов", ner_cmd)

    print(f"\n{'='*60}")
    print(f"  ✓ ВСЁ ГОТОВО")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
