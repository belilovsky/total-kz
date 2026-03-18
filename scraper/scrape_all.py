#!/usr/bin/env python3
"""
Полный пайплайн: сбор URL → скачивание контента → импорт в БД → NER-разметка.
Надёжная версия: таймауты подпроцессов, автоматический restart, прогресс-файл.

Запуск:
    python scraper/scrape_all.py --since 2016-01-01    # собрать всё с 2016
    python scraper/scrape_all.py --days 365             # за последний год
    python scraper/scrape_all.py --force-scrape         # принудительный пересбор
    python scraper/scrape_all.py --skip-ner             # без NER
    python scraper/scrape_all.py --ner-only             # только NER
    python scraper/scrape_all.py --resume               # продолжить с последней категории
"""
import subprocess
import sys
import json
import argparse
import time
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
PROGRESS_FILE = BASE_DIR / "data" / "scrape_progress.json"


def save_progress(step, category=None, status="running"):
    """Сохранить прогресс для возможности возобновления."""
    progress = {
        "step": step,
        "category": category,
        "status": status,
        "timestamp": datetime.now().isoformat(),
    }
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def load_progress():
    """Загрузить последний прогресс."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return None


def run_step(name, cmd, timeout_minutes=120):
    """Запустить шаг пайплайна с таймаутом."""
    print(f"\n{'='*60}")
    print(f"  ШАГ: {name}")
    print(f"  Таймаут: {timeout_minutes} мин")
    print(f"{'='*60}\n", flush=True)

    try:
        result = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            timeout=timeout_minutes * 60,
        )

        if result.returncode != 0:
            print(f"\n  ⚠ Шаг '{name}' вернул код {result.returncode}", flush=True)
            return False

        print(f"\n  ✓ {name} — завершено", flush=True)
        return True

    except subprocess.TimeoutExpired:
        print(f"\n  ❌ Таймаут {timeout_minutes} мин на шаге: {name}", flush=True)
        return False

    except KeyboardInterrupt:
        print(f"\n  ⚠ Прервано пользователем на шаге: {name}", flush=True)
        return False


def main():
    parser = argparse.ArgumentParser(description="Полный сбор total.kz")
    parser.add_argument("--days", type=int, default=365, help="За сколько дней")
    parser.add_argument("--since", type=str, help="С какой даты (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=15, help="Параллельных загрузок")
    parser.add_argument("--skip-ner", action="store_true", help="Пропустить NER")
    parser.add_argument("--ner-only", action="store_true", help="Только NER")
    parser.add_argument("--ner-batch", type=int, default=500, help="Размер NER-батча")
    parser.add_argument("--force-scrape", action="store_true", help="Принудительный пересбор")
    parser.add_argument("--resume", action="store_true", help="Продолжить с последней категории")
    parser.add_argument("--resume-from", type=str, help="Продолжить с конкретной категории")
    args = parser.parse_args()

    start_time = time.time()

    if not args.ner_only:
        # Шаг 1: Сбор URL
        scrape_cmd = [sys.executable, "scraper/scrape_urls.py"]
        if args.since:
            scrape_cmd += ["--since", args.since]
        else:
            scrape_cmd += ["--days", str(args.days)]
        if args.force_scrape:
            scrape_cmd += ["--force"]

        # Resume support
        if args.resume:
            progress = load_progress()
            if progress and progress.get("step") == "urls" and progress.get("category"):
                scrape_cmd += ["--resume-from", progress["category"]]
                print(f"♻ Возобновляем сбор URL с категории: {progress['category']}")
        elif args.resume_from:
            scrape_cmd += ["--resume-from", args.resume_from]

        save_progress("urls", status="running")

        # Таймаут: 90 мин на сбор URL (сайт бывает медленный)
        success = run_step("Сбор URL статей", scrape_cmd, timeout_minutes=90)
        if not success:
            print("\n  ⚠ Сбор URL не завершён полностью, но продолжаем загрузку контента\n", flush=True)

        save_progress("urls", status="done")

        # Шаг 2: Скачивание контента + импорт
        download_cmd = [
            sys.executable, "scraper/download_content.py",
            "--workers", str(args.workers),
            "--import-db",
        ]

        save_progress("content", status="running")

        # Таймаут: 120 мин на скачивание контента
        success = run_step("Скачивание контента и импорт в БД", download_cmd, timeout_minutes=120)
        if not success:
            print("\n  ⚠ Скачивание не завершено полностью\n", flush=True)

        save_progress("content", status="done")

    if not args.skip_ner:
        # Шаг 3: NER
        ner_cmd = [
            sys.executable, "scraper/extract_entities.py",
            "--batch", str(args.ner_batch),
        ]

        save_progress("ner", status="running")
        success = run_step("NER-разметка", ner_cmd, timeout_minutes=60)
        save_progress("ner", status="done" if success else "failed")

    elapsed = time.time() - start_time
    minutes = int(elapsed / 60)

    save_progress("all", status="completed")

    print(f"\n{'='*60}")
    print(f"  ✓ ВСЁ ГОТОВО ({minutes} мин)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
