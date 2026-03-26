#!/usr/bin/env python3
"""
NER-извлечение сущностей из статей Total.kz (PostgreSQL версия).
Использует Natasha (русскоязычный NLP) для извлечения:
- Персон (PER)
- Организаций (ORG)
- Локаций (LOC)

Также денормализует теги из article_enrichments.keywords в article_tags.

Использует multiprocessing (4 workers) — Natasha CPU-bound.

Запуск:
    python scripts/extract_entities_pg.py              # обработать все статьи
    python scripts/extract_entities_pg.py --batch 1000 # лимит N статей
    python scripts/extract_entities_pg.py --workers 4  # число CPU workers
    python scripts/extract_entities_pg.py --tags-only  # только теги из enrichments
"""
import json
import os
import re
import sys
import argparse
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count

import psycopg2
import psycopg2.extras

# ── pymorphy3 shim for Natasha ──
try:
    import pymorphy3
    sys.modules['pymorphy2'] = pymorphy3
    sys.modules['pymorphy2.analyzer'] = pymorphy3.analyzer
    sys.modules['pymorphy2.tagset'] = pymorphy3.tagset
    sys.modules['pymorphy2.shapes'] = pymorphy3.shapes

    from natasha import (
        Segmenter, MorphVocab,
        NewsEmbedding, NewsMorphTagger, NewsNERTagger,
        Doc,
    )
    HAS_NATASHA = True
except ImportError:
    HAS_NATASHA = False
    print("⚠ Natasha не установлена. pip install natasha pymorphy3 pymorphy3-dicts-ru")

PG_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz"
)

# ═══════════════════════════════════════════════════════════════════
# ЧЁРНЫЙ СПИСОК
# ═══════════════════════════════════════════════════════════════════
BLACKLIST_EXACT = {
    "total.kz", "total", "тотал", "тотал.kz",
    "иа тотал казахстан", "иа тотал",
    "иа total.kz", "иаtotal.kz", "ia total.kz",

    "диас калиакпаров", "тулеубек габбасов", "назира даримбет",
    "айнур коскина", "василий смирнов", "алма омарова",
    "сабина токабаева", "партнерский материал",

    "lenta.ru", "лента.ру", "лента",
    "interfax", "интерфакс", "интерфакс-казахстан",
    "риа новости", "риа", "ria novosti",
    "тасс", "tass",
    "казинформ", "kazinform",
    "bnews.kz", "bnews", "бньюс",
    "informburo.kz", "informburo", "информбюро",
    "tengrinews.kz", "tengrinews", "тенгриньюс",
    "nur.kz", "nur",
    "zakon.kz", "zakon",
    "sputnik", "sputnik казахстан", "спутник",
    "forbes.kz", "forbes kazakhstan", "форбс",
    "vласть", "vlast.kz",
    "reuters", "рейтер",
    "associated press", "ap",
    "bbc", "бибиси",
    "cnn",
    "rbc", "рбк",
    "коммерсантъ", "коммерсант",
    "известия",
    "ведомости",
    "газета.ру", "gazeta.ru",
    "regnum",
    "новости-казахстан",
    "курсив", "kursiv.kz",
    "капитал", "kapital.kz",
    "хабар 24", "khabar", "хабар",

    "twitter", "instagram", "facebook", "telegram",
    "youtube", "tiktok", "вконтакте", "vk", "whatsapp",
    "одноклассники",

    "google", "apple", "microsoft", "amazon", "meta",
    "samsung", "huawei", "xiaomi",

    "фото", "видео", "источник", "редакция", "автор",
    "корреспондент", "собеседник", "эксперт", "спикер",
    "казахстанец", "казахстанцы", "казахстанка",
    "респондент", "читатель", "глава государства",
    "президент", "премьер-министр", "министр",
    "депутат", "аким", "сенатор",

    "formobiles", "ferra", "mob-info",
    "adblock", "bitrix", "yandex",

    "мажилисмен", "правозащитник", "омбудсмен",
    "политолог", "экономист", "аналитик",
    "спасатель", "полицейский", "пограничник",
    "сми", "опг", "снг",

    "касым", "абай",
}

BLACKLIST_PATTERNS = [
    r"^https?://",
    r"^www\.",
    r"^\d+$",
    r"^[а-яё]$",
    r"^[а-яёa-z]\.$",
]

MIN_NAME_LENGTH = {
    "person": 3,
    "org": 2,
    "location": 2,
}

KNOWN_BRANDS = {
    "whatsapp", "tiktok", "youtube", "openai", "spacex",
    "chatgpt", "iphone", "linkedin", "facebook", "instagram",
    "telegram", "twitter", "wabetainfo", "playstation", "xbox",
    "kazmunaygaz", "казмунайгаз", "арселормиттал", "казтрансгаз",
    "казтрансойл", "казавтожол", "казахтелеком", "казгидромет",
    "центркредит", "банкцентркредит", "казпочта", "казатомпром",
    "самрук-казына", "самрук-қазына", "байтерек", "egov", "egov.kz",
    "kaspi", "kaspi.kz", "коммерсантъ", "коммерсант",
    "aspir", "аспир",
}

ALIAS_MAP = {
    "токаев": ("Касым-Жомарт Токаев", "касым-жомарт токаев"),
    "касым-жомарт токаев": ("Касым-Жомарт Токаев", "касым-жомарт токаев"),
    "к.-ж. токаев": ("Касым-Жомарт Токаев", "касым-жомарт токаев"),
    "назарбаев": ("Нурсултан Назарбаев", "нурсултан назарбаев"),
    "нурсултан назарбаев": ("Нурсултан Назарбаев", "нурсултан назарбаев"),
    "н.а. назарбаев": ("Нурсултан Назарбаев", "нурсултан назарбаев"),
    "масимов": ("Карим Масимов", "карим масимов"),
    "сагинтаев": ("Бакытжан Сагинтаев", "бакытжан сагинтаев"),
    "смаилов": ("Алихан Смаилов", "алихан смаилов"),
    "бектенов": ("Олжас Бектенов", "олжас бектенов"),
    "жомарт токаев": ("Касым-Жомарт Токаев", "касым-жомарт токаев"),
    "жумангарин": ("Серик Жумангарин", "серик жумангарин"),
    "серик жумангарин": ("Серик Жумангарин", "серик жумангарин"),
    "альназарова": ("Акмарал Альназарова", "акмарал альназарова"),
    "акмарал альназарова": ("Акмарал Альназарова", "акмарал альназарова"),
    "шарлапаев": ("Шарлапаев", "шарлапаев"),
    "саткалиев": ("Саткалиев", "саткалиев"),

    "путин": ("Владимир Путин", "владимир путин"),
    "в. путин": ("Владимир Путин", "владимир путин"),
    "в.в. путин": ("Владимир Путин", "владимир путин"),
    "трамп": ("Дональд Трамп", "дональд трамп"),
    "дональд трамп": ("Дональд Трамп", "дональд трамп"),
    "лукашенко": ("Александр Лукашенко", "александр лукашенко"),
    "зеленский": ("Владимир Зеленский", "владимир зеленский"),
    "эрдоган": ("Реджеп Эрдоган", "реджеп эрдоган"),
    "маск": ("Илон Маск", "илон маск"),
    "илон маск": ("Илон Маск", "илон маск"),

    "вко": ("Восточно-Казахстанская область", "восточно-казахстанская область"),
    "зко": ("Западно-Казахстанская область", "западно-казахстанская область"),
    "юко": ("Южно-Казахстанская область", "южно-казахстанская область"),
    "ско": ("Северо-Казахстанская область", "северо-казахстанская область"),
    "рк": ("Казахстан", "казахстан"),
    "республика казахстан": ("Казахстан", "казахстан"),

    "рф": ("Россия", "россия"),
    "российская федерация": ("Россия", "россия"),
    "сша": ("США", "сша"),
    "соединенные штаты": ("США", "сша"),
    "соединенные штаты америки": ("США", "сша"),
    "америка": ("США", "сша"),
    "кнр": ("Китай", "китай"),
    "кр": ("Кыргызстан", "кыргызстан"),
    "руз": ("Узбекистан", "узбекистан"),
    "рб": ("Беларусь", "беларусь"),

    "мвд": ("МВД", "мвд"),
    "мвд рк": ("МВД Казахстана", "мвд казахстана"),
    "министерство внутренних дел": ("МВД Казахстана", "мвд казахстана"),
    "мчс": ("МЧС", "мчс"),
    "мчс рк": ("МЧС Казахстана", "мчс казахстана"),
    "мид": ("МИД", "мид"),
    "мид рк": ("МИД Казахстана", "мид казахстана"),
    "мон": ("Министерство образования и науки", "министерство образования и науки"),
    "оон": ("ООН", "оон"),
    "нато": ("НАТО", "нато"),
    "ес": ("Евросоюз", "евросоюз"),
    "еаэс": ("ЕАЭС", "еаэс"),
    "евразийский экономический союз": ("ЕАЭС", "еаэс"),
    "одкб": ("ОДКБ", "одкб"),
    "шос": ("ШОС", "шос"),
    "мажилис": ("Мажилис", "мажилис"),
    "мажилис парламента": ("Мажилис", "мажилис"),
    "сенат": ("Сенат", "сенат"),
    "сенат парламента": ("Сенат", "сенат"),
    "нацбанк": ("Национальный банк Казахстана", "национальный банк казахстана"),
    "нб рк": ("Национальный банк Казахстана", "национальный банк казахстана"),
}

_STUCK_VERB_SUFFIXES = [
    "подписал", "принял", "поздравил", "назначил", "освободил",
    "утвердил", "отметил", "заявил", "сообщил", "открыл",
    "предложил", "потребовал", "поручил", "одобрил",
    "отправил", "провел", "выступил", "рассказал",
    "объявил", "направил", "внес", "отклонил",
    "посетил", "обсудил", "приняла", "подписала",
]


# ═══════════════════════════════════════════════════════════════════
# Helper functions
# ═══════════════════════════════════════════════════════════════════

def normalize_name(name: str) -> str:
    return " ".join(name.strip().split()).lower()


def is_known_brand(name: str) -> bool:
    lower = name.lower().replace(" ", "").replace("«", "").replace("»", "").replace('"', '')
    for brand in KNOWN_BRANDS:
        if brand in lower:
            return True
    return False


def strip_outer_quotes(name: str) -> str:
    if name.startswith('«') and name.endswith('»'):
        inner = name[1:-1]
        if '«' in inner and '»' in inner:
            name = inner
    if name.startswith('"') and name.endswith('"') and len(name) > 2:
        inner = name[1:-1]
        if '"' not in inner:
            name = inner
    return name.strip()


def clean_garbage_prefix(name: str) -> str:
    if is_known_brand(name):
        return name
    first_space = name.find(' ')
    first_word = name[:first_space] if first_space > 0 else name
    rest = name[first_space:] if first_space > 0 else ""

    match = re.search(r'[а-яё]([А-ЯЁ])', first_word)
    if match:
        cut_pos = match.start(1)
        prefix = first_word[:cut_pos]
        suffix = first_word[cut_pos:]
        if prefix == prefix.lower() and len(prefix) >= 1:
            return suffix + rest

    match = re.search(r'[a-z]([A-Z])', first_word)
    if match:
        cut_pos = match.start(1)
        prefix = first_word[:cut_pos]
        suffix = first_word[cut_pos:]
        if prefix == prefix.lower() and len(prefix) >= 1:
            return suffix + rest

    if first_word and first_word[0].islower():
        if rest.strip():
            return rest.strip()

    return name


def has_stuck_suffix(name: str) -> bool:
    lower_name = name.lower()
    for suffix in _STUCK_VERB_SUFFIXES:
        if lower_name.endswith(suffix):
            before = name[:len(name) - len(suffix)]
            if len(before) >= 2:
                return True
    return False


def is_blacklisted(normalized: str) -> bool:
    if normalized in BLACKLIST_EXACT:
        return True
    for pattern in BLACKLIST_PATTERNS:
        if re.search(pattern, normalized):
            return True
    return False


def clean_entity_name(raw_name: str, entity_type: str):
    if not raw_name:
        return None
    name = raw_name.strip()
    name = strip_outer_quotes(name)
    if not name:
        return None

    if normalize_name(name) in BLACKLIST_EXACT:
        return None

    norm = normalize_name(name)
    for pattern in BLACKLIST_PATTERNS:
        if re.search(pattern, norm):
            return None

    name = clean_garbage_prefix(name)
    if name is None:
        return None

    if has_stuck_suffix(name):
        return None

    for word in name.split():
        if is_known_brand(word):
            continue
        if re.search(r'[а-яё]([А-ЯЁ])', word):
            return None

    if not name:
        return None

    min_len = MIN_NAME_LENGTH.get(entity_type, 2)
    if len(name) < min_len:
        return None

    if entity_type == "person":
        words = name.split()
        if not all(w[0].isupper() for w in words if w):
            name = name.title()
            words = name.split()
            if not all(w[0].isupper() for w in words if w):
                return None

    if len(name) > 100:
        return None

    return name


def get_conn():
    """Get a psycopg2 connection from PG_URL."""
    return psycopg2.connect(PG_URL)


# ═══════════════════════════════════════════════════════════════════
# Worker: NER processing in a separate process
# ═══════════════════════════════════════════════════════════════════

def _init_worker():
    """Called once per worker process — loads Natasha models."""
    global _segmenter, _morph_vocab, _emb, _morph_tagger, _ner_tagger
    import pymorphy3
    sys.modules['pymorphy2'] = pymorphy3
    sys.modules['pymorphy2.analyzer'] = pymorphy3.analyzer
    sys.modules['pymorphy2.tagset'] = pymorphy3.tagset
    sys.modules['pymorphy2.shapes'] = pymorphy3.shapes
    from natasha import (
        Segmenter, MorphVocab,
        NewsEmbedding, NewsMorphTagger, NewsNERTagger,
    )
    _segmenter = Segmenter()
    _morph_vocab = MorphVocab()
    _emb = NewsEmbedding()
    _morph_tagger = NewsMorphTagger(_emb)
    _ner_tagger = NewsNERTagger(_emb)


def _process_article(args):
    """
    Process a single article in a worker. Returns:
    (article_id, [(display_name, normalized, entity_type, mention_count), ...])
    """
    art_id, title, body_text = args
    text = f"{title or ''}. {body_text or ''}"[:5000]

    try:
        doc = Doc(text)
        doc.segment(_segmenter)
        doc.tag_morph(_morph_tagger)
        doc.tag_ner(_ner_tagger)

        for span in doc.spans:
            span.normalize(_morph_vocab)

        etype_map = {"PER": "person", "ORG": "org", "LOC": "location"}
        mentions = {}  # (norm, type) -> (count, display_name)

        for span in doc.spans:
            etype = etype_map.get(span.type)
            if not etype:
                continue

            raw_name = span.normal or span.text
            if not raw_name:
                continue

            clean = clean_entity_name(raw_name, etype)
            if not clean:
                continue

            norm = normalize_name(clean)
            if not norm or len(norm) < 2:
                continue

            if is_blacklisted(norm):
                continue

            if norm in ALIAS_MAP:
                display_alias, norm_alias = ALIAS_MAP[norm]
                clean = display_alias
                norm = norm_alias

            key = (norm, etype)
            if key in mentions:
                mentions[key] = (mentions[key][0] + 1, mentions[key][1])
            else:
                mentions[key] = (1, clean)

        results = []
        for (norm, etype), (count, display_name) in mentions.items():
            results.append((display_name, norm, etype, count))
        return (art_id, results)

    except Exception as e:
        return (art_id, None)


# ═══════════════════════════════════════════════════════════════════
# Tags denormalization from article_enrichments
# ═══════════════════════════════════════════════════════════════════

def extract_tags_from_enrichments():
    """Denormalize keywords from article_enrichments.keywords JSONB into article_tags."""
    print("\n═══ Денормализация тегов из article_enrichments ═══")

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Count already-processed
        cur.execute("SELECT COUNT(DISTINCT article_id) FROM article_tags")
        existing = cur.fetchone()[0]
        print(f"  Уже в article_tags: {existing} статей")

        # Get enrichments with keywords that don't have tags yet
        cur.execute("""
            SELECT ae.article_id, ae.keywords
            FROM article_enrichments ae
            LEFT JOIN (
                SELECT DISTINCT article_id FROM article_tags
            ) at ON ae.article_id = at.article_id
            WHERE at.article_id IS NULL
              AND ae.keywords IS NOT NULL
        """)
        rows = cur.fetchall()
        print(f"  Статей для обработки: {len(rows)}")

        count = 0
        batch_data = []

        for article_id, keywords in rows:
            if not keywords:
                continue

            # keywords is JSONB — could be list or dict
            if isinstance(keywords, str):
                try:
                    keywords = json.loads(keywords)
                except (json.JSONDecodeError, TypeError):
                    continue

            if isinstance(keywords, list):
                tag_list = keywords
            elif isinstance(keywords, dict):
                tag_list = keywords.get("keywords", [])
                if not tag_list:
                    tag_list = list(keywords.values()) if keywords else []
            else:
                continue

            for tag in tag_list:
                if isinstance(tag, str):
                    tag = tag.strip()
                    if tag and len(tag) >= 2:
                        batch_data.append((article_id, tag))
                        count += 1

            if len(batch_data) >= 5000:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO article_tags (article_id, tag) VALUES %s ON CONFLICT DO NOTHING",
                    batch_data,
                    template="(%s, %s)",
                )
                conn.commit()
                batch_data = []
                print(f"  ... {count} тегов", flush=True)

        # Flush remaining
        if batch_data:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO article_tags (article_id, tag) VALUES %s ON CONFLICT DO NOTHING",
                batch_data,
                template="(%s, %s)",
            )
            conn.commit()

        print(f"  Готово: {count} тегов добавлено")
        return count

    except Exception as e:
        conn.rollback()
        print(f"  ⚠ Ошибка: {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# Main NER pipeline
# ═══════════════════════════════════════════════════════════════════

def extract_ner(batch_limit=None, num_workers=4):
    """Extract NER entities from all unprocessed articles using multiprocessing."""
    if not HAS_NATASHA:
        print("⚠ Natasha не установлена — NER пропущен")
        return 0

    print(f"\n═══ NER-извлечение сущностей ({num_workers} workers) ═══")

    conn = get_conn()
    cur = conn.cursor()

    # Fetch unprocessed articles
    query = """
        SELECT a.id, a.title, a.body_text
        FROM articles a
        LEFT JOIN (
            SELECT DISTINCT article_id FROM article_entities
        ) ae ON a.id = ae.article_id
        WHERE ae.article_id IS NULL
          AND a.body_text IS NOT NULL
          AND a.body_text != ''
        ORDER BY a.id
    """
    if batch_limit:
        query += f" LIMIT {int(batch_limit)}"

    cur.execute(query)
    articles = cur.fetchall()
    total = len(articles)
    print(f"  Статей для обработки: {total}")

    if total == 0:
        cur.close()
        conn.close()
        return 0

    cur.close()
    conn.close()

    # Process with multiprocessing pool
    total_links = 0
    total_unique = 0
    errors = 0
    t0 = time.time()

    # Prepare article tuples: (id, title, body_text)
    article_args = [(a[0], a[1], a[2]) for a in articles]

    # Process in chunks of 1000, write results to DB after each chunk
    CHUNK_SIZE = 1000
    processed = 0

    with Pool(processes=num_workers, initializer=_init_worker) as pool:
        for chunk_start in range(0, total, CHUNK_SIZE):
            chunk = article_args[chunk_start:chunk_start + CHUNK_SIZE]

            results = pool.map(_process_article, chunk)

            # Write results to DB
            conn = get_conn()
            conn.autocommit = False
            cur = conn.cursor()

            try:
                for art_id, entities in results:
                    if entities is None:
                        errors += 1
                        continue

                    for display_name, norm, etype, mention_count in entities:
                        # Insert entity (ON CONFLICT DO NOTHING)
                        cur.execute("""
                            INSERT INTO entities (name, entity_type, normalized)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (normalized, entity_type) DO NOTHING
                        """, (display_name, etype, norm))

                        # Get entity id
                        cur.execute("""
                            SELECT id FROM entities
                            WHERE normalized = %s AND entity_type = %s
                        """, (norm, etype))
                        row = cur.fetchone()
                        if not row:
                            continue
                        eid = row[0]

                        # Insert article_entity link
                        cur.execute("""
                            INSERT INTO article_entities (article_id, entity_id, mention_count)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (article_id, entity_id)
                            DO UPDATE SET mention_count = article_entities.mention_count + EXCLUDED.mention_count
                        """, (art_id, eid, mention_count))
                        total_links += 1

                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"  ⚠ DB error: {e}")
                errors += 1
            finally:
                cur.close()
                conn.close()

            processed += len(chunk)
            elapsed = time.time() - t0
            speed = processed / elapsed if elapsed > 0 else 0

            if processed % 5000 < CHUNK_SIZE or processed == total:
                print(
                    f"  [{processed}/{total}] {processed/total*100:.1f}% | "
                    f"связей: {total_links} | "
                    f"ошибок: {errors} | "
                    f"{speed:.0f} ст/сек | "
                    f"{elapsed:.0f}с",
                    flush=True
                )

    # Update mention_count on entities table
    print("  Обновляем mention_count в entities...")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE entities e
            SET mention_count = sub.cnt
            FROM (
                SELECT entity_id, COUNT(*) as cnt
                FROM article_entities
                GROUP BY entity_id
            ) sub
            WHERE e.id = sub.entity_id
        """)
        conn.commit()
        # Check if mention_count column exists, add if not
    except psycopg2.errors.UndefinedColumn:
        conn.rollback()
        cur.execute("ALTER TABLE entities ADD COLUMN IF NOT EXISTS mention_count INTEGER DEFAULT 0")
        conn.commit()
        cur.execute("""
            UPDATE entities e
            SET mention_count = sub.cnt
            FROM (
                SELECT entity_id, COUNT(*) as cnt
                FROM article_entities
                GROUP BY entity_id
            ) sub
            WHERE e.id = sub.entity_id
        """)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    elapsed = time.time() - t0
    print(f"\n  Готово: {total_links} связей, {errors} ошибок, {elapsed:.0f}с")
    return total_links


def main():
    parser = argparse.ArgumentParser(description="NER-извлечение из статей Total.kz (PostgreSQL)")
    parser.add_argument("--batch", type=int, default=None, help="Лимит статей для обработки")
    parser.add_argument("--workers", type=int, default=4, help="Число CPU workers")
    parser.add_argument("--tags-only", action="store_true", help="Только теги из enrichments")
    args = parser.parse_args()

    started = datetime.now().isoformat()

    # Log run start
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scrape_runs (started_at, phase, status) VALUES (%s, 'ner', 'running') RETURNING id",
        (started,)
    )
    run_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    try:
        tag_count = extract_tags_from_enrichments()

        if not args.tags_only:
            ner_count = extract_ner(batch_limit=args.batch, num_workers=args.workers)
        else:
            ner_count = 0

        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE scrape_runs SET finished_at=%s, status='completed', articles_found=%s, articles_downloaded=%s WHERE id=%s",
            (datetime.now().isoformat(), ner_count, tag_count, run_id)
        )
        conn.commit()
        cur.close()
        conn.close()

        print(f"\n{'='*60}")
        print(f"  ГОТОВО: {tag_count} тегов, {ner_count} NER-связей")
        print(f"{'='*60}")

    except Exception as e:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE scrape_runs SET finished_at=%s, status='failed', log=%s WHERE id=%s",
            (datetime.now().isoformat(), str(e), run_id)
        )
        conn.commit()
        cur.close()
        conn.close()
        raise


if __name__ == "__main__":
    main()
