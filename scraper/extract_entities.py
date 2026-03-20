#!/usr/bin/env python3
"""
NER-извлечение сущностей из статей Total.kz.
Использует Natasha (русскоязычный NLP) для извлечения:
- Персон (PER)
- Организаций (ORG)
- Локаций (LOC)

Также денормализует теги из JSON-поля в таблицу article_tags.

Запуск:
    python scraper/extract_entities.py              # обработать все статьи
    python scraper/extract_entities.py --batch 1000 # по 1000 за раз
    python scraper/extract_entities.py --tags-only   # только теги, без NER
    python scraper/extract_entities.py --reprocess   # переобработать все (сброс)
"""
import json
import re
import sys
import argparse
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.database import get_db, init_db

try:
    # pymorphy2 сломан на Python 3.12+ (pkg_resources)
    # Подменяем на pymorphy3
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
    print("⚠ Natasha не установлена. Запустите: pip install natasha pymorphy3 pymorphy3-dicts-ru")
    print("  Будут обработаны только теги.")


# ═══════════════════════════════════════════════════════════════════
# ЧЁРНЫЙ СПИСОК — только то, что однозначно мусор
# ═══════════════════════════════════════════════════════════════════
BLACKLIST_EXACT = {
    # ── Сайт / бренд (сам источник) ──
    "total.kz", "total", "тотал", "тотал.kz",
    "иа тотал казахстан", "иа тотал",

    # ── Авторы Total.kz (не должны попадать в персоны) ──
    "диас калиакпаров", "тулеубек габбасов", "назира даримбет",
    "айнур коскина", "василий смирнов", "алма омарова",
    "сабина токабаева", "партнерский материал",

    # ── СМИ-источники (перепечатки, не контент) ──
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

    # ── Соцсети / платформы (не контент-сущности) ──
    "twitter", "instagram", "facebook", "telegram",
    "youtube", "tiktok", "вконтакте", "vk", "whatsapp",
    "одноклассники",

    # ── Техно-бренды (шум в NER) ──
    "google", "apple", "microsoft", "amazon", "meta",
    "samsung", "huawei", "xiaomi",

    # ── Служебные слова, ложно распознанные как сущности ──
    "фото", "видео", "источник", "редакция", "автор",
    "корреспондент", "собеседник", "эксперт", "спикер",
    "казахстанец", "казахстанцы", "казахстанка",
    "респондент", "читатель", "глава государства",
    "президент", "премьер-министр", "министр",
    "депутат", "аким", "сенатор",

    # ── Рекламный / тех мусор из HTML ──
    "formobiles", "ferra", "mob-info",
    "adblock", "bitrix", "yandex",

    # ── Общие слова ошибочно распознанные как персоны ──
    "мажилисмен", "правозащитник", "омбудсмен",
    "политолог", "экономист", "аналитик",
    "спасатель", "полицейский", "пограничник",
    "сми", "опг", "снг",

    # ── Слишком короткие / неоднозначные имена ──
    "касым", "абай",
}

# Паттерны — если normalized содержит, сущность отбрасывается
BLACKLIST_PATTERNS = [
    r"^https?://",      # URL-ы
    r"^www\.",          # URL-ы
    r"^\d+$",           # чистые числа
    r"^[а-яё]$",        # одиночные буквы
    r"^[а-яёa-z]\.$",   # инициалы типа "К.", "А."
]

# Минимальная длина имени для каждого типа
MIN_NAME_LENGTH = {
    "person": 3,   # "Ли" → слишком коротко, но "Маск" → ok
    "org": 2,
    "location": 2,
}

# ═══════════════════════════════════════════════════════════════════
# ИЗВЕСТНЫЕ БРЕНДЫ — CamelCase / смешанный регистр, не трогать
# ═══════════════════════════════════════════════════════════════════
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


# ═════════════════════════════════════════════════════════════════
# АЛИАСЫ — объединение дубликатов
# Ключ = normalized (нижний регистр), значение = (каноническое имя, нормализованное)
# ═════════════════════════════════════════════════════════════════
ALIAS_MAP = {
    # Персоны — Казахстан
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

    # Персоны — Международные
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

    # Локации — аббревиатуры регионов Казахстана
    "вко": ("Восточно-Казахстанская область", "восточно-казахстанская область"),
    "зко": ("Западно-Казахстанская область", "западно-казахстанская область"),
    "юко": ("Южно-Казахстанская область", "южно-казахстанская область"),
    "ско": ("Северо-Казахстанская область", "северо-казахстанская область"),
    "рк": ("Казахстан", "казахстан"),
    "республика казахстан": ("Казахстан", "казахстан"),

    # Локации — страны
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

    # Организации — аббревиатуры
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


def normalize_name(name: str) -> str:
    """Нормализация имени для дедупликации."""
    return " ".join(name.strip().split()).lower()


def is_known_brand(name: str) -> bool:
    """Проверяем, является ли имя (или его часть) известным брендом."""
    lower = name.lower().replace(" ", "").replace("«", "").replace("»", "").replace('"', '')
    for brand in KNOWN_BRANDS:
        if brand in lower:
            return True
    return False


def strip_outer_quotes(name: str) -> str:
    """
    Убирает внешнюю пару кавычек «...» если внутри уже есть парные кавычки.
    «АО «Казпочта»» → АО «Казпочта»
    «НПП «Атамекен»» → НПП «Атамекен»
    Но НЕ трогает: АО «Казпочта» → оставляет как есть
    """
    # Если обёрнуто в «...» и внутри есть ещё «...»
    if name.startswith('«') and name.endswith('»'):
        inner = name[1:-1]
        # Если внутри есть парные «», значит внешние — лишние
        if '«' in inner and '»' in inner:
            name = inner
    # Аналогично для "
    if name.startswith('"') and name.endswith('"') and len(name) > 2:
        inner = name[1:-1]
        if '"' not in inner:  # простые внешние кавычки
            name = inner
    return name.strip()


def clean_garbage_prefix(name: str) -> str:
    """
    Отрезает мусорный префикс от прилипшего слова.
    
    Примеры:
        "делАрман Исетов" → "Арман Исетов"
        "наТАСС" → "ТАСС"
        "здравоохраненияАжар Гиният" → "Ажар Гиният"
        "президентКасым-Жомарт Токаев" → "Касым-Жомарт Токаев"
        "финансовЕрулан Жамаубаев" → "Ерулан Жамаубаев"
        "наИнтерфакс" → "Интерфакс"
        "государстваКасым-Жомарт" → "Касым-Жомарт"
        
    НЕ трогает:
        "WhatsApp" → "WhatsApp" (известный бренд)
        "КазМунайГаз" → "КазМунайГаз" (известный бренд)
        "SpaceX" → "SpaceX"
        "YouTube" → "YouTube"
    """
    # Если это известный бренд — не трогаем
    if is_known_brand(name):
        return name
    
    # Ищем паттерн: строчные кириллические буквы + заглавная буква (кириллица или латиница)
    # Это основной маркер мусорного префикса: "наТАСС", "делАрман", "государстваКасым"
    # Важно: ищем ТОЛЬКО в первом слове (до первого пробела)
    first_space = name.find(' ')
    first_word = name[:first_space] if first_space > 0 else name
    rest = name[first_space:] if first_space > 0 else ""
    
    # Паттерн: кириллические строчные + (Заглавная кириллическая ИЛИ Заглавная латиница)
    match = re.search(r'[а-яё]([А-ЯЁ])', first_word)
    if match:
        cut_pos = match.start(1)
        prefix = first_word[:cut_pos]
        suffix = first_word[cut_pos:]
        
        # Дополнительная проверка: prefix должен быть ПОЛНОСТЬЮ строчным
        # (чтобы не отрезать "КазМунай" от "КазМунайГаз")
        if prefix == prefix.lower() and len(prefix) >= 1:
            return suffix + rest
    
    # Аналогично для латиницы: "inAktau" → "Aktau"
    match = re.search(r'[a-z]([A-Z])', first_word)
    if match:
        cut_pos = match.start(1)
        prefix = first_word[:cut_pos]
        suffix = first_word[cut_pos:]
        if prefix == prefix.lower() and len(prefix) >= 1:
            return suffix + rest
    
    # Спец. случай: первое слово начинается со строчной — это мусорный префикс, но мы не смогли его отрезать
    # "казахстанакасым-жомарт" — полностью строчное, это мусор
    if first_word and first_word[0].islower():
        # Если всё первое слово строчное — это мусор
        # Если есть rest, отдаём rest. Иначе — мусор.
        if rest.strip():
            return rest.strip()
        else:
            # Всё слово строчное и ничего после — не можем очистить
            pass  # пропускаем, оставляем как есть
    
    return name


# Частые глагольные суффиксы, которые прилипают к именам
_STUCK_VERB_SUFFIXES = [
    "подписал", "принял", "поздравил", "назначил", "освободил",
    "утвердил", "отметил", "заявил", "сообщил", "открыл",
    "предложил", "потребовал", "поручил", "одобрил",
    "отправил", "провел", "выступил", "рассказал",
    "объявил", "направил", "внес", "отклонил",
    "посетил", "обсудил", "приняла", "подписала",
]


def has_stuck_suffix(name: str) -> bool:
    """
    Проверяет, есть ли слипшийся суффикс (мусор после имени).
    "Касым-Жомарт Токаевподписал" → True
    "Касым-Жомарт Токаев" → False
    "Токаев" → False
    """
    lower_name = name.lower()
    for suffix in _STUCK_VERB_SUFFIXES:
        if lower_name.endswith(suffix):
            # Проверяем что перед суффиксом есть буквы (не просто сам глагол)
            before = name[:len(name) - len(suffix)]
            if len(before) >= 2:
                return True
    return False


def clean_entity_name(raw_name: str, entity_type: str) -> str | None:
    """
    Очищает и валидирует имя сущности.
    Возвращает очищенное имя или None, если сущность мусорная.
    """
    if not raw_name:
        return None

    name = raw_name.strip()
    
    # Убираем непарные внешние кавычки
    name = strip_outer_quotes(name)

    if not name:
        return None

    # Проверяем чёрный список (точное совпадение)
    if normalize_name(name) in BLACKLIST_EXACT:
        return None

    # Проверяем паттерны чёрного списка
    norm = normalize_name(name)
    for pattern in BLACKLIST_PATTERNS:
        if re.search(pattern, norm):
            return None

    # Очистка мусорных префиксов
    name = clean_garbage_prefix(name)
    if name is None:
        return None
    
    # Отбрасываем если есть слипшийся суффикс
    if has_stuck_suffix(name):
        return None
    
    # Отбрасываем если в любом слове (не только первом) есть слипшиеся слова
    # "прокуратурыЕлдос" — кириллические строчные + заглавная внутри слова
    for word in name.split():
        if is_known_brand(word):
            continue
        if re.search(r'[а-яё]([А-ЯЁ])', word):
            return None

    if not name:
        return None

    # Проверяем минимальную длину
    min_len = MIN_NAME_LENGTH.get(entity_type, 2)
    if len(name) < min_len:
        return None

    # Для персон — каждое слово должно начинаться с заглавной
    # Одиночные фамилии (Токаев, Путин) разрешены
    if entity_type == "person":
        words = name.split()
        if not all(w[0].isupper() for w in words if w):
            # Попробуем title case
            name = name.title()
            words = name.split()
            if not all(w[0].isupper() for w in words if w):
                return None

    # Финальная проверка — не слишком длинное
    if len(name) > 100:
        return None

    return name


def is_blacklisted(normalized: str) -> bool:
    """Проверка — шумная ли это сущность."""
    if normalized in BLACKLIST_EXACT:
        return True
    for pattern in BLACKLIST_PATTERNS:
        if re.search(pattern, normalized):
            return True
    return False


def extract_tags(conn):
    """Денормализовать теги из JSON-поля articles.tags в article_tags."""
    print("\n═══ Денормализация тегов ═══")

    # Сколько уже обработано
    existing = conn.execute("SELECT COUNT(DISTINCT article_id) FROM article_tags").fetchone()[0]
    print(f"  Уже обработано статей: {existing}")

    # Получаем статьи с тегами, которых ещё нет в article_tags
    rows = conn.execute("""
        SELECT a.id, a.tags FROM articles a
        WHERE a.tags IS NOT NULL AND a.tags != '[]' AND a.tags != ''
        AND a.id NOT IN (SELECT DISTINCT article_id FROM article_tags)
    """).fetchall()

    print(f"  Статей для обработки: {len(rows)}")
    count = 0

    for row in rows:
        try:
            tags = json.loads(row[1])
            for tag in tags:
                tag = tag.strip()
                if tag:
                    conn.execute(
                        "INSERT OR IGNORE INTO article_tags (article_id, tag) VALUES (?, ?)",
                        (row[0], tag)
                    )
                    count += 1
        except (json.JSONDecodeError, TypeError):
            continue

        if count % 5000 == 0 and count > 0:
            conn.commit()
            print(f"  ... {count} тегов", flush=True)

    conn.commit()
    print(f"  Готово: {count} тегов добавлено")
    return count


def extract_ner(conn, batch_size=500):
    """Извлечь NER-сущности из текстов статей."""
    if not HAS_NATASHA:
        print("⚠ Natasha не установлена — NER пропущен")
        return 0

    print("\n═══ NER-извлечение сущностей ═══")

    # Инициализация Natasha
    segmenter = Segmenter()
    morph_vocab = MorphVocab()
    emb = NewsEmbedding()
    morph_tagger = NewsMorphTagger(emb)
    ner_tagger = NewsNERTagger(emb)

    # Какие статьи уже обработаны
    processed_ids = set()
    rows = conn.execute("SELECT DISTINCT article_id FROM article_entities").fetchall()
    for r in rows:
        processed_ids.add(r[0])
    print(f"  Уже обработано: {len(processed_ids)} статей")

    # Получаем необработанные
    articles = conn.execute("""
        SELECT id, title, body_text FROM articles
        WHERE body_text IS NOT NULL AND body_text != ''
        AND id NOT IN (SELECT DISTINCT article_id FROM article_entities)
        ORDER BY id
    """).fetchall()

    total = len(articles)
    print(f"  Статей для обработки: {total}")

    if total == 0:
        return 0

    entity_cache = {}  # (normalized, type) -> entity_id
    total_entities = 0
    errors = 0
    filtered_out = 0

    for i, art in enumerate(articles):
        art_id, title, body = art[0], art[1] or "", art[2] or ""
        text = f"{title}. {body}"[:5000]  # ограничиваем для скорости

        try:
            doc = Doc(text)
            doc.segment(segmenter)
            doc.tag_morph(morph_tagger)
            doc.tag_ner(ner_tagger)

            # Нормализуем спаны
            for span in doc.spans:
                span.normalize(morph_vocab)

            # NB: span.extract(names_extractor) не работает в текущей версии Natasha
            # (DocSpan has no attribute 'extract'). span.normal достаточно.

            # Считаем упоминания
            mentions = {}  # (norm, type) -> (count, display_name)
            for span in doc.spans:
                etype_map = {"PER": "person", "ORG": "org", "LOC": "location"}
                etype = etype_map.get(span.type)
                if not etype:
                    continue

                # Приоритет: span.normal (нормализованная форма Natasha)
                raw_name = span.normal or span.text
                if not raw_name:
                    continue

                # Очищаем и валидируем
                clean = clean_entity_name(raw_name, etype)
                if not clean:
                    filtered_out += 1
                    continue

                norm = normalize_name(clean)
                if not norm or len(norm) < 2:
                    filtered_out += 1
                    continue

                # Проверяем чёрный список по нормализованному
                if is_blacklisted(norm):
                    filtered_out += 1
                    continue

                # Применяем алиасы для объединения дубликатов
                if norm in ALIAS_MAP:
                    display_alias, norm_alias = ALIAS_MAP[norm]
                    clean = display_alias
                    norm = norm_alias

                key = (norm, etype)
                if key in mentions:
                    mentions[key] = (mentions[key][0] + 1, mentions[key][1])
                else:
                    mentions[key] = (1, clean)

            # Записываем в БД
            for (norm, etype), (count, display_name) in mentions.items():
                # Получаем или создаём entity
                cache_key = (norm, etype)
                if cache_key in entity_cache:
                    eid = entity_cache[cache_key]
                else:
                    # Пробуем найти
                    row = conn.execute(
                        "SELECT id FROM entities WHERE normalized = ? AND entity_type = ?",
                        (norm, etype)
                    ).fetchone()
                    if row:
                        eid = row[0]
                    else:
                        cursor = conn.execute(
                            "INSERT OR IGNORE INTO entities (name, entity_type, normalized) VALUES (?, ?, ?)",
                            (display_name, etype, norm)
                        )
                        if cursor.lastrowid:
                            eid = cursor.lastrowid
                        else:
                            row = conn.execute(
                                "SELECT id FROM entities WHERE normalized = ? AND entity_type = ?",
                                (norm, etype)
                            ).fetchone()
                            eid = row[0] if row else None

                    if eid:
                        entity_cache[cache_key] = eid

                if eid:
                    conn.execute(
                        "INSERT OR IGNORE INTO article_entities (article_id, entity_id, mention_count) VALUES (?, ?, ?)",
                        (art_id, eid, count)
                    )
                    total_entities += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  ⚠ Ошибка статьи {art_id}: {e}")

        # Прогресс
        if (i + 1) % batch_size == 0:
            conn.commit()
            pct = (i + 1) / total * 100
            print(f"  [{i+1}/{total}] {pct:.1f}% | сущностей: {total_entities} | уникальных: {len(entity_cache)} | отфильтровано: {filtered_out} | ошибок: {errors}", flush=True)

    conn.commit()
    print(f"\n  Готово: {total_entities} связей, {len(entity_cache)} уникальных сущностей, {filtered_out} отфильтровано, {errors} ошибок")
    return total_entities


def main():
    parser = argparse.ArgumentParser(description="NER-извлечение из статей Total.kz")
    parser.add_argument("--batch", type=int, default=500, help="Размер батча для коммитов")
    parser.add_argument("--tags-only", action="store_true", help="Только теги, без NER")
    parser.add_argument("--reprocess", action="store_true", help="Переобработать все (сбросить NER-данные)")
    args = parser.parse_args()

    init_db()

    if args.reprocess:
        print("⚠ Сброс NER-данных...")
        with get_db() as conn:
            conn.execute("DELETE FROM article_entities")
            conn.execute("DELETE FROM entities")
            print("  Удалено всё из entities и article_entities")

    # Логируем запуск
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_runs (started_at, phase, status) VALUES (?, 'ner', 'running')",
            (datetime.now().isoformat(),)
        )
        run_id = cursor.lastrowid

    try:
        with get_db() as conn:
            tag_count = extract_tags(conn)

            if not args.tags_only:
                ner_count = extract_ner(conn, batch_size=args.batch)
            else:
                ner_count = 0

        with get_db() as conn:
            conn.execute(
                "UPDATE scrape_runs SET finished_at=?, status='completed', articles_found=?, articles_downloaded=? WHERE id=?",
                (datetime.now().isoformat(), ner_count, tag_count, run_id)
            )

        print(f"\n{'='*60}")
        print(f"  ГОТОВО: {tag_count} тегов, {ner_count} NER-связей")
        print(f"{'='*60}")

    except Exception as e:
        with get_db() as conn:
            conn.execute(
                "UPDATE scrape_runs SET finished_at=?, status='failed', log=? WHERE id=?",
                (datetime.now().isoformat(), str(e), run_id)
            )
        raise


if __name__ == "__main__":
    main()
