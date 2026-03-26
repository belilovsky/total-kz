#!/usr/bin/env python3
"""
Create persons tables and seed from existing entities.
"""
import sqlite3, re, unicodedata
from pathlib import Path

DB = Path(__file__).parent.parent / "data" / "total.db"

TRANSLIT = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh',
    'з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o',
    'п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts',
    'ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu',
    'я':'ya','ә':'a','і':'i','ң':'n','ғ':'g','ү':'u','ұ':'u','қ':'q',
    'ө':'o','һ':'h',
}

def make_slug(name: str) -> str:
    """Cyrillic-friendly slug: олжас-бектенов"""
    s = name.lower().strip()
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'\s+', '-', s)
    s = re.sub(r'-+', '-', s).strip('-')
    return s

KNOWN = {
    # ── Government (Kazakhstan) ──
    'Касым-Жомарт Токаев': {'pos': 'Президент Республики Казахстан', 'org': 'Акорда', 'type': 'government', 'zakon': '30100479', 'birth': '1953-05-17', 'place': 'Алма-Ата'},
    'Олжас Бектенов': {'pos': 'Премьер-Министр Республики Казахстан', 'org': 'Правительство РК', 'type': 'government', 'zakon': '37121274', 'birth': '1980-12-13', 'place': 'Алматы'},
    'Серик Жумангарин': {'pos': 'Первый заместитель Премьер-Министра РК', 'org': 'Правительство РК', 'type': 'government'},
    'Алмасадам Саткалиев': {'pos': 'Министр энергетики РК', 'org': 'Правительство РК', 'type': 'government'},
    'Канат Шарлапаев': {'pos': 'Министр промышленности и строительства РК', 'org': 'Правительство РК', 'type': 'government'},
    'Аида Балаева': {'pos': 'Министр культуры и информации РК', 'org': 'Правительство РК', 'type': 'government'},
    'Канат Бозумбаев': {'pos': 'Заместитель Премьер-Министра РК', 'org': 'Правительство РК', 'type': 'government'},
    'Ерболат Досаев': {'pos': 'Аким города Алматы', 'org': 'Акимат Алматы', 'type': 'government'},
    'Маулен Ашимбаев': {'pos': 'Председатель Сената Парламента РК', 'org': 'Парламент РК', 'type': 'government'},
    'Тимур Сулейменов': {'pos': 'Руководитель Администрации Президента РК', 'org': 'Администрация Президента', 'type': 'government'},
    'Нурсултан Назарбаев': {'pos': 'Первый Президент РК', 'org': '', 'type': 'government'},
    'Алихан Смаилов': {'pos': 'Бывший Премьер-Министр РК', 'org': '', 'type': 'government'},
    'Куандык Бишимбаев': {'pos': 'Бывший Министр национальной экономики РК', 'org': '', 'type': 'government'},
    'Дархан Сатыбалды': {'pos': 'Племянник первого президента', 'org': '', 'type': 'government'},
    'Акмарал Альназарова': {'pos': 'Вице-министр здравоохранения РК', 'org': 'Минздрав РК', 'type': 'government'},
    'Чингис Аринов': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Санжар Адилов': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Дина Смаилова': {'pos': 'Общественный деятель', 'org': '', 'type': 'government'},
    'Шынгыс Алекешев': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Берик Асылов': {'pos': 'Генеральный прокурор РК', 'org': 'Генеральная прокуратура РК', 'type': 'government'},
    'Елнур Бейсенбаев': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Сергей Пономарев': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Екатерина Смышляева': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Перизат Кайрат': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Марат Баркулов': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Галымжан Койгельдиев': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Ерлан Сарсембаев': {'pos': 'Государственный деятель', 'org': '', 'type': 'government'},
    'Ерлан Кошанов': {'pos': 'Председатель Мажилиса Парламента РК', 'org': 'Парламент РК', 'type': 'government'},
    'Тамара Дуйсенова': {'pos': 'Заместитель Премьер-Министра РК', 'org': 'Правительство РК', 'type': 'government'},
    'Руслан Жаксылыков': {'pos': 'Министр обороны РК', 'org': 'Министерство обороны РК', 'type': 'government'},
    'Азат Перуашев': {'pos': 'Депутат Мажилиса Парламента РК', 'org': 'Парламент РК', 'type': 'government'},
    'Мурат Нуртлеу': {'pos': 'Министр иностранных дел РК', 'org': 'МИД РК', 'type': 'government'},
    'Карим Масимов': {'pos': 'Бывший Председатель КНБ РК', 'org': '', 'type': 'government'},

    # ── International ──
    'Дональд Трамп': {'pos': 'Президент США', 'org': 'Белый дом', 'type': 'international'},
    'Илон Маск': {'pos': 'CEO Tesla, SpaceX', 'org': 'Tesla', 'type': 'international'},
    'Владимир Путин': {'pos': 'Президент Российской Федерации', 'org': 'Кремль', 'type': 'international'},
    'Садыр Жапаров': {'pos': 'Президент Кыргызской Республики', 'org': '', 'type': 'international'},
    'Шавкат Мирзиеев': {'pos': 'Президент Республики Узбекистан', 'org': '', 'type': 'international'},
    'Си Цзиньпин': {'pos': 'Председатель КНР', 'org': '', 'type': 'international'},
    'Ильхам Алиев': {'pos': 'Президент Азербайджана', 'org': '', 'type': 'international'},
    'Джо Байден': {'pos': 'Бывший Президент США', 'org': '', 'type': 'international'},
    'Владимир Зеленский': {'pos': 'Президент Украины', 'org': '', 'type': 'international'},
    'Александр Лукашенко': {'pos': 'Президент Беларуси', 'org': '', 'type': 'international'},
    'Реджеп Эрдоган': {'pos': 'Президент Турции', 'org': '', 'type': 'international'},

    # ── Business ──
    'Павел Дуров': {'pos': 'CEO Telegram', 'org': 'Telegram', 'type': 'business'},

    # ── Sports ──
    'Елена Рыбакина': {'pos': 'Теннисистка', 'org': '', 'type': 'sports'},
    'Килиан Мбаппе': {'pos': 'Футболист', 'org': '', 'type': 'sports'},
    'Лионель Месси': {'pos': 'Футболист', 'org': '', 'type': 'sports'},
    'Криштиану Роналду': {'pos': 'Футболист', 'org': '', 'type': 'sports'},
    'Шавкат Рахмонов': {'pos': 'Боец UFC', 'org': '', 'type': 'sports'},
    'Арина Соболенко': {'pos': 'Теннисистка', 'org': '', 'type': 'sports'},
    'Геннадий Головкин': {'pos': 'Боксёр', 'org': '', 'type': 'sports'},

    # ── Media ──
    'Ермурат Бапи': {'pos': 'Журналист', 'org': '', 'type': 'media'},
    'Досым Сатпаев': {'pos': 'Политолог', 'org': '', 'type': 'media'},
}

INTERNATIONAL_MARKERS = ['Трамп', 'Маск', 'Путин', 'Жапаров', 'Мирзиеев', 'Си Цзиньпин',
                          'Байден', 'Макрон', 'Эрдоган', 'Лукашенко', 'Зеленский',
                          'Алиев', 'Мбаппе', 'Месси', 'Роналду', 'Соболенко']

def main():
    conn = sqlite3.connect(str(DB))

    # Create tables
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS persons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER REFERENCES entities(id),
            slug TEXT UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            short_name TEXT,
            birth_date TEXT,
            birth_place TEXT,
            photo_url TEXT,
            current_position TEXT,
            current_org TEXT,
            bio_summary TEXT,
            education TEXT,
            languages TEXT,
            awards TEXT,
            zakon_doc_id TEXT,
            person_type TEXT DEFAULT 'government',
            is_featured INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS person_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER REFERENCES persons(id),
            position_title TEXT NOT NULL,
            organization TEXT,
            start_date TEXT,
            end_date TEXT,
            decree_url TEXT,
            sort_order INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_persons_slug ON persons(slug);
        CREATE INDEX IF NOT EXISTS idx_persons_entity ON persons(entity_id);
        CREATE INDEX IF NOT EXISTS idx_pp_person ON person_positions(person_id);
    """)

    # Get top 150 persons by recent mentions (last 90 days), then total
    top = conn.execute("""
        SELECT e.id, e.name, e.short_name,
               COUNT(ae.article_id) as cnt,
               SUM(CASE WHEN a.pub_date >= date('now', '-90 days') THEN 1 ELSE 0 END) as recent_cnt
        FROM entities e
        JOIN article_entities ae ON e.id = ae.entity_id
        JOIN articles a ON ae.article_id = a.id
        WHERE e.entity_type = 'person'
        GROUP BY e.id
        ORDER BY recent_cnt DESC, cnt DESC
        LIMIT 150
    """).fetchall()

    inserted = 0
    for eid, name, short, cnt, recent_cnt in top:
        slug = make_slug(short or name)
        # Skip if already exists
        exists = conn.execute("SELECT id FROM persons WHERE entity_id = ?", (eid,)).fetchone()
        if exists:
            continue

        info = KNOWN.get(short or name, KNOWN.get(name, {}))
        ptype = info.get('type', 'government')
        if not ptype or ptype == 'government':
            for marker in INTERNATIONAL_MARKERS:
                if marker in name or marker in (short or ''):
                    ptype = 'international'
                    break

        conn.execute("""
            INSERT INTO persons (entity_id, slug, full_name, short_name,
                birth_date, birth_place, current_position, current_org,
                zakon_doc_id, person_type, is_featured)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            eid, slug, name, short or name,
            info.get('birth'), info.get('place'),
            info.get('pos'), info.get('org'),
            info.get('zakon'), ptype,
            1 if cnt >= 50 else 0
        ))
        inserted += 1
        pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Add current position if known
        if info.get('pos'):
            conn.execute("""
                INSERT INTO person_positions (person_id, position_title, organization, sort_order)
                VALUES (?, ?, ?, 0)
            """, (pid, info['pos'], info.get('org', '')))

        print(f"  {cnt:4d} (recent:{recent_cnt:3d}) | {short or name} [{ptype}]")

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
    print(f"\nInserted: {inserted}, Total persons: {total}")
    conn.close()

if __name__ == "__main__":
    main()
