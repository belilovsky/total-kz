#!/usr/bin/env python3
"""
Seed realistic comments for articles from 2026.
1-3 comments per article, mix of Russian and Kazakh names/text.
Comments are dated within 1-48 hours after article publication.
"""

import os
import random
import sys
from datetime import datetime, timedelta

import psycopg2

DB_URL = os.environ.get(
    "PG_DATABASE_URL",
    "postgresql://total_kz:T0tal_kz_2026!@db:5432/total_kz",
)

# ── Realistic Kazakh and Russian names ──
RU_NAMES = [
    "Алексей", "Дмитрий", "Сергей", "Андрей", "Максим", "Артём", "Иван",
    "Елена", "Ольга", "Наталья", "Анна", "Мария", "Ирина", "Екатерина",
    "Владимир", "Николай", "Павел", "Михаил", "Денис", "Роман",
    "Татьяна", "Светлана", "Юлия", "Виктория", "Александра",
]

KZ_NAMES = [
    "Айдар", "Нурлан", "Ерлан", "Бауыржан", "Дастан", "Асхат", "Мирас",
    "Айгерим", "Дана", "Алия", "Гульнара", "Жанна", "Динара", "Камила",
    "Арман", "Тимур", "Санжар", "Ернар", "Болат", "Канат",
    "Айнур", "Мадина", "Сауле", "Жазира", "Аружан",
]

RU_SURNAMES = [
    "К.", "М.", "С.", "Б.", "Т.", "А.", "Н.", "О.", "П.", "Д.",
]

KZ_SURNAMES = [
    "Б.", "Т.", "А.", "Н.", "К.", "М.", "С.", "Е.", "Ж.", "Д.",
]

# ── Comment templates by category ──
# {topic} will be replaced contextually

RU_COMMENTS_GENERAL = [
    "Интересная информация, спасибо",
    "Спасибо за статью, очень актуально",
    "Вот это новость! Не ожидал",
    "Наконец-то кто-то написал об этом",
    "А есть ещё подробности?",
    "Хорошая статья, коротко и по делу",
    "Жду продолжения",
    "Важная тема, надо обсуждать",
    "Ну наконец-то",
    "Давно пора",
    "Согласен, ситуация непростая",
    "Надеюсь на лучшее",
    "Это касается всех нас",
    "Спасибо редакции за оперативность",
    "Нужно больше таких материалов",
    "А что думают эксперты?",
    "Подписываюсь под каждым словом",
    "Молодцы, что поднимаете эту тему",
]

RU_COMMENTS_POLITICS = [
    "Правильное решение, давно пора было",
    "Посмотрим, что из этого выйдет",
    "Главное чтобы не на бумаге осталось",
    "Хотелось бы больше конкретики",
    "Сложный вопрос, однозначного ответа нет",
    "Надеюсь это реально поможет людям",
    "Уже сколько раз обещали, а воз и ныне там",
    "Шаг в правильном направлении",
]

RU_COMMENTS_ECONOMY = [
    "Как это отразится на ценах?",
    "Опять на простых людях сэкономят",
    "Хорошо если реально заработает",
    "А что с инфляцией будет?",
    "Цены растут, зарплаты нет",
    "Бизнесу тоже нужна поддержка",
    "Посмотрим через полгода",
    "Нужно диверсифицировать экономику",
]

RU_COMMENTS_SOCIETY = [
    "Грустно читать такое",
    "Нужно что-то менять в обществе",
    "Каждый должен начать с себя",
    "А почему молчат ответственные?",
    "Поддерживаю! Вместе мы сила",
    "Пока не случится с ними — не поймут",
    "Нужно привлекать внимание к этим проблемам",
    "У нас в городе та же ситуация",
]

RU_COMMENTS_SPORT = [
    "Молодцы наши! Так держать!",
    "Горжусь нашими спортсменами",
    "Отличный результат!",
    "Казахстан вперёд! 🇰🇿",
    "Заслуженная победа",
    "Надо больше поддерживать спорт",
    "Ждём на Олимпиаде!",
    "Супер! Продолжайте в том же духе",
]

RU_COMMENTS_WORLD = [
    "Сложная ситуация в мире",
    "Как это может повлиять на нас?",
    "Мир сходит с ума",
    "Надеюсь до нас не дойдёт",
    "Интересно, а что думает МИД?",
    "Геополитика — страшная вещь",
    "За всем этим стоят большие деньги",
]

RU_COMMENTS_INCIDENTS = [
    "Ужас, надеюсь все живы",
    "Когда уже наведут порядок?",
    "Сколько можно? Когда закончится?",
    "Берегите себя и близких",
    "Нужно ужесточить наказание",
    "Виновные должны ответить",
    "Печально это всё читать",
]

# Kazakh comments
KZ_COMMENTS_GENERAL = [
    "Қызық ақпарат, рахмет",
    "Рахмет, өте маңызды мақала",
    "Бұл жаңалықты күткен едім",
    "Жақсы жазылған, қысқа әрі нұсқа",
    "Маңызды тақырып, талқылау керек",
    "Мақаланы оқып, ойландым",
    "Осындай материалдар көбірек болса",
    "Рахмет, өте пайдалы",
    "Бұл бізге де қатысты",
    "Жалғасын күтемін",
    "Дұрыс айтады",
    "Қолдаймын!",
]

KZ_COMMENTS_SPORT = [
    "Жарайсыңдар! Алға, Қазақстан! 🇰🇿",
    "Біздің спортшыларға мақтанамын",
    "Керемет нәтиже!",
    "Олимпиадада күтеміз!",
    "Жеңіс құтты болсын!",
    "Спортшыларымызды қолдайық",
]

KZ_COMMENTS_POLITICS = [
    "Дұрыс шешім",
    "Нәтижесін көрейік",
    "Халыққа пайдалы болса екен",
    "Бұл мәселені көтергені жақсы",
    "Іс жүзінде орындалса екен",
]

KZ_COMMENTS_SOCIETY = [
    "Қоғамда өзгеріс керек",
    "Әркім өзінен бастасын",
    "Бұл мәселе бәрімізге қатысты",
    "Назар аударған жөн",
    "Жағдай қиын, бірақ үміт бар",
]

CATEGORY_COMMENTS = {
    "vnutrennyaya_politika": RU_COMMENTS_POLITICS,
    "vneshnyaya_politika": RU_COMMENTS_POLITICS,
    "gossektor": RU_COMMENTS_POLITICS,
    "politika": RU_COMMENTS_POLITICS,
    "ekonomika_sobitiya": RU_COMMENTS_ECONOMY,
    "ekonomika": RU_COMMENTS_ECONOMY,
    "finansi": RU_COMMENTS_ECONOMY,
    "biznes": RU_COMMENTS_ECONOMY,
    "obshchestvo": RU_COMMENTS_SOCIETY,
    "obshchestvo_sobitiya": RU_COMMENTS_SOCIETY,
    "zhizn": RU_COMMENTS_SOCIETY,
    "proisshestviya": RU_COMMENTS_INCIDENTS,
    "bezopasnost": RU_COMMENTS_INCIDENTS,
    "sport": RU_COMMENTS_SPORT,
    "mir": RU_COMMENTS_WORLD,
    "tehno": RU_COMMENTS_GENERAL,
    "nauka": RU_COMMENTS_GENERAL,
    "stil_zhizni": RU_COMMENTS_GENERAL,
    "kultura": RU_COMMENTS_GENERAL,
    "religiya": RU_COMMENTS_GENERAL,
    "mneniya": RU_COMMENTS_GENERAL,
    "den_v_istorii": RU_COMMENTS_GENERAL,
}

KZ_CATEGORY_COMMENTS = {
    "sport": KZ_COMMENTS_SPORT,
    "vnutrennyaya_politika": KZ_COMMENTS_POLITICS,
    "vneshnyaya_politika": KZ_COMMENTS_POLITICS,
    "gossektor": KZ_COMMENTS_POLITICS,
    "obshchestvo": KZ_COMMENTS_SOCIETY,
    "obshchestvo_sobitiya": KZ_COMMENTS_SOCIETY,
}


def generate_comment(article, is_kz=False):
    """Generate a realistic comment for an article."""
    cat = article["sub_category"] or ""
    
    if is_kz:
        name = random.choice(KZ_NAMES) + " " + random.choice(KZ_SURNAMES)
        pool = KZ_CATEGORY_COMMENTS.get(cat, KZ_COMMENTS_GENERAL)
    else:
        # Mix: 60% Russian names, 40% Kazakh names (realistic for KZ)
        if random.random() < 0.4:
            name = random.choice(KZ_NAMES) + " " + random.choice(KZ_SURNAMES)
        else:
            name = random.choice(RU_NAMES) + " " + random.choice(RU_SURNAMES)
        pool = CATEGORY_COMMENTS.get(cat, RU_COMMENTS_GENERAL)
    
    text = random.choice(pool)
    
    # Random time offset: 1-48 hours after article
    try:
        pub = datetime.fromisoformat(article["pub_date"].replace("T", " ").split("+")[0])
    except:
        pub = datetime(2026, 1, 15, 12, 0)
    
    offset_hours = random.uniform(1, 48)
    comment_time = pub + timedelta(hours=offset_hours)
    
    # Don't create comments in the future
    if comment_time > datetime.now():
        comment_time = datetime.now() - timedelta(hours=random.uniform(1, 24))
    
    return {
        "article_id": article["id"],
        "username": name,
        "comment": text,
        "created_at": comment_time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def main():
    import sqlite3
    from pathlib import Path
    
    # Get articles from PostgreSQL
    pg = psycopg2.connect(DB_URL)
    pg_cur = pg.cursor()
    pg_cur.execute("""
        SELECT id, title, sub_category, pub_date 
        FROM articles 
        WHERE pub_date >= '2026-01-01' AND status = 'published'
        ORDER BY pub_date
    """)
    articles = [{"id": r[0], "title": r[1], "sub_category": r[2], "pub_date": r[3]} 
                for r in pg_cur.fetchall()]
    pg.close()
    
    print(f"Found {len(articles)} articles from 2026")
    
    # Comments go to SQLite (public_comments table)
    db_path = Path("/app/data/total.db")
    conn = sqlite3.connect(str(db_path))
    
    # Ensure table exists
    conn.execute("""CREATE TABLE IF NOT EXISTS public_comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id INTEGER NOT NULL,
        author_name TEXT NOT NULL,
        text TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        ip_address TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now')),
        moderated_at TEXT DEFAULT NULL,
        moderated_by TEXT DEFAULT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pc_article ON public_comments(article_id, status)")
    conn.commit()
    
    # Check existing
    existing = conn.execute("SELECT count(*) FROM public_comments").fetchone()[0]
    if existing > 100:
        print(f"Already {existing} comments exist. Skipping.")
        conn.close()
        return
    
    total_comments = 0
    batch = []
    
    for article in articles:
        # 1-3 Russian comments per article
        num_ru = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        for _ in range(num_ru):
            c = generate_comment(article, is_kz=False)
            batch.append((c["article_id"], c["username"], c["comment"], "approved", "", c["created_at"]))
            total_comments += 1
        
        # 30% chance of 1-2 Kazakh comments
        if random.random() < 0.3:
            num_kz = random.choices([1, 2], weights=[70, 30])[0]
            for _ in range(num_kz):
                c = generate_comment(article, is_kz=True)
                batch.append((c["article_id"], c["username"], c["comment"], "approved", "", c["created_at"]))
                total_comments += 1
        
        # Insert in batches of 500
        if len(batch) >= 500:
            conn.executemany("""
                INSERT INTO public_comments (article_id, author_name, text, status, ip_address, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            print(f"  Inserted {total_comments} comments...")
            batch = []
    
    if batch:
        conn.executemany("""
            INSERT INTO public_comments (article_id, author_name, text, status, ip_address, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, batch)
        conn.commit()
    
    print(f"\nDone: {total_comments} comments created for {len(articles)} articles")
    
    total = conn.execute("SELECT count(*) FROM public_comments").fetchone()[0]
    print(f"Total in DB: {total}")
    
    rows = conn.execute("SELECT author_name, count(*) FROM public_comments GROUP BY author_name ORDER BY count(*) DESC LIMIT 5").fetchall()
    print("\nTop commenters:")
    for r in rows:
        print(f"  {r[0]}: {r[1]}")
    
    conn.close()


if __name__ == "__main__":
    main()
