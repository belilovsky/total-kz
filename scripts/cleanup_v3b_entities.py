#!/usr/bin/env python3
"""
Entity cleanup v3b – Extended fixes:
- Media sources as orgs → delete
- Grammar-broken Министерство/Комитет variants → merge into canonical
- Org specificity (Верховная рада → Верховная Рада Украины, etc.)
- Person fixes (Елбасы=Назарбаев, declension errors, Нур-Султан→Астана)
- Bulk delete low-link garbage

Run: python scripts/cleanup_v3b_entities.py              # dry run
     python scripts/cleanup_v3b_entities.py --execute     # apply
"""
import sqlite3
import os
import sys
import re
import shutil
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'total.db')
BACKUP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'backups')


def backup_db():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    bp = os.path.join(BACKUP_DIR, f'total_{ts}.db')
    print(f"Backup... ", end='', flush=True)
    shutil.copy2(DB_PATH, bp)
    print(f"✓ ({os.path.getsize(bp)/1024/1024:.0f} MB)")


def find_entity(conn, name, entity_type=None):
    if entity_type:
        row = conn.execute(
            "SELECT id, name, entity_type FROM entities WHERE normalized=? AND entity_type=?",
            (name.lower().strip(), entity_type)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id, name, entity_type FROM entities WHERE normalized=?",
            (name.lower().strip(),)
        ).fetchone()
    return row


def get_link_count(conn, entity_id):
    return conn.execute("SELECT COUNT(*) FROM article_entities WHERE entity_id=?", (entity_id,)).fetchone()[0]


def merge_entity(conn, source_id, target_id, dry_run):
    """Merge source→target: move links, delete source."""
    if source_id == target_id:
        return
    if not dry_run:
        conn.execute("""
            UPDATE article_entities SET entity_id=?
            WHERE entity_id=? AND article_id NOT IN
            (SELECT article_id FROM article_entities WHERE entity_id=?)
        """, (target_id, source_id, target_id))
        conn.execute("DELETE FROM article_entities WHERE entity_id=?", (source_id,))
        conn.execute("DELETE FROM entities WHERE id=?", (source_id,))


def delete_by_id(conn, entity_id, dry_run):
    if not dry_run:
        conn.execute("DELETE FROM article_entities WHERE entity_id=?", (entity_id,))
        conn.execute("DELETE FROM entities WHERE id=?", (entity_id,))


def run(dry_run=True):
    if not dry_run:
        backup_db()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-200000")
    conn.execute("PRAGMA temp_store=MEMORY")

    total_b = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"ENTITY CLEANUP v3b {'(DRY RUN)' if dry_run else '(EXECUTING)'}")
    print(f"{'='*60}")
    print(f"Entities before: {total_b:,}\n")

    stats = {'merged': 0, 'deleted': 0, 'renamed': 0}

    # ═══════════════════════════════════════════════════════════════
    # STEP 1: DELETE MEDIA SOURCE ORGS
    # ═══════════════════════════════════════════════════════════════
    print("── Step 1: Delete media source orgs ──")

    # These are citation sources extracted from article text, not real entities
    MEDIA_DELETE = [
        # .ru sites
        'RG.ru', 'Film.ru', 'Ura.ru', 'NEWS.ru', 'Life.ru',
        'RuNews24.ru', '360.ru',
        # .kz sites
        'Sports.kz', 'Stan.kz', 'Skifnews.kz', 'Lada.kz', 'Olympic.kz',
        'Enbek.kz', 'E-petition.kz', 'Petition.kz', 'Almaty.kz', 'Vesti.kz',
        'Nege.kz', '24.kz', 'Adyrna.kz', 'Sn.kz', 'YK-news.kz', 'Tota.kz',
        'Kolesa.kz', 'Exclusive.kz', 'Press.kz', 'Massaget.kz', 'Krisha.kz',
        'Polisia.kz', 'eGov.kz',
        # Media names without domains
        'Kstnews', 'Kinonews', 'Pavlodarnews', 'Euronews', 'Euronews.com',
        'ABC News', 'NBC News',
        'Tobolinfo', 'Offside', 'WABetaInfo', 'AlmatyJoly', 'Inaktau',
        'Atyrau Online', 'Ranking', 'Pavon', 'Halyqstan', 'Respublica',
        'Vesti', 'Petition',
        # Pure tech/entertainment brands as citation sources
        'Deadline', 'Variety',
        # Generic
        'Комитет',  # bare word, no specifics
        'Министерство',  # bare word
        'Администрация',  # bare word
        'ИА',  # Information Agency without name
    ]

    for name in MEDIA_DELETE:
        row = find_entity(conn, name, 'org')
        if row:
            cnt = get_link_count(conn, row[0])
            print(f"    delete «{row[1]}» ({cnt} links)")
            delete_by_id(conn, row[0], dry_run)
            stats['deleted'] += 1

    # 1b. Auto-delete ALL domain-like org entities (*.ru, *.kz, *.com, etc.)
    # These are always citation sources from article text, not real entities
    DOMAIN_EXTENSIONS = ('.ru', '.kz', '.uz', '.com', '.net', '.org', '.io', '.me',
                         '.tv', '.kg', '.by', '.ua', '.info', '.pro',
                         '.ру', '.уз')
    # Whitelist: real orgs that happen to have domain-like names
    DOMAIN_WHITELIST = {'kaspi.kz'}

    domain_orgs = conn.execute("""
        SELECT e.id, e.name
        FROM entities e
        WHERE e.entity_type = 'org'
    """).fetchall()
    domain_del = 0
    for eid, ename in domain_orgs:
        name_lower = ename.lower().strip()
        if any(name_lower.endswith(ext) for ext in DOMAIN_EXTENSIONS):
            if name_lower in DOMAIN_WHITELIST:
                continue
            cnt = get_link_count(conn, eid)
            if cnt > 0:
                print(f"    delete domain org «{ename}» ({cnt} links)")
            delete_by_id(conn, eid, dry_run)
            domain_del += 1
            stats['deleted'] += 1

    # Also delete orgs whose name contains 'news'/'News' and have <= 50 links
    news_orgs = conn.execute("""
        SELECT e.id, e.name, COUNT(ae.article_id) as cnt
        FROM entities e
        LEFT JOIN article_entities ae ON ae.entity_id = e.id
        WHERE e.entity_type = 'org'
          AND (e.name LIKE '%news%' OR e.name LIKE '%News%')
        GROUP BY e.id
        HAVING cnt <= 50
    """).fetchall()
    for eid, ename, cnt in news_orgs:
        print(f"    delete news-source org «{ename}» ({cnt} links)")
        delete_by_id(conn, eid, dry_run)
        domain_del += 1
        stats['deleted'] += 1
    print(f"  Auto-deleted {domain_del} domain/news-source orgs")

    # ═══════════════════════════════════════════════════════════════
    # STEP 2: MERGE BROKEN МИНИСТЕРСТВО/КОМИТЕТ VARIANTS
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 2: Merge broken Министерство/Комитет/Служба/Агентство ──")

    # Map pattern → canonical entity to merge into
    # Key: substring match in entity name → target canonical name
    MINISTRY_MERGES = {
        # Digital development → МЦРИАП
        'цифров': 'МЦРИАП РК',
        'информации': 'МЦРИАП РК',
        'информация': 'МЦРИАП РК',
        'информационные': 'МЦРИАП РК',
        'цифровизация': 'МЦРИАП РК',
        'ии и цифров': 'МЦРИАП РК',
        'аэрокосмическ': 'МЦРИАП РК',
        'искусственный интеллект': 'МЦРИАП РК',

        # Science & higher education
        'наука и высочайшее': 'МОН РК',
        'наука и образование': 'МОН РК',
        'высочайшее образование': 'МОН РК',
        'образование и наука': 'МОН РК',

        # Economy
        'нацэкономика': 'Миннацэкономики РК',
        'нацэкономики': 'Миннацэкономики РК',
        'экономика и бюджет': 'Миннацэкономики РК',
        'экономика и финанс': 'Миннацэкономики РК',
        'экономика и планир': 'Миннацэкономики РК',
        'экономика и торговл': 'Миннацэкономики РК',
        'экономическое развитие': 'Миннацэкономики РК',
        'экономика и коммерц': 'Миннацэкономики РК',

        # Trade & integration → Минторговли
        'торговля и интеграция': 'Минторговли РК',

        # Agriculture
        'сельское хозяйств': 'Минсельхоз РК',
        'сельского': 'Минсельхоз РК',
        'сельский хозяйств': 'Минсельхоз РК',

        # Labor
        'труд и социальная защита': 'Минтруда РК',
        'труд и соцзащита': 'Минтруда РК',
        'занятость': 'Минтруда РК',
        'социальное обеспечение': 'Минтруда РК',

        # Health
        'здравоохранение': 'Минздрав РК',

        # Defense
        'обороны': 'Минобороны РК',

        # MIA
        'внутренние дела': 'МВД РК',
        'внутенние': 'МВД РК',
        'внутренние': 'МВД РК',

        # Emergency
        'чрезвычайн': 'МЧС РК',
        'по чс': 'МЧС РК',

        # Ecology
        'экология': 'Минэкологии РК',
        'окружающая среда': 'Минэкологии РК',
        'природные ресурсы': 'Минэкологии РК',
        'природа': 'Минэкологии РК',
        'охрана окружающ': 'Минэкологии РК',

        # Transport
        'транспорт': 'Минтранс РК',
        'коммуникац': 'Минтранс РК',
        'связь': 'Минтранс РК',
        'путей сообщен': 'Минтранс РК',

        # Finance
        'финансов': 'Минфин РК',
        'финансы': 'Минфин РК',

        # Energy
        'энергетик': 'Минэнерго РК',
        'энергетич': 'Минэнерго РК',
        'гидроэнергет': 'Минэнерго РК',

        # Industry & construction
        'промышленность': 'Минпром РК',
        'строительств': 'Минпром РК',
        'индустри': 'Минпром РК',

        # Education
        'просвещение': 'Минпросвещения РК',
        'образование': 'Минпросвещения РК',
        'дошкольный': 'Минпросвещения РК',
        'среднее образов': 'Минпросвещения РК',

        # Justice
        'юстиция': 'Минюст РК',

        # Foreign Affairs
        'иностранн': 'МИД РК',

        # Culture
        'культура': 'Минкультуры РК',

        # Tourism & Sport
        'туризм': 'Минтуризма РК',
        'спорт': 'Минтуризма РК',

        # Water
        'водные ресурс': 'Минводресурсов РК',

        # Investment
        'инвестици': 'Минпром РК',

        # State security (NK)
        'государственная безопасность': 'КНБ РК',

        # Propaganda
        'пропаганд': None,  # delete – Afghan/Taliban ministry, not KZ

        # Mining
        'горная промышленность': 'Минпром РК',

        # Posts
        'почта': 'Минтранс РК',
    }

    # Process all "Министерство ..." entities
    all_ministry = conn.execute("""
        SELECT e.id, e.name, COUNT(ae.article_id) as cnt
        FROM entities e
        LEFT JOIN article_entities ae ON ae.entity_id = e.id
        WHERE e.entity_type = 'org' AND e.name LIKE 'Министерство%'
        GROUP BY e.id
    """).fetchall()

    # Exclude already canonical names
    CANONICAL_NAMES = {
        'Минздрав РК', 'МВД РК', 'МИД РК', 'Минобороны РК', 'Минфин РК',
        'Минсельхоз РК', 'Минэнерго РК', 'Минтруда РК', 'МЧС РК',
        'Минпросвещения РК', 'Минюст РК', 'Минтранс РК', 'Минпром РК',
        'Минторговли РК', 'Минтуризма РК', 'Минкультуры РК', 'Минэкологии РК',
        'Миннацэкономики РК', 'Минводресурсов РК', 'МЦРИАП РК', 'МОН РК',
    }

    merged_ministry = 0
    deleted_ministry = 0
    for eid, ename, cnt in all_ministry:
        if ename in CANONICAL_NAMES:
            continue
        name_lower = ename.lower()
        target_name = None

        for pattern, canon in MINISTRY_MERGES.items():
            if pattern in name_lower:
                target_name = canon
                break

        if target_name is None:
            # Check for garbage (слипшиеся строки – letters run together)
            if re.search(r'[а-яА-Я][А-Я]', ename) and len(ename) > 30:
                # Garbage – fused text
                print(f"    delete garbage «{ename}» ({cnt} links)")
                delete_by_id(conn, eid, dry_run)
                deleted_ministry += 1
                stats['deleted'] += 1
                continue
            elif cnt <= 2:
                # Low-link unknown ministry, just delete
                delete_by_id(conn, eid, dry_run)
                deleted_ministry += 1
                stats['deleted'] += 1
                continue
            else:
                print(f"    ? unmatched: «{ename}» ({cnt} links)")
                continue

        if target_name == '':
            continue  # skip

        # target_name is None → delete
        if target_name is None:
            print(f"    delete «{ename}» ({cnt} links)")
            delete_by_id(conn, eid, dry_run)
            deleted_ministry += 1
            stats['deleted'] += 1
            continue

        # Find target
        target = find_entity(conn, target_name, 'org')
        if not target:
            # Create it by renaming this one
            print(f"    rename «{ename}» → «{target_name}» ({cnt} links) [new canonical]")
            if not dry_run:
                conn.execute("UPDATE entities SET name=?, normalized=? WHERE id=?",
                             (target_name, target_name.lower(), eid))
            stats['renamed'] += 1
            continue

        if target[0] == eid:
            continue

        print(f"    merge «{ename}» ({cnt} links) → «{target_name}»")
        merge_entity(conn, eid, target[0], dry_run)
        merged_ministry += 1
        stats['merged'] += 1

    print(f"  Ministry variants: {merged_ministry} merged, {deleted_ministry} deleted")

    # Now process Комитет variants – bulk delete all with <= 5 links
    print("\n  Processing Комитет variants...")
    all_komitet = conn.execute("""
        SELECT e.id, e.name, COUNT(ae.article_id) as cnt
        FROM entities e
        LEFT JOIN article_entities ae ON ae.entity_id = e.id
        WHERE e.entity_type = 'org' AND e.name LIKE 'Комитет%'
        GROUP BY e.id
    """).fetchall()

    # Specific merges for committees
    KOMITET_MERGES = {
        'санитарно-эпидемиологическ': 'Комитет санэпидконтроля МЗ РК',
        'санэпидконтроль': 'Комитет санэпидконтроля МЗ РК',
        'гражданская авиация': 'Комитет гражданской авиации МТ РК',
        'автомобильные дороги': 'Комитет автомобильных дорог МТ РК',
        'автомобильный транспорт': 'Комитет автомобильных дорог МТ РК',
        'уголовно-исполнительн': 'КУИС МЮ РК',
        'уголовно-исправительн': 'КУИС МЮ РК',
        'техническое регулирование': 'Комитет техрегулирования МТИ РК',
        'техрегулирования': 'Комитет техрегулирования МТИ РК',
        'госдоход': 'КГД МФ РК',
        'государственные доходы': 'КГД МФ РК',
        'госинспекц': 'Госинспекция в АПК',
        'государственная инспекция': 'Госинспекция в АПК',
        'рыбное хозяйство': 'Комитет рыбного хозяйства МЭ РК',
        'ветеринарн': 'Комитет ветконтроля МСХ РК',
        'фармацевтическ': 'КМФК МЗ РК',
        'фармация': 'КМФК МЗ РК',
        'медицинск': 'КМФК МЗ РК',
        'медико-фармац': 'КМФК МЗ РК',
        'охрана права дети': 'Комитет по охране прав детей МТСЗН РК',
        'защита права потребител': 'Комитет по защите прав потребителей МТИ РК',
        'управление земельные': 'Комитет по управлению земресурсами МСХ РК',
        'земельные ресурсы': 'Комитет по управлению земресурсами МСХ РК',
        'денежно-кредитная': 'Комитет по ДКП НБ РК',
        'гражданская оборона': 'КГО МЧС РК',
        'административная полиция': 'Комитет адмполиции МВД РК',
        'адмполиции': 'Комитет адмполиции МВД РК',
        'правовая статистика': 'КПС ГП РК',
        'спецучет': 'КПС ГП РК',
        'естественные монополии': 'КРЕМ МНЭ РК',
        'регулирование естественные': 'КРЕМ МНЭ РК',
        'внутренний государственный аудит': 'КВГА МФ РК',
        'внутренний госаудит': 'КВГА МФ РК',
        'внешний государственный аудит': 'Высшая аудиторская палата РК',
        'госаудит': 'КВГА МФ РК',
        'промышленная безопасность': 'Комитет промбезопасности МЧС РК',
        'противодействие наркопреступ': 'Комитет по противодействию наркопреступности МВД РК',
        'казначейство': 'Комитет казначейства МФ РК',
        'торговля': 'Комитет торговли МТИ РК',
        'индустрия туризм': 'Комитет индустрии туризма МТС РК',
        'индустри': 'Комитет индустрии туризма МТС РК',
        'противопожарн': 'Комитет противопожарной службы МЧС РК',
        'предупреждение чс': 'Комитет предупреждения ЧС МЧС РК',
        'предупреждение чрезвычайн': 'Комитет предупреждения ЧС МЧС РК',
        'нацбезопасность': 'КНБ РК',
        'национальная безопасность': 'КНБ РК',
        'государственная безопасность': 'КНБ РК',
        'госбезопасность': 'КНБ РК',
        'энергонадзор': 'Комитет энергонадзора МЭ РК',
        'энергетический надзор': 'Комитет энергонадзора МЭ РК',
        'атомное': 'Комитет энергонадзора МЭ РК',
        'миграция': 'Комитет миграционной службы МВД РК',
        'миграционная': 'Комитет миграционной службы МВД РК',
        'солдатских': None,  # not KZ entity
        'религия': 'Комитет по делам религий МК РК',
        'религии': 'Комитет по делам религий МК РК',
        'по делам строительств': 'Комитет строительства и ЖКХ МПРК',
        'строительство и жилищн': 'Комитет строительства и ЖКХ МПРК',
    }

    komitet_merged = 0
    komitet_deleted = 0
    for eid, ename, cnt in all_komitet:
        name_lower = ename.lower()
        target_name = None

        for pattern, canon in KOMITET_MERGES.items():
            if pattern in name_lower:
                target_name = canon
                break

        if target_name is None:
            if cnt <= 3:
                delete_by_id(conn, eid, dry_run)
                komitet_deleted += 1
                stats['deleted'] += 1
                continue
            # Check for garbage
            if re.search(r'[а-яА-Я][А-Я]', ename) and len(ename) > 30:
                delete_by_id(conn, eid, dry_run)
                komitet_deleted += 1
                stats['deleted'] += 1
                continue
            if cnt > 3:
                print(f"    ? unmatched: «{ename}» ({cnt} links)")
            continue

        if target_name is None:
            # Explicit delete
            print(f"    delete «{ename}» ({cnt} links)")
            delete_by_id(conn, eid, dry_run)
            komitet_deleted += 1
            stats['deleted'] += 1
            continue

        target = find_entity(conn, target_name, 'org')
        if not target:
            print(f"    rename «{ename}» → «{target_name}» ({cnt} links) [new canonical]")
            if not dry_run:
                conn.execute("UPDATE entities SET name=?, normalized=? WHERE id=?",
                             (target_name, target_name.lower(), eid))
            stats['renamed'] += 1
            continue

        if target[0] == eid:
            continue

        merge_entity(conn, eid, target[0], dry_run)
        komitet_merged += 1
        stats['merged'] += 1

    print(f"  Komitet variants: {komitet_merged} merged, {komitet_deleted} deleted")

    # ═══════════════════════════════════════════════════════════════
    # STEP 3: SPECIFIC ORG FIXES
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 3: Specific org fixes ──")

    SPECIFIC_FIXES = [
        # Rename for specificity
        ('Верховная рада', 'org', 'rename', 'Верховная Рада Украины'),
        ('Администрация президент', 'org', 'rename', 'Администрация Президента РК'),
        # СЦК merge
        ('Служба центральные коммуникации', 'org', 'merge', 'СЦК'),
        # Таможенный союз ЕАЭС → merge into ЕАЭС (it's the same org context)
        ('Таможенный союз ЕАЭС', 'org', 'merge', 'ЕАЭС'),
        # ЕАЭС location duplicate
        ('ЕАЭС', 'location', 'delete', None),
        # МОН РК – merge Министерство наука into it
        ('Министерство наука', 'org', 'merge', 'МОН РК'),
        ('Министерство образование', 'org', 'merge', 'Минпросвещения РК'),
    ]

    for name, etype, action, target_name in SPECIFIC_FIXES:
        row = find_entity(conn, name, etype)
        if not row:
            continue
        cnt = get_link_count(conn, row[0])

        if action == 'rename':
            # Check if target already exists
            existing = find_entity(conn, target_name, etype)
            if existing and existing[0] != row[0]:
                print(f"    merge «{row[1]}» ({cnt} links) → «{target_name}» (exists)")
                merge_entity(conn, row[0], existing[0], dry_run)
                stats['merged'] += 1
            else:
                print(f"    rename «{row[1]}» → «{target_name}» ({cnt} links)")
                if not dry_run:
                    conn.execute("UPDATE entities SET name=?, normalized=? WHERE id=?",
                                 (target_name, target_name.lower(), row[0]))
                stats['renamed'] += 1

        elif action == 'merge':
            target = find_entity(conn, target_name, etype)
            if target and target[0] != row[0]:
                print(f"    merge «{row[1]}» ({cnt} links) → «{target_name}»")
                merge_entity(conn, row[0], target[0], dry_run)
                stats['merged'] += 1
            elif not target:
                print(f"    rename «{row[1]}» → «{target_name}» ({cnt} links) [target not found]")
                if not dry_run:
                    conn.execute("UPDATE entities SET name=?, normalized=? WHERE id=?",
                                 (target_name, target_name.lower(), row[0]))
                stats['renamed'] += 1

        elif action == 'delete':
            print(f"    delete «{row[1]}» ({etype}, {cnt} links)")
            delete_by_id(conn, row[0], dry_run)
            stats['deleted'] += 1

    # ═══════════════════════════════════════════════════════════════
    # STEP 4: PERSON FIXES
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 4: Person fixes ──")

    # Елбасы → Нурсултан Назарбаев
    row_elbasy = find_entity(conn, 'Елбасы', 'person')
    row_naz = find_entity(conn, 'Нурсултан Назарбаев', 'person')
    if row_elbasy and row_naz:
        cnt = get_link_count(conn, row_elbasy[0])
        print(f"    merge «Елбасы» ({cnt} links) → «Нурсултан Назарбаев»")
        merge_entity(conn, row_elbasy[0], row_naz[0], dry_run)
        stats['merged'] += 1
    elif row_elbasy:
        print(f"    rename «Елбасы» → «Нурсултан Назарбаев»")
        if not dry_run:
            conn.execute("UPDATE entities SET name=?, normalized=? WHERE id=?",
                         ('Нурсултан Назарбаев', 'нурсултан назарбаев', row_elbasy[0]))
        stats['renamed'] += 1

    # Fix declension errors in person names
    PERSON_FIXES = [
        ('Барак Обамы', 'Барак Обама'),
        ('Тамары Дуйсеновой', 'Тамара Дуйсенова'),
        ('Маншук Маметовой', 'Маншук Маметова'),
        ('Акмарал Альназаровой', 'Акмарал Альназарова'),
        ('Аиде Балаевой', 'Аида Балаева'),
        ('Айгуль Сайлыбаевой', 'Айгуль Сайлыбаева'),
        ('Гаухар Шаймерденовой', 'Гаухар Шаймерденова'),
        ('Фаризы Онгарсыновой', 'Фариза Онгарсынова'),
        ('А. Молдагуловой', 'Алия Молдагулова'),
        ('Зауре Галиевы', 'Зауре Галиева'),
        ('Гайни Алашбаевой', 'Гайни Алашбаева'),
    ]
    for wrong, correct in PERSON_FIXES:
        row = find_entity(conn, wrong, 'person')
        if not row:
            continue
        cnt = get_link_count(conn, row[0])
        target = find_entity(conn, correct, 'person')
        if target and target[0] != row[0]:
            print(f"    merge «{row[1]}» ({cnt} links) → «{correct}»")
            merge_entity(conn, row[0], target[0], dry_run)
            stats['merged'] += 1
        else:
            print(f"    rename «{row[1]}» → «{correct}» ({cnt} links)")
            if not dry_run:
                conn.execute("UPDATE entities SET name=?, normalized=? WHERE id=?",
                             (correct, correct.lower(), row[0]))
            stats['renamed'] += 1

    # Bulk fix: find all person names ending in declension forms (ой, ей, ых, ому, etc.)
    # These are likely wrong grammatical forms
    print("\n  Bulk delete declension-form person names with <= 3 links...")
    decl_persons = conn.execute("""
        SELECT e.id, e.name, COUNT(ae.article_id) as cnt
        FROM entities e
        LEFT JOIN article_entities ae ON ae.entity_id = e.id
        WHERE e.entity_type = 'person'
          AND e.name LIKE '% %'
          AND (e.name LIKE '%ой' OR e.name LIKE '%ей' OR e.name LIKE '%ому'
               OR e.name LIKE '%ем' OR e.name LIKE '%ых' OR e.name LIKE '%их')
        GROUP BY e.id
        HAVING cnt <= 3
    """).fetchall()
    # Filter: only if last word ends with declension suffix
    dec_count = 0
    for eid, ename, cnt in decl_persons:
        parts = ename.split()
        if len(parts) >= 2:
            last = parts[-1]
            # Skip legitimate names ending in -ой, -ей (e.g. Андрей, Тимофей, Алексей)
            # Only target clear declension of surnames
            if last.endswith(('овой', 'евой', 'иной', 'овны', 'евны', 'ской', 'ному', 'евич')):
                delete_by_id(conn, eid, dry_run)
                dec_count += 1
                stats['deleted'] += 1
    print(f"    Deleted {dec_count} declension-form persons")

    # ═══════════════════════════════════════════════════════════════
    # STEP 5: LOCATION FIXES
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 5: Location fixes ──")

    # Нур-Султан and Нурсултан → merge into Астана
    loc_astana = find_entity(conn, 'Астана', 'location')
    if loc_astana:
        for variant in ['Нур-Султан', 'Нурсултан', 'Нур-султан']:
            row = find_entity(conn, variant, 'location')
            if row and row[0] != loc_astana[0]:
                cnt = get_link_count(conn, row[0])
                print(f"    merge «{row[1]}» ({cnt} links) → «Астана»")
                merge_entity(conn, row[0], loc_astana[0], dry_run)
                stats['merged'] += 1

    # ═══════════════════════════════════════════════════════════════
    # STEP 6: BULK DELETE LOW-LINK GARBAGE
    # ═══════════════════════════════════════════════════════════════
    print("\n── Step 6: Bulk delete entities with 1 link ──")

    # Entities with only 1 article link are usually extraction errors
    singles = conn.execute("""
        SELECT e.id, e.entity_type
        FROM entities e
        JOIN article_entities ae ON ae.entity_id = e.id
        GROUP BY e.id
        HAVING COUNT(ae.article_id) = 1
    """).fetchall()
    print(f"    Found {len(singles)} entities with exactly 1 link – deleting all")
    if not dry_run:
        for eid, etype in singles:
            conn.execute("DELETE FROM article_entities WHERE entity_id=?", (eid,))
            conn.execute("DELETE FROM entities WHERE id=?", (eid,))
    stats['deleted'] += len(singles)

    # Also delete orphan entities (no links at all)
    orphans = conn.execute("""
        SELECT e.id FROM entities e
        LEFT JOIN article_entities ae ON ae.entity_id = e.id
        WHERE ae.article_id IS NULL
    """).fetchall()
    print(f"    Found {len(orphans)} orphan entities (no links) – deleting all")
    if not dry_run:
        for (eid,) in orphans:
            conn.execute("DELETE FROM entities WHERE id=?", (eid,))
    stats['deleted'] += len(orphans)

    # ═══════════════════════════════════════════════════════════════
    # COMMIT
    # ═══════════════════════════════════════════════════════════════
    if not dry_run:
        conn.commit()

    total_a = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"  Merged:   {stats['merged']}")
    print(f"  Deleted:  {stats['deleted']}")
    print(f"  Renamed:  {stats['renamed']}")
    print(f"  Entities: {total_b:,} → {total_a:,} ({total_b - total_a:,} removed)")
    print(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    execute = "--execute" in sys.argv
    run(dry_run=not execute)
