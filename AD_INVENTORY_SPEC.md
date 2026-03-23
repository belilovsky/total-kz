# Рекламный инвентарь total.kz — Спецификация v1.0

## Позиции баннерной рекламы

### Глобальные (base.html — на всех страницах)
| ID | Название | Класс CSS | Размер Desktop | Размер Mobile | Расположение |
|----|----------|-----------|---------------|--------------|-------------|
| A1 | Топ-баннер | `ad-top-banner` | 970×90 | 320×50 | Над шапкой (самый верх) |
| A2 | Под шапкой | `ad-below-header` | 728×90 | 320×100 | Под навигацией, над контентом |
| A3 | Футер-баннер | `ad-footer-banner` | 728×90 | 320×50 | Перед футером |

### Главная (home.html)
| ID | Название | Класс CSS | Размер Desktop | Размер Mobile | Расположение |
|----|----------|-----------|---------------|--------------|-------------|
| H1 | После hero | `ad-leaderboard` | 728×90 | 320×100 | Между hero и категориями |
| H2 | В ленте #1 | `ad-in-feed` | 100%×80 | 100%×80 | После 3-й секции категорий |
| H3 | В ленте #2 | `ad-in-feed` | 100%×80 | 100%×80 | После 6-й секции категорий |
| H4 | Нативный в ленте | `ad-native-feed` | 100% | 100% | Между карточками (выглядит как карточка) |

### Статья (article.html)
| ID | Название | Класс CSS | Размер Desktop | Размер Mobile | Расположение |
|----|----------|-----------|---------------|--------------|-------------|
| R1 | Под заголовком | `ad-article-top` | 728×90 | 320×100 | Между заголовком и телом статьи |
| R2 | В тексте статьи | `ad-inline` | 100%×80 | 100%×80 | После 3-го параграфа (JS injection) |
| R3 | В тексте статьи #2 | `ad-inline` | 100%×80 | 100%×80 | После 7-го параграфа (JS injection) |
| R4 | Сайдбар sticky | `ad-sidebar` | 300×250 | скрыт | Sticky в сайдбаре |
| R5 | Сайдбар #2 | `ad-sidebar-btf` | 300×600 | скрыт | Второй блок сайдбара |
| R6 | После статьи | `ad-after-article` | 728×90 | 320×100 | Между статьёй и блоком "Читайте также" |

### Категория (category.html)
| ID | Название | Класс CSS | Размер Desktop | Размер Mobile | Расположение |
|----|----------|-----------|---------------|--------------|-------------|
| C1 | Над списком | `ad-leaderboard` | 728×90 | 320×100 | После заголовка категории |
| C2 | Сайдбар | `ad-sidebar` | 300×250 | скрыт | В сайдбаре |
| C3 | В ленте | `ad-in-feed` | 100%×80 | 100%×80 | После каждых 10 статей |

### Поиск (search.html)
| ID | Название | Класс CSS | Размер Desktop | Размер Mobile | Расположение |
|----|----------|-----------|---------------|--------------|-------------|
| S1 | Над результатами | `ad-leaderboard` | 728×90 | 320×100 | Между поисковой формой и результатами |

### Персоны / Теги (persons.html, tag.html, entity.html)
| ID | Название | Класс CSS | Размер Desktop | Размер Mobile | Расположение |
|----|----------|-----------|---------------|--------------|-------------|
| P1 | Над списком | `ad-leaderboard` | 728×90 | 320×100 | После заголовка |
| P2 | В ленте | `ad-in-feed` | 100%×80 | 100%×80 | После 10 элементов |

### Профиль персоны (person.html)
| ID | Название | Класс CSS | Размер Desktop | Размер Mobile | Расположение |
|----|----------|-----------|---------------|--------------|-------------|
| PP1 | Между секциями | `ad-person-mid` | 728×90 | 320×100 | Между биографией и статьями |

## CSS-классы рекламных блоков

```css
/* Базовый стиль заглушки */
.ad-placeholder {
  display:flex; align-items:center; justify-content:center;
  border:none; border-radius:var(--radius-md);
  background:linear-gradient(135deg, var(--surface-2) 0%, var(--surface-3) 50%, var(--surface-2) 100%);
  opacity:0.45; position:relative; overflow:hidden;
  color:var(--text-faint); font-size:var(--text-xs);
  text-transform:uppercase; letter-spacing:0.1em;
}

/* data-ad-slot="..." — для идентификации позиции */
/* data-ad-size="970x90" — для идентификации размера */
```

## HTML-шаблон заглушки
```html
<div class="ad-placeholder ad-{type}" data-ad-slot="{slot_id}" data-ad-size="{WxH}" aria-hidden="true">
  <span class="ad-label">Реклама</span>
</div>
```

## Адаптивность
- Desktop (≥1100px): все позиции видимы
- Tablet (768-1099px): сайдбар скрыт, leaderboard → 728×90
- Mobile (≤767px): leaderboard → 320×50/100, in-feed → 100%×80, sidebar скрыт

## Таблица ad_placements (SQLite)
```sql
CREATE TABLE IF NOT EXISTS ad_placements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slot_id TEXT UNIQUE NOT NULL,       -- 'A1', 'H1', 'R4' etc.
    name TEXT NOT NULL,                  -- 'Топ-баннер'
    page TEXT NOT NULL,                  -- 'global', 'home', 'article', etc.
    position TEXT NOT NULL,              -- 'above-header', 'in-feed', 'sidebar'
    size_desktop TEXT,                   -- '970x90'
    size_mobile TEXT,                    -- '320x50'
    css_class TEXT NOT NULL,             -- 'ad-top-banner'
    is_active INTEGER DEFAULT 1,         -- 1=показывать заглушку, 0=скрыть
    client_name TEXT,                    -- 'Kaspi Bank'
    client_url TEXT,                     -- redirect URL
    image_url TEXT,                      -- баннер (если загружен)
    start_date TEXT,                     -- '2026-04-01'
    end_date TEXT,                       -- '2026-04-30'
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    cpm_rate REAL,                       -- цена за 1000 показов
    notes TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
```
