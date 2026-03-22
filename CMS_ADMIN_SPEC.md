# CMS Admin Panel Specification — total.kz v10

## Overview
Build a full CMS admin panel for journalists and editors. The current `/admin` is an analytics dashboard — it stays as-is (renamed to "Аналитика" section). The NEW admin must be a real working CMS, mobile-first, optimized for editing from a phone.

## Tech Stack
- Backend: FastAPI (Python 3.11), SQLite, Jinja2 templates
- Frontend: Vanilla JS (no React/Vue), HTML templates extending `base.html`
- CSS: existing `style.css` with CSS variables (light/dark theme)
- Font: Inter (already loaded via Google Fonts)
- Brand: #d83236 (for total.kz brand accent), but admin uses blue accent (#2563eb)
- ALL UI text in Russian

## Database Schema Changes
Add new columns to `articles` table:
```sql
ALTER TABLE articles ADD COLUMN status TEXT DEFAULT 'published';  -- 'draft', 'published', 'archived'
ALTER TABLE articles ADD COLUMN updated_at TEXT;
ALTER TABLE articles ADD COLUMN editor_note TEXT;  -- internal notes for editors
```

Add new table for media uploads:
```sql
CREATE TABLE IF NOT EXISTS media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    original_name TEXT,
    mime_type TEXT,
    file_size INTEGER,
    url TEXT NOT NULL,
    uploaded_at TEXT DEFAULT (datetime('now')),
    uploaded_by TEXT
);
```

## Navigation Structure
Keep the current sidebar + mobile bottom tabs. Update nav items:

1. **Дашборд** (`/admin`) — existing analytics dashboard, unchanged
2. **Статьи** (`/admin/articles`) — article list with filters + bulk actions
3. **Создать** (`/admin/create`) — NEW: create article page
4. **Контент** (`/admin/content`) — existing tags/entities management
5. **Аналитика** (`/admin/analytics`) — existing GSC/SEO analytics

## Pages to Build

### 1. Article List (`/admin/articles`) — ENHANCE existing
- Add status filter tabs: Все | Опубликованные | Черновики | Архив
- Add "Создать статью" button (top-right on desktop, FAB on mobile)
- Each row shows: thumbnail, title, category, author, date, status badge
- Bulk actions: publish, archive, delete (with checkboxes)
- Quick status toggle (draft ↔ published) via button
- Search works on title + body_text

### 2. Article Editor (`/admin/article/{id}`) — MAJOR REWRITE
This is the core feature. Must work well on mobile.

**Layout (mobile-first):**
- Full-width content area, no sidebar distraction
- Sticky top bar: ← Back, Status badge, Save button
- Bottom action bar on mobile: Preview | Save Draft | Publish

**Editor sections (top to bottom):**
1. **Title** — large textarea, auto-resize
2. **Excerpt/Lead** — textarea, max 300 chars with counter
3. **Main Image** — image preview + URL input + upload button
4. **Body** — Rich text editor using TinyMCE (CDN) or Quill
   - Toolbar: Bold, Italic, Link, Image, Blockquote, H2, H3, Lists, Code
   - Image insertion via URL
   - HTML source view toggle
   - Mobile: compact floating toolbar
5. **Category** — dropdown select
6. **Tags** — chip editor (existing logic works, keep it)
7. **Author** — text input with autocomplete from known authors
8. **Editor Notes** — textarea for internal notes (not shown on site)
9. **Status** — draft/published/archived radio buttons
10. **Metadata** — read-only: ID, URL, import date, last updated

**API endpoints needed:**
- `PATCH /api/article/{id}` — update (already exists, extend with body_html, status, editor_note, updated_at)
- `POST /api/article` — create new article
- `DELETE /api/article/{id}` — soft-delete (set status='archived')

### 3. Create Article (`/admin/create`) — NEW
Same editor UI as article edit, but:
- Empty form
- Auto-generate slug from title (transliterate Russian → Latin)
- Default status: 'draft'
- URL auto-constructed: https://total.kz/ru/news/{category}/{slug}
- On save: redirect to `/admin/article/{new_id}`

### 4. Article Preview
- Button "Предпросмотр" opens article in public template style
- Either as modal overlay or new tab to `/news/{category}/{slug}?preview=1`

## API Updates (main.py)

### Extend PATCH /api/article/{id}
Add to allowed fields: `body_html`, `body_text`, `status`, `editor_note`, `updated_at`
On save, auto-set `updated_at` to current timestamp.

### POST /api/article (NEW)
Required: title, sub_category
Optional: everything else
Auto-generate URL if not provided
Returns: {ok: true, id: new_id}

### DELETE /api/article/{id} (NEW)  
Set status = 'archived', return {ok: true}

### POST /api/upload (NEW — Phase 2, stub for now)
File upload endpoint (for later when we connect real file storage)
For now, just return an error message saying "Загрузка файлов будет доступна позже"

## Styling Guidelines
- Use existing CSS variables from style.css
- Status badges: draft=orange, published=green, archived=gray
- Editor must be readable on mobile (min font 16px for inputs to prevent iOS zoom)
- WYSIWYG toolbar should be sticky/floating on mobile
- Touch targets: min 44px
- Auto-save indicator (optional nice-to-have): "Сохранено 2 мин назад"
- Numbers: always use space as thousands separator (format_num filter)

## File Structure
```
app/
  templates/
    base.html          — update nav (add "Создать" item)
    articles.html      — enhance with status filter, bulk actions
    article.html       — REWRITE: full WYSIWYG editor
    article_create.html — NEW: create article form
  main.py              — add new endpoints
  database.py          — add create_article(), update schema
  static/
    css/style.css      — add editor styles, status badges
    js/app.js          — existing JS
    js/editor.js       — NEW: editor logic (optional, can be inline)
```

## Important Rules
- ALL numbers use space as thousands separator
- ALL UI in Russian
- Mobile-first: design for 375px width first, then scale up
- Dark/light theme must work everywhere
- No external dependencies except TinyMCE or Quill CDN
- Keep existing admin pages working — don't break dashboard/analytics/content
- body_html is the source of truth for article content (body_text is derived)
