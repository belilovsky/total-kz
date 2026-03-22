# CMS Admin Enhancement Spec — total.kz v10.1

## Overview
Enhance the CMS admin panel with professional features that make it a joy to use for journalists. All changes are in existing files — no new templates needed.

## Tech Stack (same as before)
- FastAPI + Jinja2 + SQLite + Vanilla JS
- ALL UI in Russian
- Mobile-first, dark/light theme

---

## 1. Auto-save Drafts + Save Indicator

**File: `article.html`, `article_create.html`**

Add auto-save for the article editor:
- Every 30 seconds, if any field has changed, auto-save via PATCH (edit) or POST (create)
- Show save indicator in the sticky save bar: "Сохранено" / "Сохранение..." / "Не сохранено (ошибка)"
- Use a subtle timestamp: "Сохранено 2 мин назад" that updates every minute
- On first create (article_create.html), after auto-save creates the article, redirect to edit page (so subsequent saves use PATCH)
- Track dirty state: compare current field values to last saved values
- Visual: small text in the save bar, green dot when saved, yellow when unsaved, red on error

Implementation:
```javascript
var lastSaved = null;
var isDirty = false;
var autoSaveTimer = null;

function markDirty() { isDirty = true; updateSaveIndicator('unsaved'); }
function startAutoSave() {
    autoSaveTimer = setInterval(function() {
        if (isDirty) { saveArticle(true); } // true = silent (no toast)
    }, 30000);
}
```

---

## 2. Article Preview Modal

**Files: `article.html`, `style.css`**

Add a "Предпросмотр" button next to Save that opens a full-screen modal showing the article as it would appear on the public site.

- Modal overlays the editor (z-index 9999)
- Close button top-right
- Content rendered in public article styles (import from public.css or inline)
- Mobile: full-screen overlay with slide-up animation
- Shows: title, image, author, date, body, tags — like public/article.html layout
- Button in save bar: "👁 Превью" (or just SVG eye icon)

CSS for modal:
```css
.preview-modal {
    position: fixed; inset: 0; z-index: 9999;
    background: var(--bg-body);
    overflow-y: auto;
    transform: translateY(100%);
    transition: transform 0.3s ease;
}
.preview-modal.open { transform: translateY(0); }
```

---

## 3. Keyboard Shortcuts

**File: `article.html`, `article_create.html`**

Add keyboard shortcuts:
- `Ctrl+S` (or Cmd+S on Mac) — Save article (prevent default browser save)
- `Ctrl+Shift+P` — Toggle preview modal
- `Escape` — Close preview / close modals

```javascript
document.addEventListener('keydown', function(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        saveArticle();
    }
    if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'P') {
        e.preventDefault();
        togglePreview();
    }
    if (e.key === 'Escape') { closePreview(); }
});
```

---

## 4. Word Count + Reading Time

**File: `article.html`, `article_create.html`**

Show live word count and estimated reading time below the Quill editor.

- Count words in quill.getText()
- Reading time: words / 200 (avg Russian reading speed)
- Display: "1 234 слова · ~6 мин чтения"
- Update on every text-change event from Quill
- Style: small muted text below editor, like the char counter

```javascript
quill.on('text-change', function() {
    var text = quill.getText().trim();
    var words = text ? text.split(/\s+/).length : 0;
    var minutes = Math.max(1, Math.round(words / 200));
    var formatted = words.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '\u00a0');
    document.getElementById('wordCount').textContent = formatted + ' слов · ~' + minutes + ' мин чтения';
});
```

---

## 5. Tags Autocomplete

**Files: `article.html`, `article_create.html`, `main.py`**

Replace the basic tag input with an autocomplete dropdown:
- Fetch existing tags from `/api/tags?limit=200` on page load (or lazy on focus)
- As user types in tag input, show matching suggestions dropdown
- Click suggestion to add tag
- Enter still works to add custom tag
- Show tag usage count in suggestions: "Казахстан (1 234)"
- Style: dropdown below input, max 6 items visible, scrollable

API already exists: `GET /api/tags?limit=200`

---

## 6. Article Revision History (Audit Log)

**Files: `database.py`, `main.py`, `article.html`**

Add a revision/audit log for article changes.

Database:
```sql
CREATE TABLE IF NOT EXISTS article_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER REFERENCES articles(id),
    changed_at TEXT DEFAULT (datetime('now')),
    changed_by TEXT DEFAULT 'editor',
    changes_json TEXT,  -- JSON: {"field": {"old": "...", "new": "..."}}
    revision_type TEXT DEFAULT 'edit'  -- 'edit', 'create', 'status_change', 'auto_save'
);
CREATE INDEX IF NOT EXISTS idx_revisions_article ON article_revisions(article_id);
```

On every PATCH:
- Before saving, fetch current article state
- Compare old vs new values for changed fields
- Insert a revision record with the diff
- Skip recording body_html diffs for auto-saves (too large) — just note "body updated"

In article editor:
- Add collapsible "История изменений" section at bottom
- Shows last 20 revisions: timestamp, type, what changed
- Each revision expandable to show old/new values
- Fetch via new API: `GET /api/article/{id}/revisions`

---

## 7. Duplicate Article Action

**Files: `main.py`, `database.py`, `article.html`**

Add "Дублировать" button in the article editor (next to "Удалить"):
- Creates a copy of the article with title prefixed "Копия: "
- Status set to 'draft'
- New URL with random suffix
- Redirects to the new article editor

API: `POST /api/article/{id}/duplicate`
Returns: `{ok: true, id: new_id}`

---

## 8. Mobile UX Improvements

**Files: `style.css`, `articles.html`**

Swipe actions on article list items (mobile only):
- Swipe left: reveal "Архив" button (red)
- Swipe right: reveal "Редактировать" button (blue)
- Implementation: CSS transform + touch event listeners
- Only on mobile (check touch capability)

Actually, SKIP swipe — it's complex and brittle. Instead:

**Long-press context menu on mobile:**
- Long press (500ms) on article card shows action menu
- Options: Редактировать | Дублировать | Архивировать
- Menu appears as bottom sheet (reuse existing bottom sheet pattern)
- Desktop: right-click context menu on article cards

**Article list improvements:**
- Infinite scroll (load next page on scroll bottom) instead of pagination on mobile
- Add subtle loading skeleton while navigating

---

## IMPORTANT RULES
- Do NOT break any existing functionality
- Do NOT change the database schema for existing tables (only add new table article_revisions)
- Auto-migration for article_revisions in init_db()
- Version bump to v10.1 in base.html
- All numbers use nbsp as thousands separator
- All UI in Russian
- Dark theme must work for all new elements
