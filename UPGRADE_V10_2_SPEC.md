# total.kz v10.2 — Five Major Upgrades

## CRITICAL CONTEXT
- FastAPI + Jinja2 + SQLite app
- Docker: `docker-compose.yml` with services `app` and `cron`
- Templates: `app/templates/` (admin: base.html, article.html, article_create.html, articles.html; public: public/*)
- CSS: `app/static/css/style.css` (admin), `app/static/css/public.css` (public site)
- ALL UI in Russian
- Brand: #d83236, font: Onest (public), Inter (admin)
- Numbers: space as thousands separator
- Current DB: 16,844 articles in data/total.db

---

## 1. Editor.js — Replace Quill with Block Editor

### Files to modify:
- `app/templates/article.html` — replace Quill with Editor.js
- `app/templates/article_create.html` — same
- `app/static/css/style.css` — editor styling
- `app/database.py` — add `body_blocks` column (JSON) to articles
- `app/main.py` — update PATCH/POST to handle blocks
- `app/templates/public/article.html` — render blocks on public site

### CDN Resources:
```html
<script src="https://cdn.jsdelivr.net/npm/@editorjs/editorjs@2.30.8/dist/editorjs.umd.bundle.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@editorjs/header@2.8.8/dist/header.umd.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@editorjs/list@2.0.2/dist/list.umd.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@editorjs/quote@2.7.4/dist/quote.umd.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@editorjs/image@2.10.0/dist/image.umd.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@editorjs/embed@2.7.6/dist/embed.umd.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@editorjs/delimiter@1.4.2/dist/delimiter.umd.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@editorjs/code@2.9.3/dist/code.umd.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@editorjs/inline-code@1.5.1/dist/inline-code.umd.js"></script>
```

### Editor Config:
```javascript
const editor = new EditorJS({
    holder: 'editorjs',
    placeholder: 'Начните писать или нажмите Tab для блоков...',
    tools: {
        header: { class: Header, config: { levels: [2, 3, 4], defaultLevel: 2 } },
        list: { class: NestedList, inlineToolbar: true },
        quote: { class: Quote, config: { quotePlaceholder: 'Цитата', captionPlaceholder: 'Автор цитаты' } },
        image: { class: ImageTool, config: { 
            endpoints: { byUrl: '/api/upload/fetch-url' },
            // For now: URL-only mode (no file upload)
            uploader: { uploadByUrl(url) { return Promise.resolve({ success: 1, file: { url: url } }); } }
        }},
        embed: Embed,
        delimiter: Delimiter,
        code: CodeTool,
        inlineCode: InlineCode,
    },
    data: existingBlocksData || {},
    onChange: () => { markDirty(); }
});
```

### Data Flow:
- Save: `editor.save()` → JSON blocks → POST/PATCH to `/api/article` as `body_blocks`
- Also generate `body_html` from blocks server-side (for RSS, public site fallback)
- Also generate `body_text` from blocks server-side (for search indexing)
- Store blocks JSON in new column `body_blocks TEXT` on articles table

### Database migration (in init_db):
```python
if "body_blocks" not in article_cols:
    conn.execute("ALTER TABLE articles ADD COLUMN body_blocks TEXT")
```

### Server-side blocks→HTML converter (in database.py or new utils.py):
```python
def blocks_to_html(blocks_json: str) -> str:
    """Convert Editor.js JSON blocks to HTML string."""
    blocks = json.loads(blocks_json) if isinstance(blocks_json, str) else blocks_json
    html_parts = []
    for block in blocks.get("blocks", []):
        t = block["type"]
        d = block["data"]
        if t == "header":
            lvl = d.get("level", 2)
            html_parts.append(f'<h{lvl}>{d["text"]}</h{lvl}>')
        elif t == "paragraph":
            html_parts.append(f'<p>{d["text"]}</p>')
        elif t == "list":
            tag = "ol" if d.get("style") == "ordered" else "ul"
            items = "".join(f'<li>{i.get("content","") if isinstance(i,dict) else i}</li>' for i in d.get("items", []))
            html_parts.append(f'<{tag}>{items}</{tag}>')
        elif t == "quote":
            caption = f'<cite>{d["caption"]}</cite>' if d.get("caption") else ""
            html_parts.append(f'<blockquote><p>{d["text"]}</p>{caption}</blockquote>')
        elif t == "image":
            cap = f'<figcaption>{d["caption"]}</figcaption>' if d.get("caption") else ""
            html_parts.append(f'<figure><img src="{d["file"]["url"]}" alt="{d.get("caption","")}" loading="lazy">{cap}</figure>')
        elif t == "embed":
            html_parts.append(f'<div class="embed-container"><iframe src="{d["embed"]}" frameborder="0" allowfullscreen></iframe></div>')
        elif t == "delimiter":
            html_parts.append('<hr>')
        elif t == "code":
            html_parts.append(f'<pre><code>{d["code"]}</code></pre>')
    return "\n".join(html_parts)

def blocks_to_text(blocks_json: str) -> str:
    """Extract plain text from Editor.js blocks for search indexing."""
    import re
    blocks = json.loads(blocks_json) if isinstance(blocks_json, str) else blocks_json
    parts = []
    for block in blocks.get("blocks", []):
        d = block["data"]
        if "text" in d:
            parts.append(re.sub(r'<[^>]+>', '', d["text"]))
        if "items" in d:
            for item in d["items"]:
                text = item.get("content","") if isinstance(item, dict) else str(item)
                parts.append(re.sub(r'<[^>]+>', '', text))
        if "code" in d:
            parts.append(d["code"])
    return "\n".join(parts)
```

### Public site rendering (article.html):
- If article has `body_blocks` → render from blocks (better structure)
- Else fallback to `body_html` (backward compatible for old articles)
- Use Jinja2 macro or filter to render blocks

### Backward compatibility:
- Old articles keep body_html, no body_blocks
- New/edited articles get body_blocks + auto-generated body_html + body_text
- PATCH endpoint: if `body_blocks` present, auto-generate body_html and body_text from it

---

## 2. Meilisearch — Full-text Search

### Docker setup — add to docker-compose.yml:
```yaml
  meilisearch:
    image: getmeili/meilisearch:v1.12
    container_name: total_kz_meilisearch
    ports:
      - "127.0.0.1:7700:7700"
    environment:
      - MEILI_ENV=production
      - MEILI_MASTER_KEY=total-kz-search-key-2026
      - MEILI_DB_PATH=/meili_data
    volumes:
      - meili_data:/meili_data
    restart: unless-stopped

volumes:
  meili_data:
```

### New file: `app/search_engine.py`
```python
import httpx
import json

MEILI_URL = "http://meilisearch:7700"
MEILI_KEY = "total-kz-search-key-2026"
INDEX = "articles"

headers = {"Authorization": f"Bearer {MEILI_KEY}", "Content-Type": "application/json"}

def index_article(article: dict):
    """Index a single article into Meilisearch."""
    doc = {
        "id": article["id"],
        "title": article.get("title", ""),
        "excerpt": article.get("excerpt", ""),
        "body_text": (article.get("body_text") or "")[:5000],
        "author": article.get("author", ""),
        "sub_category": article.get("sub_category", ""),
        "pub_date": article.get("pub_date", ""),
        "tags": article.get("tags", []) if isinstance(article.get("tags"), list) else [],
        "status": article.get("status", "published"),
        "thumbnail": article.get("thumbnail") or article.get("main_image") or "",
    }
    httpx.post(f"{MEILI_URL}/indexes/{INDEX}/documents", json=[doc], headers=headers, timeout=5)

def search(query: str, filters: str = "", page: int = 1, per_page: int = 30) -> dict:
    """Search articles via Meilisearch."""
    payload = {
        "q": query,
        "limit": per_page,
        "offset": (page - 1) * per_page,
        "attributesToHighlight": ["title", "excerpt"],
        "highlightPreTag": "<mark>",
        "highlightPostTag": "</mark>",
    }
    if filters:
        payload["filter"] = filters
    try:
        r = httpx.post(f"{MEILI_URL}/indexes/{INDEX}/search", json=payload, headers=headers, timeout=5)
        data = r.json()
        return {
            "hits": data.get("hits", []),
            "total": data.get("estimatedTotalHits", 0),
            "query": query,
        }
    except Exception:
        return {"hits": [], "total": 0, "query": query}

def setup_index():
    """Create index with settings."""
    httpx.post(f"{MEILI_URL}/indexes", json={"uid": INDEX, "primaryKey": "id"}, headers=headers, timeout=5)
    settings = {
        "searchableAttributes": ["title", "excerpt", "body_text", "author", "tags"],
        "filterableAttributes": ["sub_category", "status", "author", "pub_date"],
        "sortableAttributes": ["pub_date"],
        "displayedAttributes": ["id", "title", "excerpt", "author", "sub_category", "pub_date", "tags", "thumbnail", "status"],
    }
    httpx.patch(f"{MEILI_URL}/indexes/{INDEX}/settings", json=settings, headers=headers, timeout=10)

def reindex_all():
    """Bulk reindex all articles from SQLite."""
    from . import database as db
    with db.get_db() as conn:
        rows = conn.execute("""
            SELECT id, title, excerpt, body_text, author, sub_category, pub_date, tags, status, thumbnail, main_image
            FROM articles
        """).fetchall()
        docs = []
        for r in rows:
            tags = []
            try: tags = json.loads(r["tags"] or "[]")
            except: pass
            docs.append({
                "id": r["id"], "title": r["title"] or "", "excerpt": r["excerpt"] or "",
                "body_text": (r["body_text"] or "")[:5000], "author": r["author"] or "",
                "sub_category": r["sub_category"] or "", "pub_date": r["pub_date"] or "",
                "tags": tags, "status": r["status"] or "published",
                "thumbnail": r["thumbnail"] or r["main_image"] or "",
            })
        # Batch in chunks of 1000
        for i in range(0, len(docs), 1000):
            httpx.post(f"{MEILI_URL}/indexes/{INDEX}/documents", json=docs[i:i+1000], headers=headers, timeout=30)
```

### Integration:
- New API: `GET /api/search/articles?q=...&category=...&page=1`
- Update public search page to use Meilisearch instead of LIKE queries
- On article save/create/delete → also index/deindex in Meilisearch
- New CLI command: `python -m app.search_engine reindex` for initial bulk indexing
- Public search: instant results with highlighting

### New file: `scraper/reindex_meilisearch.py`
Script to run initial full reindex of all articles.

---

## 3. imgproxy — Image Optimization

### Docker setup — add to docker-compose.yml:
```yaml
  imgproxy:
    image: darthsim/imgproxy:v3.27
    container_name: total_kz_imgproxy
    ports:
      - "127.0.0.1:8080:8080"
    environment:
      - IMGPROXY_BIND=:8080
      - IMGPROXY_MAX_SRC_RESOLUTION=50  # 50 megapixels max
      - IMGPROXY_ALLOWED_SOURCES=https://total.kz/,http://total.kz/
      - IMGPROXY_ENABLE_WEBP_DETECTION=true
      - IMGPROXY_ENFORCE_WEBP=true
      - IMGPROXY_QUALITY=80
      - IMGPROXY_USE_ETAG=true
      - IMGPROXY_CACHE_CONTROL_PASSTHROUGH=false
      - IMGPROXY_SET_CANONICAL_HEADER=true
    restart: unless-stopped
```

### Jinja2 helper (in public_routes.py):
```python
import hashlib, base64

IMGPROXY_URL = "http://imgproxy:8080"  # internal Docker network

def imgproxy_url(source_url: str, width: int = 800, height: int = 0, resize: str = "fit") -> str:
    """Generate imgproxy URL for an image.
    In production, this proxies through nginx: /img/ → imgproxy.
    """
    if not source_url or not source_url.startswith("http"):
        return source_url or ""
    # Plain URL encoding (no signature for simplicity)
    encoded = base64.urlsafe_b64encode(source_url.encode()).decode().rstrip("=")
    h = f"/{height}" if height else ""
    return f"/img/resize:{resize}:{width}:{height or 0}/plain/{source_url}@webp"
```

### Nginx config addition (for production):
```nginx
location /img/ {
    proxy_pass http://127.0.0.1:8080/;
    proxy_cache_valid 200 30d;
    proxy_set_header Host $host;
}
```

### Template updates:
- In `public/article.html`: hero image uses imgproxy for responsive sizes
  - `<img src="{{ imgproxy_url(article.main_image, 800) }}" srcset="{{ imgproxy_url(article.main_image, 400) }} 400w, {{ imgproxy_url(article.main_image, 800) }} 800w, {{ imgproxy_url(article.main_image, 1200) }} 1200w">`
- In `public/home.html`: thumbnails via imgproxy (300px width)
- In `public/category.html`: same

### For now (without nginx):
Use a FastAPI proxy endpoint `/img/{path:path}` that forwards to imgproxy internally.
This works in Docker (app→imgproxy on internal network).

---

## 4. Umami — Self-hosted Analytics

### Docker setup — add to docker-compose.yml:
```yaml
  umami:
    image: ghcr.io/umami-software/umami:postgresql-latest
    container_name: total_kz_umami
    ports:
      - "127.0.0.1:3000:3000"
    environment:
      - DATABASE_URL=postgresql://umami:umami@umami-db:5432/umami
      - APP_SECRET=total-kz-umami-secret-2026
    depends_on:
      - umami-db
    restart: unless-stopped

  umami-db:
    image: postgres:15-alpine
    container_name: total_kz_umami_db
    volumes:
      - umami_data:/var/lib/postgresql/data
    environment:
      - POSTGRES_DB=umami
      - POSTGRES_USER=umami
      - POSTGRES_PASSWORD=umami
    restart: unless-stopped
```

Add to volumes:
```yaml
  umami_data:
```

### Integration:
1. After first deploy, go to http://62.72.32.112:3000 → login with admin/umami → create website "total.kz"
2. Copy tracking script snippet
3. Add to `public/base.html` before </head>:
```html
<script defer src="http://62.72.32.112:3000/script.js" data-website-id="YOUR_WEBSITE_ID"></script>
```
4. Later: put behind nginx on subdomain analytics.total.kz

### Admin dashboard integration:
- In admin analytics page, add iframe or API call to Umami stats
- Umami has REST API: GET /api/websites/{id}/stats for live stats

---

## 5. Wagtail-inspired Patterns — StreamField, Preview, Workflow

### 5a. Publication Workflow (database.py, main.py, article.html)

Add `scheduled_at` column to articles:
```sql
ALTER TABLE articles ADD COLUMN scheduled_at TEXT;
```

Status flow: draft → review → scheduled → published → archived

New statuses:
- `review` — submitted for review (yellow badge: "На проверке")
- `scheduled` — will auto-publish at `scheduled_at` time (blue badge: "Запланировано DD.MM в HH:MM")

Article editor gets:
- "Запланировать" button that shows date/time picker
- When selected, sets status='scheduled' and scheduled_at=datetime
- Cron job checks every minute for scheduled articles and publishes them

### 5b. StreamField-style Block Content

Already handled by Editor.js (point 1). The blocks JSON IS the StreamField equivalent.

Additional block types to add:
- **infographic** — title + items list (key-value pairs), rendered as styled card
- **related_articles** — array of article IDs, rendered as card row
- **raw_html** — for embeds, ads, custom HTML

### 5c. Live Preview (already partially done in v10.1)

Enhance the preview modal to be more faithful:
- Load actual public CSS (public.css) in the preview modal
- Render blocks using the same logic as public site
- Show mobile/desktop toggle in preview bar

### 5d. Image Focal Point

Add `focal_x` and `focal_y` float columns to articles (0.0-1.0):
```sql
ALTER TABLE articles ADD COLUMN focal_x REAL DEFAULT 0.5;
ALTER TABLE articles ADD COLUMN focal_y REAL DEFAULT 0.5;
```

In article editor, add focal point picker on main image:
- Click on image to set focus point
- Small crosshair shows where the focal point is
- Used by imgproxy for smart cropping: `gravity:fp:{x}:{y}`

---

## Implementation Order

1. Database migrations (body_blocks, scheduled_at, focal_x/y) — database.py
2. Editor.js integration — article.html, article_create.html
3. blocks_to_html converter — new utils.py or in database.py
4. Public site block rendering — public/article.html
5. Meilisearch search_engine.py + docker-compose
6. imgproxy docker-compose + proxy endpoint + template helpers
7. Umami docker-compose snippet (just the config, user deploys manually)
8. Publication workflow (review, scheduled statuses)
9. Image focal point picker

## Version: v10.2 in base.html
