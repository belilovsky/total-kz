# Total.kz API Reference

Base URL: `https://total.qdev.run`

---

## Public Endpoints

### Feeds

| Method | Endpoint | Content-Type | Description |
|--------|----------|-------------|-------------|
| GET | `/rss` | `application/rss+xml` | RSS 2.0 — 50 articles, full `content:encoded`, WebSub hub |
| GET | `/rss/{category}` | `application/rss+xml` | RSS 2.0 per category |
| GET | `/feed.json` | `application/json` | JSON Feed 1.1 |
| GET | `/turbo/rss.xml` | `application/rss+xml` | Yandex Turbo Pages — full `turbo:content` |
| GET | `/zen/rss.xml` | `application/rss+xml` | Yandex Zen — 50 articles with `content:encoded` |
| GET | `/fb-ia/rss.xml` | `application/rss+xml` | Facebook Instant Articles — IA markup |
| GET | `/flipboard/rss.xml` | `application/rss+xml` | Flipboard — `media:content` (1200×675) |

### SEO & Discovery

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/sitemap.xml` | Sitemap index |
| GET | `/sitemap-main.xml` | Main pages sitemap |
| GET | `/sitemap-articles-{page}.xml` | Articles sitemap (paginated) |
| GET | `/sitemap-persons.xml` | Persons sitemap |
| GET | `/sitemap-news.xml` | Google News sitemap |
| GET | `/kz/sitemap.xml` | Kazakh version sitemap |
| GET | `/robots.txt` | Robots directives |
| GET | `/llms.txt` | LLM crawler guidance |
| GET | `/llms-full.txt` | Full LLM crawler data |
| GET | `/.well-known/nlweb.json` | NLWeb protocol |
| GET | `/manifest.json` | PWA manifest |

### Content API

#### GET `/api/feed`
Paginated article feed (returns HTML fragment).

**Query params:**
- `offset` (int, default: 30) — skip articles
- `limit` (int, default: 20, max: 50) — articles per page

---

#### GET `/api/recommendations`
Personalized article recommendations (returns HTML fragment).

---

#### GET `/api/suggest`
Autocomplete suggestions for search.

**Query params:**
- `q` (string) — search query

**Response:** JSON array of suggestions.

---

#### POST `/api/view/{article_id}`
Increment article view counter.

**Response:** `{"ok": true}`

---

#### GET `/api/public/comments/{article_id}`
Get comments for an article.

**Response:**
```json
[
  {
    "id": 1,
    "author_name": "Иван",
    "text": "Отличная статья!",
    "created_at": "2026-03-30T12:00:00",
    "status": "approved"
  }
]
```

---

#### POST `/api/public/comments/{article_id}`
Add a comment to an article.

**Body (form-data):**
- `author_name` (string, required)
- `text` (string, required)
- `author_email` (string, optional)

**Response:** `{"ok": true, "message": "Комментарий отправлен на модерацию"}`

---

#### GET `/api/public/reactions/{article_id}`
Get reactions for an article.

**Response:**
```json
{
  "like": 5,
  "useful": 3,
  "funny": 1,
  "sad": 0,
  "angry": 0
}
```

---

#### POST `/api/public/reactions/{article_id}`
Add a reaction.

**Body (JSON):**
```json
{"reaction": "like"}
```

Valid reactions: `like`, `useful`, `funny`, `sad`, `angry`

---

#### POST `/api/push/subscribe`
Subscribe to Web Push notifications.

**Body (JSON):**
```json
{
  "endpoint": "https://fcm.googleapis.com/...",
  "keys": {
    "p256dh": "...",
    "auth": "..."
  }
}
```

---

#### DELETE `/api/push/unsubscribe`
Unsubscribe from Web Push.

---

#### GET `/api/display-mode`
Get current display mode.

**Response:**
```json
{"mode": "normal"}
```

Possible values: `normal`, `mourning`, `holiday`, `evening`

---

## Admin Endpoints

> All admin endpoints require authentication (session cookie from `/admin/login`).

### Display Mode

#### POST `/api/admin/display-mode`
Change site display mode.

**Body (JSON):**
```json
{"mode": "mourning"}
```

---

### Auto-Posting

#### GET `/api/admin/auto-post`
Get auto-posting status.

**Response:**
```json
{
  "enabled": true,
  "last_run": "2026-03-30T23:00:00",
  "posted_count": 42
}
```

---

#### POST `/api/admin/auto-post`
Toggle or trigger auto-posting.

**Body (JSON):**
```json
{"action": "toggle"}
```

Actions: `toggle`, `trigger`

---

### Articles

#### POST `/api/article/{article_id}/workflow/{action}`
Change article workflow state.

Actions: `submit` (draft→review), `approve` (review→published), `reject` (review→draft), `unpublish` (published→draft)

---

#### POST `/api/article/{article_id}/assign`
Assign editor to article.

**Body (JSON):**
```json
{"user_id": 1}
```

---

#### POST `/api/articles/bulk`
Bulk article operations.

**Body (JSON):**
```json
{
  "ids": [1, 2, 3],
  "action": "publish"
}
```

Actions: `publish`, `unpublish`, `delete`

---

### Media

#### POST `/api/media/upload`
Upload media file.

**Body:** multipart/form-data with `file` field.

**Response:**
```json
{
  "id": 1,
  "url": "/media/ab/abcdef1234.jpg",
  "filename": "photo.jpg",
  "size": 123456
}
```

---

### CRUD Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/user` | Create user |
| DELETE | `/api/user/{id}` | Delete user |
| POST | `/api/category` | Create category |
| DELETE | `/api/category/{id}` | Delete category |
| POST | `/api/author` | Create author |
| DELETE | `/api/author/{id}` | Delete author |
| POST | `/api/tag` | Create tag |
| DELETE | `/api/tag/{name}` | Delete tag |
| POST | `/api/tags/merge` | Merge duplicate tags |
| POST | `/api/entity` | Create entity |
| DELETE | `/api/entity/{id}` | Delete entity |
| POST | `/api/entities/merge` | Merge duplicate entities |
| POST | `/api/story` | Create story |
| GET | `/api/story/{id}` | Get story details |
| DELETE | `/api/story/{id}` | Delete story |
| POST | `/api/story/{id}/articles` | Add article to story |
| DELETE | `/api/story/{id}/articles/{article_id}` | Remove article from story |
| POST | `/api/comments/{id}/moderate` | Moderate comment |
| POST | `/api/ads/{slot}/toggle` | Toggle ad slot |
| POST | `/api/ads/{slot}/update` | Update ad slot config |

---

## WebSub

All RSS feeds include WebSub hub link:
```xml
<link rel="hub" href="https://pubsubhubbub.appspot.com/" />
```

The server automatically pings the hub when new articles are published.

### Manual ping

```bash
curl -X POST https://pubsubhubbub.appspot.com/ \
  -d "hub.mode=publish" \
  -d "hub.url=https://total.qdev.run/rss"
```

---

## Rate Limits

- View counter: 1 per article per IP per 5 minutes
- Comments: 1 per article per IP per minute
- Reactions: 1 per article per type per IP (permanent)
- Auto-post Telegram: 1 message per 3 seconds
- Feed endpoints: no rate limit (cached by Nginx)

---

## Error Responses

All API errors return JSON:
```json
{
  "detail": "Error description"
}
```

| Status | Description |
|--------|-------------|
| 400 | Bad request / validation error |
| 401 | Unauthorized (admin endpoints) |
| 403 | Forbidden |
| 404 | Not found |
| 429 | Rate limited |
| 500 | Internal server error |
| 503 | Service unavailable (database) |
