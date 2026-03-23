# Phase 3: Custom Editor.js Blocks for CMS

## Goal
Add 3 custom Editor.js block tools to the admin article editor so journalists/editors can insert:
1. **Infobox** — icon + title + text (context boxes, explainers)
2. **Callout** — colored callout with type selector (info/warning/success)
3. **Number Box** — grid of statistics (value + label + optional delta)

## Current Architecture

### Editor Setup
- File: `app/templates/article.html` (admin template, not public)
- Editor.js v2.30.8 initialized at line ~340
- Current tools: header, list, quote, image, embed, delimiter, code, inlineCode, marker, underline, table, warning
- CDN scripts loaded at lines 294-306
- Editor data saved as JSON in `body_blocks`, converted to HTML via `blocks_to_html()` in `app/database.py` line 315

### blocks_to_html() (database.py line 315-359)
Converts Editor.js block JSON → HTML string. Currently handles: header, paragraph, list, quote, image, embed, delimiter, code, table, warning.

### htmlToBlocks() (article.html line 313-330)
Client-side function that converts scraped HTML → Editor.js blocks on first edit. Currently handles: h2-h4, ul/ol, blockquote, figure, img, hr, pre, iframe, paragraph.

### Preview function (article.html line 532-553)
`openPreview()` renders blocks to HTML for in-editor preview. Must also handle new block types.

### CSS classes already exist (public.css v9.2):
- `.article-infobox`, `.article-infobox-header`, `.article-infobox-icon`, `.article-infobox-title` + `p` children
- `.article-callout`, `.article-callout--info`, `.article-callout--warning`, `.article-callout--success`, `.article-callout-title`
- `.article-number-box`, `.article-number-box-grid`, `.article-number-stat`, `.article-number-value`, `.article-number-label`, `.article-number-delta`, `.article-number-delta.up`, `.article-number-delta.down`

## What To Implement

### 1. Custom Editor.js Tool Classes (inline JS in article.html)

Create 3 tool classes as plain JS (no build step, no modules), placed AFTER the CDN script tags but BEFORE the EditorJS initialization.

#### InfoboxTool
- Block type: `infobox`
- Data: `{ title: string, text: string }`
- Toolbox: title "Инфобокс", icon = info circle SVG
- Render: div with title input + text textarea
- Admin styling: simple bordered box in the editor

#### CalloutTool  
- Block type: `callout`
- Data: `{ type: "info"|"warning"|"success", title: string, text: string }`
- Toolbox: title "Выноска", icon = alert SVG
- Render: div with type dropdown + title input + text textarea
- The type dropdown should show: Информация, Предупреждение, Успех
- Admin styling: colored left border matching type

#### NumberBoxTool
- Block type: `numberbox`
- Data: `{ items: [{ value: string, label: string, delta: string }] }`
- Toolbox: title "Цифры", icon = hash/stats SVG
- Render: grid of inputs for value/label/delta, with + button to add more items
- Default: 2 items
- Admin styling: grid with accent-colored value preview

### 2. Register tools in EditorJS config (~line 352)

Add to the `tools: { ... }` object:
```js
infobox: { class: InfoboxTool },
callout: { class: CalloutTool },
numberbox: { class: NumberBoxTool },
```

### 3. Backend: blocks_to_html() in database.py

Add 3 new elif cases after the `warning` case (line 355-358):

```python
elif t == "infobox":
    title = d.get("title", "")
    text_html = "".join(f"<p>{p}</p>" for p in d.get("text", "").split("\n") if p.strip())
    html_parts.append(
        f'<div class="article-infobox">'
        f'<div class="article-infobox-header">'
        f'<div class="article-infobox-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg></div>'
        f'<div class="article-infobox-title">{title}</div>'
        f'</div>{text_html}</div>'
    )
elif t == "callout":
    ctype = d.get("type", "info")
    title = d.get("title", "")
    text = d.get("text", "")
    html_parts.append(
        f'<div class="article-callout article-callout--{ctype}">'
        f'<div class="article-callout-title">{title}</div>'
        f'<p>{text}</p></div>'
    )
elif t == "numberbox":
    items = d.get("items", [])
    stats = ""
    for it in items:
        val = it.get("value", "")
        label = it.get("label", "")
        delta = it.get("delta", "")
        delta_class = "up" if delta.startswith("+") or delta.startswith("↑") else ("down" if delta.startswith("-") or delta.startswith("↓") else "")
        delta_html = f'<span class="article-number-delta {delta_class}">{delta}</span>' if delta else ""
        stats += f'<div class="article-number-stat"><span class="article-number-value">{val}</span><span class="article-number-label">{label}</span>{delta_html}</div>'
    html_parts.append(f'<div class="article-number-box"><div class="article-number-box-grid">{stats}</div></div>')
```

### 4. Preview function in article.html (~line 538-548)

Add these cases in the `blocks.blocks.forEach` callback, alongside existing types:

```js
else if (b.type==='infobox') {
    bodyHtml+='<div class="article-infobox"><div class="article-infobox-header"><div class="article-infobox-icon"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg></div><div class="article-infobox-title">'+(d.title||'')+'</div></div><p>'+(d.text||'').replace(/\n/g,'</p><p>')+'</p></div>';
}
else if (b.type==='callout') {
    bodyHtml+='<div class="article-callout article-callout--'+(d.type||'info')+'"><div class="article-callout-title">'+(d.title||'')+'</div><p>'+(d.text||'')+'</p></div>';
}
else if (b.type==='numberbox') {
    var stats=(d.items||[]).map(function(it){var dc=it.delta&&(it.delta.indexOf('+')===0||it.delta.indexOf('↑')===0)?'up':(it.delta&&(it.delta.indexOf('-')===0||it.delta.indexOf('↓')===0)?'down':'');return'<div class="article-number-stat"><span class="article-number-value">'+(it.value||'')+'</span><span class="article-number-label">'+(it.label||'')+'</span>'+(it.delta?'<span class="article-number-delta '+dc+'">'+ it.delta+'</span>':'')+'</div>';}).join('');
    bodyHtml+='<div class="article-number-box"><div class="article-number-box-grid">'+stats+'</div></div>';
}
```

### 5. htmlToBlocks() — handle new block types from scraped HTML (line 313-330)

Add cases for when these divs exist in imported HTML (unlikely but good to handle):
```js
// In the div handling:
else if (tag === 'div' && node.classList.contains('article-infobox')) {
    var infoTitle = node.querySelector('.article-infobox-title');
    var infoText = Array.from(node.querySelectorAll('p')).map(p => p.textContent).join('\n');
    blocks.push({ type: 'infobox', data: { title: infoTitle ? infoTitle.textContent : '', text: infoText } });
}
else if (tag === 'div' && node.classList.contains('article-callout')) {
    var cType = 'info';
    if (node.classList.contains('article-callout--warning')) cType = 'warning';
    if (node.classList.contains('article-callout--success')) cType = 'success';
    var cTitle = node.querySelector('.article-callout-title');
    var cText = node.querySelector('p');
    blocks.push({ type: 'callout', data: { type: cType, title: cTitle ? cTitle.textContent : '', text: cText ? cText.textContent : '' } });
}
else if (tag === 'div' && node.classList.contains('article-number-box')) {
    var nItems = [];
    node.querySelectorAll('.article-number-stat').forEach(function(st) {
        nItems.push({ value: st.querySelector('.article-number-value')?.textContent || '', label: st.querySelector('.article-number-label')?.textContent || '', delta: st.querySelector('.article-number-delta')?.textContent || '' });
    });
    blocks.push({ type: 'numberbox', data: { items: nItems } });
}
```

## Important Notes
- All UI text MUST be in Russian
- The Editor.js tool classes must be plain ES5-compatible JS (no class syntax, no arrow functions, no modules) — use `function` and `prototype`
- Admin template uses a DIFFERENT base.html than public (app/templates/base.html vs app/templates/public/base.html)  
- Admin CSS is inline in the templates, NOT in public.css
- The custom tool classes should include their own render styles (inline CSS or style tags) for the admin editor view
- Numbers MUST use space as thousands separator (e.g. "33 258" not "33258")
- Do NOT change any files outside of: `app/templates/article.html`, `app/templates/article_create.html`, `app/database.py`
- `article_create.html` has its own copy of the Editor.js setup — update it too with the same tools
- Test that the existing tools still work — don't break the header, list, quote, etc.
