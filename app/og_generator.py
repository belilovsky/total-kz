"""OG Card Generator — branded 1200x630 social media images for Total.kz articles."""

import hashlib
import logging
import textwrap
from io import BytesIO
from pathlib import Path

import httpx
from PIL import Image, ImageDraw, ImageFilter, ImageFont

logger = logging.getLogger(__name__)

OG_WIDTH = 1200
OG_HEIGHT = 630
OG_CACHE_DIR = Path(__file__).parent.parent / "data" / "og_cards"

# Fonts – DejaVu Sans supports Cyrillic and is available on most Linux systems
_FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
_FONT_REGULAR = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"

# Logo path
_LOGO_PATH = Path(__file__).parent / "static" / "img" / "logo.png"

# Category label map (matches the site)
_CAT_LABELS = {
    "politika": "Политика",
    "ekonomika_sobitiya": "Экономика",
    "sport": "Спорт",
    "obshchestvo": "Общество",
    "mir": "Мир",
    "kultura": "Культура",
    "nauka_i_tekhnologii": "Технологии",
    "proisshestviya": "Происшествия",
    "zdorove": "Здоровье",
    "avtomobili": "Авто",
    "nedvizhimost": "Недвижимость",
    "finansy": "Финансы",
    "obrazovanie": "Образование",
    "turizm": "Туризм",
}


def _get_cat_label(category: str) -> str:
    return _CAT_LABELS.get(category, category.replace("_", " ").title() if category else "")


def _load_font(path: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(path, size)
    except (OSError, IOError):
        return ImageFont.load_default()


def _fetch_image(url: str, timeout: float = 10.0) -> Image.Image | None:
    """Download an image from URL, return as PIL Image or None on failure."""
    try:
        resp = httpx.get(url, timeout=timeout, follow_redirects=True)
        resp.raise_for_status()
        return Image.open(BytesIO(resp.content)).convert("RGB")
    except Exception as e:
        logger.warning("OG: failed to fetch image %s: %s", url, e)
        return None


def _cache_key(article_id: int, title: str, image_url: str, category: str) -> str:
    """Generate a cache key based on article content so the card regenerates when content changes."""
    raw = f"{article_id}:{title}:{image_url}:{category}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def generate_og_card(
    article_id: int,
    title: str,
    image_url: str | None = None,
    category: str | None = None,
    force: bool = False,
) -> Path | None:
    """Generate a 1200x630 OG card image.

    Returns path to the generated PNG, or None on failure.
    Uses cache in data/og_cards/ unless force=True.
    """
    OG_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    cache_hash = _cache_key(article_id, title or "", image_url or "", category or "")
    filename = f"og_{article_id}_{cache_hash}.png"
    filepath = OG_CACHE_DIR / filename

    if not force and filepath.exists():
        return filepath

    # Clean old versions for this article
    for old in OG_CACHE_DIR.glob(f"og_{article_id}_*.png"):
        if old.name != filename:
            old.unlink(missing_ok=True)

    try:
        card = _render_card(title, image_url, category)
        card.save(str(filepath), "PNG", optimize=True)
        logger.info("OG card generated: %s", filepath.name)
        return filepath
    except Exception:
        logger.exception("OG card generation failed for article %d", article_id)
        return None


def _render_card(title: str, image_url: str | None, category: str | None) -> Image.Image:
    """Render the actual OG card image."""
    # Background: article photo or solid dark gray
    bg = None
    if image_url:
        bg = _fetch_image(image_url)

    if bg:
        # Resize to cover 1200x630
        bg = _cover_crop(bg, OG_WIDTH, OG_HEIGHT)
        # Darken the background
        darkened = Image.new("RGB", (OG_WIDTH, OG_HEIGHT), (0, 0, 0))
        bg = Image.blend(bg, darkened, alpha=0.45)
        # Slight blur for text readability
        bg = bg.filter(ImageFilter.GaussianBlur(radius=2))
    else:
        # Fallback: dark gradient-like solid
        bg = Image.new("RGB", (OG_WIDTH, OG_HEIGHT), (24, 24, 32))

    draw = ImageDraw.Draw(bg)

    # Bottom gradient overlay for text area
    gradient = Image.new("RGBA", (OG_WIDTH, OG_HEIGHT), (0, 0, 0, 0))
    grad_draw = ImageDraw.Draw(gradient)
    for y in range(OG_HEIGHT // 3, OG_HEIGHT):
        alpha = int(200 * (y - OG_HEIGHT // 3) / (OG_HEIGHT - OG_HEIGHT // 3))
        grad_draw.rectangle([(0, y), (OG_WIDTH, y)], fill=(0, 0, 0, alpha))
    bg.paste(Image.alpha_composite(Image.new("RGBA", bg.size, (0, 0, 0, 0)), gradient).convert("RGB"),
             mask=gradient.split()[3])

    # Logo top-left
    try:
        if _LOGO_PATH.exists():
            logo = Image.open(_LOGO_PATH).convert("RGBA")
            logo_h = 48
            logo_w = int(logo.width * logo_h / logo.height)
            logo = logo.resize((logo_w, logo_h), Image.LANCZOS)
            bg.paste(logo, (40, 32), logo)
    except Exception:
        # Fallback: draw text logo
        logo_font = _load_font(_FONT_BOLD, 32)
        draw.text((40, 32), "Total.kz", fill=(216, 50, 54), font=logo_font)

    # Category badge top-right
    if category:
        cat_text = _get_cat_label(category)
        if cat_text:
            badge_font = _load_font(_FONT_BOLD, 22)
            bbox = draw.textbbox((0, 0), cat_text, font=badge_font)
            tw = bbox[2] - bbox[0]
            th = bbox[3] - bbox[1]
            pad_x, pad_y = 16, 8
            bx = OG_WIDTH - 40 - tw - pad_x * 2
            by = 32
            # Draw badge background
            draw.rounded_rectangle(
                [(bx, by), (bx + tw + pad_x * 2, by + th + pad_y * 2)],
                radius=6,
                fill=(216, 50, 54),
            )
            draw.text((bx + pad_x, by + pad_y), cat_text, fill="white", font=badge_font)

    # Title text — centered in bottom half, white, bold, max 3 lines
    title_font = _load_font(_FONT_BOLD, 48)
    max_chars_per_line = 38
    wrapped = textwrap.wrap(title or "Без заголовка", width=max_chars_per_line)[:3]
    if len(textwrap.wrap(title or "", width=max_chars_per_line)) > 3:
        wrapped[2] = wrapped[2][:max_chars_per_line - 3] + "..."

    line_height = 62
    total_text_h = len(wrapped) * line_height
    text_y = OG_HEIGHT - 60 - total_text_h  # 60px from bottom

    for i, line in enumerate(wrapped):
        bbox = draw.textbbox((0, 0), line, font=title_font)
        tw = bbox[2] - bbox[0]
        tx = (OG_WIDTH - tw) // 2
        ty = text_y + i * line_height
        # Text shadow for readability
        draw.text((tx + 2, ty + 2), line, fill=(0, 0, 0, 180), font=title_font)
        draw.text((tx, ty), line, fill="white", font=title_font)

    return bg


def _cover_crop(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """Resize and crop image to exactly fill target dimensions (cover mode)."""
    src_ratio = img.width / img.height
    tgt_ratio = target_w / target_h

    if src_ratio > tgt_ratio:
        # Image is wider — scale to target height, crop width
        new_h = target_h
        new_w = int(img.width * target_h / img.height)
    else:
        # Image is taller — scale to target width, crop height
        new_w = target_w
        new_h = int(img.height * target_w / img.width)

    img = img.resize((new_w, new_h), Image.LANCZOS)
    left = (new_w - target_w) // 2
    top = (new_h - target_h) // 2
    return img.crop((left, top, left + target_w, top + target_h))
