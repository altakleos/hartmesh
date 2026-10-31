"""Draw a bot's picture: a pure function of its name, key and kind."""

from __future__ import annotations

import io
import unicodedata
from pathlib import Path
from typing import Literal

from PIL import Image, ImageDraw, ImageFont

AVATAR_PIXELS = 640
AVATAR_COLORS = ("#1D4ED8", "#0F766E", "#15803D", "#A16207", "#C2410C", "#B91C1C", "#A21CAF", "#6D28D9")
CHAT_COLOR = "#334155"
_CENTRE = AVATAR_PIXELS // 2
_FONTS = tuple(Path(__file__).parent / "fonts" / name for name in ("DejaVuSans-Bold.ttf", "DroidSansFallbackFull.ttf"))


def initials_of(name: str) -> str:
    """The first character of the first two runs of letters or digits, in upper case."""
    words, current = [], ""
    for character in name:
        if unicodedata.category(character)[0] in "LN":
            current += character
        elif current:
            words.append(current)
            current = ""
    if current:
        words.append(current)
    return "".join(word[0].upper()[0] for word in words[:2])


def color_of(key: str) -> str:
    """Entry ``(hash >> 16) % 8`` of the palette, by 32-bit FNV-1a of the key's UTF-8."""
    value = 0x811C9DC5
    for byte in key.encode("utf-8"):
        value = ((value ^ byte) * 0x01000193) & 0xFFFFFFFF
    return AVATAR_COLORS[(value >> 16) % len(AVATAR_COLORS)]


def _fonts(size: int) -> tuple:
    return tuple(ImageFont.truetype(path, size, layout_engine=ImageFont.Layout.BASIC) for path in _FONTS)


def _mask(font, character: str) -> tuple:
    mask = font.getmask(character)
    return mask.size, bytes(mask)


def _has(font, character: str) -> bool:
    """Differs from the missing-glyph mask, and has ink: U+3164 is a blank letter."""
    mask = _mask(font, character)
    return mask != _mask(font, "\uffff") and any(mask[1])


def _font_for(character: str, fonts: tuple):
    """The first font that has the character."""
    return next((font for font in fonts if _has(font, character)), None)


def _drawable(initials: str, fonts: tuple) -> bool:
    return bool(initials) and all(unicodedata.bidirectional(character) in {"L", "EN"} and _font_for(character, fonts) is not None for character in initials)


def _draw_initials(draw, initials: str, fonts: tuple, centre: tuple[int, int]) -> None:
    glyphs = [(character, _font_for(character, fonts)) for character in initials]
    boxes = [font.getbbox(character) for character, font in glyphs]
    widths = [font.getlength(character) for character, font in glyphs]
    top, bottom = min(box[1] for box in boxes), max(box[3] for box in boxes)
    x, y = centre[0] - sum(widths) / 2, centre[1] - (top + bottom) / 2
    for (character, font), width in zip(glyphs, widths):
        draw.text((x, y), character, font=font, fill="white")
        x += width


def _draw_chat_mark(draw, background: str) -> None:
    draw.rounded_rectangle((160, 190, 480, 410), radius=60, fill="white")
    draw.polygon(((220, 410), (300, 410), (220, 480)), fill="white")
    for x in (240, 320, 400):
        draw.ellipse((x - 22, 278, x + 22, 322), fill=background)


def _draw_pill(draw, background: str, centre_y: int) -> None:
    draw.rounded_rectangle((220, centre_y - 42, 420, centre_y + 42), radius=42, fill="white")
    font = _fonts(60)[0]
    box = font.getbbox("AI")
    draw.text((_CENTRE - (box[0] + box[2]) / 2, centre_y - (box[1] + box[3]) / 2), "AI", font=font, fill=background)


def render_avatar(initials: str, color: str, kind: Literal["instance", "custom_agent", "lead_agent"]) -> bytes:
    """A square JPEG. Telegram crops it to a circle, so everything stays inside that."""
    image = Image.new("RGB", (AVATAR_PIXELS, AVATAR_PIXELS), color)
    draw = ImageDraw.Draw(image)
    fonts = _fonts(300 if len(initials) == 1 else 240)
    drawable = kind != "lead_agent" and _drawable(initials, fonts)
    if kind == "instance":
        if drawable:
            _draw_initials(draw, initials, fonts, (_CENTRE, 290))
        _draw_pill(draw, color, 520 if drawable else _CENTRE)
    elif drawable:
        _draw_initials(draw, initials, fonts, (_CENTRE, _CENTRE))
    else:
        _draw_chat_mark(draw, color)
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()
