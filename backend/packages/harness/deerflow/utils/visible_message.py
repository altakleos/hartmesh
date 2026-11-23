"""Visible message content shared by transcripts and durable reply reconciliation."""

import re
from typing import Any

#: Control messages the page never draws, by the name their middleware gives them.
HIDDEN_CONTROL_MESSAGE_NAMES = frozenset({"summary", "loop_warning", "todo_reminder", "todo_completion_reminder"})

#: Tags the backend wraps around injected context; stripped from any message that slipped past the hidden filter.
INTERNAL_MARKER_TAGS = ("current_uploads", "uploaded_files", "slash_skill_activation", "system-reminder", "memory", "current_date")

_INTERNAL_MARKER_RE = re.compile(r"<(" + "|".join(re.escape(tag) for tag in INTERNAL_MARKER_TAGS) + r")>[\s\S]*?</\1>")
_CONTEXT_TAG_RE = re.compile(r"<(current_uploads|uploaded_files|slash_skill_activation)>[\s\S]*?</\1>")
_THINK_OPEN_TAG = "<think>"
_THINK_TAG_RE = re.compile(r"<think>\s*([\s\S]*?)\s*</think>")

#: What the page's ``String.prototype.trim`` removes. ``str.strip()`` differs:
#: it keeps a byte-order mark and removes the ASCII separators ``\x1c``-``\x1f``.
_JS_WHITESPACE = "\t\n\v\f\r \u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000\ufeff"


def _trim(text: str) -> str:
    return text.strip(_JS_WHITESPACE)


def _without_inline_reasoning(content: str) -> str:
    """An assistant's words without the ``<think>`` reasoning some models write inline.

    A tag right after a backtick is the model writing about the tag inside
    inline code, and stays. An opener with no closer is reasoning cut off, and
    takes the rest.
    """

    def _drop(match: re.Match[str]) -> str:
        return match.group(0) if match.start() > 0 and content[match.start() - 1] == "`" else ""

    cleaned = _THINK_TAG_RE.sub(_drop, content)
    opener = cleaned.find(_THINK_OPEN_TAG)
    if opener != -1 and (opener == 0 or cleaned[opener - 1] != "`"):
        cleaned = cleaned[:opener]
    return _trim(cleaned)


def _string_content(message: dict[str, Any], content: str) -> str:
    return _without_inline_reasoning(content) if message.get("type") == "ai" else _trim(content)


def _text_only(message: dict[str, Any]) -> str:
    """The message's text parts, images left out: what the hidden-message rule reads."""
    content = message.get("content")
    if isinstance(content, str):
        return _string_content(message, content)
    if isinstance(content, list):
        return _trim("\n".join(part if isinstance(part, str) else (part.get("text") or "") if isinstance(part, dict) and part.get("type") == "text" else "" for part in content))
    return ""


def _part_text(part: Any) -> str:
    if isinstance(part, str):
        return part
    if not isinstance(part, dict):
        return ""
    if part.get("type") == "text":
        return part.get("text") or ""
    if part.get("type") == "image_url":
        image = part.get("image_url")
        return f"![image]({image if isinstance(image, str) else (image or {}).get('url')})"
    return ""


def drawn_content(message: dict[str, Any]) -> str:
    """The text the page draws for this message: its words and images, inline reasoning removed."""
    content = message.get("content")
    if isinstance(content, str):
        return _string_content(message, content)
    if isinstance(content, list):
        return _trim("\n".join(_part_text(part) for part in content))
    return ""


def _is_hidden(message: dict[str, Any]) -> bool:
    if (message.get("additional_kwargs") or {}).get("hide_from_ui") is True:
        return True
    name = message.get("name")
    if isinstance(name, str) and name in HIDDEN_CONTROL_MESSAGE_NAMES:
        return True
    if message.get("type") != "human":
        return False
    # A skill activation with nothing of the person's own beside it.
    text = _text_only(message)
    return "<slash_skill_activation>" in text and not _trim(_CONTEXT_TAG_RE.sub("", text))


def is_visible(message: dict[str, Any]) -> bool:
    """Whether the page draws this message at all: not hidden from it, and not a tool result."""
    return message.get("type") != "tool" and not _is_hidden(message)


def visible_text(message: dict[str, Any]) -> str:
    """The words the transcript carries for this message, with every internal marker removed."""
    return _trim(_INTERNAL_MARKER_RE.sub("", drawn_content(message)))
