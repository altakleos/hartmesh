"""A conversation's transcript as its person reads it: what the page shows, nothing internal.

Every transcript a person downloads is written here
(``GET /api/threads/{thread_id}/export``), from the messages the page draws:
the conversation's message feed (``app.gateway.thread_feed``), which keeps
every turn however far summarization cut the checkpoint down, woven with the
checkpoint for what the feed does not hold yet. A transcript carries the
user's and the assistant's words only: no reasoning, tool calls, tool
results, hidden control messages, or the markers the backend wraps around
injected context (``<current_uploads>``, ``<system-reminder>``, ``<memory>``
and the rest), which would put server paths and memory into a file the
person hands on.

The page decides what it shows in ``frontend-hm/src/core/messages/utils.ts``.
Both sides are held to ``contracts/visible_transcript_contract.json``, so a
download says what the page showed; change the two together.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from app.gateway.deps import get_run_event_store, get_run_manager
from app.gateway.services import build_checkpoint_state_accessor
from app.gateway.thread_feed import feed_messages
from deerflow.runtime import serialize_channel_values_for_api
from deerflow.utils.messages import strip_injected_user_message_id_suffix
from deerflow.utils.time import coerce_iso

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


#: What a transcript is written as, by the ``format`` a caller names.
FORMATS = {"markdown": ("md", "text/markdown; charset=utf-8"), "json": ("json", "application/json; charset=utf-8")}


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


def _when(value: str | datetime | None) -> str:
    if not value:
        return "Unknown"
    try:
        moment = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except ValueError:
        return str(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def transcript_markdown(*, title: str, created_at: str | None, messages: list[dict[str, Any]], exported_at: datetime) -> str:
    """The conversation as Markdown: a heading per turn, the person's and the assistant's words."""
    lines = [f"# {title}", "", f"*Exported on {_when(exported_at)} · Created {_when(created_at)}*", "", "---", ""]
    for message in messages:
        if not is_visible(message) or message.get("type") not in ("human", "ai"):
            continue
        text = visible_text(message)
        if not text:
            continue
        heading = "## 🧑 User" if message.get("type") == "human" else "## 🤖 Assistant"
        lines += [heading, "", text, "", "---", ""]
    return "\n".join(lines).rstrip() + "\n"


def transcript_json(*, title: str, thread_id: str, created_at: str | None, messages: list[dict[str, Any]], exported_at: datetime) -> str:
    """The conversation as JSON: its title and times, and each visible message that has words."""
    rows = []
    for message in messages:
        if not is_visible(message):
            continue
        text = visible_text(message)
        if not text:
            continue
        row: dict[str, Any] = {"type": message.get("type")}
        if message.get("id") is not None:
            row["id"] = message["id"]
        row["content"] = text
        rows.append(row)
    document: dict[str, Any] = {"title": title, "thread_id": thread_id}
    if created_at:
        document["created_at"] = created_at
    document["exported_at"] = exported_at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    document["messages"] = rows
    return json.dumps(document, ensure_ascii=False, indent=2)


def transcript_stem(title: str) -> str:
    """A file name for the conversation: its title's letters, digits, spaces, hyphens and underscores."""
    kept = "".join(char for char in title if unicodedata.category(char)[0] in "LN" or char in "_- ").strip()
    return kept or "conversation"


def content_disposition(stem: str, extension: str) -> str:
    """``attachment`` under the conversation's name, with a plain-ASCII name for a client that reads only that."""
    name = f"{stem}.{extension}"
    fallback = name if name.isascii() and name.isprintable() else f"conversation.{extension}"
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(name, safe='')}"


@dataclass(frozen=True)
class Conversation:
    """One conversation as its transcript needs it."""

    thread_id: str
    title: str
    created_at: str | None
    messages: list[dict[str, Any]]


def _identity(message: dict[str, Any]) -> str | None:
    """What the page matches a feed row and a checkpoint copy on: the id, a user turn's reminder suffix removed."""
    message_id = message.get("id")
    if not isinstance(message_id, str) or not message_id:
        return None
    return strip_injected_user_message_id_suffix(message_id) if message.get("type") == "human" else message_id


def weave(feed: list[dict[str, Any]], checkpoint: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The feed, with each checkpoint message it does not hold placed after the last one both hold.

    The feed holds every finished turn, however far the checkpoint was
    compacted. The checkpoint holds a turn not yet in the feed, and all of a
    conversation from before the feed existed; those go where the page puts
    them, next to the messages around them.
    """
    held = {identity for message in feed if (identity := _identity(message)) is not None}
    before: list[dict[str, Any]] = []
    after: dict[str, list[dict[str, Any]]] = {}
    anchor: str | None = None
    for message in checkpoint:
        identity = _identity(message)
        if identity is not None and identity in held:
            anchor = identity
            continue
        (before if anchor is None else after.setdefault(anchor, [])).append(message)
    woven = list(before)
    for message in feed:
        woven.append(message)
        woven.extend(after.pop(_identity(message) or "", []))
    return woven


async def read_conversation(scope: Any, thread_id: str, record: dict[str, Any] | None, *, user_id: str | None) -> Conversation | None:
    """The conversation as the page draws it; ``None`` when nothing was recorded.

    ``scope`` needs only ``app``. ``record`` is the thread's row: its agent
    decides which graph reads the checkpoint, and its times head the transcript.
    """
    feed = await feed_messages(get_run_event_store(scope), get_run_manager(scope), thread_id, user_id=user_id)
    accessor, config = build_checkpoint_state_accessor(scope, thread_id=thread_id, assistant_id=(record or {}).get("assistant_id"))
    snapshot = await accessor.aget(config)
    values = serialize_channel_values_for_api(snapshot.values or {}) if ((snapshot.config or {}).get("configurable") or {}).get("checkpoint_id") else {}
    checkpoint = values.get("messages") if isinstance(values.get("messages"), list) else []
    messages = weave(feed, [message for message in checkpoint if isinstance(message, dict)])
    if not messages:
        return None
    title = values.get("title") or (record or {}).get("display_name") or "Untitled"
    created_at = coerce_iso((record or {}).get("created_at", "")) or None
    return Conversation(thread_id=thread_id, title=str(title), created_at=created_at, messages=messages)
