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
from deerflow.utils.visible_message import HIDDEN_CONTROL_MESSAGE_NAMES as HIDDEN_CONTROL_MESSAGE_NAMES
from deerflow.utils.visible_message import INTERNAL_MARKER_TAGS as INTERNAL_MARKER_TAGS
from deerflow.utils.visible_message import drawn_content as drawn_content
from deerflow.utils.visible_message import is_visible, visible_text

#: What a transcript is written as, by the ``format`` a caller names.
FORMATS = {"markdown": ("md", "text/markdown; charset=utf-8"), "json": ("json", "application/json; charset=utf-8")}


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


async def read_conversation(scope: Any, thread_id: str, record: dict[str, Any] | None, *, user_id: str | None, visible_runs: set[str] | None = None, visible_human_runs: set[str] | None = None) -> Conversation | None:
    """The conversation as the page draws it; ``None`` when nothing was recorded.

    ``scope`` needs only ``app``. ``record`` is the thread's row: its agent
    decides which graph reads the checkpoint, and its times head the transcript.
    """

    def observe(run_id, message):
        if message.get("type") in {"ai", "human"} and is_visible(message) and visible_text(message):
            if visible_runs is not None:
                visible_runs.add(run_id)
            if visible_human_runs is not None and message.get("type") == "human":
                visible_human_runs.add(run_id)

    feed = await feed_messages(get_run_event_store(scope), get_run_manager(scope), thread_id, user_id=user_id, **({"observe": observe} if visible_runs is not None or visible_human_runs is not None else {}))
    from app.gateway.agent_conversations import inspection_for_read

    inspection = await inspection_for_read(scope, thread_id, user_id=user_id)
    accessor, config = build_checkpoint_state_accessor(scope, thread_id=thread_id, assistant_id=(record or {}).get("assistant_id"), **({"agent_inspection": inspection} if inspection is not None else {}))
    snapshot = await accessor.aget(config)
    values = serialize_channel_values_for_api(snapshot.values or {}) if ((snapshot.config or {}).get("configurable") or {}).get("checkpoint_id") else {}
    checkpoint = values.get("messages") if isinstance(values.get("messages"), list) else []
    messages = weave(feed, [message for message in checkpoint if isinstance(message, dict)])
    if not messages:
        return None
    title = values.get("title") or (record or {}).get("display_name") or "Untitled"
    created_at = coerce_iso((record or {}).get("created_at", "")) or None
    return Conversation(thread_id=thread_id, title=str(title), created_at=created_at, messages=messages)
