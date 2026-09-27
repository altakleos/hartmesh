"""What a conversation's message feed holds: the journal rows the page draws, in order.

The page reads a conversation from the run-event journal
(``GET /api/threads/{thread_id}/messages/page``), not from the checkpoint:
summarization and ``/compact`` cut the checkpoint down to a summary and the
recent turns, and the journal keeps every turn. The feed leaves out what the
page never draws -- a middleware's own messages, a subagent's replies, and the
runs a regenerate or an edit replaced -- and a transcript reads the same rows
(``app.gateway.transcript``).
"""

from __future__ import annotations

from typing import Any

#: Rows read per journal query while walking a whole conversation.
FEED_SCAN_BATCH = 500


def message_type(message: Any) -> str | None:
    """A message's type, from an object or its serialized form; an ``assistant`` role reads as ``ai``."""
    value = getattr(message, "type", None)
    if value is None and isinstance(message, dict):
        value = message.get("type") or message.get("role")
    if value == "assistant":
        return "ai"
    return str(value) if value else None


def is_history_hidden_row(row: dict[str, Any]) -> bool:
    """A journal row the page never draws: a middleware's message, or a subagent's reply."""
    caller = str((row.get("metadata") or {}).get("caller", ""))
    return caller.startswith("middleware:") or (caller.startswith("subagent:") and message_type(row.get("content")) == "ai")


async def history_hidden_run_ids(run_mgr: Any, thread_id: str, *, user_id: str | None) -> set[str]:
    """The runs a regenerate or an edit replaced, which the page's history leaves out."""
    superseded_run_ids = await run_mgr.list_successful_regenerate_sources(thread_id, user_id=user_id)
    edit_visibility = await run_mgr.list_edit_replay_visibility(thread_id, user_id=user_id)
    return set(superseded_run_ids) | set(edit_visibility.hidden_source_run_ids) | set(edit_visibility.hidden_attempt_run_ids)


async def feed_messages(event_store: Any, run_mgr: Any, thread_id: str, *, user_id: str | None) -> list[dict[str, Any]]:
    """Every message of the conversation's feed, oldest first, as the page's history pages hold them."""
    hidden_runs = await history_hidden_run_ids(run_mgr, thread_id, user_id=user_id)
    messages: list[dict[str, Any]] = []
    after_seq = 0
    while True:
        rows = await event_store.list_messages(thread_id, limit=FEED_SCAN_BATCH, after_seq=after_seq, user_id=user_id)
        for row in rows:
            content = row.get("content")
            if isinstance(content, dict) and not is_history_hidden_row(row) and row.get("run_id") not in hidden_runs:
                messages.append(content)
        if len(rows) < FEED_SCAN_BATCH:
            return messages
        next_seq = max(row["seq"] for row in rows)
        if next_seq <= after_seq:
            raise RuntimeError(f"message feed for thread {thread_id} did not advance past seq {after_seq}")
        after_seq = next_seq
