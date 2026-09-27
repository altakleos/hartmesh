"""A conversation's transcript, written by the Gateway: what the page shows, nothing internal.

Every transcript a person downloads -- one conversation from its menu, or all
of them at once -- is written here, from the same messages the page draws.
``contracts/visible_transcript_contract.json`` holds the page to the same
cases (``frontend-hm/tests/unit/core/messages/visible-transcript-contract.test.ts``),
so a download says what the page showed.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore

from app.gateway import transcript
from app.gateway.auth.models import User
from app.gateway.routers import threads
from deerflow.persistence.thread_meta.memory import THREADS_NS, MemoryThreadMetaStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import EditReplayVisibility

CONTRACT = json.loads((Path(__file__).resolve().parents[2] / "contracts" / "visible_transcript_contract.json").read_text(encoding="utf-8"))
EXPORTED_AT = datetime(2026, 9, 27, 10, 30, tzinfo=UTC)
CREATED_AT = "2026-09-20T08:15:00+00:00"
CALLER = User(id="6f1c2b1e-2d7a-4a8e-9c1b-0e4d5f6a7b8c", email="pat@example.com", password_hash="x", system_role="user")


def test_the_same_injected_context_tags_are_stripped() -> None:
    assert list(transcript.INTERNAL_MARKER_TAGS) == CONTRACT["internal_marker_tags"]


@pytest.mark.parametrize("case", CONTRACT["cases"], ids=[case["name"] for case in CONTRACT["cases"]])
def test_each_message_is_shown_as_the_page_shows_it(case: dict) -> None:
    assert transcript.is_visible(case["message"]) is case["visible"]
    assert transcript.drawn_content(case["message"]) == case["content"]
    assert transcript.visible_text(case["message"]) == case["text"]


def _messages() -> list[dict]:
    return [
        {"id": "h-1", "type": "human", "content": "hello"},
        {"id": "h-2", "type": "human", "content": "internal reminder", "additional_kwargs": {"hide_from_ui": True}},
        {"id": "a-1", "type": "ai", "content": "<think>secret reasoning</think>hi there", "tool_calls": [{"id": "1", "name": "task", "args": {}}]},
        {"id": "t-1", "type": "tool", "name": "task", "tool_call_id": "1", "content": "internal trace"},
        {"id": "a-2", "type": "ai", "content": "<think>only thinking</think>"},
        {"id": "a-3", "type": "ai", "content": "done"},
    ]


def test_the_markdown_transcript_is_the_visible_conversation_under_its_title() -> None:
    text = transcript.transcript_markdown(title="Monthly review", created_at=CREATED_AT, messages=_messages(), exported_at=EXPORTED_AT)

    assert text == ("# Monthly review\n\n*Exported on 2026-09-27 10:30 UTC · Created 2026-09-20 08:15 UTC*\n\n---\n\n## 🧑 User\n\nhello\n\n---\n\n## 🤖 Assistant\n\nhi there\n\n---\n\n## 🤖 Assistant\n\ndone\n\n---\n")


def test_a_conversation_with_no_recorded_creation_says_unknown() -> None:
    text = transcript.transcript_markdown(title="Untitled", created_at=None, messages=[], exported_at=EXPORTED_AT)

    assert text == "# Untitled\n\n*Exported on 2026-09-27 10:30 UTC · Created Unknown*\n\n---\n"


def test_the_json_transcript_carries_only_visible_messages_with_content() -> None:
    document = json.loads(transcript.transcript_json(title="Monthly review", thread_id="thread-1", created_at=CREATED_AT, messages=_messages(), exported_at=EXPORTED_AT))

    assert document == {
        "title": "Monthly review",
        "thread_id": "thread-1",
        "created_at": CREATED_AT,
        "exported_at": "2026-09-27T10:30:00.000Z",
        "messages": [
            {"type": "human", "id": "h-1", "content": "hello"},
            {"type": "ai", "id": "a-1", "content": "hi there"},
            {"type": "ai", "id": "a-3", "content": "done"},
        ],
    }


def test_a_message_without_an_id_is_written_without_one() -> None:
    document = json.loads(transcript.transcript_json(title="t", thread_id="thread-1", created_at=None, messages=[{"type": "human", "content": "hi"}], exported_at=EXPORTED_AT))

    assert document["messages"] == [{"type": "human", "content": "hi"}]


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("Monthly review", "Monthly review"),
        ("Q3: revenue/costs?", "Q3 revenuecosts"),
        ("Café réunion", "Café réunion"),
        ("***", "conversation"),
        ("  ", "conversation"),
    ],
)
def test_the_file_is_named_for_the_conversation(title: str, expected: str) -> None:
    assert transcript.transcript_stem(title) == expected


# ── GET /api/threads/{thread_id}/export ─────────────────────────────────


class _OwnedThreadMetaStore(MemoryThreadMetaStore):
    """Ownership as the SQL store decides it, keyed to one fixed caller."""

    def __init__(self, store: InMemoryStore, owners: dict[str, str | None]) -> None:
        super().__init__(store)
        self._owners = owners

    async def check_access(self, thread_id, user_id, *, require_existing=False):  # type: ignore[override]
        if thread_id not in self._owners:
            return not require_existing
        owner = self._owners[thread_id]
        return owner is None or owner == user_id

    async def get(self, thread_id, *, user_id=None):  # type: ignore[override]
        item = await self._store.aget(THREADS_NS, thread_id)
        return dict(item.value) if item is not None else None


class _RawStateAccessor:
    def __init__(self, checkpointer):
        self._checkpointer = checkpointer

    async def aget(self, config):
        from types import SimpleNamespace

        saved = await self._checkpointer.aget_tuple(config)
        if saved is None:
            return SimpleNamespace(values={}, config={}, parent_config=None, metadata={}, next=(), tasks=(), created_at=None)
        return SimpleNamespace(values=saved.checkpoint["channel_values"], config=saved.config, parent_config=saved.parent_config, metadata=saved.metadata, next=(), tasks=(), created_at=None)


def _app(monkeypatch, owners: dict[str, str | None], *, superseded: set[str] | None = None):
    app = make_authed_test_app(user_factory=lambda: CALLER)
    store = InMemoryStore()
    checkpointer = InMemorySaver()
    app.state.store = store
    app.state.checkpointer = checkpointer
    app.state.thread_store = _OwnedThreadMetaStore(store, owners)
    app.state.run_event_store = MemoryRunEventStore()
    run_manager = AsyncMock()
    run_manager.list_successful_regenerate_sources.return_value = superseded or set()
    run_manager.list_edit_replay_visibility.return_value = EditReplayVisibility()
    app.state.run_manager = run_manager
    app.include_router(threads.router)
    asked: list[str | None] = []

    def _accessor(request, *, thread_id, assistant_id=None, checkpoint_id=None):
        asked.append(assistant_id)
        return _RawStateAccessor(request.app.state.checkpointer), {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}

    monkeypatch.setattr(transcript, "build_checkpoint_state_accessor", _accessor)
    app.state.asked_assistants = asked
    return app, store, checkpointer


async def _journal(app, run_id: str, message: dict, *, caller: str = "lead_agent") -> None:
    await app.state.run_event_store.put(
        thread_id="thread-1",
        run_id=run_id,
        event_type="llm.ai.response" if message["type"] == "ai" else "llm.human.input",
        category="message",
        content={"additional_kwargs": {}, **message},
        metadata={"caller": caller},
    )


async def _seed(store, checkpointer, thread_id: str, messages: list, *, title: str | None = "Monthly review", assistant_id: str | None = None, display_name: str | None = None) -> None:
    await store.aput(THREADS_NS, thread_id, {"thread_id": thread_id, "status": "idle", "created_at": CREATED_AT, "updated_at": CREATED_AT, "metadata": {}, "assistant_id": assistant_id, "display_name": display_name})
    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"messages": messages, **({"title": title} if title else {})}
    versions = {channel: 1 for channel in checkpoint["channel_values"]}
    checkpoint["channel_versions"] = versions
    await checkpointer.aput({"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}, checkpoint, {"step": 1, "source": "loop", "writes": {}, "parents": {}}, versions)


def _conversation() -> list:
    return [
        HumanMessage(id="h-1", content="hello"),
        AIMessage(id="a-1", content="<think>secret reasoning</think>hi there", tool_calls=[{"id": "1", "name": "task", "args": {}}]),
        ToolMessage(id="t-1", content="internal trace", tool_call_id="1", name="task"),
        AIMessage(id="a-2", content="done"),
    ]


def test_the_route_downloads_one_conversation_as_markdown(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)})
    asyncio.run(_seed(store, checkpointer, "thread-1", _conversation()))

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "markdown"})

    assert response.status_code == 200, response.text
    assert response.headers["content-type"] == "text/markdown; charset=utf-8"
    assert response.headers["content-disposition"] == "attachment; filename=\"Monthly review.md\"; filename*=UTF-8''Monthly%20review.md"
    # A transcript is the person's words; no browser or proxy keeps a copy.
    assert response.headers["cache-control"] == "no-store"
    body = response.text
    assert body.startswith("# Monthly review\n") and "Created 2026-09-20 08:15 UTC" in body
    assert "hi there" in body and "done" in body
    assert "secret reasoning" not in body and "internal trace" not in body


def test_the_route_downloads_one_conversation_as_json(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)})
    asyncio.run(_seed(store, checkpointer, "thread-1", _conversation(), title=None))

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "json"})

    assert response.status_code == 200, response.text
    assert response.headers["content-type"] == "application/json; charset=utf-8"
    assert response.headers["content-disposition"] == "attachment; filename=\"Untitled.json\"; filename*=UTF-8''Untitled.json"
    document = response.json()
    assert document["title"] == "Untitled" and document["thread_id"] == "thread-1" and document["created_at"] == CREATED_AT
    assert [message["content"] for message in document["messages"]] == ["hello", "hi there", "done"]


def test_a_title_outside_latin_1_keeps_its_name(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)})
    asyncio.run(_seed(store, checkpointer, "thread-1", _conversation(), title="月度 review"))

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "markdown"})

    assert response.status_code == 200
    assert response.headers["content-disposition"] == "attachment; filename=\"conversation.md\"; filename*=UTF-8''%E6%9C%88%E5%BA%A6%20review.md"


def test_someone_else_s_conversation_is_not_found(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": "someone-else"})
    asyncio.run(_seed(store, checkpointer, "thread-1", _conversation()))

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "markdown"})

    assert response.status_code == 404


def test_a_conversation_with_nothing_recorded_is_not_found(monkeypatch) -> None:
    app, _, _ = _app(monkeypatch, {"thread-1": str(CALLER.id)})

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "markdown"})

    assert response.status_code == 404


def test_an_unknown_format_is_refused(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)})
    asyncio.run(_seed(store, checkpointer, "thread-1", _conversation()))

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "html"})

    assert response.status_code == 422


def test_a_personal_access_token_cannot_reach_it() -> None:
    from app.gateway.auth.pat import is_pat_allowed_route

    assert not is_pat_allowed_route("GET", "/api/threads/thread-1/export")


# ── Which messages: the feed the page draws, not the compacted checkpoint ──


def _texts(response) -> list[str]:
    return [message["content"] for message in response.json()["messages"]]


def test_a_compacted_conversation_exports_every_turn_the_page_still_shows(monkeypatch) -> None:
    """Summarization cuts the checkpoint to a summary and the recent turns; the feed keeps them all."""
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)})

    async def seed() -> None:
        for run_id, message in [
            ("run-1", {"type": "human", "id": "h-1", "content": "first question"}),
            ("run-1", {"type": "ai", "id": "a-1", "content": "first answer"}),
            ("run-2", {"type": "human", "id": "h-2", "content": "second question"}),
            ("run-2", {"type": "ai", "id": "a-2", "content": "second answer"}),
        ]:
            await _journal(app, run_id, message)
        compacted = [AIMessage(id="s-1", name="summary", content="they asked two things"), HumanMessage(id="h-2", content="second question"), AIMessage(id="a-2", content="second answer")]
        await _seed(store, checkpointer, "thread-1", compacted)

    asyncio.run(seed())
    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "json"})

    assert response.status_code == 200, response.text
    assert _texts(response) == ["first question", "first answer", "second question", "second answer"]


def test_the_feed_leaves_out_what_the_page_never_draws(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)}, superseded={"run-old"})

    async def seed() -> None:
        await _journal(app, "run-1", {"type": "human", "id": "h-1", "content": "question"})
        await _journal(app, "run-1", {"type": "ai", "id": "t-1", "content": "Monthly review"}, caller="middleware:title")
        await _journal(app, "run-1", {"type": "ai", "id": "sub-1", "content": "subagent notes"}, caller="subagent:general-purpose")
        await _journal(app, "run-old", {"type": "ai", "id": "a-old", "content": "replaced answer"})
        await _journal(app, "run-new", {"type": "ai", "id": "a-new", "content": "regenerated answer"})
        await _seed(store, checkpointer, "thread-1", [])

    asyncio.run(seed())
    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "json"})

    assert _texts(response) == ["question", "regenerated answer"]


def test_a_turn_not_yet_in_the_feed_follows_the_messages_before_it(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)})

    async def seed() -> None:
        await _journal(app, "run-1", {"type": "human", "id": "h-1", "content": "question"})
        await _journal(app, "run-1", {"type": "ai", "id": "a-1", "content": "answer"})
        await _journal(app, "run-2", {"type": "human", "id": "h-2", "content": "follow-up"})
        # The checkpoint carries the reminder-swapped id for the same user turn, and the answer still being written.
        await _seed(store, checkpointer, "thread-1", [HumanMessage(id="h-1", content="question"), AIMessage(id="a-1", content="answer"), HumanMessage(id="h-2__user", content="follow-up"), AIMessage(id="a-2", content="answer in progress")])

    asyncio.run(seed())
    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "json"})

    assert _texts(response) == ["question", "answer", "follow-up", "answer in progress"]


def test_the_thread_s_own_agent_reads_its_checkpoint(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)})
    asyncio.run(_seed(store, checkpointer, "thread-1", _conversation(), assistant_id="analyst"))

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "markdown"})

    assert response.status_code == 200 and app.state.asked_assistants == ["analyst"]


def test_a_conversation_with_no_messages_has_no_transcript(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id)})
    asyncio.run(_seed(store, checkpointer, "thread-1", []))

    with TestClient(app) as client:
        response = client.get("/api/threads/thread-1/export", params={"format": "markdown"})

    assert response.status_code == 404


def test_weaving_keeps_the_feed_order_and_places_what_it_lacks_by_its_neighbours() -> None:
    feed = [{"type": "human", "id": "h-2"}, {"type": "ai", "id": "a-2"}]
    checkpoint = [{"type": "human", "id": "h-0"}, {"type": "human", "id": "h-2__user"}, {"type": "ai", "id": "x-1"}, {"type": "ai", "id": "a-2"}, {"type": "ai", "id": "x-2"}, {"type": "ai"}]

    woven = transcript.weave(feed, checkpoint)

    assert [message.get("id") for message in woven] == ["h-0", "h-2", "x-1", "a-2", "x-2", None]
    assert transcript.weave([], checkpoint) == checkpoint
    assert transcript.weave(feed, []) == feed


def test_the_title_is_the_conversation_s_own_then_the_name_the_sidebar_shows(monkeypatch) -> None:
    app, store, checkpointer = _app(monkeypatch, {"thread-1": str(CALLER.id), "thread-2": str(CALLER.id)})
    asyncio.run(_seed(store, checkpointer, "thread-1", _conversation(), title=None, display_name="Renamed in the sidebar"))
    asyncio.run(_seed(store, checkpointer, "thread-2", _conversation(), title="From the conversation", display_name="Renamed in the sidebar"))

    with TestClient(app) as client:
        named = client.get("/api/threads/thread-1/export", params={"format": "json"}).json()
        titled = client.get("/api/threads/thread-2/export", params={"format": "json"}).json()

    assert named["title"] == "Renamed in the sidebar" and titled["title"] == "From the conversation"


def test_times_are_given_in_utc_whatever_zone_they_were_recorded_in() -> None:
    text = transcript.transcript_markdown(title="t", created_at="2026-09-20T10:15:00+02:00", messages=[], exported_at=datetime(2026, 9, 27, 12, 30, tzinfo=timezone(timedelta(hours=2))))

    assert "*Exported on 2026-09-27 10:30 UTC · Created 2026-09-20 08:15 UTC*" in text
