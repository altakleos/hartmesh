"""Real SQL/current host custody; recorded remote rows/SSE are protocol seeds.

The actual model callback-to-SSE provenance is qualified separately.
"""

import asyncio
import json
import weakref

import httpx
import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from test_telegram_publication_authority import attempt_environment as attempt_environment  # noqa: F401
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment  # noqa: F401
from test_telegram_publication_coordinator import saved_marks
from test_telegram_publication_observation import ACK, run_row


@pytest.mark.asyncio
async def test_pending_stop_waits_for_exact_unique_run_before_cancel_post(coordinator_environment):
    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    calls = []
    exact = run_row(record, run_id="actual-old", created="2026-10-25T11:00:00Z")
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    unrelated = run_row(record, run_id="unrelated-new", publication_id="b" * 32)
    unrelated["metadata"] = {"channel_turn_id": "b" * 32}

    async def respond(request):
        calls.append((request.method, request.url.path))
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [unrelated, exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        if request.url.path.endswith("/cancel"):
            return httpx.Response(202)
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=b"")
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)

    async def no_preview(_preview):
        pytest.fail("No text was streamed")

    async def line(_key, _values):
        return None

    try:
        assert await observer.request_stop(record, e.link, stop_id="original-stop") == "pending"
        assert calls == []
        assert await observer.watch(record, e.link, on_preview=no_preview, on_line=line, turn_done=asyncio.Event()) == "actual-old"
        assert observer.run_id(e.thread_id, record.publication_id) == "actual-old"
        assert [(method, path) for method, path in calls if method == "POST"] == [("POST", "/original/api/threads/" + e.thread_id + "/runs/actual-old/cancel")]
    finally:
        await observer.drain()


@pytest.mark.asyncio
async def test_exact_scoped_preview_is_an_opaque_object_not_raw_bus_text(coordinator_environment):
    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    from deerflow.runtime.runs.preview import PreviewSpan, text_digest

    span = PreviewSpan(1, record.scope.owner_user_id, record.scope.thread_id, "coordinator-run", record.publication_id, "invocation", "model", "message", 1, 0, text_digest("Prefix"), text_digest("Prefix"))
    from dataclasses import asdict

    data = [{"type": "AIMessageChunk", "id": "message", "content": "Prefix"}, {"deerflow_preview_span": asdict(span)}]

    async def respond(request):
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=("id: 1-0\nevent: messages\ndata: " + json.dumps(data) + "\n\n").encode())
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    seen = []

    async def preview(value):
        seen.append(value)
        observer.record_shown(value, mode="draft", draft_id=701, part_id="draft-current", source_start=0, source_end=len(value.text))

    async def line(_key, _values):
        return None

    try:
        assert await observer.watch(record, e.link, on_preview=preview, on_line=line, turn_done=asyncio.Event()) == "coordinator-run"
        assert len(seen) == 1 and seen[0].text == "Prefix"
        with pytest.raises(RuntimeError):
            observer.record_shown({"text": "raw bus hint"}, mode="draft", draft_id=701, part_id="forged")
    finally:
        await observer.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content",
    [
        pytest.param([{"type": "text", "text": "Prefix", "index": 1}], id="claude"),
        pytest.param([{"type": "thinking", "thinking": "PRIVATE", "index": 0}, {"type": "text", "text": "Prefix", "index": 1}], id="claude-thinking"),
        pytest.param([{"type": "reasoning", "summary": ["PRIVATE"], "index": 0}, {"type": "text", "text": "Prefix", "annotations": [], "id": "msg_1", "index": 1}], id="responses"),
    ],
)
async def test_provider_block_chunks_preview_only_their_text_and_leave_the_watch_running(coordinator_environment, content):
    from dataclasses import asdict

    from app.telegram_bots.progress import RunObserver
    from deerflow.runtime.runs.preview import PreviewSpan, text_digest

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    span = PreviewSpan(1, record.scope.owner_user_id, record.scope.thread_id, "coordinator-run", record.publication_id, "invocation", "model", "message", 1, 0, text_digest("Prefix"), text_digest("Prefix"))
    text = [{"type": "AIMessageChunk", "id": "message", "content": content}, {"deerflow_preview_span": asdict(span)}]
    tool = [{"type": "AIMessageChunk", "id": "message", "content": "", "tool_calls": [{"id": "call-1", "name": "web_search", "args": {"query": "PRIVATE"}}]}, {}]
    frames = "".join(f"event: messages\ndata: {json.dumps(frame)}\n\n" for frame in (text, tool))

    async def respond(request):
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=frames.encode())
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    previews, lines = [], []

    async def preview(value):
        previews.append(value.text)

    async def line(key, values):
        lines.append((key, values))

    try:
        await observer.watch(record, e.link, on_preview=preview, on_line=line, turn_done=asyncio.Event())
        assert previews == ["Prefix"]
        assert ("activity.search", {}) in lines, "A tool line after block text must still be seen"
    finally:
        await observer.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("network_unknown", [False, True])
async def test_only_definite_cancel_refusal_clears_original_stop_barrier(coordinator_environment, network_unknown):
    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}

    async def respond(request):
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        if request.url.path.endswith("/cancel"):
            if network_unknown:
                raise httpx.ReadError("controlled transport uncertainty")
            return httpx.Response(409, json={"detail": "not cancellable"})
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    fresh = (await e.coordinator.locate_run(record, e.link, max_pages=1)).record
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    try:
        assert await observer.request_stop(fresh, e.link, stop_id="only-stop") == ("unknown" if network_unknown else "refused")
        rows = await saved_marks(e)
        expected = "only-stop" if network_unknown else None
        assert all(row.route_basis["publications"][-1]["publication_execution"].get("stop_id") == expected for row in rows)
        assert len([call for call in e.requests if call[0] == "POST"]) == 1
    finally:
        await observer.drain()


@pytest.mark.asyncio
async def test_completed_preview_callbacks_do_not_retain_every_cumulative_snapshot(coordinator_environment):
    from dataclasses import asdict

    from app.telegram_bots.progress import RunObserver
    from deerflow.runtime.runs.preview import PreviewSpan, text_digest

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    frames = []
    for sequence in range(1, 21):
        span = PreviewSpan(1, record.scope.owner_user_id, record.scope.thread_id, "coordinator-run", record.publication_id, "invocation", "model", "message", sequence, sequence - 1, text_digest("x"), text_digest("x" * sequence))
        data = [{"type": "AIMessageChunk", "id": "message", "content": "x"}, {"deerflow_preview_span": asdict(span)}]
        frames.append(f"id: {sequence}-0\nevent: messages\ndata: {json.dumps(data)}\n\n")

    async def respond(request):
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content="".join(frames).encode())
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    references = []

    async def preview(value):
        references.append(weakref.ref(value))

    async def line(_key, _values):
        return None

    try:
        await observer.watch(record, e.link, on_preview=preview, on_line=line, turn_done=asyncio.Event())
        assert len(references) == 20 and all(reference() is None for reference in references)
        observer.retire(record)
        assert observer._states == {}, "Retired, fully closed lifetimes must release their registry association"
    finally:
        await observer.drain()


@pytest.mark.asyncio
async def test_retired_observer_cannot_start_cancel_after_an_admitted_proof_read(coordinator_environment):
    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(request):
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        if request.url.path.endswith("/runs/coordinator-run"):
            entered.set()
            await release.wait()
        return httpx.Response(202) if request.method == "POST" else httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    record = (await e.coordinator.locate_run(record, e.link, max_pages=1)).record
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    stop = asyncio.create_task(observer.request_stop(record, e.link, stop_id="retired-stop"))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        observer.retire(record)
        release.set()
        assert await asyncio.wait_for(stop, 3) == "unknown"
        assert not any(method == "POST" for method, _path, _headers in e.requests)
    finally:
        release.set()
        await asyncio.gather(stop, return_exceptions=True)
        await observer.drain()


@pytest.mark.asyncio
async def test_activity_is_scoped_advisory_and_never_discloses_arguments(coordinator_environment):
    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    frames = []
    for index, (name, args) in enumerate(
        [
            ("read_file", {"path": "/private/tenant/notes.md"}),
            ("bash", {"command": "PRIVATE_COMMAND"}),
            ("web_search", {"query": "PRIVATE_QUERY"}),
            ("web_fetch", {"url": "https://example.com/private?secret=PRIVATE_QUERY"}),
            ("ask_clarification", {"question": "PRIVATE_QUERY"}),
        ]
    ):
        data = [{"type": "AIMessageChunk", "id": "tool-message", "content": "", "tool_calls": [{"id": f"call-{index}", "name": name, "args": args}]}, {}]
        frames.append(f"event: messages\ndata: {json.dumps(data)}\n\n")
        frames.append(f"event: messages\ndata: {json.dumps([{'type': 'tool', 'tool_call_id': f'call-{index}', 'content': 'PRIVATE_RESULT'}, {}])}\n\n")
    # Scoped join may include namespaced subagent messages; these do not become
    # the foreground activity line by looking like root events.
    frames.append('event: messages|subgraph\ndata: [{"type":"AIMessageChunk","tool_calls":[{"name":"write_file","args":{"path":"wrong.md"}}]},{}]\n\n')

    async def respond(request):
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content="".join(frames).encode())
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    lines = []

    async def line(key, values):
        lines.append((key, values))

    async def preview(_value):
        pytest.fail("Tool frames are not lead text")

    try:
        await observer.watch(record, e.link, on_preview=preview, on_line=line, turn_done=asyncio.Event())
        assert [(key, values) for key, values in lines if key is not None] == [("activity.read", {"file": "notes.md"}), ("activity.command", {}), ("activity.search", {}), ("activity.fetch", {"host": "example.com"})]
        assert "PRIVATE" not in json.dumps(lines) and "/tenant/" not in json.dumps(lines)
        assert lines[-1] == (None, {})
    finally:
        await observer.drain()


@pytest.mark.asyncio
async def test_reopened_exact_observation_resumes_only_its_original_cursor(coordinator_environment):
    from dataclasses import asdict

    from app.telegram_bots.progress import RunObserver
    from deerflow.runtime.runs.preview import PreviewSpan, text_digest

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    cursors, seen = [], []

    async def respond(request):
        if request.url.path.endswith("/join"):
            cursors.append(request.headers.get("Last-Event-ID"))
            sequence = len(cursors)
            span = PreviewSpan(1, record.scope.owner_user_id, record.scope.thread_id, "coordinator-run", record.publication_id, "root", "model", "message", sequence, sequence - 1, text_digest("x"), text_digest("x" * sequence))
            data = [{"type": "AIMessageChunk", "id": "message", "content": "x"}, {"deerflow_preview_span": asdict(span)}]
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=f"id: {sequence}-0\nevent: messages\ndata: {json.dumps(data)}\n\n".encode())
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        return httpx.Response(200, headers=ACK, json=exact)

    async def preview(value):
        seen.append(value.text)

    async def line(*args):
        pass

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    try:
        for _ in range(2):
            assert await observer.watch(record, e.link, on_preview=preview, on_line=line, turn_done=asyncio.Event()) == "coordinator-run"
        assert cursors == [None, "1-0"] and seen == ["x", "xx"]
    finally:
        await observer.drain()


@pytest.mark.asyncio
async def test_accepted_stop_cannot_replace_a_normally_completed_exact_output(coordinator_environment):
    from test_telegram_publication_observation import output_projection

    from app.telegram_bots.progress import RunObserver
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}

    async def respond(request):
        if request.url.path.endswith("/cancel"):
            return httpx.Response(202)
        if request.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=output_projection(record))
        if request.url.path.endswith("/runs/page"):
            return httpx.Response(200, headers=ACK, json={"data": [exact], "has_more": False, "next_before_created_at": None, "next_before_run_id": None})
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    record = (await e.coordinator.locate_run(record, e.link, max_pages=1)).record
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    try:
        assert await observer.request_stop(record, e.link, stop_id="completion-race") == "requested"
        identity = await observer.plan_final(record, e.link, stopped=True)
        assert identity is not None
        assert observer.final_read(record) == {"output": "complete:None", "inventory": "matched:complete:None"}
        async with e.db.sessions() as session:
            row = await session.get(TelegramOutboxRow, identity)
            assert row.payload["reply_basis"]["outcome"]["kind"] == "model_text"
            assert all(part["source_ref"].get("key") != "stop.answer" for part in row.parts)
    finally:
        await observer.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["requested", "refused", "terminal", "unknown"])
async def test_deferred_stop_retains_exact_decision_without_renewing_or_resubmitting(coordinator_environment, outcome):
    from sqlalchemy import event

    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    if outcome == "terminal":
        exact["status"] = "success"
    calls = []

    async def respond(request):
        calls.append(request)
        if request.url.path.endswith("/cancel"):
            if outcome == "unknown":
                raise httpx.ReadError("controlled cancel uncertainty")
            return httpx.Response(409 if outcome == "refused" else 202)
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=b"")
        return httpx.Response(200, headers=ACK, json=exact)

    async def no_preview(_value):
        pytest.fail("No model text in this controlled response")

    async def no_line(*args):
        pass

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    try:
        assert observer.stop_outcome(record, e.link, stop_id="original-stop") is None
        assert await observer.request_stop(record, e.link, stop_id="original-stop") == "pending"
        assert observer.stop_outcome(record, e.link, stop_id="original-stop") == "pending"
        assert await observer.watch(record, e.link, on_preview=no_preview, on_line=no_line, turn_done=asyncio.Event()) == "coordinator-run"
        statements = []

        def sql(*args):
            statements.append(args[2])

        count = len(calls)
        event.listen(e.db.engine.sync_engine, "before_cursor_execute", sql)
        try:
            assert observer.stop_outcome(record, e.link, stop_id="original-stop") == outcome
            assert observer.stop_outcome(record, e.link, stop_id="another-stop") is None
            assert await observer.request_stop(record, e.link, stop_id="original-stop") == outcome
            assert not statements and len(calls) == count
        finally:
            event.remove(e.db.engine.sync_engine, "before_cursor_execute", sql)
        assert len([request for request in calls if request.method == "POST"]) == (0 if outcome == "terminal" else 1)
        rows = await saved_marks(e)
        assert all(row.route_basis["publications"][-1]["publication_execution"].get("stop_id") == (None if outcome == "refused" else "original-stop") for row in rows)
        observer.retire(record)
        assert observer.stop_outcome(record, e.link, stop_id="original-stop") is None
    finally:
        await observer.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["requested", "refused", "terminal"])
async def test_deferred_stop_same_id_replay_does_not_lose_decision_or_repeat_post(coordinator_environment, outcome):
    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    if outcome == "terminal":
        exact["status"] = "success"
    posts = []

    async def respond(request):
        if request.url.path.endswith("/cancel"):
            posts.append(request)
            return httpx.Response(409 if outcome == "refused" else 202)
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=b"")
        return httpx.Response(200, headers=ACK, json=exact)

    async def no_preview(_value):
        pytest.fail("No text expected")

    async def no_line(*args):
        pass

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    try:
        assert await observer.request_stop(record, e.link, stop_id="same-original-stop") == "pending"
        assert await observer.watch(record, e.link, on_preview=no_preview, on_line=no_line, turn_done=asyncio.Event()) == "coordinator-run"
        assert await observer.request_stop(record, e.link, stop_id="same-original-stop") == outcome
        assert len(posts) == (0 if outcome == "terminal" else 1)
    finally:
        await observer.drain()


@pytest.mark.asyncio
async def test_conclusive_turn_end_lapses_an_unapplied_stop_decision(coordinator_environment):
    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    initial_requests = len(e.requests)
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    done = asyncio.Event()
    done.set()

    async def unexpected(*args):
        pytest.fail("Unmatched ended turn has no preview/activity")

    try:
        assert await observer.request_stop(record, e.link, stop_id="lapsed-stop") == "pending"
        assert await observer.watch(record, e.link, on_preview=unexpected, on_line=unexpected, turn_done=done) is None
        assert observer.stop_outcome(record, e.link, stop_id="lapsed-stop") is None
        assert len(e.requests) == initial_requests
    finally:
        await observer.drain()
