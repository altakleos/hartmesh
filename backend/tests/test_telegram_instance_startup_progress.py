"""Exact-run progress frames over real HTTP ownership and instance SQL authority."""

import asyncio
import json

import httpx
import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_admission import admission as admission
from test_telegram_instance_publication import publication as publication
from test_telegram_publication_observation import ACK, run_row

from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.progress import RunObserver

pytestmark = pytest.mark.asyncio


class Frames(httpx.AsyncByteStream):
    def __init__(self):
        self.queue = asyncio.Queue()
        self.closed = False

    async def __aiter__(self):
        while True:
            value = await self.queue.get()
            if value is None:
                return
            event, data = value if isinstance(value, tuple) else ("custom", value)
            yield ("event: " + event + "\ndata: " + json.dumps(data) + "\n\n").encode()

    async def aclose(self):
        self.closed = True


@pytest_asyncio.fixture
async def starting(publication):
    e = publication
    asgi = httpx.ASGITransport(app=e.http_app)
    e.frames = Frames()

    async def respond(request):
        if "/runs/" in request.url.path:
            if request.url.path.endswith("/join"):
                return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=e.frames)
            return httpx.Response(200, headers=ACK, json=e.exact)
        return await asgi.handle_async_request(request)

    old = e.http_client
    e.http_client = GatewayClientOwner(gateway_url="http://gateway.example", langgraph_url="http://runtime.example/api", transport=httpx.MockTransport(respond))
    assert e.http_client.endpoint_id == old.endpoint_id
    e.threads.client = e.host._client = e.coordinator._http_owner = e.http_client
    await old.stop()
    e.record = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    e.exact = run_row(e.record)
    e.exact["metadata"] = {"channel_turn_id": e.record.publication_id}
    e.lines, e.delays, e.now = [], [], [10.0]
    e.waiting, e.release, e.cleaned = asyncio.Event(), asyncio.Event(), asyncio.Event()
    e.cleanup_gate, e.cleanup_entered = None, asyncio.Event()
    e.line_gate, e.line_entered = None, asyncio.Event()

    async def sleep(delay):
        e.delays.append(delay)
        e.waiting.set()
        try:
            await e.release.wait()
        finally:
            e.cleanup_entered.set()
            if e.cleanup_gate is not None:
                await e.cleanup_gate.wait()
            e.cleaned.set()

    e.observer = RunObserver(coordinator=e.coordinator, http_owner=e.http_client, capture_attempt=e.host.capture, is_current=lambda record: e.active[0], clock=lambda: e.now[0], sleep=sleep)

    async def line(key, values):
        if key == "activity.command" and e.line_gate is not None:
            e.line_entered.set()
            await e.line_gate.wait()
        e.lines.append((key, values))

    async def preview(value):
        pytest.fail("Progress cannot create a model preview")

    e.watch = asyncio.create_task(e.observer.watch(e.record, e.link, on_line=line, on_preview=preview, turn_done=asyncio.Event()))
    try:
        yield e
    finally:
        await e.observer.drain()
        await asyncio.gather(e.watch, return_exceptions=True)
        await asgi.aclose()


def frame(e, **values):
    return {"type": "turn_progress", "run_id": e.exact["run_id"], "stage": "workspace_starting", "at_ms": 1, **values}


async def wait_for_line(e, key):
    async def wait():
        while not any(value[0] == key for value in e.lines):
            if e.watch.done():
                await e.watch
                pytest.fail("Stream ended before progress")
            # Let SQLite authority preparation settle; retain the same deadline.
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), 1)


async def test_actual_scoped_starting_frame_shows_initial_and_thirty_second_lines(starting):
    e = starting
    await e.frames.queue.put(frame(e))
    await wait_for_line(e, "start.ws_1")
    await asyncio.wait_for(e.waiting.wait(), 1)
    assert e.delays == [30]
    e.now[0] += 30
    e.release.set()
    await wait_for_line(e, "start.ws_30")
    await e.frames.queue.put(frame(e))
    await e.frames.queue.put(frame(e, stage="thinking", at_ms=30002))
    await e.frames.queue.put(None)
    await asyncio.wait_for(e.watch, 2)
    assert [key for key, _ in e.lines if key] == ["start.ws_1", "start.ws_30"]
    assert e.frames.closed


@pytest.mark.parametrize("change", [{"run_id": "another-run"}, {"type": "tool_result"}, {"stage": "model-supplied-name"}, {"at_ms": -1}, {"at_ms": True}, {"extra": "untrusted"}])
async def test_malformed_or_foreign_progress_never_selects_a_line(starting, change):
    e = starting
    await e.frames.queue.put(frame(e, **change))
    await e.frames.queue.put(None)
    await asyncio.wait_for(e.watch, 2)
    assert not [key for key, _ in e.lines if key] and not e.delays


@pytest.mark.parametrize("ending", ["thinking", "retired", "cancelled"])
async def test_starting_timer_is_owned_until_stream_or_turn_settles(starting, ending):
    e = starting
    await e.frames.queue.put(frame(e))
    await wait_for_line(e, "start.ws_1")
    await asyncio.wait_for(e.waiting.wait(), 1)
    if ending == "thinking":
        await e.frames.queue.put(frame(e, stage="thinking"))
        await e.frames.queue.put(None)
    elif ending == "retired":
        e.observer.retire(e.record)
    else:
        e.watch.cancel()
    await asyncio.wait_for(asyncio.gather(e.watch, return_exceptions=True), 2)
    assert e.cleaned.is_set()
    e.now[0] += 30
    e.release.set()
    await asyncio.sleep(0)
    assert [key for key, _ in e.lines if key] == ["start.ws_1"]
    assert e.frames.closed


async def test_repeated_watch_cancellation_drains_timer_then_closes_stream(starting):
    e = starting
    e.cleanup_gate = asyncio.Event()
    e.line_gate = asyncio.Event()
    await e.frames.queue.put(frame(e))
    await wait_for_line(e, "start.ws_1")
    await asyncio.wait_for(e.waiting.wait(), 1)
    await e.frames.queue.put(("messages", [{"type": "AIMessageChunk", "id": "message", "content": "", "tool_calls": [{"id": "call", "name": "bash", "args": {}}]}, {}]))
    await asyncio.wait_for(e.line_entered.wait(), 1)
    try:
        e.watch.cancel()
        await asyncio.wait_for(e.cleanup_entered.wait(), 1)
        e.watch.cancel()
        await asyncio.sleep(0)
        assert not e.watch.done()
        e.cleanup_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await e.watch
        assert e.cleaned.is_set() and e.frames.closed
    finally:
        e.cleanup_gate.set()
        e.line_gate.set()


async def test_startup_line_uses_current_instance_name_and_use(admission):
    from types import SimpleNamespace

    from sqlalchemy import update

    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = admission
    shown = []

    async def activity(key, values):
        shown.append((key, values))

    e.turn.presentation = SimpleNamespace(activity=activity)
    await e.turns._activity(e.turn, "start.ws_1", {"name": "Untrusted frame name"})
    assert shown[0][0] == "start.ws_1" and shown[0][1]["name"] == "Fixture employee"
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
    with pytest.raises(AgentDenied):
        await e.turns._activity(e.turn, "start.ws_30", {})
    assert len(shown) == 1
    e.turn.presentation = None


@pytest.mark.parametrize("kind", ["lead_agent", "custom_agent"])
async def test_instance_startup_wording_is_not_applied_to_other_chat_kinds(kind):
    from types import SimpleNamespace

    observer = RunObserver(coordinator=None, http_owner=None, capture_attempt=None, is_current=lambda _: True)
    state = SimpleNamespace(link=SimpleNamespace(agent_kind=kind), record=SimpleNamespace(run_id="run"), progress_stage=0)

    async def forbidden(*args):
        pytest.fail("Other chat kinds cannot receive instance startup wording")

    await observer._progress(state, {"type": "turn_progress", "run_id": "run", "stage": "workspace_starting", "at_ms": 0}, forbidden)
    assert state.progress_stage == 0
