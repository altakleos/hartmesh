"""Actual SDK acknowledgement must retain its exact observed SSE source.

SSE spans are protocol seeds over real RunObserver/SQL/SDK components; these
cases do not claim model-worker provenance, which is tested separately.
"""

import asyncio
import json
from dataclasses import asdict

import httpx
import pytest
from _telegram_live_test_support import own_view
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_bot_adapter import build, start
from test_telegram_bot_handlers import sent
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_observation import ACK, run_row
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.inbox import publication_identity
from app.telegram_bots.producer import OutboxSender
from app.telegram_bots.progress import RunObserver
from app.telegram_bots.stopped_preview import take_shown_preview
from app.telegram_bots.streaming import LivePresentation
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.runtime.runs.preview import PreviewSpan, text_digest


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["draft", "edit"])
async def test_new_preview_during_actual_send_cannot_replace_acknowledged_source(coordinator_environment, mode):
    e = coordinator_environment
    ui = await build(e.db, link=e.link, connected=False)
    ui.services.config = e.db.config.model_copy(update={"streaming": mode})
    ui.services.outbox, e.outbox.clock = e.outbox, ui.clock.now
    ui.services.callbacks = CallbackRepository(e.db.sessions, config=e.db.config)
    ui.services.prepare_live_owner = e.live.authority.prepare_live_owner

    async def values(_link):
        return {"product": "HartMesh", "name": "Chat", "agent": "Chat"}

    ui.services.prepare_ingress_values = values
    e.current[0] = ui.channel
    await start(ui)
    async with e.db.sessions.begin() as session:
        await session.execute(update(TelegramOutboxRow).values(state="skipped", skip_reason="superseded"))
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = {**run_row(record), "metadata": {"channel_turn_id": record.publication_id}}
    frames = []
    for sequence, text in enumerate(("AAA first span", "BBB other span"), 1):
        span = PreviewSpan(1, e.link.owner_user_id, e.thread_id, "coordinator-run", record.publication_id, "invocation", "model-" + str(sequence), "message-" + str(sequence), sequence, 0, text_digest(text), text_digest(text))
        payload = [{"type": "AIMessageChunk", "id": span.message_id, "content": text}, {"deerflow_preview_span": asdict(span)}]
        frames.append(f"id: {sequence}-0\nevent: messages\ndata: " + json.dumps(payload) + "\n\n")

    async def respond(request):
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content="".join(frames).encode())
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    view = LivePresentation(channel=ui.channel, link=e.link, draft_id=701, topic=None, is_current=lambda: True, observer=observer, origins=e.origins)
    own_view(view)
    sender = OutboxSender(e.outbox, run_id="live-source-race", deliver_part=lambda **kw: ui.channel.send_part(**{key: value for key, value in kw.items() if key != "link_id"}), eligibility=e.live.authority.delivery)
    release, flushing, previews = asyncio.Event(), None, []

    async def preview(value):
        nonlocal flushing
        previews.append(value)
        await view.preview(value)
        if len(previews) == 1:
            ui.clock.advance(1)
            endpoint = "sendMessageDraft" if mode == "draft" else "editMessageText"
            held = ui.request.enqueue(endpoint, result=True if mode == "draft" else sent(ui, identity=7011), release=release)
            flushing = asyncio.create_task(view.flush())
            await asyncio.wait_for(held.started.wait(), 5)
        else:
            assert not flushing.done()
            release.set()
            await flushing

    async def activity(_key, _values):
        return None

    try:
        if mode == "draft":
            ui.request.enqueue("sendMessageDraft")
        await view.start()
        if mode == "edit":
            ui.request.enqueue("sendMessage", result=sent(ui, identity=7011))
            await sender.tick()
            await sender.drain()
        await observer.watch(record, e.link, on_preview=preview, on_line=activity, turn_done=asyncio.Event())
        shown = take_shown_preview(observer.freeze_shown(record, e.link), publication_identity=publication_identity(record), run_id="coordinator-run")
        assert len(shown.segments) == 1
        segment = shown.segments[0]
        assert (segment.span_id, segment.message_id, segment.text) == ("model-1", "message-1", "AAA first span")
        assert view._dirty is True, "the newer B preview must remain queued for the next flush"
    finally:
        release.set()
        if flushing is not None:
            await asyncio.gather(flushing, return_exceptions=True)
        await view.close()
        await observer.drain()
        await sender.stop()
        await ui.channel.stop()
