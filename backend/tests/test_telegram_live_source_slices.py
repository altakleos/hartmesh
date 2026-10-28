"""Recorded SSE provenance seeds exercise real observer, SQL and pinned Bot SDK.

Actual worker provenance is covered separately by the real model callback suite.
These checks qualify the presentation acknowledgement and raw-slice boundary.
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
from app.telegram_bots.producer import OutboxSender
from app.telegram_bots.progress import RunObserver
from app.telegram_bots.stopped_preview import take_shown_preview
from app.telegram_bots.streaming import LivePresentation
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.runtime.runs.preview import PreviewSpan, text_digest


@pytest.mark.asyncio
async def test_edit_split_acknowledges_exact_raw_slices_and_never_resends_prefix(coordinator_environment):
    e = coordinator_environment
    ui = await build(e.db, link=e.link, connected=False)
    ui.services.config = e.db.config.model_copy(update={"streaming": "edit"})
    ui.services.outbox = e.outbox
    # Sender pacing and UI pacing share one explicit fixture clock. A fast
    # backend must not depend on real wall time passing between SQL calls.
    e.outbox.clock = ui.clock.now
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
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}
    raw = "A" * 3500 + "last"
    span = PreviewSpan(1, e.link.owner_user_id, e.thread_id, "coordinator-run", record.publication_id, "invocation", "model", "message", 1, 0, text_digest(raw), text_digest(raw))
    payload = [{"type": "AIMessageChunk", "id": "message", "content": raw}, {"deerflow_preview_span": asdict(span)}]

    async def respond(request):
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=("id: 1-0\nevent: messages\ndata: " + json.dumps(payload) + "\n\n").encode())
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    view = LivePresentation(channel=ui.channel, link=e.link, draft_id=10701, topic=None, is_current=lambda: True, observer=observer, origins=e.origins)
    own_view(view)
    sender = OutboxSender(e.outbox, run_id="live-raw-slices", deliver_part=lambda **kw: ui.channel.send_part(**{key: value for key, value in kw.items() if key != "link_id"}), eligibility=e.live.authority.delivery)

    async def tick(identity):
        ui.request.enqueue("sendMessage", result=sent(ui, identity=identity))
        await sender.tick()
        await sender.drain()

    async def preview(value):
        await view.preview(value)
        ui.clock.advance(1)
        ui.request.enqueue("editMessageText", result=sent(ui, identity=7011))
        await view.flush()
        await tick(7012)
        ui.clock.advance(1)
        ui.request.enqueue("editMessageText", result=sent(ui, identity=7012))
        await view.flush()

    async def activity(_key, _values):
        return None

    try:
        await view.start()
        await tick(7011)
        first = next(call for call in ui.request.calls if call.endpoint == "sendMessage")
        assert first.parameters["reply_parameters"]["message_id"] == e.origins[0].message_id
        await observer.watch(record, e.link, on_preview=preview, on_line=activity, turn_done=asyncio.Event())
        edits = [call for call in ui.request.calls if call.endpoint == "editMessageText"]
        assert [(call.parameters["message_id"], call.parameters["text"]) for call in edits] == [(7011, "A" * 3500), (7012, "last")]
        assert edits[0].parameters["reply_markup"] == {"inline_keyboard": []}
        assert edits[1].parameters["reply_markup"]["inline_keyboard"][0][0]["callback_data"].startswith("h1:")
        from app.telegram_bots.inbox import publication_identity

        shown = take_shown_preview(observer.freeze_shown(record, e.link), publication_identity=publication_identity(record), run_id="coordinator-run")
        assert [(part.source_start, part.source_end, part.anchor_message_id) for part in shown.segments] == [(0, 3500, 7011), (3500, 3504, 7012)]
    finally:
        await view.close()
        await observer.drain()
        await sender.stop()
        await ui.channel.stop()
