"""A periodic recovery scan must leave the foreground owner its acknowledged preview."""

import asyncio
import json
from dataclasses import asdict

import httpx
import pytest
from _telegram_compose_turn_support import _answer_every_call
from _telegram_repository_test_support import db as db
from _telegram_turn_stack import only_turn, send, stack
from sqlalchemy import select
from test_telegram_publication_observation import output_projection
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.inbox import publication_identity
from app.telegram_bots.model_source import ModelSource
from app.telegram_bots.producer import OutboxSender
from app.telegram_bots.recovery import PublicationRecoveryService
from app.telegram_bots.stopped_preview import take_shown_preview
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.runtime.runs.preview import PreviewSpan, text_digest


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["draft", "edit"])
async def test_periodic_scan_leaves_acknowledged_preview_with_registered_live_turn(db, monkeypatch, tmp_path, mode):
    s = await stack(db, monkeypatch, tmp_path, live_presentation=True)
    s.services.config = s.services.config.model_copy(update={"streaming": mode})
    s.services.model_source = ModelSource(read_output=s.services.publications.read_output)
    _answer_every_call(s.e.request, monkeypatch)
    prefix = "A visible preview"
    final = prefix + " and a durable conclusion."
    outbox = s.services.outbox
    recovery = PublicationRecoveryService(outbox.inbox, outbox, s.services.publications, db.links, lambda: True, owned=s.turns.owns_publication)

    async def deliver(**kwargs):
        return await s.e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"})

    sender = OutboxSender(outbox, run_id="active-display", deliver_part=deliver, eligibility=s.e.live.authority.delivery, jitter=lambda: 0)

    def streamed_prefix(request):
        if request.url.path.endswith("/join") and s.gateway.runs:
            publication, run = next(iter(s.gateway.runs.items()))
            row = run["row"]
            span = PreviewSpan(1, s.e.link.owner_user_id, row["thread_id"], row["run_id"], publication, "invocation", "model", "synthetic-assistant", 1, 0, text_digest(prefix), text_digest(prefix))
            data = [{"type": "AIMessageChunk", "id": span.message_id, "content": prefix}, {"deerflow_preview_span": asdict(span)}]
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=("id: 1-0\nevent: messages\ndata: " + json.dumps(data) + "\n\n").encode())
        return None

    s.gateway.fault = streamed_prefix
    read = outbox.read_display_primary
    scans = []

    async def scan_after_live_read(record, **kwargs):
        existing = await read(record, **kwargs)
        assert existing is None and s.turns.owns_publication(record, s.e.link)
        scans.append(await recovery.tick())
        return existing

    monkeypatch.setattr(outbox, "read_display_primary", scan_after_live_read)

    async def advance():
        s.e.clock.advance(1)
        await sender.tick()
        await sender.drain()
        turn = only_turn(s)
        if turn:
            turn.wake.set()
            if turn.presentation is not None:
                await turn.presentation.flush()
        await asyncio.sleep(0.03)
        return turn

    try:
        assert await send(s, 800, "Please review the guide")
        async with asyncio.timeout(30):
            while True:
                turn = await advance()
                if turn and turn.run_id and any(prefix in call.parameters.get("text", "") for call in s.e.request.calls if call.endpoint in {"sendMessageDraft", "editMessageText"}):
                    break
        record = turn.record
        shown = take_shown_preview(s.turns.observer.freeze_shown(record, turn.link), publication_identity=publication_identity(record), run_id=turn.run_id)
        assert [segment.mode for segment in shown.segments] == [mode]
        s.gateway.finish(record.publication_id, output=output_projection(record, text=final))
        async with asyncio.timeout(30):
            while not turn.done.is_set() and not any(scan.planned for scan in scans):
                await advance()
        assert len(scans) == 1 and scans[0].scanned == 1 and scans[0].planned == 0, scans
        query = select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == s.e.link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id)
        async with asyncio.timeout(30):
            while True:
                await advance()
                async with db.sessions() as reader:
                    primary = (await reader.scalars(query)).one()
                if primary.state == "sent":
                    break
        basis = primary.payload["reply_basis"]
        assert basis["version"] == 4 and basis["display"]["segments"] == [asdict(segment) for segment in shown.segments]
        [visible] = [call for call in s.e.request.calls if call.endpoint in {"sendMessage", "editMessageText"} and final in call.parameters.get("text", "")]
        if mode == "edit":
            assert visible.endpoint == "editMessageText" and visible.parameters["message_id"] == shown.segments[0].anchor_message_id
        else:
            assert visible.endpoint == "sendMessage"
    finally:
        await recovery.stop()
        await sender.stop()
        await s.close()
