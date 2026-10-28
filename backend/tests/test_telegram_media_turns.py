"""Actual SDK input/SQL/controller joins; execution observation is controlled."""

import asyncio
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from telegram import Update
from test_telegram_bot_handlers import ingress, message
from test_telegram_chat_turns import controlled_execution, until


def media(e, identity, *, caption=None, album=None, forward=False):
    payload = message(e, identity, content="Actual file").to_dict()
    saved = payload["message"]
    saved.pop("text")
    saved.pop("entities", None)
    saved["document"] = {"file_id": f"sdk-file-{identity}", "file_unique_id": f"sdk-unique-{identity}", "file_name": f"input-{identity}.pdf", "mime_type": "application/pdf", "file_size": 3}
    if caption is not None:
        saved["caption"] = caption
    if album is not None:
        saved["media_group_id"] = album
    if forward:
        saved["forward_origin"] = {"type": "hidden_user", "sender_user_name": "Original sender", "date": 1893456000}
    return Update.de_json(payload, e.channel.bot)


async def controller(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    monkeypatch.setattr(e.channel, "_admission", lambda update: SimpleNamespace(link=e.link))
    monkeypatch.setattr(e.channel, "_classified", lambda *args, **kwargs: None)
    return e, turns, state


@pytest.mark.asyncio
async def test_captioned_actual_media_enters_the_existing_turn(db, monkeypatch):
    e, turns, state = await controller(db, monkeypatch)
    try:
        assert await turns.accept(e.channel, media(e, 1801, caption="Read the actual file"))
        await until(lambda: state.posts == 1)
        turn = next(iter(turns._turns.values()))
        assert turn.inbound.text == "Read the actual file"
        assert turn.inbound.files[0]["file_id"] == "sdk-file-1801"
        assert turn.record.origins[0].message_id == 1801
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["album", "longtext", "forward"])
async def test_joining_waits_without_a_worker_then_preserves_order_and_actual_origins(db, monkeypatch, kind):
    e, turns, state = await controller(db, monkeypatch)
    try:
        if kind == "album":
            first, second = media(e, 1811, caption="Caption", album="actual-album"), media(e, 1812, album="actual-album")
        else:
            first = message(e, 1811, content="L" * 4000 if kind == "longtext" else "First material")
            second = message(e, 1812, content="Second material")
            if kind == "forward":
                converted = []
                for incoming in (first, second):
                    payload = incoming.to_dict()
                    payload["message"]["forward_origin"] = {"type": "hidden_user", "sender_user_name": "Original sender", "date": 1893456000}
                    converted.append(Update.de_json(payload, e.channel.bot))
                first, second = converted
        assert await turns.accept(e.channel, first)
        assert not turns._turns and state.posts == 0
        assert await turns.accept(e.channel, second)
        assert not turns._turns and state.posts == 0
        await turns.flush_inputs(e.channel)
        await until(lambda: state.posts == 1)
        turn = next(iter(turns._turns.values()))
        assert [row.message_id for row in turn.origins] == [1811, 1812]
        assert turn.inbound.thread_ts == "1811"
        if kind == "album":
            assert turn.inbound.text == "Caption" and [file["file_id"] for file in turn.inbound.files] == ["sdk-file-1811", "sdk-file-1812"]
        elif kind == "longtext":
            assert turn.inbound.text == "L" * 4000 + "\n\nSecond material"
        else:
            assert turn.inbound.text.count("Someone else wrote the text below.") == 2
            assert turn.inbound.text.endswith("Second material")
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_actual_turn_issues_exact_file_context_and_auxiliary_failure_plan(db, monkeypatch):
    e, turns, state = await controller(db, monkeypatch)
    try:
        assert await turns.accept(e.channel, media(e, 1821, caption="Use exactly this file"))
        await until(lambda: state.posts == 1)
        turn = next(iter(turns._turns.values()))
        file = turn.inbound.files[0]
        context = await turns.file_context(e.channel, file)
        assert context.current() is True
        assert context.record.publication_id == turn.record.publication_id
        assert context.record.scope.thread_id == "controller-original-thread"
        assert context.text == "Use exactly this file" and context.files == [file]
        assert await turns.file_context(e.channel, {**file, "file_id": "different-file"}) is None
        identity = await turns.file_notice(e.channel, context, key="file.not_saved", values={"file": file["filename"]}, retry=True)
        planned = await e.services.outbox.get(e.link, identity)
        assert planned.payload["inbound_file_basis"]["file"] == file
        assert planned.payload["product_basis"]["finish_route"] is False
        await turns.retire(e.channel)
        assert context.current() is False
        assert await turns.file_notice(e.channel, context, key="file.not_saved", values={"file": file["filename"]}, retry=True) is None
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_general_scope_flush_does_not_admit_other_topic_join(db, monkeypatch):
    e, turns, state = await controller(db, monkeypatch)
    try:
        general = message(e, 1831, content="G" * 4000)
        payload = message(e, 1832, content="T" * 4000).to_dict()
        payload["message"]["message_thread_id"] = 7
        topic = Update.de_json(payload, e.channel.bot)
        await turns.accept(e.channel, general)
        await turns.accept(e.channel, topic)
        await turns.flush_inputs(e.channel, topic=None)
        assert set(turns._turns) == {(e.link.id, None)}
        assert set(turns._joins) == {(e.link.id, 7)}
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_context_rejects_bool_size_in_place_of_actual_one_byte_file(db, monkeypatch):
    e, turns, state = await controller(db, monkeypatch)
    try:
        payload = media(e, 1841, caption="One byte").to_dict()
        payload["message"]["document"]["file_size"] = 1
        await turns.accept(e.channel, Update.de_json(payload, e.channel.bot))
        await until(lambda: state.posts == 1)
        file = next(iter(turns._turns.values())).inbound.files[0]
        assert await turns.file_context(e.channel, file) is not None
        assert await turns.file_context(e.channel, {**file, "size": True}) is None
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("timer_popped", [False, True])
async def test_captionless_album_attaches_to_actual_immediate_next_text(db, monkeypatch, timer_popped):
    from app.telegram_bots.files import PendingFiles

    e, turns, state = await controller(db, monkeypatch)
    pending = PendingFiles(e.services.callbacks, e.services.outbox.inbox, guard_factory=lambda link: None)
    e.services.pending_files = pending

    async def allowed(*args):
        return True

    monkeypatch.setattr(e.channel, "admit_file", allowed)
    entered, release, routed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    actual_route = e.services.outbox.inbox.route

    async def route(row, **kwargs):
        result = await actual_route(row, **kwargs)
        if row.message_id == 1852:
            routed.set()
        return result

    monkeypatch.setattr(e.services.outbox.inbox, "route", route)
    actual_take = pending.take

    async def take(link, row, **kwargs):
        if timer_popped and row.message_id == 1852:
            assert release.is_set(), "Later text must wait for admitted earlier album persistence"
        return await actual_take(link, row, **kwargs)

    monkeypatch.setattr(pending, "take", take)
    actual_add = pending.add

    async def add(*args, **kwargs):
        if timer_popped:
            entered.set()
            await release.wait()
        return await actual_add(*args, **kwargs)

    monkeypatch.setattr(pending, "add", add)
    flush = following = None
    try:
        await turns.accept(e.channel, media(e, 1851, album="actual-album"))
        if timer_popped:
            joined = turns._joins[(e.link.id, None)]
            flush = asyncio.create_task(turns._flush_join((e.link.id, None), joined))
            await asyncio.wait_for(entered.wait(), 15)
            following = asyncio.create_task(turns.accept(e.channel, message(e, 1852, content="Use that file")))
            await asyncio.wait_for(routed.wait(), 15)
            assert not following.done()
            release.set()
            await flush
            await following
        else:
            await turns.accept(e.channel, message(e, 1852, content="Use that file"))
        await until(lambda: state.posts == 1)
        turn = next(iter(turns._turns.values()))
        assert turn.inbound.text == "Use that file"
        assert [file["message_id"] for file in turn.inbound.files] == [1851]
    finally:
        release.set()
        for task in (flush, following):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await turns.stop()
        await e.channel.stop()
