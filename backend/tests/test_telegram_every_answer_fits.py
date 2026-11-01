"""An answer's shape cannot stop it arriving: it is rendered once and then cut, and a table always draws."""

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from _telegram_turn_stack import mark, only_turn, output_projection, past_the_limit, planned_keys, restart, send, stack, until
from sqlalchemy import select
from test_telegram_media_turns import controller, media
from test_telegram_publication_custody import publication_environment as publication_environment
from test_telegram_publication_primary import _managed_turn, _primary
from test_telegram_streamed_primary import cancelled, shown
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_verified_reply import projection

from app.telegram_bots.inbox import publication_identity
from app.telegram_bots.markdown import to_html
from app.telegram_bots.model_source import ModelSource, prepare_display_reply
from app.telegram_bots.producer import Outbox, OutboxSender, SendingPart, SourceGone, SourceNotRead, _item
from app.telegram_bots.split import utf16_units
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow

MODEL_TEXT = {"kind": "model_text", "stop_id": None, "segments": [], "waiting": False}


def table(columns, rows, *, cell="value"):
    head = "| " + " | ".join(f"Column {index}" for index in range(columns)) + " |"
    rule = "| " + " | ".join("---" for _ in range(columns)) + " |"
    return "\n".join([head, rule, *("| " + " | ".join(f"{cell} {row}" for _ in range(columns)) + " |" for row in range(rows))])


SHAPES = {
    "four columns": table(4, 3),
    "five columns many rows": table(5, 200),
    "three columns with a long note": table(3, 30, cell="a note that runs on for quite a while " * 3),
    "a hundred and twenty rows": table(3, 120),
    "one long code block": "```python\n" + "def total(rows):\n    return sum(rows)\n\n" * 400 + "```",
    "a long tight list": "\n".join(f"{index}. An item of a long list that goes on" for index in range(1, 300)),
    "reference links": "\n\n".join(f"[guide {index}][{index}]" for index in range(200)) + "\n\n" + "\n".join(f"[{index}]: https://example.test/{index}" for index in range(200)),
    "ordinary prose": "A sentence that is a perfectly ordinary length.\n\n" * 200,
    "chinese": "一句普通长度的中文句子，用来测试切分。\n\n" * 300,
    "a picture with no description": "![](https://example.test/chart.png)",
}


async def turn_record(e):
    return replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", SHAPES)
async def test_every_shape_of_answer_becomes_messages_that_fit(publication_environment, shape):
    text = SHAPES[shape]
    record = await turn_record(publication_environment)
    _basis, parts = prepare_display_reply(record, projection(record, text=text), display=MODEL_TEXT)
    shown_text = [part["visible_text"] for part in parts]
    assert all(0 < utf16_units(value) <= 4096 and value.strip() for value in shown_text)
    assert "".join(shown_text) == to_html(text).plain, "Nothing is lost at a cut"
    assert not any("```" in value or value.lstrip().startswith("|") for value in shown_text), "Nothing shows as raw Markdown"
    assert all(part["html"].count("<pre>") == part["html"].count("</pre>") for part in parts)


@pytest.mark.asyncio
async def test_every_part_of_a_long_answer_validates_when_it_is_sent(publication_environment):
    e = publication_environment
    record = await turn_record(e)
    text = SHAPES["one long code block"]
    output = projection(record, text=text)
    identity = await Outbox(e.sessions).plan_streamed_publication_reply(record, output=output, shown=shown(record, empty=True), guard=e.guard_factory())
    row = await _primary(e.db, identity)
    assert len(row.parts) > 1
    source = ModelSource(read_output=AsyncMock(return_value=output))
    ready = [await source.read_part(SendingPart(e.link, _item(row), part, "process", "c" * 32, e.link.token_revision)) for part in row.parts]
    assert "".join(part.plain_text for part in ready) == to_html(text).plain


@pytest.mark.asyncio
async def test_a_stopped_answer_shows_the_formatting_it_had_and_fits(publication_environment):
    from app.telegram_bots.stopped_preview import StoppedSegment, _issue_stopped_preview
    from deerflow.runtime.runs.preview import text_digest

    e = publication_environment
    record = await turn_record(e)
    record = await e.inbox.request_publication_stop(record, stop_id="original-stop", guard=e.guard_factory())
    text = "**plan** and [invoice](https://example.test/inv)"
    segment = StoppedSegment("model", "synthetic-assistant", 1, text, text_digest(text), "edit", 701, None, 0, "shown-first", 0, len(text))
    preview = _issue_stopped_preview(publication_identity=publication_identity(record), run_id=record.run_id, segments=(segment,), check_current=lambda: True, stop_id="original-stop")
    identity = await Outbox(e.sessions).plan_stopped_publication_reply(record, output=cancelled(record), preview=preview, guard=e.guard_factory(), waiting=False)
    row = await _primary(e.db, identity)
    assert row.parts[0]["html"] == '<b>plan</b> and <a href="https://example.test/inv">invoice</a>'


@pytest.mark.asyncio
async def test_a_blank_stopped_segment_sends_nothing(publication_environment):
    from app.telegram_bots.stopped_preview import StoppedSegment, _issue_stopped_preview
    from deerflow.runtime.runs.preview import text_digest

    e = publication_environment
    record = await turn_record(e)
    record = await e.inbox.request_publication_stop(record, stop_id="original-stop", guard=e.guard_factory())
    text = "   \n\n"
    segment = StoppedSegment("model", "synthetic-assistant", 1, text, text_digest(text), "edit", 701, None, 0, "shown-first", 0, len(text))
    preview = _issue_stopped_preview(publication_identity=publication_identity(record), run_id=record.run_id, segments=(segment,), check_current=lambda: True, stop_id="original-stop")
    identity = await Outbox(e.sessions).plan_stopped_publication_reply(record, output=cancelled(record), preview=preview, guard=e.guard_factory(), waiting=False)
    row = await _primary(e.db, identity)
    assert [part["source_ref"] for part in row.parts] == [{"key": "stop.answer", "values": {"written": False, "waiting": False}}]


def a_token_sample():
    return "A sample looks like 123456789:" + "A" * 35


async def assert_on_web(s):
    assert await planned_keys(s) == ["answer.on_web"]
    async with s.db.sessions() as session:
        reply = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("reply:")))
        tries = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message")))
    assert reply.parts[0]["url_button"] == {"page": "web.conversation", "url": "https://hartmesh.example/chat"}
    assert tries == [], "Trying again would end the same way"


@pytest.mark.asyncio
async def test_an_answer_no_chat_can_carry_says_it_is_on_the_web_and_goes_out(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    monkeypatch.setattr("app.telegram_bots.pages.page_address", lambda page, link, thread_id=None: "https://hartmesh.example/chat")
    e = s.e
    try:
        assert await send(s, 11001, "First") is True
        turn = only_turn(s)
        await until(lambda: turn.run_id is not None)
        s.gateway.finish(turn.record.publication_id, output=output_projection(turn.record, text=a_token_sample()))
        await until(lambda: turn.quiet_since is not None)
        await past_the_limit(s, turn)
        await until(lambda: not s.turns._turns)
        await assert_on_web(s)
        assert (await mark(s, 11001)).route_complete
        async with s.db.sessions.begin() as session:
            for row in await session.scalars(select(TelegramOutboxRow)):
                if not row.dedupe_key.startswith("reply:"):
                    row.state, row.closed_at = "sent", e.clock.now()

        async def deliver(**kwargs):
            return await e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"})

        sender = OutboxSender(s.services.outbox, run_id="test-sender", deliver_part=deliver, eligibility=e.live.authority.delivery, jitter=lambda: 0)
        e.request.enqueue("sendMessage", result={"message_id": 900, "date": 1_893_456_000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "x"})
        await sender.tick()
        await sender.drain()
        sent = next(call for call in e.request.calls if call.endpoint == "sendMessage")
        assert sent.parameters["text"] == "This answer cannot be shown here. It is in HartMesh on the web."
        assert sent.parameters["reply_markup"]["inline_keyboard"][0][0]["url"] == "https://hartmesh.example/chat"
        await sender.stop()
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_a_run_that_wrote_no_answer_is_not_said_to_be_on_the_web(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 11005, "First") is True
        turn = only_turn(s)
        await until(lambda: turn.run_id is not None)
        s.gateway.finish(turn.record.publication_id, output=output_projection(turn.record, failed=True) | {"output": {**output_projection(turn.record, failed=True)["output"], "run_status": "success", "reason": "unsupported_output"}})
        await until(lambda: turn.quiet_since is not None)
        await past_the_limit(s, turn)
        await until(lambda: not s.turns._turns)
        assert await planned_keys(s) == ["error.generic"]
        async with s.db.sessions() as session:
            tries = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message")))
        assert len(tries) == 1, "There is nothing to open, so the way on is to try again"
    finally:
        await s.close()


@pytest.mark.asyncio
async def test_an_answer_no_chat_can_carry_ends_on_the_web_with_no_live_turn_too(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    monkeypatch.setattr("app.telegram_bots.pages.page_address", lambda page, link, thread_id=None: "https://hartmesh.example/chat")
    recovery = None
    try:
        assert await send(s, 11011, "First") is True
        turn = only_turn(s)
        await until(lambda: turn.run_id is not None)
        record = turn.record
        s.gateway.finish(record.publication_id, output=output_projection(record, text=a_token_sample()))
        recovery = await restart(s)
        assert (await recovery.tick()).planned == 1
        await assert_on_web(s)
    finally:
        if recovery is not None:
            await recovery.stop()
        await s.close()


@pytest.mark.parametrize("gone", [True, False])
@pytest.mark.asyncio
async def test_a_saved_part_that_no_longer_validates_is_gone_and_not_retried_for_good(publication_environment, gone):
    e = publication_environment
    record = await turn_record(e)
    text = "A short answer."
    output = projection(record, text=text)
    identity = await Outbox(e.sessions).plan_streamed_publication_reply(record, output=output, shown=shown(record, empty=True), guard=e.guard_factory())
    row = await _primary(e.db, identity)
    item = _item(row)
    if gone:
        item.parts[0]["html"] = "Rendered by something else"
        raised = SourceGone
    else:
        raised = SourceNotRead
    read = AsyncMock(return_value=output) if gone else AsyncMock(side_effect=OSError("blip"))
    with pytest.raises(raised):
        await ModelSource(read_output=read).read_part(SendingPart(e.link, item, item.parts[0], "process", "c" * 32, e.link.token_revision))


def test_the_live_preview_of_a_growing_table_does_not_step_back():
    from types import SimpleNamespace

    from app.telegram_bots.streaming import LivePresentation

    table_text = "| Customer name | Contact person | A note |\n| --- | --- | --- |\n" + "".join(f"| Customer {row} | Person {row} | Something to say about them |\n" for row in range(300))
    shown_before = 0
    for size in range(400, len(table_text), 200):
        _start, end, _raw = LivePresentation._fit(SimpleNamespace(_activity=""), 0, size, table_text[:size])
        assert end >= shown_before - 200, "A row shown stays shown"
        shown_before = max(shown_before, end)


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["file.too_large_out", "file.out_failed", "file.not_saved", "file.own_full", "file.own_no_seat", "file.own_unavailable", "answer.on_web"])
async def test_every_file_warning_renders_from_what_is_saved(db, monkeypatch, key):
    from test_telegram_bot_handlers import ingress

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    try:
        text = await e.channel._render_source(e.channel.link, {"key": key, "values": {"file": "export.zip"} if key != "answer.on_web" else {}})
        assert text.strip() and "{" not in text
    finally:
        await e.channel.stop()


async def notices(db):
    async with db.sessions() as session:
        rows = list(await session.scalars(select(TelegramOutboxRow)))
    return [part["source_ref"] for row in rows for part in row.parts if part.get("source_ref", {}).get("key", "").startswith("file.saved_ask")]


@pytest.mark.asyncio
async def test_a_captionless_album_is_asked_about_once(db, monkeypatch):
    e, turns, state = await controller(db, monkeypatch)
    e.services.pending_files = AsyncMock()
    monkeypatch.setattr(e.channel, "admit_file", AsyncMock(return_value=True), raising=False)
    try:
        for identity in (11101, 11102, 11103):
            assert await turns.accept(e.channel, media(e, identity, album="actual-album", kind="photo"))
        await turns.flush_inputs(e.channel)
        assert await notices(db) == [{"key": "file.saved_ask_many", "values": {"n": 3}}]
        assert state.posts == 0
        assert e.services.pending_files.add.await_count == 3
        async with db.sessions() as session:
            marks = {identity: await session.get(TelegramInboundMarkRow, (e.link.id, identity, e.link.chat_id)) for identity in (11101, 11102, 11103)}
        assert all(row.route_complete for row in marks.values()), "One question answers for every photo"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind, source", [("photo", {"key": "file.saved_ask_photo", "values": {}}), ("document", {"key": "file.saved_ask_plain", "values": {"file": "input-11201.pdf"}})])
async def test_one_file_is_named_only_when_it_has_a_name(db, monkeypatch, kind, source):
    e, turns, _state = await controller(db, monkeypatch)
    e.services.pending_files = AsyncMock()
    monkeypatch.setattr(e.channel, "admit_file", AsyncMock(return_value=True), raising=False)
    try:
        assert await turns.accept(e.channel, media(e, 11201, kind=kind))
        await turns.flush_inputs(e.channel)
        assert await notices(db) == [source]
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_file_refused_in_an_album_leaves_the_others_their_question(db, monkeypatch):
    e, turns, _state = await controller(db, monkeypatch)
    e.services.pending_files = AsyncMock()
    answers = iter([False, True, True])
    monkeypatch.setattr(e.channel, "admit_file", AsyncMock(side_effect=lambda *_args: next(answers)), raising=False)
    try:
        for identity in (11301, 11302, 11303):
            assert await turns.accept(e.channel, media(e, identity, album="another-album", kind="photo"))
        await turns.flush_inputs(e.channel)
        assert await notices(db) == [{"key": "file.saved_ask_many", "values": {"n": 2}}]
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_an_upload_waits_long_enough_for_telegram_to_take_a_large_file():
    from _telegram_bot_test_support import RecordingRequest, offline_bot

    from app.telegram_bots.adapter import BoundedRateLimiter
    from app.telegram_bots.outbound_upload import UPLOAD_READ_SECONDS, upload_output

    request = RecordingRequest()
    request.enqueue_get_me()
    request.enqueue("sendDocument", result={"message_id": 10, "date": 1, "chat": {"id": 7000000001, "type": "private"}})
    seen = []
    actual = request.do_request

    async def do_request(url, method, request_data=None, **kwargs):
        seen.append(kwargs.get("read_timeout"))
        return await actual(url, method, request_data, **kwargs)

    request.do_request = do_request
    bot = offline_bot(request, rate_limiter=BoundedRateLimiter())

    async def current():
        return None

    async with bot:
        await upload_output(bot, kind="document", files=(("big.bin", b"abc"),), chat_id=7000000001, message_thread_id=None, reply_to_message_id=None, silent=False, protect_content=False, caption=None, plain=False, before_dispatch=current)
    assert seen[-1] == UPLOAD_READ_SECONDS
