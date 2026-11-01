"""A press gets an answer, unless it repeats a press that worked, and spent buttons leave the message.

Real pinned Application, real SQL, real handlers. Only the Bot API transport is recorded, and where
a turn is needed, the Gateway reads.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from _telegram_human_input_test_support import request
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Update
from test_telegram_bot_handlers import ingress, message, sent
from test_telegram_callback_commands import drain, press
from test_telegram_chat_turns import controlled_execution, terminal, until
from test_telegram_chat_turns import message as chat_message
from test_telegram_human_input_sdk import ui as ui
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots import streaming
from app.telegram_bots.callbacks import CallbackRepository, Prompts
from app.telegram_bots.commands import CallbackCommands
from app.telegram_bots.inbox import InboxRepository
from app.telegram_bots.producer import OutboxSender
from app.telegram_bots.retention import sweep_rows
from app.telegram_bots.threads import ThreadCoordinator, ThreadPreparationFailure
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

REQUIRED_CHECKBOX = {"name": "agree", "label": "I agree to the terms", "type": "checkbox", "required": True}
COLOURS = {"name": "colours", "label": "Colours", "type": "multi_select", "required": True, "options": [{"id": name, "label": name.title(), "value": name} for name in ("red", "green", "blue")]}
NETWORK = {
    "source": "sandbox_network",
    "input_mode": "single_choice",
    "details": {"host": "example.test", "port": 443, "ttl_seconds": 300},
    "options": [{"id": "allow_temporary", "label": "Allow", "value": "allow_temporary"}, {"id": "deny", "label": "Deny", "value": "deny"}],
}
CARD = 701
REQUIRED = "This one is required, so the form cannot go on without it."
NOTHING_WAITING = "Nothing is waiting on this any more."


def form(*fields):
    raw = request(version=2, input_mode="form", fields=list(fields))
    raw.pop("options")
    return raw


async def show(e, raw):
    await e.offer(raw)
    e.request.enqueue("sendMessage", result=sent(e, identity=CARD))
    await drain(e)


async def rows(e, kind):
    async with e.services.outbox.sessions() as session:
        return list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind).order_by(TelegramCallbackRow.created_at)))


async def live(e, kind, option=None):
    """The newest live button of a kind, as on screen."""
    found = [row for row in await rows(e, kind) if row.consumed_at is None and (option is None or row.payload.get("option_id") == option)]
    return found[-1].id


async def tap(e, update_id, callback, *, message_id=CARD):
    e.request.enqueue("answerCallbackQuery")
    assert await e.channel.handle_update(press(e, update_id, callback, message_id=message_id))
    await asyncio.gather(*tuple(e.channel._ingress_tasks))


def alerts(e):
    return [call.parameters.get("text") for call in e.request.calls if call.endpoint == "answerCallbackQuery"]


def said(e, after=0):
    return [call.parameters.get("text") for call in e.request.calls[after:] if call.endpoint == "sendMessage"]


async def the_form(e):
    return (await rows(e, "form"))[0].payload


async def spend(e, kind):
    row = await e.services.callbacks.get(await live(e, kind))
    link, _item, _source, guard = await e.services.human_inputs._callback_source(e.channel, row)
    await e.services.callbacks.consume_group(row.group_id, link=link, guard=guard)


def quoted(e, identity, content, target):
    payload = message(e, identity, content=content).to_dict()
    payload["message"]["reply_to_message"] = sent(e, identity=target)
    return Update.de_json(payload, e.channel.bot)


@pytest.mark.asyncio
async def test_a_press_the_pause_covers_gets_its_alert(ui):
    e = ui
    await show(e, request(**NETWORK))
    await e.services.links.update(e.link.id, acts_paused_since=datetime.now(UTC))
    await e.channel.refresh_link()
    await tap(e, 1700, await live(e, "network", "allow_temporary"))
    assert alerts(e)[-1].startswith("Paused on this bot"), "It used to crash on {product}, leaving the button spinning"


@pytest.mark.asyncio
async def test_no_on_a_required_checkbox_says_why_and_the_form_waits(ui):
    e = ui
    await show(e, form(REQUIRED_CHECKBOX))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await tap(e, 1710, await live(e, "form_option", "no"))
    await drain(e)
    assert said(e)[-1] == REQUIRED
    assert (await the_form(e))["state"] == "field" and await live(e, "form_option", "yes")


@pytest.mark.asyncio
async def test_done_on_an_empty_required_list_says_why(ui):
    e = ui
    await show(e, form(COLOURS))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await tap(e, 1711, await live(e, "form_next"))
    await drain(e)
    assert said(e)[-1] == REQUIRED


@pytest.mark.asyncio
async def test_a_press_that_failed_says_so_and_its_button_stays(ui, monkeypatch):
    e = ui
    await show(e, request())
    callback = await live(e, "clarify_option")
    monkeypatch.setattr(e.services.human_inputs, "execute_callback", AsyncMock(side_effect=OSError("synthetic")))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await tap(e, 1712, callback)
    await drain(e)
    assert said(e)[-1] == "That did not go through. Tap again."
    assert (await e.services.callbacks.get(callback)).consumed_at is None


def in_flight_together(e, monkeypatch):
    """Both presses pass their checks with the row as it was, and then run one after the other.

    That is what two presses in flight do: the checks read the row, and the writer serialises the rest.
    """
    real = e.services.human_inputs.execute_callback
    arrived, both, finished = [], asyncio.Event(), [asyncio.Event(), asyncio.Event()]

    async def gated(channel, row, *, update_id, query_id=None):
        index = len(arrived)
        arrived.append(row.id)
        if len(arrived) == 2:
            both.set()
        await both.wait()
        if index == 1:
            await finished[0].wait()
        try:
            return await real(channel, row, update_id=update_id, query_id=query_id)
        finally:
            finished[index].set()

    monkeypatch.setattr(e.services.human_inputs, "execute_callback", gated)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw, kind, second",
    [
        (request(), "clarify_option", "option-1"),
        (request(options=[{"id": "option-1", "label": "First", "value": "First"}, {"id": "option-2", "label": "Second", "value": "Second"}]), "clarify_option", "option-2"),
        (request(**NETWORK), "network", "deny"),
    ],
    ids=["same answer", "another answer", "blocked site"],
)
async def test_a_second_press_while_the_first_goes_in_stays_silent(ui, monkeypatch, raw, kind, second):
    e = ui
    await show(e, raw)
    option = "deny" if kind == "network" else "option-1"
    first, other = await live(e, kind, option), await live(e, kind, second)
    in_flight_together(e, monkeypatch)
    before = len(e.request.calls)
    for update_id, callback in ((1713, first), (1714, other)):
        e.request.enqueue("answerCallbackQuery")
        assert await e.channel.handle_update(press(e, update_id, callback, message_id=CARD))
    await asyncio.gather(*tuple(e.channel._ingress_tasks))
    await drain(e)
    assert said(e, before) == [] and e.services.turns.accept_staged.await_count == 1, "The answer went through, and nothing says otherwise"


@pytest.mark.asyncio
async def test_a_saved_answer_that_failed_on_its_way_in_does_not_ask_for_a_tap_on_a_gone_keyboard(ui):
    e = ui
    await show(e, request())
    e.services.turns.accept_staged = AsyncMock(side_effect=OSError("synthetic"))
    e.request.enqueue("editMessageText", result=sent(e, identity=CARD))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await tap(e, 1715, await live(e, "clarify_option"))
    await drain(e)
    await drain(e)
    assert said(e)[-1] == "Something went wrong and the message was not answered. It is kept."


def conversation_deleted(e, *, status=404):
    """What the Gateway really answers: `read_request` never returns None for a deleted conversation.

    The run's read is refused for want of an acknowledgement, the same as when the Gateway is failing.
    Only the thread's own read tells the two apart, so `status` is what that read says.
    """
    transport = e.services.thread_client._http_client._transport
    real = transport.handler

    async def respond(incoming):
        if incoming.url.path.endswith("/output"):
            return httpx.Response(404, json={"detail": "Run output is unavailable"})
        if incoming.url.path.endswith("/request-thread"):
            return httpx.Response(status, json={"detail": "Thread not found"})
        result = real(incoming)
        return await result if asyncio.iscoroutine(result) else result

    transport.handler = respond


async def card_buttons_left(e):
    return [row for kind in ("clarify_option", "clarify_other") for row in await rows(e, kind) if row.consumed_at is None]


@pytest.mark.asyncio
async def test_a_card_whose_conversation_is_gone_says_nothing_is_waiting(ui):
    e = ui
    await show(e, request())
    callback = await live(e, "clarify_option")
    conversation_deleted(e)
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await tap(e, 1716, callback)
    await drain(e)
    assert said(e)[-1] == NOTHING_WAITING, "Tapping again could never work"


@pytest.mark.asyncio
async def test_a_card_while_the_gateway_is_failing_can_be_tapped_again(ui):
    e = ui
    await show(e, request())
    callback = await live(e, "clarify_option")
    conversation_deleted(e, status=503)
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await tap(e, 1718, callback)
    await drain(e)
    assert said(e)[-1] == "That did not go through. Tap again."
    assert (await e.services.callbacks.get(callback)).consumed_at is None


@pytest.mark.asyncio
async def test_a_typed_answer_to_a_prompt_whose_conversation_is_gone_is_told_so(ui):
    e = ui
    await show(e, form({"name": "note", "label": "Note", "type": "text", "required": True}))
    conversation_deleted(e)
    assert await e.services.human_inputs.handle_text(e.channel, message(e, 1719, content="My note"), captured_link=e.channel.link), "It used to raise, and hold up the chat's updates"
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await drain(e)
    assert e.request.calls[-1].parameters["text"].startswith("This prompt can no longer take an answer")


@pytest.mark.asyncio
async def test_a_command_closes_a_card_whose_conversation_is_gone(ui):
    e = ui
    await show(e, request())
    conversation_deleted(e)
    await e.services.human_inputs.command(e.channel, message(e, 1721, content="/stop"), "stop")
    assert not await card_buttons_left(e), "Nothing else would ever close it"


@pytest.mark.asyncio
async def test_a_new_message_closes_a_card_whose_conversation_is_gone(ui):
    e = ui
    await show(e, request())
    conversation_deleted(e)
    origin, _ = await e.services.outbox.inbox.accept(e.link, message(e, 1722, content="A new question"), sanitized_input={"type": "text", "text": "A new question"})
    await e.services.human_inputs.before_plain_publication(e.channel, e.link, [origin])
    assert not await card_buttons_left(e), "It used to raise, for every message after it"


@pytest.mark.asyncio
async def test_a_press_from_a_link_that_changed_says_nothing_here(ui, monkeypatch):
    e = ui
    await show(e, request())
    callback = await live(e, "clarify_option")

    async def moved_on(*args, **kwargs):
        monkeypatch.setattr(e.channel, "_callback_current", lambda link: False)
        raise OSError("synthetic")

    monkeypatch.setattr(e.services.human_inputs, "execute_callback", moved_on)
    before = len(e.request.calls)
    await tap(e, 1717, callback)
    await drain(e)
    assert said(e, before) == []


async def live_turn(db, monkeypatch, *, identity, real_view=False):
    """A chat message whose run is under way."""
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    view = streaming.LivePresentation
    turns, state, reached = await controlled_execution(e, monkeypatch)
    if real_view:
        monkeypatch.setattr(streaming, "LivePresentation", view)
        e.services.protect_notice = AsyncMock(return_value=True)
    e.services.callbacks = CallbackRepository(db.sessions, config=db.config)
    e.services.prompts = Prompts(e.services.callbacks)
    e.services.commands = CallbackCommands(e.services)
    e.services.eligibility = e.live.authority.delivery
    e.services.threads.ensure_callback_thread = AsyncMock(return_value="controller-original-thread")
    sender = OutboxSender(e.services.outbox, run_id="every-press", deliver_part=lambda **kw: e.channel.send_part(**{key: value for key, value in kw.items() if key != "link_id"}), eligibility=e.services.eligibility)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state, row.skip_reason = "skipped", "superseded"
    await e.channel.handle_update(chat_message(e, identity, content="Work on this"))
    await asyncio.wait_for(reached.wait(), 5)
    await until(lambda: state.posts == 1)

    async def settle():
        for _ in range(3):
            e.clock.advance(2)
            await sender.tick()
            await sender.drain()

    return SimpleNamespace(e=e, turns=turns, state=state, settle=settle, first=next(iter(turns._turns.values())))


async def failed_turn(db, monkeypatch, *, identity):
    """The same, after its run failed, so that its answer carries [Try again]."""
    world = await live_turn(db, monkeypatch, identity=identity)
    e = world.e
    world.state.terminal = terminal(world.first.record, status="error", reason=None)
    world.first.wake.set()
    await until(world.first.done.is_set)
    await asyncio.sleep(0.2)
    for number in range(3):
        e.request.enqueue("sendMessage", result=sent(e, identity=1500 + number))
    await world.settle()
    async with db.sessions() as session:
        retry = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message"))
    world.retry_id, world.retry_message = retry.id, retry.message_id
    return world


async def cleanup_targets(db):
    async with db.sessions() as session:
        return [row.parts[0]["target_message_id"] for row in await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["kind"].as_string() == "keyboard_cleanup"))]


@pytest.mark.asyncio
async def test_try_again_after_its_message_was_swept_says_nothing_is_waiting(db, monkeypatch):
    world = await failed_turn(db, monkeypatch, identity=2301)
    e = world.e
    try:
        await sweep_rows(db.sessions, datetime.now(UTC) + timedelta(days=8), callback_max_age_days=180)
        e.request.enqueue("sendMessage", result=sent(e, identity=1600))
        await tap(e, 2302, world.retry_id, message_id=world.retry_message)
        await world.settle()
        assert said(e)[-1] == NOTHING_WAITING, "Tapping again could never have worked"
    finally:
        await world.turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_try_again_that_ended_before_a_run_takes_its_button_away(db, monkeypatch):
    world = await failed_turn(db, monkeypatch, identity=2311)
    e = world.e
    try:
        e.services.threads.ensure_callback_thread = AsyncMock(side_effect=ThreadPreparationFailure("files_full"))
        world.state.terminal = None
        await tap(e, 2312, world.retry_id, message_id=world.retry_message)
        await until(lambda: not world.turns._turns)
        await asyncio.sleep(0.2)
        assert await cleanup_targets(db) == [world.retry_message]
    finally:
        await world.turns.stop()
        await e.channel.stop()


@pytest.mark.parametrize("kind, cleared", [("retry_message", True), ("file_retry", True), ("prompt_send_new", True), ("clarify_option", False), ("form_send", False), ("network", False)])
def test_only_buttons_nothing_else_replaces_are_taken_away(kind, cleared):
    row = SimpleNamespace(kind=kind, consumed_at=None, closed_reason=None, message_id=1902, link_id="link", group_id="a" * 32, binding_generation=1, chat_id=7, message_thread_id=None)
    added = []
    InboxRepository._spend_group(SimpleNamespace(add=added.append), row, [row], owner_user_id="owner")
    assert row.consumed_at is not None and [saved.parts[0]["target_message_id"] for saved in added] == ([1902] if cleared else []), "A question's own view replaces its keyboard"


def test_a_group_spent_twice_plans_one_cleanup():
    row = SimpleNamespace(kind="retry_message", consumed_at=None, closed_reason=None, message_id=1902, link_id="link", group_id="a" * 32, binding_generation=1, chat_id=7, message_thread_id=None)
    added = []
    session = SimpleNamespace(add=added.append)
    InboxRepository._spend_group(session, row, [row], owner_user_id="owner")
    InboxRepository._spend_group(session, row, [row], owner_user_id="owner")
    assert len(added) == 1, "Confirming a dispatch and binding its run both arrive here"


async def stop_button(db, world):
    e = world.e
    await until(lambda: world.first.presentation is not None and world.first.presentation._anchor_outbox is not None)
    e.request.enqueue("sendMessage", result=sent(e, identity=1610))
    await world.settle()
    async with db.sessions() as session:
        return await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "stop_answer"))


@pytest.mark.asyncio
async def test_a_second_tap_on_stop_stays_silent(db, monkeypatch):
    world = await live_turn(db, monkeypatch, identity=2401, real_view=True)
    e = world.e
    try:
        stop = await stop_button(db, world)
        await tap(e, 2402, stop.id, message_id=stop.message_id)
        assert world.first.stop_requested
        before = len(e.request.calls)
        await tap(e, 2403, stop.id, message_id=stop.message_id)
        for number in range(3):
            e.request.enqueue("sendMessage", result=sent(e, identity=1620 + number))
        await world.settle()
        assert NOTHING_WAITING not in said(e, before)
    finally:
        await world.turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_stop_the_run_refused_says_so(db, monkeypatch):
    world = await live_turn(db, monkeypatch, identity=2411, real_view=True)
    e = world.e
    try:
        stop = await stop_button(db, world)
        world.turns._observer.request_stop = AsyncMock(return_value="refused")
        await tap(e, 2412, stop.id, message_id=stop.message_id)
        for number in range(3):
            e.request.enqueue("sendMessage", result=sent(e, identity=1640 + number))
        await world.settle()
        assert "Stopping did not work. Send /stop again." in said(e)
    finally:
        await world.turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_stop_the_run_refuses_later_still_says_so(db, monkeypatch):
    world = await live_turn(db, monkeypatch, identity=2413, real_view=True)
    e, first = world.e, world.first
    try:
        stop = await stop_button(db, world)
        world.turns._observer.request_stop = AsyncMock(return_value="pending")
        await tap(e, 2414, stop.id, message_id=stop.message_id)
        world.turns._observer.stop_outcome = lambda *args, **kwargs: "refused"
        first.wake.set()
        await until(lambda: first.stop_requested is False)
        for number in range(3):
            e.request.enqueue("sendMessage", result=sent(e, identity=1645 + number))
        await world.settle()
        assert "Stopping did not work. Send /stop again." in said(e)
    finally:
        await world.turns.stop()
        await e.channel.stop()


async def held_turn(db, monkeypatch, *, identity):
    """The same, while it waits for room, so that its notice carries [Cancel]."""
    world = await live_turn(db, monkeypatch, identity=identity)
    e, first = world.e, world.first
    offered = asyncio.Event()
    actual_plan = e.services.outbox.plan_product_keyboard

    async def plan(*args, **kwargs):
        result = await actual_plan(*args, **kwargs)
        if kwargs.get("source_id", "").startswith("capacity:"):
            offered.set()
        return result

    monkeypatch.setattr(e.services.outbox, "plan_product_keyboard", plan)
    world.state.terminal = terminal(first.record, reason="sandbox_capacity_exceeded")
    first.wake.set()
    await asyncio.wait_for(offered.wait(), 8)
    async with db.sessions() as session:
        saved = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "cancel_wait"))
        world.callback, group, parent = saved.id, saved.group_id, saved.payload["anchor_outbox_id"]
    assert await e.services.callbacks.adopt(e.channel.link, group, 1601, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: True)
    return world


@pytest.mark.asyncio
async def test_cancel_on_a_held_message_ends_it_and_takes_its_button_away(db, monkeypatch):
    world = await held_turn(db, monkeypatch, identity=2321)
    e = world.e
    try:
        await tap(e, 2322, world.callback, message_id=1601)
        await until(world.first.done.is_set)
        assert (await e.services.callbacks.get(world.callback)).consumed_at is not None
        assert await cleanup_targets(db) == [1601]
    finally:
        await world.turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_cancel_while_the_held_message_is_being_tried_says_how_to_stop_it(db, monkeypatch):
    world = await held_turn(db, monkeypatch, identity=2421)
    e, first = world.e, world.first
    try:
        first.retry_at = first.channel._time()
        first.wake.set()
        await until(lambda: first.capacity_waiting is False)
        await tap(e, 2422, world.callback, message_id=1601)
        for number in range(3):
            e.request.enqueue("sendMessage", result=sent(e, identity=1651 + number))
        await world.settle()
        assert "Stopping did not work. Send /stop again." in said(e) and NOTHING_WAITING not in said(e)
        assert (await e.services.callbacks.get(world.callback)).consumed_at is None, "The button works again if this attempt is refused"
    finally:
        await world.turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_discarding_a_held_message_says_so_on_the_notice_and_leaves_no_button(ui):
    e = ui
    await show(e, request())
    await spend(e, "clarify_option")
    assert await e.services.human_inputs.handle_text(e.channel, quoted(e, 1785, "Held until the person chooses", CARD), captured_link=e.channel.link)
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    await drain(e)
    e.request.enqueue("editMessageText", result=sent(e, identity=703))
    await tap(e, 1786, await live(e, "prompt_discard"), message_id=703)
    await drain(e)
    edit = e.request.calls[-1]
    assert (edit.endpoint, edit.parameters["message_id"]) == ("editMessageText", 703) and edit.parameters["text"].startswith("Discarded.")
    assert edit.parameters["reply_markup"] == {"inline_keyboard": []}


@pytest.mark.asyncio
@pytest.mark.parametrize("word", ["help", "settings", "status"])
async def test_a_command_leaves_a_form_open_and_its_buttons_live(ui, word):
    e = ui
    await show(e, form({"name": "kind", "label": "Kind", "type": "select", "required": True, "options": [{"id": "a", "label": "A", "value": "a"}]}, {"name": "note", "label": "Note", "type": "text", "required": True}))
    callback = await live(e, "form_option", "a")
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    assert await e.channel.handle_update(message(e, 1740, content="/" + word))
    await drain(e)
    assert (await the_form(e))["state"] == "field" and (await e.services.callbacks.get(callback)).consumed_at is None
    e.request.enqueue("editMessageText", result=sent(e, identity=CARD))
    await tap(e, 1741, callback)
    await drain(e)
    assert (await the_form(e))["values"] == {"kind": "a"}, "A press still moves the form on"


@pytest.mark.asyncio
async def test_new_still_ends_a_form(ui):
    e = ui
    await show(e, form(COLOURS))
    services = e.services
    e.services.threads = ThreadCoordinator(links=services.links, authority=e.live.authority, client=services.thread_client, channel=services.channel, send_allowed=services.send_allowed, outbox=services.outbox, product_name="HartMesh")
    try:
        e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=CARD))
        e.request.enqueue("sendMessage", result=sent(e, identity=702))
        assert await e.channel.handle_update(message(e, 1745, content="/new"))
        await drain(e)
        assert (await the_form(e))["state"] == "cancelled"
    finally:
        await e.services.threads.stop()


@pytest.mark.asyncio
async def test_a_command_does_not_tell_a_blocked_site_card_that_a_message_was_sent(ui):
    e = ui
    await show(e, request(**NETWORK))
    callback = await live(e, "network", "deny")
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    assert await e.channel.handle_update(message(e, 1746, content="/help"))
    await drain(e)
    assert not [call for call in e.request.calls if call.endpoint == "editMessageText"]
    assert (await e.services.callbacks.get(callback)).consumed_at is None


@pytest.mark.asyncio
async def test_quick_ticks_all_land_on_the_keyboard_on_screen(ui):
    e = ui
    await show(e, form(COLOURS))
    before = len(alerts(e))
    on_screen = {row.payload["option_id"]: row.id for row in await rows(e, "form_option")}
    for update_id, colour in zip((1750, 1751, 1752), ("red", "green", "blue"), strict=True):
        await tap(e, update_id, on_screen[colour])
    assert (await the_form(e))["values"] == {"colours": ["red", "green", "blue"]}
    assert not any(alerts(e)[before:]), "No tick is told that nothing is waiting"


@pytest.mark.asyncio
async def test_a_repeat_tap_before_its_tick_shows_keeps_the_tick(ui):
    e = ui
    await show(e, form(COLOURS))
    red = await live(e, "form_option", "red")
    await tap(e, 1755, red)
    await tap(e, 1756, red)
    assert (await the_form(e))["values"] == {"colours": ["red"]}, "The second tap did not know about the first"


@pytest.mark.asyncio
async def test_a_tick_can_be_undone_from_the_keyboard_that_shows_it(ui):
    e = ui
    await show(e, form(COLOURS))
    e.request.enqueue("editMessageText", result=sent(e, identity=CARD))
    await tap(e, 1757, await live(e, "form_option", "red"))
    await drain(e)
    await tap(e, 1758, await live(e, "form_option", "red"))
    assert (await the_form(e))["values"] == {"colours": []}


@pytest.mark.asyncio
async def test_done_can_arrive_after_a_tick_that_overtook_it(ui):
    e = ui
    await show(e, form(COLOURS))
    on_screen = {row.payload["option_id"]: row.id for row in await rows(e, "form_option")}
    done = await live(e, "form_next")
    await tap(e, 1763, on_screen["red"])
    await tap(e, 1762, done)
    assert (await the_form(e))["state"] == "review"


@pytest.mark.asyncio
async def test_closing_a_form_clears_its_message_once_however_many_ticks_it_took(ui):
    e = ui
    await show(e, form(COLOURS))
    for update_id, colour in zip((1764, 1765), ("red", "green"), strict=True):
        e.request.enqueue("editMessageText", result=sent(e, identity=CARD))
        await tap(e, update_id, await live(e, "form_option", colour))
        await drain(e)
    before = len(e.request.calls)
    await e.services.human_inputs.command(e.channel, message(e, 1766, content="/new"), "new")
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=CARD))
    await drain(e)
    await drain(e)
    assert len([call for call in e.request.calls[before:] if call.endpoint == "editMessageReplyMarkup"]) == 1


@pytest.mark.asyncio
async def test_a_tick_that_finishes_after_a_later_one_still_lands(ui):
    e = ui
    await show(e, form(COLOURS))
    on_screen = {row.payload["option_id"]: row.id for row in await rows(e, "form_option")}
    await tap(e, 1761, on_screen["red"])
    await tap(e, 1760, on_screen["green"])
    assert sorted((await the_form(e))["values"]["colours"]) == ["green", "red"]


@pytest.mark.asyncio
async def test_done_works_on_the_keyboard_on_screen_after_a_tick_and_retires_every_older_button(ui):
    e = ui
    await show(e, form(COLOURS))
    on_screen = {row.payload["option_id"]: row.id for row in await rows(e, "form_option")}
    done = await live(e, "form_next")
    await tap(e, 1770, on_screen["red"])
    await tap(e, 1771, done)
    assert (await the_form(e))["state"] == "review"
    assert all(row.consumed_at is not None for row in await rows(e, "form_option")), "A later reply to this message has to find it spent"


@pytest.mark.asyncio
async def test_a_reply_to_a_spent_card_holds_that_text_alone(ui):
    e = ui
    await show(e, request())
    await spend(e, "clarify_other")
    assert await e.services.human_inputs.handle_text(e.channel, quoted(e, 1780, "An answer to something long gone", CARD), captured_link=e.channel.link)
    assert not await e.services.human_inputs.handle_text(e.channel, message(e, 1781, content="A new question"), captured_link=e.channel.link), "Later text is not held behind a card nobody can answer"
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    await drain(e)
    labels = [button["text"] for line in e.request.calls[-1].parameters["reply_markup"]["inline_keyboard"] for button in line]
    assert "Reopen answer" not in labels and len(labels) == 2, "There is nothing to reopen"
    assert await e.services.human_inputs.handle_text(e.channel, quoted(e, 1782, "Another answer to the same card", CARD), captured_link=e.channel.link)
    e.request.enqueue("sendMessage", result=sent(e, identity=704))
    await drain(e)
    labels = [button["text"] for line in e.request.calls[-1].parameters["reply_markup"]["inline_keyboard"] for button in line]
    assert "Reopen answer" not in labels, "A second reply finds the same dead card"


async def form_on_its_typed_field(e):
    await show(e, form({"name": "kind", "label": "Kind", "type": "select", "required": True, "options": [{"id": "a", "label": "A", "value": "a"}]}, {"name": "note", "label": "Note", "type": "text", "required": True}))
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=CARD))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await tap(e, 1783, await live(e, "form_option", "a"))
    await drain(e)
    await drain(e)


@pytest.mark.asyncio
async def test_a_reply_to_a_spent_keyboard_of_an_open_form_can_be_reopened(ui):
    e = ui
    await form_on_its_typed_field(e)
    assert await e.services.human_inputs.handle_text(e.channel, quoted(e, 1784, "My note", CARD), captured_link=e.channel.link)
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    await drain(e)
    labels = [button["text"] for line in e.request.calls[-1].parameters["reply_markup"]["inline_keyboard"] for button in line]
    assert "Reopen answer" in labels, "The form is open, so it is not dead"


@pytest.mark.asyncio
async def test_a_reply_to_a_spent_keyboard_of_a_form_with_no_conversation_left_is_still_answered(ui):
    e = ui
    await form_on_its_typed_field(e)
    conversation_deleted(e)
    assert await e.services.human_inputs.handle_text(e.channel, quoted(e, 1787, "My note", CARD), captured_link=e.channel.link), "It used to raise, and hold up the chat's updates"
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    await drain(e)
    assert e.request.calls[-1].parameters["text"].startswith("This prompt can no longer take an answer")


@pytest.mark.asyncio
async def test_a_receipt_too_long_to_edit_in_place_still_takes_the_buttons_off_its_card(ui):
    e = ui
    await show(e, request(question="Q" * 4070))
    callback = await live(e, "clarify_option")
    before = len(e.request.calls)
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=CARD))
    await tap(e, 1793, callback)
    for number in range(4):
        e.request.enqueue("sendMessage", result=sent(e, identity=720 + number))
    for _ in range(3):  # One pass sends one row, and these two were planned together in no set order.
        await drain(e)
    wire = [(call.endpoint, call.parameters.get("message_id")) for call in e.request.calls[before:]]
    assert ("editMessageReplyMarkup", CARD) in wire, wire


@pytest.mark.asyncio
async def test_a_reply_to_a_list_after_a_tick_is_a_reply_to_one_card(ui):
    e = ui
    await show(e, form(COLOURS))
    e.request.enqueue("editMessageText", result=sent(e, identity=CARD))
    await tap(e, 1788, await live(e, "form_option", "red"))
    await drain(e)
    route = await e.services.callbacks.for_message(e.link.id, CARD, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert not route.retired_presentation, "Two keyboards of one list used to read as a card nobody can answer"


@pytest.mark.asyncio
async def test_reopen_takes_the_buttons_off_its_notice(ui):
    e = ui
    await show(e, form({"name": "note", "label": "Note", "type": "text", "required": True}))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    assert await e.channel.handle_update(message(e, 1789, content="/help"))
    await drain(e)
    assert await e.services.human_inputs.handle_text(e.channel, quoted(e, 1790, "Late answer", CARD), captured_link=e.channel.link)
    e.request.enqueue("sendMessage", result=sent(e, identity=703))
    await drain(e)
    await tap(e, 1791, await live(e, "prompt_reopen"), message_id=703)
    async with e.services.outbox.sessions() as session:
        cleanup = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["kind"].as_string() == "keyboard_cleanup"))
    assert cleanup.parts[0]["target_message_id"] == 703


@pytest.mark.asyncio
async def test_a_command_before_a_question_reaches_the_screen_does_not_lose_it(ui):
    e = ui
    await e.offer(form({"name": "note", "label": "Note", "type": "text", "required": True}))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    assert await e.channel.handle_update(message(e, 1792, content="/help"))
    await asyncio.gather(*tuple(e.channel._ingress_tasks))
    e.request.enqueue("sendMessage", result=sent(e, identity=CARD))
    for _ in range(3):
        e.clock.advance(2)
        await e.sender.tick()
        await e.sender.drain()
    assert any("Note" in (text or "") for text in said(e)), "A form with no card on screen can only be ended by accident"


@pytest.mark.asyncio
async def test_a_look_at_the_web_list_before_a_question_reaches_the_screen_does_not_lose_it(ui):
    e = ui
    await e.services.commands.services.outbox.plan_product_keyboard(
        e.link,
        source_id="help-keyboard",
        parts=[{"kind": "text", "source_ref": {"key": "tap.failed", "values": {}}}],
        keyboard=[[{"label": "label.web_list", "kind": "web_list", "target_kind": "link", "target_id": e.link.id, "payload": {}}]],
        callbacks=e.services.callbacks,
        guard=e.services.links.publication_guard(e.link, check_live_owner=await e.services.prepare_live_owner(e.link.owner_user_id), check_attempt_current=lambda: True),
    )
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    await drain(e)
    await e.offer(form({"name": "note", "label": "Note", "type": "text", "required": True}))
    e.request.enqueue("editMessageText", result=sent(e, identity=702))
    await tap(e, 1794, await live(e, "web_list"), message_id=702)
    e.request.enqueue("sendMessage", result=sent(e, identity=CARD))
    for _ in range(3):
        e.clock.advance(2)
        await e.sender.tick()
        await e.sender.drain()
    assert any("Note" in (text or "") for text in said(e))
