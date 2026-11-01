"""A saved message always ends somewhere: a failed attempt goes on, and what nothing can finish is told so."""

import asyncio
import logging
from datetime import timedelta

import httpx
import pytest
from _telegram_bot_test_support import update_payload
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from _telegram_turn_stack import bound_again, mark, only_turn, output_projection, planned_keys, press_try_again, rebind, restart, send, stack, until
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from telegram import Update
from test_telegram_bot_handlers import ingress
from test_telegram_bot_handlers import message as sdk_message
from test_telegram_chat_turns import controlled_execution, message
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.turns import _Guard
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


def fail_once(monkeypatch, owner, name, *, when=lambda *args, **kwargs: True):
    """The next call that `when` picks raises, as a database blip does; every other call is the real one."""
    actual = getattr(owner, name)
    fired = []

    def fire(args, kwargs):
        if not fired and when(*args, **kwargs):
            fired.append(True)
            raise OperationalError("SELECT 1", {}, Exception("server closed the connection unexpectedly"))

    if asyncio.iscoroutinefunction(actual):

        async def wrapper(*args, **kwargs):
            fire(args, kwargs)
            return await actual(*args, **kwargs)

    else:

        def wrapper(*args, **kwargs):
            fire(args, kwargs)
            return actual(*args, **kwargs)

    monkeypatch.setattr(owner, name, wrapper)
    return fired


async def saved_input(s, link, identity):
    """A message at rest in SQL: routed, with no turn, as a stopped process leaves it."""
    inbox = s.services.outbox.inbox
    row, _ = await inbox.accept(link, sdk_message(s.e, identity, content=f"Waiting {identity}"), sanitized_input={"type": "text", "text": f"Waiting {identity}"})
    return await inbox.route(row, kind="turn", target_id=link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])


def starting_up(s):
    """The supervisor's phase while it still holds ingress."""
    s.services._copy_decision = False
    s.services._cohort_complete = s.services._ready = True
    s.services._send_ready = s.services._stopping = s.services._stopped = False
    s.services._cause = None


async def test_a_failure_before_the_route_goes_on_when_the_update_comes_back(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    fail_once(monkeypatch, s.services.outbox.inbox, "route")
    try:
        with pytest.raises(OperationalError):
            await send(s, 10001, "Saved, never routed")
        assert (await mark(s, 10001)).route_kind is None
        assert await send(s, 10001, "Saved, never routed") is True
        await until(lambda: s.gateway.posts() == 1)
        assert (await mark(s, 10001)).route_kind == "turn"
    finally:
        await s.close()


async def test_a_failure_after_the_route_goes_on_when_the_update_comes_back(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    fail_once(monkeypatch, s.turns.queue, "admit")
    try:
        with pytest.raises(OperationalError):
            await send(s, 10011, "Routed, never queued")
        assert (await mark(s, 10011)).route_kind == "turn" and not s.turns._turns
        assert await send(s, 10011, "Routed, never queued") is True
        await until(lambda: s.gateway.posts() == 1)
    finally:
        await s.close()


async def test_a_message_already_in_line_is_not_put_in_line_again_by_its_replay(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    channel = s.e.channel
    try:
        assert await send(s, 10021, "Running") is True
        await until(lambda: s.gateway.posts() == 1)
        link = channel.link
        fail_once(monkeypatch, channel, "send_text", when=lambda key, **_kwargs: key == "busy.followup")
        with pytest.raises(OperationalError):
            await send(s, 10022, "Waiting")
        assert s.turns.queue.waiting_count(link, None) == 1
        assert await send(s, 10022, "Waiting") is True
        assert s.turns.queue.waiting_count(link, None) == 1
    finally:
        await s.close()


async def test_a_message_held_in_a_join_is_not_joined_again_by_its_replay(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    long = "word " * 900
    try:
        assert await send(s, 10025, long) is True
        joined = next(iter(s.turns._joins.values()))
        assert await send(s, 10025, long) is True
        assert [row.message_id for row in joined.origins] == [10025]
    finally:
        await s.close()


async def test_a_stop_whose_first_attempt_failed_still_stops(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    inbox = s.services.outbox.inbox
    try:
        assert await send(s, 10031, "Running") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.runs and turn.run_id is not None)
        fail_once(monkeypatch, inbox, "route")
        with pytest.raises(OperationalError):
            await send(s, 10032, "/stop")
        assert not turn.stop_requested
        assert await send(s, 10032, "/stop") is True
        assert turn.stop_requested
        await until(lambda: any(path.endswith("/cancel") for _method, path in s.gateway.calls))
        assert [row.message_id for row in turn.stop_commands] == [10032]
    finally:
        await s.close()


async def test_a_stop_is_not_counted_twice_when_it_comes_back_after_it_took_effect(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 10035, "Running") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.runs and turn.run_id is not None)
        assert await send(s, 10036, "/stop") is True
        assert await send(s, 10036, "/stop") is True
        assert [row.message_id for row in turn.stop_commands] == [10036]
    finally:
        await s.close()


async def test_a_stop_on_a_channel_going_away_is_not_acknowledged(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    inbox = s.services.outbox.inbox
    actual = inbox.accept

    async def going_away(*args, **kwargs):
        result = await actual(*args, **kwargs)
        s.e.active[0] = False
        return result

    monkeypatch.setattr(inbox, "accept", going_away)
    try:
        assert await send(s, 10038, "/stop") is False
        monkeypatch.setattr(inbox, "accept", actual)
        s.e.active[0] = True
        assert await send(s, 10038, "/stop") is True
        assert await planned_keys(s) == ["stop.nothing"]
    finally:
        await s.close()


async def test_a_routed_message_from_before_a_restart_is_left_to_recovery(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        await saved_input(s, s.e.channel.link, 10041)
        recovery = await restart(s)
        assert await send(s, 10041, "Waiting 10041") is True
        assert not s.turns._turns and s.gateway.posts() == 0
        await recovery.stop()
    finally:
        await s.close()


async def test_a_message_with_no_route_runs_when_its_update_comes_back_after_a_restart(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    inbox = s.services.outbox.inbox
    try:
        link = s.e.channel.link
        await inbox.accept(link, sdk_message(s.e, 10051, content="Never routed"), sanitized_input={"type": "text", "text": "Never routed"})
        recovery = await restart(s)
        starting_up(s)
        await s.turns.recover_startup()
        assert await planned_keys(s) == [], "Recovery cannot read a row with no route"
        assert await send(s, 10051, "Never routed") is True
        await until(lambda: s.gateway.posts() == 1)
        await recovery.stop()
    finally:
        await s.close()


async def test_a_finished_message_stays_finished_when_its_update_comes_back(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 10055, "First") is True
        await until(lambda: s.gateway.posts() == 1)
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
        s.gateway.finish(turn.record.publication_id, output=output_projection(turn.record))
        await until(lambda: not s.turns._turns)
        assert await send(s, 10055, "First") is True
        assert not s.turns._turns and s.gateway.posts() == 1
    finally:
        await s.close()


async def test_a_typed_answer_answers_its_prompt_after_a_failed_read(db, monkeypatch):
    from app.telegram_bots.callbacks import Prompts
    from app.telegram_bots.forms import Forms
    from app.telegram_bots.human_input import HumanInputs

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _reached = await controlled_execution(e, monkeypatch)
    services = e.services
    services.prompts = Prompts(services.callbacks)
    services.forms = Forms(services.callbacks)
    services.human_inputs = HumanInputs(services, source=None)
    link = e.channel.link
    anchor = (await rows(db, TelegramOutboxRow))[0]
    prompt = await services.prompts.open(link, "clarify_other", {"thread_id": "t", "request_id": "r"}, chat_id=link.chat_id, message_thread_id=None, message_id=None)
    async with db.sessions.begin() as session:
        row = await session.get(TelegramCallbackRow, prompt)
        row.payload = {**row.payload, "anchor_outbox_id": anchor.id, "presentation_version": 0, "presentation_pending": False}
    applied = []
    attempts = []

    async def apply(channel, link, guard, origin, target):
        attempts.append(origin.message_id)
        if len(attempts) == 1:
            raise OperationalError("SELECT 1", {}, Exception("gone away"))
        applied.append(target[0].id)
        return True

    monkeypatch.setattr(services.human_inputs, "_apply_text", apply)
    try:
        with pytest.raises(OperationalError):
            await e.channel.handle_update(message(e, 10061, content="my typed answer"))
        assert await e.channel.handle_update(message(e, 10061, content="my typed answer"))
        assert applied == [prompt], "The answer goes to its prompt, not to the chat as a new message"
        assert not turns._turns and state.posts == 0
    finally:
        await turns.stop()
        await e.channel.stop()


async def test_a_message_queued_before_a_prompt_opened_is_not_taken_for_its_answer(db, monkeypatch):
    from app.telegram_bots.callbacks import Prompts
    from app.telegram_bots.forms import Forms
    from app.telegram_bots.human_input import HumanInputs

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _reached = await controlled_execution(e, monkeypatch)
    services = e.services
    services.prompts = Prompts(services.callbacks)
    services.forms = Forms(services.callbacks)
    services.human_inputs = HumanInputs(services, source=None)
    link = e.channel.link
    anchor = (await rows(db, TelegramOutboxRow))[0]
    fail_once(monkeypatch, turns.queue, "admit")

    async def apply(*_args):
        pytest.fail("A prompt that appeared after the person spoke was not what they were answering")

    monkeypatch.setattr(services.human_inputs, "_apply_text", apply)
    try:
        with pytest.raises(OperationalError):
            await e.channel.handle_update(message(e, 10065, content="An ordinary message"))
        prompt = await services.prompts.open(link, "clarify_other", {"thread_id": "t", "request_id": "r"}, chat_id=link.chat_id, message_thread_id=None, message_id=None)
        async with db.sessions.begin() as session:
            row = await session.get(TelegramCallbackRow, prompt)
            row.payload = {**row.payload, "anchor_outbox_id": anchor.id, "presentation_version": 0, "presentation_pending": False}
        assert await e.channel.handle_update(message(e, 10065, content="An ordinary message"))
        await until(lambda: turns._turns or state.posts)
    finally:
        await turns.stop()
        await e.channel.stop()


async def test_a_join_that_fails_to_flush_says_so_and_offers_try_again(db, monkeypatch, tmp_path, caplog):
    s = await stack(db, monkeypatch, tmp_path)
    fired = fail_once(monkeypatch, s.turns.queue, "admit")
    long = "word " * 900
    try:
        assert await send(s, 10071, long) is True
        with caplog.at_level(logging.WARNING):
            await until(lambda: fired)
            await until(planned_keys_of(s))
        assert await planned_keys(s) == ["error.generic"]
        assert (await mark(s, 10071)).route_complete
        assert not s.turns._joins and not s.turns._turns
        assert f"chat_join_held link={s.e.link.id}" in caplog.text
        async with s.db.sessions() as session:
            control = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message"))
        assert control.target_id == "10071"
        assert await send(s, 10072, "Next") is True
        await until(lambda: s.gateway.posts() == 1)
    finally:
        await s.close()


def planned_keys_of(s):
    async def any_planned():
        return await planned_keys(s)

    return any_planned


async def test_a_join_in_line_whose_notice_failed_stays_in_line(db, monkeypatch, tmp_path, caplog):
    s = await stack(db, monkeypatch, tmp_path)
    channel = s.e.channel
    long = "word " * 900
    try:
        assert await send(s, 10075, "Running") is True
        await until(lambda: s.gateway.posts() == 1)
        link = channel.link
        fired = fail_once(monkeypatch, channel, "send_text", when=lambda key, **_kwargs: key == "busy.followup")
        with caplog.at_level(logging.WARNING):
            assert await send(s, 10076, long) is True
            await until(lambda: fired)
            await until(lambda: "chat_join_held" in caplog.text and not s.turns._join_busy)
        assert s.turns.queue.waiting_count(link, None) == 1
        assert "error.generic" not in await planned_keys(s)
        assert not (await mark(s, 10076)).route_complete
    finally:
        await s.close()


async def test_a_join_whose_ending_cannot_be_written_is_logged_and_the_next_start_closes_it(db, monkeypatch, tmp_path, caplog):
    s = await stack(db, monkeypatch, tmp_path)
    fired = fail_once(monkeypatch, s.turns.queue, "admit")
    refused = fail_once(monkeypatch, s.services.outbox, "plan_input_outcome")
    long = "word " * 900
    try:
        with caplog.at_level(logging.WARNING):
            assert await send(s, 10077, long) is True
            await until(lambda: fired and refused and not s.turns._joins and not s.turns._join_busy)
            await until(lambda: caplog.text.count("chat_join_held") == 2)
        assert "chat_turn_held" not in caplog.text, "Both failures are handled, not left to the task's callback"
        assert not (await mark(s, 10077)).route_complete
        await s.turns.recover_link(s.e.channel)
        assert (await mark(s, 10077)).route_complete
    finally:
        await s.close()


async def test_a_cancelled_admission_waits_for_its_notice_to_finish_writing(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    channel = s.e.channel
    writing, release, finished = asyncio.Event(), asyncio.Event(), []
    actual = channel.send_text

    async def send_text(key, **kwargs):
        if key != "busy.followup":
            return await actual(key, **kwargs)
        writing.set()
        await release.wait()
        finished.append(key)

    try:
        assert await send(s, 10091, "Running") is True
        await until(lambda: s.gateway.posts() == 1)
        monkeypatch.setattr(channel, "send_text", send_text)
        waiting = asyncio.create_task(send(s, 10092, "Waiting"))
        await writing.wait()
        waiting.cancel()
        await asyncio.sleep(0.05)
        assert not waiting.done(), "A write cancelled halfway holds SQLite's write lock with no one to release it"
        release.set()
        await asyncio.gather(waiting, return_exceptions=True)
        assert finished == ["busy.followup"]
    finally:
        release.set()
        await s.close()


async def test_a_failed_flush_does_not_fail_the_update_that_triggered_it(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    fail_once(monkeypatch, s.turns.queue, "admit")
    long = "word " * 900
    try:
        assert await send(s, 10078, long) is True
        assert await send(s, 10079, "/stop") is True, "Nothing is wrong with the /stop itself"
        assert (await mark(s, 10079)).route_complete
        assert sorted(await planned_keys(s)) == ["error.generic", "stop.nothing"]
    finally:
        await s.close()


async def test_a_new_channel_closes_what_the_last_one_left(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        s.gateway.stream_status = 503
        assert await send(s, 10081, "Running") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
        assert await send(s, 10082, "Waiting") is True
        assert await send(s, 10083, "Also waiting") is True
        await s.turns.retire(s.e.channel)
        assert "chat.restarted" not in await planned_keys(s)
        await s.turns.recover_link(s.e.channel)
        assert (await planned_keys(s)).count("chat.restarted") == 2
        assert (await mark(s, 10082)).route_complete and (await mark(s, 10083)).route_complete
        assert not (await mark(s, 10081)).route_complete, "A dispatched message is publication recovery's"
    finally:
        await s.close()


async def test_a_new_channel_leaves_other_links_alone(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    inbox = s.services.outbox.inbox
    try:
        link = s.e.channel.link
        await saved_input(s, link, 10085)
        other = await bind_link(db, await add_link(db, owner=link.owner_user_id, bot=9_000_000_077), account=7_000_000_077, message=7701, update=7702)
        update = Update.de_json(update_payload(10086, message_id=10086, account=7_000_000_077, text="Another bot's chat"), None)
        row, _ = await inbox.accept(other, update, sanitized_input={"type": "text", "text": "Another bot's chat"})
        await inbox.route(row, kind="turn", target_id=other.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
        await s.turns.recover_link(s.e.channel)
        assert (await mark(s, 10085)).route_complete
        async with s.db.sessions() as session:
            theirs = await session.get(TelegramInboundMarkRow, (other.id, 10086, other.chat_id))
        assert not theirs.route_complete
    finally:
        await s.close()


async def test_a_new_channel_that_is_replaced_or_out_of_time_closes_nothing_more(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        await saved_input(s, s.e.channel.link, 10087)
        monkeypatch.setattr("app.telegram_bots.turns.LEFT_INPUTS_SECONDS", 0)
        await s.turns.recover_link(s.e.channel)
        assert not (await mark(s, 10087)).route_complete
        monkeypatch.setattr("app.telegram_bots.turns.LEFT_INPUTS_SECONDS", 30)
        actual = s.services.channel
        s.services.channel = lambda _identity: None
        await s.turns.recover_link(s.e.channel)
        assert not (await mark(s, 10087)).route_complete
        s.services.channel = actual
        await s.turns.recover_link(s.e.channel)
        assert (await mark(s, 10087)).route_complete
    finally:
        await s.close()


async def test_a_failed_read_at_channel_start_does_not_stop_the_channel(db, monkeypatch, tmp_path, caplog):
    s = await stack(db, monkeypatch, tmp_path)
    fail_once(monkeypatch, s.services.outbox.inbox, "prepublication_page")
    try:
        with caplog.at_level(logging.WARNING):
            await s.turns.recover_link(s.e.channel)
        assert "chat_startup_page_held" in caplog.text
    finally:
        await s.close()


async def test_every_page_of_left_inputs_is_closed_at_startup(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        link = s.e.channel.link
        for offset in range(34):
            await saved_input(s, link, 10100 + offset)
        recovery = await restart(s)
        starting_up(s)
        await s.turns.recover_startup()
        assert (await planned_keys(s)).count("chat.restarted") == 34
        await recovery.stop()
    finally:
        await s.close()


async def test_a_held_late_answer_stays_held_through_a_restart(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    recovery = None
    try:
        link = s.e.channel.link
        origin = await saved_input(s, link, 10201)
        guard = _Guard(s.services, s.e.channel, link, lambda: True, repair=True)
        await s.services.outbox.plan_product_keyboard(
            link,
            source_id="prompt-closed:p:10201:0",
            parts=[{"kind": "text", "source_ref": {"key": "prompt.closed", "values": {}}, "reply_to_message_id": 10201}],
            keyboard=[],
            callbacks=s.services.callbacks,
            guard=guard,
            inbox_rows=[origin],
            finish_route=False,
        )
        async with s.db.sessions.begin() as session:
            row = await session.get(TelegramInboundMarkRow, (link.id, 10201, link.chat_id))
            row.route_basis = {**row.route_basis, "human_input_ui": {"held_prompt_id": "p" * 32}}
        recovery = await restart(s)
        starting_up(s)
        await s.turns.recover_startup()
        assert "chat.restarted" not in await planned_keys(s)
        assert not (await mark(s, 10201)).route_complete
    finally:
        if recovery is not None:
            await recovery.stop()
        await s.close()


async def test_access_off_closes_every_open_message_of_the_ended_binding(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        link = s.e.channel.link
        await saved_input(s, link, 10251)
        held = await s.db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
        assert held is not None
        row = await mark(s, 10251)
        assert (row.state, row.route_complete) == ("done", True)
    finally:
        await s.close()


async def test_a_policy_stop_leaves_the_binding_and_its_messages_open(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        link = s.e.channel.link
        await saved_input(s, link, 10255)
        await s.db.links.end_pending(link.id, skip_reason="policy-off", consume_callbacks=False)
        assert not (await mark(s, 10255)).route_complete
    finally:
        await s.close()


async def test_disconnect_closes_a_try_again_waiting_in_line(db, monkeypatch, tmp_path, caplog):
    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.inbox import callback_publication_pending

    s = await stack(db, monkeypatch, tmp_path)
    fired = fail_once(monkeypatch, s.turns.queue, "admit")
    try:
        link = s.e.channel.link
        assert await send(s, 10261, "word " * 900) is True
        await until(lambda: fired)
        await until(planned_keys_of(s))
        s.gateway.stream_status = 503
        assert await send(s, 10262, "Running") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
        await press_try_again(s, "10261")
        await until(lambda: s.turns.queue.waiting_count(s.e.channel.link, None) == 1)
        fresh = await rebind(s, link)
        await bound_again(s, link, fresh)
        await until(lambda: not s.turns._turns)
        assert not s.turns._callback_origins

        async def waiting():
            async with s.db.sessions() as session:
                return [row for row in await session.scalars(select(TelegramCallbackRow)) if callback_publication_pending(row.payload)]

        assert await waiting() == []
        with caplog.at_level(logging.WARNING):
            await s.turns.recover_link(s.e.channel)
        assert "chat_startup_input_held" not in caplog.text
        await CallbackRepository(s.services.outbox.sessions).sweep(s.e.clock.now() + timedelta(days=400))
        async with s.db.sessions() as session:
            left = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message")))
        assert left == [], "Nothing keeps its group from the sweep"
    finally:
        await s.close()


async def test_disconnect_closes_every_open_message_of_the_old_binding(db, monkeypatch, tmp_path):
    from app.telegram_bots.retention import sweep_rows

    s = await stack(db, monkeypatch, tmp_path)
    try:
        link = s.e.channel.link
        s.gateway.stream_status = 503
        assert await send(s, 10301, "Running") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
        assert await send(s, 10302, "Waiting") is True
        fresh = await rebind(s, link)
        for identity in (10301, 10302):
            row = await mark(s, identity)
            assert (row.state, row.route_complete) == ("done", True) and row.closed_at is not None
        await bound_again(s, link, fresh)
        await until(lambda: not s.turns._turns)
        assert not s.turns._origins, "Nothing keeps a message of the old binding"
        recovery = await restart(s)
        assert (await recovery.tick()).held == 0
        await recovery.stop()
        now = s.e.clock.now() + timedelta(days=8)
        async with s.db.sessions() as session:
            before = len(list(await session.scalars(select(TelegramInboundMarkRow))))
        assert before >= 2
        await sweep_rows(s.db.sessions, now, callback_max_age_days=30)
        assert await mark(s, 10301) is None and await mark(s, 10302) is None
    finally:
        await s.close()


async def test_a_stop_whose_cancel_outcome_was_unknown_is_asked_again(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    failing = [True]

    def fault(request):
        if failing[0] and request.url.path.endswith("/cancel"):
            return httpx.Response(500, json={"detail": "scripted"})

    s.gateway.fault = fault
    cancels = lambda: sum(1 for _method, path in s.gateway.calls if path.endswith("/cancel"))  # noqa: E731
    try:
        assert await send(s, 10401, "Running") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.runs and turn.run_id is not None)
        assert await send(s, 10402, "/stop") is True
        await until(lambda: turn.stop_status == "unknown")
        failing[0] = False
        await until(lambda: cancels() >= 2 and turn.stop_status == "requested")
    finally:
        await s.close()


async def waiting_reply(s, identity):
    """A finished turn whose reply has not gone out, and a later notice queued behind it."""
    from app.telegram_bots.model_source import ModelSource
    from app.telegram_bots.producer import OutboxSender

    e = s.e
    s.services.model_source = ModelSource(read_output=s.services.publications.read_output)
    assert await send(s, identity, "First") is True
    turn = only_turn(s)
    await until(lambda: s.gateway.runs and turn.record.dispatch_started)
    s.gateway.finish(turn.record.publication_id, output=output_projection(turn.record))
    await until(lambda: turn.done.is_set())
    async with s.db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            if not row.dedupe_key.startswith("reply:"):
                row.state, row.closed_at = "sent", e.clock.now()
    e.clock.advance(5)
    await e.channel.send_text("stop.nothing", source_id="later-notice", _captured_link=e.channel.link)

    async def deliver(**kwargs):
        return await e.channel.send_part(**{key: value for key, value in kwargs.items() if key != "link_id"})

    return turn, OutboxSender(s.services.outbox, run_id="test-sender", deliver_part=deliver, eligibility=e.live.authority.delivery, jitter=lambda: 0)


async def delivery_states(s):
    async with s.db.sessions() as session:
        rows_ = list(await session.scalars(select(TelegramOutboxRow).order_by(TelegramOutboxRow.created_at)))
    return {row.payload.get("kind", row.dedupe_key.split(":")[0]): row for row in rows_}


async def run_sender(s, sender, ticks=3):
    for _ in range(ticks):
        await sender.tick()
        await sender.drain()
        s.e.clock.advance(2)


async def test_a_reply_whose_source_is_gone_fails_and_the_next_one_goes_out(db, monkeypatch, tmp_path, caplog):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        turn, sender = await waiting_reply(s, 10501)
        s.gateway.runs[turn.record.publication_id]["output"] = None
        for _ in range(4):
            s.e.request.enqueue("sendMessage", result={"message_id": 900, "date": 1_893_456_000, "chat": {"id": s.e.link.chat_id, "type": "private"}, "text": "x"})
        with caplog.at_level(logging.WARNING):
            await run_sender(s, sender, ticks=4)
        states = await delivery_states(s)
        assert states["reply"].state == "failed"
        assert "delivery.failed" in await planned_keys(s)
        assert any(row.state == "sent" and row.dedupe_key.endswith("later-notice") for row in states.values())
        assert f"delivery_source_gone link={s.e.link.id} delivery={states['reply'].id}" in caplog.text
        assert "send_failed" not in caplog.text, "Nothing was sent, so nothing about the bot's sending failed"
        await sender.stop()
    finally:
        await s.close()


async def test_a_reply_that_could_not_be_read_just_now_backs_off_and_the_next_one_goes_out(db, monkeypatch, tmp_path, caplog):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        turn, sender = await waiting_reply(s, 10511)
        failing = [True]

        def fault(request):
            if failing[0] and request.url.path.endswith("/output"):
                return httpx.Response(503, json={"detail": "scripted"})

        s.gateway.fault = fault
        s.e.request.enqueue("sendMessage", result={"message_id": 900, "date": 1_893_456_000, "chat": {"id": s.e.link.chat_id, "type": "private"}, "text": "x"})
        with caplog.at_level(logging.WARNING):
            await run_sender(s, sender, ticks=2)
        states = await delivery_states(s)
        assert states["reply"].state == "pending" and states["reply"].attempts >= 1, "Backed off, and not lost"
        assert any(row.state == "sent" and row.dedupe_key.endswith("later-notice") for row in states.values())
        assert f"delivery_source_unread link={s.e.link.id} delivery={states['reply'].id} attempts=1" in caplog.text
        await sender.stop()
    finally:
        await s.close()
