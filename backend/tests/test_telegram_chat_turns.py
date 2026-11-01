"""Actual ingress commits input before an owned chat task can start."""

import asyncio
import importlib
from dataclasses import replace
from datetime import UTC, datetime
from types import MappingProxyType, MethodType, SimpleNamespace

import httpx
import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Chat, Message, Update, User
from test_telegram_bot_handlers import ingress
from test_telegram_bot_handlers import message as sdk_message
from test_telegram_callback_publication import retry_input_environment as retry_input_environment
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow


def message(e, identity, **kwargs):
    # Root's real ingress emits one disposable typing signal per text gesture.
    if not kwargs.get("content", "/agent").startswith("/"):
        e.request.enqueue("sendChatAction", result=True)
    return sdk_message(e, identity, **kwargs)


class ControlledView:
    """No transport proof: isolate controller decisions from UI/SDK timing."""

    def __init__(self, **kwargs):
        self.closed = False
        self.mode, self.started, self.paused = "edit", False, False

    async def start(self):
        pass

    async def flush(self):
        pass

    async def preview(self, value):
        pass

    async def activity(self, key, values):
        pass

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def close(self):
        self.closed = True

    async def abandon(self):
        self.closed = True

    async def prepare_final(self):
        self.closed = True
        return True


def test_retired_turn_cannot_drop_a_replacement_owners_waiting_inputs():
    from test_telegram_chat_queue import environment
    from test_telegram_chat_queue import message as queued

    from app.telegram_bots.turns import ChatTurns, _Turn

    link, queue = environment()
    turns = ChatTurns(SimpleNamespace(config=SimpleNamespace(queue=SimpleNamespace(depth=10))))
    turns.queue = queue
    origins = (SimpleNamespace(message_thread_id=None),)
    retired = _Turn(object(), link, queued(link, 1), origins, active=False)
    queue.admit(link, queued(link, 3))
    queue.admit(link, queued(link, 4))
    turns._turns[(link.id, None)] = _Turn(object(), link, queued(link, 3), origins)
    turns._drop_waiting(retired)
    assert queue.waiting_count(link, None) == 1
    assert not retired.cancelled_waiting


async def controlled_execution(e, monkeypatch):
    """Real ingress/SQL custody, with explicitly controlled execution reads."""
    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.streaming import LivePresentation

    module = importlib.import_module("app.telegram_bots.turns")
    turns = module.ChatTurns(e.services)
    e.services.turns = turns
    e.services.channel = lambda identity: e.channel if identity == e.link.id else None
    e.services.callbacks = CallbackRepository(e.services.outbox.sessions)
    e.services.thread_client = SimpleNamespace(endpoint_id="a" * 64)
    monkeypatch.setattr(importlib.import_module(LivePresentation.__module__), "LivePresentation", ControlledView)
    reached, release = asyncio.Event(), asyncio.Event()
    state = SimpleNamespace(record=None, reads=0, plans=0, posts=0, terminal=None, pinned=None, stop_calls=0)

    async def capture(link, **kwargs):
        owner = await e.services.prepare_live_owner(link.owner_user_id)
        turn = next(iter(turns._turns.values()))
        guard = e.services.links.publication_guard(link, check_live_owner=owner, check_attempt_current=lambda: turns._current(turn))
        return SimpleNamespace(create_guard=guard, read_guard=guard, cancel_guard=guard)

    async def ensure(*args, **kwargs):
        return "controller-original-thread"

    async def dispatch_record(record, link):
        turn = next(iter(turns._turns.values()))
        state.pinned = turn.record is not None
        reached.set()
        assert turn.record is not None, "Actual publication must be pinned before the irreversible marker"
        assert turn.record.publication_id == record.publication_id
        attempt = await capture(link)
        await e.services.outbox.inbox.mark_dispatch_started(record, guard=attempt.create_guard)
        state.record, state.posts = record, state.posts + 1
        reached.set()
        return replace(record, dispatch_started=True)

    async def dispatch(link, rows, *, thread_id):
        attempt = await capture(link)
        record = await e.services.outbox.inbox.reserve_publication(link, rows, thread_id=thread_id, endpoint_id=e.services.thread_client.endpoint_id, guard=attempt.create_guard)
        return await dispatch_record(record, link)

    async def watch(*args, **kwargs):
        # Deliberate EOF with no run is not completion evidence.
        reached.set()
        return None

    async def read_run(*args):
        state.reads += 1
        if state.terminal is not None:
            attempt = await capture(e.channel.link)
            await e.services.outbox.inbox.bind_publication_run(args[0], run_id=state.terminal.row["run_id"], guard=attempt.read_guard)
        return state.terminal

    async def retry(previous, link, *, on_reserved=None):
        attempt = await capture(link)
        successor = await e.services.outbox.inbox.reserve_retry_publication(previous, terminal=state.terminal, guard=attempt.create_guard)
        if on_reserved is not None:
            on_reserved(successor)
        state.terminal = None
        return await dispatch_record(successor, link)

    async def request_stop(record, link, *, stop_id):
        state.stop_calls += 1
        attempt = await capture(link)
        await e.services.outbox.inbox.request_publication_stop(record, stop_id=stop_id, guard=attempt.cancel_guard)
        return "terminal" if state.terminal is not None else "pending"

    async def plan_final(*args, **kwargs):
        state.plans += 1
        assert state.terminal is not None, "SSE EOF/absence cannot authorize a final plan"
        return "qualified-primary"

    async def drain():
        release.set()

    e.services.threads = SimpleNamespace(ensure_thread=ensure)
    e.services.publication_authority = SimpleNamespace(capture=capture)
    e.services.publications = SimpleNamespace(dispatch=dispatch, dispatch_record=dispatch_record, retry=retry, fence=lambda record: None, worker_state=lambda record: "taken")
    final_read = lambda record: {"output": "unavailable:unsupported_output"}  # noqa: E731
    turns._observer = SimpleNamespace(watch=watch, read_run=read_run, plan_final=plan_final, final_read=final_read, request_stop=request_stop, stop_outcome=lambda *args, **kwargs: None, retire=lambda record: None, drain=drain)
    return turns, state, reached


def terminal(record, *, reason=None, status="error"):
    from app.channels.execution_client import ValidatedRun
    from app.gateway.channel_execution import ChannelExecutionReceipt

    receipt = ChannelExecutionReceipt(version=1, publication_id=record.publication_id, owner_user_id=record.scope.owner_user_id, thread_id=record.scope.thread_id, assistant_id=record.scope.assistant_id, agent_name=record.scope.agent_name)
    return ValidatedRun(
        MappingProxyType(
            {
                "run_id": "run-" + record.publication_id,
                "thread_id": record.scope.thread_id,
                "status": status,
                "stop_reason": reason,
                "metadata": {"channel_turn_id": record.publication_id},
                "kwargs": {"channel_execution": receipt.model_dump()},
            }
        ),
        receipt,
    )


async def until(predicate):
    async with asyncio.timeout(8):
        while not predicate():
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_actual_failure_primary_releases_one_turn_and_starts_waiting_input(db, monkeypatch, caplog):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    try:
        await e.channel.handle_update(message(e, 1071, content="Will fail"))
        await asyncio.wait_for(reached.wait(), 5)
        first = next(iter(turns._turns.values()))
        await until(lambda: state.posts == 1)
        await e.channel.handle_update(message(e, 1072, content="Next input"))
        state.terminal = terminal(first.record)
        first.wake.set()
        # Clear the controlled old run only when the next publication is queued.
        actual_dispatch = e.services.publications.dispatch_record

        async def next_dispatch(record, link):
            state.terminal = None
            return await actual_dispatch(record, link)

        e.services.publications.dispatch_record = next_dispatch
        await until(lambda: state.posts == 2)
        assert first.done.is_set() and state.plans == 0
        assert next(iter(turns._turns.values())).inbound.text == "Next input"
        chat = f"link={e.link.id} bot={e.link.bot_id} turn={first.record.publication_id} run=run-{first.record.publication_id}"
        assert f"chat_run_failed {chat} status=error outcome=error.generic" in caplog.text
        async with db.sessions() as session:
            row = await session.get(TelegramInboundMarkRow, (e.link.id, 1071, e.link.chat_id))
            primary = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + first.record.turn_id))
            assert row.route_complete and row.outbox_id == primary.id
            assert primary.parts[0]["source_ref"] == {"key": "error.generic", "values": {}}
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_capacity_retry_keeps_logical_turn_deadline_and_gives_up_once(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    try:
        await e.channel.handle_update(message(e, 1081, content="Capacity input"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        original = first.record
        state.terminal = terminal(original, reason="sandbox_capacity_exceeded")
        first.wake.set()
        await until(lambda: first.waiting_since is not None)
        deadline_start = first.waiting_since
        e.clock.advance(10)
        first.wake.set()
        await until(lambda: state.posts == 2)
        assert first is next(iter(turns._turns.values())) and first.waiting_since == deadline_start
        assert first.record.turn_id == original.turn_id and first.record.publication_id != original.publication_id
        state.terminal = terminal(first.record, reason="sandbox_capacity_exceeded")
        e.clock.advance(111)
        first.wake.set()
        await until(first.done.is_set)
        assert state.posts == 2 and not turns.queue._turns
        async with db.sessions() as session:
            primary = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + original.turn_id))
            assert primary.parts[0]["source_ref"]["key"] == "busy.gave_up"
            notices = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:capacity:" + original.turn_id)))
            assert len(notices) == 1
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_stop_during_thread_preparation_closes_actual_input_and_dropped_waiters_without_dispatch(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _ = await controlled_execution(e, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def held_thread(*args, **kwargs):
        entered.set()
        await release.wait()
        return "admitted-thread-result"

    e.services.threads.ensure_thread = held_thread
    try:
        await e.channel.handle_update(message(e, 1091, content="Stop before thread returns"))
        await asyncio.wait_for(entered.wait(), 5)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(message(e, 1092, content="Dropped waiting input"))
        await e.channel.handle_update(message(e, 1093, content="/stop"))
        assert first.stop_requested and first.record is None and state.posts == 0
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        release.set()
        await until(first.done.is_set)
        assert state.posts == 0 and state.stop_calls == 0
        async with db.sessions() as session:
            rows = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([1091, 1092, 1093]))))
            assert len(rows) == 3 and all(row.route_complete and row.turn_id is None for row in rows)
            assert all(row.run_id is None and not row.route_basis.get("publications") for row in rows)
    finally:
        release.set()
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_retained_final_plan_survives_phase_one_while_new_create_and_ui_stay_closed(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    reading = [True]
    e.services.publication_read_allowed = lambda: reading[0]
    try:
        await e.channel.handle_update(message(e, 1101, content="Original accepted input"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        e.active[0] = False
        assert turns._current(first) and not turns._creating(first)
        state.terminal = terminal(first.record)
        first.wake.set()
        await until(first.done.is_set)
        assert state.posts == 1
        async with db.sessions() as session:
            row = await session.get(TelegramInboundMarkRow, (e.link.id, 1101, e.link.chat_id))
            assert row.route_complete and row.outbox_id is not None
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_actual_missing_custom_configuration_closes_complete_group_without_thread_http(db, monkeypatch):
    from app.telegram_bots.supervisor import TelegramSupervisor
    from app.telegram_bots.threads import ThreadCoordinator
    from deerflow.config import agents_config

    e = await ingress(db, monkeypatch=monkeypatch)
    turns, state, _ = await controlled_execution(e, monkeypatch)
    e.services._app_config = e.live.config
    e.services.prepare_ingress_values = MethodType(TelegramSupervisor.prepare_ingress_values, e.services)

    def missing(*args, **kwargs):
        raise FileNotFoundError("controlled actual missing custom configuration")

    monkeypatch.setattr(agents_config, "load_agent_config", missing)
    controller = ThreadCoordinator(links=db.links, authority=e.live.authority, client=e.services.thread_client, channel=e.services.channel, send_allowed=e.services.send_allowed, outbox=e.services.outbox, product_name="HartMesh")
    e.services.threads = controller
    origins = []
    try:
        for identity in (1111, 1112):
            incoming = sdk_message(e, identity, content=f"Kept input {identity}")
            row, _ = await e.services.outbox.inbox.accept(e.channel.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
            row = await e.services.outbox.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
            origins.append(row)
            turns._origins[(e.link.id, identity)] = row
            turns._origin_channels[(e.link.id, identity)] = e.channel
        inbound = turns._inbound(e.channel.link, origins)
        assert turns.queue.admit(e.channel.link, inbound) == "run"
        turns._start(e.channel, e.channel.link, inbound)
        turn = next(iter(turns._turns.values()))
        await asyncio.wait_for(asyncio.shield(turn.task), 8)
        assert turn.done.is_set() and state.posts == 0
        async with db.sessions() as session:
            rows = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([1111, 1112]))))
            assert len(rows) == 2 and all(row.route_complete and row.turn_id is None for row in rows)
            assert rows[0].delivery_ids == rows[1].delivery_ids and len(rows[0].delivery_ids) == 1
            primary = await session.get(TelegramOutboxRow, rows[0].delivery_ids[0])
            assert primary.parts[0]["source_ref"]["key"] == "custom_agent.deleted"
    finally:
        await turns.stop()
        await controller.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_stop_after_reserve_before_marker_prevents_actual_dispatch_and_seals_without_a_run(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _ = await controlled_execution(e, monkeypatch)
    entered, release, refused = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def held_marker(record, link):
        owner = await e.services.prepare_live_owner(link.owner_user_id)
        guard = e.services.links.publication_guard(link, check_live_owner=owner, check_attempt_current=lambda: e.channel._callback_current(link))
        entered.set()
        await release.wait()
        try:
            await e.services.outbox.inbox.mark_dispatch_started(record, guard=guard)
        except ValueError:
            refused.set()
            return record
        raise AssertionError("The actual stored Stop/closure must fence the original marker")

    e.services.publications.dispatch_record = held_marker
    try:
        await e.channel.handle_update(message(e, 1121, content="Stop the reserved original publication"))
        await asyncio.wait_for(entered.wait(), 5)
        first = next(iter(turns._turns.values()))
        assert first.record is not None and not first.record.dispatch_started
        await e.channel.handle_update(message(e, 1122, content="/stop"))
        assert first.done.is_set()
        release.set()
        await asyncio.wait_for(refused.wait(), 5)
        await first.task
        async with db.sessions() as session:
            row = await session.get(TelegramInboundMarkRow, (e.link.id, 1121, e.link.chat_id))
            assert row.route_complete and row.run_id is None
            assert row.route_basis["publications"][-1]["publication_execution"]["dispatch_started"] is False
            reply = await session.get(TelegramOutboxRow, row.outbox_id)
            assert reply.parts[0]["source_ref"]["key"] == "busy.cancelled"
            assert reply.payload["turn_outcome_basis"]["proof"]["kind"] == "before_dispatch_stop"
    finally:
        release.set()
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("retire", [False, True])
async def test_startup_only_closes_frozen_complete_input_and_never_enqueues(db, monkeypatch, retire):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _ = await controlled_execution(e, monkeypatch)
    e.services._copy_decision = False
    e.services._cohort_complete = e.services._ready = True
    e.services._send_ready = e.services._stopping = e.services._stopped = False
    e.services._cause = None
    row, _ = await e.services.outbox.inbox.accept(e.channel.link, sdk_message(e, 1131, content="Retained original input"), sanitized_input={"type": "text", "text": "Retained original input"})
    await e.services.outbox.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    original = e.services.outbox.inbox.prepublication_page

    async def page(**kwargs):
        result = await original(**kwargs)
        if retire:
            e.services._send_ready = True
        return result

    monkeypatch.setattr(e.services.outbox.inbox, "prepublication_page", page)

    # Fixed restart closure does not acquire model/create or private-read scope.
    async def no_target_reload(*args):
        pytest.fail("A fixed startup input closure must not reload a target or acquire execution authority")

    e.services.prepare_ingress_values = no_target_reload
    try:
        await turns.recover_startup()
        assert not turns._turns and state.posts == 0
        async with db.sessions() as session:
            saved = await session.get(TelegramInboundMarkRow, (e.link.id, 1131, e.link.chat_id))
            assert saved.route_complete is not retire and saved.turn_id is None and saved.run_id is None
            if not retire:
                reply = await session.get(TelegramOutboxRow, saved.delivery_ids[-1])
                assert reply.parts[0]["source_ref"]["key"] == "chat.restarted"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing_link", "refused_entry"])
async def test_stale_startup_entry_does_not_abort_the_following_actual_input(db, monkeypatch, fault):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _ = await controlled_execution(e, monkeypatch)
    e.services._copy_decision = False
    e.services._cohort_complete = e.services._ready = True
    e.services._send_ready = e.services._stopping = e.services._stopped = False
    e.services._cause = None
    for identity in (1141, 1142):
        row, _ = await e.services.outbox.inbox.accept(e.channel.link, sdk_message(e, identity, content="Kept original"), sanitized_input={"type": "text", "text": "Kept original"})
        await e.services.outbox.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    original_get, original_plan = e.services.links.get, e.services.outbox.plan_input_outcome
    calls = []

    async def get(*args, **kwargs):
        calls.append("get")
        return None if len(calls) == 1 else await original_get(*args, **kwargs)

    async def plan(*args, **kwargs):
        calls.append("plan")
        if len(calls) == 1:
            raise ValueError("controlled stale-generation input")
        return await original_plan(*args, **kwargs)

    if fault == "missing_link":
        monkeypatch.setattr(e.services.links, "get", get)
    else:
        monkeypatch.setattr(e.services.outbox, "plan_input_outcome", plan)
    try:
        await turns.recover_startup()
        async with db.sessions() as session:
            first = await session.get(TelegramInboundMarkRow, (e.link.id, 1141, e.link.chat_id))
            second = await session.get(TelegramInboundMarkRow, (e.link.id, 1142, e.link.chat_id))
            assert not first.route_complete and second.route_complete
        assert state.posts == 0
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["refused", "terminal", "requested"])
async def test_only_accepted_stop_drops_waiters_and_refusal_keeps_later_input(db, monkeypatch, outcome):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    read_gate = asyncio.Event()

    async def read_run(*args):
        await read_gate.wait()
        return None

    async def request_stop(record, link, *, stop_id):
        attempt = await e.services.publication_authority.capture(link)
        fresh = await e.services.outbox.inbox.request_publication_stop(record, stop_id=stop_id, guard=attempt.cancel_guard)
        if outcome == "refused":
            await e.services.outbox.inbox.clear_publication_stop(fresh, stop_id=stop_id, guard=attempt.cancel_guard)
        return outcome

    turns._observer.read_run = read_run
    turns._observer.request_stop = request_stop
    try:
        await e.channel.handle_update(message(e, 1151, content="Current input"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(message(e, 1152, content="Original waiter"))
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        await e.channel.handle_update(message(e, 1153, content="/stop"))
        if outcome == "requested":
            assert turns.queue.waiting_count(e.channel.link, None) == 0 and first.stop_requested
        else:
            assert turns.queue.waiting_count(e.channel.link, None) == 1
            assert not first.stop_requested and not first.cancel_future and not first.cancelled_waiting
            await e.channel.handle_update(message(e, 1154, content="New input after stop decision"))
            assert turns.queue.waiting_count(e.channel.link, None) == 2
        assert not first.done.is_set()
    finally:
        read_gate.set()
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_publication_is_pinned_before_mark_and_stream_eof_does_not_release(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    try:
        assert await e.channel.handle_update(message(e, 1061, content="Original input"))
        await asyncio.wait_for(reached.wait(), 5)
        assert state.pinned is True
        await until(lambda: state.posts == 1)
        turn = next(iter(turns._turns.values()))
        assert state.posts == 1 and turn.record is not None
        assert state.plans == 0 and not turn.done.is_set()
        assert turns.queue._turns
        attempt = await e.services.publication_authority.capture(e.channel.link)
        fresh, origins = await e.services.outbox.inbox.read_publication_input(turn.record, guard=attempt.read_guard)
        assert fresh.dispatch_started and all(not row.route_complete for row in origins)
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_terminal_metadata_without_eligible_output_keeps_turn_and_waiters(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    checked = asyncio.Event()

    async def unavailable(*args, **kwargs):
        checked.set()
        return None

    turns._observer.plan_final = unavailable
    try:
        await e.channel.handle_update(message(e, 1065, content="Output must be qualified"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(message(e, 1066, content="Keep this waiter"))
        state.terminal = terminal(first.record, status="success")
        first.wake.set()
        await asyncio.wait_for(checked.wait(), 5)
        assert not first.done.is_set() and state.posts == 1
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        async with db.sessions() as session:
            saved = await session.get(TelegramInboundMarkRow, (e.link.id, 1065, e.link.chat_id))
            assert not saved.route_complete and saved.outbox_id is None
    finally:
        await turns.stop()
        await e.channel.stop()


async def finished_turn(e, turns, state, reached, *, status="success"):
    """A turn whose run reached `status`, with a later message waiting behind it."""
    await e.channel.handle_update(message(e, 1068, content="Answer with a card"))
    await asyncio.wait_for(reached.wait(), 5)
    await until(lambda: state.posts == 1)
    first = next(iter(turns._turns.values()))
    await e.channel.handle_update(message(e, 1069, content="And then this"))
    state.terminal = terminal(first.record, status=status)
    first.wake.set()
    return first


async def past_the_bound(turn):
    await until(lambda: turn.run_id is not None and turn.quiet_since is not None)
    turn.quiet_since -= 1000
    turn.wake.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, ValueError("the output cannot be carried")])
async def test_a_finished_run_whose_reply_cannot_be_planned_ends_visibly(db, monkeypatch, refusal, caplog):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    outcomes = []

    async def unplannable(*args, **kwargs):
        if refusal is not None:
            raise refusal

    async def outcome(turn, key, *, run=None, refusal=None, undeliverable=False):
        outcomes.append((key, undeliverable))
        return "a" * 32

    turns._observer.plan_final = unplannable
    monkeypatch.setattr(turns, "_publication_outcome", outcome)
    try:
        first = await finished_turn(e, turns, state, reached)
        await until(lambda: first.run_id is not None and first.quiet_since is not None)
        assert not first.done.is_set() and outcomes == [], "a short wait is an ordinary one"

        await past_the_bound(first)
        await asyncio.wait_for(first.done.wait(), 5)
        assert outcomes == [("error.generic", True)]
        logged = caplog.text
        chat = f"link={e.link.id} bot={e.link.bot_id} turn={first.record.publication_id} run=run-{first.record.publication_id}"
        assert f"chat_final_undeliverable {chat} output=unavailable:unsupported_output" in logged
        assert refusal is None or (f"chat_final_unplanned {chat} cause=ValueError:test_telegram_chat_turns.py:" in logged and "cannot be carried" not in logged)
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_turn_that_goes_on_says_so_from_the_driver(db, monkeypatch, caplog):
    from app.telegram_bots.turns import TURN_WAIT_REPORT_SECONDS

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    clock = {"now": 1000.0}
    e.channel._time = lambda: clock["now"]
    try:
        await e.channel.handle_update(message(e, 1067, content="Take your time"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        await until(lambda: first.observed_since is not None)
        assert "chat_turn_waiting" not in caplog.text

        clock["now"] += TURN_WAIT_REPORT_SECONDS
        first.wake.set()
        await until(lambda: first.wait_reported)
        assert f"chat_turn_waiting link={e.link.id} bot={e.link.bot_id} turn={first.record.publication_id} worker=taken waited_s={TURN_WAIT_REPORT_SECONDS}" in caplog.text
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_the_bound_starts_when_an_attempt_ends_so_a_slow_one_is_retried(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    clock, tries = {"now": 1000.0}, []
    e.channel._time = lambda: clock["now"]

    async def slow(*args, **kwargs):
        tries.append(clock["now"])
        clock["now"] += 90
        return "b" * 32 if len(tries) == 2 else None

    turns._observer.plan_final = slow
    try:
        first = await finished_turn(e, turns, state, reached)
        await asyncio.wait_for(first.done.wait(), 5)
        assert len(tries) == 2, "Ninety seconds in the first attempt must not spend the retry"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_reply_that_did_commit_wins_over_the_late_error(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    committed = []

    async def lost_result(*args, **kwargs):
        return "c" * 32 if committed else None

    async def conflict(turn, key, **kwargs):
        committed.append(key)
        raise ValueError("product-source-conflict")

    turns._observer.plan_final = lost_result
    monkeypatch.setattr(turns, "_publication_outcome", conflict)
    try:
        first = await finished_turn(e, turns, state, reached)
        await past_the_bound(first)
        await until(lambda: committed)
        assert not first.done.is_set(), "A refused error plan completes nothing"
        first.wake.set()
        await asyncio.wait_for(first.done.wait(), 5)
        assert committed == ["error.generic"], "The next read found the reply, and nothing is planned over it"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_stop_that_never_got_an_answer_cannot_hold_an_interrupted_run_forever(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    outcomes = []

    async def no_proof_of_our_stop(*args, stopped=False, **kwargs):
        assert stopped is True

    async def outcome(turn, key, *, run=None, refusal=None, undeliverable=False):
        outcomes.append((key, undeliverable))
        return "d" * 32

    turns._observer.plan_final = no_proof_of_our_stop
    monkeypatch.setattr(turns, "_publication_outcome", outcome)
    try:
        await e.channel.handle_update(message(e, 1068, content="Answer with a card"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(message(e, 1069, content="And then this"))
        first.stop_requested = True
        state.terminal = terminal(first.record, status="interrupted")
        first.wake.set()
        await until(lambda: first.run_id is not None and first.quiet_since is not None)
        assert not first.done.is_set() and outcomes == [], "An accepted Stop's proof is still worth a short wait"

        await past_the_bound(first)
        await asyncio.wait_for(first.done.wait(), 5)
        assert outcomes == [("error.generic", False)], "It closes as the interrupted run it is, under the ordinary run proof"
        assert not first.waiting_dropped, "An unproved Stop drops nobody's later message"
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_failed_owned_read_closes_advisory_view_while_durable_turn_stays_held(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)

    async def refused(*args):
        raise ValueError("controlled fresh source authority refusal")

    turns._observer.read_run = refused
    try:
        await e.channel.handle_update(message(e, 1067, content="Hold source failure"))
        await asyncio.wait_for(reached.wait(), 5)
        first = next(iter(turns._turns.values()))
        await until(lambda: first.presentation is not None and first.presentation.closed)
        assert not first.task.done()
        assert first.presentation.closed
        assert not first.done.is_set() and turns.queue._turns
        assert state.posts == 1
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["read", "committed_outcome"])
async def test_transient_observation_failure_recovers_same_publication_and_releases_waiter(db, monkeypatch, fault, caplog):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    failed, release = asyncio.Event(), asyncio.Event()
    original_read = turns._observer.read_run
    original_plan = e.services.outbox.plan_publication_outcome

    async def read(*args):
        if fault == "read" and not release.is_set():
            failed.set()
            raise httpx.ReadError("offline transient read failure")
        return await original_read(*args)

    async def plan(*args, **kwargs):
        identity = await original_plan(*args, **kwargs)
        if fault == "committed_outcome" and not release.is_set():
            failed.set()
            raise OSError("lost local result after actual terminal plan commit")
        return identity

    turns._observer.read_run = read
    monkeypatch.setattr(e.services.outbox, "plan_publication_outcome", plan)
    try:
        await e.channel.handle_update(message(e, 1301, content="Keep this exact publication"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        original = first.record.publication_id
        await e.channel.handle_update(message(e, 1302, content="Run after its terminal plan"))
        if fault == "committed_outcome":
            state.terminal = terminal(first.record)
            first.wake.set()
        await asyncio.wait_for(failed.wait(), 5)
        await until(lambda: first.presentation.closed)
        assert not first.task.done(), "A transient safe observation failure must retain a recovery driver"
        assert state.posts == 1 and first.record.publication_id == original
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        assert f"cause={'ReadError' if fault == 'read' else 'OSError'}:test_telegram_chat_turns.py:" in caplog.text and "transient read failure" not in caplog.text and "lost local result" not in caplog.text
        state.terminal = terminal(first.record)
        release.set()
        first.wake.set()
        await until(lambda: first.done.is_set() and state.posts == 2)
        assert first.record.publication_id == original
        async with db.sessions() as session:
            primaries = list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + first.record.turn_id)))
            assert len(primaries) == 1
    finally:
        release.set()
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_stop_while_group_preparation_returns_closes_input_without_second_gesture(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _ = await controlled_execution(e, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.services.outbox.inbox.group_prepublication

    async def group(*args, **kwargs):
        result = await original(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(e.services.outbox.inbox, "group_prepublication", group)
    try:
        await e.channel.handle_update(message(e, 1311, content="Stop at the original creating fence"))
        await asyncio.wait_for(entered.wait(), 5)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(message(e, 1312, content="Discard this waiting input"))
        await e.channel.handle_update(message(e, 1313, content="/stop"))
        release.set()
        await asyncio.wait_for(asyncio.shield(first.task), 5)
        assert first.done.is_set() and state.posts == 0
        async with db.sessions() as session:
            rows = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([1311, 1312, 1313]))))
            assert len(rows) == 3 and all(row.route_complete and row.turn_id is None for row in rows)
    finally:
        release.set()
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_followup_does_not_emit_typing_while_the_current_draft_is_alive(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    try:
        await e.channel.handle_update(message(e, 1321, content="Initial typing is allowed"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1 and next(iter(turns._turns.values())).presentation is not None)
        first = next(iter(turns._turns.values()))
        # The view seam isolates the actual adapter/limiter admission policy;
        # the separate full composition exercises a real SDK draft.
        first.presentation.mode, first.presentation.started, first.presentation.paused = "draft", True, False
        await e.channel._drain_ingress()
        before = sum(call.endpoint == "sendChatAction" for call in e.request.calls)
        await e.channel.handle_update(message(e, 1322, content="Wait behind that draft"))
        await e.channel._drain_ingress()
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        assert sum(call.endpoint == "sendChatAction" for call in e.request.calls) == before
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_text_is_durable_before_execution_and_waiters_never_get_a_worker(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    module = importlib.import_module("app.telegram_bots.turns")
    turns = module.ChatTurns(e.services)
    e.services.turns = turns
    entered, release = asyncio.Event(), asyncio.Event()
    captured = []

    async def drive(turn):
        async with db.sessions() as session:
            row = await session.get(TelegramInboundMarkRow, (e.link.id, 1001, e.link.chat_id))
            assert row.route_kind == "turn" and row.sanitized_input == {"type": "text", "text": "First request"}
        captured.append(turn)
        entered.set()
        await release.wait()

    monkeypatch.setattr(turns, "_drive", drive)
    try:
        assert await e.channel.handle_update(message(e, 1001, content="First request"))
        await asyncio.wait_for(entered.wait(), 3)
        assert await e.channel.handle_update(message(e, 1002, content="Second request"))
        assert len(captured) == 1
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        async with db.sessions() as session:
            marks = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([1001, 1002]))))
            assert len(marks) == 2 and all(row.route_kind == "turn" and not row.route_complete for row in marks)
            notices = list(await session.scalars(select(TelegramOutboxRow)))
            assert sum(any(part.get("source_ref", {}).get("key") == "busy.followup" for part in row.parts) for row in notices) == 1
        assert await e.channel.handle_update(message(e, 1002, content="Second request"))
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        assert len(captured) == 1
    finally:
        release.set()
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_same_update_after_controller_restart_is_not_dispatched_again(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    module = importlib.import_module("app.telegram_bots.turns")
    row, _ = await e.services.outbox.inbox.accept(e.channel.link, message(e, 1011, content="Kept input"), sanitized_input={"type": "text", "text": "Kept input"})
    await e.services.outbox.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    turns = module.ChatTurns(e.services)
    e.services.turns = turns

    async def forbidden(_turn):
        pytest.fail("A retained update must be resolved by recovery, never republished by redelivery")

    monkeypatch.setattr(turns, "_drive", forbidden)
    try:
        assert await e.channel.handle_update(message(e, 1011, content="Kept input"))
        assert not turns._tasks
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_forwarded_material_never_becomes_trusted_plain_input(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _reached = await controlled_execution(e, monkeypatch)
    payload = message(e, 1021, content="Someone else's instructions").to_dict()
    payload["message"]["forward_origin"] = {"type": "hidden_user", "date": 1_893_456_000, "sender_user_name": "External author"}
    update = Update.de_json(payload, e.channel.bot)

    try:
        assert await e.channel.handle_update(update)
        assert state.posts == 0  # The short join wait occupies no execution slot.
        async with db.sessions() as session:
            row = await session.get(TelegramInboundMarkRow, (e.link.id, 1021, e.link.chat_id))
            assert row.route_basis["input_profile"] == {"version": 2, "kind": "chat_input", "forwarded": True, "origin": "External author"}
        await turns.flush_inputs(e.channel)
        await until(lambda: state.posts == 1)
        turn = next(iter(turns._turns.values()))
        assert turn.inbound.text.startswith("Forwarded message from External author. Someone else wrote the text below.")
        assert "not an instruction from the person" in turn.inbound.text
        assert turn.inbound.text.endswith("Someone else's instructions")
        assert turn.origins[0].message_id == 1021
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["accept", "route"])
async def test_retirement_while_input_write_settles_never_occupies_queue(db, monkeypatch, boundary):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    module = importlib.import_module("app.telegram_bots.turns")
    turns = module.ChatTurns(e.services)
    e.services.turns = turns
    inbox = e.services.outbox.inbox
    actual = getattr(inbox, boundary)

    async def retire_after(*args, **kwargs):
        result = await actual(*args, **kwargs)
        e.active[0] = False
        return result

    monkeypatch.setattr(inbox, boundary, retire_after)
    try:
        assert await e.channel.handle_update(message(e, 1031, content="Save before retirement")) is False, "The next channel must get this update"
        assert not turns._tasks and not turns._turns and not turns.queue._turns
        async with db.sessions() as session:
            row = await session.get(TelegramInboundMarkRow, (e.link.id, 1031, e.link.chat_id))
            assert row is not None and not row.route_complete
            assert row.route_kind == (None if boundary == "accept" else "turn")
        monkeypatch.setattr(inbox, boundary, actual)
        e.active[0] = True
        assert await e.channel.handle_update(message(e, 1031, content="Save before retirement")) is True
        assert turns._turns
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_retire_fences_queue_then_drains_original_owned_task_cleanup(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns = importlib.import_module("app.telegram_bots.turns").ChatTurns(e.services)
    e.services.turns = turns
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def drive(_turn):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
            await release.wait()

    monkeypatch.setattr(turns, "_drive", drive)
    retiring = None
    try:
        assert await e.channel.handle_update(message(e, 1041, content="Original active input"))
        await asyncio.wait_for(entered.wait(), 3)
        assert await e.channel.handle_update(message(e, 1042, content="Original waiting input"))
        retiring = asyncio.create_task(turns.retire(e.channel))
        await asyncio.wait_for(cancelled.wait(), 3)
        assert not retiring.done()
        assert not turns._turns and not turns.queue._turns
        release.set()
        await retiring
        assert not turns._tasks
        async with db.sessions() as session:
            rows = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([1041, 1042]))))
            assert len(rows) == 2 and all(not row.route_complete for row in rows)
    finally:
        release.set()
        if retiring is not None:
            await asyncio.gather(retiring, return_exceptions=True)
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_deliberate_callback_waits_as_one_sql_owned_unit_then_dispatches_its_kept_input_once(retry_input_environment, monkeypatch):
    from app.telegram_bots.threads import ThreadCoordinator

    e = retry_input_environment

    class Channel:
        def __init__(self):
            self.link, self.transport_revision = e.link, e.link.token_revision
            self.application, self._stopping = SimpleNamespace(running=True), False

        @staticmethod
        def _same_scope(left, right):
            return left == right

        def _callback_current(self, link):
            return self.link == link and e.current[0] is self and e.creating[0]

        @staticmethod
        def _time():
            return asyncio.get_running_loop().time()

    channel = Channel()
    e.current[0] = channel

    async def values(link):
        return {"name": "HartMesh", "product": "HartMesh"}

    services = SimpleNamespace(
        config=e.db.config,
        links=e.db.links,
        outbox=e.outbox,
        callbacks=e.callbacks,
        thread_client=e.http,
        publications=e.coordinator,
        publication_authority=e.host,
        channel=lambda identity: channel if identity == e.link.id else None,
        prepare_live_owner=e.live.authority.prepare_live_owner,
        prepare_ingress_values=values,
        product_name="HartMesh",
    )
    channel._services = services
    threads = ThreadCoordinator(links=e.db.links, authority=e.live.authority, client=e.http, channel=services.channel, send_allowed=lambda: e.creating[0], outbox=e.outbox, product_name="HartMesh")
    services.threads = threads
    turns = importlib.import_module("app.telegram_bots.turns").ChatTurns(services)
    monkeypatch.setattr(importlib.import_module("app.telegram_bots.streaming"), "LivePresentation", ControlledView)
    first_entered, hold = asyncio.Event(), asyncio.Event()
    actual_drive = turns._drive

    async def drive(turn):
        if turn.callback_id is None:
            first_entered.set()
            await hold.wait()
        else:
            await actual_drive(turn)

    async def none(*args, **kwargs):
        return None

    turns._observer = SimpleNamespace(watch=none, read_run=none, retire=lambda record: None, drain=none)
    monkeypatch.setattr(turns, "_drive", drive)
    original = await e.add_origin(message_id=1201, text="Current newer input")
    inbound = turns._inbound(e.link, (original,))
    turns._origins[(e.link.id, 1201)] = original
    turns._origin_channels[(e.link.id, 1201)] = channel
    turns.queue.admit(e.link, inbound)
    turns._start(channel, e.link, inbound)
    try:
        await asyncio.wait_for(first_entered.wait(), 5)
        first = next(iter(turns._turns.values()))
        row = await e.callbacks.get(e.callback_id)
        await turns.execute_callback(channel, row, update_id=3901, query_id="actual-press")
        await turns.execute_callback(channel, row, update_id=3901, query_id="same-press")
        queued = turns.queue.waiting_inputs(e.link, None)
        assert len(queued) == 1 and queued[0].callback_id == e.callback_id
        assert queued[0].inbound.metadata["telegram_source_message_ids"] == [701, 702]
        with pytest.raises(asyncio.QueueEmpty):
            e.bus.get_inbound_nowait()
        # A positive SQL completion of the controlled current turn precedes its
        # single queue release. No model/output qualification is claimed here.
        guard = turns._guard(first)
        await e.outbox.plan_input_outcome(e.link, source_id="controlled-current-finished", source_ref={"key": "error.generic", "values": {}}, keyboard=(), callbacks=e.callbacks, guard=guard, inbox_rows=(original,))
        await turns._complete(first)
        callback = next(iter(turns._turns.values()))
        await until(lambda: callback.record is not None and callback.record.dispatch_started)
        sent = e.bus.get_inbound_nowait()
        assert sent.metadata["channel_turn_id"] == callback.record.publication_id
        assert sent.text == "\n\n".join(item.sanitized_input["text"] for item in e.origins)
        e.bus.inbound_task_done()
        with pytest.raises(asyncio.QueueEmpty):
            e.bus.get_inbound_nowait()
        saved = await e.inbox.read_callback_input(e.link, e.callback_id, guard=turns._guard(callback))
        assert saved.record.publication_id == callback.record.publication_id
        assert saved.record.owner_callback_id == e.callback_id and saved.record.dispatch_started
    finally:
        hold.set()
        await turns.stop()
        await threads.stop()


@pytest.mark.asyncio
async def test_public_stop_retries_failed_actual_joined_response_cleanup(coordinator_environment):
    from test_telegram_publication_observation import ACK, run_row

    from app.channels.execution import ChannelExecutionProtocolError
    from app.telegram_bots.progress import RunObserver

    e = coordinator_environment
    record = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    exact = run_row(record)
    exact["metadata"] = {"channel_turn_id": record.publication_id}

    class FailingResponse(httpx.AsyncByteStream):
        def __init__(self):
            self.entered, self.release = asyncio.Event(), asyncio.Event()
            self.allow_close, self.closed, self.attempts = False, False, 0

        async def __aiter__(self):
            yield b"event: values\ndata: {}\n\n"
            self.entered.set()
            await self.release.wait()

        async def aclose(self):
            self.attempts += 1
            if not self.allow_close:
                raise OSError("controlled actual HTTP response cleanup failure")
            self.closed = True

    body = FailingResponse()

    async def respond(request):
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=body)
        return httpx.Response(200, headers=ACK, json=exact)

    e.responder = respond
    observer = RunObserver(coordinator=e.coordinator, http_owner=e.http, capture_attempt=e.host.capture, is_current=lambda _record: True)
    turns = importlib.import_module("app.telegram_bots.turns").ChatTurns(SimpleNamespace(config=e.db.config))
    turns._observer = observer

    async def none(*args):
        return None

    watch = asyncio.create_task(observer.watch(record, e.link, on_preview=none, on_line=none, turn_done=asyncio.Event()))
    try:
        await asyncio.wait_for(body.entered.wait(), 5)
        with pytest.raises(ChannelExecutionProtocolError):
            await turns.stop()
        assert turns._closed and not body.closed and body.attempts > 0
        body.allow_close = True
        await turns.stop()
        assert body.closed and not observer._states
    finally:
        body.allow_close = True
        body.release.set()
        watch.cancel()
        await asyncio.gather(watch, return_exceptions=True)
        await observer.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [False, True, "refusal"])
async def test_lost_dispatch_result_observes_and_stops_original_run_without_second_enqueue(coordinator_environment, monkeypatch, retry):
    from test_telegram_publication_observation import ACK, run_row

    from app.channels.execution import ChannelExecutionBusyError
    from app.channels.execution_client import CHANNEL_EXECUTION_BUSY_CODE
    from app.channels.outcomes import CreationRefusedMessage, capture_creation_refusal
    from app.telegram_bots.callbacks import CallbackRepository

    e = coordinator_environment
    clock = [0.0]
    refusal_case = retry == "refusal"

    class Channel:
        def __init__(self):
            self.link, self.transport_revision = e.link, e.link.token_revision
            self.application, self._stopping = SimpleNamespace(running=True), False

        @staticmethod
        def _same_scope(left, right):
            return left == right

        def _callback_current(self, link):
            return self.link == link and e.current[0] is self and e.creating[0]

        async def _input_current(self, link):
            return self._callback_current(link)

        def _admission(self, update):
            return SimpleNamespace(link=self.link)

        def _classified(self, *args, **kwargs):
            pass

        @staticmethod
        def _time():
            return clock[0]

    channel = Channel()
    e.current[0] = channel

    async def values(link):
        return {"name": "HartMesh", "product": "HartMesh"}

    async def ensure(*args, **kwargs):
        return e.thread_id

    services = SimpleNamespace(
        config=e.db.config,
        links=e.db.links,
        outbox=e.outbox,
        callbacks=CallbackRepository(e.db.sessions),
        thread_client=e.http,
        publications=e.coordinator,
        publication_authority=e.host,
        threads=SimpleNamespace(ensure_thread=ensure),
        channel=lambda identity: channel if identity == e.link.id else None,
        prepare_live_owner=e.live.authority.prepare_live_owner,
        prepare_ingress_values=values,
        product_name="HartMesh",
    )
    channel._services = services
    turns = importlib.import_module("app.telegram_bots.turns").ChatTurns(services)
    monkeypatch.setattr(importlib.import_module("app.telegram_bots.streaming"), "LivePresentation", ControlledView)
    dispatched, cancelled, first_sent, observed_current = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()
    available, attempts, cancels, captured = [False], [], [], []
    received, refused_proofs = [], []
    actual_dispatch = e.coordinator.dispatch_record
    actual_retry = e.coordinator.retry

    async def lost(record, link):
        attempts.append(record.publication_id)
        captured.append(record)
        result = await actual_dispatch(record, link)
        first_sent.set()
        if retry:
            return result
        dispatched.set()
        raise OSError("controlled lost result after actual enqueue")

    async def lost_retry(previous, link, **kwargs):
        result = await actual_retry(previous, link, **kwargs)
        attempts.append(result.publication_id)
        captured.append(result)
        if refusal_case:
            messages = [e.bus.get_inbound_nowait(), e.bus.get_inbound_nowait()]
            for message in messages:
                received.append(message.metadata["channel_turn_id"])
                e.bus.inbound_task_done()
            context = messages[-1].execution_context
            try:
                await context.client.runs.wait(
                    context.thread_id,
                    context.assistant_id,
                    input={"messages": [{"type": "human", "content": messages[-1].text}]},
                    config={},
                    context={},
                    metadata={"channel_turn_id": context.publication_id},
                    multitask_strategy="reject",
                    channel_execution={"version": 1, "publication_id": context.publication_id},
                    on_disconnect="continue",
                    on_completion=None,
                    headers={"Idempotency-Key": context.publication_id},
                )
            except ChannelExecutionBusyError as error:
                await context.admission.authorize("run_read")
                proof = capture_creation_refusal(context, error)
                refused_proofs.append(proof)
                await turns.outbound(
                    channel,
                    CreationRefusedMessage(
                        channel_name="telegram", chat_id=context.scope.chat_id, text="Host busy", thread_id=context.thread_id, owner_user_id=context.owner_user_id, connection_id=context.connection_id, creation_refusal=proof
                    ),
                )
            else:
                raise AssertionError("The actual captured facade must reject the creating request with typed busy")
            dispatched.set()
            return result
        dispatched.set()
        raise OSError("controlled lost result after actual successor enqueue")

    async def respond(request):
        if request.url.path.endswith("/" + e.thread_id):
            return httpx.Response(200, json={"thread_id": e.thread_id, "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})
        if request.url.path.endswith("/cancel"):
            cancels.append(request.url.path)
            cancelled.set()
            return httpx.Response(202)
        if request.url.path.endswith("/join"):
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=b"")
        if refusal_case and request.method == "POST" and request.url.path.endswith("/runs/wait"):
            return httpx.Response(409, json={"detail": {"code": CHANNEL_EXECUTION_BUSY_CODE}})
        if len(captured) > 1 and request.method == "GET" and request.headers.get("idempotency-key") == captured[-1].publication_id:
            observed_current.set()
        predecessor = retry and captured and (request.headers.get("idempotency-key") == captured[0].publication_id or request.url.path.endswith("/initial-capacity-run"))
        if not predecessor and not available[0]:
            return httpx.Response(404, headers=ACK, json={"lookup": "idempotency-key", "detail": "Run not found"})
        selected = captured[0] if predecessor else captured[-1]
        row = run_row(selected, run_id="initial-capacity-run" if predecessor else "actual-lost-run")
        row["metadata"] = {"channel_turn_id": selected.publication_id}
        if predecessor:
            row.update(status="error", stop_reason="sandbox_capacity_exceeded")
        return httpx.Response(200, headers=ACK, json=row)

    monkeypatch.setattr(e.coordinator, "dispatch_record", lost)
    monkeypatch.setattr(e.coordinator, "retry", lost_retry)
    e.responder = respond
    inbound = turns._inbound(e.link, e.origins)
    for origin in e.origins:
        turns._origins[(e.link.id, origin.message_id)] = origin
        turns._origin_channels[(e.link.id, origin.message_id)] = channel
    turns.queue.admit(e.link, inbound)
    turns._start(channel, e.link, inbound)
    try:
        if retry:
            await asyncio.wait_for(first_sent.wait(), 5)
            turn = next(iter(turns._turns.values()))
            await until(lambda: turn.capacity_waiting)
            clock[0] = 10
            turn.wake.set()
        await asyncio.wait_for(dispatched.wait(), 5)
        turn = next(iter(turns._turns.values()))
        enqueued = list(received)
        if not refusal_case:
            for _ in range(2 if retry else 1):
                sent = e.bus.get_inbound_nowait()
                enqueued.append(sent.metadata["channel_turn_id"])
                e.bus.inbound_task_done()
        assert enqueued == attempts and enqueued[-1] == turn.record.publication_id
        if refusal_case:
            await asyncio.wait_for(observed_current.wait(), 5)
            assert len(refused_proofs) == 1 and turn.refusal is refused_proofs[0]
            assert len(attempts) == 2
            with pytest.raises(asyncio.QueueEmpty):
                e.bus.get_inbound_nowait()
            return
        incoming = Update(1501, message=Message(1501, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Original human", False), text="/stop"))
        await turns.stop_chat(channel, incoming)
        available[0] = True
        turn.wake.set()
        await asyncio.wait_for(cancelled.wait(), 5)
        assert len(attempts) == (2 if retry else 1)
        assert cancels == ["/original/api/threads/" + e.thread_id + "/runs/actual-lost-run/cancel"]
        with pytest.raises(asyncio.QueueEmpty):
            e.bus.get_inbound_nowait()
    finally:
        await turns.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("capacity", [True, False])
async def test_terminal_stop_cancels_only_a_proven_capacity_wait_otherwise_releases_waiters(db, monkeypatch, capacity):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    gate = asyncio.Event()
    original_read = turns._observer.read_run

    async def read(*args):
        await gate.wait()
        return await original_read(*args)

    turns._observer.read_run = read
    try:
        await e.channel.handle_update(message(e, 1161, content="Original terminal outcome"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(message(e, 1162, content="Waiting human input"))
        state.terminal = terminal(first.record, reason="sandbox_capacity_exceeded" if capacity else None)
        await e.channel.handle_update(message(e, 1163, content="/stop"))
        assert turns.queue.waiting_count(e.channel.link, None) == 1
        original_dispatch = e.services.publications.dispatch_record

        async def next_dispatch(record, link):
            state.terminal = None
            return await original_dispatch(record, link)

        e.services.publications.dispatch_record = next_dispatch
        gate.set()
        first.wake.set()
        await until(first.done.is_set)
        if not capacity:
            await until(lambda: state.posts == 2)
        async with db.sessions() as session:
            saved = await session.get(TelegramInboundMarkRow, (e.link.id, 1161, e.link.chat_id))
            waiting = await session.get(TelegramInboundMarkRow, (e.link.id, 1162, e.link.chat_id))
            reply = await session.get(TelegramOutboxRow, saved.outbox_id)
            assert reply.parts[0]["source_ref"]["key"] == ("busy.cancelled" if capacity else "error.generic")
            assert waiting.route_complete is capacity
        assert state.posts == (1 if capacity else 2)
    finally:
        gate.set()
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_failed_thread_request_ends_the_turn_and_creates_nothing_more(db, monkeypatch):
    from app.telegram_bots.gateway_client import GatewayClientOwner
    from app.telegram_bots.threads import ThreadCoordinator

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, _ = await controlled_execution(e, monkeypatch)
    calls = []

    def respond(request):
        calls.append((request.method, request.url.path))
        return httpx.Response(500, json={"detail": "controlled server failure"})

    client = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://gateway.example/api", transport=httpx.MockTransport(respond))
    threads = ThreadCoordinator(links=db.links, authority=e.live.authority, client=client, channel=e.services.channel, send_allowed=e.services.send_allowed, outbox=e.services.outbox, product_name="HartMesh")
    e.services.threads, e.services.thread_client = threads, client
    try:
        await e.channel.handle_update(message(e, 1171, content="Keep failed thread input"))
        first = next(iter(turns._turns.values()))
        await asyncio.wait_for(first.done.wait(), 8)
        assert any(method == "POST" and path.endswith("/threads") for method, path in calls)
        assert state.posts == 0 and not turns._turns
        async with db.sessions() as session:
            saved = await session.get(TelegramInboundMarkRow, (e.link.id, 1171, e.link.chat_id))
            assert saved.route_complete and saved.turn_id is None and saved.run_id is None
    finally:
        await turns.stop()
        await threads.stop()
        await client.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_accepted_stop_keeps_cancelled_waiting_fact_for_the_qualified_final_planner(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    original_stop, original_plan = turns._observer.request_stop, turns._observer.plan_final
    final_arguments = []

    async def accepted(record, link, *, stop_id):
        await original_stop(record, link, stop_id=stop_id)
        return "requested"

    async def planned(*args, **kwargs):
        final_arguments.append(kwargs)
        return await original_plan(*args, **kwargs)

    turns._observer.request_stop, turns._observer.plan_final = accepted, planned
    try:
        await e.channel.handle_update(message(e, 1181, content="Current answer"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(message(e, 1182, content="Waiting input to cancel"))
        await e.channel.handle_update(message(e, 1183, content="/stop"))
        assert first.cancelled_waiting and turns.queue.waiting_count(e.channel.link, None) == 0
        state.terminal = terminal(first.record, status="interrupted", reason="cancelled")
        first.wake.set()
        await until(first.done.is_set)
        assert final_arguments == [{"stopped": True, "waiting": True}]
        assert state.posts == 1
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_stop_admitted_for_finished_turn_never_moves_to_its_queued_successor(db, monkeypatch):
    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    try:
        await e.channel.handle_update(message(e, 1191, content="Original answer"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        await e.channel.handle_update(message(e, 1192, content="Later waiting answer"))
        actual_accept = e.services.outbox.inbox.accept
        actual_dispatch = e.services.publications.dispatch_record

        async def next_dispatch(record, link):
            state.terminal = None
            return await actual_dispatch(record, link)

        async def finish_during_accept(*args, **kwargs):
            result = await actual_accept(*args, **kwargs)
            if kwargs.get("sanitized_input") == {"command": "stop"}:
                state.terminal = terminal(first.record)
                first.wake.set()
                await until(lambda: state.posts == 2)
            return result

        e.services.publications.dispatch_record = next_dispatch
        monkeypatch.setattr(e.services.outbox.inbox, "accept", finish_during_accept)
        await e.channel.handle_update(message(e, 1193, content="/stop"))
        successor = next(iter(turns._turns.values()))
        assert first.done.is_set() and successor is not first
        assert not successor.stop_requested and successor.stop_id is None and state.stop_calls == 0
        async with db.sessions() as session:
            mark = await session.get(TelegramInboundMarkRow, (e.link.id, 1192, e.link.chat_id))
            assert "stop_id" not in mark.route_basis["publications"][-1]["publication_execution"]
    finally:
        await turns.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [True, False])
async def test_saved_cancel_wait_control_cannot_cancel_a_retry_that_is_already_running(db, monkeypatch, held):
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = await ingress(db, monkeypatch=monkeypatch, lead=True)
    turns, state, reached = await controlled_execution(e, monkeypatch)
    offered = asyncio.Event()
    actual_plan = e.services.outbox.plan_product_keyboard

    async def plan(*args, **kwargs):
        result = await actual_plan(*args, **kwargs)
        if kwargs.get("source_id", "").startswith("capacity:"):
            offered.set()
        return result

    monkeypatch.setattr(e.services.outbox, "plan_product_keyboard", plan)
    try:
        await e.channel.handle_update(message(e, 1211, content="Capacity retry input"))
        await asyncio.wait_for(reached.wait(), 5)
        await until(lambda: state.posts == 1)
        first = next(iter(turns._turns.values()))
        state.terminal = terminal(first.record, reason="sandbox_capacity_exceeded")
        first.wake.set()
        await asyncio.wait_for(offered.wait(), 8)
        async with db.sessions() as session:
            saved = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == e.link.id, TelegramCallbackRow.kind == "cancel_wait"))
            assert saved is not None
            callback_id, group_id, parent_id = saved.id, saved.group_id, saved.payload["anchor_outbox_id"]
        assert await e.services.callbacks.adopt(e.channel.link, group_id, 1600, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent_id, presentation_version=0, check_current=lambda: True)
        row = await e.services.callbacks.get(callback_id)
        if held:
            await turns.execute_callback(e.channel, row, update_id=1601, query_id="held-cancel")
            await until(first.done.is_set)
            assert state.posts == 1
        else:
            e.clock.advance(10)
            first.wake.set()
            await until(lambda: state.posts == 2)
            assert not first.capacity_waiting and first.waiting_since is not None
            with pytest.raises(CallbackRefused):
                await turns.execute_callback(e.channel, row, update_id=1601, query_id="old-wait-cancel")
            assert not first.stop_requested and state.stop_calls == 0
    finally:
        await turns.stop()
        await e.channel.stop()
