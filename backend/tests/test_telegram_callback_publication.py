"""Callback-owned input/publication receipts keep real human input identities."""

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, select
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.telegram_bots import inbox
from app.telegram_bots.callbacks import CallbackRepository
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.parametrize("payload,pending", [({}, False), ({"publication_owner": None}, True), ({"publication_owner": {"version": 1, "state": "done"}}, True)])
def test_sweep_holds_unknown_owner_presence_instead_of_inventing_completion(payload, pending):
    assert inbox.callback_publication_pending(payload) is pending


@pytest_asyncio.fixture
async def retry_input_environment(coordinator_environment, request):
    e = coordinator_environment
    e.callbacks = CallbackRepository(e.db.sessions, config=e.db.config)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)

    def guard():
        return e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: e.creating[0] and e.reading[0])

    e.guard_factory = guard
    if getattr(request, "param", None) != "unprepared":
        from app.channels.message_bus import InboundMessage
        from app.telegram_bots.threads import ThreadCoordinator

        await e.db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id=e.thread_id)
        controller = ThreadCoordinator(links=e.db.links, authority=e.live.authority, client=e.http, channel=lambda _id: e.current[0], send_allowed=lambda: e.creating[0], outbox=e.outbox, product_name="HartMesh")
        inbound = InboundMessage("telegram", str(e.link.chat_id), str(e.link.telegram_user_id), e.origins[0].sanitized_input["text"], connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, workspace_id=str(e.link.bot_id))
        try:
            assert await controller.ensure_thread(e.link, inbound, origin=e.origins[0]) == e.thread_id
        finally:
            await controller.stop()
    # Protocol disposition seeded through the actual fixed planner. This is
    # input/choice SQL coverage, not a claim that a process restart occurred.
    parent = await e.outbox.plan_product_keyboard(
        e.link,
        source_id="retry-input-unit",
        parts=[{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {"kept": True}}}],
        keyboard=[[{"label": "label.try_again", "kind": "retry_message", "target_kind": "thread", "target_id": e.thread_id, "payload": {}}]],
        callbacks=e.callbacks,
        guard=guard(),
        inbox_rows=e.origins,
        finish_route=True,
    )
    async with e.db.sessions() as session:
        row = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == e.link.id))
        e.callback_id, group = row.id, row.group_id
    assert await e.callbacks.adopt(e.link, group, 1901, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: True)

    async def stage():
        return await e.inbox.stage_callback_input(e.link, callback_id=e.callback_id, update_id=3901, chat_id=e.link.chat_id, message_id=1901, message_thread_id=None, callbacks=e.callbacks, guard=guard())

    e.stage = stage
    yield e


@pytest.mark.asyncio
async def test_callback_stage_snapshots_original_input_without_reopening_old_rows(retry_input_environment):
    e = retry_input_environment
    async with e.db.sessions() as session:
        before = [
            (row.message_id, row.route_complete, row.turn_id, row.outbox_id, list(row.delivery_ids))
            for row in await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id).order_by(TelegramInboundMarkRow.message_id))
        ]
        deliveries = set(await session.scalars(select(TelegramOutboxRow.id)))
    stage = await e.stage()
    assert stage.callback_id == e.callback_id and stage.record is None
    assert [row.message_id for row in stage.origins] == [701, 702]
    assert [row.sanitized_input for row in stage.origins] == [row.sanitized_input for row in e.origins]
    assert await e.stage() == stage
    assert (await e.callbacks.get(e.callback_id)).consumed_at is None
    async with e.db.sessions() as session:
        after = [
            (row.message_id, row.route_complete, row.turn_id, row.outbox_id, list(row.delivery_ids))
            for row in await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id).order_by(TelegramInboundMarkRow.message_id))
        ]
        assert after == before
        assert set(await session.scalars(select(TelegramOutboxRow.id))) == deliveries


@pytest.mark.asyncio
async def test_callback_input_truth_survives_original_inbox_gc(retry_input_environment):
    e = retry_input_environment
    stage = await e.stage()
    async with e.db.sessions.begin() as session:
        await session.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    restored = await e.inbox.read_callback_input(e.link, e.callback_id, guard=e.guard_factory())
    assert restored == stage
    assert [row.message_id for row in restored.origins] == [701, 702]
    assert inbox.callback_publication_pending((await e.callbacks.get(e.callback_id)).payload) is True


@pytest.mark.asyncio
async def test_callback_reservation_keeps_old_tombstone_and_has_its_own_exact_identity(retry_input_environment):
    e = retry_input_environment
    stage = await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
    assert record.owner_callback_id == e.callback_id and record.turn_id == e.callback_id
    assert record.event_id == "c" + e.callback_id and not record.dispatch_started
    assert record.input_sha256 == stage.input_sha256
    assert [origin.message_id for origin in record.origins] == [701, 702]
    assert await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory()) == record
    assert (await e.callbacks.get(e.callback_id)).consumed_at is None
    with pytest.raises(ValueError):
        await e.inbox.confirm_callback_dispatch(record, guard=e.guard_factory())
    fresh, inputs = await e.inbox.read_publication_input(record, guard=e.guard_factory())
    assert fresh == record and [row.message_id for row in inputs] == [701, 702]
    async with e.db.sessions() as session:
        marks = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id)))
        assert all(mark.route_complete and mark.turn_id is None and mark.run_id is None for mark in marks)


@pytest.mark.asyncio
async def test_callback_owner_marker_and_binding_survive_consumed_controls(retry_input_environment):
    e = retry_input_environment
    await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
    assert await e.inbox.mark_dispatch_started(record, guard=e.guard_factory()) is True
    assert (await e.callbacks.get(e.callback_id)).consumed_at is None
    # Explicit SQL protocol confirmation, not proof of an actual bus enqueue.
    confirmed = await e.inbox.confirm_callback_dispatch(record, guard=e.guard_factory())
    assert confirmed.dispatch_started and (await e.callbacks.get(e.callback_id)).consumed_at is not None
    bound = await e.inbox.bind_publication_run(confirmed, run_id="callback-exact-run", guard=e.guard_factory())
    assert bound.run_id == "callback-exact-run"
    reread, original = await e.inbox.read_publication_input(bound, guard=e.guard_factory())
    assert reread == bound and [row.message_id for row in original] == [701, 702]


@pytest.mark.asyncio
async def test_callback_dispatch_consumes_only_after_actual_bus_commit(retry_input_environment):
    e = retry_input_environment
    await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
    dispatched = await e.coordinator.dispatch_record(record, e.link)
    assert dispatched.dispatch_started and (await e.callbacks.get(e.callback_id)).consumed_at is not None
    message = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert message.metadata["event_id"] == "c" + e.callback_id
    assert message.metadata["channel_turn_id"] == record.publication_id
    assert message.text == "\n\n".join(row.sanitized_input["text"] for row in e.origins)
    assert message.metadata["message_id"] == e.origins[0].message_id
    assert (await e.coordinator.dispatch_record(record, e.link)).publication_id == record.publication_id
    assert e.bus.inbound_queue.empty()
    assert not any(method == "POST" for method, _, _ in e.requests)


@pytest.mark.asyncio
async def test_callback_prethread_fixed_outcome_is_atomic_and_new_retry_keeps_snapshot_after_gc(retry_input_environment):
    e = retry_input_environment
    first = await e.stage()
    async with e.db.sessions.begin() as session:
        await session.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    parent = await e.outbox.plan_input_outcome(
        e.link,
        source_id="callback-files-full:" + e.callback_id,
        source_ref={"key": "chat.files_full", "values": {}},
        keyboard=[[{"label": "label.try_again", "kind": "retry_message", "target_kind": "thread", "target_id": e.thread_id, "payload": {}}]],
        callbacks=e.callbacks,
        guard=e.guard_factory(),
        input_owner_callback_id=e.callback_id,
    )
    closed = await e.inbox.read_callback_input(e.link, e.callback_id, guard=e.guard_factory())
    assert closed.record is None and closed.route_complete and closed.origins == first.origins
    assert (await e.callbacks.get(e.callback_id)).consumed_at is not None
    async with e.db.sessions() as session:
        callback = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.id != e.callback_id, TelegramCallbackRow.link_id == e.link.id))
        callback_id, group_id = callback.id, callback.group_id
    assert await e.callbacks.adopt(e.link, group_id, 1902, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: True)
    restaged = await e.inbox.stage_callback_input(e.link, callback_id=callback_id, update_id=3902, chat_id=e.link.chat_id, message_id=1902, message_thread_id=None, callbacks=e.callbacks, guard=e.guard_factory())
    assert restaged.origins == first.origins and restaged.input_sha256 == first.input_sha256


@pytest.mark.asyncio
async def test_callback_published_recovery_is_not_lost_when_original_marks_are_done(retry_input_environment):
    e = retry_input_environment
    await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
    page = await e.inbox.recovery_page(after=None)
    assert len(page.entries) == 1 and page.entries[0].record == record and page.entries[0].hold_reason is None
    assert page.entries[0].origins == (await e.inbox.read_callback_input(e.link, e.callback_id, guard=e.guard_factory())).origins


@pytest.mark.asyncio
async def test_callback_false_marker_restart_closure_is_atomic_and_readable(retry_input_environment, monkeypatch):
    from _telegram_recovery_test_support import issue_restart_epoch

    e = retry_input_environment
    await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
    e.endpoint_id = e.http.endpoint_id
    host = issue_restart_epoch(e, monkeypatch, (record,))
    try:
        attempt = await host.epoch.capture(record, e.link)
        identity = await e.outbox.plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        admission = await host.epoch.prepare_receipt_read(record)
        receipt = await e.inbox.read_restart_receipt(record, captured_link=admission.captured_link, check_live_owner=admission.check_live_owner, check_current=admission.check_current)
        assert receipt.outbox_id == identity and receipt.record == record
        assert (await e.callbacks.get(e.callback_id)).consumed_at is not None
        with pytest.raises(ValueError):
            await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
    finally:
        await host.aclose()


@pytest.mark.asyncio
async def test_callback_capacity_successor_has_distinct_event_after_original_gc(retry_input_environment):
    from test_telegram_publication_retry import responder

    e = retry_input_environment
    await e.stage()
    old = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
    old = await e.coordinator.dispatch_record(old, e.link)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    async with e.db.sessions.begin() as session:
        await session.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    await responder(e, old)
    new = await e.coordinator.retry(old, e.link)
    assert new.owner_callback_id == e.callback_id and new.turn_id == old.turn_id
    assert new.event_id == "c" + e.callback_id + ":r1" and new.input_sha256 == old.input_sha256
    assert e.bus.inbound_queue.qsize() == 1
    assert (await e.callbacks.get(e.callback_id)).consumed_at is not None


@pytest.mark.asyncio
async def test_completed_callback_owner_settles_and_no_longer_blocks_sweep(retry_input_environment):
    e = retry_input_environment
    await e.stage()
    identity = await e.outbox.plan_input_outcome(
        e.link, source_id="fixed-finish:" + e.callback_id, source_ref={"key": "chat.files_full", "values": {}}, keyboard=[], callbacks=e.callbacks, guard=e.guard_factory(), input_owner_callback_id=e.callback_id
    )
    async with e.db.sessions.begin() as session:
        delivery = await session.get(TelegramOutboxRow, identity)
        delivery.state = "sent"
        await e.inbox.settle_deliveries(link_id=e.link.id, session=session)
    assert inbox.callback_publication_pending((await e.callbacks.get(e.callback_id)).payload) is False
    stage = await e.inbox.read_callback_input(e.link, e.callback_id, guard=e.guard_factory())
    assert stage.state == "done" and stage.route_complete and stage.origins


@pytest.mark.asyncio
async def test_callback_bound_fixed_outcome_keeps_its_input_for_next_explicit_retry_after_gc(retry_input_environment):
    from test_telegram_publication_retry import responder

    e = retry_input_environment
    first = await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
    record = await e.coordinator.dispatch_record(record, e.link)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    await responder(e, record, reason=None)
    found = await e.coordinator.locate_run(record, e.link, max_pages=1)
    assert found.state == "matched"
    async with e.db.sessions.begin() as session:
        await session.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    identity = await e.outbox.plan_publication_outcome(
        found.record,
        run=found.run,
        source_ref={"key": "error.generic", "values": {}},
        keyboard=[[{"label": "label.try_again", "kind": "retry_message", "target_kind": "thread", "target_id": e.thread_id, "payload": {}}]],
        callbacks=e.callbacks,
        guard=e.guard_factory(),
    )
    async with e.db.sessions() as session:
        parent = await session.get(TelegramOutboxRow, identity)
        assert parent.dedupe_key == "reply:" + record.turn_id
        callback = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.id != e.callback_id, TelegramCallbackRow.link_id == e.link.id))
        next_id, group = callback.id, callback.group_id
    assert await e.callbacks.adopt(e.link, group, 1903, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=identity, presentation_version=0, check_current=lambda: True)
    next_stage = await e.inbox.stage_callback_input(e.link, callback_id=next_id, update_id=3903, chat_id=e.link.chat_id, message_id=1903, message_thread_id=None, callbacks=e.callbacks, guard=e.guard_factory())
    assert next_stage.origins == first.origins and next_stage.input_sha256 == first.input_sha256


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_input_environment", ["unprepared"], indirect=True)
async def test_callback_reservation_requires_saved_source_thread_or_completed_preparation(retry_input_environment):
    e = retry_input_environment
    stage = await e.stage()
    assert stage.thread_id is None and stage.thread_preparation is None
    with pytest.raises(ValueError):
        await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id="unproven-caller-thread", endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
