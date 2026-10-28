"""Actual SQL fixed outcomes; terminal run rows are explicit protocol fixtures."""

from dataclasses import replace
from types import MappingProxyType

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select
from test_telegram_publication_custody import publication_environment as publication_environment  # noqa: F401
from test_telegram_publication_observation import run_row
from test_telegram_publication_primary import _managed_turn, _primary

from app.channels.execution_client import ValidatedRun
from app.gateway.channel_execution import receipt_from_run_kwargs
from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.producer import Outbox, _item, validate_publication_outcome
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow


@pytest.mark.asyncio
async def test_input_outcome_finishes_actual_group_without_inventing_a_thread(publication_environment):
    e = publication_environment
    box, callbacks = Outbox(e.sessions), CallbackRepository(e.sessions)
    identity = await box.plan_input_outcome(e.link, source_id="input-failed", source_ref={"key": "error.generic", "values": {}}, keyboard=[], callbacks=callbacks, guard=e.guard_factory(), inbox_rows=e.origins)
    assert await box.plan_input_outcome(e.link, source_id="input-failed", source_ref={"key": "error.generic", "values": {}}, keyboard=[], callbacks=callbacks, guard=e.guard_factory(), inbox_rows=e.origins) == identity
    async with e.sessions() as session:
        rows = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([origin.message_id for origin in e.origins]))))
        assert len(rows) == len(e.origins)
        assert all(row.route_complete and identity in row.delivery_ids and row.thread_id is None and row.turn_id is None for row in rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,key", [(None, "error.generic"), ("files-full", "chat.files_full"), ("sandbox_capacity_exceeded", "busy.gave_up")])
async def test_terminal_outcome_is_one_exact_current_primary_with_no_fake_receipt(publication_environment, reason, key):
    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    row = run_row(record, run_id=record.run_id)
    row.update(status="error", stop_reason=reason, metadata={"channel_turn_id": record.publication_id})
    run = ValidatedRun(MappingProxyType(row), receipt_from_run_kwargs(row["kwargs"]))
    box, callbacks = Outbox(e.sessions), CallbackRepository(e.sessions)
    identity = await box.plan_publication_outcome(record, run=run, source_ref={"key": key, "values": {}}, keyboard=[], callbacks=callbacks, guard=e.guard_factory())
    item = _item(await _primary(e.db, identity))
    proof = validate_publication_outcome(item)
    assert proof.run_id == record.run_id and proof.kind == "run"
    assert "reply_basis" not in item.payload and "delivery_kind" not in item.payload
    assert await box.plan_publication_outcome(record, run=run, source_ref={"key": key, "values": {}}, keyboard=[], callbacks=callbacks, guard=e.guard_factory()) == identity
    with pytest.raises(ValueError):
        await box.plan_publication_outcome(record, run=run, source_ref={"key": "busy.queued", "values": {}}, keyboard=[], callbacks=callbacks, guard=e.guard_factory())


@pytest.mark.asyncio
async def test_waiting_auxiliary_cannot_close_unpublished_input(publication_environment):
    e = publication_environment
    with pytest.raises(ValueError):
        await Outbox(e.sessions).plan_input_outcome(e.link, source_id="still-waiting", source_ref={"key": "busy.queued", "values": {}}, keyboard=[], callbacks=CallbackRepository(e.sessions), guard=e.guard_factory(), inbox_rows=e.origins)
    async with e.sessions() as session:
        rows = list(await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id, TelegramInboundMarkRow.message_id.in_([origin.message_id for origin in e.origins]))))
        assert all(not row.route_complete for row in rows)


@pytest.mark.asyncio
async def test_pre_dispatch_stop_closes_only_the_exact_false_marker(publication_environment):
    e = publication_environment
    record = await e.reserve()
    record = await e.inbox.request_publication_stop(record, stop_id="before-post", guard=e.guard_factory())
    box = Outbox(e.sessions)
    identity = await box.plan_cancelled_before_dispatch(record, stop_id="before-post", guard=e.guard_factory(), callbacks=CallbackRepository(e.sessions))
    proof = validate_publication_outcome(_item(await _primary(e.db, identity)))
    assert proof.kind == "before_dispatch_stop" and proof.run_id is None and proof.stop_id == "before-post"
    with pytest.raises(ValueError):
        await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())


@pytest.mark.asyncio
async def test_valid_guard_for_another_actual_link_cannot_open_outcome_writer(publication_environment):
    from _telegram_repository_test_support import add_link, bind_link
    from sqlalchemy import event

    e = publication_environment
    record = replace(await _managed_turn(e), dispatch_started=True, run_id="managed-primary-run")
    other = await bind_link(e.db, await add_link(e.db, owner=e.owner_id, bot=e.link.bot_id + 1, target="other-target"), message=102, update=202)
    row = run_row(record, run_id=record.run_id)
    row.update(status="error", stop_reason=None, metadata={"channel_turn_id": record.publication_id})
    run = ValidatedRun(MappingProxyType(row), receipt_from_run_kwargs(row["kwargs"]))
    statements = []

    def sql(*args):
        statements.append(args[2])

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", sql)
    try:
        with pytest.raises(ValueError):
            await Outbox(e.sessions).plan_publication_outcome(record, run=run, source_ref={"key": "error.generic", "values": {}}, keyboard=[], callbacks=CallbackRepository(e.sessions), guard=e.guard_factory(captured_link=other))
        assert statements == []
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", sql)
