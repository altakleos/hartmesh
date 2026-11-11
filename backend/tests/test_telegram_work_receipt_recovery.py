"""Unknown Work actions recover committed receipts without issuing commands."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_outcome_reads import review_confirmation
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


async def lost_outcome(e, monkeypatch):
    _record, _request, _seen, control = await review_confirmation(e)
    host = admissions(e)
    act = await stage(e, host, control)
    original = host.acts._work_outcome

    async def lost(*args, **kwargs):
        if kwargs.get("state") == "done":
            raise RuntimeError("Lost the local completion write")
        return await original(*args, **kwargs)

    monkeypatch.setattr(host.acts, "_work_outcome", lost)
    pending = await host.acts.check(act.id)
    assert pending.state == "pending" and pending.attempts == 1
    monkeypatch.setattr(host.acts, "_work_outcome", original)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramActRow, act.id)).state = "uncertain"
    return host.acts, act.id


@pytest.mark.asyncio
async def test_read_only_reconciliation_settles_uncertain_committed_action(work_delivery, monkeypatch):
    e = work_delivery
    acts, identity = await lost_outcome(e, monkeypatch)

    async def forbidden(*args, **kwargs):
        raise AssertionError("receipt recovery cannot issue a command")

    monkeypatch.setattr(e.human.work, "command", forbidden)
    result = await acts.reconcile_receipt(identity)
    assert result.state == "done" and result.attempts == 1
    assert (await acts.reconcile_receipt(identity)).body == result.body
    async with e.db.sessions() as reader:
        events = list(await reader.scalars(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == identity)))
        assert len(events) == 1


@pytest.mark.asyncio
async def test_absent_receipt_leaves_unknown_operation_unchanged(work_delivery, monkeypatch):
    e = work_delivery
    _record, _request, _seen, control = await review_confirmation(e)
    host = admissions(e)
    act = await stage(e, host, control)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, act.id)
        row.state, row.attempts = "uncertain", 3
    captured = await host.acts._work_record(act.id)

    async def forbidden(*args, **kwargs):
        raise AssertionError("a missing receipt is not permission to retry")

    monkeypatch.setattr(e.human.work, "command", forbidden)
    assert await host.acts.reconcile_receipt(act.id) == captured


@pytest.mark.asyncio
async def test_observer_reconciles_then_plans_without_executing_actions(work_delivery, monkeypatch):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery

    e = work_delivery
    acts, identity = await lost_outcome(e, monkeypatch)

    async def forbidden(*args, **kwargs):
        raise AssertionError("observation is not an action retry")

    monkeypatch.setattr(e.human.work, "command", forbidden)
    recovery = WorkOutcomeRecovery(WorkOutcomePlans(e.plans, acts))
    await recovery.tick(e.link, guard=await guard_for(e))
    await recovery.tick(e.link, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, identity)).state == "done"
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == identity)))
        assert len(rows) == 1 and rows[0].state == "pending"
    await recovery.stop()


@pytest.mark.asyncio
async def test_bad_receipt_does_not_starve_next_page(work_delivery, monkeypatch):
    import app.telegram_bots.work_outcome_recovery as module
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans

    e = work_delivery
    from copy import deepcopy

    acts, good = await lost_outcome(e, monkeypatch)
    bad = "0" * 32
    async with e.db.sessions.begin() as writer:
        original = await writer.get(TelegramActRow, good)
        values = {column.name: deepcopy(getattr(original, column.name)) for column in TelegramActRow.__table__.columns}
        values["id"] = bad
        values["body"]["operation_id"] = bad
        writer.add(TelegramActRow(**values))
    seen = []
    reconcile = acts.reconcile_receipt

    async def unavailable(identity):
        seen.append(identity)
        if identity == bad:
            raise ValueError("unreadable receipt")
        return await reconcile(identity)

    monkeypatch.setattr(acts, "reconcile_receipt", unavailable)
    monkeypatch.setattr(module, "_PAGE", 1)
    recovery = module.WorkOutcomeRecovery(WorkOutcomePlans(e.plans, acts))
    await recovery.tick(e.link, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        original = await writer.get(TelegramActRow, good)
        values = {column.name: deepcopy(getattr(original, column.name)) for column in TelegramActRow.__table__.columns}
        values["id"] = "f" * 32
        values["body"]["operation_id"] = "f" * 32
        writer.add(TelegramActRow(**values))
    await recovery.tick(e.link, guard=await guard_for(e))
    await recovery.tick(e.link, guard=await guard_for(e))
    assert seen == [bad, good, bad]
    assert (await acts._work_record(bad)).state == "uncertain"
    assert (await acts._work_record(good)).state == "done"
    await recovery.stop()


@pytest.mark.asyncio
async def test_stop_during_preparation_drains_without_publication(work_delivery):
    import asyncio

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery

    e = work_delivery
    _record, _request, _seen, control = await review_confirmation(e)
    actions = admissions(e)
    act = await stage(e, actions, control)
    await actions.acts.check(act.id)
    base = await guard_for(e)
    entered, release = asyncio.Event(), asyncio.Event()

    class HeldGuard:
        async def prepare(self):
            entered.set()
            await release.wait()
            from app.telegram_bots.links import prepare_publication_guard

            await prepare_publication_guard(base)

        def current(self):
            return base.current()

        async def check(self, writer):
            return await base.check(writer)

    recovery = WorkOutcomeRecovery(WorkOutcomePlans(e.plans, actions.acts))
    running = asyncio.create_task(recovery.tick(e.link, guard=HeldGuard()))
    await asyncio.wait_for(entered.wait(), 5)
    stopping = asyncio.create_task(recovery.stop())
    try:
        await asyncio.sleep(0)
        assert not stopping.done()
    finally:
        release.set()
        await asyncio.gather(running, stopping)
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act.id)))
        assert rows == []
    assert not recovery._tasks


@pytest.mark.asyncio
async def test_delivered_phase_is_not_observed_again(work_delivery, monkeypatch):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup, outcome
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery

    e = work_delivery
    plans, _identity, _outbox_id = await outcome(e)
    await clear_outcome_cleanup(e)
    await dispatch(e, 803)

    calls = []

    async def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError("an acknowledged phase needs no new observation")

    monkeypatch.setattr(plans.acts, "reconcile_receipt", forbidden)
    monkeypatch.setattr(plans, "plan", forbidden)
    recovery = WorkOutcomeRecovery(plans)
    await recovery.tick(e.link, guard=await guard_for(e))
    await recovery.stop()
    assert not calls


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["generation", "grant"])
async def test_receipt_reconciliation_still_requires_current_reader(work_delivery, monkeypatch, change):
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = work_delivery
    acts, identity = await lost_outcome(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        if change == "generation":
            (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
        else:
            grant = await writer.scalar(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref, AgentInstanceGrantRow.user_id == e.link.owner_user_id))
            grant.permissions = 1
    with pytest.raises(AgentDenied):
        await acts.reconcile_receipt(identity)
    assert (await acts._work_record(identity)).state == "uncertain"


@pytest.mark.asyncio
async def test_cancelling_tick_drains_receipt_read_and_prevents_publication(work_delivery, monkeypatch):
    import asyncio

    from _telegram_repository_test_support import add_link, bind_link

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery

    e = work_delivery
    acts, identity = await lost_outcome(e, monkeypatch)
    entered, release, other_entered, other_release, settled = (asyncio.Event() for _ in range(5))
    other = await bind_link(e.db, await add_link(e.db, owner=e.link.owner_user_id, bot=9_000_000_010))
    read = acts.receipt_for

    async def held(*args, **kwargs):
        result = await read(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    finish = acts._work_outcome

    async def finished(*args, **kwargs):
        result = await finish(*args, **kwargs)
        settled.set()
        return result

    class OtherGuard:
        async def prepare(self):
            other_entered.set()
            await other_release.wait()

        def current(self):
            pass

    monkeypatch.setattr(acts, "receipt_for", held)
    monkeypatch.setattr(acts, "_work_outcome", finished)
    recovery = WorkOutcomeRecovery(WorkOutcomePlans(e.plans, acts))
    running = asyncio.create_task(recovery.tick(e.link, guard=await guard_for(e)))
    await asyncio.wait_for(entered.wait(), 5)
    second = asyncio.create_task(recovery.tick(other, guard=OtherGuard()))
    await asyncio.wait_for(other_entered.wait(), 5)
    running.cancel()
    try:
        await asyncio.sleep(0)
        release.set()
        await asyncio.wait_for(settled.wait(), 5)
        await asyncio.sleep(0)
        assert not running.done()
    finally:
        release.set()
        other_release.set()
        await second
        with pytest.raises(asyncio.CancelledError):
            await running
        await recovery.stop()
    assert (await acts._work_record(identity)).state == "done"
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == identity)))
        assert rows == []
    assert not acts._work_active and not recovery._tasks


@pytest.mark.asyncio
async def test_another_link_tick_is_not_dropped_behind_a_blocked_read(work_delivery, monkeypatch):
    import asyncio

    from _telegram_repository_test_support import add_link, bind_link

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery

    e = work_delivery
    acts, _identity = await lost_outcome(e, monkeypatch)
    other = await bind_link(e.db, await add_link(e.db, owner=e.link.owner_user_id, bot=9_000_000_010))
    entered, release, other_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = acts.receipt_for

    async def held(*args, **kwargs):
        result = await original(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(acts, "receipt_for", held)
    owner = await e.live.authority.prepare_live_owner(other.owner_user_id)
    base = e.db.links.publication_guard(other, check_live_owner=owner, check_attempt_current=lambda: True)

    class OtherGuard:
        async def prepare(self):
            other_entered.set()

        def current(self):
            return base.current()

        async def check(self, writer):
            return await base.check(writer)

    recovery = WorkOutcomeRecovery(WorkOutcomePlans(e.plans, acts))
    first = asyncio.create_task(recovery.tick(e.link, guard=await guard_for(e)))
    await asyncio.wait_for(entered.wait(), 5)
    second = asyncio.create_task(recovery.tick(other, guard=OtherGuard()))
    try:
        await asyncio.wait_for(other_entered.wait(), 5)
        await second
        assert not first.done()
    finally:
        release.set()
        await asyncio.gather(first, second)
        await recovery.stop()
    assert not recovery._tasks


@pytest.mark.asyncio
async def test_shutdown_drains_other_links_after_one_preparation_fails(work_delivery, monkeypatch):
    import asyncio

    from _telegram_repository_test_support import add_link, bind_link

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery

    e = work_delivery
    acts, _identity = await lost_outcome(e, monkeypatch)
    other = await bind_link(e.db, await add_link(e.db, owner=e.link.owner_user_id, bot=9_000_000_010))
    entered, release, failing, fail = (asyncio.Event() for _ in range(4))
    original = acts.receipt_for

    async def held(*args, **kwargs):
        result = await original(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(acts, "receipt_for", held)

    class FailingGuard:
        async def prepare(self):
            failing.set()
            await fail.wait()
            raise ValueError("unavailable publication guard")

        def current(self):
            pass

    recovery = WorkOutcomeRecovery(WorkOutcomePlans(e.plans, acts))
    first = asyncio.create_task(recovery.tick(e.link, guard=await guard_for(e)))
    await asyncio.wait_for(entered.wait(), 5)
    second = asyncio.create_task(recovery.tick(other, guard=FailingGuard()))
    await asyncio.wait_for(failing.wait(), 5)
    stopping = asyncio.create_task(recovery.stop())
    try:
        await asyncio.sleep(0)
        fail.set()
        with pytest.raises(ValueError):
            await second
        assert not stopping.done()
    finally:
        release.set()
        results = await asyncio.gather(first, second, stopping, return_exceptions=True)
    assert isinstance(results[-1], ValueError)
    assert not recovery._tasks
