"""Real Work history and SQL delivery rollback; projection is an explicit test seam."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_actions import actions as actions

from app.telegram_bots.producer import Outbox
from app.telegram_bots.work_event_cursors import WorkEventCursors
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow, TelegramWorkCursorRow


async def later_work(e, title="Later Work"):
    return await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective=title, success_criteria="Preserve event ownership"))


def reader(e, project=None, *, batch=200):
    from app.telegram_bots.work_event_reader import WorkEventReader

    async def fixture_projection(writer, link, event):
        # A fixed fixture delivery tests atomic custody, not recipient semantics.
        await Outbox(e.db.sessions).add(
            link,
            dedupe_key="event:" + event.id,
            class_="info",
            payload={"parts": [{"kind": "text", "source_ref": {"key": "tap.failed", "values": {}}}]},
            instance_id=event.instance_id,
            work_id=event.work_id,
            event_id=event.id,
            session=writer,
        )

    return WorkEventReader(links=e.db.links, project=project or fixture_projection, batch=batch)


async def event_rows(e):
    async with e.db.sessions() as session:
        return list(await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.event_id.is_not(None))))


@pytest.mark.asyncio
async def test_restart_reads_new_work_once_without_rebaselining(actions):
    e = actions
    await WorkEventCursors(e.db.links).initialize()
    work = await later_work(e)
    assert await reader(e).tick() == 1
    assert await reader(e).tick() == 0
    rows = await event_rows(e)
    assert len(rows) == 1 and rows[0].work_id == work["id"]
    async with e.db.sessions() as session:
        assert (await session.get(TelegramWorkCursorRow, (e.link.id, work["id"]))).revision == work["revision"]


@pytest.mark.asyncio
async def test_delivery_and_cursor_roll_back_together_then_recover(actions):
    e = actions
    await WorkEventCursors(e.db.links).initialize()
    work = await later_work(e)
    normal = reader(e)

    async def interrupted(writer, link, event):
        await normal.project(writer, link, event)
        raise RuntimeError("interrupted projection")

    broken = reader(e, interrupted)
    assert await broken.tick() == 0
    assert not await event_rows(e)
    async with e.db.sessions() as session:
        assert await session.get(TelegramWorkCursorRow, (e.link.id, work["id"])) is None
    assert await normal.tick() == 1
    assert len(await event_rows(e)) == 1


@pytest.mark.asyncio
async def test_no_recipient_still_consumes_the_event_and_later_old_work_event_is_seen(actions):
    e = actions
    await WorkEventCursors(e.db.links).initialize()
    seen = []

    async def no_recipient(writer, link, event):
        seen.append((event.action, event.revision))

    operation = uuid4().hex
    await e.acts.respond(e.link, operation_id=operation, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="A later response.")
    assert await reader(e, no_recipient).tick() == 1
    assert seen == [("human_response", 3)]
    assert await reader(e, no_recipient).tick() == 0
    assert not await event_rows(e)


@pytest.mark.asyncio
async def test_two_readers_serialize_one_pair_without_duplicate_projection(actions):
    import asyncio

    e = actions
    await WorkEventCursors(e.db.links).initialize()
    await later_work(e)
    counts = await asyncio.gather(reader(e).tick(), reader(e).tick())
    assert sum(counts) == 1
    assert len(await event_rows(e)) == 1


@pytest.mark.asyncio
async def test_failed_pair_does_not_starve_independent_work(actions):
    e = actions
    await WorkEventCursors(e.db.links).initialize()
    first, second = await later_work(e, "Failing"), await later_work(e, "Independent")
    normal = reader(e)

    async def sometimes(writer, link, event):
        if event.work_id == first["id"]:
            raise RuntimeError("source unavailable")
        await normal.project(writer, link, event)

    assert await reader(e, sometimes).tick() == 1
    assert [row.work_id for row in await event_rows(e)] == [second["id"]]


@pytest.mark.asyncio
async def test_uninitialized_and_disconnected_generations_are_not_read(actions):
    e = actions
    assert await reader(e).tick() == 0
    await WorkEventCursors(e.db.links).initialize()
    await later_work(e)
    await e.db.links.unbind(e.link, actor_id=e.link.owner_user_id)
    assert await reader(e).tick() == 0
    assert not await event_rows(e)


@pytest.mark.asyncio
async def test_pair_batch_is_bounded_and_rotates_links(actions):
    from test_telegram_work_event_cursors import peer_binding

    e = actions
    cursors = WorkEventCursors(e.db.links)
    cursors.install_binding_hook()
    await cursors.initialize()
    peer, _ = await peer_binding(e)
    for index in range(3):
        await later_work(e, f"Assignment {index}")
    worker = reader(e, batch=1)
    assert await worker.tick() == 1
    assert await worker.tick() == 1
    rows = await event_rows(e)
    assert len(rows) == 2 and {row.link_id for row in rows} == {e.link.id, peer.id}
    assert await worker.tick() == 1


@pytest.mark.asyncio
async def test_incomplete_history_holds_cursor_and_all_delivery_writes(actions):
    from sqlalchemy import delete

    e = actions
    await WorkEventCursors(e.db.links).initialize()
    await e.acts.respond(e.link, operation_id=uuid4().hex, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Preserve missing evidence.")
    async with e.db.sessions.begin() as writer:
        await writer.execute(delete(AgentWorkEventRow).where(AgentWorkEventRow.work_id == e.work["id"], AgentWorkEventRow.revision == 3))
    assert await reader(e).tick() == 0
    async with e.db.sessions() as session:
        assert (await session.get(TelegramWorkCursorRow, (e.link.id, e.work["id"]))).revision == 2
    assert not await event_rows(e)


@pytest.mark.asyncio
async def test_failed_pair_rotates_even_when_batch_is_one(actions):
    e = actions
    await WorkEventCursors(e.db.links).initialize()
    works = sorted([await later_work(e), await later_work(e)], key=lambda item: item["id"])
    normal = reader(e)

    async def sometimes(writer, link, event):
        if event.work_id == works[0]["id"]:
            raise RuntimeError("source unavailable")
        await normal.project(writer, link, event)

    worker = reader(e, sometimes, batch=1)
    assert await worker.tick() == 0
    assert await worker.tick() == 1
    assert [row.work_id for row in await event_rows(e)] == [works[1]["id"]]


@pytest.mark.asyncio
async def test_binding_retired_after_selection_cannot_publish(actions, monkeypatch):
    e = actions
    await WorkEventCursors(e.db.links).initialize()
    await later_work(e)
    worker = reader(e)
    consume = worker._consume

    async def retire(*args):
        await e.db.links.unbind(e.link, actor_id=e.link.owner_user_id)
        return await consume(*args)

    monkeypatch.setattr(worker, "_consume", retire)
    assert await worker.tick() == 0
    assert not await event_rows(e)


@pytest.mark.asyncio
async def test_cancellation_drains_admitted_projection_and_commit(actions):
    import asyncio

    e = actions
    await WorkEventCursors(e.db.links).initialize()
    work = await later_work(e)
    normal = reader(e)
    entered, release = asyncio.Event(), asyncio.Event()

    async def held(writer, link, event):
        await normal.project(writer, link, event)
        entered.set()
        await release.wait()

    task = asyncio.create_task(reader(e, held).tick())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert len(await event_rows(e)) == 1
    async with e.db.sessions() as session:
        assert (await session.get(TelegramWorkCursorRow, (e.link.id, work["id"]))).revision == 1
    assert await reader(e).tick() == 0


@pytest.mark.asyncio
async def test_existing_delivery_dedupe_does_not_prevent_cursor_progress(actions):
    e = actions
    await WorkEventCursors(e.db.links).initialize()
    work = await later_work(e)
    worker = reader(e)
    async with e.db.links._transaction() as writer:
        event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == work["id"]))
        await worker.project(writer, e.link, event)
    assert await worker.tick() == 1
    assert len(await event_rows(e)) == 1


@pytest.mark.asyncio
async def test_backlog_pages_keep_revision_order_in_one_commit(actions):
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = actions
    await WorkEventCursors(e.db.links).initialize()
    # Actual canonical route commands create > one page of Work history.
    for revision in range(1, 102):
        await e.human.command(actor=e.actor, request_id=e.request["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=revision, expected_request_revision=revision, recipient_id=e.actor.subject_id))
    seen = []

    async def record(writer, link, event):
        seen.append(event.revision)

    assert await reader(e, record).tick() == 1
    assert seen == list(range(3, 104))
    assert await reader(e, record).tick() == 0
