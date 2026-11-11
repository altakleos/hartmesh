"""Durable counts over seeded attempts; these fixtures do not claim execution."""

from copy import deepcopy

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instances import instances as instances
from test_agent_work_records import delegate
from test_agent_work_records import work as work

from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow


@pytest.mark.asyncio
async def test_active_count_uses_durable_unresolved_attempts_beyond_a_page(work):
    service, instance, sf = work
    record = await delegate(service, instance)
    assert await service.count_active(actor=ALICE, instance_id=instance.id) == 0
    async with sf() as session, session.begin():
        original = await session.get(AgentWorkRow, record["id"])
        for index in range(105):
            fields = {column.name: deepcopy(getattr(original, column.name)) for column in AgentWorkRow.__table__.columns}
            fields.update(id=operation())
            session.add(AgentWorkRow(**fields))
            await session.flush()
            session.add(
                AgentWorkAttemptRow(
                    id=operation(),
                    work_id=fields["id"],
                    activation_id=operation(),
                    requester_id="alice",
                    assignment_revision=1,
                    instance_generation=instance.generation,
                    definition_revision=instance.definition_revision,
                    status=("starting", "running", "stopping", "uncertain", "succeeded")[index % 5],
                    request={},
                )
            )
    assert await service.count_active(actor=BOB, instance_id=instance.id) == 84
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 1
    with pytest.raises(AgentDenied):
        await service.count_active(actor=BOB, instance_id=instance.id)


@pytest.mark.asyncio
async def test_cancelled_count_retains_its_sql_unit_until_completion(work):
    import asyncio

    from sqlalchemy import event
    from sqlalchemy.util.concurrency import await_only

    service, instance, sf = work
    entered, release = asyncio.Event(), asyncio.Event()

    def hold(_connection, _cursor, statement, _parameters, _context, _many):
        if "count(agent_work_attempts.id)" in statement:
            entered.set()
            await_only(release.wait())

    event.listen(sf.kw["bind"].sync_engine, "before_cursor_execute", hold)
    task = asyncio.create_task(service.count_active(actor=ALICE, instance_id=instance.id))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await service.count_active(actor=ALICE, instance_id=instance.id) == 0
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(sf.kw["bind"].sync_engine, "before_cursor_execute", hold)
