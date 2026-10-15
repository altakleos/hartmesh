"""Host-only orphan recovery uses real SQL and preserves slot ownership."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from sqlalchemy import func, select
from test_agent_instances import create, pending_creation
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, Refusal
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow
from deerflow.spaces.contract import Permission, SpaceConflict, SpaceDenied


def inactive_humans(agents, monkeypatch, inactive=(ALICE, BOB)):
    original = agents.directory.human

    async def lookup(reference):
        return None if reference in inactive else await original(reference)

    monkeypatch.setattr(agents.directory, "human", lookup)


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions", [0, 1, 2, 4, 7])
async def test_host_home_grant_admits_current_inspect_in_its_own_transaction(instances, permissions):
    agents, files, sf, _catalog = instances
    instance = await create(agents, custody="company")
    callback = agents.space_audience_admission(instance.home_id, BOB)
    async with sf() as session, session.begin():
        if permissions:
            session.add(AgentInstanceGrantRow(instance_id=instance.id, user_id="bob", permissions=permissions))
        before = (await session.get(SpaceRow, instance.home_id)).generation
    if permissions & 2:
        await files.registry.host_grant(instance.home_id, BOB, Permission(27), host_id="operator-host", admission=callback)
    else:
        with pytest.raises(SpaceDenied, match="A grant on the agent's files requires Inspect on the agent"):
            await files.registry.host_grant(instance.home_id, BOB, Permission(27), host_id="operator-host", admission=callback)
    async with sf() as session:
        assert (await session.get(SpaceRow, instance.home_id)).generation == before + bool(permissions & 2)


@pytest.mark.asyncio
async def test_host_home_grant_callback_rechecks_human_sign_in(instances, monkeypatch):
    agents, files, sf, _catalog = instances
    instance = await create(agents, custody="company")
    callback = agents.space_audience_admission(instance.home_id, BOB)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=instance.id, user_id="bob", permissions=7))
    inactive_humans(agents, monkeypatch, inactive=(BOB,))
    with pytest.raises(SpaceDenied, match="requires Inspect"):
        await files.registry.host_grant(instance.home_id, BOB, Permission(27), admission=callback)


@pytest.mark.asyncio
@pytest.mark.parametrize("custody", ["personal", "company"])
async def test_host_delete_marks_an_agent_and_its_space_deleted_together(instances, monkeypatch, custody):
    agents, files, sf, _catalog = instances
    instance = await create(agents, custody=custody)
    async with sf() as session:
        home = await session.get(SpaceRow, instance.home_id)
        old_generation, handle = home.generation, home.backing_handle
        slot = (await session.get(SpaceBackingRow, handle)).slot_id
    bound = {kind: set(slots) for kind, slots in files.bound_slots.items()}
    unclaimed = {kind: set(slots) for kind, slots in files.unclaimed_slots.items()}
    inactive_humans(agents, monkeypatch)

    assert await agents.host_delete(instance.id, host_id="operator-host") == instance.home_id
    assert await agents.host_delete(instance.id, host_id="operator-host") == instance.home_id

    async with sf() as session:
        row = await session.get(AgentInstanceRow, instance.id)
        home = await session.get(SpaceRow, instance.home_id)
        assert row.status == home.status == "deleted"
        assert row.generation == instance.generation + 1
        assert home.generation == old_generation + 1
        event = await session.get(SpaceEventRow, (home.id, home.generation))
        assert (event.action, event.actor_kind, event.actor_id) == ("delete", "human", "alice")
        assert event.details == {"retained_backups": False, "retained_binding": True, "by": "host", "cause": "operator-deleted", "operator_host_id": "operator-host"}
        assert (await session.get(SpaceBackingRow, handle)).space_id == home.id
    assert any(slot in slots for slots in files.bound_slots.values())
    assert files.bound_slots == bound
    assert files.unclaimed_slots == unclaimed


@pytest.mark.asyncio
@pytest.mark.parametrize("custody,active", [("personal", "owner"), ("company", "supervisor"), ("company", "manager")])
async def test_host_delete_refuses_a_live_owner_supervisor_or_manager(instances, monkeypatch, custody, active):
    agents, _files, sf, _catalog = instances
    instance = await create(agents, custody=custody)
    async with sf() as session, session.begin():
        before = (await session.get(SpaceRow, instance.home_id)).generation
        if active == "supervisor":
            await session.delete(await session.get(AgentInstanceGrantRow, (instance.id, "alice")))
        elif active == "manager":
            session.add(AgentInstanceGrantRow(instance_id=instance.id, user_id="bob", permissions=4))
    if active == "manager":
        inactive_humans(agents, monkeypatch, inactive=(ALICE,))

    with pytest.raises(Refusal) as caught:
        await agents.host_delete(instance.id, host_id="operator-host")
    assert caught.value.reason == ("owner-active" if custody == "personal" else "managers-active")
    expected = (
        f"{instance.id} is an AI assistant and its owner's account is active: the owner deletes it in the product"
        if custody == "personal"
        else f"{instance.id} is an AI employee with a manager who can sign in: a manager deletes it in the product"
    )
    assert str(caught.value) == expected
    async with sf() as session:
        row, home = await session.get(AgentInstanceRow, instance.id), await session.get(SpaceRow, instance.home_id)
        assert row.status == home.status == "active"
        assert (row.generation, home.generation) == (instance.generation, before)


@pytest.mark.asyncio
async def test_host_delete_dry_run_changes_nothing_and_actual_delete_requires_fencing(instances, monkeypatch):
    agents, _files, sf, _catalog = instances
    instance = await create(agents, custody="company")
    attachment = operation()
    async with sf() as session, session.begin():
        generation = (await session.get(SpaceRow, instance.home_id)).generation
        session.add(SpaceAttachmentRow(id=attachment, incarnation=operation(), actor_kind="nonhuman", actor_id=instance.principal.subject_id, host_id="operator-host", container_id="a" * 64, phase="active"))
        await session.flush()
        session.add(SpaceMountRow(attachment_id=attachment, space_id=instance.home_id, generation=generation, alias="home", writable=1))
    inactive_humans(agents, monkeypatch)

    assert await agents.host_delete(instance.id, host_id="operator-host", dry_run=True) == instance.home_id
    with pytest.raises(SpaceConflict, match="must be fenced"):
        await agents.host_delete(instance.id, host_id="operator-host")
    async with sf() as session:
        row, home = await session.get(AgentInstanceRow, instance.id), await session.get(SpaceRow, instance.home_id)
        assert row.status == home.status == "active"
        assert (row.generation, home.generation) == (instance.generation, generation)
        assert (await session.get(SpaceAttachmentRow, attachment)).phase == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize("which", ["agent", "home"])
async def test_host_delete_exhausted_generation_rolls_back_both_rows(instances, monkeypatch, which):
    agents, _files, sf, _catalog = instances
    instance = await create(agents)
    async with sf() as session, session.begin():
        row, home = await session.get(AgentInstanceRow, instance.id), await session.get(SpaceRow, instance.home_id)
        (row if which == "agent" else home).generation = 2**31 - 1
        before = (row.generation, home.generation)
        events = await session.scalar(select(func.count()).select_from(SpaceEventRow))
    inactive_humans(agents, monkeypatch)

    with pytest.raises(AgentConflict, match="generation is exhausted"):
        await agents.host_delete(instance.id, host_id="operator-host")
    async with sf() as session:
        row, home = await session.get(AgentInstanceRow, instance.id), await session.get(SpaceRow, instance.home_id)
        assert row.status == home.status == "active"
        assert (row.generation, home.generation) == before
        assert await session.scalar(select(func.count()).select_from(SpaceEventRow)) == events


@pytest.mark.asyncio
@pytest.mark.parametrize("with_home", [False, True])
async def test_host_delete_refuses_missing_and_unfinished_instances(instances, monkeypatch, with_home):
    agents, _files, sf, _catalog = instances
    pending = await pending_creation(agents, with_home=with_home)
    inactive_humans(agents, monkeypatch)
    with pytest.raises(AgentDenied, match="unavailable"):
        await agents.host_delete(operation(), host_id="operator-host")
    with pytest.raises(AgentConflict, match="not set up"):
        await agents.host_delete(pending.id, host_id="operator-host")
    async with sf() as session:
        assert (await session.get(AgentInstanceRow, pending.id)).status == "provisioning"
        assert await session.get(AgentInstanceGrantRow, (pending.id, "alice")) is not None


@pytest.mark.asyncio
async def test_host_delete_borrows_the_locked_session_for_live_account_checks(instances, monkeypatch):
    agents, _files, sf, _catalog = instances
    instance = await create(agents, custody="company")
    checked = []

    class Directory:
        async def __call__(self, reference):
            raise AssertionError("Eligibility must reuse the existing SQL connection")

        async def in_session(self, reference, session):
            assert session.in_transaction()
            assert await session.get(AgentInstanceRow, instance.id) is not None
            checked.append(reference)
            return None

    monkeypatch.setattr(agents.directory, "human", Directory())
    assert await agents.host_delete(instance.id, host_id="operator-host") == instance.home_id
    assert ALICE in checked


@pytest.mark.asyncio
async def test_host_delete_rechecks_eligibility_after_dry_run(instances, monkeypatch):
    agents, _files, sf, _catalog = instances
    instance = await create(agents, custody="company")
    original = agents.directory.human
    inactive_humans(agents, monkeypatch)
    assert await agents.host_delete(instance.id, host_id="operator-host", dry_run=True) == instance.home_id
    monkeypatch.setattr(agents.directory, "human", original)
    with pytest.raises(Refusal) as caught:
        await agents.host_delete(instance.id, host_id="operator-host")
    assert caught.value.reason == "managers-active"
    async with sf() as session:
        assert (await session.get(AgentInstanceRow, instance.id)).status == "active"
        assert (await session.get(SpaceRow, instance.home_id)).status == "active"


@pytest.mark.asyncio
async def test_host_delete_preserves_pending_file_and_lifecycle_evidence(instances, monkeypatch):
    agents, _files, sf, _catalog = instances
    instance = await create(agents, custody="company")
    lifecycle, file_operation = operation(), operation()
    async with sf() as session, session.begin():
        generation = (await session.get(SpaceRow, instance.home_id)).generation
        session.add(
            AgentLifecycleRow(
                instance_id=instance.id,
                operation_id=lifecycle,
                actor_id="alice",
                generation=instance.generation,
                home_generation=generation,
                home_id=instance.home_id,
                request={"action": "archive"},
                attachment_ids=[],
                phase="pending",
                prior_status="active",
            )
        )
        session.add(SpaceFileOperationRow(space_id=instance.home_id, operation_id=file_operation, actor_kind="human", actor_id="alice", generation=generation, request={"action": "unknown"}, phase="pending"))
    inactive_humans(agents, monkeypatch)
    await agents.host_delete(instance.id, host_id="operator-host")
    async with sf() as session:
        intent = await session.get(AgentLifecycleRow, (instance.id, lifecycle))
        file_intent = await session.get(SpaceFileOperationRow, (instance.home_id, file_operation))
        assert (intent.phase, intent.request, intent.attachment_ids) == ("pending", {"action": "archive"}, [])
        assert (file_intent.phase, file_intent.request, file_intent.result) == ("pending", {"action": "unknown"}, None)


@pytest.mark.asyncio
async def test_cancelled_host_delete_drains_the_atomic_commit(instances, monkeypatch):
    agents, files, sf, _catalog = instances
    instance = await create(agents, custody="company")
    inactive_humans(agents, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original = files.registry.host_locked

    @asynccontextmanager
    async def delayed(scope):
        async with original(scope) as locked:
            yield locked
            entered.set()
            await release.wait()

    monkeypatch.setattr(files.registry, "host_locked", delayed)
    task = asyncio.create_task(agents.host_delete(instance.id, host_id="operator-host"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    async with sf() as session:
        assert (await session.get(AgentInstanceRow, instance.id)).status == "deleted"
        assert (await session.get(SpaceRow, instance.home_id)).status == "deleted"
