"""Operator documents from real SQL and filesystem policy, with offline Docker."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _storage_spaces_test_support import make_storage_fixture, operation
from sqlalchemy import select
from test_agent_instances import definition
from test_native_create_bridge import fixture as fixture

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.storage_spaces import build_storage_services
from deerflow.config.storage_spaces_config import StoragePlan, StorageSpacesConfig
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.base import Base
from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.persistence.user.access import DisabledIdentityRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def operator(tmp_path, request, monkeypatch):
    from app.gateway.agent_operator_services import OperatorCommands

    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: False)
    async for original, sf, catalog in make_storage_fixture(tmp_path, request):
        tables = [
            UserRow,
            DisabledIdentityRow,
            RoleLimitRow,
            StorageFeatureLinkRow,
            AgentDefinitionRevisionRow,
            AgentInstanceRow,
            AgentInstanceGrantRow,
            AgentLifecycleRow,
            AgentWorkRow,
            AgentWorkAttemptRow,
            AgentWorkEventRow,
            RunRow,
            RunChangeClockRow,
        ]
        async with sf.kw["bind"].begin() as connection:
            await connection.run_sync(Base.metadata.create_all, tables=[row.__table__ for row in tables])
        repository = SQLiteUserRepository(sf)
        owner = User(email="owner@example.com", system_role="admin")
        other = User(email="other@example.com")
        await repository.create_user(owner)
        await repository.create_user(other)
        plan = StoragePlan(users=1, user_gib=1, ai_assistants_per_user=1, ai_assistant_gib=1, ai_employees=1, ai_employee_gib=1, company_gib=1)
        config = StorageSpacesConfig(enabled=True, inventory_path=str(tmp_path / "inventory.v2.json"), plan=plan)
        for spec, kind in zip(catalog.volumes.values(), ("user", "assistant", "employee", "company"), strict=True):
            spec.kind = kind
        files, agents = build_storage_services(config, catalog, sf, repository)

        class Docker:
            host_id = "current-daemon"

            def _same_host(self):
                pass

            def _call(self, *args):
                assert args == ("ps", "--no-trunc", "--format", "{{.Names}}")
                return SimpleNamespace(returncode=0, stdout="", stderr="")

        app_config = SimpleNamespace(storage_spaces=config, sandbox=SimpleNamespace(replicas=2))
        monkeypatch.setattr("app.gateway.storage_spaces.AGENTS_HOLD_PATH", tmp_path / "agents-hold")
        command = OperatorCommands(files=files, agents=agents, repository=repository, runs=RunRepository(sf), config=app_config, adapter=Docker(), home=tmp_path)
        command.fixture = SimpleNamespace(sf=sf, catalog=catalog, owner=owner, other=other, path=tmp_path)
        yield command


async def create(command, *, custody="personal", name="Analyst"):
    owner = command.fixture.owner
    return await command.agents.create(actor=PrincipalRef("human", str(owner.id)), creation_id=operation(), name=name, custody=custody, supervisor=PrincipalRef("human", str(owner.id)), definition=definition(str(owner.id)))


@pytest.mark.asyncio
async def test_list_reads_current_accounts_and_inflight_runs_without_updating_bindings(operator):
    agent = await create(operator)
    now = datetime.now(UTC)
    for identifier, status, lease in (("live", "running", now + timedelta(minutes=1)), ("expired", "running", now - timedelta(seconds=1)), ("finished", "success", now + timedelta(minutes=1))):
        await operator.runs.put(identifier, thread_id=identifier, user_id=str(operator.fixture.owner.id), status=status, metadata={"agent_instance_id": agent.id}, lease_expires_at=lease.isoformat())
    async with operator.fixture.sf() as session:
        binding = (await session.scalars(select(SpaceBackingRow))).one()
        before = binding.max_bytes, binding.max_inodes
    document = await operator.run("list", all=False)
    assert document["returncode"] == 0 and document["count"] == 1 and document["deleted_released"] == 0
    row = document["instances"][0]
    assert (row["id"], row["kind"], row["owner_live"], row["supervisor"]["live"], row["runs_in_flight"], row["attempts_unresolved"]) == (agent.id, "assistant", True, True, 1, 0)
    assert row["space"]["verified"] is True and row["workspace"] is None
    assert row["last_run_at"] is not None and row["execution"] == "ready"
    async with operator.fixture.sf() as session:
        binding = (await session.scalars(select(SpaceBackingRow))).one()
        assert (binding.max_bytes, binding.max_inodes) == before
    await operator.repository.disable_identity("urn:hartmesh:local-account", str(operator.fixture.owner.id))
    # Release is an independent persisted account state, read on the next call.
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = now
    row = (await operator.run("list", all=True))["instances"][0]
    assert row["owner_live"] is row["supervisor"]["live"] is False


@pytest.mark.asyncio
async def test_list_omits_only_deleted_agents_whose_binding_was_released(operator):
    agent = await create(operator)
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(AgentInstanceRow, agent.id)).status = "deleted"
        (await session.get(SpaceRow, agent.home_id)).status = "deleted"
    document = await operator.run("list", all=False)
    assert document["count"] == 1 and document["instances"][0]["space"]["release"] == "pending"
    async with operator.fixture.sf() as session, session.begin():
        await session.delete((await session.scalars(select(SpaceBackingRow))).one())
    document = await operator.run("list", all=False)
    assert (document["count"], document["deleted_released"], document["instances"]) == (0, 1, [])
    document = await operator.run("list", all=True)
    assert document["count"] == document["deleted_released"] == 1
    assert document["instances"][0]["space"]["slot_id"] is None
    assert document["instances"][0]["space"]["release"] == "done"


@pytest.mark.asyncio
async def test_verify_lists_missing_bound_slot_and_does_not_repair_inventory(operator):
    agent = await create(operator)
    async with operator.fixture.sf() as session:
        binding = (await session.scalars(select(SpaceBackingRow))).one()
    removed = operator.fixture.catalog.volumes.pop(binding.slot_id)
    document = await operator.run("verify")
    assert document["returncode"] == 2 and document["total"] == 3 and document["verified"] == 3
    row = next(row for row in document["spaces"] if row["slot_id"] == removed.slot_id)
    assert row["listed"] is False and row["kind"] is None and row["state"] == "unverified"
    assert row["holder_id"] == agent.id and row["space_id"] == agent.home_id
    assert row["used_bytes"] is row["empty"] is None and row["max_bytes"] == binding.max_bytes
    assert removed.slot_id not in operator.fixture.catalog.volumes


@pytest.mark.asyncio
async def test_status_requires_actual_gateway_lifetime_and_keeps_daemon_outage_an_answer(operator):
    from app.gateway.gateway_observation import GatewayObservationUnavailable, publish

    with pytest.raises(GatewayObservationUnavailable):
        await operator.run("status")
    start = datetime.now(UTC) - timedelta(seconds=10)
    lifetime = publish(operator.fixture.path, started_at=start)
    try:
        operator.adapter = None
        operator.adapter_error = OSError("offline daemon")
        document = await operator.run("status")
        assert document["returncode"] == 0 and document["storage"] == document["execution"] == "unavailable"
        assert document["storage_reason"] == "admission-failed" and document["reason"] == "storage-unavailable"
        assert document["since_gateway_start"] == {"at": start.isoformat(), "attachments_fenced": {}, "work_attempts_settled": 0, "file_operations_settled": 0}
        assert document["host_id"] is None and document["foreign_hosts"] == []
        assert document["sandbox_slots"] == {"limit": 2, "workspaces_running": None, "chat_sandboxes_running": None}
    finally:
        lifetime.close()


@pytest.mark.asyncio
async def test_status_counts_committed_transitions_since_gateway_start_not_intent_dates(operator):
    from app.gateway.gateway_observation import publish

    agent, other = await create(operator), await create(operator, custody="company")
    start = datetime.now(UTC) - timedelta(seconds=10)
    old, recent = start - timedelta(days=1), start + timedelta(seconds=1)
    lifetime = publish(operator.fixture.path, started_at=start)
    try:
        async with operator.fixture.sf() as session, session.begin():
            attachment = operation()
            session.add(
                SpaceAttachmentRow(
                    id=attachment,
                    incarnation=operation(),
                    actor_kind="nonhuman",
                    actor_id=agent.principal.subject_id,
                    host_id="current-daemon",
                    container_id="a" * 64,
                    phase="fenced",
                    fence_reason="operator",
                    fenced_by_kind="host",
                    fenced_by_id="current-daemon",
                    fenced_at=recent,
                )
            )
            await session.flush()
            for item in (agent, other):
                session.add(SpaceMountRow(attachment_id=attachment, space_id=item.home_id, generation=1, alias="home" if item == agent else "other", writable=1))
            for result in ({"resolution": "host-settled", "host_settled_at": recent.isoformat()}, {"resolution": "host-settled"}, {"resolution": "host-discarded", "host_settled_at": old.isoformat()}, {"value": "ordinary completion"}):
                session.add(
                    SpaceFileOperationRow(
                        space_id=agent.home_id,
                        operation_id=operation(),
                        actor_kind="human",
                        actor_id=str(operator.fixture.owner.id),
                        generation=1,
                        phase="complete",
                        request={"action": "remove", "path": "absent"},
                        result=result,
                        created_at=old,
                    )
                )
            work_id = operation()
            session.add(
                AgentWorkRow(
                    id=work_id,
                    instance_id=agent.id,
                    creator_id=str(operator.fixture.owner.id),
                    objective="Check",
                    success_criteria="Done",
                    priority="normal",
                    definition_revision=agent.definition_revision,
                    assignment_revision=1,
                    revision=3,
                    status="open",
                    review_required=False,
                    review_state="none",
                )
            )
            await session.flush()
            for revision, action, at in ((1, "attempt_contained", old), (2, "attempt_contained", recent), (3, "attempt_finished", recent)):
                session.add(
                    AgentWorkEventRow(
                        id=operation(),
                        instance_id=agent.id,
                        work_id=work_id,
                        actor_id="current-daemon",
                        actor_kind="host",
                        operation_id=operation(),
                        action=action,
                        revision=revision,
                        assignment_revision=1,
                        request={},
                        result={},
                        created_at=at,
                    )
                )
        document = await operator.run("status")
        assert document["returncode"] == 0 and document["storage"] == document["execution"] == "ready"
        assert document["since_gateway_start"] == {"at": start.isoformat(), "attachments_fenced": {"operator": 1}, "work_attempts_settled": 1, "file_operations_settled": 1}
        assert document["host_id"] == "current-daemon" and document["attachments_active"] == 0
    finally:
        lifetime.close()


@pytest.mark.asyncio
async def test_status_refuses_a_gateway_restart_during_its_sql_snapshot(operator, monkeypatch):
    from app.gateway.gateway_observation import GatewayObservationUnavailable, publish

    lifetimes = [publish(operator.fixture.path, started_at=datetime.now(UTC))]
    snapshot = operator._snapshot

    async def restarted(**options):
        result = await snapshot(**options)
        lifetimes[0].close()
        lifetimes.append(publish(operator.fixture.path, started_at=datetime.now(UTC)))
        return result

    monkeypatch.setattr(operator, "_snapshot", restarted)
    try:
        with pytest.raises(GatewayObservationUnavailable):
            await operator.run("status")
    finally:
        for lifetime in lifetimes:
            lifetime.close()


async def attachment(command, agent, *, host="current-daemon", phase="active"):
    identifier = operation()
    async with command.fixture.sf() as session, session.begin():
        row = SpaceAttachmentRow(id=identifier, incarnation=operation(), actor_kind="nonhuman", actor_id=agent.principal.subject_id, host_id=host, container_id="a" * 64, phase=phase)
        session.add(row)
        await session.flush()
        home = await session.get(SpaceRow, agent.home_id)
        session.add(SpaceMountRow(attachment_id=identifier, space_id=agent.home_id, generation=home.generation, alias="home", writable=1))
    return identifier


@pytest.mark.asyncio
async def test_adopt_host_requires_explicit_acknowledgement_and_changes_only_the_mapping(operator):
    from app.gateway.agent_operator_contract import CommandRefused

    agent = await create(operator)
    wanted = await attachment(operator, agent, host="old-daemon")
    await attachment(operator, agent, host="old-daemon", phase="fenced")
    with pytest.raises(CommandRefused) as refused:
        await operator.run("adopt-host", old_host="old-daemon", acknowledge_old_host_gone=False)
    assert str(refused.value) == "1 attachments are held by Docker host old-daemon; confirm that VM is powered off or destroyed, then pass --acknowledge-old-host-gone"
    assert refused.value.fields == {"attachments_named": 1}
    async with operator.fixture.sf() as session:
        assert (await session.scalars(select(StorageHostReplacementRow))).all() == []
        before = [(row.id, row.phase, row.container_id, row.fenced_at) for row in (await session.scalars(select(SpaceAttachmentRow).order_by(SpaceAttachmentRow.id))).all()]
    document = await operator.run("adopt-host", old_host="old-daemon", acknowledge_old_host_gone=True)
    assert document["returncode"] == 0 and document["verdict"] == "adopted" and document["attachments_named"] == 1
    assert document["from"] == "old-daemon" and document["host_id"] == "current-daemon" and document["acknowledged"] is True
    async with operator.fixture.sf() as session:
        assert [(row.id, row.phase, row.container_id, row.fenced_at) for row in (await session.scalars(select(SpaceAttachmentRow).order_by(SpaceAttachmentRow.id))).all()] == before
        mapping = await session.get(StorageHostReplacementRow, ("old-daemon", "current-daemon"))
        assert mapping.attachments_named == 1
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(SpaceAttachmentRow, wanted)).phase = "fenced"
    repeated = await operator.run("adopt-host", old_host="old-daemon", acknowledge_old_host_gone=True)
    assert repeated["verdict"] == "already_adopted" and repeated["attachments_named"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old,expected", [(None, "give --from <Docker engine ID>"), ("current-daemon", "current-daemon is this Docker host; there is nothing to adopt"), ("missing-daemon", "no attachment names Docker host missing-daemon; hosts on record: ")]
)
async def test_adopt_host_refuses_missing_current_and_unknown_hosts_before_a_mapping(operator, old, expected):
    from app.gateway.agent_operator_contract import CommandRefused

    with pytest.raises(CommandRefused) as refused:
        await operator.run("adopt-host", old_host=old, acknowledge_old_host_gone=True)
    assert str(refused.value) == expected
    async with operator.fixture.sf() as session:
        assert (await session.scalars(select(StorageHostReplacementRow))).all() == []


@pytest.mark.asyncio
async def test_verify_refuses_an_unreadable_inventory_instead_of_success_with_a_fallback_catalog(operator):
    from app.gateway.agent_operator_contract import CommandRefused

    operator.storage_reason, operator.storage_text = "inventory-unreadable", "invalid manifest version"
    with pytest.raises(CommandRefused, match="^the inventory cannot be loaded: invalid manifest version$"):
        await operator.run("verify")


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign,missing_id", [(True, False), (False, True)])
async def test_list_keeps_unconfirmed_workspaces_visible_without_claiming_absence(operator, foreign, missing_id):
    agent = await create(operator)
    identifier = await attachment(operator, agent, host="old-daemon" if foreign else "current-daemon", phase="pending" if missing_id else "active")
    if missing_id:
        async with operator.fixture.sf() as session, session.begin():
            (await session.get(SpaceAttachmentRow, identifier)).container_id = None
    row = (await operator.run("list", all=False))["instances"][0]
    assert row["workspace"]["container"] == "unknown" and row["workspace"]["attachment_id"] == identifier
    if foreign:
        assert row["execution"] == "unavailable" and row["execution_reason"] == "host-replaced"


@pytest.mark.asyncio
async def test_close_workspace_already_closed_has_every_field_and_no_runtime_effect(operator):
    agent = await create(operator)
    document = await operator.run("close-workspace", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    assert document == {
        "command": "close-workspace",
        "returncode": 0,
        "note": f"workspace of {agent.id} is already closed; the next message starts a new workspace",
        "instance_id": agent.id,
        "verdict": "already_closed",
        "attachments": [],
        "attachments_unconfirmed": [],
        "runs_found": 0,
        "runs_ended": 0,
        "runs_unconfirmed": [],
        "attempts_settled": [],
        "attempts_unconfirmed": [],
        "waited_seconds": 0.0,
    }


@pytest.mark.asyncio
async def test_close_workspace_refuses_live_work_before_opening_native_evidence(operator, monkeypatch):
    from app.gateway.agent_operator_contract import CommandRefused

    agent = await create(operator)
    identifier = await attachment(operator, agent)
    await operator.runs.put("live", thread_id="live", user_id=str(operator.fixture.owner.id), status="running", metadata={"agent_instance_id": agent.id}, lease_expires_at=(datetime.now(UTC) + timedelta(minutes=2)).isoformat())
    with pytest.raises(CommandRefused) as refused:
        await operator.run("close-workspace", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    assert str(refused.value) == f"{agent.id} has 1 run in flight; wait for it to end or pass --end-running-work"
    async with operator.fixture.sf() as session:
        row = await session.get(SpaceAttachmentRow, identifier)
        assert row.phase == "active" and row.fenced_at is None
    assert (await operator.runs.get("live", user_id=None))["status"] == "running"


@pytest.mark.asyncio
async def test_close_workspace_foreign_preflight_refuses_before_any_native_effect(operator):
    from app.gateway.agent_operator_contract import CommandRefused

    agent = await create(operator)
    local = await attachment(operator, agent)
    foreign = await attachment(operator, agent, host="old-daemon")
    with pytest.raises(CommandRefused) as refused:
        await operator.run("close-workspace", instance_id=agent.id, end_running_work=True, wait_seconds=0)
    assert str(refused.value) == f"attachment {foreign} was started by Docker host old-daemon, not this one; see adopt-host"
    async with operator.fixture.sf() as session:
        assert (await session.get(SpaceAttachmentRow, local)).phase == (await session.get(SpaceAttachmentRow, foreign)).phase == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_original,late_run", [(False, False), (True, False), (False, True)])
async def test_close_workspace_uses_original_graph_and_never_cancels_or_finalizes(operator, fixture, changed_original, late_run):
    from test_native_resident_containment_scope import prepare
    from test_storage_spaces_attachments import ContainedProvider

    from deerflow.spaces.attachments import SpaceAttachments
    from deerflow.spaces.native_create_evidence import NativeEvidenceReader
    from deerflow.spaces.operator_containment import OperatorResidentContainmentScopes

    agent = await create(operator)
    provider = ContainedProvider()
    attachments = SpaceAttachments(operator.files, provider)
    storage = (operator.files, operator.fixture.sf, operator.fixture.catalog, attachments, provider)
    _gateway, saved, _captured, objects, calls = await prepare(storage, fixture, space_id=agent.home_id, actor=PrincipalRef("human", str(operator.fixture.owner.id)))
    provider._operator_only = True
    evidence = NativeEvidenceReader(fixture[0].pool, daemon_id=provider.host_id)
    provider._containment_scope = OperatorResidentContainmentScopes(files=operator.files, adapter=provider, evidence=evidence)
    operator.adapter, operator.attachments = provider, attachments
    journal = fixture[0].pool / ".native-creates" / (saved.id + ".json")
    before = journal.read_bytes(), journal.stat().st_ino, journal.stat().st_mtime_ns
    try:
        if late_run:
            from app.gateway.agent_operator_contract import CommandRefused

            entered, release = asyncio.Event(), asyncio.Event()

            async def held_factory():
                entered.set()
                await release.wait()
                operator.attachments = attachments

            operator.attachments, operator._attachment_factory = None, held_factory
            task = asyncio.create_task(operator.run("close-workspace", instance_id=agent.id, end_running_work=False, wait_seconds=0))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                await operator.runs.put("late", thread_id="late", user_id=str(operator.fixture.owner.id), status="running", metadata={"agent_instance_id": agent.id}, lease_expires_at=(datetime.now(UTC) + timedelta(minutes=2)).isoformat())
                release.set()
                with pytest.raises(CommandRefused, match="has 1 run in flight"):
                    await task
                assert objects and not any(command[1] in ("stop", "rm") for command in calls)
                async with operator.fixture.sf() as session:
                    assert (await session.get(SpaceAttachmentRow, saved.attachment_id)).phase == "active"
                assert (journal.read_bytes(), journal.stat().st_ino, journal.stat().st_mtime_ns) == before
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
            return
        await operator.runs.put("live", thread_id="live", user_id=str(operator.fixture.owner.id), status="running", metadata={"agent_instance_id": agent.id}, lease_expires_at=(datetime.now(UTC) + timedelta(minutes=2)).isoformat())
        if changed_original:
            objects[saved.container_id]["Created"] = "changed original"
        document = await operator.run("close-workspace", instance_id=agent.id, end_running_work=True, wait_seconds=0)
        assert document["returncode"] == 2 and document["runs_found"] == 1 and document["runs_ended"] == 0 and document["runs_unconfirmed"] == ["live"]
        if changed_original:
            assert document["verdict"] == "unconfirmed" and document["attachments"] == [] and document["attachments_unconfirmed"] == [saved.attachment_id]
            assert objects and not any(command[1] in ("rm", "stop") for command in calls)
        else:
            assert document["verdict"] == "closed" and document["attachments_unconfirmed"] == []
            closed = document["attachments"][0]
            assert (closed["attachment_id"], closed["container_id"], closed["fence_reason"]) == (saved.attachment_id, saved.container_id, "operator")
            assert closed["fenced_at"] and not objects
        run = await operator.runs.get("live", user_id=None)
        assert run["status"] == "running" and run["cancel_action"] is None
        assert (journal.read_bytes(), journal.stat().st_ino, journal.stat().st_mtime_ns) == before
        assert not any(command[1] in ("create", "start", "run") for command in calls)
    finally:
        evidence.close()


@pytest.mark.asyncio
async def test_closure_wait_deadline_covers_a_stalled_session_and_preserves_original_ids(operator, monkeypatch):
    closed = asyncio.Event()

    @asynccontextmanager
    async def stalled():
        try:
            await asyncio.sleep(10)
            pytest.fail("Stalled checkout must time out")
            yield
        finally:
            closed.set()

    monkeypatch.setattr(operator.files.registry, "_sf", stalled)
    result = await asyncio.wait_for(operator._wait_closed_work(["original-run"], ["original-attempt"], wait_seconds=0.01), 1)
    assert result["runs_found"] == 1 and result["runs_ended"] == 0
    assert result["runs_unconfirmed"] == ["original-run"] and result["attempts_unconfirmed"] == ["original-attempt"]
    assert result["attempts_settled"] == [] and closed.is_set()


@pytest.mark.asyncio
async def test_delete_refuses_a_live_owner_before_workspace_or_data_changes(operator):
    from app.gateway.agent_operator_contract import CommandRefused

    agent = await create(operator)
    identifier = await attachment(operator, agent)
    with pytest.raises(CommandRefused) as refused:
        await operator.run("delete", instance_id=agent.id, end_running_work=True, wait_seconds=0)
    assert refused.value.fields == {"reason": "owner-active"}
    async with operator.fixture.sf() as session:
        assert (await session.get(AgentInstanceRow, agent.id)).status == "active"
        assert (await session.get(SpaceRow, agent.home_id)).status == "active"
        assert (await session.get(SpaceAttachmentRow, identifier)).phase == "active"


@pytest.mark.asyncio
async def test_delete_tombstones_atomically_and_leaves_the_bound_slot_for_gateway(operator, monkeypatch):
    from deerflow.spaces.recovery import SpaceRecovery

    agent = await create(operator)
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
        binding = (await session.scalars(select(SpaceBackingRow))).one()
        slot, handle = binding.slot_id, binding.backing_handle

    async def forbidden(*_args, **_kwargs):
        pytest.fail("The operator command cannot free capacity")

    monkeypatch.setattr(SpaceRecovery, "host_release", forbidden)
    document = await operator.run("delete", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    assert document["returncode"] == 2 and document["verdict"] == "deleted"
    assert document["space"] == {"space_id": agent.home_id, "slot_id": slot, "release": "pending", "use": "releasing"}
    async with operator.fixture.sf() as session:
        assert (await session.get(AgentInstanceRow, agent.id)).status == (await session.get(SpaceRow, agent.home_id)).status == "deleted"
        assert (await session.get(SpaceBackingRow, handle)).slot_id == slot
    # The Gateway's independent release is observed on an idempotent command retry.
    async with operator.fixture.sf() as session, session.begin():
        await session.delete(await session.get(SpaceBackingRow, handle))
    repeated = await operator.run("delete", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    assert repeated["returncode"] == 0 and repeated["verdict"] == "already_deleted"
    assert repeated["space"] == {"space_id": agent.home_id, "slot_id": None, "release": "done", "use": None}


@pytest.mark.asyncio
@pytest.mark.parametrize("end_running_work", [False, True])
async def test_delete_cannot_skip_unfinished_work_when_workspace_was_already_closed(operator, end_running_work):
    from app.gateway.agent_operator_contract import CommandRefused

    agent = await create(operator)
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
    await operator.runs.put("live", thread_id="live", user_id=str(operator.fixture.owner.id), status="running", metadata={"agent_instance_id": agent.id}, lease_expires_at=(datetime.now(UTC) + timedelta(minutes=2)).isoformat())
    if end_running_work:
        document = await operator.run("delete", instance_id=agent.id, end_running_work=True, wait_seconds=0)
        assert document["returncode"] == 2 and document["verdict"] == "workspace_closed" and document["runs_unconfirmed"] == ["live"]
        assert document["space"]["release"] is None
    else:
        with pytest.raises(CommandRefused, match="has 1 run in flight"):
            await operator.run("delete", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    async with operator.fixture.sf() as session:
        assert (await session.get(AgentInstanceRow, agent.id)).status == (await session.get(SpaceRow, agent.home_id)).status == "active"


@pytest.mark.asyncio
async def test_export_writes_a_private_external_archive_without_changing_home(operator, monkeypatch):
    import hashlib
    import os
    import tarfile

    from app.gateway import agent_operator_actions as module

    agent = await create(operator)
    async with operator.fixture.sf() as session:
        binding = (await session.scalars(select(SpaceBackingRow))).one()
    volume = operator.fixture.catalog.verified[binding.slot_id]
    source = volume.data_path / "outputs" / "result.txt"
    source.write_text("Verified result", encoding="utf-8")
    destination = operator.fixture.path.parent / (operator.fixture.path.name + "-exports")
    monkeypatch.setattr(module, "EXPORT_ROOT", destination, raising=False)
    monkeypatch.setattr(os, "statvfs", lambda _path: SimpleNamespace(f_bavail=1 << 50, f_frsize=1))
    ownership = []

    def assign_container_owner(descriptor, uid, gid):
        # Host tests cannot chown to the deployment's container UID. Verify the
        # exact requested ownership without claiming physical qualification.
        assert os.fstat(descriptor).st_mode & 0o777 == 0o600
        ownership.append((uid, gid))

    monkeypatch.setattr(os, "fchown", assign_container_owner)
    document = await operator.run("export", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    archive = destination / Path(document["path"]).name
    assert document["returncode"] == 0 and document["verdict"] == "exported" and archive.exists()
    assert document["space"] == {"space_id": agent.home_id, "slot_id": binding.slot_id, "kind": "assistant"}
    assert archive.stat().st_mode & 0o777 == 0o600
    assert ownership == ([(1000, 1000)] if (os.getuid(), os.getgid()) != (1000, 1000) else [])
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == document["sha256"] and archive.stat().st_size == document["bytes"]
    with tarfile.open(archive) as contents:
        assert contents.extractfile("outputs/result.txt").read() == b"Verified result"
        assert len(contents.getmembers()) == document["files"]
    assert source.read_text(encoding="utf-8") == "Verified result"
    assert document["workspace_closed"] is False and document["attachments_unconfirmed"] == []


@pytest.mark.asyncio
async def test_export_checks_disk_headroom_before_closing_workspace_or_creating_a_file(operator, monkeypatch):
    import os

    from app.gateway import agent_operator_actions as module
    from app.gateway.agent_operator_contract import CommandRefused

    agent = await create(operator)
    identifier = await attachment(operator, agent)
    destination = operator.fixture.path.parent / (operator.fixture.path.name + "-exports")
    monkeypatch.setattr(module, "EXPORT_ROOT", destination, raising=False)
    monkeypatch.setattr(os, "statvfs", lambda _path: SimpleNamespace(f_bavail=1 << 29, f_frsize=1))
    with pytest.raises(CommandRefused, match="would leave the data disk under 1 GiB free"):
        await operator.run("export", instance_id=agent.id, end_running_work=True, wait_seconds=0)
    assert not destination.exists()
    async with operator.fixture.sf() as session:
        assert (await session.get(SpaceAttachmentRow, identifier)).phase == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize("discard", [False, True])
async def test_settle_acts_only_on_the_exact_pending_pair_and_reports_the_files(operator, discard):
    agent = await create(operator)
    wanted, sibling = operation(), operation()
    request = {"action": "remove", "path": "outputs/result.txt" if discard else "outputs/absent.txt"}
    async with operator.fixture.sf() as session, session.begin():
        generation = (await session.get(SpaceRow, agent.home_id)).generation
        binding = (await session.scalars(select(SpaceBackingRow))).one()
        for identifier in (wanted, sibling):
            session.add(SpaceFileOperationRow(space_id=agent.home_id, operation_id=identifier, actor_kind="human", actor_id=str(operator.fixture.owner.id), generation=generation, phase="pending", request=request))
    volume = operator.fixture.catalog.verified[binding.slot_id]
    (volume.data_path / "outputs" / "result.txt").write_bytes(b"Kept")
    document = await operator.run("settle", space_id=agent.home_id, operation_id=wanted, discard=discard)
    assert document["returncode"] == 0 and document["verdict"] == ("discarded" if discard else "settled-complete")
    assert document["resolution"] == ("host-discarded" if discard else "host-settled")
    assert document["kind"] == "assistant" and document["namespace"] is document["on_behalf_of"] is document["receipt"] is None
    assert document["request"] == request and document["files"] == ["result.txt"]
    async with operator.fixture.sf() as session:
        assert (await session.get(SpaceFileOperationRow, (agent.home_id, sibling))).phase == "pending"
        row = await session.get(SpaceFileOperationRow, (agent.home_id, wanted))
        assert row.phase == ("failed" if discard else "complete") and row.result["host_settled_at"]
        if discard:
            assert row.result["by"] == "operator" and row.result["host_id"] == "current-daemon"
    assert (volume.data_path / "outputs" / "result.txt").read_bytes() == b"Kept"


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_status", ["active", "suspended", "archived"])
async def test_supervise_uses_product_lifecycle_and_preserves_status_and_attribution(operator, prior_status):
    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
        (await session.get(AgentInstanceRow, agent.id)).status = prior_status
    document = await operator.run("supervise", instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    assert document["returncode"] == 0 and document["verdict"] == "changed" and document["grants"] == {"instance": 7, "space": 27}
    assert document["previous"]["id"] == str(operator.fixture.owner.id) and document["previous"]["live"] is False
    assert document["supervisor"]["id"] == str(operator.fixture.other.id) and document["lifecycle_operation_id"]
    async with operator.fixture.sf() as session:
        row = await session.get(AgentInstanceRow, agent.id)
        assert row.status == prior_status and row.supervisor_id == str(operator.fixture.other.id)
        assert row.generation == document["generation"] == agent.generation + 1
        assert (await session.get(AgentInstanceGrantRow, (agent.id, str(operator.fixture.other.id)))).permissions == 7
        assert (await session.get(SpaceGrantRow, (agent.home_id, "human", str(operator.fixture.other.id)))).permissions == 27
        lifecycle = await session.get(AgentLifecycleRow, (agent.id, document["lifecycle_operation_id"]))
        assert lifecycle.phase == "complete" and lifecycle.actor_id == str(operator.fixture.other.id) and lifecycle.prior_status == prior_status
        events = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.action.in_(("operator-supervise-intent", "grant"))).order_by(SpaceEventRow.generation))).all()
        intent = next(event for event in events if event.action == "operator-supervise-intent")
        grant = next(event for event in events if event.action == "grant" and event.actor_id == str(operator.fixture.other.id))
        assert grant.generation == intent.generation + 1 and grant.details["operator_host_id"] == "current-daemon"
        assert (grant.actor_kind, grant.actor_id) == ("human", str(operator.fixture.other.id))
    repeated = await operator.run("supervise", instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    assert repeated["verdict"] == "unchanged" and repeated["lifecycle_operation_id"] is None


@pytest.mark.asyncio
async def test_supervise_resumes_only_its_exact_intent_after_the_first_grant_commits(operator, monkeypatch):
    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
    original = operator.files.registry.host_grant

    async def interrupted(*_args, **_kwargs):
        raise OSError("Interrupted before the Space grant")

    monkeypatch.setattr(operator.files.registry, "host_grant", interrupted)
    with pytest.raises(OSError, match="Interrupted before the Space grant"):
        await operator.run("supervise", instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    async with operator.fixture.sf() as session:
        assert (await session.get(AgentInstanceGrantRow, (agent.id, str(operator.fixture.other.id)))).permissions == 7
        assert await session.get(SpaceGrantRow, (agent.home_id, "human", str(operator.fixture.other.id))) is None
        before = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.action == "operator-supervise-intent"))).one()
        facts = before.generation, before.details, before.created_at
    monkeypatch.setattr(operator.files.registry, "host_grant", original)
    document = await operator.run("supervise", instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    assert document["verdict"] == "changed"
    async with operator.fixture.sf() as session:
        after = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.action == "operator-supervise-intent"))).one()
        assert (after.generation, after.details, after.created_at) == facts
        assert document["lifecycle_operation_id"] == after.details["lifecycle_operation_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("which", ["home", "instance"])
async def test_supervise_reserves_enough_generation_room_before_publishing_a_manager(operator, which):
    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
        if which == "home":
            (await session.get(SpaceRow, agent.home_id)).generation = 2**31 - 2
        else:
            (await session.get(AgentInstanceRow, agent.id)).generation = 2**31 - 1
    with pytest.raises(Exception, match="generation"):
        await operator.run("supervise", instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    async with operator.fixture.sf() as session:
        assert await session.get(AgentInstanceGrantRow, (agent.id, str(operator.fixture.other.id))) is None
        assert await session.get(SpaceGrantRow, (agent.home_id, "human", str(operator.fixture.other.id))) is None
        assert (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.action == "operator-supervise-intent"))).all() == []


@pytest.mark.asyncio
async def test_supervise_rechecks_manager_liveness_after_held_containment_proof_before_closure(operator, fixture):
    from test_native_resident_containment_scope import prepare
    from test_storage_spaces_attachments import ContainedProvider

    from app.gateway.agent_operator_contract import CommandRefused
    from deerflow.spaces.attachments import SpaceAttachments
    from deerflow.spaces.native_create_evidence import NativeEvidenceReader
    from deerflow.spaces.operator_containment import OperatorResidentContainmentScopes

    agent = await create(operator, custody="company")
    provider = ContainedProvider()
    attachments = SpaceAttachments(operator.files, provider)
    _gateway, saved, _captured, objects, calls = await prepare(
        (operator.files, operator.fixture.sf, operator.fixture.catalog, attachments, provider), fixture, space_id=agent.home_id, actor=PrincipalRef("human", str(operator.fixture.owner.id))
    )
    provider._operator_only = True
    evidence = NativeEvidenceReader(fixture[0].pool, daemon_id=provider.host_id)
    provider._containment_scope = OperatorResidentContainmentScopes(files=operator.files, adapter=provider, evidence=evidence)
    operator.adapter, operator.attachments = provider, None
    entered, release = asyncio.Event(), asyncio.Event()

    async def held():
        entered.set()
        await release.wait()
        operator.attachments = attachments

    operator._attachment_factory = held
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
    task = asyncio.create_task(operator.run("supervise", instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with operator.fixture.sf() as session, session.begin():
            (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = None
        release.set()
        with pytest.raises(CommandRefused, match="manager who can sign in"):
            await task
        assert objects and not any(command[1] in ("stop", "rm") for command in calls)
        async with operator.fixture.sf() as session:
            assert (await session.get(SpaceAttachmentRow, saved.attachment_id)).phase == "active"
            assert await session.get(AgentInstanceGrantRow, (agent.id, str(operator.fixture.other.id))) is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        evidence.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["host", "target"])
async def test_supervise_partial_grant_cannot_be_resumed_by_another_host_or_target(operator, monkeypatch, changed):
    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
    original = operator.files.registry.host_grant

    async def interrupted(*_args, **_kwargs):
        raise OSError("lost before Home grant")

    monkeypatch.setattr(operator.files.registry, "host_grant", interrupted)
    arguments = dict(instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    with pytest.raises(OSError, match="lost before Home grant"):
        await operator.run("supervise", **arguments)
    monkeypatch.setattr(operator.files.registry, "host_grant", original)
    if changed == "host":
        operator.adapter.host_id = "another-daemon"
    else:
        third = User(email="third@example.com")
        await operator.repository.create_user(third)
        arguments["email"] = str(third.email)
    with pytest.raises(RuntimeError, match="exact host and target"):
        await operator.run("supervise", **arguments)
    async with operator.fixture.sf() as session:
        intents = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.action == "operator-supervise-intent"))).all()
        assert len(intents) == 1 and intents[0].details["target_id"] == str(operator.fixture.other.id)
        assert (await session.get(AgentInstanceRow, agent.id)).supervisor_id == str(operator.fixture.owner.id)
        assert await session.get(SpaceGrantRow, (agent.home_id, "human", str(operator.fixture.other.id))) is None


@pytest.mark.asyncio
async def test_supervise_lost_home_grant_ack_reuses_the_committed_intent_and_grant(operator, monkeypatch):
    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
    original = operator.files.registry.host_grant

    async def lost_ack(*args, **kwargs):
        await original(*args, **kwargs)
        raise OSError("lost Home grant acknowledgement")

    monkeypatch.setattr(operator.files.registry, "host_grant", lost_ack)
    arguments = dict(instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    with pytest.raises(OSError, match="lost Home grant acknowledgement"):
        await operator.run("supervise", **arguments)
    monkeypatch.setattr(operator.files.registry, "host_grant", original)
    document = await operator.run("supervise", **arguments)
    assert document["verdict"] == "changed"
    async with operator.fixture.sf() as session:
        events = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.actor_id == str(operator.fixture.other.id)))).all()
        assert [event.action for event in sorted(events, key=lambda row: row.generation)] == ["operator-supervise-intent", "grant"]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["home-grant", "lifecycle"])
async def test_supervise_rechecks_old_manager_at_each_authority_boundary(operator, monkeypatch, phase):
    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
    original = operator.files.registry.host_grant

    async def reactivated(*args, **kwargs):
        if phase == "lifecycle":
            await original(*args, **kwargs)
        async with operator.fixture.sf() as session, session.begin():
            (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = None
        if phase == "home-grant":
            await original(*args, **kwargs)

    monkeypatch.setattr(operator.files.registry, "host_grant", reactivated)
    with pytest.raises(RuntimeError, match="manager who can sign in"):
        await operator.run("supervise", instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    async with operator.fixture.sf() as session:
        assert (await session.get(AgentInstanceRow, agent.id)).supervisor_id == str(operator.fixture.owner.id)
        assert (await session.get(AgentInstanceGrantRow, (agent.id, str(operator.fixture.other.id)))).permissions == 7
        grant = await session.get(SpaceGrantRow, (agent.home_id, "human", str(operator.fixture.other.id)))
        assert (grant.permissions if grant else None) == (27 if phase == "lifecycle" else None)
        assert (await session.scalars(select(AgentLifecycleRow).where(AgentLifecycleRow.instance_id == agent.id))).all() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", ["[]", "null", '{"facts":[]}'])
@pytest.mark.parametrize("discard", [False, True])
async def test_settle_malformed_receipt_remains_inspectable_and_can_be_explicitly_discarded(operator, payload, discard):
    from deerflow.spaces.workflows import WorkflowReceipt

    agent = await create(operator)
    identifier = operation()
    request = {"action": "feature-workflow", "namespace": "hm.shared"}
    async with operator.fixture.sf() as session, session.begin():
        generation = (await session.get(SpaceRow, agent.home_id)).generation
        binding = (await session.scalars(select(SpaceBackingRow))).one()
        session.add(SpaceFileOperationRow(space_id=agent.home_id, operation_id=identifier, actor_kind="human", actor_id=str(operator.fixture.owner.id), generation=generation, phase="pending", request=request))
    receipt = WorkflowReceipt(operator.fixture.catalog.verified[binding.slot_id], identifier, request)
    receipt.path.write_text(payload, encoding="utf-8")
    receipt.path.chmod(0o600)
    before = receipt.path.read_bytes(), receipt.path.stat().st_ino
    document = await operator.run("settle", space_id=agent.home_id, operation_id=identifier, discard=discard)
    assert (document["verdict"], document["returncode"]) == (("discarded", 0) if discard else ("unsettled", 2))
    assert document["receipt"] is None
    assert (receipt.path.read_bytes(), receipt.path.stat().st_ino) == before
    async with operator.fixture.sf() as session:
        assert (await session.get(SpaceFileOperationRow, (agent.home_id, identifier))).phase == ("failed" if discard else "pending")


@pytest.mark.asyncio
async def test_supervise_handoff_allows_a_later_independently_orphaned_recovery(operator):
    agent = await create(operator, custody="company")
    earlier = operation()
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
        current = await session.get(AgentInstanceRow, agent.id)
        current.generation += 1
        current.status = "suspended"
        home = await session.get(SpaceRow, agent.home_id)
        session.add(
            AgentLifecycleRow(
                instance_id=agent.id,
                operation_id=earlier,
                actor_id=str(operator.fixture.owner.id),
                generation=agent.generation,
                home_generation=home.generation,
                home_id=agent.home_id,
                request={"action": "suspend"},
                attachment_ids=[],
                phase="pending",
                prior_status="active",
            )
        )
    arguments = dict(instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    first = await operator.run("supervise", **arguments)
    assert first["verdict"] == "manager_added" and first["returncode"] == 2
    repeated = await operator.run("supervise", **arguments)
    assert repeated["verdict"] == "manager_added" and repeated["grants"] == {"instance": 7, "space": 27}
    # A human manager resolves the earlier product operation. Later, every
    # manager leaves; a different live person must be recoverable independently.
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(AgentLifecycleRow, (agent.id, earlier))).phase = "complete"
        (await session.get(UserRow, str(operator.fixture.other.id))).released_at = datetime.now(UTC)
    third = User(email="replacement@example.com")
    await operator.repository.create_user(third)
    arguments["email"] = str(third.email)
    second = await operator.run("supervise", **arguments)
    assert second["verdict"] == "changed" and second["supervisor"]["id"] == str(third.id)
    async with operator.fixture.sf() as session:
        intents = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.action == "operator-supervise-intent"))).all()
        assert len(intents) == 2 and intents[0].details != intents[1].details


@pytest.mark.asyncio
async def test_supervise_resumes_its_own_pending_lifecycle_without_new_grants(operator, monkeypatch):
    from deerflow.spaces.attachments import AttachmentPending, SpaceAttachments

    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
    original = SpaceAttachments.retire

    async def interrupted(self, **kwargs):
        await original(self, **kwargs)
        raise AttachmentPending("Confirmation was interrupted")

    monkeypatch.setattr(SpaceAttachments, "retire", interrupted)
    arguments = dict(instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    first = await operator.run("supervise", **arguments)
    assert first["verdict"] == "manager_added" and first["returncode"] == 2
    async with operator.fixture.sf() as session:
        pending = (await session.scalars(select(AgentLifecycleRow).where(AgentLifecycleRow.instance_id == agent.id))).one()
        assert pending.phase == "pending" and pending.actor_id == str(operator.fixture.other.id)
        selected_id = pending.operation_id
        assert (await session.get(AgentInstanceRow, agent.id)).status == "suspended"
    monkeypatch.setattr(SpaceAttachments, "retire", original)
    second = await operator.run("supervise", **arguments)
    assert second["verdict"] == "changed" and second["lifecycle_operation_id"] == selected_id
    async with operator.fixture.sf() as session:
        assert (await session.get(AgentLifecycleRow, (agent.id, selected_id))).phase == "complete"
        assert (await session.get(AgentInstanceRow, agent.id)).status == "active"
        events = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.actor_id == str(operator.fixture.other.id)))).all()
        assert {event.action for event in events} == {"operator-supervise-intent", "grant"}


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [None, "pending-retry", "later-orphan"])
async def test_supervise_hands_off_a_real_product_lifecycle_started_after_the_grants(operator, monkeypatch, interruption):
    from deerflow.agent_instances.lifecycle import InstanceLifecycle
    from deerflow.spaces.attachments import AttachmentPending, SpaceAttachments

    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
    # A newly admitted human manager starts an ordinary lifecycle at precisely
    # the grant gap. Its genuine withdrawal must remain the product's operation.
    operator.attachments = SpaceAttachments(operator.files, operator.adapter)
    host_grant, retire, product_id = operator.files.registry.host_grant, SpaceAttachments.retire, operation()

    async def pending_retire(self, **kwargs):
        await retire(self, **kwargs)
        raise AttachmentPending("Product confirmation is pending")

    async def product_at_gap(*args, **kwargs):
        await host_grant(*args, **kwargs)
        monkeypatch.setattr(SpaceAttachments, "retire", pending_retire)
        try:
            result = await InstanceLifecycle(operator.agents).change(actor=PrincipalRef("human", str(operator.fixture.other.id)), instance_id=agent.id, expected_generation=agent.generation, operation_id=product_id, action="archive")
            assert result["complete"] is False
        finally:
            monkeypatch.setattr(SpaceAttachments, "retire", retire)

    monkeypatch.setattr(operator.files.registry, "host_grant", product_at_gap)
    arguments = dict(instance_id=agent.id, email=str(operator.fixture.other.email), issuer=None, subject=None, end_running_work=False, wait_seconds=0)
    handoff = operator._handoff_recovery
    if interruption:

        async def interrupted(*_args):
            raise OSError("Handoff publication interrupted")

        monkeypatch.setattr(operator, "_handoff_recovery", interrupted)
        with pytest.raises(OSError, match="Handoff publication interrupted"):
            await operator.run("supervise", **arguments)
        monkeypatch.setattr(operator, "_handoff_recovery", handoff)
        monkeypatch.setattr(operator.files.registry, "host_grant", host_grant)
        if interruption == "later-orphan":
            async with operator.fixture.sf() as session, session.begin():
                (await session.get(AgentLifecycleRow, (agent.id, product_id))).phase = "complete"
                (await session.get(AgentInstanceRow, agent.id)).status = "archived"
                (await session.get(UserRow, str(operator.fixture.other.id))).released_at = datetime.now(UTC)
            third = User(email="successor@example.com")
            await operator.repository.create_user(third)
            arguments["email"] = str(third.email)
    result = await operator.run("supervise", **arguments)
    assert (result["verdict"], result["returncode"]) == (("changed", 0) if interruption == "later-orphan" else ("manager_added", 2))
    async with operator.fixture.sf() as session:
        product = await session.get(AgentLifecycleRow, (agent.id, product_id))
        assert product.phase == ("complete" if interruption == "later-orphan" else "pending") and product.request == {"action": "archive"} and product.actor_id == str(operator.fixture.other.id)
        if interruption == "later-orphan":
            current = await session.get(AgentInstanceRow, agent.id)
            assert current.supervisor_id == str(third.id) and current.status == "archived"
        else:
            assert (await session.get(AgentInstanceRow, agent.id)).supervisor_id == str(operator.fixture.owner.id)
            handoff_event = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.action == "operator-supervise-handoff"))).one()
            assert handoff_event.details["blocking_operation_id"] == product_id


@pytest.mark.asyncio
async def test_gateway_logs_operator_delete_only_after_actual_slot_reclamation(operator, monkeypatch, caplog):
    import logging

    from test_compose_user_reclamation import ContainmentFixture

    from app.gateway import storage_reclamation as module
    from app.gateway import storage_spaces as ss

    agent = await create(operator, custody="company")
    async with operator.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(operator.fixture.owner.id))).released_at = datetime.now(UTC)
        slot = (await session.scalars(select(SpaceBackingRow))).one().slot_id
    await operator.run("delete", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    provider = ContainmentFixture(operator.fixture.sf)
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=operator.files, storage_spaces_enabled=True, agent_instances=operator.agents, agent_execution=ss.ExecutionStatus(qualified_provider=provider)))

    async def no_users(_app):
        return 0

    monkeypatch.setattr(module, "reclaim_released_user_spaces", no_users)
    caplog.set_level(logging.INFO)
    expected = f"agents: employee {agent.id} deleted by the operator; space {slot} released"
    provider.proof_error = RuntimeError("absence is unconfirmed")
    assert await module.finish_releases(app) == 0
    assert expected not in caplog.messages
    provider.proof_error = None
    assert await module.finish_releases(app) == 1
    assert caplog.messages.count(expected) == 1
    assert await module.finish_releases(app) == 0
    assert caplog.messages.count(expected) == 1


@pytest.mark.asyncio
async def test_closure_wait_requires_terminal_runs_and_preserves_normal_completion_without_a_fence_reason(operator):
    agent = await create(operator)
    owner = str(operator.fixture.owner.id)
    for identifier, status in (("completed", "success"), ("expired", "running")):
        await operator.runs.put(identifier, thread_id=identifier, user_id=owner, status=status, metadata={"agent_instance_id": agent.id}, lease_expires_at=(datetime.now(UTC) - timedelta(seconds=5)).isoformat())
    work_id, attempt_id = operation(), operation()
    async with operator.fixture.sf() as session, session.begin():
        session.add(
            AgentWorkRow(
                id=work_id,
                instance_id=agent.id,
                creator_id=owner,
                objective="Check",
                success_criteria="Done",
                priority="normal",
                definition_revision=agent.definition_revision,
                assignment_revision=1,
                revision=1,
                status="open",
                review_required=False,
                review_state="none",
            )
        )
        await session.flush()
        session.add(
            AgentWorkAttemptRow(
                id=attempt_id,
                work_id=work_id,
                activation_id=operation(),
                requester_id=owner,
                assignment_revision=1,
                instance_generation=agent.generation,
                definition_revision=agent.definition_revision,
                status="succeeded",
                run_id="completed",
                request={},
                settled_at=datetime.now(UTC),
                contained_reason=None,
            )
        )
    result = await operator._wait_closed_work(["completed", "expired"], [attempt_id], wait_seconds=0.5)
    assert result["runs_found"] == 2 and result["runs_ended"] == 1 and result["runs_unconfirmed"] == ["expired"]
    assert result["attempts_settled"] == [{"attempt_id": attempt_id, "work_id": work_id, "run_id": "completed", "reason": None}]
    assert result["attempts_unconfirmed"] == []
