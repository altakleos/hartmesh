"""Host settlement uses exact durable containment, without runtime installation."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from _storage_spaces_test_support import ALICE, home, operation
from sqlalchemy import select
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_storage_spaces_attachments import ContainedProvider
from test_work_execution import activation, run_record
from test_work_execution import execution as execution

from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow, StorageHostReplacementRow
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments


async def started(execution, *, ready=False):
    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    record = run_record(admitted, scope)
    await scope.bind(admitted, record, admitted.thread_incarnation)
    mounts = SpaceAttachments(service.agents.files, ContainedProvider())
    attachment = await mounts.attach(actor=admitted.instance.principal, incarnation=operation(), resources=[ResourceMount(admitted.instance.home_id, admitted.home_generation, "home", writable=True)])
    if ready:
        await scope.workspace_ready(admitted, attachment.id)
    return service, admitted, row, sf, scope, record, mounts, attachment


@pytest.mark.asyncio
async def test_workspace_ready_records_correlation_without_advancing_work(execution):
    service, admitted, row, sf, scope, _record, _mounts, attached = await started(execution)
    before = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    await scope.workspace_ready(admitted, attached.id)
    after = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert after["revision"] == before["revision"]
    async with sf() as session:
        attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
        assert attempt.attachment_id == attached.id and attempt.workspace_ready_at is not None


@pytest.mark.asyncio
async def test_activation_exists_reads_the_exact_work_and_operation(execution):
    service, admitted, row, _sf = execution
    request = activation(admitted, row)
    assert not await service.activation_exists(row["id"], request.operation_id)
    await service.reserve(execution=admitted, work_id=row["id"], request=request)
    assert await service.activation_exists(row["id"], request.operation_id)
    assert not await service.activation_exists(operation(), request.operation_id)
    assert not await service.activation_exists(row["id"], operation())


@pytest.mark.asyncio
async def test_host_settlement_preserves_candidate_work_fields_and_writes_one_attributed_event(execution):
    service, admitted, row, sf, scope, record, mounts, attached = await started(execution, ready=True)
    current = (await scope.context(admitted))["work"]
    await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="outcome", statement="Candidate awaiting confirmed cleanup."))
    await scope.finish(admitted, record, settled=False)
    before = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert await mounts.contain_host(attached.id, "idle")
    arguments = {"instance_id": admitted.instance.id, "host_id": attached.host_id, "reason": "idle", "attachment_id": attached.id, "live_run_ids": frozenset()}
    assert await service.settle_contained(**arguments) == [scope.attempt_id]
    assert await service.settle_contained(**arguments) == []
    after = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert after["revision"] == before["revision"] + 1
    assert {key: after[key] for key in ("status", "outcome", "review", "next_action")} == {key: before[key] for key in ("status", "outcome", "review", "next_action")}
    assert after["attempt"]["candidate"] == before["attempt"]["candidate"]
    assert after["attempt"]["status"] == "failed"
    async with sf() as session:
        attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
        assert attempt.contained_reason == "idle" and attempt.settled_at is not None
        event = (await session.execute(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == row["id"], AgentWorkEventRow.action == "attempt_contained"))).scalar_one()
        assert (event.actor_kind, event.actor_id) == ("host", attached.host_id)
        assert event.request == {"attempt_id": scope.attempt_id, "run_id": record.run_id, "status": "failed", "reason": "idle", "attachment_id": attached.id}
    assert await scope.finish(admitted, record, settled=True) == "failed"


@pytest.mark.asyncio
async def test_host_settlement_preserves_live_worker_then_waits_for_fenced_attachment(execution):
    service, admitted, _row, sf, scope, record, mounts, attached = await started(execution, ready=True)
    arguments = {"instance_id": admitted.instance.id, "host_id": attached.host_id, "reason": "idle", "attachment_id": attached.id}
    assert await service.settle_contained(**arguments, live_run_ids=frozenset()) == []
    mounts.provider.uncertain = True
    assert not await mounts.contain_host(attached.id, "outcome-uncertain")
    assert await service.settle_contained(**arguments, live_run_ids=frozenset()) == []
    mounts.provider.uncertain = False
    assert await mounts.contain_host(attached.id, "idle")
    assert await service.settle_contained(**arguments, live_run_ids=frozenset({record.run_id})) == []
    assert await service.settle_contained(**arguments, live_run_ids=frozenset()) == [scope.attempt_id]
    async with sf() as session:
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).contained_reason == "outcome-uncertain"


@pytest.mark.asyncio
async def test_unstarted_attempt_settles_only_after_its_utc_age_cutoff(execution):
    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    arguments = {"instance_id": admitted.instance.id, "host_id": "qualified-test-host", "reason": "run-orphaned", "attachment_id": None, "live_run_ids": frozenset(), "unstarted_after": 120}
    assert await service.settle_contained(**arguments) == []
    async with sf() as session, session.begin():
        (await session.get(AgentWorkAttemptRow, scope.attempt_id)).created_at = datetime.now(UTC) - timedelta(seconds=121)
    assert await service.settle_contained(**arguments) == [scope.attempt_id]
    async with sf() as session:
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).contained_reason == "run-never-started"


@pytest.mark.asyncio
async def test_finish_returns_new_and_existing_status_without_changing_decisions(execution):
    service, admitted, row, _sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    record = run_record(admitted, scope)
    await scope.bind(admitted, record, admitted.thread_incarnation)
    assert await scope.finish(admitted, record, settled=True) == "succeeded"
    assert await scope.finish(admitted, record, settled=False) == "succeeded"


@pytest.mark.parametrize("changed", ["container", "host", "generation"])
@pytest.mark.asyncio
async def test_workspace_ready_rejects_invalid_container_or_foreign_host(execution, changed):
    _service, admitted, _row, sf, scope, _record, _mounts, attached = await started(execution)
    async with sf() as session, session.begin():
        durable = await session.get(SpaceAttachmentRow, attached.id)
        if changed == "container":
            durable.container_id = "invalid"
        elif changed == "host":
            durable.host_id = "foreign-host"
        else:
            (await session.get(SpaceMountRow, (attached.id, admitted.instance.home_id))).generation += 1
    with pytest.raises(AgentDenied):
        await scope.workspace_ready(admitted, attached.id)
    async with sf() as session:
        attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
        assert attempt.attachment_id is None and attempt.workspace_ready_at is None


@pytest.mark.asyncio
async def test_host_settlement_requires_exact_callback_attachment(execution):
    service, admitted, _row, _sf, scope, _record, mounts, attached = await started(execution, ready=True)
    assert await mounts.contain_host(attached.id, "idle")
    arguments = {"instance_id": admitted.instance.id, "host_id": attached.host_id, "reason": "idle", "live_run_ids": frozenset()}
    assert await service.settle_contained(**arguments, attachment_id=operation()) == []
    assert await service.settle_contained(**arguments, attachment_id=attached.id) == [scope.attempt_id]


@pytest.mark.asyncio
async def test_host_settlement_accepts_historical_generation_and_human_reason(execution):
    service, admitted, _row, sf, scope, _record, mounts, attached = await started(execution, ready=True)
    await mounts.retire(actor=ALICE, space_id=admitted.instance.home_id, expected_generation=admitted.home_generation, reason="storage-recovery")
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, admitted.instance.id)).generation += 1
    assert await service.settle_contained(instance_id=admitted.instance.id, host_id=attached.host_id, reason="idle", attachment_id=attached.id, live_run_ids=frozenset()) == [scope.attempt_id]
    async with sf() as session:
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).contained_reason == "storage-recovery"


@pytest.mark.asyncio
async def test_host_settlement_rolls_back_attempt_work_event_and_journal_on_failure(execution, monkeypatch, caplog):
    service, admitted, row, sf, scope, _record, mounts, attached = await started(execution, ready=True)
    assert await mounts.contain_host(attached.id, "idle")
    before = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    original = service._advance

    def fail(_row):
        raise RuntimeError("injected publication failure")

    monkeypatch.setattr(service, "_advance", fail)
    arguments = {"instance_id": admitted.instance.id, "host_id": attached.host_id, "reason": "idle", "attachment_id": attached.id, "live_run_ids": frozenset()}
    with caplog.at_level("INFO"), pytest.raises(RuntimeError, match="publication"):
        await service.settle_contained(**arguments)
    assert not any("agents: attempt" in record.message for record in caplog.records)
    after = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert after["revision"] == before["revision"] and after["attempt"]["status"] == "running"
    async with sf() as session:
        assert (await session.execute(select(AgentWorkEventRow).where(AgentWorkEventRow.action == "attempt_contained"))).scalars().all() == []
    monkeypatch.setattr(service, "_advance", original)
    assert await service.settle_contained(**arguments) == [scope.attempt_id]


@pytest.mark.asyncio
async def test_worker_and_host_settlement_preserve_first_terminal_commit(execution):
    service, admitted, row, sf, scope, record, mounts, attached = await started(execution, ready=True)
    assert await mounts.contain_host(attached.id, "idle")
    await asyncio.gather(
        scope.finish(admitted, record, settled=True),
        service.settle_contained(instance_id=admitted.instance.id, host_id=attached.host_id, reason="idle", attachment_id=attached.id, live_run_ids=frozenset()),
    )
    async with sf() as session:
        attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
        assert attempt.status in ("succeeded", "failed")
        events = (await session.execute(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == row["id"], AgentWorkEventRow.action.in_(("attempt_contained", "attempt_settled"))))).scalars().all()
        assert len(events) == 1
        assert events[0].request["status"] == attempt.status


@pytest.mark.parametrize("changed", ["actor", "home"])
@pytest.mark.asyncio
async def test_host_settlement_refuses_wrong_actor_or_home_correlation(execution, changed):
    service, admitted, _row, sf, scope, _record, mounts, attached = await started(execution, ready=True)
    assert await mounts.contain_host(attached.id, "idle")
    other = await home(service.agents.files) if changed == "home" else None
    async with sf() as session, session.begin():
        if changed == "actor":
            durable = await session.get(SpaceAttachmentRow, attached.id)
            durable.actor_kind, durable.actor_id = "human", "alice"
        else:
            (await session.get(SpaceMountRow, (attached.id, admitted.instance.home_id))).space_id = other.id
    assert await service.settle_contained(instance_id=admitted.instance.id, host_id=attached.host_id, reason="idle", attachment_id=attached.id, live_run_ids=frozenset()) == []
    async with sf() as session:
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "running"


@pytest.mark.asyncio
async def test_host_settlement_requires_recorded_host_replacement(execution):
    service, admitted, _row, sf, scope, _record, mounts, attached = await started(execution, ready=True)
    assert await mounts.contain_host(attached.id, "idle")
    async with sf() as session, session.begin():
        (await session.get(SpaceAttachmentRow, attached.id)).host_id = "previous-host"
    arguments = {"instance_id": admitted.instance.id, "host_id": attached.host_id, "reason": "idle", "attachment_id": attached.id, "live_run_ids": frozenset()}
    assert await service.settle_contained(**arguments) == []
    async with sf() as session, session.begin():
        session.add(StorageHostReplacementRow(old_host_id="previous-host", new_host_id=attached.host_id, attachments_named=1))
    assert await service.settle_contained(**arguments) == [scope.attempt_id]


@pytest.mark.asyncio
async def test_host_settlement_preserves_worker_first_committed_outcome(execution):
    service, admitted, row, sf, scope, record, mounts, attached = await started(execution, ready=True)
    assert await mounts.contain_host(attached.id, "idle")
    assert await scope.finish(admitted, record, settled=True) == "succeeded"
    before = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert await service.settle_contained(instance_id=admitted.instance.id, host_id=attached.host_id, reason="idle", attachment_id=attached.id, live_run_ids=frozenset()) == []
    after = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert (after["revision"], after["attempt"]["status"]) == (before["revision"], "succeeded")
    async with sf() as session:
        assert (await session.execute(select(AgentWorkEventRow).where(AgentWorkEventRow.action == "attempt_contained"))).scalars().all() == []
