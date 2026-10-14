"""Owned startup barriers with real private evidence FDs and fake Docker only."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from test_native_create_provider import fixture as fixture
from test_native_create_provider import install
from test_resident_restoration import claim

from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.sandbox.admission import ProviderAdmission
from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError


@pytest.fixture
def build(fixture):
    def create(*, qualified=True):
        from deerflow.agent_instances.bootstrap import NativeAccountingProof, ResidentBootstrapCoordinator

        provider, owner = fixture
        install(provider, owner)
        admission = ProviderAdmission()
        provider._bind_runtime_admission(admission)
        attachments = SimpleNamespace(retained_claims=AsyncMock(return_value=(claim(),)), uncontained={})
        residents = ResidentRegistry(
            attachments=attachments,
            provider=provider,
            run_manager=SimpleNamespace(task_runs=lambda _key: {}),
            settle=AsyncMock(return_value=[]),
            owner_of=AsyncMock(return_value=None),
            replicas=3,
            idle_timeout=60,
        )
        clock = [100.0]
        validate = Mock(side_effect=lambda: None)

        async def account(token):
            return NativeAccountingProof.capture(provider, token, validate_locked=validate)

        coordinator = ResidentBootstrapCoordinator(
            registry=residents,
            admission=admission,
            entered=clock[0],
            clock=lambda: clock[0],
            drain_producers=AsyncMock(),
            account_native=AsyncMock(side_effect=account),
            reconcile=AsyncMock(return_value=qualified),
            drain_workers=AsyncMock(),
        )
        return coordinator, residents, provider, admission, clock, validate

    return create


@pytest.mark.anyio
@pytest.mark.parametrize("qualified", [True, False])
async def test_complete_accounting_publishes_atomically_independent_of_workspace_qualification(build, qualified):
    coordinator, residents, provider, admission, _clock, validate = build(qualified=qualified)
    validate.side_effect = lambda: None if provider._lock.locked() else (_ for _ in ()).throw(AssertionError("publication lock missing"))
    result = await coordinator.poll(timeout=2)
    assert result.workspace_qualified is qualified
    assert provider.external_slots == residents.external_slots and provider.evict_external == residents.evict_external
    admission.require(provider)
    with provider._lock:
        assert provider._counted_slots_locked() == (3, 0, 0, 1)
    assert await coordinator.poll(timeout=0) is result
    residents.attachments.retained_claims.assert_awaited_once()
    coordinator.drain_producers.assert_awaited_once()
    await coordinator.close()


@pytest.mark.anyio
async def test_absolute_deadline_retains_snapshot_and_retry_consumes_original_task(build):
    coordinator, residents, provider, admission, clock, _validate = build()
    entered, release = asyncio.Event(), asyncio.Event()

    async def snapshot():
        entered.set()
        await release.wait()
        return (claim(),)

    residents.attachments.retained_claims.side_effect = snapshot
    assert await coordinator.poll(timeout=0) is None
    await asyncio.wait_for(entered.wait(), 1)
    task = coordinator.task
    clock[0] += 130
    assert await coordinator.poll() is None and coordinator.task is task and not task.cancelled()
    assert coordinator.deadline == 220.0
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    release.set()
    assert await coordinator.poll(timeout=2) is not None
    residents.attachments.retained_claims.assert_awaited_once()
    await coordinator.close()


@pytest.mark.anyio
async def test_cancelled_waiter_does_not_cancel_owned_physical_accounting(build):
    coordinator, _residents, provider, admission, _clock, _validate = build()
    account = coordinator.account_native.side_effect
    entered, release = asyncio.Event(), asyncio.Event()

    async def held(token):
        entered.set()
        await release.wait()
        return await account(token)

    coordinator.account_native.side_effect = held
    waiter = asyncio.create_task(coordinator.poll(timeout=5))
    await asyncio.wait_for(entered.wait(), 1)
    owned = coordinator.task
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert not owned.done() and not owned.cancelling()
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    release.set()
    assert await coordinator.poll(timeout=2) is not None
    coordinator.account_native.assert_awaited_once()
    await coordinator.close()


@pytest.mark.anyio
async def test_snapshot_failure_keeps_populated_registry_and_retry_never_substitutes_it(build):
    coordinator, residents, provider, admission, _clock, _validate = build()
    old = claim("f")
    token = residents.begin_restore()
    residents.finish_restore(token, (old,))
    residents.attachments.retained_claims.side_effect = RuntimeError("SQL unavailable")
    assert await coordinator.poll(timeout=2) is None
    assert residents.retained == {old.id: old} and not residents.restoration_complete
    assert isinstance(coordinator.last_error, RuntimeError)
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    residents.attachments.retained_claims.side_effect = None
    assert await coordinator.poll(timeout=2) is not None
    assert coordinator.registry is residents and residents.retained[old.id] is old
    assert provider.external_slots() == 2
    await coordinator.close()


@pytest.mark.anyio
async def test_native_completeness_failure_keeps_sql_charges_and_gate_closed(build):
    coordinator, residents, provider, admission, _clock, validate = build()
    validate.side_effect = RuntimeError("incomplete census or undrained producer")
    assert await coordinator.poll(timeout=2) is None
    assert residents.restoration_complete and len(residents.retained) == 1
    assert provider.external_slots() == 0
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    validate.side_effect = lambda: None
    assert await coordinator.poll(timeout=2) is not None
    assert provider.external_slots() == 1
    await coordinator.close()


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["epoch", "bridge", "evidence_revision", "attachments"])
async def test_late_publication_rechecks_exact_lifecycle_and_native_evidence(build, change):
    coordinator, residents, provider, admission, _clock, _validate = build()

    async def changed():
        if change == "epoch":
            provider.external_changed(lambda: admission.open(provider))
            provider._close_runtime_admission()
        elif change == "bridge":
            provider._native_creation = object()
        elif change == "evidence_revision":
            provider._native_creation.evidence._teardown_revision += 1
        else:
            residents.attachments = object()
        return True

    coordinator.reconcile.side_effect = changed
    assert await coordinator.poll(timeout=2) is None
    assert residents.retained and provider.external_slots() == 0
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    await coordinator.close()


@pytest.mark.anyio
async def test_success_boolean_cannot_replace_exact_native_accounting_proof(build):
    coordinator, residents, provider, admission, _clock, _validate = build()
    coordinator.account_native.side_effect = None
    coordinator.account_native.return_value = True
    assert await coordinator.poll(timeout=2) is None
    assert residents.retained and isinstance(coordinator.last_error, TypeError)
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    await coordinator.close()


@pytest.mark.anyio
async def test_shutdown_seals_first_and_drains_workers_initializer_then_registry(build):
    coordinator, residents, provider, admission, _clock, _validate = build()
    entered, release = asyncio.Event(), asyncio.Event()
    order = []

    async def reconcile():
        entered.set()
        await release.wait()
        order.append("initializer")
        return True

    async def workers():
        with pytest.raises(SandboxRuntimeAdmissionError):
            admission.require(provider)
        order.append("workers")

    registry_close = residents.close

    async def close_registry():
        order.append("registry")
        await registry_close()

    coordinator.reconcile.side_effect = reconcile
    coordinator.drain_workers.side_effect = workers
    residents.close = close_registry
    assert await coordinator.poll(timeout=0) is None
    await asyncio.wait_for(entered.wait(), 1)
    closing = asyncio.create_task(coordinator.close())
    await asyncio.sleep(0)
    assert not closing.done()
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.open(provider)
    release.set()
    await closing
    assert order == ["workers", "initializer", "registry"]
    assert provider.external_slots() == 0 and coordinator.result is None


@pytest.mark.anyio
async def test_shutdown_worker_failure_still_drains_initializer_and_registry(build):
    coordinator, residents, _provider, _admission, _clock, _validate = build()
    await coordinator.poll(timeout=2)
    coordinator.drain_workers.side_effect = RuntimeError("worker drain failed")
    residents.close = AsyncMock(wraps=residents.close)
    with pytest.raises(ExceptionGroup):
        await coordinator.close()
    residents.close.assert_awaited_once()
    assert coordinator.task is None
    with pytest.raises(ExceptionGroup):
        await coordinator.close()
    coordinator.drain_workers.assert_awaited_once()


@pytest.mark.anyio
async def test_cancelled_and_concurrent_close_waiters_share_one_owned_drain(build):
    coordinator, residents, provider, admission, _clock, _validate = build()
    await coordinator.poll(timeout=2)
    entered, release = asyncio.Event(), asyncio.Event()

    async def workers():
        entered.set()
        await release.wait()

    coordinator.drain_workers.side_effect = workers
    residents.close = AsyncMock(wraps=residents.close)
    first = asyncio.create_task(coordinator.close())
    await asyncio.wait_for(entered.wait(), 1)
    second = asyncio.create_task(coordinator.close())
    first.cancel()
    await asyncio.sleep(0)
    assert not first.done() and not second.done()
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    assert provider.external_slots() == 1
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second
    coordinator.drain_workers.assert_awaited_once()
    residents.close.assert_awaited_once()


@pytest.mark.anyio
async def test_failed_callback_cannot_leave_reopened_admission_with_empty_callbacks(build):
    coordinator, residents, provider, admission, _clock, _validate = build()

    async def bad_reconcile():
        provider.external_changed(lambda: admission.open(provider))
        raise RuntimeError("reconciliation failed after a lifecycle transition")

    coordinator.reconcile.side_effect = bad_reconcile
    assert await coordinator.poll(timeout=2) is None
    assert residents.retained
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    await coordinator.close()


@pytest.mark.anyio
async def test_invalidation_cannot_discard_an_unconsumed_initializer_result(build):
    coordinator, _residents, provider, admission, _clock, _validate = build()
    assert await coordinator.poll(timeout=0) is None
    owned = coordinator.task
    await asyncio.wait_for(asyncio.shield(owned), 2)
    with pytest.raises(RuntimeError, match="owned"):
        coordinator.invalidate()
    admission.require(provider)
    assert await coordinator.poll(timeout=0) is not None
    coordinator.invalidate()
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    assert provider.external_slots() == 1
    assert await coordinator.poll(timeout=2) is not None
    await coordinator.close()


@pytest.mark.anyio
async def test_submitted_producer_commit_is_in_complete_snapshot_before_publication(build):
    coordinator, residents, provider, admission, _clock, _validate = build()
    rows = [claim()]
    entered, release = asyncio.Event(), asyncio.Event()

    async def drain():
        entered.set()
        await release.wait()
        # Simulate an already submitted legacy preparation committing without
        # an in-process typed update. The post-drain SQL census must include it.
        rows.append(claim("f"))

    async def snapshot():
        return tuple(rows)

    coordinator.drain_producers.side_effect = drain
    residents.attachments.retained_claims.side_effect = snapshot
    assert await coordinator.poll(timeout=0) is None
    await asyncio.wait_for(entered.wait(), 1)
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    release.set()
    assert await coordinator.poll(timeout=2) is not None
    assert residents.retained == {row.id: row for row in rows}
    assert provider.external_slots() == 2
    await coordinator.close()
