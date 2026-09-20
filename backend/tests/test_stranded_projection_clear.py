"""A thread held under a clear nobody still holds a token for.

``release_accepted_skill_consumer`` retries only for a caller that still has
the exact consumer token. When the last holder of that token is gone -- the
worker finished its terminal cleanup, or the process that bound it is no
longer running the turn -- the thread stays fenced as clearing and every later
admission on it is refused. The coordinator's own ``clearing`` state is the
fact that says so, and finishing the clear from it is the point of action.
"""

from __future__ import annotations

import pytest

from deerflow.runtime.skill_projection import (
    SkillProjectionClear,
    get_skill_projection_coordinator,
)
from deerflow.sandbox.accepted_material import AcceptedSkillSandboxBindingV1
from deerflow.sandbox.accepted_projection import (
    complete_pending_projection_clear,
)
from deerflow.sandbox.capabilities import AcceptedSkillProjection
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import (
    SandboxProvider,
    reset_sandbox_provider,
    set_sandbox_provider,
)


class _ClearingProvider(SandboxProvider, AcceptedSkillProjection):
    """Records every clear attempt and answers with a scripted result."""

    def __init__(self, *, clear_results: list[bool], absent_results: list[bool] | None = None) -> None:
        self.clear_results = list(clear_results)
        self.absent_results = list(absent_results or [])
        self.clear_attempts = 0
        self.absent_attempts = 0
        self.release_attempts = 0

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        del thread_id, user_id
        raise AssertionError("the stranded-clear path must not acquire a sandbox")

    def get(self, sandbox_id: str) -> Sandbox | None:
        del sandbox_id
        return None

    def has_accepted_skill_isolation(self, sandbox_id: str) -> bool:
        del sandbox_id
        return True

    def release(self, sandbox_id: str) -> None:
        del sandbox_id
        self.release_attempts += 1

    def bind_accepted_skill_snapshot(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> None:
        del sandbox_id, thread_id, user_id, binding

    def clear_accepted_skill_snapshot(self, clear: SkillProjectionClear) -> bool:
        del clear
        self.clear_attempts += 1
        if not self.clear_results:
            return True
        return self.clear_results.pop(0)

    def ensure_accepted_skill_snapshot_absent(self, clear: SkillProjectionClear) -> bool:
        del clear
        self.absent_attempts += 1
        if not self.absent_results:
            return False
        return self.absent_results.pop(0)


def _strand(*, user_id: str, thread_id: str, run_id: str, provider: _ClearingProvider) -> None:
    """Drive one release that cannot confirm, leaving the thread clearing."""
    from deerflow.sandbox.accepted_projection import release_accepted_skill_consumer

    coordinator = get_skill_projection_coordinator()
    coordinator.claim_committed_run(
        user_id=user_id,
        thread_id=thread_id,
        run_id=run_id,
        snapshot_id=None,
    )
    token = coordinator.activate(
        user_id=user_id,
        thread_id=thread_id,
        sandbox_id=f"sandbox-{thread_id}",
        run_id=run_id,
        snapshot_id=None,
        consumer_id=f"run:{run_id}:lead",
    )
    set_sandbox_provider(provider)
    assert release_accepted_skill_consumer(token) is False
    assert coordinator.is_busy(user_id=user_id, thread_id=thread_id)


def test_pending_clear_is_the_thread_s_own_clearing_proof() -> None:
    coordinator = get_skill_projection_coordinator()
    provider = _ClearingProvider(clear_results=[False])
    user_id = "owner-pending-clear"
    thread_id = "thread-pending-clear"
    try:
        assert coordinator.pending_clear(user_id=user_id, thread_id=thread_id) is None
        _strand(user_id=user_id, thread_id=thread_id, run_id="run-pending-clear", provider=provider)
        clear = coordinator.pending_clear(user_id=user_id, thread_id=thread_id)
        assert clear is not None
        assert coordinator.is_clearing(clear)
        assert clear.run_id == "run-pending-clear"
        assert clear.sandbox_id == f"sandbox-{thread_id}"
    finally:
        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is True
        reset_sandbox_provider()


def test_stranded_clear_is_finished_from_identity_alone() -> None:
    coordinator = get_skill_projection_coordinator()
    provider = _ClearingProvider(clear_results=[False, True])
    user_id = "owner-stranded"
    thread_id = "thread-stranded"
    try:
        _strand(user_id=user_id, thread_id=thread_id, run_id="run-stranded", provider=provider)

        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is True
        assert provider.clear_attempts == 2
        assert provider.release_attempts == 1
        assert not coordinator.is_busy(user_id=user_id, thread_id=thread_id)
        assert coordinator.try_claim_committed_run(
            user_id=user_id,
            thread_id=thread_id,
            run_id="run-after-stranded",
            snapshot_id=None,
        )
    finally:
        coordinator.release_unactivated_run(
            user_id=user_id,
            thread_id=thread_id,
            run_id="run-after-stranded",
        )
        reset_sandbox_provider()


def test_unconfirmed_clear_keeps_the_thread_fenced() -> None:
    coordinator = get_skill_projection_coordinator()
    provider = _ClearingProvider(clear_results=[False, False])
    user_id = "owner-unconfirmed"
    thread_id = "thread-unconfirmed"
    try:
        _strand(user_id=user_id, thread_id=thread_id, run_id="run-unconfirmed", provider=provider)

        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is False
        assert provider.absent_attempts == 2
        assert provider.release_attempts == 0
        assert coordinator.is_busy(user_id=user_id, thread_id=thread_id)
    finally:
        provider.clear_results = [True]
        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is True
        reset_sandbox_provider()


def test_absence_proof_finishes_a_clear_the_provider_cannot_compare() -> None:
    coordinator = get_skill_projection_coordinator()
    provider = _ClearingProvider(clear_results=[False, False], absent_results=[False, True])
    user_id = "owner-absence"
    thread_id = "thread-absence"
    try:
        _strand(user_id=user_id, thread_id=thread_id, run_id="run-absence", provider=provider)

        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is True
        assert provider.absent_attempts == 2
        assert not coordinator.is_busy(user_id=user_id, thread_id=thread_id)
    finally:
        reset_sandbox_provider()


def test_a_thread_a_consumer_still_owns_is_never_cleared() -> None:
    coordinator = get_skill_projection_coordinator()
    provider = _ClearingProvider(clear_results=[True])
    user_id = "owner-live"
    thread_id = "thread-live"
    run_id = "run-live"
    coordinator.claim_committed_run(
        user_id=user_id,
        thread_id=thread_id,
        run_id=run_id,
        snapshot_id=None,
    )
    token = coordinator.activate(
        user_id=user_id,
        thread_id=thread_id,
        sandbox_id="sandbox-live",
        run_id=run_id,
        snapshot_id=None,
        consumer_id=f"run:{run_id}:lead",
    )
    set_sandbox_provider(provider)
    try:
        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is False
        assert provider.clear_attempts == 0
        assert provider.absent_attempts == 0
        assert coordinator.owns(token)
    finally:
        from deerflow.sandbox.accepted_projection import release_accepted_skill_consumer

        release_accepted_skill_consumer(token)
        reset_sandbox_provider()


def test_an_unknown_thread_asks_the_provider_nothing() -> None:
    provider = _ClearingProvider(clear_results=[True])
    set_sandbox_provider(provider)
    try:
        assert complete_pending_projection_clear(user_id="owner-absent", thread_id="thread-absent") is False
        assert provider.clear_attempts == 0
        assert provider.absent_attempts == 0
    finally:
        reset_sandbox_provider()


@pytest.mark.parametrize("failure", [RuntimeError("clear exploded"), ValueError("bad clear")])
def test_a_raising_provider_leaves_the_thread_fenced_without_propagating(failure: Exception) -> None:
    coordinator = get_skill_projection_coordinator()
    provider = _ClearingProvider(clear_results=[False])
    user_id = "owner-raising"
    thread_id = f"thread-raising-{type(failure).__name__}"
    try:
        _strand(user_id=user_id, thread_id=thread_id, run_id="run-raising", provider=provider)

        def _raise(clear: SkillProjectionClear) -> bool:
            del clear
            raise failure

        provider.clear_accepted_skill_snapshot = _raise  # type: ignore[method-assign]
        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is False
        assert coordinator.is_busy(user_id=user_id, thread_id=thread_id)
    finally:
        del provider.clear_accepted_skill_snapshot
        provider.clear_results = [True]
        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is True
        reset_sandbox_provider()


@pytest.mark.asyncio
async def test_admission_finishes_a_stranded_clear_instead_of_refusing_the_thread(
    monkeypatch,
) -> None:
    """The tenant symptom: the next message on the chat, not a restart."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.gateway import services
    from deerflow.runtime import ConflictError

    coordinator = get_skill_projection_coordinator()
    user_id = "projection-owner"
    thread_id = "thread-admission-stranded"
    provider = _ClearingProvider(clear_results=[False, False])
    _strand(user_id=user_id, thread_id=thread_id, run_id="run-admission-stranded", provider=provider)

    manager = SimpleNamespace(
        get_by_external_identity=AsyncMock(return_value=None),
        ensure_or_reject=AsyncMock(),
    )
    seed = AsyncMock()
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(services, "ensure_checkpoint_history_seeded", seed)
    adapter = services._GatewayDurableRuns(SimpleNamespace())

    material = SimpleNamespace(skill_snapshot=None)
    launch = SimpleNamespace(
        thread_id=thread_id,
        assistant_id="lead-agent",
        multitask_strategy="reject",
        user_id=user_id,
        accepted_invocation=SimpleNamespace(
            agent_revision=SimpleNamespace(material=material),
        ),
        external_scope=None,
        external_key=None,
    )

    try:
        # Still unconfirmed: the thread stays refused, exactly as before.
        async with adapter.admission_scope(thread_id):
            with pytest.raises(ConflictError, match="skill projection"):
                await adapter.prepare_admission(launch)
        seed.assert_not_awaited()

        # The provider can now prove it gone; the same admission goes through.
        provider.clear_results = [True]
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)
        seed.assert_awaited_once()
        assert coordinator.is_busy(user_id=user_id, thread_id=thread_id)
    finally:
        for reservation in list(adapter._projection_reservations.values()):
            coordinator.abort_admission(reservation)
        reset_sandbox_provider()


@pytest.mark.asyncio
async def test_worker_claim_finishes_a_stranded_clear_before_giving_up(
    monkeypatch,
) -> None:
    """The replacement run's bounded wait, whose only other answer is refusal."""
    import asyncio
    from types import SimpleNamespace

    from deerflow.runtime.runs import worker
    from deerflow.runtime.skill_projection import SkillProjectionBusyError

    coordinator = get_skill_projection_coordinator()
    user_id = "owner-worker-claim"
    thread_id = "thread-worker-claim"
    provider = _ClearingProvider(clear_results=[False, False])
    _strand(user_id=user_id, thread_id=thread_id, run_id="run-worker-claim", provider=provider)
    monkeypatch.setattr(worker, "_SKILL_PROJECTION_CLAIM_TIMEOUT_SECONDS", 0.0)
    material = SimpleNamespace(skill_snapshot=None)

    async def _claim(run_id: str) -> bool:
        return await worker._await_accepted_skill_projection_claim(
            user_id=user_id,
            thread_id=thread_id,
            run_id=run_id,
            material=material,
            abort_event=asyncio.Event(),
        )

    try:
        with pytest.raises(SkillProjectionBusyError):
            await _claim("run-worker-replacement")
        assert coordinator.is_busy(user_id=user_id, thread_id=thread_id)

        provider.clear_results = [True]
        assert await _claim("run-worker-replacement") is True
    finally:
        coordinator.release_unactivated_run(
            user_id=user_id,
            thread_id=thread_id,
            run_id="run-worker-replacement",
        )
        reset_sandbox_provider()


def test_a_park_that_raises_still_freed_the_thread_and_says_so() -> None:
    """Parking runs after the fence is released, so its failure is not a fence."""
    coordinator = get_skill_projection_coordinator()
    provider = _ClearingProvider(clear_results=[False, True])
    user_id = "owner-park-failure"
    thread_id = "thread-park-failure"
    try:
        _strand(user_id=user_id, thread_id=thread_id, run_id="run-park-failure", provider=provider)

        def _raise(sandbox_id: str) -> None:
            del sandbox_id
            raise RuntimeError("parking the sandbox failed")

        provider.release = _raise  # type: ignore[method-assign]
        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is True
        assert not coordinator.is_busy(user_id=user_id, thread_id=thread_id)
    finally:
        reset_sandbox_provider()


def test_a_second_driver_of_one_clear_touches_nothing() -> None:
    """The park is the unfenced step: a late driver must not repeat it.

    Once the first driver finalizes, the thread is free and a new turn may
    reclaim the same warm sandbox at once. A second driver arriving at the
    park would then stop a container that run is executing in.
    """
    coordinator = get_skill_projection_coordinator()
    provider = _ClearingProvider(clear_results=[False])
    user_id = "owner-second-driver"
    thread_id = "thread-second-driver"
    try:
        _strand(user_id=user_id, thread_id=thread_id, run_id="run-second-driver", provider=provider)
        clear = coordinator.pending_clear(user_id=user_id, thread_id=thread_id)
        assert clear is not None

        provider.clear_results = [True]
        assert complete_pending_projection_clear(user_id=user_id, thread_id=thread_id) is True
        before = (provider.clear_attempts, provider.absent_attempts, provider.release_attempts)

        from deerflow.sandbox.accepted_projection import _drive_clear

        # The proof a late driver still holds; the thread behind it is gone.
        assert _drive_clear(clear) is True
        assert (provider.clear_attempts, provider.absent_attempts, provider.release_attempts) == before
        assert coordinator.finalize_release(clear) is False
    finally:
        reset_sandbox_provider()


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["reject", "interrupt"])
async def test_admission_of_every_strategy_takes_the_freed_thread_normally(
    monkeypatch,
    strategy: str,
) -> None:
    """A freed thread has no owner to supersede, whatever the strategy is."""
    import hashlib
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.gateway import services
    from deerflow.runtime.skill_projection import SkillProjectionEvidence
    from deerflow.runtime.skill_snapshot import SkillSnapshotProjection

    coordinator = get_skill_projection_coordinator()
    user_id = "projection-owner"
    thread_id = f"thread-strategy-{strategy}"
    provider = _ClearingProvider(clear_results=[False, True])
    _strand(user_id=user_id, thread_id=thread_id, run_id=f"run-strategy-{strategy}", provider=provider)

    monkeypatch.setattr(
        services,
        "get_run_manager",
        lambda _request: SimpleNamespace(
            get_by_external_identity=AsyncMock(return_value=None),
            ensure_or_reject=AsyncMock(),
        ),
    )
    monkeypatch.setattr(services, "ensure_checkpoint_history_seeded", AsyncMock())
    adapter = services._GatewayDurableRuns(SimpleNamespace())

    # The production shape: a committed snapshot, not the empty set.
    digest = hashlib.sha256(b"strategy-snapshot").hexdigest()
    projection = SkillSnapshotProjection(
        name="report",
        category="public",
        relative_path="report",
        manifest_digest=digest,
        content_digest=digest,
        file_count=1,
        total_bytes=7,
    )
    snapshot = SimpleNamespace(
        snapshot_id=digest,
        content_digest=digest,
        projections=(projection,),
        file_count=1,
        total_bytes=7,
    )
    assert SkillProjectionEvidence.from_snapshot(snapshot).snapshot_id == digest
    launch = SimpleNamespace(
        thread_id=thread_id,
        assistant_id="lead-agent",
        multitask_strategy=strategy,
        user_id=user_id,
        accepted_invocation=SimpleNamespace(
            agent_revision=SimpleNamespace(material=SimpleNamespace(skill_snapshot=snapshot)),
        ),
        external_scope=None,
        external_key=None,
    )

    try:
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)
        # A freed thread is reserved outright; nothing was superseded.
        assert adapter._projection_reservations
        assert not adapter._projection_supersessions
        reservation = next(iter(adapter._projection_reservations.values()))
        assert reservation.snapshot_id == digest
    finally:
        for reservation in list(adapter._projection_reservations.values()):
            coordinator.abort_admission(reservation)
        reset_sandbox_provider()
