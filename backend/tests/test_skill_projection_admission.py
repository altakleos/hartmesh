from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.gateway import services
from deerflow.runtime import ConflictError, RunManager, RunStatus
from deerflow.runtime.runs.store.base import AdmissionOutcome, LifecycleType
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.skill_projection import (
    SkillProjectionBusyError,
    get_skill_projection_coordinator,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1

_TEST_TENANT = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()


def _launch(*, thread_id: str, external_key: str):
    material = SimpleNamespace(skill_snapshot=None)
    accepted = SimpleNamespace(
        agent_revision=SimpleNamespace(material=material),
        tenant=_TEST_TENANT,
        to_persisted=lambda: {},
    )
    return SimpleNamespace(
        thread_id=thread_id,
        assistant_id="lead-agent",
        on_disconnect="cancel",
        metadata={},
        kwargs={},
        multitask_strategy="reject",
        model_name=None,
        user_id="projection-owner",
        accepted_invocation=accepted,
        external_scope="scope-1",
        external_key=external_key,
        request_digest="a" * 64,
        request_digest_version="v1",
        caller_intent_json={"version": 1},
        caller_intent_digest="b" * 64,
        caller_intent_digest_version="v1",
    )


@pytest.mark.asyncio
async def test_projection_busy_rejects_before_checkpoint_seed_or_sql_admission(
    monkeypatch,
) -> None:
    thread_id = "projection-busy-before-seed"
    coordinator = get_skill_projection_coordinator()
    reservation = coordinator.reserve_admission(
        user_id="projection-owner",
        thread_id=thread_id,
        reservation_id="older-reservation",
        snapshot_id=None,
    )
    coordinator.promote_admission(reservation, run_id="older-run")

    manager = SimpleNamespace(
        get_by_external_identity=AsyncMock(return_value=None),
        ensure_or_reject=AsyncMock(),
    )
    seed = AsyncMock()
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(services, "ensure_checkpoint_history_seeded", seed)
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(thread_id=thread_id, external_key="new-key")

    try:
        async with adapter.admission_scope(thread_id):
            with pytest.raises(ConflictError, match="skill projection"):
                await adapter.prepare_admission(launch)
        seed.assert_not_awaited()
        manager.ensure_or_reject.assert_not_awaited()
    finally:
        assert coordinator.release_unactivated_run(
            user_id="projection-owner",
            thread_id=thread_id,
            run_id="older-run",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("strategy", "old_status"),
    [
        ("interrupt", RunStatus.interrupted),
        ("rollback", RunStatus.error),
    ],
)
async def test_atomic_replacement_commits_while_old_projection_remains_exclusive(
    monkeypatch,
    strategy,
    old_status,
) -> None:
    thread_id = f"projection-{strategy}-replacement"
    store = MemoryRunStore()
    manager = RunManager(store=store, tenant=_TEST_TENANT)
    old = await manager.create_or_reject(
        thread_id,
        user_id="projection-owner",
    )
    await manager.set_status(old.run_id, RunStatus.running)
    coordinator = get_skill_projection_coordinator()
    reservation = coordinator.reserve_admission(
        user_id="projection-owner",
        thread_id=thread_id,
        reservation_id=f"old:{strategy}",
        snapshot_id=None,
    )
    coordinator.promote_admission(reservation, run_id=old.run_id)
    old_token = coordinator.activate(
        user_id="projection-owner",
        thread_id=thread_id,
        sandbox_id=f"sandbox:{strategy}",
        run_id=old.run_id,
        snapshot_id=None,
        consumer_id=f"run:{old.run_id}:lead",
    )

    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(
        services,
        "ensure_checkpoint_history_seeded",
        AsyncMock(),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(
        thread_id=thread_id,
        external_key=f"replacement:{strategy}",
    )
    launch.multitask_strategy = strategy

    try:
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)
            admitted = await adapter.admit(
                launch,
                candidate_run_id=str(uuid.uuid4()),
            )

        assert admitted.outcome is AdmissionOutcome.created
        old_row = await store.get(old.run_id)
        new_row = await store.get(admitted.record.run_id)
        assert old_row is not None and old_row["status"] == old_status
        assert new_row is not None and new_row["status"] == RunStatus.pending
        events = await store.list_lifecycle_events(thread_id=thread_id)
        assert [(event["run_id"], event["lifecycle_type"]) for event in events] == [
            (old.run_id, LifecycleType.accepted),
            (old.run_id, LifecycleType.started),
            (old.run_id, LifecycleType.interrupted),
            (admitted.record.run_id, LifecycleType.accepted),
        ]
        assert (
            coordinator.current_token(
                user_id="projection-owner",
                thread_id=thread_id,
            )
            == old_token
        )
        with pytest.raises(SkillProjectionBusyError):
            coordinator.claim_committed_run(
                user_id="projection-owner",
                thread_id=thread_id,
                run_id=admitted.record.run_id,
                snapshot_id=None,
            )

        clear = coordinator.release(old_token)
        assert clear is not None
        assert coordinator.finalize_release(clear)
        coordinator.claim_committed_run(
            user_id="projection-owner",
            thread_id=thread_id,
            run_id=admitted.record.run_id,
            snapshot_id=None,
        )
    finally:
        coordinator.release(old_token)
        coordinator.release_unactivated_run(
            user_id="projection-owner",
            thread_id=thread_id,
            run_id=admitted.record.run_id if "admitted" in locals() else "not-created",
        )


@pytest.mark.asyncio
async def test_atomic_replacement_promotes_after_unactivated_owner_is_superseded(
    monkeypatch,
) -> None:
    thread_id = "projection-unactivated-replacement"
    coordinator = get_skill_projection_coordinator()
    reservation = coordinator.reserve_admission(
        user_id="projection-owner",
        thread_id=thread_id,
        reservation_id="old:unactivated",
        snapshot_id=None,
    )
    coordinator.promote_admission(reservation, run_id="old-unactivated")
    replacement = SimpleNamespace(run_id="new-replacement")
    manager = SimpleNamespace(
        get_by_external_identity=AsyncMock(return_value=None),
        ensure_or_reject=AsyncMock(
            return_value=SimpleNamespace(
                record=replacement,
                outcome=AdmissionOutcome.created,
            )
        ),
    )
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(
        services,
        "ensure_checkpoint_history_seeded",
        AsyncMock(),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(
        thread_id=thread_id,
        external_key="unactivated-replacement",
    )
    launch.multitask_strategy = "interrupt"

    try:
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)
            admitted = await adapter.admit(
                launch,
                candidate_run_id=str(uuid.uuid4()),
            )

        assert admitted.outcome is AdmissionOutcome.created
        assert coordinator.release_unactivated_run(
            user_id="projection-owner",
            thread_id=thread_id,
            run_id=replacement.run_id,
        )
    finally:
        coordinator.release_unactivated_run(
            user_id="projection-owner",
            thread_id=thread_id,
            run_id="old-unactivated",
        )
        coordinator.release_unactivated_run(
            user_id="projection-owner",
            thread_id=thread_id,
            run_id=replacement.run_id,
        )


@pytest.mark.asyncio
async def test_known_key_under_lock_bypasses_projection_busy_and_seed(
    monkeypatch,
) -> None:
    thread_id = "projection-known-bypass"
    coordinator = get_skill_projection_coordinator()
    reservation = coordinator.reserve_admission(
        user_id="projection-owner",
        thread_id=thread_id,
        reservation_id="creator-reservation",
        snapshot_id=None,
    )
    coordinator.promote_admission(reservation, run_id="known-run")
    record = SimpleNamespace(run_id="known-run", thread_id=thread_id)
    manager = SimpleNamespace(
        get_by_external_identity=AsyncMock(return_value=record),
        ensure_or_reject=AsyncMock(
            return_value=SimpleNamespace(
                record=record,
                outcome=AdmissionOutcome.known_same,
            )
        ),
    )
    seed = AsyncMock()
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(services, "ensure_checkpoint_history_seeded", seed)
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(thread_id=thread_id, external_key="same-key")

    try:
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)
            admitted = await adapter.admit(
                launch,
                candidate_run_id=str(uuid.uuid4()),
            )
        assert admitted.outcome is AdmissionOutcome.known_same
        seed.assert_not_awaited()
        manager.ensure_or_reject.assert_awaited_once()
    finally:
        assert coordinator.release_unactivated_run(
            user_id="projection-owner",
            thread_id=thread_id,
            run_id="known-run",
        )


@pytest.mark.asyncio
async def test_concurrent_equal_key_loser_rechecks_under_lock_and_converges(
    monkeypatch,
) -> None:
    thread_id = "projection-equal-key-race"
    record = SimpleNamespace(
        run_id="race-winner",
        thread_id=thread_id,
    )

    class _Manager:
        def __init__(self) -> None:
            self.record = None
            self.ensure_calls = 0

        async def get_by_external_identity(self, *_args, **_kwargs):
            return self.record

        async def ensure_or_reject(self, *_args, **_kwargs):
            self.ensure_calls += 1
            if self.record is None:
                self.record = record
                return SimpleNamespace(
                    record=record,
                    outcome=AdmissionOutcome.created,
                )
            return SimpleNamespace(
                record=self.record,
                outcome=AdmissionOutcome.known_same,
            )

    manager = _Manager()
    seed = AsyncMock()
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(services, "ensure_checkpoint_history_seeded", seed)
    launch_one = _launch(thread_id=thread_id, external_key="same-racing-key")
    launch_two = _launch(thread_id=thread_id, external_key="same-racing-key")
    adapters = [
        services._GatewayDurableRuns(SimpleNamespace()),
        services._GatewayDurableRuns(SimpleNamespace()),
    ]

    async def admit(index: int):
        adapter = adapters[index]
        launch = (launch_one, launch_two)[index]
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)
            return await adapter.admit(
                launch,
                candidate_run_id=str(uuid.uuid4()),
            )

    outcomes = await asyncio.gather(admit(0), admit(1))
    assert [item.outcome for item in outcomes] == [
        AdmissionOutcome.created,
        AdmissionOutcome.known_same,
    ]
    assert {item.record.run_id for item in outcomes} == {"race-winner"}
    assert seed.await_count == 1
    assert manager.ensure_calls == 2
    assert get_skill_projection_coordinator().release_unactivated_run(
        user_id="projection-owner",
        thread_id=thread_id,
        run_id="race-winner",
    )


@pytest.mark.asyncio
async def test_checkpoint_cancellation_aborts_unpromoted_projection(
    monkeypatch,
) -> None:
    thread_id = "projection-checkpoint-cancel"
    manager = SimpleNamespace(
        get_by_external_identity=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(
        services,
        "ensure_checkpoint_history_seeded",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(thread_id=thread_id, external_key="cancel-before-sql")

    with pytest.raises(asyncio.CancelledError):
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)

    assert not get_skill_projection_coordinator().is_busy(
        user_id="projection-owner",
        thread_id=thread_id,
    )


@pytest.mark.asyncio
async def test_unkeyed_admission_cancellation_aborts_unpromoted_projection(
    monkeypatch,
) -> None:
    thread_id = "projection-unkeyed-cancel"
    manager = SimpleNamespace(
        create_or_reject=AsyncMock(side_effect=asyncio.CancelledError()),
    )
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(
        services,
        "ensure_checkpoint_history_seeded",
        AsyncMock(),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(thread_id=thread_id, external_key="unused")
    launch.external_scope = None
    launch.external_key = None

    with pytest.raises(asyncio.CancelledError):
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)
            await adapter.admit(
                launch,
                candidate_run_id=str(uuid.uuid4()),
            )

    assert not get_skill_projection_coordinator().is_busy(
        user_id="projection-owner",
        thread_id=thread_id,
    )


@pytest.mark.asyncio
async def test_gateway_does_not_detach_or_reclassify_cancelled_admission(
    monkeypatch,
) -> None:
    thread_id = "projection-created-during-cancel"
    started = asyncio.Event()
    release = asyncio.Event()
    record = SimpleNamespace(run_id="run-created-during-cancel")

    class _Manager:
        async def get_by_external_identity(self, *_args, **_kwargs):
            return None

        async def ensure_or_reject(self, *_args, **_kwargs):
            started.set()
            await release.wait()
            return SimpleNamespace(
                record=record,
                outcome=AdmissionOutcome.created,
            )

    monkeypatch.setattr(services, "get_run_manager", lambda _request: _Manager())
    monkeypatch.setattr(
        services,
        "ensure_checkpoint_history_seeded",
        AsyncMock(),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(thread_id=thread_id, external_key="created-during-cancel")

    async with adapter.admission_scope(thread_id):
        await adapter.prepare_admission(launch)
        admission_task = asyncio.create_task(
            adapter.admit(
                launch,
                candidate_run_id=str(uuid.uuid4()),
            )
        )
        await started.wait()
        admission_task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await admission_task

    assert not get_skill_projection_coordinator().release_unactivated_run(
        user_id="projection-owner",
        thread_id=thread_id,
        run_id=record.run_id,
    )


@pytest.mark.asyncio
async def test_keyed_evidence_validation_aborts_prepared_projection(
    monkeypatch,
) -> None:
    thread_id = "projection-invalid-evidence"
    manager = SimpleNamespace(
        get_by_external_identity=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(
        services,
        "ensure_checkpoint_history_seeded",
        AsyncMock(),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(thread_id=thread_id, external_key="invalid-evidence")
    launch.caller_intent_digest = None

    async with adapter.admission_scope(thread_id):
        await adapter.prepare_admission(launch)
        with pytest.raises(RuntimeError, match="canonical admission evidence"):
            await adapter.admit(
                launch,
                candidate_run_id=str(uuid.uuid4()),
            )

    assert not get_skill_projection_coordinator().is_busy(
        user_id="projection-owner",
        thread_id=thread_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [OSError, asyncio.CancelledError])
async def test_gateway_does_not_guess_creator_ownership_after_admission_failure(
    monkeypatch,
    failure,
) -> None:
    thread_id = f"projection-ambiguous-{failure.__name__}"
    record = SimpleNamespace(
        run_id=f"committed-{failure.__name__}",
        thread_id=thread_id,
        user_id="projection-owner",
        caller_intent_json={"version": 1},
        caller_intent_digest="b" * 64,
        caller_intent_digest_version="v1",
    )

    class _Manager:
        def __init__(self) -> None:
            self.lookup_count = 0

        async def get_by_external_identity(self, *_args, **_kwargs):
            self.lookup_count += 1
            return None if self.lookup_count == 1 else record

        async def ensure_or_reject(self, *_args, **_kwargs):
            raise failure("response lost after commit")

    manager = _Manager()
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(
        services,
        "ensure_checkpoint_history_seeded",
        AsyncMock(),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(thread_id=thread_id, external_key="ambiguous-key")

    async with adapter.admission_scope(thread_id):
        await adapter.prepare_admission(launch)
        with pytest.raises(failure):
            await adapter.admit(
                launch,
                candidate_run_id=str(uuid.uuid4()),
            )

    assert manager.lookup_count == 1
    assert not get_skill_projection_coordinator().is_busy(
        user_id="projection-owner",
        thread_id=thread_id,
    )


@pytest.mark.asyncio
async def test_gateway_real_manager_recovers_response_lost_creator_by_candidate_id(
    monkeypatch,
) -> None:
    thread_id = "projection-response-lost-creator"
    candidate_run_id = "773be6ee-2915-45e1-af81-84a6694fd712"

    class _ResponseLostStore(MemoryRunStore):
        def __init__(self) -> None:
            super().__init__()
            self.lose_response = True

        async def ensure_run_atomic(self, run_id, **kwargs):
            result = await super().ensure_run_atomic(run_id, **kwargs)
            if self.lose_response:
                self.lose_response = False
                raise OSError("lost after commit")
            return result

    manager = RunManager(
        store=_ResponseLostStore(),
        tenant=_TEST_TENANT,
    )
    monkeypatch.setattr(services, "get_run_manager", lambda _request: manager)
    monkeypatch.setattr(
        services,
        "ensure_checkpoint_history_seeded",
        AsyncMock(),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(thread_id=thread_id, external_key="response-lost-key")

    try:
        async with adapter.admission_scope(thread_id):
            await adapter.prepare_admission(launch)
            admitted = await adapter.admit(
                launch,
                candidate_run_id=candidate_run_id,
            )

        assert admitted.outcome is AdmissionOutcome.created
        assert admitted.record.run_id == candidate_run_id
        assert admitted.record.attachment_supervised is True
    finally:
        await adapter.fail_start(
            await manager.get(candidate_run_id),
            "worker_attachment_failed",
        )
        assert not get_skill_projection_coordinator().is_busy(
            user_id="projection-owner",
            thread_id=thread_id,
        )


@pytest.mark.asyncio
async def test_fail_start_transfers_compensation_ownership_before_releasing_material(
    monkeypatch,
) -> None:
    order: list[str] = []

    class _Manager:
        async def fail_start_if_pending(self, run_id, *, error):
            assert run_id == "candidate-run"
            assert error == "worker construction failed"
            order.append("compensation-owned")
            return True

    coordinator = get_skill_projection_coordinator()

    def release_unactivated_run(_self, *, user_id, thread_id, run_id):
        assert (user_id, thread_id, run_id) == (
            "projection-owner",
            "projection-release-order",
            "candidate-run",
        )
        order.append("material-released")
        return True

    monkeypatch.setattr(services, "get_run_manager", lambda _request: _Manager())
    monkeypatch.setattr(
        type(coordinator),
        "release_unactivated_run",
        release_unactivated_run,
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    record = SimpleNamespace(
        run_id="candidate-run",
        user_id="projection-owner",
        thread_id="projection-release-order",
    )

    await adapter.fail_start(record, "worker construction failed")

    assert order == ["compensation-owned", "material-released"]


@pytest.mark.asyncio
async def test_cancel_start_transfers_compensation_ownership_before_releasing_material(
    monkeypatch,
) -> None:
    order: list[str] = []

    class _Manager:
        async def cancel_start_if_pending(self, run_id):
            assert run_id == "candidate-run"
            order.append("compensation-owned")
            return True

    coordinator = get_skill_projection_coordinator()

    def release_unactivated_run(_self, *, user_id, thread_id, run_id):
        assert (user_id, thread_id, run_id) == (
            "projection-owner",
            "projection-cancel-release-order",
            "candidate-run",
        )
        order.append("material-released")
        return True

    monkeypatch.setattr(services, "get_run_manager", lambda _request: _Manager())
    monkeypatch.setattr(
        type(coordinator),
        "release_unactivated_run",
        release_unactivated_run,
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    record = SimpleNamespace(
        run_id="candidate-run",
        user_id="projection-owner",
        thread_id="projection-cancel-release-order",
    )

    await adapter.cancel_start(record)

    assert order == ["compensation-owned", "material-released"]


@pytest.mark.asyncio
async def test_failed_admission_transfers_compensation_ownership_before_releasing_material(
    monkeypatch,
) -> None:
    order: list[str] = []

    class _Manager:
        async def ensure_or_reject(self, *_args, **_kwargs):
            raise OSError("admission response unavailable")

    coordinator = get_skill_projection_coordinator()

    def release_unactivated_run(_self, *, user_id, thread_id, run_id):
        assert (user_id, thread_id, run_id) == (
            "projection-owner",
            "projection-admit-release-order",
            "candidate-run",
        )
        order.append("material-released")
        return True

    async def terminalize(_run_manager, candidate_run_id):
        assert candidate_run_id == "candidate-run"
        order.append("compensation-owned")

    monkeypatch.setattr(services, "get_run_manager", lambda _request: _Manager())
    monkeypatch.setattr(
        type(coordinator),
        "release_unactivated_run",
        release_unactivated_run,
    )
    monkeypatch.setattr(
        services._GatewayDurableRuns,
        "_terminalize_unattached_candidate",
        staticmethod(terminalize),
    )
    adapter = services._GatewayDurableRuns(SimpleNamespace())
    launch = _launch(
        thread_id="projection-admit-release-order",
        external_key="failed-admission",
    )

    with pytest.raises(OSError, match="admission response unavailable"):
        await adapter.admit(
            launch,
            candidate_run_id="candidate-run",
        )

    assert order == ["compensation-owned", "material-released"]
