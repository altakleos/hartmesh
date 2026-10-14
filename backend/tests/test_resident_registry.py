"""Owning-loop registry bookkeeping with injected containment and no Docker."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.agent_instances.contract import ResidentBusy, ResidentRestarting, ResidentRunLimit, ResidentWorkLimit
from deerflow.agent_instances.residents import ResidentRegistry


class Provider:
    def __init__(self):
        self.lock = threading.Lock()
        self.calls = 0

    def sandbox_ready_timeout(self):
        return 30

    def external_changed(self, change):
        with self.lock:
            change()

    async def admit_external_async(self, key, reserve, *, wait, is_reserved):
        await asyncio.sleep(0)
        with self.lock:
            if not is_reserved():
                self.calls += 1
                reserve()


@pytest.fixture
def registry():
    # Construct inside the test's running loop, never at fixture setup.
    def build(*, replicas=3, idle_timeout=60):
        provider = Provider()
        records = {}
        manager = SimpleNamespace(task_runs=lambda key: {run_id: {**value, "value": value["metadata"][key]} for run_id, value in records.items() if key in value["metadata"]})
        attachments = SimpleNamespace(contain_host=AsyncMock(return_value=True), container_gone=AsyncMock(return_value=False), fence_reason=AsyncMock(return_value=None), uncontained={})
        clock = [100.0]
        result = ResidentRegistry(
            attachments=attachments, provider=provider, run_manager=manager, settle=AsyncMock(return_value=[]), owner_of=AsyncMock(return_value=None), replicas=replicas, idle_timeout=idle_timeout, clock=lambda: clock[0]
        )
        return result, provider, records, clock

    return build


def instance(name="first"):
    return SimpleNamespace(id=name, home_id="home-" + name, name=name)


def running(records, run_id, agent, *, attempt=None):
    metadata = {"agent_instance_id": agent.id, "requester_id": "requester"}
    if attempt is not None:
        metadata.update(agent_work_attempt_id=attempt, agent_work_id="work")
    records[run_id] = {"metadata": metadata, "user_id": "requester", "created_at": "created", "thread_id": run_id}


@pytest.mark.anyio
async def test_same_home_slot_admissions_count_once(registry):
    residents, provider, _records, _clock = registry()
    agent = instance()
    await asyncio.gather(*(residents.take_slot(agent, wait=False) for _ in range(2)))
    assert residents.slots == {agent.home_id} and provider.calls == 1


@pytest.mark.anyio
async def test_work_limit_reservations_and_binding_count_once(registry):
    residents, _provider, records, _clock = registry(replicas=2)
    agent = instance()
    reserved = await residents.admit(agent, work=True)
    with pytest.raises(ResidentWorkLimit):
        await residents.admit(instance("second"), work=True)
    residents.bind(reserved, "attempt")
    running(records, "run", agent, attempt="attempt")
    assert residents.attempts_in_flight() == [{"attempt_id": "attempt", "run_id": "run", "instance_id": agent.id, "work_id": "work", "requester_id": "requester", "started_at": "created"}]
    residents.claimed(agent, "attachment", attempt_id="attempt")
    assert not residents.reservations and residents.external_slots() == 1


@pytest.mark.anyio
async def test_two_conversation_limit_preserves_admission_order(registry):
    residents, _provider, records, _clock = registry()
    agent = instance()
    for run_id in ("first", "second", "third"):
        running(records, run_id, agent)
    await residents.admit(agent, work=False, run_id="second")
    with pytest.raises(ResidentRunLimit):
        await residents.admit(agent, work=False, run_id="third")
    with pytest.raises(ResidentRunLimit):
        await residents.admit(agent, work=False)


@pytest.mark.anyio
async def test_expiry_and_cancel_preserve_another_reservation_or_live_run(registry):
    residents, _provider, records, clock = registry(replicas=5)
    agent = instance()
    first = await residents.admit(agent, work=True)
    clock[0] += 1
    second = await residents.admit(agent, work=True)
    clock[0] = first.expires_at
    residents.attempts_in_flight()
    assert first.id not in residents.reservations and second.id in residents.reservations
    assert residents.slots == {agent.home_id}
    running(records, "run", agent)
    residents.cancel(second)
    assert residents.slots == {agent.home_id}
    records.clear()
    residents._drop_slot(agent.home_id)
    assert not residents.slots


@pytest.mark.anyio
async def test_unbound_claim_does_not_consume_other_admissions(registry):
    residents, _provider, _records, _clock = registry()
    agent = instance()
    reserved = await residents.admit(agent, work=True)
    residents.claimed(agent, "attachment")
    assert residents.reservations == {reserved.id: reserved}


@pytest.mark.anyio
async def test_uncertain_preparation_survives_expiry_and_unrelated_containment(registry):
    residents, _provider, _records, clock = registry()
    agent = instance()
    reserved = await residents.admit(agent, work=True)
    entry = residents.entry(agent)
    async with entry.lock:
        claim = residents.preparing(agent, incarnation="incarnation", generation=1, host_id="host")
        clock[0] = reserved.expires_at
        residents.attempts_in_flight()
        await residents.contained("unrelated", [agent.home_id], "idle")
        assert entry.preparation is claim and agent.home_id in residents.slots
        assert residents.preparation_absent(agent.home_id, claim)
    assert not residents.slots


@pytest.mark.anyio
async def test_late_preparation_completion_cannot_replace_new_claim(registry):
    residents, _provider, _records, _clock = registry()
    agent = instance()
    entry = residents.entry(agent)
    async with entry.lock:
        old = residents.preparing(agent, incarnation="old", generation=1, host_id="host")
        assert residents.preparation_absent(agent.home_id, old)
        new = residents.preparing(agent, incarnation="new", generation=1, host_id="host")
        assert not residents.preparation_claimed(agent, old, "old-attachment")
        assert not residents.preparation_absent(agent.home_id, old)
        assert entry.preparation is new and entry.attachment_id is None
        assert residents.preparation_claimed(agent, new, "new-attachment")
        assert entry.preparation is None and entry.attachment_id == "new-attachment"


@pytest.mark.anyio
async def test_contained_callback_under_home_lock_preserves_replacement(registry):
    residents, _provider, _records, _clock = registry()
    agent = instance()
    residents.claimed(agent, "new")
    entry = residents.entry(agent)
    async with entry.lock:
        await asyncio.wait_for(residents.contained("old", [agent.home_id], "idle"), 1)
        assert entry.attachment_id == "new" and residents.slots == {agent.home_id}
        await asyncio.wait_for(residents.contained("new", [agent.home_id], "idle"), 1)
    assert not residents.slots


@pytest.mark.anyio
async def test_recycle_rechecks_live_run_after_waiting_for_home_lock(registry):
    residents, _provider, records, _clock = registry()
    agent = instance()
    residents.claimed(agent, "attachment")
    entry = residents.entry(agent)
    await entry.lock.acquire()
    task = asyncio.create_task(residents.request_recycle(agent.id, agent.home_id, "attachment", "start-failed"))
    try:
        await asyncio.sleep(0)
        running(records, "new-run", agent)
    finally:
        entry.lock.release()
    assert not await task
    residents.attachments.contain_host.assert_not_awaited()
    with pytest.raises(ResidentRestarting):
        await residents.admit(agent, work=False)


@pytest.mark.anyio
async def test_worker_finished_is_not_live_while_task_finishes(registry):
    residents, _provider, records, _clock = registry()
    agent = instance()
    running(records, "run", agent)
    await residents.take_slot(agent, wait=False)
    await residents.run_ended(agent.id, agent.home_id, "run", None, False)
    assert not residents.in_flight(agent.id) and not residents.slots
    records.clear()
    await residents.run_ended(agent.id, agent.home_id, "another", None, False)
    assert not residents.ended


@pytest.mark.anyio
async def test_worker_eviction_selects_and_contains_only_on_owner_loop(registry):
    residents, _provider, _records, _clock = registry()
    agent = instance()
    residents.claimed(agent, "attachment")
    owner_thread = threading.get_ident()

    async def contain(attachment_id, reason):
        assert threading.get_ident() == owner_thread
        assert residents.entry(agent).lock.locked()
        await residents.contained(attachment_id, [agent.home_id], reason)
        return True

    residents.attachments.contain_host.side_effect = contain
    assert not residents.evict_external()
    assert await asyncio.to_thread(residents.evict_external)
    assert not residents.slots
    await residents.close()


@pytest.mark.anyio
async def test_eviction_timeout_retains_task_and_slot_until_drained(registry):
    residents, _provider, _records, _clock = registry()
    agent = instance()
    residents.claimed(agent, "attachment")
    residents.eviction_timeout = 0.02
    entered, release = asyncio.Event(), asyncio.Event()

    async def contain(attachment_id, reason):
        entered.set()
        await release.wait()
        await residents.contained(attachment_id, [agent.home_id], reason)
        return True

    residents.attachments.contain_host.side_effect = contain
    task = asyncio.create_task(asyncio.to_thread(residents.evict_external))
    await asyncio.wait_for(entered.wait(), 1)
    assert not await task and residents.slots == {agent.home_id}
    closing = asyncio.create_task(residents.close())
    await asyncio.sleep(0)
    assert not closing.done()
    release.set()
    await asyncio.wait_for(closing, 1)
    assert not residents.slots


@pytest.mark.anyio
async def test_expired_activation_during_admission_cannot_publish_a_reservation(registry):
    residents, provider, _records, clock = registry()
    original = provider.admit_external_async

    async def delay(*args, **kwargs):
        clock[0] += 31
        residents.attempts_in_flight()
        return await original(*args, **kwargs)

    provider.admit_external_async = delay
    with pytest.raises(ResidentBusy):
        await residents.admit(instance(), work=True)
    assert not residents.reservations and not residents.slots


@pytest.mark.anyio
async def test_close_during_slot_admission_prevents_late_publication(registry):
    residents, provider, _records, _clock = registry()
    original = provider.admit_external_async

    async def delay(*args, **kwargs):
        await residents.close()
        return await original(*args, **kwargs)

    provider.admit_external_async = delay
    with pytest.raises(ResidentRestarting):
        await residents.take_slot(instance(), wait=False)
    assert not residents.slots


@pytest.mark.anyio
async def test_run_end_wakes_chat_waiting_for_now_idle_resident(tmp_path, monkeypatch, registry):
    from _sandbox_provider_fakes import _make_provider

    residents, _provider, records, _clock = registry()
    provider, _backend = _make_provider(tmp_path, monkeypatch, replicas=1)
    provider._config["capacity_wait_timeout"] = 5
    residents.provider = provider
    provider.external_slots = residents.external_slots
    provider.evict_external = residents.evict_external
    agent = instance()
    residents.claimed(agent, "attachment")
    running(records, "run", agent)
    waiting = asyncio.Event()
    monkeypatch.setattr(provider, "_open_capacity_wait_span", lambda *_args: waiting.set() or True)

    async def contain(attachment_id, reason):
        await residents.contained(attachment_id, [agent.home_id], reason)
        return True

    residents.attachments.contain_host.side_effect = contain
    task = asyncio.create_task(provider._admit_create_async("chat"))
    try:
        await asyncio.wait_for(waiting.wait(), 1)
        await residents.run_ended(agent.id, agent.home_id, "run", "attachment", False)
        await asyncio.wait_for(task, 1)
        assert provider._starting == {"chat"} and not residents.slots
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await residents.close()
        provider.reset()


@pytest.mark.anyio
async def test_expired_reservation_cannot_bind(registry):
    residents, _provider, _records, clock = registry()
    reserved = await residents.admit(instance(), work=True)
    clock[0] = reserved.expires_at
    with pytest.raises(RuntimeError, match="expired"):
        residents.bind(reserved, "attempt")
    assert not residents.reservations and not residents.slots


@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.anyio
async def test_failed_or_cancelled_completion_still_drops_empty_slot(registry, cancelled):
    residents, _provider, records, _clock = registry()
    agent = instance()
    reserved = await residents.admit(agent, work=True)
    residents.bind(reserved, "attempt")
    running(records, "run", agent, attempt="attempt")
    entered, release, settled = asyncio.Event(), asyncio.Event(), []

    async def settle(**_kwargs):
        entered.set()
        await release.wait()
        settled.append(True)
        raise RuntimeError("injected settlement failure")

    residents.settle.side_effect = settle
    task = asyncio.create_task(residents.run_ended(agent.id, agent.home_id, "run", None, True, attempt_id="attempt"))
    await asyncio.wait_for(entered.wait(), 1)
    if cancelled:
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        task.cancel()
    release.set()
    await asyncio.wait_for(task, 1)
    assert settled == [True] and not residents.slots and not residents.reservations


@pytest.mark.anyio
async def test_uncertain_recycle_grace_refuses_new_runs_after_five_minutes(registry):
    residents, _provider, records, clock = registry()
    agent = instance()
    residents.claimed(agent, "attachment")
    running(records, "other", agent)
    assert not await residents.request_recycle(agent.id, agent.home_id, "attachment", "outcome-uncertain")
    clock[0] += 299
    await residents.admit(agent, work=False)
    clock[0] += 1
    with pytest.raises(ResidentRestarting):
        await residents.admit(agent, work=False)
    residents.attachments.contain_host.assert_not_awaited()


@pytest.mark.anyio
async def test_held_eviction_rechecks_new_run_and_replacement(registry):
    residents, _provider, records, _clock = registry()
    agent = instance()
    residents.claimed(agent, "old")
    entry = residents.entry(agent)
    await entry.lock.acquire()
    evicting = asyncio.create_task(asyncio.to_thread(residents.evict_external))
    try:
        for _ in range(100):
            if residents._evictions:
                break
            await asyncio.sleep(0.005)
        assert residents._evictions
        await residents.contained("old", [agent.home_id], "idle")
        residents.claimed(agent, "new")
        running(records, "run", agent)
    finally:
        entry.lock.release()
    assert not await asyncio.wait_for(evicting, 1)
    residents.attachments.contain_host.assert_not_awaited()
    assert entry.attachment_id == "new" and residents.slots == {agent.home_id}
    await residents.close()


@pytest.mark.anyio
async def test_reaper_contains_idle_only_and_preserves_unknown_preparation(registry):
    residents, _provider, _records, clock = registry()
    agent, preparing = instance(), instance("preparing")
    residents.claimed(agent, "attachment")
    await residents.take_slot(preparing, wait=False)
    async with residents.entry(preparing).lock:
        claim = residents.preparing(preparing, incarnation="unknown", generation=1, host_id="host")
    residents.attachments.uncontained["uncorrelated-intent"] = "start-failed"

    async def contain(attachment_id, reason):
        await residents.contained(attachment_id, [agent.home_id], reason)
        return True

    residents.attachments.contain_host.side_effect = contain
    clock[0] += 60
    await residents.reap()
    residents.attachments.contain_host.assert_awaited_once_with("attachment", "idle")
    assert residents.entry(preparing).preparation is claim and residents.slots == {preparing.home_id}
    assert residents.attachments.uncontained == {"uncorrelated-intent": "start-failed"}


@pytest.mark.anyio
async def test_falsey_attempt_ids_remain_live_and_cancel_requires_exact_reservation(registry):
    from dataclasses import replace

    residents, _provider, records, _clock = registry()
    agent = instance()
    reserved = await residents.admit(agent, work=True)
    residents.bind(reserved, "")
    running(records, "run", agent, attempt="")
    residents.cancel(replace(reserved))
    assert residents.reservations == {reserved.id: reserved}
    assert len(residents.attempts_in_flight()) == 1
    residents.claimed(agent, "attachment", attempt_id="")
    assert not residents.reservations


@pytest.mark.anyio
async def test_attempt_projection_uses_explicit_requester_metadata(registry):
    residents, _provider, records, _clock = registry()
    running(records, "run", instance(), attempt="attempt")
    records["run"]["user_id"] = "durable-conversation-owner"
    records["run"]["metadata"]["requester_id"] = "authorized-collaborator"
    assert residents.attempts_in_flight()[0]["requester_id"] == "authorized-collaborator"
