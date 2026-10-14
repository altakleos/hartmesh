"""Exact durable capacity claims remain distinct from usable Home identities."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_agent_execution_runtime import execution as make_execution
from test_resident_registry import registry as registry

from deerflow.agent_instances.contract import ResidentRestarting, ResidentStartingElsewhere
from deerflow.spaces.attachments import ResourceMount
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable


def claim(identifier="d", *, actor=None, resources=None, phase="active", container_id="e" * 64):
    from deerflow.spaces.attachments import RetainedAttachment

    instance = make_execution().instance
    return RetainedAttachment(identifier * 32, identifier * 32, actor or instance.principal, "host", tuple(resources or [ResourceMount(instance.home_id, 1, "home", True)]), phase, container_id)


def begin(residents, *claims):
    token = residents.begin_restore()
    residents.finish_restore(token, claims)


@pytest.mark.anyio
async def test_distinct_attachments_on_one_home_each_count_and_block_home_admission(registry):
    residents, _provider, _records, _clock = registry()
    instance = make_execution().instance
    begin(residents, claim("d"), claim("f"))
    assert residents.restoration_complete and residents.external_slots() == 2
    assert residents.entry(instance).attachment_id is None
    with pytest.raises(ResidentStartingElsewhere):
        await residents.admit(instance, work=False)


@pytest.mark.anyio
async def test_unknown_owner_and_multi_space_claim_each_consume_one_environment(registry):
    residents, _provider, _records, _clock = registry()
    home = make_execution().instance.home_id
    multi = claim(resources=[ResourceMount(home, 1, "home", True), ResourceMount("c" * 32, 2, "shared")])
    foreign = claim("f", actor=PrincipalRef("human", "unknown-owner"))
    begin(residents, multi, foreign)
    assert residents.external_slots() == 2 and not residents.homes


@pytest.mark.anyio
async def test_incomplete_restore_refuses_before_existing_home_slot_shortcut(registry):
    residents, provider, _records, _clock = registry()
    instance = make_execution().instance
    await residents.take_slot(instance, wait=False)
    token = residents.begin_restore()
    assert not residents.restoration_complete
    with pytest.raises(ResidentRestarting):
        await residents.take_slot(instance, wait=False)
    with pytest.raises(StorageRuntimeUnavailable):
        residents.external_slots()
    residents.abort_restore(token)
    assert not residents.restoration_complete and residents.slots == {instance.home_id}
    assert provider.calls == 1
    with pytest.raises(ResidentRestarting):
        await residents.admit(instance, work=False)


@pytest.mark.anyio
async def test_failed_or_malformed_snapshot_retains_previous_claims_and_readiness_stays_false(registry):
    residents, _provider, _records, _clock = registry()
    existing = claim()
    begin(residents, existing)
    token = residents.begin_restore()
    with pytest.raises(ValueError):
        residents.finish_restore(token, [existing, object()])
    assert residents.retained == {existing.id: existing} and not residents.restoration_complete
    residents.abort_restore(token)
    with pytest.raises(StorageRuntimeUnavailable):
        residents.external_slots()


@pytest.mark.anyio
async def test_unique_exact_association_preserves_capacity_charge_and_requires_home_lock(registry):
    residents, _provider, _records, _clock = registry()
    instance = make_execution().instance
    residents.attachments.provider = SimpleNamespace(host_id="host")
    existing = claim()
    begin(residents, existing)
    with pytest.raises(RuntimeError, match="lock"):
        residents.associate(instance, existing, generation=1)
    async with residents.entry(instance).lock:
        residents.associate(instance, existing, generation=1)
    assert residents.external_slots() == 1 and residents.entry(instance).attachment_id == existing.id
    assert residents.retained[existing.id] is existing


@pytest.mark.anyio
@pytest.mark.parametrize("changed", ["actor", "host", "generation", "extra", "pending"])
async def test_wrong_scope_or_unusable_claim_cannot_become_home_identity(registry, changed):
    residents, _provider, _records, _clock = registry()
    instance = make_execution().instance
    residents.attachments.provider = SimpleNamespace(host_id="host")
    existing = claim()
    if changed == "actor":
        existing = replace(existing, actor=PrincipalRef("human", "foreign"))
    elif changed == "host":
        existing = replace(existing, host_id="foreign-host")
    elif changed == "generation":
        existing = replace(existing, resources=(ResourceMount(instance.home_id, 2, "home", True),))
    elif changed == "extra":
        existing = replace(existing, resources=existing.resources + (ResourceMount("c" * 32, 1, "extra"),))
    else:
        existing = replace(existing, phase="pending")
    begin(residents, existing)
    async with residents.entry(instance).lock:
        with pytest.raises(ValueError):
            residents.associate(instance, existing, generation=1)
    assert residents.external_slots() == 1 and residents.entry(instance).attachment_id is None


@pytest.mark.anyio
async def test_stale_snapshot_cannot_resurrect_exact_contained_claim(registry):
    residents, _provider, _records, _clock = registry()
    old = claim()
    begin(residents, old)
    token = residents.begin_restore()
    await residents.contained(old.id, [make_execution().instance.home_id], "idle")
    residents.finish_restore(token, [old])
    assert residents.restoration_complete and not residents.retained and residents.external_slots() == 0


@pytest.mark.anyio
async def test_phase_update_and_new_creation_win_over_delayed_snapshot_without_identity_changes(registry):
    residents, _provider, _records, _clock = registry()
    pending = claim(phase="pending", container_id=None)
    begin(residents, pending)
    token = residents.begin_restore()
    active = replace(pending, phase="active", container_id="e" * 64)
    created = claim("f")
    residents.retain(active)
    residents.retain(created)
    residents.finish_restore(token, [pending])
    assert residents.retained == {active.id: active, created.id: created}
    assert residents.external_slots() == 2


@pytest.mark.anyio
async def test_immutable_identity_collision_fails_closed_and_preserves_previous_claim(registry):
    residents, _provider, _records, _clock = registry()
    old = claim()
    begin(residents, old)
    token = residents.begin_restore()
    with pytest.raises(ValueError):
        residents.finish_restore(token, [replace(old, incarnation="f" * 32)])
    assert residents.retained[old.id] is old and not residents.restoration_complete


@pytest.mark.anyio
async def test_containment_clears_only_matching_claim_even_when_later_settlement_fails(registry):
    residents, _provider, _records, _clock = registry()
    first, second = claim(), claim("f")
    begin(residents, first, second)
    instance = make_execution().instance
    residents.owner_of.return_value = instance
    residents.settle.side_effect = RuntimeError("settlement unavailable")
    with pytest.raises(RuntimeError):
        await residents.contained(first.id, [instance.home_id], "idle")
    assert residents.retained == {second.id: second} and residents.external_slots() == 1


@pytest.mark.anyio
async def test_reservation_expiry_and_run_ended_cannot_drop_unassociated_durable_claim(registry):
    residents, _provider, _records, clock = registry()
    existing = claim()
    begin(residents, existing)
    instance = make_execution().instance
    clock[0] += 1000
    residents.attempts_in_flight()
    await residents.run_ended(instance.id, instance.home_id, "run", None, False)
    assert residents.retained == {existing.id: existing} and residents.external_slots() == 1


@pytest.mark.anyio
async def test_missing_claim_in_complete_snapshot_is_not_physical_containment_proof(registry):
    residents, _provider, _records, _clock = registry()
    existing = claim()
    begin(residents, existing)
    begin(residents)
    assert residents.retained == {existing.id: existing} and residents.external_slots() == 1


@pytest.mark.anyio
async def test_untouched_registry_preserves_behavior_but_has_no_restore_proof(registry):
    residents, _provider, _records, _clock = registry()
    assert not residents.restoration_complete
    await residents.take_slot(make_execution().instance, wait=False)
    assert residents.external_slots() == 1


@pytest.mark.anyio
async def test_live_identity_collision_invalidates_readiness_without_replacing_known_claim(registry):
    residents, _provider, _records, _clock = registry()
    existing = claim()
    begin(residents, existing)
    with pytest.raises(ValueError):
        residents.retain(replace(existing, incarnation="f" * 32))
    assert residents.retained[existing.id] is existing and not residents.restoration_complete
    with pytest.raises(StorageRuntimeUnavailable):
        residents.external_slots()


@pytest.mark.anyio
async def test_live_snapshot_bound_refuses_extra_identity_before_insertion_and_closes_admission(registry, monkeypatch):
    from deerflow.agent_instances import residents as module

    residents, _provider, _records, _clock = registry()
    existing = claim()
    begin(residents, existing)
    monkeypatch.setattr(module, "MAX_RETAINED_ATTACHMENTS", 1)
    for identifier in ("f", "c"):
        with pytest.raises(ValueError):
            residents.retain(claim(identifier))
    assert residents.retained == {existing.id: existing} and not residents.restoration_complete


@pytest.mark.anyio
async def test_exact_preparation_transfer_publishes_one_durable_charge_atomically(registry, monkeypatch):
    residents, provider, _records, _clock = registry()
    instance = make_execution().instance
    begin(residents)
    await residents.take_slot(instance, wait=False)
    existing = claim(phase="pending", container_id=None)
    observed = []
    original = provider.external_changed

    def observe(change):
        observed.append(residents.external_slots())
        original(change)
        observed.append(residents.external_slots())

    monkeypatch.setattr(provider, "external_changed", observe)
    async with residents.entry(instance).lock:
        preparation = residents.preparing(instance, incarnation=existing.incarnation, generation=1, host_id="host")
        assert residents.preparation_retained(instance, preparation, existing)
    assert observed == [1, 1]
    assert residents.retained == {existing.id: existing}
    assert residents.entry(instance).preparation is None and residents.entry(instance).attachment_id == existing.id
    with pytest.raises(ResidentStartingElsewhere):
        await residents.admit(instance, work=False)


@pytest.mark.anyio
async def test_wrong_incarnation_cannot_transfer_or_clear_exact_preparation(registry):
    residents, _provider, _records, _clock = registry()
    instance = make_execution().instance
    begin(residents)
    await residents.take_slot(instance, wait=False)
    async with residents.entry(instance).lock:
        preparation = residents.preparing(instance, incarnation="c" * 32, generation=1, host_id="host")
        with pytest.raises(ValueError):
            residents.preparation_retained(instance, preparation, claim())
        assert residents.entry(instance).preparation is preparation and not residents.retained
    assert residents.external_slots() == 1


@pytest.mark.anyio
async def test_legacy_creation_during_restore_cannot_publish_complete_stale_snapshot(registry):
    residents, _provider, _records, _clock = registry()
    instance = make_execution().instance
    token = residents.begin_restore()
    residents.claimed(instance, "d" * 32)
    with pytest.raises(RuntimeError):
        residents.finish_restore(token, [])
    assert not residents.restoration_complete and residents.slots == {instance.home_id}


@pytest.mark.anyio
async def test_snapshot_cannot_reuse_incarnation_of_retained_claim_omitted_from_sql(registry):
    residents, _provider, _records, _clock = registry()
    old = claim()
    begin(residents, old)
    token = residents.begin_restore()
    with pytest.raises(ValueError):
        residents.finish_restore(token, [replace(claim("f"), incarnation=old.incarnation)])
    assert residents.retained == {old.id: old} and not residents.restoration_complete


@pytest.mark.anyio
async def test_tombstone_update_bound_overflow_cannot_publish_stale_complete_snapshot(registry, monkeypatch):
    from deerflow.agent_instances import residents as module

    residents, _provider, _records, _clock = registry()
    first, second = claim(), claim("f")
    begin(residents, first, second)
    monkeypatch.setattr(module, "MAX_RETAINED_ATTACHMENTS", 1)
    token = residents.begin_restore()
    await residents.contained(first.id, [], "idle")
    await residents.contained(second.id, [], "idle")
    with pytest.raises(RuntimeError):
        residents.finish_restore(token, [])
    assert not residents.restoration_complete and not residents.retained


@pytest.mark.anyio
async def test_old_restore_token_cannot_abort_or_publish_new_owned_restore(registry):
    residents, _provider, _records, _clock = registry()
    old = residents.begin_restore()
    residents.abort_restore(old)
    current = residents.begin_restore()
    residents.abort_restore(old)
    with pytest.raises(RuntimeError):
        residents.finish_restore(old, [])
    residents.finish_restore(current, [])
    assert residents.restoration_complete
