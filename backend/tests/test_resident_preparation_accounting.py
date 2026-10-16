"""Restored registries receive exact immutable facts before usable publication."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest
from test_resident_preparation import prepare, setup

from deerflow.agent_instances.contract import ResidentRestarting, ResidentStartFailed
from deerflow.spaces.attachments import AttachmentState, ResourceMount, RetainedAttachment


def accounting(parts):
    execution, _environment, residents, mounts, attached, *_ = parts
    token = residents.begin_restore()
    residents.finish_restore(token, ())
    facts = {}

    async def read(identifier):
        assert identifier == attached.id
        if identifier not in facts:
            preparation = residents.entry(execution.instance).preparation
            facts[identifier] = RetainedAttachment(
                identifier,
                preparation.incarnation,
                execution.instance.principal,
                "host",
                (ResourceMount(execution.instance.home_id, execution.home_generation, "home", True),),
                "active",
                attached.container_id,
            )
        return facts[identifier]

    mounts.retained_attachment = AsyncMock(side_effect=read)
    residents.provider.external_slots = residents.external_slots
    return facts


@pytest.mark.anyio
async def test_successful_attach_transfers_exact_preparation_without_a_count_gap(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    _execution, environment, residents, mounts, attached, *_ = parts
    facts = accounting(parts)
    await prepare(parts)
    assert residents.retained[attached.id] is facts[attached.id]
    assert residents.external_slots() == 1 and environment.attachment_id == attached.id
    mounts.retained_attachment.assert_awaited_once_with(attached.id)


@pytest.mark.anyio
async def test_attach_already_submitted_during_restore_publishes_typed_update(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    _execution, _environment, residents, mounts, attached, *_ = parts
    facts = accounting(parts)
    entered, release = asyncio.Event(), asyncio.Event()

    async def attach(**_kwargs):
        entered.set()
        await release.wait()
        return attached

    mounts.attach.side_effect = attach
    worker = asyncio.create_task(prepare(parts))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        token = residents.begin_restore()
        release.set()
        await worker
        residents.finish_restore(token, ())
        assert residents.retained[attached.id] == facts[attached.id]
        assert residents.external_slots() == 1
    finally:
        release.set()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.anyio
async def test_unreadable_post_attach_facts_retain_returned_identity_and_opaque_claim(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, attached, *_ = parts
    accounting(parts)
    mounts.retained_attachment.side_effect = RuntimeError("SQL read unavailable")
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    assert residents.entry(execution.instance).preparation is not None
    assert residents.external_slots() == 1
    assert environment.attachment_id == attached.id and environment.provider is None


@pytest.mark.anyio
@pytest.mark.parametrize("changed", ["incarnation", "actor", "scope", "container"])
async def test_mismatched_post_attach_claim_never_publishes_a_client(tmp_path, monkeypatch, changed):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, attached, *_ = parts
    accounting(parts)
    original = mounts.retained_attachment.side_effect

    async def wrong(identifier):
        current = await original(identifier)
        if changed == "incarnation":
            return replace(current, incarnation="f" * 32)
        if changed == "actor":
            return replace(current, actor=replace(execution.instance.principal, subject_id="another-agent"))
        if changed == "scope":
            return replace(current, resources=(ResourceMount(execution.instance.home_id, 2, "home", True),))
        return replace(current, container_id="f" * 64)

    mounts.retained_attachment.side_effect = wrong
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    assert environment.provider is None
    assert residents.entry(execution.instance).preparation is not None
    assert residents.external_slots() == 1


@pytest.mark.anyio
async def test_resuming_ready_workspace_transfers_the_bound_work_reservation(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, _environment, residents, mounts, attached, *_ = parts
    facts = accounting(parts)
    current = RetainedAttachment(attached.id, "a" * 32, execution.instance.principal, "host", (ResourceMount(execution.instance.home_id, 1, "home", True),), "active", attached.container_id)
    facts[attached.id] = current
    residents.retain(current)
    async with residents.entry(execution.instance).lock:
        residents.associate(execution.instance, current, generation=1)
    reservation = await residents.admit(execution.instance, work=True)
    residents.bind(reservation, "attempt")
    execution = replace(execution, work=SimpleNamespace(attempt_id="attempt"))
    parts[1].execution = execution
    parts = (execution, *parts[1:])
    mounts.resume.return_value = attached
    await prepare(parts)
    assert not residents.reservations and residents.external_slots() == 1


@pytest.mark.anyio
@pytest.mark.parametrize("confirmed", [True, False])
async def test_fenced_preparation_clears_only_after_fresh_exact_physical_confirmation(tmp_path, monkeypatch, confirmed):
    parts = setup(tmp_path, monkeypatch)
    execution, _environment, residents, mounts, attached, *_ = parts
    accounting(parts)
    mounts.retained_attachment.return_value = None
    mounts.retained_attachment.side_effect = None

    async def history(**_kwargs):
        return SimpleNamespace(id=attached.id, phase="fenced", container_id=attached.container_id)

    mounts.inspect_preparation.side_effect = history
    mounts.confirm_fenced_preparation = AsyncMock(return_value=confirmed)
    mounts.contain_host.return_value = True
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    mounts.confirm_fenced_preparation.assert_awaited_once_with(
        actor=execution.instance.principal,
        incarnation=mounts.attach.call_args.kwargs["incarnation"],
        resources=[ResourceMount(execution.instance.home_id, 1, "home", True)],
        host_id="host",
        attachment_id=attached.id,
        container_id=attached.container_id,
    )
    assert (residents.entry(execution.instance).preparation is None) is confirmed
    assert residents.external_slots() == (0 if confirmed else 1)


@pytest.mark.anyio
@pytest.mark.parametrize("path", ["attach", "resume"])
@pytest.mark.parametrize("phase", ["pending", "fence_pending"])
async def test_nonactive_detached_facts_never_reach_sdk_preparation(tmp_path, monkeypatch, path, phase):
    from deerflow.agent_instances import runtime

    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, attached, *_ = parts
    facts = accounting(parts)
    if path == "resume":
        current = RetainedAttachment(attached.id, "a" * 32, execution.instance.principal, "host", (ResourceMount(execution.instance.home_id, 1, "home", True),), "active", attached.container_id)
        facts[attached.id] = current
        residents.retain(current)
        async with residents.entry(execution.instance).lock:
            residents.associate(execution.instance, current, generation=1)
        mounts.resume.return_value = attached
    original = mounts.retained_attachment.side_effect

    async def closing(identifier):
        return replace(await original(identifier), phase=phase)

    mounts.retained_attachment.side_effect = closing
    client = AsyncMock()
    monkeypatch.setattr(runtime, "_prepare_client", client)
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    client.assert_not_awaited()
    assert environment.provider is None and residents.external_slots() == 1
    mounts.contain_host.assert_awaited_once_with(attached.id, "start-failed")


@pytest.mark.anyio
async def test_fresh_active_snapshot_cannot_undo_a_newer_retained_closure(tmp_path, monkeypatch):
    from deerflow.agent_instances import runtime

    parts = setup(tmp_path, monkeypatch)
    _execution, environment, residents, mounts, attached, *_ = parts
    accounting(parts)
    original = mounts.retained_attachment.side_effect

    async def delayed(identifier):
        current = await original(identifier)
        residents.retain(replace(current, phase="fence_pending"))
        return current

    mounts.retained_attachment.side_effect = delayed
    client = AsyncMock()
    monkeypatch.setattr(runtime, "_prepare_client", client)
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    client.assert_not_awaited()
    assert residents.retained[attached.id].phase == "fence_pending"
    assert residents.external_slots() == 1 and environment.provider is None


async def externally_closed(parts, phase="active"):
    """The operator committed closure without the Gateway's in-process observer."""
    execution, _environment, residents, mounts, attached, *_ = parts
    facts = accounting(parts)
    old = RetainedAttachment("1" * 32, "2" * 32, execution.instance.principal, "host", (ResourceMount(execution.instance.home_id, 1, "home", True),), "active", "3" * 64)
    residents.retain(old)
    async with residents.entry(execution.instance).lock:
        residents.associate(execution.instance, old, generation=1)
    if phase != "active":
        residents.retain(replace(old, phase=phase))
    read = mounts.retained_attachment.side_effect

    async def current(identifier):
        return None if identifier == old.id else await read(identifier)

    mounts.retained_attachment.side_effect = current
    mounts.inspect_preparation.return_value = AttachmentState(old.id, "fenced", old.host_id, old.container_id)

    async def notify(identifier, reason, **_kwargs):
        assert identifier == old.id and reason == "container-absent"
        assert residents.entry(execution.instance).lock.locked()
        await residents.contained(identifier, (execution.instance.home_id,), "operator")
        return True

    mounts.contain_host.side_effect = notify
    return old, facts


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["active", "fence_pending"])
async def test_operator_closed_workspace_is_confirmed_before_replacement(tmp_path, monkeypatch, phase):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, attached, _config, client = parts
    old, facts = await externally_closed(parts, phase)
    assert await prepare(parts) is client
    mounts.inspect_preparation.assert_awaited_once_with(actor=old.actor, incarnation=old.incarnation, resources=old.resources, host_id=old.host_id)
    mounts.contain_host.assert_awaited_once_with(old.id, "container-absent", admission=ANY)
    mounts.attach.assert_awaited_once()
    assert environment.attachment_id == attached.id and residents.entry(execution.instance).attachment_id == attached.id
    assert residents.retained == {attached.id: facts[attached.id]} and residents.external_slots() == 1


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["unconfirmed", "confirmation-error", "notification-missing", "containment-pending"])
async def test_unconfirmed_external_closure_retains_the_original_charge(tmp_path, monkeypatch, failure):
    from deerflow.spaces.attachments import AttachmentPending

    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, *_ = parts
    old, _facts = await externally_closed(parts)
    if failure == "unconfirmed":
        mounts.inspect_preparation.return_value = None
    elif failure == "confirmation-error":
        mounts.inspect_preparation.side_effect = AttachmentPending(failure)
    else:
        mounts.contain_host.side_effect = None
        mounts.contain_host.return_value = failure == "notification-missing"
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    mounts.attach.assert_not_awaited()
    assert environment.provider is None and residents.entry(execution.instance).attachment_id == old.id
    assert residents.retained == {old.id: old} and residents.external_slots() == 1


@pytest.mark.anyio
async def test_operator_closure_racing_with_resume_is_confirmed_before_replacement(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    _execution, _environment, residents, mounts, attached, _config, client = parts
    old, _facts = await externally_closed(parts)
    closed = False
    read = mounts.retained_attachment.side_effect

    async def current(identifier):
        return old if identifier == old.id and not closed else await read(identifier)

    async def resume(**_kwargs):
        nonlocal closed
        closed = True
        return None

    mounts.retained_attachment.side_effect = current
    mounts.resume.side_effect = resume
    assert await prepare(parts) is client
    mounts.inspect_preparation.assert_awaited_once()
    mounts.contain_host.assert_awaited_once_with(old.id, "container-absent", admission=ANY)
    assert old.id not in residents.retained and attached.id in residents.retained


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["inspection", "notification"])
async def test_external_closure_confirmation_drains_before_cancelled_preparation_returns(tmp_path, monkeypatch, stage):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, attached, _config, client = parts
    old, _facts = await externally_closed(parts)
    entered, release = asyncio.Event(), asyncio.Event()

    original = mounts.inspect_preparation if stage == "inspection" else mounts.contain_host.side_effect

    async def confirm(*args, **kwargs):
        assert residents.entry(execution.instance).lock.locked()
        entered.set()
        await release.wait()
        if stage == "inspection":
            return AttachmentState(old.id, "fenced", old.host_id, old.container_id)
        return await original(*args, **kwargs)

    if stage == "inspection":
        mounts.inspect_preparation.side_effect = confirm
    else:
        mounts.contain_host.side_effect = confirm
    worker = asyncio.create_task(prepare(parts))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        worker.cancel()
        await asyncio.sleep(0)
        assert not worker.done() and residents.retained == {old.id: old}
        mounts.attach.assert_not_awaited()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert environment.provider is client and environment.attachment_id == attached.id
        assert old.id not in residents.retained and residents.external_slots() == 1
    finally:
        release.set()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("changed", ["actor", "host", "scope", "container"])
async def test_external_closure_cannot_release_a_mismatched_retained_identity(tmp_path, monkeypatch, changed):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, *_ = parts
    old, _facts = await externally_closed(parts)
    updates = {
        "actor": dict(actor=replace(old.actor, subject_id="another-agent")),
        "host": dict(host_id="another-host"),
        "scope": dict(resources=(ResourceMount(execution.instance.home_id, 2, "home", True),)),
        "container": dict(container_id=None, phase="pending"),
    }
    changed_claim = replace(old, **updates[changed])
    residents.retained[old.id] = changed_claim
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    mounts.inspect_preparation.assert_not_awaited()
    mounts.attach.assert_not_awaited()
    assert environment.provider is None and residents.entry(execution.instance).attachment_id == old.id
    assert residents.retained == {old.id: changed_claim} and residents.external_slots() == 1


@pytest.mark.anyio
async def test_empty_resume_does_not_release_a_still_active_retained_workspace(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, *_ = parts
    old, _facts = await externally_closed(parts)
    mounts.retained_attachment.side_effect = None
    mounts.retained_attachment.return_value = old
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    mounts.inspect_preparation.assert_not_awaited()
    mounts.attach.assert_not_awaited()
    assert environment.provider is None and residents.entry(execution.instance).attachment_id == old.id
    assert residents.retained == {old.id: old} and residents.external_slots() == 1


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["incomplete", "closing", "reconciling"])
async def test_external_closure_does_not_bypass_closed_runtime_admission(tmp_path, monkeypatch, state):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, residents, mounts, *_ = parts
    old, _facts = await externally_closed(parts)
    if state == "incomplete":
        residents.restoration_complete = False
    elif state == "closing":
        residents._closing = True
    else:
        residents.entry(execution.instance).reconciling = True
    with pytest.raises(ResidentRestarting):
        await prepare(parts)
    mounts.inspect_preparation.assert_not_awaited()
    mounts.contain_host.assert_not_awaited()
    mounts.attach.assert_not_awaited()
    assert environment.provider is None and residents.retained == {old.id: old}
