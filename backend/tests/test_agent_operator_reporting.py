"""Known failures after real or uncertain closure cannot claim no effects."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import pytest_asyncio
from test_agent_operator_commands import create
from test_agent_operator_commands import operator as operator
from test_native_create_bridge import fixture as fixture
from test_native_resident_containment_scope import prepare
from test_storage_spaces_attachments import ContainedProvider

from app.gateway.agent_operator_contract import CommandRefused
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.attachments import SpaceAttachments
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.native_create_evidence import NativeEvidenceReader
from deerflow.spaces.operator_containment import OperatorResidentContainmentScopes


@pytest_asyncio.fixture
async def workspace(operator, fixture, monkeypatch):
    agent = await create(operator)
    provider = ContainedProvider()
    attachments = SpaceAttachments(operator.files, provider)
    _gateway, saved, _captured, _objects, calls = await prepare(
        (
            operator.files,
            operator.fixture.sf,
            operator.fixture.catalog,
            attachments,
            provider,
        ),
        fixture,
        space_id=agent.home_id,
        actor=PrincipalRef("human", str(operator.fixture.owner.id)),
    )
    provider._operator_only = True
    evidence = NativeEvidenceReader(fixture[0].pool, daemon_id=provider.host_id)
    provider._containment_scope = OperatorResidentContainmentScopes(files=operator.files, adapter=provider, evidence=evidence)
    operator.adapter, operator.attachments = provider, attachments
    destination = operator.fixture.path.parent / (operator.fixture.path.name + "-reporting-exports")
    monkeypatch.setattr("app.gateway.agent_operator_actions.EXPORT_ROOT", destination)
    monkeypatch.setattr("os.statvfs", lambda _path: SimpleNamespace(f_bavail=1 << 50, f_frsize=1))
    try:
        yield SimpleNamespace(
            command=operator,
            agent=agent,
            attachment_id=saved.attachment_id,
            calls=calls,
            destination=destination,
        )
    finally:
        evidence.close()


def note(error):
    return getattr(error, "note", "refused; nothing was changed")


@pytest.mark.asyncio
async def test_export_late_headroom_failure_reports_the_actual_closed_workspace(workspace, monkeypatch):
    count = 0

    def headroom(_path):
        nonlocal count
        count += 1
        return SimpleNamespace(f_bavail=(1 << 50) if count == 1 else 0, f_frsize=1)

    monkeypatch.setattr("os.statvfs", headroom)
    command, agent = workspace.command, workspace.agent
    with pytest.raises(CommandRefused, match="would leave the data disk under 1 GiB free") as failure:
        await command.run("export", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    assert note(failure.value) == "failed; run the verb again"
    assert not workspace.destination.exists()
    async with command.fixture.sf() as session:
        row = await session.get(SpaceAttachmentRow, workspace.attachment_id)
        assert row.phase == "fenced" and row.fenced_at is not None


@pytest.mark.asyncio
async def test_delete_late_owner_reactivation_preserves_reason_and_reports_prior_closure(workspace, monkeypatch):
    command, agent = workspace.command, workspace.agent
    async with command.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(command.fixture.owner.id))).released_at = datetime.now(UTC)
    close = command._close

    async def reactivated(*args, **kwargs):
        result = await close(*args, **kwargs)
        async with command.fixture.sf() as session, session.begin():
            (await session.get(UserRow, str(command.fixture.owner.id))).released_at = None
        return result

    monkeypatch.setattr(command, "_close", reactivated)
    with pytest.raises(CommandRefused, match="owner's account is active") as failure:
        await command.run("delete", instance_id=agent.id, end_running_work=False, wait_seconds=0)
    assert note(failure.value) == "failed; run the verb again"
    assert failure.value.fields == {"reason": "owner-active"}
    async with command.fixture.sf() as session:
        assert (await session.get(SpaceAttachmentRow, workspace.attachment_id)).phase == "fenced"
        assert (await session.get(AgentInstanceRow, agent.id)).status == "active"
        assert (await session.get(SpaceRow, agent.home_id)).status == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize("verb", ["delete", "export"])
async def test_lost_closure_acknowledgement_never_reports_that_nothing_changed(workspace, monkeypatch, verb):
    command, agent = workspace.command, workspace.agent
    if verb == "delete":
        async with command.fixture.sf() as session, session.begin():
            (await session.get(UserRow, str(command.fixture.owner.id))).released_at = datetime.now(UTC)
    contain = command.attachments.contain_host

    async def lost_ack(*args, **kwargs):
        assert await contain(*args, **kwargs)
        return False

    monkeypatch.setattr(command.attachments, "contain_host", lost_ack)
    with pytest.raises(
        CommandRefused,
        match=f"nothing was {'deleted' if verb == 'delete' else 'exported'}",
    ) as failure:
        await command.run(verb, instance_id=agent.id, end_running_work=False, wait_seconds=0)
    assert note(failure.value) == "failed; run the verb again"
    assert not workspace.destination.exists()
    async with command.fixture.sf() as session:
        assert (await session.get(SpaceAttachmentRow, workspace.attachment_id)).phase == "fenced"
        assert (await session.get(AgentInstanceRow, agent.id)).status == "active"


@pytest.mark.asyncio
@pytest.mark.parametrize("verb", ["delete", "export"])
async def test_operator_preflight_refusals_remain_effect_free(workspace, monkeypatch, verb):
    command, agent = workspace.command, workspace.agent
    if verb == "export":
        monkeypatch.setattr("os.statvfs", lambda _path: SimpleNamespace(f_bavail=0, f_frsize=1))
    with pytest.raises(CommandRefused) as failure:
        await command.run(verb, instance_id=agent.id, end_running_work=False, wait_seconds=0)
    assert note(failure.value) == "refused; nothing was changed"
    async with command.fixture.sf() as session:
        row = await session.get(SpaceAttachmentRow, workspace.attachment_id)
        assert row.phase == "active" and row.fenced_at is None
    assert not any(call[1] in ("stop", "rm") for call in workspace.calls)
    assert not workspace.destination.exists()


@pytest.mark.parametrize("after_effects", [False, True])
def test_operator_json_preserves_known_error_fields_and_distinguishes_post_effect_failures(monkeypatch, capsys, after_effects):
    from app.gateway import agent_instances as module
    from app.gateway import agent_operator_contract as contract

    error_type = contract.CommandFailed if after_effects else contract.CommandRefused

    async def run(_command, **_options):
        raise error_type("the account became active; nothing was deleted", reason="owner-active")

    monkeypatch.setattr(module, "_run", run)
    assert module.main(["delete", "a" * 32]) == 1
    output = capsys.readouterr()
    document = json.loads(output.out)
    assert document == {
        "command": "delete",
        "returncode": 1,
        "error": "the account became active; nothing was deleted",
        "note": "failed; run the verb again" if after_effects else "refused; nothing was changed",
        "reason": "owner-active",
    }
    assert output.out == json.dumps(document, sort_keys=True) + "\n"
    assert ("CommandFailed:" in output.err) if after_effects else output.err == ""


@pytest.mark.asyncio
async def test_delete_rechecks_owner_after_held_native_proof_before_any_closure_effect(
    workspace,
):
    command, agent = workspace.command, workspace.agent
    async with command.fixture.sf() as session, session.begin():
        (await session.get(UserRow, str(command.fixture.owner.id))).released_at = datetime.now(UTC)
    scope = command.adapter._containment_scope
    entered, release = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def held(attachments, captured):
        async with scope(attachments, captured) as proof:
            entered.set()
            await release.wait()
            yield proof

    command.adapter._containment_scope = held
    task = asyncio.create_task(command.run("delete", instance_id=agent.id, end_running_work=False, wait_seconds=0))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with command.fixture.sf() as session, session.begin():
            (await session.get(UserRow, str(command.fixture.owner.id))).released_at = None
        release.set()
        with pytest.raises(CommandRefused, match="owner's account is active") as failure:
            await task
        assert note(failure.value) == "refused; nothing was changed"
        assert failure.value.fields == {"reason": "owner-active"}
        async with command.fixture.sf() as session:
            row = await session.get(SpaceAttachmentRow, workspace.attachment_id)
            assert row.phase == "active" and row.fenced_at is None
        assert not any(call[1] in ("stop", "rm") for call in workspace.calls)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
