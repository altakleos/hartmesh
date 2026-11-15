"""A real other-instance activation cannot occupy a staged child operation."""

import asyncio
from dataclasses import dataclass
from uuid import uuid4

import pytest
from _storage_spaces_test_support import FixtureCatalog
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_start_context_ownership import arm_conversation_change
from test_telegram_work_actions import actions as actions
from test_telegram_work_start import start_args
from test_telegram_work_start import startable as startable

from app.telegram_bots.authority import internal_request
from app.telegram_bots.work_start_conversations import context_child_id
from deerflow.agent_instances.contract import DefinitionSnapshot
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.base import Base
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.spaces.filesystem import ConfinedFilesystem
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles


@dataclass(frozen=True)
class PreparedVolume:
    spec: object
    data_path: object
    control_path: object
    device: int
    root_inode: int
    control_inode: int

    def filesystem(self):
        return ConfinedFilesystem(self.data_path, self.control_path)


def prepared_catalog(path):
    path.mkdir()
    catalog = FixtureCatalog(path)
    catalog.verified = {key: PreparedVolume(value.spec, value.data_path, value.control_path, value.device, value.root_inode, value.control_inode) for key, value in catalog.verified.items()}
    return catalog


@pytest.mark.asyncio
async def test_late_other_instance_operation_is_refused_at_actual_child_native_entry(startable, monkeypatch, tmp_path):
    from app.gateway import work_execution

    e = startable
    # Create the other instance through the actual instance/Space services on
    # the same SQL engine. Prepared directories are a resource adapter, not
    # native containment evidence. No actor, event or attempt frame is invented.
    async with e.db.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    directory = e.human.agents.directory
    files = SpaceFiles(
        SpaceRegistry(e.db.sessions, HostPrincipalResolver(human=directory.human, nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session)), await asyncio.to_thread(prepared_catalog, tmp_path / "other-instance-storage")
    )
    monkeypatch.setattr(e.human.agents, "files", files)
    other = await e.human.agents.create(
        actor=e.actor,
        creation_id=uuid4().hex,
        name="Other documentation assistant",
        custody="personal",
        supervisor=e.actor,
        definition=DefinitionSnapshot.capture(owner_id=e.actor.subject_id, config={"name": "analyst", "work_policy": {"enabled": True}}, soul="Review another guide."),
    )
    other_work = await e.human.work.delegate(actor=e.actor, instance_id=other.id, request=DelegateWork(operation_id=uuid4().hex, objective="Review another guide", success_criteria="Report its verified gaps"))
    conversation = await e.http_app.state.agent_conversations.create(actor=e.actor, instance_id=other.id, creation_id=uuid4().hex)
    root = await e.acts.stage_activation(e.link, **start_args(e))
    child_id = context_child_id(root.id)
    native = work_execution.activate_work
    arm_conversation_change(e, monkeypatch)
    armed = work_execution.activate_work
    injected = False

    async def late_other(request, **kwargs):
        nonlocal injected
        if kwargs["body"].operation_id == child_id and not injected:
            injected = True
            async with e.db.sessions() as reader:
                assert await reader.get(TelegramActRow, child_id) is not None
            await native(
                internal_request(e.http_app, e.link.owner_user_id),
                instance_id=other.id,
                work_id=other_work["id"],
                body=ActivateWork(operation_id=child_id, expected_revision=other_work["revision"], expected_assignment_revision=other_work["assignment_revision"], thread_id=conversation["thread_id"]),
            )
        return await armed(request, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", late_other)
    await e.acts.check(root.id)
    assert injected
    async with e.db.sessions() as reader:
        attempts = list(await reader.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id)))
        assert len(attempts) == 1 and attempts[0].work_id == other_work["id"]
        child = await reader.get(TelegramActRow, child_id)
        assert child.state == "refused"
    assert len(e.launches) == 1
