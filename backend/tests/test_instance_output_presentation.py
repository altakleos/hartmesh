"""SQL and confined-directory fixtures; native backing qualification lives in CI."""

import asyncio
import os
import time
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, operation
from sqlalchemy import select
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agent_instances.runtime import InstanceSandboxProvider, execution_scope
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.tools.presentation import validate_presentation


async def prepared(instances):
    agents, authority, _, sf = await conversations(instances)
    instance = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=instance.id, thread_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    async with sf() as session:
        backing = await session.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == instance.home_id))
    root = instances[3].verified[backing.slot_id].data_path
    (root / "outputs").mkdir()
    (root / "outputs/result.txt").write_text("real output", encoding="utf-8")
    runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: execution, "thread_id": execution.thread_id}, state={"thread_data": execution.thread_paths})
    return execution, runtime, root


@pytest.mark.asyncio
async def test_instance_metadata_comes_from_confined_home_and_never_opens_special_files(instances):
    execution, runtime, root = await prepared(instances)
    outputs = root / "outputs"
    (root / "private.txt").write_text("not an output", encoding="utf-8")
    (outputs / "internal.txt").symlink_to("result.txt")
    (outputs / "escape.txt").symlink_to("../private.txt")
    (outputs / "directory").mkdir()
    os.mkfifo(outputs / "pipe")
    names = ["result.txt", "internal.txt", "escape.txt", "directory", "pipe", "missing.txt"]
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        result = await asyncio.wait_for(asyncio.to_thread(validate_presentation, runtime, ["/mnt/user-data/outputs/" + name for name in names]), 5)
        stale = await asyncio.to_thread(validate_presentation, runtime, ["/mnt/spaces/home/outputs/result.txt"], written_after=time.time() + 10)
    assert result.presented == ["/mnt/user-data/outputs/result.txt", "/mnt/user-data/outputs/internal.txt"]
    assert result.sizes == dict.fromkeys(result.presented, 11)
    assert len(result.refused) == 4
    assert not stale.presented and "not written by this call" in stale.refused[0][1]


@pytest.mark.asyncio
async def test_instance_metadata_rejects_replaced_outputs_root(instances):
    execution, runtime, root = await prepared(instances)
    (root / "outputs").rename(root / "private")
    (root / "outputs").symlink_to("private", target_is_directory=True)
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        result = await asyncio.to_thread(validate_presentation, runtime, ["/mnt/user-data/outputs/result.txt"])
    assert not result.presented and result.refused


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["unprepared", "other-execution", "inspection"])
async def test_instance_metadata_requires_exact_prepared_executable_context(instances, binding):
    from dataclasses import replace

    execution, runtime, _ = await prepared(instances)
    if binding == "inspection":
        execution = replace(execution, execution_allowed=False)
        runtime.context[AGENT_EXECUTION_CONTEXT_KEY] = execution
    with execution_scope(execution) as environment:
        if binding != "unprepared":
            provider_execution = replace(execution) if binding == "other-execution" else execution
            environment.provider = InstanceSandboxProvider(provider_execution, SimpleNamespace(id="fixture"))
        result = await asyncio.to_thread(validate_presentation, runtime, ["/mnt/user-data/outputs/result.txt"])
    assert not result.presented and result.refused


@pytest.mark.asyncio
async def test_metadata_read_fences_a_generation_change_after_execution_validation(instances, monkeypatch):
    from deerflow.persistence.spaces.model import SpaceRow

    execution, runtime, _ = await prepared(instances)
    original = execution.authority.validate

    async def changed_after_validation(bound):
        await original(bound)
        async with instances[2]() as session, session.begin():
            (await session.get(SpaceRow, execution.instance.home_id)).generation += 1

    monkeypatch.setattr(execution.authority, "validate", changed_after_validation)
    with execution_scope(execution) as environment:
        environment.provider = InstanceSandboxProvider(execution, SimpleNamespace(id="fixture"))
        result = await asyncio.to_thread(validate_presentation, runtime, ["/mnt/user-data/outputs/result.txt"])
    assert not result.presented and result.refused
