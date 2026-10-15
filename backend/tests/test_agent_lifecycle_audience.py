"""Chat-only members do not become readers of unrelated conversations or Work."""

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_gateway_work_admission import request_for
from test_resident_registry import registry as registry
from test_storage_spaces_attachments import ContainedProvider
from test_work_execution import activation, run_record
from test_work_execution import execution as execution

from deerflow.agent_instances.lifecycle import InstanceLifecycle
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.attachments import SpaceAttachments
from deerflow.spaces.contract import SpaceDenied


@pytest.mark.asyncio
async def test_chat_only_revokes_existing_files_access_without_blocking_other_chats_or_work(execution, registry, monkeypatch):
    from app.gateway.work_execution import activate_work_run

    service, admitted, row, sf = execution
    agents = service.agents
    SpaceAttachments(agents.files, ContainedProvider())
    lifecycle = InstanceLifecycle(agents)
    result = await lifecycle.change(actor=ALICE, instance_id=admitted.instance.id, expected_generation=admitted.instance.generation, operation_id=operation(), action="grant", member=BOB, permissions=1)
    assert result["complete"]
    async with sf() as session:
        assert await session.get(SpaceGrantRow, (admitted.instance.home_id, "human", "bob")) is None
    with pytest.raises(SpaceDenied):
        await agents.files.registry.get(actor=BOB, space_id=admitted.instance.home_id)
    fresh = await admitted.authority.execution(actor=ALICE, thread_id=admitted.thread_id)
    await admitted.authority.admit(fresh)
    own = await admitted.authority.create(actor=BOB, instance_id=admitted.instance.id, creation_id=operation())
    await admitted.authority.admit(await admitted.authority.execution(actor=BOB, thread_id=own["thread_id"]))
    residents, _provider, _records, _clock = registry()
    agents.residents = residents
    launches = []

    async def launch(_body, _thread, _request, **kwargs):
        scope = kwargs["work_attempt"]
        launches.append(scope.attempt_id)
        return run_record(fresh, scope, status="running")

    monkeypatch.setattr("app.gateway.services.start_run", launch)
    scope = await activate_work_run(request_for(service), service, fresh, row["id"], activation(fresh, row))
    assert launches == [scope.attempt_id]
