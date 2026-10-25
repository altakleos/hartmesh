"""Work completion keeps native authority separate from its finite SQL commit."""

import pytest
from _storage_spaces_test_support import ALICE
from test_agent_instances import instances as instances
from test_agent_work_records import work as work
from test_work_execution import activation, run_record
from test_work_execution import execution as execution

from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope


@pytest.mark.asyncio
@pytest.mark.parametrize("retired", [True, False])
async def test_work_finish_transaction_refuses_retired_owner_without_resolving_attempt(execution, retired):
    service, admitted, row, _sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    record = run_record(admitted, scope)
    await scope.bind(admitted, record, admitted.thread_incarnation)
    owner = CaptureIOOwner()
    if retired:
        owner.retire()
    try:
        with capture_io_scope(owner):
            if retired:
                with pytest.raises(CaptureIOClosed):
                    await scope.finish(admitted, record, settled=True)
            else:
                await scope.finish(admitted, record, settled=True)
        current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
        assert current["attempt"]["status"] == ("running" if retired else "succeeded")
    finally:
        owner.retire()
        await owner.drain()
