"""Run-start refusals preserve grants and never contain a workspace."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.agent_instances.contract import AgentDenied, ExecutionUnavailable, ResidentFilesBusy, ResidentStartFailed, ResidentStartingElsewhere
from deerflow.spaces.attachments import AttachmentPending
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.service import SpaceOperationPending


@pytest.mark.anyio
@pytest.mark.parametrize(
    "error,expected",
    [(SpaceOperationPending("busy", cause="lifecycle"), ResidentFilesBusy), (BackingUnavailable("missing"), ExecutionUnavailable), (AttachmentPending("pending"), ResidentStartingElsewhere), (AgentDenied("denied"), AgentDenied)],
)
async def test_start_admission_maps_only_storage_refusals(error, expected):
    from deerflow.agent_instances.runtime import admit_storage

    attachments = SimpleNamespace(unfenced=AsyncMock(return_value=SimpleNamespace(phase="pending")), contain_host=AsyncMock(side_effect=AssertionError("Admission cannot contain")))
    agents = SimpleNamespace(files=SimpleNamespace(attachments=attachments), execution_probe=AsyncMock(return_value=SimpleNamespace(reason=None)))
    instance = SimpleNamespace(id="agent", home_id="home", name="Analyst")
    call = AsyncMock(side_effect=error)
    with pytest.raises(expected):
        await admit_storage(agents, instance, call, wait=True)
    call.assert_awaited_once()
    attachments.contain_host.assert_not_awaited()


@pytest.mark.anyio
async def test_fence_pending_admission_reports_start_failed_without_cleanup():
    from deerflow.agent_instances.runtime import admit_storage

    agents = SimpleNamespace(files=SimpleNamespace(attachments=SimpleNamespace(unfenced=AsyncMock(return_value=SimpleNamespace(phase="fence_pending")))))
    with pytest.raises(ResidentStartFailed):
        await admit_storage(agents, SimpleNamespace(id="agent", home_id="home", name="Analyst"), AsyncMock(side_effect=AttachmentPending("pending")))
