"""Workspace refusals retain the generic host envelope and stable sentences."""

import httpx
import pytest
from fastapi import FastAPI

from app.gateway.refusals import refusal_handler
from deerflow.agent_instances import contract


@pytest.mark.anyio
@pytest.mark.parametrize(
    "class_name,reason,detail",
    [
        ("ResidentBusy", "busy", "Too much is running at once, so River could not start. Try again in a minute."),
        ("ResidentWorkLimit", "work-limit", "As many assignments are running as this deployment runs at once. Start this one when one of them finishes."),
        ("ResidentRunLimit", "run-limit", "River is already working in two conversations. Try again when one of them finishes."),
        ("ResidentRestarting", "restarting", "River's workspace restarts once another conversation finishes. Try again in a few minutes."),
        ("ResidentStartingElsewhere", "starting-elsewhere", "Another conversation is starting River. Try again in a moment."),
        ("ResidentStartFailed", "start-failed", "River's workspace did not start. Try again."),
        ("ResidentFilesFull", "files-full", "River's files are full, so River cannot start. Delete files or ask for more room."),
        ("ResidentFilesBusy", "storage-busy", "River's files are being changed right now and cannot be used. Try again in a few minutes."),
    ],
)
async def test_workspace_refusal_uses_existing_http_envelope(class_name, reason, detail):
    refusal = getattr(contract, class_name)("River", cause="workflow")
    assert isinstance(refusal, contract.ResidentUnavailable)
    assert isinstance(refusal, contract.Refusal)
    assert str(refusal) == refusal.detail == detail
    app = FastAPI()
    app.add_exception_handler(contract.Refusal, refusal_handler)

    @app.get("/refuse")
    async def refuse():
        raise refusal

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/refuse")
    assert response.status_code == refusal.status == 409
    assert response.json() == {"reason": reason, "detail": detail, "cause": "workflow"}


def test_interruption_is_a_frame_name_and_display_names_remain_data():
    assert contract.RESIDENT_INTERRUPTED == "ResidentInterrupted"
    assert not hasattr(contract, "ResidentInterrupted")
    refusal = contract.ResidentBusy("River {name}")
    assert "River {name} could not start" in refusal.detail
