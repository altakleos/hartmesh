"""Whether a scheduler is actually running for the saved schedules (``GET /api/scheduler``)."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.gateway.authz import require_permission
from app.gateway.deps import get_config

router = APIRouter(prefix="/api", tags=["scheduled-tasks"])

SchedulerState = Literal["running", "disabled_by_configuration", "not_running", "unavailable"]


class SchedulerStateResponse(BaseModel):
    """Whether a scheduler is actually running for the saved schedules."""

    version: int = Field(description="Shape version of this record.")
    running: bool = Field(description="Whether a scheduler loop is polling here, read from the live service rather than from configuration.")
    configured: bool = Field(description="What scheduler.enabled says now. The file hot-reloads and the scheduler starts once at startup, so this and `running` can legitimately disagree.")
    state: SchedulerState = Field(
        description=(
            "The single discriminator to branch on. `running`: schedules will run. "
            "`disabled_by_configuration`: scheduling is turned off. `not_running`: turned on, "
            "but no loop is polling here. `unavailable`: this Gateway has no scheduler at all. "
            "Saved schedules are untouched in every case, and triggering one by hand still "
            "works except under `unavailable`."
        )
    )


@router.get("/scheduler", response_model=SchedulerStateResponse)
@require_permission("threads", "read")
async def get_scheduler_state(request: Request) -> SchedulerStateResponse:
    """Whether anything is actually going to run the saved schedules.

    A task row carries its own status and its next run time, and neither can
    say that no scheduler is polling for it: a row can read "enabled, next run
    last week" on a Gateway whose scheduler never started, with no surface
    that explains the gap. This is that fact, so a reader is told instead of
    left to infer it from a date.

    ``running`` is read from the service, not from configuration: the
    configuration file hot-reloads and the scheduler starts once at startup, so
    the two can legitimately disagree, and a start that failed is worth
    surfacing rather than papering over. ``configured`` is what the file says
    now. ``state`` is the single discriminator a caller should branch on.

    A read, and only a read -- opening a page never starts anything.
    Triggering a task by hand is unaffected in every state but
    ``unavailable``: the service is constructed whether or not it is started.
    """
    service = getattr(request.app.state, "scheduled_task_service", None)
    configured = bool(get_config().scheduler.enabled)
    running = bool(service is not None and getattr(service, "running", False))
    if running:
        state: SchedulerState = "running"
    elif service is None:
        state = "unavailable"
    elif not configured:
        state = "disabled_by_configuration"
    else:
        state = "not_running"
    return SchedulerStateResponse(version=1, running=running, configured=configured, state=state)
