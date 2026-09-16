"""Read-only feature-flag endpoint for the frontend bootstrap.

Reports which optional features are exposed over HTTP so the frontend can gate
UI and avoid firing requests that the backend would reject. Config-only flags
read through ``get_config`` so edits to ``config.yaml`` take effect on the next
request, while startup-scoped capabilities report the runtime that actually
started.
"""

from typing import Literal

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from app.gateway.browser_capability import browser_capability
from app.gateway.deps import get_config
from deerflow.config.app_config import AppConfig
from deerflow.config.ui_config import MAX_STARTER_PROMPT_CHARS, MAX_STARTER_TITLE_CHARS, MAX_STARTERS, UiConfig
from deerflow.subagents.capacity import configured_subagent_max_running

router = APIRouter(prefix="/api", tags=["features"])


class AgentsApiFeature(BaseModel):
    """Availability of the custom-agent management API."""

    enabled: bool = Field(..., description="Whether the agents_api routes are exposed over HTTP")


class BrowserControlFeature(BaseModel):
    """Availability of live agentic browser control."""

    enabled: bool = Field(..., description="Whether the live browser routes and UI are available")


class McpTasksFeature(BaseModel):
    """Availability of the durable MCP task runtime."""

    enabled: bool = Field(..., description="Whether durable MCP task APIs and UI are available")


class SubagentBatchesFeature(BaseModel):
    """Persistence, worker, and process capacity for native-subagent batches."""

    enabled: bool = Field(..., description="Compatibility alias for worker_running")
    repository_available: bool = Field(..., description="Whether durable batch history APIs are available")
    worker_running: bool = Field(..., description="Whether this Gateway process is executing durable batch work")
    max_running: int = Field(..., description="Native subagent execution slots in this Gateway process")


class UiStarter(BaseModel):
    """One thing Home offers before anyone has typed."""

    id: str = Field(..., max_length=64, description="Stable identifier; the grid's key")
    title: str = Field(..., max_length=MAX_STARTER_TITLE_CHARS, description="The words on the tile")
    prompt: str = Field(..., max_length=MAX_STARTER_PROMPT_CHARS, description="What choosing the tile puts in the message box; nothing is sent")


class UiFeature(BaseModel):
    """What the deployment says the workspace should show."""

    profile: Literal["business", "developer"] = Field(..., description="'business' keeps the developer screens for administrators; 'developer' offers them to everyone")
    starters: list[UiStarter] = Field(..., max_length=MAX_STARTERS, description="Home's starter grid, in the order it is shown")


class FeaturesResponse(BaseModel):
    """Frontend-facing feature availability flags."""

    agents_api: AgentsApiFeature
    browser_control: BrowserControlFeature
    mcp_tasks: McpTasksFeature
    subagent_batches: SubagentBatchesFeature
    ui: UiFeature


@router.get(
    "/features",
    response_model=FeaturesResponse,
    summary="List Feature Flags",
    description="Report which optional features are available, so the frontend can gate UI before issuing requests.",
)
async def list_features(request: Request, config: AppConfig = Depends(get_config)) -> FeaturesResponse:
    """Return availability of optional frontend features."""
    browser = browser_capability(config)
    subagent_batch_worker_running = bool(getattr(request.app.state, "subagent_batches_available", False))
    return FeaturesResponse(
        agents_api=AgentsApiFeature(enabled=config.agents_api.enabled),
        browser_control=BrowserControlFeature(enabled=browser.available),
        # MCP task bindings and the submitter are startup-scoped. Report the
        # capability that actually started rather than a hot-reloaded config
        # value that would require a Gateway restart to take effect.
        mcp_tasks=McpTasksFeature(enabled=bool(getattr(request.app.state, "mcp_tasks_available", False))),
        subagent_batches=SubagentBatchesFeature(
            # Keep the historical `enabled` field as a compatibility alias
            # while exposing read persistence independently from execution.
            # A stopped/disabled worker must not hide durable history/export.
            enabled=subagent_batch_worker_running,
            repository_available=getattr(request.app.state, "subagent_batch_repo", None) is not None,
            worker_running=subagent_batch_worker_running,
            max_running=configured_subagent_max_running(),
        ),
        # Presentation the frontend cannot decide for itself: the profile is
        # the deployment's choice and the starters are its words. Read through
        # `get_config`, so an edit reaches the next page load.
        ui=_ui_feature(config.ui),
    )


def _ui_feature(ui: UiConfig) -> UiFeature:
    """The workspace presentation, as `UiConfig` resolved it."""
    return UiFeature(
        profile=ui.profile,
        starters=[UiStarter(id=starter.id, title=starter.title, prompt=starter.prompt) for starter in ui.starters],
    )
