"""Read-only feature-flag endpoint for the frontend bootstrap.

Reports which optional features are exposed over HTTP so the frontend can gate
UI and avoid firing requests that the backend would reject. Config-only flags
read through ``get_config`` so edits to ``config.yaml`` take effect on the next
request, while startup-scoped capabilities report the runtime that actually
started.
"""

import asyncio
import logging
from typing import Literal

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from app.gateway.auth.mode import local_registration_open
from app.gateway.browser_capability import browser_capability
from app.gateway.conversation_access import conversation_references_enabled
from app.gateway.customer_administration import request_customer_administration_policy, resolve_customer_management_actor
from app.gateway.deps import get_config, get_current_user_from_request
from app.gateway.internal_auth import get_trusted_internal_owner_user_id
from app.gateway.knowledge_scope_admission import RAGFLOW_KNOWLEDGE_SEARCH_PROVIDER
from app.gateway.run_models import MAX_CONVERSATION_REFERENCES
from app.telegram_bots.router import telegram_feature
from deerflow.config.app_config import AppConfig
from deerflow.config.tenant_bundle import MAX_COMPANY_NAME_CHARS, MAX_SUPPORT_URL_CHARS, TenantBundle, configured_tenant_bundle
from deerflow.config.ui_config import MAX_STARTER_PROMPT_CHARS, MAX_STARTER_TITLE_CHARS, MAX_STARTERS, UiConfig
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.subagents.capacity import configured_subagent_max_running
from deerflow.utils.file_io import run_file_io

router = APIRouter(prefix="/api", tags=["features"])
logger = logging.getLogger(__name__)


class AgentsApiFeature(BaseModel):
    """Availability of the custom-agent management API."""

    enabled: bool = Field(..., description="Whether the agents_api routes are exposed over HTTP")


class StorageSpacesFeature(BaseModel):
    enabled: bool = False
    backend: str | None = None
    native_attachments: bool = False
    editor_concurrency: str | None = None
    user_spaces: bool = False
    state: Literal["off", "ready", "incomplete", "unavailable"] = "off"
    reason: str | None = None
    own: Literal["ready", "not-assigned", "no-seat", "unverified"] = "ready"


class SeatCount(BaseModel):
    allowed: int = 0
    in_use: int = 0


class AccountsFeature(BaseModel):
    seats: SeatCount = Field(default_factory=SeatCount)


class AssistantAllowance(SeatCount):
    per_user: int = 0
    mine: int = 0


class AgentInstancesFeature(BaseModel):
    enabled: bool = False
    execution: Literal["ready", "unavailable"] = "unavailable"
    reason: str | None = None
    ai_assistants: AssistantAllowance = Field(default_factory=AssistantAllowance)
    ai_employees: SeatCount = Field(default_factory=SeatCount)


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


class ConversationReferencesFeature(BaseModel):
    """Availability of explicit conversation references on run requests."""

    enabled: bool = Field(..., description="Whether the opt-in read_conversation tool is configured, so run requests may carry conversation_references")
    max_references: int = Field(..., description="Maximum conversation references accepted on one run request")


class KnowledgeBaseFeature(BaseModel):
    """Availability of RAGFlow retrieval scope selection in chat."""

    scope_selection_enabled: bool = Field(
        ...,
        description="Whether chat may select a per-message RAGFlow retrieval scope",
    )


class UiStarter(BaseModel):
    """One thing Home offers before anyone has typed."""

    id: str = Field(..., max_length=64, description="Stable identifier; the grid's key")
    title: str = Field(..., max_length=MAX_STARTER_TITLE_CHARS, description="The words on the tile")
    prompt: str = Field(..., max_length=MAX_STARTER_PROMPT_CHARS, description="What choosing the tile puts in the message box; nothing is sent")


class UiFeature(BaseModel):
    """What the deployment says the workspace should show."""

    profile: Literal["business", "developer"] = Field(..., description="'business' keeps the developer screens for administrators; 'developer' offers them to everyone")
    starters: list[UiStarter] = Field(..., max_length=MAX_STARTERS, description="Home's starter grid, in the order it is shown")


class BrandColors(BaseModel):
    """The two colours the tenant bundle names, as #rrggbb, or nothing."""

    primary: str | None = Field(..., description="Primary brand colour as #rrggbb, or null")
    secondary: str | None = Field(..., description="Secondary brand colour as #rrggbb, or null")


class ProviderSupport(BaseModel):
    """Operator support, separate from the customer's workspace identity."""

    display_name: str | None = Field(default=None, max_length=MAX_COMPANY_NAME_CHARS)
    support_url: str | None = Field(default=None, max_length=MAX_SUPPORT_URL_CHARS)


class BrandingFeature(BaseModel):
    """Whose workspace this is, as the tenant bundle says; every field is optional."""

    company_name: str | None = Field(..., max_length=MAX_COMPANY_NAME_CHARS, description="The company the workspace shows, or null for the product's own name")
    colors: BrandColors
    has_logo: bool = Field(..., description="Whether GET /api/branding/logo serves a picture")
    provider: ProviderSupport = Field(default_factory=ProviderSupport)


class CustomerAdministrationFeature(BaseModel):
    """Effective operations available to this caller in the active host."""

    plugin_management: bool = False
    local_skill_management: bool = False
    local_mcp_management: bool = False
    provider_operations: bool = False


class TelegramFeature(BaseModel):
    enabled: bool = False
    bots: Literal["ready", "unavailable"] = "unavailable"
    reason: str | None = None
    voice: Literal["available", "unavailable"] = "unavailable"
    voice_provider: Literal["openai", "gemini"] | None = None


async def _customer_administration_feature(request: Request, config: AppConfig) -> CustomerAdministrationFeature:
    policy = request_customer_administration_policy(request)
    actor = await resolve_customer_management_actor(request)
    administrator = actor.administrator is True
    if not administrator and getattr(actor, "private_skill_owner", False) is not True:
        return CustomerAdministrationFeature()
    private_supported = False
    if policy.local_skill_management:
        from app.gateway.routers.skills import _get_owned_private_skill_storage

        try:
            storage = await asyncio.to_thread(_get_owned_private_skill_storage, config)
            await asyncio.to_thread(storage._require_private_writable_path, storage.get_user_custom_root())
            private_supported = True
        except Exception:
            pass
    plugin_supported = False
    if policy.plugin_management and administrator:
        from app.gateway.app import _resolve_extension_plugin_management_async

        loaded = getattr(request.app.state, "extensions", None)
        for source, plugin in getattr(loaded, "plugins", ()):
            from deerflow.extensions.plugin_tools import plugin_settings

            if plugin_settings(source, plugin)["enabled"] is not True:
                continue
            if any(item.purpose == "management" for item in (*plugin.backend, *plugin.tools)):
                if await _resolve_extension_plugin_management_async(request, plugin.namespace, "write") is True:
                    plugin_supported = True
                    break
    return CustomerAdministrationFeature(
        plugin_management=plugin_supported,
        local_skill_management=private_supported,
        local_mcp_management=administrator and policy.local_mcp_management and bool(policy.approved_local_launches),
    )


class FeaturesResponse(BaseModel):
    """Frontend-facing feature availability flags."""

    customer_administration: CustomerAdministrationFeature = Field(default_factory=CustomerAdministrationFeature)
    storage_spaces: StorageSpacesFeature = Field(default_factory=StorageSpacesFeature)
    accounts: AccountsFeature = Field(default_factory=AccountsFeature)
    agent_instances: AgentInstancesFeature = Field(default_factory=AgentInstancesFeature)
    people_search: Literal["prefix", "exact"] = "exact"
    agents_api: AgentsApiFeature
    browser_control: BrowserControlFeature
    mcp_tasks: McpTasksFeature
    subagent_batches: SubagentBatchesFeature
    conversation_references: ConversationReferencesFeature
    knowledge_base: KnowledgeBaseFeature
    ui: UiFeature
    branding: BrandingFeature
    telegram: TelegramFeature | None = None


@router.get(
    "/features",
    response_model=FeaturesResponse,
    summary="List Feature Flags",
    description="Report which optional features are available, so the frontend can gate UI before issuing requests.",
)
async def list_features(request: Request, config: AppConfig = Depends(get_config)) -> FeaturesResponse:
    """Return availability of optional frontend features."""
    browser = browser_capability(config)
    bundle = configured_tenant_bundle(config.tenant_bundle.path)
    subagent_batch_worker_running = bool(getattr(request.app.state, "subagent_batches_available", False))
    storage, accounts, instances = await _storage_features(request, config)
    try:
        people_search = "exact" if await run_file_io(local_registration_open) else "prefix"
    except Exception:
        logger.exception("People-search availability could not be read")
        people_search = "exact"
    return FeaturesResponse(
        storage_spaces=storage,
        accounts=accounts,
        agent_instances=instances,
        people_search=people_search,
        customer_administration=await _customer_administration_feature(request, config),
        agents_api=AgentsApiFeature(enabled=config.agents_api.enabled),
        telegram=telegram_feature(request.app, config),
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
        # Same predicate as run admission (``prepare_conversation_reader``), read
        # through ``get_config`` so enabling the tool in config.yaml shows up
        # without a restart. A UI with no entry point still needs no change here.
        conversation_references=ConversationReferencesFeature(
            enabled=conversation_references_enabled(config),
            max_references=MAX_CONVERSATION_REFERENCES,
        ),
        knowledge_base=KnowledgeBaseFeature(
            scope_selection_enabled=_knowledge_scope_selection_enabled(config),
        ),
        # Presentation the frontend cannot decide for itself: the profile is
        # the deployment's choice and the starters are its words. Read through
        # `get_config`, so an edit reaches the next page load; the bundle is
        # read from disk the same way, so an operator's edit there does too.
        ui=_ui_feature(config.ui, bundle),
        branding=BrandingFeature(
            company_name=bundle.company_name,
            colors=BrandColors(primary=bundle.primary, secondary=bundle.secondary),
            has_logo=bundle.logo is not None,
            provider=ProviderSupport(display_name=bundle.provider_name, support_url=bundle.support_url),
        ),
    )


async def _feature_owner(request):
    from app.gateway.auth_disabled import AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SESSION

    source = getattr(request.state, "auth_source", None)
    if source == AUTH_SOURCE_INTERNAL:
        return get_trusted_internal_owner_user_id(request)
    if source in (AUTH_SOURCE_SESSION, AUTH_SOURCE_AUTH_DISABLED):
        user = await get_current_user_from_request(request)
        return str(user.id) if getattr(user, "id", None) is not None else None
    return None


async def _storage_features(request, config):
    from app.gateway import storage_spaces as ss

    app = request.app
    try:
        state = await ss.storage_state(app)
        if state.state == "off":
            return StorageSpacesFeature(), AccountsFeature(), AgentInstancesFeature()
        files = app.state.storage_spaces
        execution = await ss.execution_state(app)
        counts = {kind: await ss.allowance(app, kind) for kind in ("user", "assistant", "employee")}
        owner = await _feature_owner(request)
        mine, own = 0, "ready"
        if owner is not None:
            async with files.registry._sf() as session:
                mine = await session.scalar(
                    select(func.count()).select_from(AgentInstanceRow).where(AgentInstanceRow.custody == "personal", AgentInstanceRow.owner_id == owner, AgentInstanceRow.status.in_(("provisioning", "active", "suspended", "archived")))
                )
                link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + owner))
                space_id = link.space_id if link is not None else None
            if state.state not in ("off", "unavailable"):
                if space_id is None:
                    own = "not-assigned" if files.assignable_slots("user", unverified=app.state.agent_execution.unverified) else "no-seat"
                else:
                    own = "ready" if await files.binding_verified(space_id) else "unverified"
        storage = StorageSpacesFeature(enabled=True, backend="fixed-ext4", editor_concurrency="shared", user_spaces=files.user_spaces, state=state.state, reason=state.reason, own=own)
        accounts = AccountsFeature(seats=SeatCount(allowed=counts["user"].allowed, in_use=counts["user"].in_use))
        instances = AgentInstancesFeature(
            enabled=config.agents_api.enabled,
            execution=execution.execution,
            reason=execution.reason,
            ai_assistants=AssistantAllowance(per_user=getattr(config.agents_api, "ai_assistants_per_user", 1), allowed=counts["assistant"].allowed, in_use=counts["assistant"].in_use, mine=mine),
            ai_employees=SeatCount(allowed=counts["employee"].allowed, in_use=counts["employee"].in_use),
        )
        return storage, accounts, instances
    except Exception:
        logger.exception("Storage and agent availability could not be read")
        return StorageSpacesFeature(state="unavailable"), AccountsFeature(), AgentInstancesFeature()


def _ui_feature(ui: UiConfig, bundle: TenantBundle) -> UiFeature:
    """The workspace presentation: the bundle's starters where it has a usable list, else the config's."""
    starters = ui.starters if bundle.starters is None else bundle.starters
    return UiFeature(
        profile=ui.profile,
        starters=[UiStarter(id=starter.id, title=starter.title, prompt=starter.prompt) for starter in starters],
    )


def _knowledge_scope_selection_enabled(config: AppConfig) -> bool:
    """Fail closed unless the effective knowledge_search entry is RAGFlow."""
    settings = config.knowledge_base
    if not settings.enabled or not settings.scope_selection_enabled:
        return False
    tool = config.get_tool_config("knowledge_search")
    return tool is not None and tool.use == RAGFLOW_KNOWLEDGE_SEARCH_PROVIDER
