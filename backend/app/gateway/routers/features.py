"""Read-only feature-flag endpoint for the frontend bootstrap.

Reports which optional features are exposed over HTTP so the frontend can gate
UI and avoid firing requests that the backend would reject. Config-only flags
read through ``get_config`` so edits to ``config.yaml`` take effect on the next
request, while startup-scoped capabilities report the runtime that actually
started.
"""

import asyncio
from typing import Literal

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from app.gateway.browser_capability import browser_capability
from app.gateway.conversation_access import conversation_references_enabled
from app.gateway.customer_administration import request_customer_administration_policy, resolve_customer_management_actor
from app.gateway.deps import get_config
from app.gateway.knowledge_scope_admission import RAGFLOW_KNOWLEDGE_SEARCH_PROVIDER
from app.gateway.run_models import MAX_CONVERSATION_REFERENCES
from deerflow.config.app_config import AppConfig
from deerflow.config.tenant_bundle import MAX_COMPANY_NAME_CHARS, MAX_SUPPORT_URL_CHARS, TenantBundle, configured_tenant_bundle
from deerflow.config.ui_config import MAX_STARTER_PROMPT_CHARS, MAX_STARTER_TITLE_CHARS, MAX_STARTERS, UiConfig
from deerflow.subagents.capacity import configured_subagent_max_running

router = APIRouter(prefix="/api", tags=["features"])


class AgentsApiFeature(BaseModel):
    """Availability of the custom-agent management API."""

    enabled: bool = Field(..., description="Whether the agents_api routes are exposed over HTTP")


class StorageSpacesFeature(BaseModel):
    enabled: bool = False
    backend: str | None = None
    native_attachments: bool = False
    editor_concurrency: str | None = None


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
    agents_api: AgentsApiFeature
    browser_control: BrowserControlFeature
    mcp_tasks: McpTasksFeature
    subagent_batches: SubagentBatchesFeature
    conversation_references: ConversationReferencesFeature
    knowledge_base: KnowledgeBaseFeature
    ui: UiFeature
    branding: BrandingFeature


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
    return FeaturesResponse(
        storage_spaces=StorageSpacesFeature(
            enabled=getattr(request.app.state, "storage_spaces", None) is not None,
            backend="fixed-ext4" if getattr(request.app.state, "storage_spaces", None) is not None else None,
            editor_concurrency="exclusive-host-window" if getattr(request.app.state, "storage_spaces", None) is not None else None,
        ),
        customer_administration=await _customer_administration_feature(request, config),
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
