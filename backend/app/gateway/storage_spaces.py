"""Host identity and startup wiring for mandatory, chat-independent storage."""

from pathlib import Path

from app.gateway.auth.mode import account_refusal
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.auth_disabled import AUTH_DISABLED_USER_ID, AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SESSION, is_auth_disabled
from app.gateway.internal_auth import get_trusted_internal_owner_user_id
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.service import AgentInstances
from deerflow.config.storage_spaces_config import StorageSpacesConfig
from deerflow.spaces.backings import PreparedVolumeCatalog
from deerflow.spaces.contract import PrincipalRef, ResolvedPrincipal
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import run_file_io


def supports_storage_credentials(request) -> bool:
    """Legacy route scope/default identity is never resource authentication."""
    source = getattr(getattr(request, "state", None), "auth_source", None)
    if source == AUTH_SOURCE_INTERNAL:
        return get_trusted_internal_owner_user_id(request) is not None
    return source in (AUTH_SOURCE_SESSION, AUTH_SOURCE_AUTH_DISABLED)


def human_lookup(repository):
    async def lookup(reference: PrincipalRef) -> ResolvedPrincipal | None:
        if reference.kind != "human":
            return None
        # The one synthetic identity is supported only by the explicit local
        # development adapter, never by missing request context or a missing row.
        if reference.subject_id == AUTH_DISABLED_USER_ID and is_auth_disabled():
            return ResolvedPrincipal(reference, can_provision_company=True)
        user = await repository.get_user_by_id(reference.subject_id)
        if user is None or str(user.id) != reference.subject_id or account_refusal(user) is not None:
            return None
        return ResolvedPrincipal(reference, can_provision_company=user.system_role == "admin" and getattr(user, "role_limit", None) != "user")

    return lookup


async def initialize_storage_spaces(app, config: StorageSpacesConfig, *, session_factory) -> None:
    app.state.storage_spaces_enabled = config.enabled
    app.state.storage_spaces = None
    app.state.agent_instances = None
    app.state.agent_conversations = AgentConversations(None, session_factory=session_factory) if session_factory is not None else None
    thread_store = getattr(app.state, "thread_store", None)
    if thread_store is not None and hasattr(thread_store, "instance_authority"):
        thread_store.instance_authority = app.state.agent_conversations
    if not config.enabled:
        return
    if session_factory is None:
        raise RuntimeError("Storage spaces require a durable database; memory mode is unsupported")
    catalog = await run_file_io(PreparedVolumeCatalog.from_manifest, Path(config.inventory_path))
    # Refuse startup before advertising a filesystem capability when any slot
    # or the platform reserve cannot establish the promised kernel facts.
    for slot_id in catalog.volumes:
        await run_file_io(catalog.verify, slot_id)
    if not catalog.volumes:
        raise RuntimeError("Storage spaces require at least one qualified provider volume")
    human = human_lookup(SQLiteUserRepository(session_factory))
    directory = InstanceDirectory(session_factory, human=human)
    registry = SpaceRegistry(session_factory, HostPrincipalResolver(human=human, nonhuman=directory.lookup))
    app.state.storage_spaces = SpaceFiles(registry, catalog)
    app.state.agent_instances = AgentInstances(app.state.storage_spaces, directory)
    app.state.agent_conversations = AgentConversations(app.state.agent_instances)
    if thread_store is not None and hasattr(thread_store, "instance_authority"):
        thread_store.instance_authority = app.state.agent_conversations
