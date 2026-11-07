"""Qualify extension file APIs that do not yet have a managed storage adapter."""

from deerflow_extension_api import AgentRunError


async def require_legacy_artifact_scope(request, thread_id):
    """Refuse before any directory creation, sandbox acquisition or file I/O."""
    state = request.app.state
    if getattr(state, "storage_spaces_enabled", False) is True:
        raise AgentRunError(422, "Team artifact handoffs are unavailable for managed Spaces; use the conversation's file APIs")
    authority = getattr(state, "agent_conversations", None)
    if authority is not None and await authority.binding(thread_id) is not None:
        raise AgentRunError(422, "Team artifact handoffs are unavailable for instance Homes; use the Home Space")
