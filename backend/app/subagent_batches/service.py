"""Gateway account authority for the harness-owned batch service."""

from types import SimpleNamespace

from app.gateway.auth.mode import account_refusal
from app.gateway.auth_disabled import AUTH_DISABLED_USER_ID, is_auth_disabled
from app.gateway.authz import resolve_route_permissions
from app.gateway.internal_auth import INTERNAL_SYSTEM_ROLE
from deerflow.config.paths import make_safe_user_id
from deerflow.persistence.user.access import limited_role
from deerflow.subagents.batch_service import SubagentBatchService
from deerflow.utils.assembly_io import run_assembly


async def batch_owner_allowed(batch: dict) -> bool:
    """Re-read the owner while retaining the batch's original authority.

    A demotion cancels old work rather than silently running it under a
    different role; a promotion never expands its saved authority. Missing
    external owners fail closed. Only the explicit development identity and
    the trusted, unbound channel shape may operate without an account row.
    """
    from app.gateway.deps import get_local_provider

    user_id = batch["user_id"]
    spec = batch["execution_spec"]
    role = spec.get("user_role")
    oauth_provider, oauth_id = spec.get("oauth_provider"), spec.get("oauth_id")
    internal = spec.get("is_internal") is True
    development = is_auth_disabled() and user_id == AUTH_DISABLED_USER_ID and role == "admin" and not oauth_provider and not oauth_id
    if not development:
        owner = await get_local_provider().get_user(user_id)
        if owner is not None:
            if await run_assembly(account_refusal, owner):
                return False
            if role not in {"admin", "user"} or limited_role(role, owner.system_role) != role:
                return False
            if oauth_provider != owner.oauth_provider or oauth_id != owner.oauth_id:
                return False
        else:
            channel_user_id = spec.get("channel_user_id")
            if not (internal and isinstance(channel_user_id, str) and channel_user_id and role is None and oauth_provider is None and oauth_id is None and user_id == make_safe_user_id(channel_user_id)):
                return False
    principal_user = SimpleNamespace(id=user_id, system_role=role or INTERNAL_SYSTEM_ROLE, oauth_provider=oauth_provider, oauth_id=oauth_id)
    return "runs:create" in await resolve_route_permissions(principal_user, is_internal=internal)


__all__ = ["SubagentBatchService", "batch_owner_allowed"]
