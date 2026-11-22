"""Resolve durable item owners through the Gateway's current account store."""

import logging

from app.gateway.auth.mode import account_refusal
from app.gateway.auth_disabled import AUTH_DISABLED_USER_ID, get_auth_disabled_user, is_auth_disabled
from deerflow.authz.provider import Principal
from deerflow.config.paths import make_safe_user_id
from deerflow.persistence.user.access import ROLES, limited_role
from deerflow.subagents.batch_scope import is_ordinary_batch_spec, known_agent_batch_scope
from deerflow.utils.assembly_io import run_assembly

logger = logging.getLogger(__name__)


async def resolve_batch_principal(batch, *, load_user) -> Principal:
    owner = batch["user_id"]
    spec = batch["execution_spec"]
    scope = known_agent_batch_scope(spec)
    if scope is None and not is_ordinary_batch_spec(spec):
        raise PermissionError("Unrecognized batch session authority")
    internal = spec.get("is_internal") is True
    if scope is not None:
        admitted_role, oauth_provider, oauth_id = scope.system_role, scope.oauth_provider, scope.oauth_id
        development_owner = False
    else:
        admitted_role = spec.get("user_role")
        oauth_provider, oauth_id = spec.get("oauth_provider"), spec.get("oauth_id")
        development_owner = owner == AUTH_DISABLED_USER_ID and is_auth_disabled() and admitted_role == "admin" and oauth_provider is None and oauth_id is None
    if development_owner:
        user = get_auth_disabled_user()
    else:
        try:
            if load_user is None:
                raise RuntimeError("Account repository unavailable")
            user = await load_user(owner)
        except Exception as exc:
            # Database exception text can contain SQL parameters or credentials.
            logger.warning("Batch owner lookup failed (batch_id=%s, error_type=%s)", batch.get("id"), type(exc).__name__)
            raise PermissionError("Batch owner could not be verified") from None
    if user is None:
        # Gateway stamps no role on an unmapped, trusted internal identity.
        # A missing formerly mapped account must not take this fallback.
        channel_user_id = spec.get("channel_user_id")
        if internal and isinstance(channel_user_id, str) and channel_user_id and admitted_role is None and oauth_provider is None and oauth_id is None and owner == make_safe_user_id(channel_user_id):
            return Principal(user_id=owner, is_internal=True, channel_user_id=channel_user_id)
        raise PermissionError("Batch owner is no longer available")
    try:
        # HartMesh derives disabled/released access from the current identity
        # and account records. Live sign-on policy can also make local users
        # inert; reading it belongs off the dispatch loop.
        refusal = None if development_owner else await run_assembly(account_refusal, user)
    except Exception as exc:
        logger.warning("Batch owner policy lookup failed (batch_id=%s, error_type=%s)", batch.get("id"), type(exc).__name__)
        raise PermissionError("Batch owner could not be verified") from None
    # A session-bound job retains the exact originating login. Ordinary
    # background batches keep their previously admitted automation ceiling.
    if scope is not None and (user.token_version != scope.token_version or user.needs_setup or user.system_role != admitted_role):
        raise PermissionError("Batch session is no longer active with the saved identity")
    if str(user.id) != owner or refusal is not None:
        raise PermissionError("Batch owner is no longer active")
    if oauth_provider != user.oauth_provider or oauth_id != getattr(user, "oauth_id", None):
        raise PermissionError("Batch owner identity changed; submit a new batch with current permissions")
    if user.system_role != admitted_role:
        # Preserve HartMesh's existing automation ceiling on a known built-in
        # promotion. Custom roles remain unordered and demotions revoke work.
        if admitted_role not in ROLES or user.system_role not in ROLES or limited_role(admitted_role, user.system_role) != admitted_role:
            raise PermissionError("Batch owner role changed; submit a new batch with current permissions")
    return Principal(
        user_id=owner,
        role=admitted_role,
        oauth_provider=user.oauth_provider,
        oauth_id=getattr(user, "oauth_id", None),
        channel_user_id=spec.get("channel_user_id"),
        is_internal=internal,
        # Gateway admission does not mint ABAC attributes. Stored attributes
        # and OAuth claims are not a renewable source of authority.
    )
