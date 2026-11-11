"""Prepared route-provider checks against the action writer's current account."""

import asyncio

from app.telegram_bots.authority import AccessOff, AuthorityUnavailable, CeilingRefused
from deerflow.utils.file_io import await_drained


async def prepare_work_owner(authority, owner_id):
    """Prepare config/provider outside SQL; never borrow a second account session."""
    from app.gateway.auth.mode import account_refusal
    from app.gateway.authz import _aget_cached_route_provider
    from app.gateway.deps import bind_user_repo
    from deerflow.authz.principal import build_principal_from_context
    from deerflow.authz.provider import AuthzDecision, AuthzRequest
    from deerflow.config.app_config import get_app_config, peek_loaded_app_config

    config = await await_drained(asyncio.to_thread(get_app_config))
    policy = config.authorization
    policy_basis = policy.model_dump(mode="json")
    provider = await await_drained(_aget_cached_route_provider(policy)) if policy.enabled else None
    if policy.enabled and provider is None:
        raise AuthorityUnavailable()
    users = bind_user_repo(authority.links.session_factory)
    engine = authority.links.session_factory.kw["bind"].sync_engine
    sign_on_only = config.auth.local.enabled is False

    def configuration_current():
        current = peek_loaded_app_config()
        return current is not None and current.authorization.model_dump(mode="json") == policy_basis and (current.auth.local.enabled is False) == sign_on_only and policy.model_dump(mode="json") == policy_basis

    def principal(user):
        return build_principal_from_context(
            {"user_id": str(user.id), "user_role": user.system_role, "oauth_provider": user.oauth_provider, "oauth_id": user.oauth_id, "is_internal": False},
            default_role=policy.default_role,
        )

    async def check(captured_owner, writer):
        if not configuration_current():
            raise AuthorityUnavailable()
        if captured_owner != owner_id or writer.get_bind() is not engine or not writer.in_transaction() or not writer.is_active:
            raise AuthorityUnavailable()
        user = await users.get_user_by_id(owner_id, session=writer)
        if user is None or account_refusal(user, sign_on_only_mode=sign_on_only) is not None:
            raise AccessOff()
        captured = principal(user)
        if provider is not None:
            for permission in ("agents:read", "agents:write"):
                try:
                    decision = await provider.aauthorize(AuthzRequest(principal=captured, resource="route", action=permission.split(":")[1], target=permission))
                    if not isinstance(decision, AuthzDecision) or type(decision.allow) is not bool:
                        raise AuthorityUnavailable()
                except (AccessOff, CeilingRefused):
                    raise
                except Exception:
                    raise AuthorityUnavailable() from None
                if not decision.allow:
                    raise CeilingRefused()
        # A provider may await. Re-read account/role-limit facts in the same
        # writer and reject a principal change instead of adopting its decision.
        current = await users.get_user_by_id(owner_id, session=writer)
        if current is None or account_refusal(current, sign_on_only_mode=sign_on_only) is not None:
            raise AccessOff()
        if principal(current) != captured or not configuration_current():
            raise AuthorityUnavailable()

    return check
