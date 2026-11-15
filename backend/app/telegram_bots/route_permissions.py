"""Prepared route decisions fenced by a joined account and role snapshot."""

import asyncio
from dataclasses import dataclass

from app.telegram_bots.authority import AccessOff, AuthorityUnavailable
from deerflow.persistence.user.access import ROLES, issuer_key


@dataclass(frozen=True)
class RouteDecision:
    policy: object
    principal: object
    held: frozenset

    def check_snapshot(self, user, limits):
        self.policy.check_configuration()
        if self.policy.principal(self.policy.row_user(user, limits)) != self.principal:
            raise AuthorityUnavailable()


@dataclass(frozen=True)
class PreparedRoutes:
    config: object
    basis: dict
    provider: object
    users: object
    sign_on_only: bool

    def check_configuration(self):
        from deerflow.config.app_config import peek_loaded_app_config

        live = peek_loaded_app_config()
        if live is None or live.authorization.model_dump(mode="json") != self.basis or self.config.authorization.model_dump(mode="json") != self.basis or (live.auth.local.enabled is False) != self.sign_on_only:
            raise AuthorityUnavailable()

    def principal(self, user):
        from deerflow.authz.principal import build_principal_from_context

        if user is None:
            raise AccessOff()
        return build_principal_from_context(
            {"user_id": str(user.id), "user_role": user.system_role, "oauth_provider": user.oauth_provider, "oauth_id": user.oauth_id, "is_internal": False}, default_role=self.config.authorization.default_role
        )

    def row_user(self, user, limits):
        if user is None:
            raise AccessOff()
        roles = [row.role for row in limits if row is not None and (not user.oauth_issuer or row.issuer == issuer_key(user.oauth_issuer))] if user.oauth_id and user.oauth_provider else []
        limit = min(roles, key=lambda role: ROLES.index(role) if role in ROLES else -1) if roles else None
        return self.users._row_to_user(user, role_limit=limit)

    async def evaluate(self, user):
        from app.gateway.authz import resolve_route_permissions

        self.check_configuration()
        principal = self.principal(user)
        try:
            held = frozenset(await resolve_route_permissions(user, is_internal=False, prepared_config=self.config.authorization, prepared_provider=self.provider, strict=True))
        except Exception:
            raise AuthorityUnavailable() from None
        self.check_configuration()
        return RouteDecision(self, principal, held)


async def prepare_routes(sessions):
    from app.gateway.authz import _aget_cached_route_provider
    from app.gateway.deps import bind_user_repo
    from deerflow.config.app_config import get_app_config

    config = await asyncio.to_thread(get_app_config)
    basis = config.authorization.model_dump(mode="json")
    try:
        provider = await _aget_cached_route_provider(config.authorization) if config.authorization.enabled else None
        if config.authorization.enabled and provider is None:
            raise AuthorityUnavailable()
    except Exception:
        raise AuthorityUnavailable() from None
    return PreparedRoutes(config, basis, provider, bind_user_repo(sessions), config.auth.local.enabled is False)
