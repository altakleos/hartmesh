"""Current binding, account, role and instance authority for complete Work reads."""

import asyncio

from sqlalchemy import and_, or_, select

from app.telegram_bots.attention_scope import scoped_projection
from app.telegram_bots.authority import AuthorityUnavailable, CeilingRefused, _PreparedLiveOwner
from app.telegram_bots.policy import POLICIES, pending_discard
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow
from deerflow.persistence.user.access import ROLES, DisabledIdentityRow, RoleLimitRow, issuer_key
from deerflow.persistence.user.model import UserRow

_SCOPE = ("id", "owner_user_id", "agent_kind", "agent_ref", "bot_id", "token_revision", "binding_generation", "connection_id", "telegram_user_id", "chat_id")


async def current_attention(host, captured, *, projection=None):
    check = await prepare_attention(host, captured)
    return await check(projection=projection)


async def prepare_attention(host, captured, *, require_inspect=True):
    """Prepare off-writer configuration, then permit a shared transaction read."""
    from app.gateway.authz import _aget_cached_route_provider, resolve_route_permissions
    from app.gateway.deps import bind_user_repo
    from deerflow.authz.principal import build_principal_from_context
    from deerflow.config.app_config import get_app_config, peek_loaded_app_config

    agents = host.work.agents
    engine = host.links.session_factory.kw.get("bind")
    if captured.agent_kind != "instance" or host.work._sf.kw.get("bind") is not engine:
        raise AgentDenied("Attention requires an instance binding")
    if getattr(host.authority.app.state, "agent_instances", None) is not agents:
        raise AuthorityUnavailable()
    config = await asyncio.to_thread(get_app_config)
    policy_basis = config.authorization.model_dump(mode="json")
    sign_on_only = config.auth.local.enabled is False
    try:
        provider = await _aget_cached_route_provider(config.authorization) if config.authorization.enabled else None
        if config.authorization.enabled and provider is None:
            raise AuthorityUnavailable()
    except Exception:
        raise AuthorityUnavailable() from None
    users = bind_user_repo(host.links.session_factory)
    # The joined snapshot supplies account facts. Capture its mode from the
    # same configuration as the provider, without a second config preparation.
    owner = _PreparedLiveOwner(captured.owner_user_id, "", sign_on_only, users, engine.sync_engine)
    bot, instance, grant, policy, connection = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, TelegramInstancePolicyRow, ChannelConnectionRow
    query = (
        select(bot, instance, grant.permissions, policy.policy, connection, UserRow, DisabledIdentityRow, RoleLimitRow)
        .join(instance, instance.id == bot.agent_ref)
        .join(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
        .outerjoin(policy, policy.instance_id == instance.id)
        .join(connection, connection.id == bot.connection_id)
        .join(UserRow, UserRow.id == bot.owner_user_id)
        .outerjoin(DisabledIdentityRow, or_(DisabledIdentityRow.subject == UserRow.id, DisabledIdentityRow.subject == UserRow.oauth_id))
        .outerjoin(RoleLimitRow, RoleLimitRow.subject == UserRow.oauth_id)
        .where(bot.id == captured.id)
    )

    base_query = query

    async def check(*, projection=None, session=None):
        projection = scoped_projection(projection)
        query = projection.extend(base_query) if projection is not None else base_query

        async def snapshot(held=None):
            if session is None:
                async with host.links.session_factory() as reader:
                    rows = (await reader.execute(query)).all()
            else:
                if session.get_bind() is not engine.sync_engine or not session.in_transaction() or not session.is_active:
                    raise AuthorityUnavailable()
                rows = (await session.execute(query.execution_options(populate_existing=True))).all()
            live = peek_loaded_app_config()
            if (
                live is None
                or live.authorization.model_dump(mode="json") != policy_basis
                or config.authorization.model_dump(mode="json") != policy_basis
                or (live.auth.local.enabled is False) != sign_on_only
                or getattr(host.authority.app.state, "agent_instances", None) is not agents
            ):
                raise AuthorityUnavailable()
            if not rows:
                raise AgentDenied("Attention is unavailable")
            current, target, permissions, value, bound, user = rows[0][:6]
            owner.check_snapshot(user, [row[6] for row in rows])
            value = "allowed" if target.custody == "personal" or value is None else value
            if (
                any(getattr(current, key) != getattr(captured, key) for key in _SCOPE)
                or current.state != "connected"
                or current.health != "ok"
                or target.status != "active"
                or target.custody not in {"personal", "company"}
                or type(permissions) is not int
                or not 1 <= permissions <= 7
                or permissions & (3 if require_inspect else 1) != (3 if require_inspect else 1)
                or value not in POLICIES
                or value == "off"
                or pending_discard(current) is not None
                or (bound.owner_user_id, bound.provider, bound.status, bound.external_account_id, bound.workspace_id, bound.bot_user_id)
                != (current.owner_user_id, "telegram", "connected", str(current.telegram_user_id), str(current.bot_id), str(current.bot_id))
            ):
                raise AgentDenied("Attention is unavailable")
            limits = [row[7].role for row in rows if row[7] is not None and (not user.oauth_issuer or row[7].issuer == issuer_key(user.oauth_issuer))] if user.oauth_id and user.oauth_provider else []
            limit = min(limits, key=lambda role: ROLES.index(role) if role in ROLES else -1) if limits else None
            if projection is not None:
                qualified = getattr(projection, "check_with_permissions", None)
                if qualified is None:
                    projection.check(rows)
                elif held is not None:
                    qualified(rows, held)
            return users._row_to_user(user, role_limit=limit), (permissions, value, target.generation, target.definition_revision)

        def principal(user):
            return build_principal_from_context(
                {"user_id": str(user.id), "user_role": user.system_role, "oauth_provider": user.oauth_provider, "oauth_id": user.oauth_id, "is_internal": False},
                default_role=config.authorization.default_role,
            )

        user, target_basis = await snapshot()
        basis = principal(user)
        try:
            held = frozenset(await resolve_route_permissions(user, is_internal=False, prepared_config=config.authorization, prepared_provider=provider, strict=True))
        except Exception:
            raise AuthorityUnavailable() from None
        # Provider evaluation may await. The final joined read covers account,
        # role limits, binding, grants and policy together after that preparation.
        current_user, current_target = await snapshot(held)
        if principal(current_user) != basis or current_target != target_basis:
            raise AuthorityUnavailable()
        if "agents:read" not in held:
            raise CeilingRefused()
        return held, target_basis

    return check
