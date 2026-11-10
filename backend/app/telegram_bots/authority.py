"""Current human account, binding and target authority for managed Telegram."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.telegram_bots.adapter import SendContext
    from app.telegram_bots.links import BotLink, BotLinkRepository
    from app.telegram_bots.producer import DeliveryEligibility, OutboxItem
    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository


class AccessOff(Exception):
    """The human account is missing, disabled, released or inert."""


class CeilingRefused(Exception):
    reason = "ceiling"


class AuthorityUnavailable(Exception):
    """Authority cannot currently be read; saved delivery must wait."""


class LiveOwnerCheck(Protocol):
    @property
    def email(self) -> str: ...

    async def __call__(self, owner_user_id: str, writer: AsyncSession) -> None: ...

    def check_snapshot(self, user, disabled_identities) -> None: ...


@dataclass(frozen=True)
class _PreparedLiveOwner:
    owner_user_id: str
    email: str
    sign_on_only_mode: bool
    _users: object
    _bind: object

    def check_snapshot(self, user, disabled_identities) -> None:
        """Validate account facts returned by the caller's one final SQL read.

        Match the auth repository's local/legacy-provider identity semantics;
        an unrelated provider's identical subject cannot disable a local user.
        No I/O follows the caller's grant, policy and account snapshot.
        """
        from app.gateway.auth.mode import account_refusal
        from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
        from deerflow.persistence.user.access import issuer_key, local_account

        if user is None or user.id != self.owner_user_id:
            raise AccessOff
        disabled_at = None
        for fact in disabled_identities:
            if fact is None:
                continue
            if local_account(user):
                matches = fact.issuer == LOCAL_ACCOUNT_ISSUER and fact.subject == user.id
            else:
                matches = fact.issuer != LOCAL_ACCOUNT_ISSUER and fact.subject == user.oauth_id and (not user.oauth_issuer or fact.issuer == issuer_key(user.oauth_issuer))
            if matches:
                disabled_at = fact.disabled_at
                break
        if account_refusal(self._users._row_to_user(user, disabled_at), sign_on_only_mode=self.sign_on_only_mode) is not None:
            raise AccessOff

    async def __call__(self, owner_user_id: str, writer: AsyncSession) -> None:
        from app.gateway.auth.mode import account_refusal

        if owner_user_id != self.owner_user_id:
            raise AccessOff
        try:
            if not writer.in_transaction() or not writer.is_active or writer.get_bind() is not self._bind:
                raise AuthorityUnavailable
            # The writer retains its SQL ownership through caller cancellation;
            # it may only roll back after this borrowed cursor has finished.
            user = await await_drained(self._users.get_user_by_id(owner_user_id, session=writer))
            if user is None or str(user.id) != owner_user_id or account_refusal(user, sign_on_only_mode=self.sign_on_only_mode) is not None:
                raise AccessOff
        except AccessOff:
            raise
        except Exception:
            raise AuthorityUnavailable from None


def get_local_provider():
    # Keep catalog and operator imports independent of Gateway startup.
    from app.gateway.deps import get_local_provider as provider

    return provider()


async def ceilings(owner_user_id: str, *, prepared_config: AppConfig | None = None) -> frozenset[str]:
    """Read the live human's route ceilings without a permissive error fallback."""
    from app.gateway.auth.mode import account_refusal
    from app.gateway.authz import resolve_route_permissions
    from deerflow.config.app_config import get_app_config

    try:
        user = await get_local_provider().get_user(owner_user_id)
        if user is None or str(user.id) != owner_user_id or getattr(user, "disabled_at", None) is not None or getattr(user, "released_at", None) is not None:
            raise AccessOff
        config = prepared_config if prepared_config is not None else await await_drained(asyncio.to_thread(get_app_config))
        if account_refusal(user, sign_on_only_mode=config.auth.local.enabled is False) is not None:
            raise AccessOff
        return frozenset(await resolve_route_permissions(user, is_internal=False, prepared_config=config.authorization, strict=True))
    except AccessOff:
        raise
    except Exception:
        raise AuthorityUnavailable from None


async def require(owner_user_id: str, *needed: str, prepared_config: AppConfig | None = None) -> frozenset[str]:
    permissions = await ceilings(owner_user_id, prepared_config=prepared_config)
    if not set(needed).issubset(permissions):
        raise CeilingRefused
    return permissions


class TelegramAuthority:
    """One collaborator shared by claim, part and final raw-dispatch checks.

    Fixed product/status text reads no private thread/run/agent definition, so its
    required read ceilings are (). A model source also needs `threads:read` and
    `runs:read`, and `delivery` validates its basis.
    """

    def __init__(self, *, links: BotLinkRepository, connections: ChannelConnectionRepository, app=None):
        self.links = links
        self.connections = connections
        self.app = app

    async def instance_identity(self, link: BotLink):
        """Host lifecycle metadata; this does not grant access to private data."""
        from deerflow.agent_instances.contract import InstanceIdentity
        from deerflow.persistence.agent_instances.model import AgentInstanceRow

        async def read():
            async with self.links.session_factory() as session:
                row = await session.get(AgentInstanceRow, link.agent_ref)
                if row is None:
                    raise AuthorityUnavailable
                return InstanceIdentity.from_row(row)

        return await await_drained(read())

    async def instance_policy(self, link: BotLink) -> str:
        from app.telegram_bots.policy import InstancePolicy

        if link.agent_kind != "instance":
            return "allowed"
        identity = await self.instance_identity(link)
        return await InstancePolicy(self.links.session_factory).get(identity.id) if identity.custody == "company" else "allowed"

    async def instance_view(self, link: BotLink):
        from deerflow.agent_instances.contract import AgentPermission
        from deerflow.spaces.contract import PrincipalRef

        await require(link.owner_user_id, "agents:read")
        agents = getattr(getattr(self.app, "state", None), "agent_instances", None)
        if agents is None:
            raise AuthorityUnavailable
        return await agents.get(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, permission=AgentPermission.USE)

    async def instance_send_policy(self, link: BotLink, *, require_use: bool, bound: bool = True, require_inspect: bool = False) -> str:
        """Final point-in-time grant, target, policy and discard decision.

        Live host authority is checked first. The final joined SQL statement
        observes all mutable send facts together; no link refresh or separate
        policy/grant read can carry an earlier decision past a newer one.
        """
        from sqlalchemy import and_, select

        from app.telegram_bots.policy import POLICIES, pending_discard
        from deerflow.agent_instances.contract import AgentDenied, AgentPermission
        from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
        from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow

        agents = getattr(getattr(self.app, "state", None), "agent_instances", None)
        if require_use:
            await self.instance_view(link)
            if agents.directory.session_factory.kw.get("bind") is not self.links.session_factory.kw.get("bind"):
                raise AuthorityUnavailable

        async def read():
            async with self.links.session_factory() as session:
                bot, instance, grant, policy = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, TelegramInstancePolicyRow
                result = (
                    await session.execute(
                        select(bot, instance.custody, instance.status, grant.permissions, policy.policy)
                        .join(instance, instance.id == bot.agent_ref)
                        .outerjoin(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
                        .outerjoin(policy, policy.instance_id == instance.id)
                        .where(bot.id == link.id)
                    )
                ).one_or_none()
                if result is None:
                    raise AuthorityUnavailable
                current, custody, status, permissions, value = result
                basis = ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "token_revision", "binding_generation", "connection_id", "chat_id", "telegram_user_id")
                if any(getattr(current, key) != getattr(link, key) for key in basis) or custody not in {"personal", "company"} or bound and current.state != "connected":
                    raise AuthorityUnavailable
                value = "allowed" if custody == "personal" or value is None else value
                if value not in POLICIES:
                    raise AuthorityUnavailable
                if require_use and (not permissions or not permissions & int(AgentPermission.USE) or status != "active" or pending_discard(current) is not None):
                    raise AgentDenied("The current Telegram send is unavailable")
                if require_inspect and (not permissions or not permissions & int(AgentPermission.INSPECT)):
                    raise AgentDenied("The current Telegram presentation is unavailable")
                if require_use and getattr(getattr(self.app, "state", None), "agent_instances", None) is not agents:
                    raise AuthorityUnavailable
                if pending_discard(current) is not None and value != "off":
                    return "restoring"
                return value

        return await await_drained(read())

    async def admit_instance(self, link: BotLink):
        """The Gateway order: current Use, execution readiness, resident admission."""
        from app.gateway.storage_spaces import require_execution
        from deerflow.agent_instances.contract import AgentDenied

        view = await self.instance_view(link)
        if view.status != "active":
            raise AgentDenied("The agent is not active")
        await require_execution(self.app, link.agent_ref)
        residents = getattr(self.app.state.agent_instances, "residents", None)
        if residents is not None:
            await residents.admit(view, work=False)
        return view

    async def prepare_live_owner(self, owner_user_id: str) -> LiveOwnerCheck:
        """Prepare strict policy off-writer, then recheck the human in its writer.

        The email is presentation only. No saved preflight grants authority to
        a missing/disabled/released/inert account at the later SQL check.
        """
        from app.gateway.auth.mode import account_refusal
        from app.gateway.deps import bind_user_repo
        from deerflow.config.app_config import get_app_config

        try:
            config = await await_drained(asyncio.to_thread(get_app_config))
            await await_drained(require(owner_user_id, prepared_config=config))
            sessions = self.links.session_factory
            users = bind_user_repo(sessions)
            user = await await_drained(users.get_user_by_id(owner_user_id))
            mode = config.auth.local.enabled is False
            if user is None or str(user.id) != owner_user_id or account_refusal(user, sign_on_only_mode=mode) is not None:
                raise AccessOff
            bind = sessions.kw.get("bind")
            if bind is None:
                raise AuthorityUnavailable
            return _PreparedLiveOwner(owner_user_id, str(user.email), mode, users, getattr(bind, "sync_engine", bind))
        except AccessOff:
            raise
        except Exception:
            raise AuthorityUnavailable from None

    async def _connection_matches(self, link: BotLink, *, states=("connected",)) -> bool:
        if self.connections is None or link.state not in states or link.chat_id is None or link.telegram_user_id is None or link.connection_id is None:
            return False
        connection = await self.connections.find_connection_by_external_identity(provider="telegram", external_account_id=str(link.telegram_user_id), workspace_id=str(link.bot_id))
        return bool(
            connection
            and connection["id"] == link.connection_id
            and connection["owner_user_id"] == link.owner_user_id
            and connection["provider"] == "telegram"
            and connection["status"] == "connected"
            and connection["external_account_id"] == str(link.telegram_user_id)
            and connection["workspace_id"] == str(link.bot_id)
            and connection["bot_user_id"] == str(link.bot_id)
        )

    async def _supported_target(self, link: BotLink) -> str:
        if link.agent_kind == "lead_agent" and link.agent_ref == "lead_agent":
            return "ok"
        if link.agent_kind == "custom_agent":
            from deerflow.config.agents_config import load_agent_config

            try:
                await asyncio.to_thread(load_agent_config, link.agent_ref, user_id=link.owner_user_id)
            except FileNotFoundError:
                return "agent_unavailable"
            return "ok"
        if link.agent_kind == "instance":
            from deerflow.agent_instances.contract import AgentDenied

            if await self.instance_policy(link) == "off":
                return "policy_off"
            try:
                view = await self.instance_view(link)
            except AgentDenied:
                return "agent_unavailable"
            from app.telegram_bots.policy import pending_discard

            if pending_discard(link) is not None:
                return "policy_restoring"
            return "ok" if view.status == "active" else "agent_unavailable"
        return "unsupported"

    async def target(self, link: BotLink) -> str:
        """Current startup/reconcile target state; uncertainty raises, never holds."""
        try:
            await require(link.owner_user_id)
            if link.state in {"held", "connected", "blocked"} and not await self._connection_matches(link, states=("held", "connected", "blocked")):
                return "held"
            return await self._supported_target(link)
        except AccessOff:
            return "held"
        except AuthorityUnavailable:
            raise
        except Exception:
            raise AuthorityUnavailable from None

    async def delivery(self, link: BotLink, item: OutboxItem, part: dict | None = None) -> DeliveryEligibility:
        from app.telegram_bots.producer import DeliveryEligibility, uses_model_source

        try:
            await require(link.owner_user_id)
            current = await self.links.get(link.id)
            fields = ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "token_revision", "binding_generation", "chat_id", "telegram_user_id", "connection_id")
            if (
                current is None
                or any(getattr(current, field) != getattr(link, field) for field in fields)
                or item.link_id != link.id
                or item.owner_user_id != link.owner_user_id
                or item.binding_generation != link.binding_generation
                or item.chat_id != link.chat_id
                or not await self._connection_matches(current)
            ):
                return DeliveryEligibility.skip("link-not-connected")
            policy = await self.instance_policy(current)
            if policy == "off":
                return DeliveryEligibility.skip("policy-off")

            async def allowed():
                role_visible = any(value.get("source_ref", {}).get("key") in {"hello.assistant", "hello.employee", "help.agent"} and value.get("source_ref", {}).get("values", {}).get("role") for value in selected)
                value = await self.instance_send_policy(current, require_use=target == "ok", require_inspect=bool(role_visible)) if current.agent_kind == "instance" else "allowed"
                if value == "off":
                    return DeliveryEligibility.skip("policy-off")
                if value == "restoring":
                    return DeliveryEligibility.wait()
                return DeliveryEligibility.allow(protect_content=value == "protected")

            selected = [part] if part is not None else item.parts
            if uses_model_source(item) or not selected or any(not isinstance(value, dict) or set(value.get("source_ref", {})) != {"key", "values"} for value in selected):

                def validate_model():
                    from app.telegram_bots.model_source import validate_model_reply_basis

                    return validate_model_reply_basis(item, part)

                proof = await await_drained(asyncio.to_thread(validate_model))
                scope = proof.record.scope
                if current.id != scope.link_id or any(
                    getattr(current, field) != getattr(scope, field) for field in ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "connection_id", "telegram_user_id", "chat_id", "binding_generation")
                ):
                    return DeliveryEligibility.wait()
                await require(link.owner_user_id, "threads:read", "runs:read")
                target = await self._supported_target(current)
                if target != "ok":
                    return DeliveryEligibility.wait()
                return await allowed()
            if type(item.payload.get("reply_basis")) is dict and item.payload["reply_basis"].get("version") == 3:
                from app.telegram_bots.producer import validate_restart_reply_basis

                proof = await await_drained(asyncio.to_thread(validate_restart_reply_basis, item, part))
                scope = proof.record.scope
                if current.id != scope.link_id or any(
                    getattr(current, field) != getattr(scope, field) for field in ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "connection_id", "telegram_user_id", "chat_id", "binding_generation")
                ):
                    return DeliveryEligibility.wait()
            target = await self._supported_target(current)
            # The producer validates the reserved status allowlist and shape.
            if target == "agent_unavailable" and item.payload.get("delivery_kind") == "status_notice":
                return await allowed()
            if target != "ok":
                return DeliveryEligibility.wait()
            return await allowed()
        except AccessOff:
            return DeliveryEligibility.skip("disabled")
        except CeilingRefused:
            return DeliveryEligibility.skip("no-access")
        except Exception:
            return DeliveryEligibility.wait()

    async def protect_notice(self, context: SendContext) -> bool:
        """Select fixed-notice protection without preventing access.off notices.

        The caller authorizes the lifecycle transition separately. No identity,
        generation or captured bound destination may be adopted from a successor.
        """
        from app.telegram_bots.producer import DeliveryClaimRetired

        link = await self.links.get(context.link_id)
        if (
            link is None
            or link.token_revision != context.token_revision
            or link.binding_generation != context.binding_generation
            or context.purpose == "bound_message"
            and link.chat_id != context.chat_id
            or link.agent_kind not in {"lead_agent", "custom_agent", "instance"}
        ):
            raise DeliveryClaimRetired
        if link.agent_kind != "instance":
            return False
        try:
            policy = await self.instance_send_policy(link, require_use=context.purpose == "bound_message", bound=context.purpose == "bound_message")
            if context.expected_policy is not None and policy != context.expected_policy:
                raise DeliveryClaimRetired
            if context.purpose == "bound_message" and policy in {"off", "restoring"}:
                raise DeliveryClaimRetired
            # Optional lifecycle notices remain possible after Use/account
            # withdrawal. Off or restoration never weakens their protection.
            return policy in {"protected", "off", "restoring"}
        except Exception:
            raise DeliveryClaimRetired from None


async def is_administrator(owner_user_id: str) -> bool:
    """Read the binding owner's effective administrator role at each use."""
    try:
        user = await get_local_provider().get_user(owner_user_id)
        return user is not None and user.system_role == "admin" and getattr(user, "role_limit", None) != "user"
    except Exception:
        # Missing or unreadable accounts confer no administrator presentation.
        return False


async def read_by_others(owner_user_id: str, view, *, session_factory) -> bool:
    """Privacy presentation from a current authorized view, never a new grant."""
    from sqlalchemy import select

    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    if not view.permissions & AgentPermission.MANAGE:
        return True

    async def read():
        async with session_factory() as session:
            grant = AgentInstanceGrantRow
            return bool(await session.scalar(select(select(grant.user_id).where(grant.instance_id == view.id, grant.user_id != owner_user_id, grant.permissions.bitwise_and(int(AgentPermission.INSPECT)) != 0).exists())))

    return await await_drained(read())
