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


@dataclass(frozen=True)
class _PreparedLiveOwner:
    owner_user_id: str
    email: str
    sign_on_only_mode: bool
    _users: object
    _bind: object

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

    Only trusted fixed product/status text is supported in this slice. It reads
    no private thread/run/agent definition for delivery, so its required read
    ceilings are (). Model and file sources acquire their own checks later.
    """

    def __init__(self, *, links: BotLinkRepository, connections: ChannelConnectionRepository):
        self.links = links
        self.connections = connections

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

    @staticmethod
    async def _supported_target(link: BotLink) -> str:
        if link.agent_kind == "lead_agent" and link.agent_ref == "lead_agent":
            return "ok"
        if link.agent_kind == "custom_agent":
            from deerflow.config.agents_config import load_agent_config

            try:
                await asyncio.to_thread(load_agent_config, link.agent_ref, user_id=link.owner_user_id)
            except FileNotFoundError:
                return "agent_unavailable"
            return "ok"
        # Instance grants/protection are a later consumer; never infer permission.
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
        from app.telegram_bots.producer import DeliveryEligibility

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
            selected = [part] if part is not None else item.parts
            if not selected or any(not isinstance(value, dict) or set(value.get("source_ref", {})) != {"key", "values"} for value in selected):
                return DeliveryEligibility.wait()
            target = await self._supported_target(current)
            # The producer validates the reserved status allowlist and shape.
            if target == "agent_unavailable" and item.payload.get("delivery_kind") == "status_notice":
                return DeliveryEligibility.allow()
            if target != "ok":
                return DeliveryEligibility.wait()
            return DeliveryEligibility.allow()
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
            or link.agent_kind not in {"lead_agent", "custom_agent"}
        ):
            raise DeliveryClaimRetired
        return False


async def is_administrator(owner_user_id: str) -> bool:
    """Read the binding owner's effective administrator role at each use."""
    try:
        user = await get_local_provider().get_user(owner_user_id)
        return user is not None and user.system_role == "admin" and getattr(user, "role_limit", None) != "user"
    except Exception:
        # Missing or unreadable accounts confer no administrator presentation.
        return False
