"""Direct instance Home writes with current Telegram custody and requester Use."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from sqlalchemy import and_, or_, select

from app.telegram_bots.authority import AuthorityUnavailable, require
from app.telegram_bots.links import BotLink
from app.telegram_bots.policy import POLICIES, pending_discard
from deerflow.agent_instances.agent_files import AgentFileRefusal, AgentFiles
from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.agent_instances.service import AgentInstances
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import Permission, PrincipalRef
from deerflow.utils.file_io import await_drained

_SCOPE = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")


@dataclass(frozen=True)
class _FileWrite:
    authority: object
    link: BotLink
    instance: object
    service: AgentInstances
    files: object
    writer: AgentFiles
    home_generation: int
    live_owner: object
    guard: object
    current: object

    def _current(self):
        if self.current() is not True or getattr(self.authority.app.state, "agent_instances", None) is not self.service or self.service.files is not self.files or self.service.agent_files is not self.writer:
            raise ValueError("instance-file-retired") from None

    async def _admit(self, writer, admitted):
        """Borrow the actual Home writer; lock instance before Telegram custody."""
        self._current()
        bind = self.authority.links.session_factory.kw.get("bind")
        if not writer.in_transaction() or not writer.is_active or writer.get_bind() is not getattr(bind, "sync_engine", bind):
            raise ValueError("instance-file-writer-required") from None
        home = admitted.get(self.instance.home_id)
        if home is None or home[0].generation != self.home_generation:
            raise ValueError("instance-file-home-changed") from None
        await writer.execute(select(AgentInstanceRow.id).where(AgentInstanceRow.id == self.instance.id).with_for_update())
        await self.guard.check(writer)
        await self._check_snapshot(writer)

    async def check_receipt(self, writer):
        """Read a saved path only under current target/Use/custody; no file effect."""
        self._current()
        bind = self.authority.links.session_factory.kw.get("bind")
        if not writer.in_transaction() or not writer.is_active or writer.get_bind() is not getattr(bind, "sync_engine", bind):
            raise ValueError("instance-file-writer-required") from None
        current = await self.guard.check(writer)
        await self._check_snapshot(writer)
        return current

    async def _check_snapshot(self, writer):
        bot, instance, grant, policy = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, TelegramInstancePolicyRow
        facts = (
            await writer.execute(
                select(bot, instance, grant.permissions, policy.policy, SpaceRow.generation, UserRow, DisabledIdentityRow)
                .join(instance, instance.id == bot.agent_ref)
                .join(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
                .join(SpaceRow, SpaceRow.id == instance.home_id)
                .join(UserRow, UserRow.id == bot.owner_user_id)
                .outerjoin(policy, policy.instance_id == instance.id)
                .outerjoin(DisabledIdentityRow, or_(DisabledIdentityRow.subject == UserRow.id, DisabledIdentityRow.subject == UserRow.oauth_id))
                .where(bot.id == self.link.id)
                .execution_options(populate_existing=True)
            )
        ).all()
        if not facts:
            raise AgentDenied("The file input is no longer available")
        actual, target, permissions, policy_value, home_generation, user, _ = facts[0]
        self.live_owner.check_snapshot(user, [row[-1] for row in facts])
        policy_value = "allowed" if target.custody == "personal" or policy_value is None else policy_value
        if (
            any(getattr(actual, key) != getattr(self.link, key) for key in _SCOPE)
            or actual.state != "connected"
            or actual.health != "ok"
            or target.status != "active"
            or target.custody not in {"personal", "company"}
            or (target.id, target.principal_id, target.home_id, target.generation, target.definition_revision)
            != (self.instance.id, self.instance.principal.subject_id, self.instance.home_id, self.instance.generation, self.instance.definition_revision)
            or home_generation != self.home_generation
            or type(permissions) is not int
            or not 1 <= permissions <= 7
            or not permissions & int(AgentPermission.USE)
            or policy_value not in POLICIES
            or policy_value == "off"
            or pending_discard(actual) is not None
        ):
            raise AgentDenied("The file input is no longer available")
        # No await follows the joined grant/policy/account decision.
        self._current()

    async def apply(self, *, operation_id, name, content, size):
        """Own the direct attributed write, including its buffer users, to settlement."""

        async def perform():
            self._current()
            from app.gateway.storage_spaces import require_execution

            await require(self.link.owner_user_id, "agents:read", "threads:read", "threads:write", "runs:create")
            self._current()
            await self.authority.instance_view(self.link)
            self._current()
            await require_execution(self.authority.app, self.instance.id)
            self._current()
            if type(name) is not str or not name or "/" in name or "\\" in name:
                raise AgentFileRefusal("invalid-name")
            return await self.writer.apply(
                instance=self.instance,
                requester=PrincipalRef("human", self.link.owner_user_id),
                operation_id=operation_id,
                action="put",
                path="uploads/" + name,
                content=content,
                size=size,
                admission=self._admit,
            )

        return await await_drained(perform())


async def prepare_file_write(authority, link, *, current):
    """Use before readiness/storage diagnostics; no conversation or resident startup."""

    async def perform():
        if type(link) is not BotLink or link.agent_kind != "instance" or not callable(current) or current() is not True:
            raise ValueError("instance-file-retired") from None
        service = getattr(authority.app.state, "agent_instances", None)
        if type(service) is not AgentInstances or service.directory.session_factory.kw.get("bind") is not authority.links.session_factory.kw.get("bind"):
            raise AuthorityUnavailable

        def check():
            if current() is not True or getattr(authority.app.state, "agent_instances", None) is not service:
                raise ValueError("instance-file-retired") from None

        instance = await authority.instance_view(link)
        check()
        if instance.status != "active":
            raise AgentFileRefusal("agent-not-active", name=instance.name)
        await require(link.owner_user_id, "agents:read", "threads:read", "threads:write", "runs:create")
        check()
        from app.gateway.storage_spaces import require_execution

        await require_execution(authority.app, instance.id)
        check()
        files, file_writer = service.files, service.agent_files
        if type(file_writer) is not AgentFiles or file_writer.agents is not service or files.registry._sf.kw.get("bind") is not authority.links.session_factory.kw.get("bind"):
            raise AuthorityUnavailable
        home = await files.registry.get(actor=instance.principal, space_id=instance.home_id, permission=Permission.READ)
        check()
        owner = await authority.prepare_live_owner(link.owner_user_id)
        check()
        guard = authority.links.publication_guard(link, check_live_owner=owner, check_attempt_current=current)
        result = _FileWrite(authority, deepcopy(link), instance, service, files, file_writer, home.generation, owner, guard, current)
        result._current()
        return result

    return await await_drained(perform())
