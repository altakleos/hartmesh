"""Direct Home exports as the current human recipient, without resident access."""

from copy import deepcopy
from dataclasses import dataclass
from pathlib import PurePosixPath

from sqlalchemy import and_, or_, select

from app.telegram_bots.authority import AuthorityUnavailable, require
from app.telegram_bots.instance_file_authority import _SCOPE
from app.telegram_bots.links import BotLink
from app.telegram_bots.policy import POLICIES, pending_discard
from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.agent_instances.service import AgentInstances
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramInstancePolicyRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import Permission, PrincipalRef, SpaceDenied
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import await_drained


def _relative_output(path):
    if (
        type(path) is not str
        or not path.startswith("/mnt/user-data/outputs/")
        or str(PurePosixPath(path)) != path
        or "\\" in path
        or any(part in {".", ".."} for part in PurePosixPath(path).parts)
        or any(ord(char) < 32 or ord(char) == 127 for char in path)
    ):
        raise ValueError("instance-output-path-unavailable") from None
    return path[len("/mnt/user-data/") :]


@dataclass(frozen=True)
class _FileExport:
    authority: object
    link: BotLink
    instance: object
    service: AgentInstances
    files: SpaceFiles
    home_generation: int
    live_owner: object
    guard: object
    current: object

    def _current(self):
        state = self.authority.app.state
        if self.current() is not True or state.agent_instances is not self.service or state.storage_spaces is not self.files or self.service.files is not self.files:
            raise ValueError("instance-export-retired") from None

    async def _admit(self, writer, admitted):
        self._current()
        bind = self.authority.links.session_factory.kw.get("bind")
        if not writer.in_transaction() or not writer.is_active or writer.get_bind() is not getattr(bind, "sync_engine", bind):
            raise ValueError("instance-export-writer-required") from None
        home = admitted.get(self.instance.home_id)
        if home is None or home[0].generation != self.home_generation:
            raise SpaceDenied("The original file Home is no longer available")
        await self.guard.check(writer)
        bot, instance, grant, policy = TelegramBotLinkRow, AgentInstanceRow, AgentInstanceGrantRow, TelegramInstancePolicyRow
        facts = (
            await writer.execute(
                select(bot, instance, grant.permissions, policy.policy, SpaceRow.generation, SpaceRow.status, SpaceGrantRow.permissions, UserRow, DisabledIdentityRow)
                .join(instance, instance.id == bot.agent_ref)
                .join(grant, and_(grant.instance_id == instance.id, grant.user_id == bot.owner_user_id))
                .join(SpaceRow, SpaceRow.id == instance.home_id)
                .join(SpaceGrantRow, and_(SpaceGrantRow.space_id == instance.home_id, SpaceGrantRow.principal_kind == "human", SpaceGrantRow.subject_id == bot.owner_user_id))
                .join(UserRow, UserRow.id == bot.owner_user_id)
                .outerjoin(policy, policy.instance_id == instance.id)
                .outerjoin(DisabledIdentityRow, or_(DisabledIdentityRow.subject == UserRow.id, DisabledIdentityRow.subject == UserRow.oauth_id))
                .where(bot.id == self.link.id)
                .execution_options(populate_existing=True)
            )
        ).all()
        if not facts:
            raise SpaceDenied("The file is no longer available")
        actual, target, permissions, value, home_generation, home_status, file_permissions, user, _ = facts[0]
        self.live_owner.check_snapshot(user, [row[-1] for row in facts])
        value = "allowed" if target.custody == "personal" or value is None else value
        if (
            any(getattr(actual, key) != getattr(self.link, key) for key in _SCOPE)
            or actual.state != "connected"
            or actual.health != "ok"
            or target.status != "active"
            or target.custody not in {"personal", "company"}
            or (target.id, target.principal_id, target.home_id, target.generation, target.definition_revision)
            != (self.instance.id, self.instance.principal.subject_id, self.instance.home_id, self.instance.generation, self.instance.definition_revision)
            or type(permissions) is not int
            or not 1 <= permissions <= 7
            or permissions & 3 != 3
            or value not in POLICIES
            or value == "off"
            or pending_discard(actual) is not None
        ):
            raise AgentDenied("The current file recipient is unavailable")
        if home_status != "active" or home_generation != self.home_generation or type(file_permissions) is not int or file_permissions & int(Permission.READ | Permission.EXPORT) != int(Permission.READ | Permission.EXPORT):
            raise SpaceDenied("The original file Home is no longer available")
        self._current()

    async def authorize(self):
        async def perform():
            fresh = await prepare_file_export(self.authority, self.link, current=self.current)
            if (
                fresh.service is not self.service
                or fresh.files is not self.files
                or (fresh.instance.id, fresh.instance.home_id, fresh.instance.generation, fresh.instance.definition_revision, fresh.home_generation)
                != (self.instance.id, self.instance.home_id, self.instance.generation, self.instance.definition_revision, self.home_generation)
            ):
                raise SpaceDenied("The original file Home is no longer available")
            self._current()
            async with self.files.registry.admitted(actor=PrincipalRef("human", self.link.owner_user_id), requests={self.instance.home_id: (Permission.READ | Permission.EXPORT, self.home_generation)}) as (writer, admitted):
                await self._admit(writer, admitted)

        return await await_drained(perform())

    async def read(self, path, *, max_bytes):
        relative = _relative_output(path)
        if type(max_bytes) is not int or not 0 < max_bytes <= 50_000_000:
            raise ValueError("instance-export-limit-unavailable") from None

        async def perform():
            await self.authorize()
            return await self.files.export_file(actor=PrincipalRef("human", self.link.owner_user_id), space_id=self.instance.home_id, path=relative, max_bytes=max_bytes, controller_admission=self._admit)

        return await await_drained(perform())


async def prepare_file_export(authority, link, *, current):
    async def perform():
        if type(link) is not BotLink or link.agent_kind != "instance" or not callable(current) or current() is not True:
            raise ValueError("instance-export-retired") from None
        service = getattr(authority.app.state, "agent_instances", None)
        files = getattr(authority.app.state, "storage_spaces", None)
        bind = authority.links.session_factory.kw.get("bind")
        if type(service) is not AgentInstances or type(files) is not SpaceFiles or service.files is not files or service.directory.session_factory.kw.get("bind") is not bind or files.registry._sf.kw.get("bind") is not bind:
            raise AuthorityUnavailable

        def check():
            if current() is not True or authority.app.state.agent_instances is not service or authority.app.state.storage_spaces is not files or service.files is not files:
                raise ValueError("instance-export-retired") from None

        instance = await authority.instance_view(link)
        check()
        if instance.status != "active" or not instance.permissions & AgentPermission.INSPECT:
            raise AgentDenied("The current file recipient is unavailable")
        await require(link.owner_user_id, "agents:read", "threads:read", "runs:read")
        check()
        home = await files.registry.get(actor=PrincipalRef("human", link.owner_user_id), space_id=instance.home_id, permission=Permission.READ | Permission.EXPORT)
        check()
        owner = await authority.prepare_live_owner(link.owner_user_id)
        check()
        guard = authority.links.publication_guard(link, check_live_owner=owner, check_attempt_current=current)
        return _FileExport(authority, deepcopy(link), instance, service, files, home.generation, owner, guard, current)

    return await await_drained(perform())
