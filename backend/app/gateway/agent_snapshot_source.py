"""Human authority for publishing one bound conversation and its presented Home files."""

from dataclasses import dataclass

from fastapi import HTTPException
from sqlalchemy import select

from app.gateway.agent_conversations import _authority, inspection_for_read
from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.conversation_file_home import export_presented_output
from app.gateway.deps import get_user_repo
from app.gateway.snapshot_manifest import SnapshotAgent
from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.agent_instances.conversations import AgentConversations, AgentExecution
from deerflow.agent_instances.host_operations import human
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow
from deerflow.spaces.contract import Permission, PrincipalRef, SpaceDenied

_READ_EXPORT = Permission.READ | Permission.EXPORT


def unavailable():
    return HTTPException(404, {"code": "conversation-unavailable"})


@dataclass(frozen=True)
class AgentSnapshotSource:
    inspection: AgentExecution

    @classmethod
    async def for_thread(cls, request, thread_id):
        authority = _authority(request)
        if not isinstance(authority, AgentConversations) or await authority.binding(thread_id) is None:
            return None
        if getattr(request.state, "auth_source", None) != AUTH_SOURCE_SESSION:
            raise unavailable()
        try:
            inspection = await inspection_for_read(request, thread_id)
            if inspection is None:
                raise unavailable()
            result = cls(inspection)
            await result.recheck()
            return result
        except (AgentDenied, SpaceDenied) as exc:
            raise unavailable() from exc

    @property
    def scope(self):
        i = self.inspection
        return ("agent", i.instance.id, i.thread_incarnation, i.instance.home_id, i.instance.generation, i.home_generation)

    @property
    def actor(self):
        return self.inspection.requester

    @property
    def files(self):
        return self.inspection.authority.instances.files

    @property
    def requests(self):
        i = self.inspection
        # The Home supplies bytes only. ADMIN is a required human grant,
        # not a request to mutate the Home or exclude a running resident.
        return {i.instance.home_id: (_READ_EXPORT, i.home_generation)}

    @property
    def agent(self):
        instance = self.inspection.instance
        return SnapshotAgent(kind="assistant" if instance.custody == "personal" else "employee", id=instance.id, name=instance.name)

    async def guard(self, session, rows):
        i = self.inspection
        if i.instance.home_id not in rows:
            raise SpaceDenied("The bound Home is unavailable")
        home, grant = rows[i.instance.home_id]
        permissions = self.files.registry._view(home, grant.permissions).permissions
        required = _READ_EXPORT | Permission.ADMIN
        if grant.principal_kind != "human" or grant.subject_id != self.actor.subject_id or permissions & required != required:
            raise SpaceDenied("This Home cannot be shared by the current reader")
        try:
            await i.authority.admit_file_export(session, i, home.generation, manage=True)
        except AgentDenied as exc:
            raise SpaceDenied("Source conversation is unavailable") from exc

    async def read(self, path, maximum_bytes):
        return await export_presented_output(self.inspection, path, maximum_bytes, self.guard)

    async def recheck(self):
        try:
            async with self.files.registry.admitted(actor=self.actor, requests=self.requests) as (session, rows):
                await self.guard(session, rows)
        except (SpaceDenied, AgentDenied):
            raise unavailable() from None

    async def audience(self, authors):
        i = self.inspection
        async with self.files.registry.admitted(actor=self.actor, requests=self.requests) as (session, rows):
            await self.guard(session, rows)
            binding = await session.get(AgentConversationRow, i.thread_id)
            starter_id = binding.requester_id
            grants = (await session.scalars(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == i.instance.id, AgentInstanceGrantRow.permissions.between(1, 7)))).all()
            chat = inspect = 0
            for grant in grants:
                if await human(i.authority.instances, PrincipalRef("human", grant.user_id), session) is not None:
                    chat += bool(grant.permissions & AgentPermission.USE)
                    inspect += bool(grant.permissions & AgentPermission.INSPECT)
        starter_can_read = await i.authority.allowed(actor=PrincipalRef("human", starter_id), thread_id=i.thread_id, permission=AgentPermission.INSPECT)
        repo = get_user_repo()
        names = {}
        if repo is not None:
            for user_id in sorted({starter_id, *authors}):
                user = await repo.get_user_by_id(user_id)
                name = getattr(user, "name", None) or getattr(user, "email", None)
                if name:
                    names[user_id] = str(name)
        return {"starter": names.get(starter_id), "starter_can_read": starter_can_read, "authors": sorted({names[id_] for id_ in authors if id_ in names}), "inspect_count": inspect, "chat_count": chat}

    async def native_files(self, actor, body, descriptor):
        from app.gateway.snapshot_manifest import generated_name
        from deerflow.files.store import DescriptorSource, normalize_relative_path

        if actor != self.actor or body.thread_id != self.inspection.thread_id or not isinstance(descriptor, DescriptorSource):
            raise unavailable()
        name = generated_name(body.path)
        if name is None:
            prefix = "/mnt/user-data/outputs/"
            relative = body.path.removeprefix(prefix)
            if not body.path.startswith(prefix) or normalize_relative_path(relative) != relative:
                raise unavailable()
            name = relative.rsplit("/", 1)[-1]
        if descriptor.name != name:
            raise unavailable()
        try:
            async with self.files.registry.admitted(actor=actor, requests=self.requests) as (session, rows):
                await self.guard(session, rows)
                volume = await self.files._volume(session, rows[self.inspection.instance.home_id][0])
        except (SpaceDenied, AgentDenied):
            raise unavailable() from None
        return AgentSnapshotFiles(self, volume), descriptor


@dataclass(frozen=True)
class AgentSnapshotFiles:
    source: AgentSnapshotSource
    volume: object

    @property
    def files(self):
        return self.source.files

    @property
    def requests(self):
        return self.source.requests

    async def guard(self, session, rows):
        await self.source.guard(session, rows)

    def verify_source(self):
        from deerflow.spaces.incarnations import verify_bound

        current = self.files.catalog.verify(self.volume.spec.slot_id, previous=self.volume)
        self.files._check_incarnation(self.volume, current)
        verify_bound(current, self.volume.backing_handle, managed_home=True)

    async def copy_to(self, source, destination, **kwargs):
        from app.gateway.native_file_sources import NativeFileSources

        return await NativeFileSources.copy_verified_file(self.files, self.verify_source, source, destination, **kwargs)
