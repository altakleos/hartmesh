"""Durable conversation authority and host-owned execution admission."""

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import and_, false, or_, select
from sqlalchemy.exc import IntegrityError

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission, DefinitionSnapshot, InstanceIdentity
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.spaces.contract import Permission, PrincipalRef

AGENT_EXECUTION_CONTEXT_KEY = "__agent_execution"


@dataclass(frozen=True)
class AgentExecution:
    instance: InstanceIdentity
    requester: PrincipalRef
    definition: DefinitionSnapshot
    home_generation: int
    authority: "AgentConversations"
    thread_id: str
    thread_incarnation: str
    owner_loop: object | None = field(default=None, compare=False, repr=False)
    execution_allowed: bool = True

    @property
    def thread_paths(self):
        return {"workspace_path": "/mnt/spaces/home", "uploads_path": "/mnt/spaces/home/uploads", "outputs_path": "/mnt/spaces/home/outputs"}

    async def validate(self):
        if self.owner_loop is None or asyncio.get_running_loop() is self.owner_loop:
            return await self.authority.validate(self)
        if not self.owner_loop.is_running():
            raise AgentDenied("The host execution authority is unavailable")
        future = asyncio.run_coroutine_threadsafe(self.authority.validate(self), self.owner_loop)
        return await asyncio.wrap_future(future)

    def validate_sync(self):
        if self.owner_loop is None or not self.owner_loop.is_running():
            raise AgentDenied("Instance execution requires a live host authority loop")
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        if current is self.owner_loop:
            raise AgentDenied("Synchronous instance operations cannot block their authority loop")
        return asyncio.run_coroutine_threadsafe(self.authority.validate(self), self.owner_loop).result()


class AgentConversations:
    def __init__(self, instances, *, session_factory=None):
        self.instances = instances
        self._sf = instances._sf if instances is not None else session_factory

    async def binding(self, thread_id):
        async with self._sf() as session:
            row = await session.get(AgentConversationRow, thread_id)
            return row.instance_id if row else None

    async def clause(self, user_id, *, permission=AgentPermission.INSPECT):
        """Current grants filter before pagination; creator ownership grants nothing."""
        bound = select(AgentConversationRow.thread_id).where(AgentConversationRow.thread_id == ThreadMetaRow.thread_id).exists()
        legacy = and_(~bound, ThreadMetaRow.user_id == user_id)
        if self.instances is None:
            return legacy
        actor = PrincipalRef("human", user_id)
        try:
            await self.instances._human(actor)
        except AgentDenied:
            return false()
        async with self._sf() as session:
            candidates = (await session.execute(select(AgentInstanceRow).join(AgentInstanceGrantRow).where(AgentInstanceGrantRow.user_id == user_id))).scalars().all()
        valid = []
        for row in candidates:
            try:
                identity = InstanceIdentity.from_row(row)
            except (TypeError, ValueError):
                continue
            if identity.custody == "personal" and await self.instances.directory.human(PrincipalRef("human", identity.owner_id)) is None:
                continue
            valid.append(identity.id)
        permission_clause = AgentInstanceGrantRow.permissions.op("&")(int(permission)) == int(permission)
        if permission == AgentPermission.INSPECT:
            permission_clause = or_(permission_clause, and_(AgentConversationRow.requester_id == user_id, AgentInstanceGrantRow.permissions.op("&")(int(AgentPermission.USE)) == int(AgentPermission.USE)))
        granted = (
            select(AgentConversationRow.thread_id)
            .join(AgentInstanceRow, AgentInstanceRow.id == AgentConversationRow.instance_id)
            .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentInstanceRow.id)
            .where(
                AgentConversationRow.thread_id == ThreadMetaRow.thread_id,
                AgentInstanceRow.id.in_(valid),
                AgentInstanceGrantRow.user_id == user_id,
                permission_clause,
                AgentInstanceGrantRow.permissions.between(1, 7),
                AgentInstanceRow.status.in_(("active", "suspended", "archived")),
                AgentInstanceRow.principal_id == "agent:" + AgentInstanceRow.id,
                AgentInstanceRow.home_id.is_not(None),
            )
            .exists()
        )
        return or_(legacy, granted)

    async def allowed(self, *, actor, thread_id, permission):
        if self.instances is None:
            return False
        try:
            await self.instances._human(actor)
        except AgentDenied:
            return False
        async with self._sf() as session:
            result = (
                await session.execute(
                    select(AgentInstanceRow, AgentInstanceGrantRow.permissions, AgentConversationRow.requester_id)
                    .join(AgentConversationRow, AgentConversationRow.instance_id == AgentInstanceRow.id)
                    .join(ThreadMetaRow, ThreadMetaRow.thread_id == AgentConversationRow.thread_id)
                    .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentInstanceRow.id)
                    .where(AgentConversationRow.thread_id == thread_id, AgentInstanceGrantRow.user_id == actor.subject_id)
                )
            ).one_or_none()
        if result is None:
            return False
        row, permissions, requester_id = result
        try:
            identity = InstanceIdentity.from_row(row)
        except (TypeError, ValueError):
            return False
        if identity.status not in {"active", "suspended", "archived"} or type(permissions) is not int or not 1 <= permissions <= 7:
            return False
        if identity.custody == "personal" and await self.instances.directory.human(PrincipalRef("human", identity.owner_id)) is None:
            return False
        if permission == AgentPermission.INSPECT and requester_id == actor.subject_id and permissions & int(AgentPermission.USE):
            return True
        return permissions & int(permission) == int(permission)

    async def mutation_allowed(self, session, thread_id, user_id):
        binding = await session.get(AgentConversationRow, thread_id)
        if binding is None:
            return None
        if self.instances is None:
            return False
        # Grant/lifecycle writers also lock this parent. Keep it locked through
        # the conversation mutation so revocation cannot commit in between.
        row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == binding.instance_id).with_for_update())).scalar_one_or_none()
        try:
            identity = InstanceIdentity.from_row(row)
            await self.instances._human(PrincipalRef("human", user_id))
        except (AgentDenied, TypeError, ValueError, AttributeError):
            return False
        if identity.status == "deleted" or (identity.custody == "personal" and await self.instances.directory.human(PrincipalRef("human", identity.owner_id)) is None):
            return False
        grant = await session.get(AgentInstanceGrantRow, (binding.instance_id, user_id))
        return grant is not None and 1 <= grant.permissions <= 7 and grant.permissions & int(AgentPermission.MANAGE) == int(AgentPermission.MANAGE)

    async def create(self, *, actor, instance_id, thread_id=None, creation_id=None, metadata=None, display_name=None, permission=AgentPermission.USE):
        from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

        # Thread ID syntax remains the runtime's public contract.
        from deerflow.utils.thread_id import validate_thread_id

        thread_id = thread_id or uuid4().hex
        creation_id = creation_id or uuid4().hex
        validate_thread_id(thread_id)
        self.instances.files._operation_id(creation_id)
        view = await self.instances.get(actor=actor, instance_id=instance_id, permission=permission)
        if view.status != "active":
            raise AgentDenied("Only active instances can start conversations")

        async def admit(session, row):
            current = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())).scalar_one_or_none()
            grant = await session.get(AgentInstanceGrantRow, (instance_id, actor.subject_id))
            needed = int(permission | AgentPermission.USE)
            if current is None or current.status != "active" or current.generation != view.generation or grant is None or grant.permissions & needed != needed:
                raise AgentDenied("Instance conversation admission changed")
            await self.instances._human(actor)
            if await session.get(AgentConversationRow, thread_id) is not None:
                raise AgentDenied("Conversation identity cannot be reused")
            session.add(AgentConversationRow(thread_id=thread_id, instance_id=instance_id, requester_id=actor.subject_id, creation_id=creation_id))

        for attempt in range(3):
            async with self._sf() as session:
                existing = (await session.execute(select(AgentConversationRow).where(AgentConversationRow.requester_id == actor.subject_id, AgentConversationRow.creation_id == creation_id))).scalar_one_or_none()
                if existing is not None:
                    if existing.instance_id != instance_id:
                        raise AgentConflict("Conversation creation identity belongs to another instance")
                    row = await session.get(ThreadMetaRow, existing.thread_id)
                    if row is None:
                        raise AgentDenied("Conversation retry cannot recreate a deleted chat")
                    return ThreadMetaRepository._row_to_dict(row)
            try:
                return await ThreadMetaRepository(self._sf, instance_authority=self).create(thread_id, user_id=actor.subject_id, assistant_id="lead_agent", metadata=metadata or {}, display_name=display_name, admission=admit)
            except IntegrityError as exc:
                if attempt == 2:
                    raise AgentConflict("Conversation identity was claimed concurrently") from exc

    async def execution(self, *, actor, thread_id):
        instance_id = await self.binding(thread_id)
        if instance_id is None or not await self.allowed(actor=actor, thread_id=thread_id, permission=AgentPermission.USE) or not await self.allowed(actor=actor, thread_id=thread_id, permission=AgentPermission.INSPECT):
            raise AgentDenied("Agent conversation is unavailable")
        view = await self.instances.get(actor=actor, instance_id=instance_id, permission=AgentPermission.USE)
        if view.status != "active":
            raise AgentDenied("Agent instance is not active")
        async with self._sf() as session:
            definition = await self.instances._stored_definition(session, view.definition_revision)
        home = await self.instances.files.registry.get(actor=view.principal, space_id=view.home_id)
        if home.permissions & (Permission.READ | Permission.WRITE) != Permission.READ | Permission.WRITE:
            raise AgentDenied("Agent home is unavailable for execution")
        async with self._sf() as session:
            thread = await session.get(ThreadMetaRow, thread_id)
            if thread is None or not thread.incarnation:
                raise AgentDenied("Agent conversation is unavailable")
            return AgentExecution(view, actor, definition, home.generation, self, thread_id, thread.incarnation, asyncio.get_running_loop())

    async def validate(self, execution):
        if not execution.execution_allowed:
            raise AgentDenied("A conversation inspection context cannot execute")
        async with self._sf() as session:
            thread = await session.get(ThreadMetaRow, execution.thread_id)
            binding = await session.get(AgentConversationRow, execution.thread_id)
            if thread is None or thread.incarnation != execution.thread_incarnation or binding is None or binding.instance_id != execution.instance.id:
                raise AgentDenied("Agent conversation authority changed")
        current = await self.instances.get(actor=execution.requester, instance_id=execution.instance.id, permission=AgentPermission.USE)
        if not await self.allowed(actor=execution.requester, thread_id=execution.thread_id, permission=AgentPermission.INSPECT):
            raise AgentDenied("Agent conversation audience changed")
        if current.status != "active" or current.generation != execution.instance.generation or current.definition_revision != execution.definition.revision:
            raise AgentDenied("Agent execution authority changed")
        home = await self.instances.files.registry.get(actor=current.principal, space_id=current.home_id)
        if home.generation != execution.home_generation or home.permissions & (Permission.READ | Permission.WRITE) != Permission.READ | Permission.WRITE:
            raise AgentDenied("Agent storage authority changed")

    async def inspection(self, *, actor, thread_id):
        """Adopted graph projection for authorized readers; never a run grant."""
        from deerflow.persistence.spaces.model import SpaceRow

        instance_id = await self.binding(thread_id)
        if instance_id is None or not await self.allowed(actor=actor, thread_id=thread_id, permission=AgentPermission.INSPECT):
            raise AgentDenied("Agent conversation is unavailable")
        async with self._sf() as session:
            row = await session.get(AgentInstanceRow, instance_id)
            definition = await self.instances._stored_definition(session, row.definition_revision)
            thread = await session.get(ThreadMetaRow, thread_id)
            home = await session.get(SpaceRow, row.home_id)
            if thread is None or home is None:
                raise AgentDenied("Agent conversation is unavailable")
            return AgentExecution(InstanceIdentity.from_row(row), actor, definition, home.generation, self, thread_id, thread.incarnation, asyncio.get_running_loop(), execution_allowed=False)

    async def update_run_metadata(self, execution, *, status=None, display_name=None):
        """Host worker bookkeeping under the admitted Use capability, not Manage."""
        await execution.validate()
        async with self._sf() as session, session.begin():
            await self.instances._reserve_writer(session)
            thread = (await session.execute(select(ThreadMetaRow).where(ThreadMetaRow.thread_id == execution.thread_id).with_for_update())).scalar_one_or_none()
            parent = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == execution.instance.id).with_for_update())).scalar_one_or_none()
            grant = await session.get(AgentInstanceGrantRow, (execution.instance.id, execution.requester.subject_id))
            binding = await session.get(AgentConversationRow, execution.thread_id)
            if (
                parent is None
                or parent.status != "active"
                or parent.generation != execution.instance.generation
                or parent.definition_revision != execution.definition.revision
                or grant is None
                or not 1 <= grant.permissions <= 7
                or not grant.permissions & int(AgentPermission.USE)
                or thread is None
                or thread.incarnation != execution.thread_incarnation
                or binding is None
                or binding.instance_id != parent.id
            ):
                raise AgentDenied("Agent execution bookkeeping authority changed")
            if status is not None:
                thread.status = status
            if display_name is not None:
                thread.display_name = display_name
            thread.updated_at = datetime.now(UTC)
