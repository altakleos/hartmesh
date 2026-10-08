"""Agent intent and qualified Storage Spaces containment, with retained Homes."""

from datetime import UTC, datetime

from sqlalchemy import select

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission, DefinitionSnapshot
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.spaces.attachments import AttachmentPending
from deerflow.spaces.contract import MutationMode, Permission, PrincipalRef, SpaceConflict, validate_generation
from deerflow.spaces.registry import _stored_permissions
from deerflow.utils.file_io import await_drained


class _Complete(Exception):
    pass


class InstanceLifecycle:
    def __init__(self, agents):
        self.agents = agents

    async def _parent(self, session, actor, instance_id):
        row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())).scalar_one_or_none()
        grant = await session.get(AgentInstanceGrantRow, (instance_id, actor.subject_id))
        if row is None or grant is None:
            raise AgentDenied("Agent lifecycle is unavailable")
        view = self.agents._view(row, grant.permissions)
        if not view.permissions & AgentPermission.MANAGE:
            raise AgentDenied("Agent lifecycle requires current Manage")
        await self.agents._human(actor)
        return row, grant, view

    @staticmethod
    def _match(intent, actor, generation, request, *, company=False):
        if (intent.actor_id != actor.subject_id and not company) or intent.generation != generation or intent.request != request:
            raise AgentConflict("Lifecycle identity belongs to another request")
        if intent.phase not in {"pending", "complete", "abandoned"} or type(intent.attachment_ids) is not list or len(intent.attachment_ids) > 32:
            raise AgentConflict("Invalid lifecycle containment scope")
        for key in intent.attachment_ids:
            from deerflow.spaces.service import SpaceFiles

            SpaceFiles._operation_id(key)

    async def _restore_ready(self, session, view):
        await self.agents._human(view.supervisor)
        if view.custody == "personal":
            await self.agents._human(PrincipalRef("human", view.owner_id))
        await self.agents._stored_definition(session, view.definition_revision)
        home = await session.get(SpaceRow, view.home_id)
        grant = await session.get(SpaceGrantRow, (view.home_id, view.principal.kind, view.principal.subject_id))
        if home is None or home.status != "active" or home.mode != "native" or grant is None or _stored_permissions(grant.permissions, MutationMode.NATIVE) & (Permission.READ | Permission.WRITE) != Permission.READ | Permission.WRITE:
            raise AgentDenied("Restore requires an active Home and current resident read/write grants")
        # Existing provider verification; never adopt a folder or reconstruct roots.
        await self.agents.files._volume(session, home)

    async def _grant_ready(self, session, actor, instance_id, member, permissions):
        if member == actor and not permissions & int(AgentPermission.MANAGE):
            raise AgentDenied("Do not retire your own lifecycle management grant")
        if permissions & int(AgentPermission.MANAGE) == 0:
            managers = (await session.execute(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == instance_id))).scalars().all()
            live = [g for g in managers if g.user_id != member.subject_id and g.permissions & int(AgentPermission.MANAGE) and await self.agents.directory.human(PrincipalRef("human", g.user_id)) is not None]
            if not live:
                raise AgentDenied("Retain a current human manager")

    async def abandon(self, *, actor, instance_id, expected_generation, operation_id):
        """Resolve obsolete intent without effects or reactivating a resident."""
        validate_generation(expected_generation)
        self.agents.files._operation_id(operation_id)
        view = await self.agents.get(actor=actor, instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=True)
        async with self.agents._sf() as session:
            terminal = await session.get(AgentLifecycleRow, (instance_id, operation_id))
            if terminal is not None and terminal.phase == "abandoned" and terminal.home_id == view.home_id and (terminal.actor_id == actor.subject_id or view.custody == "company"):
                await self.agents.files.registry.get(actor=actor, space_id=view.home_id, permission=Permission.ADMIN)
                return {"instance": view, "complete": True, "abandoned": True, "operation_id": operation_id}
        if getattr(self.agents.files, "attachments", None) is None:
            from deerflow.agent_instances.runtime import qualified_attachments

            await qualified_attachments(self.agents.files)

        async def perform():
            async with self.agents.files.registry.admitted(actor=actor, requests={view.home_id: (Permission.ADMIN, None)}) as (session, homes):
                row, grant, current = await self._parent(session, actor, instance_id)
                intent = await session.get(AgentLifecycleRow, (instance_id, operation_id))
                if intent is None or (intent.actor_id != actor.subject_id and current.custody != "company") or intent.home_id != current.home_id or current.home_id != view.home_id:
                    raise AgentConflict("Pending lifecycle intent is unavailable")
                if intent.phase == "abandoned":
                    return {"instance": current, "complete": True, "abandoned": True, "operation_id": operation_id}
                if intent.phase != "pending" or row.generation != expected_generation or row.generation != intent.generation + 1 or row.status != "suspended":
                    raise AgentConflict("Lifecycle generation changed")
                if not await self._scope_fenced(session, intent):
                    raise AgentConflict("Confirm captured containment before abandoning the operation")
                intent.phase = "abandoned"
                intent.home_generation = homes[current.home_id][0].generation
                intent.resolved_by = actor.subject_id
                row.updated_at = datetime.now(UTC)
                return {"instance": self.agents._view(row, grant.permissions), "complete": True, "abandoned": True, "operation_id": operation_id}

        return await await_drained(perform())

    async def status(self, *, actor, instance_id):
        try:
            view = await self.agents.get(actor=actor, instance_id=instance_id, permission=AgentPermission.INSPECT, include_deleted=True)
        except AgentDenied:
            view = await self.agents.get(actor=actor, instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=True)
        async with self.agents._sf() as session:
            intents = (await session.execute(select(AgentLifecycleRow).where(AgentLifecycleRow.instance_id == instance_id).order_by(AgentLifecycleRow.generation.desc()).limit(20))).scalars().all()
            result = []
            for intent in intents:
                retry = {"operation_id": intent.operation_id, "generation": intent.generation, "action": intent.request["action"]}
                for name in ("supervisor_id", "member_id", "permissions"):
                    if name in intent.request:
                        retry[name] = intent.request[name]
                if intent.request.get("definition_revision"):
                    snapshot = await self.agents._stored_definition(session, intent.request["definition_revision"])
                    retry["definition_name"] = snapshot.config["name"].lower()
                result.append(
                    {
                        "operation_id": intent.operation_id,
                        "generation": intent.generation,
                        "action": intent.request["action"],
                        "complete": intent.phase != "pending",
                        "abandoned": intent.phase == "abandoned",
                        "retry": retry if intent.actor_id == actor.subject_id or view.custody == "company" else None,
                    }
                )
            return result

    @staticmethod
    async def _scope_fenced(session, intent):
        rows = (
            await session.execute(
                select(SpaceAttachmentRow.id, SpaceAttachmentRow.phase)
                .join(SpaceMountRow, SpaceMountRow.attachment_id == SpaceAttachmentRow.id)
                .where(SpaceMountRow.space_id == intent.home_id, SpaceAttachmentRow.id.in_(intent.attachment_ids))
            )
        ).all()
        if {key for key, _phase in rows} != set(intent.attachment_ids):
            raise AgentConflict("Lifecycle containment scope no longer belongs to Home")
        return all(phase == "fenced" for _key, phase in rows)

    async def change(self, *, actor, instance_id, expected_generation, operation_id, action, definition=None, supervisor=None, member=None, permissions=None):
        validate_generation(expected_generation)
        self.agents.files._operation_id(operation_id)
        if action not in {"suspend", "archive", "delete", "restore", "adopt", "supervise", "grant"}:
            raise ValueError("Unknown agent lifecycle action")
        if (action == "adopt") != (definition is not None) or (action == "supervise") != (supervisor is not None) or (action == "grant") != (member is not None and permissions is not None):
            raise ValueError("Lifecycle action fields do not match")
        if action != "grant" and (member is not None or permissions is not None):
            raise ValueError("Grant fields require a grant action")
        request = {"action": action}
        if definition is not None:
            if not isinstance(definition, DefinitionSnapshot):
                raise ValueError("Adoption requires a host-authorized captured definition")
            definition = definition.validated()
            request["definition_revision"] = definition.revision
        if supervisor is not None:
            if not isinstance(supervisor, PrincipalRef) or supervisor.kind != "human":
                raise ValueError("Supervision requires a human")
            request["supervisor_id"] = supervisor.subject_id
        if member is not None:
            if not isinstance(member, PrincipalRef) or member.kind != "human" or type(permissions) is not int or not 0 <= permissions <= 7:
                raise ValueError("Agent grants require a human and explicit permission bits")
            request.update(member_id=member.subject_id, permissions=permissions)

        async def perform():
            view = await self.agents.get(actor=actor, instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=True)
            async with self.agents._sf() as session:
                existing = await session.get(AgentLifecycleRow, (instance_id, operation_id))
                if existing is not None:
                    self._match(existing, actor, expected_generation, request, company=view.custody == "company")
                    if existing.phase != "pending":
                        return {"instance": view, "complete": True, "abandoned": existing.phase == "abandoned", "operation_id": operation_id}
                    if existing.home_id != view.home_id:
                        raise AgentConflict("Lifecycle Home identity changed")
                    ids, home_generation = list(existing.attachment_ids), existing.home_generation
                    if await self._scope_fenced(session, existing):
                        ids = []
                else:
                    ids = None
                    home_generation = (await self.agents.files.registry.get(actor=actor, space_id=view.home_id, permission=Permission.ADMIN)).generation
            attachments = getattr(self.agents.files, "attachments", None)
            if attachments is None:
                from deerflow.agent_instances.runtime import qualified_attachments

                _provider, attachments = await qualified_attachments(self.agents.files)

            async def withdraw(session, identities):
                row, grant, current = await self._parent(session, actor, instance_id)
                if current.home_id != view.home_id:
                    raise AgentConflict("Lifecycle Home identity changed")
                intent = await session.get(AgentLifecycleRow, (instance_id, operation_id))
                candidates = [key for key, _container in identities]
                if intent is not None:
                    self._match(intent, actor, expected_generation, request, company=current.custody == "company")
                    if intent.phase != "pending":
                        raise _Complete(intent.phase == "abandoned")
                    if row.generation != intent.generation + 1 or row.status != "suspended" or not set(candidates) <= set(intent.attachment_ids):
                        raise AgentConflict("Lifecycle containment scope changed")
                    return
                pending = await session.scalar(select(AgentLifecycleRow.operation_id).where(AgentLifecycleRow.instance_id == instance_id, AgentLifecycleRow.phase == "pending"))
                if pending is not None or current.generation != expected_generation or current.generation >= 2**31 - 1 or current.status == "provisioning":
                    raise AgentConflict("Agent lifecycle changed; resolve its current intent")
                if action == "restore" and (current.status == "active" or candidates):
                    raise AgentConflict("Restore requires an inactive agent without conflicting native views")
                if action != "restore" and current.status == "deleted":
                    raise AgentDenied("Restore the removed agent before modifying it")
                if len(candidates) > 32:
                    raise AgentConflict("Containment scope exceeds the qualified bound")
                if supervisor is not None:
                    await self.agents._human(supervisor)
                if member is not None and permissions:
                    await self.agents._human(member)
                if action == "grant":
                    await self._grant_ready(session, actor, instance_id, member, permissions)
                if definition is not None:
                    if definition.owner_id != actor.subject_id:
                        raise AgentDenied("Fresh definition adoption requires its current human owner")
                    stored = await session.get(AgentDefinitionRevisionRow, definition.revision)
                    if stored is None:
                        session.add(AgentDefinitionRevisionRow(revision=definition.revision, owner_id=definition.owner_id, config=definition.config, soul=definition.soul))
                    elif await self.agents._stored_definition(session, definition.revision) != definition:
                        raise AgentConflict("Adopted definition revision changed")
                if action == "restore":
                    await self._restore_ready(session, current)
                intent = AgentLifecycleRow(
                    instance_id=instance_id,
                    operation_id=operation_id,
                    actor_id=actor.subject_id,
                    generation=expected_generation,
                    home_generation=home_generation,
                    home_id=current.home_id,
                    request=request,
                    attachment_ids=candidates,
                    phase="pending",
                    prior_status=current.status,
                )
                session.add(intent)
                row.status = "suspended"
                row.generation += 1
                row.updated_at = datetime.now(UTC)

            try:
                # An empty committed scope must never become an all-current retry.
                if ids is None or ids:
                    await attachments.retire(actor=actor, space_id=view.home_id, expected_generation=home_generation, attachment_ids=ids, admission=withdraw)
            except _Complete as terminal:
                return {"instance": await self.agents.get(actor=actor, instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=True), "complete": True, "abandoned": terminal.args[0], "operation_id": operation_id}
            except AttachmentPending:
                async with self.agents._sf() as session:
                    committed = await session.get(AgentLifecycleRow, (instance_id, operation_id))
                    if committed is None:
                        raise AgentConflict("Lifecycle withdrawal was not committed; refresh before retrying") from None
                return {"instance": await self.agents.get(actor=actor, instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=True), "complete": False, "operation_id": operation_id}

            async def finalize():
                async with self.agents.files.registry.admitted(actor=actor, requests={view.home_id: (Permission.ADMIN, None)}) as (session, homes):
                    row, grant, current = await self._parent(session, actor, instance_id)
                    intent = await session.get(AgentLifecycleRow, (instance_id, operation_id))
                    if intent is None:
                        raise AgentConflict("Lifecycle intent is unavailable")
                    self._match(intent, actor, expected_generation, request, company=current.custody == "company")
                    if intent.phase != "pending":
                        return {"instance": current, "complete": True, "abandoned": intent.phase == "abandoned", "operation_id": operation_id}
                    if row.generation != intent.generation + 1 or row.status != "suspended" or current.home_id != intent.home_id or current.home_id != view.home_id:
                        raise AgentConflict("Lifecycle generation changed")
                    if not await self._scope_fenced(session, intent):
                        raise AttachmentPending("Lifecycle containment is not confirmed")
                    intent.home_generation = homes[current.home_id][0].generation
                    if action == "restore":
                        await self._restore_ready(session, current)
                    if action == "adopt":
                        if await self.agents._stored_definition(session, definition.revision) != definition:
                            raise AgentConflict("Adopted definition revision changed")
                        row.definition_revision = definition.revision
                    if supervisor is not None:
                        await self.agents._human(supervisor)
                        row.supervisor_id = supervisor.subject_id
                    if member is not None:
                        await self._grant_ready(session, actor, instance_id, member, permissions)
                        member_grant = await session.get(AgentInstanceGrantRow, (instance_id, member.subject_id))
                        if permissions:
                            await self.agents._human(member)
                            if member_grant is None:
                                session.add(AgentInstanceGrantRow(instance_id=instance_id, user_id=member.subject_id, permissions=permissions))
                            else:
                                member_grant.permissions = permissions
                        elif member_grant is not None:
                            await session.delete(member_grant)
                    row.status = {"suspend": "suspended", "archive": "archived", "delete": "deleted", "restore": "active"}.get(action, intent.prior_status)
                    row.updated_at = datetime.now(UTC)
                    intent.phase = "complete"
                    intent.resolved_by = actor.subject_id
                    return {"instance": self.agents._view(row, grant.permissions), "complete": True, "operation_id": operation_id}

            try:
                return await finalize()
            except SpaceConflict:
                return {"instance": await self.agents.get(actor=actor, instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=True), "complete": False, "operation_id": operation_id}

        return await await_drained(perform())
